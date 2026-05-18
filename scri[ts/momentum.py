#!/usr/bin/env python3
"""
momentum.py

Hybrid momentum engine for EURUSD, GBPUSD, USDJPY, XAUUSD.

Session-seeding rule:
  - Exactly 4 TD requests per calendar day (UTC).
  - One request per session window: Sydney / Tokyo / London / New York.
  - Window is open for TD_SEED_WINDOW_MINUTES after each session start.
  - Window is skipped when the session-start hour falls on a weekend or
    market holiday (no attempt is recorded, budget not wasted).
  - Emergency bootstrap (ALLOW_BOOTSTRAP_ANYTIME) is available for empty or
    too-short histories when outside all windows, capped at
    BOOTSTRAP_ANYTIME_PER_DAY, and refused if a window seed already succeeded
    today.

Files written into MOMENTUM_DATA_DIR (default: script directory):
  momentum_stooq_snapshots.jsonl
  momentum_closes.json
  momentum_telegram_alerts.json
  momentum_td_usage.json
"""
from __future__ import annotations

import csv
import json
import logging
import os
import time
from datetime import date, datetime, timedelta, timezone
from io import StringIO
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple, Union

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

try:
    from dotenv import load_dotenv
    _env_path = Path(__file__).resolve().parent / ".env"
    load_dotenv(dotenv_path=_env_path, override=False)
except ImportError:
    pass

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    try:
        from backports.zoneinfo import ZoneInfo  # type: ignore
    except ImportError:
        raise ImportError(
            "momentum.py requires the 'tzdata' package on environments without system "
            "timezone data (e.g. GitHub Actions Ubuntu runners). "
            "Add 'tzdata' to requirements.txt."
        )

log = logging.getLogger(__name__)

_LAST_MOMENTUM_DIAGNOSTICS: Dict[str, Dict[str, Union[str, float, bool]]] = {}
_LAST_EMA_DIAGNOSTICS: Dict[str, Dict[str, Union[str, float, bool]]] = {}
_LAST_EMA_STATE: Dict[str, Dict[str, Any]] = {}
_LAST_H1_EMA_STATE: Dict[str, Dict[str, Any]] = {}   # H1 state before scraper D1 override
_MOMENTUM_STARTUP_LOGGED = False

UTC = timezone.utc


# ---------------------------------------------------------------------------
# Env helpers
# ---------------------------------------------------------------------------

def _run_id() -> str:
    return os.environ.get("SCRAPER_RUN_ID") or os.environ.get("RUN_ID") or "local"


def _env_positive_int(name: str, default: int, minimum: int = 1) -> int:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        value = int(raw)
        if value < minimum:
            raise ValueError
        return value
    except ValueError:
        log.warning("momentum: invalid %s=%r; using default=%s", name, raw, default)
        return default


def _env_float(name: str, default: float, minimum: Optional[float] = None, maximum: Optional[float] = None) -> float:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        value = float(raw)
        if minimum is not None and value < minimum:
            raise ValueError
        if maximum is not None and value > maximum:
            raise ValueError
        return value
    except ValueError:
        log.warning("momentum: invalid %s=%r; using default=%s", name, raw, default)
        return default


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


# ---------------------------------------------------------------------------
# Constants / configuration
# ---------------------------------------------------------------------------

TWELVEDATA_API_KEY = os.environ.get("TWELVEDATA_API_KEY", "").strip()
TWELVEDATA_BASE = "https://api.twelvedata.com"


_DEFAULT_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)
_USER_AGENT = os.environ.get("SCRAPER_UA", _DEFAULT_UA)

LOOKBACK_BARS = _env_positive_int("MOMENTUM_LOOKBACK_BARS", 12)
EMA_FAST = _env_positive_int("MOMENTUM_EMA_FAST", 8)
PERSIST_WINDOW = _env_positive_int("MOMENTUM_PERSIST_WINDOW", 6)
SIGNAL_FLOOR = _env_float("MOMENTUM_SIGNAL_FLOOR", 0.01, minimum=0.0)
EMA_SIGNAL_FAST = _env_positive_int("MOMENTUM_EMA_SIGNAL_FAST", 20)
EMA_SIGNAL_SLOW = _env_positive_int("MOMENTUM_EMA_SIGNAL_SLOW", 50)
MOMENTUM_RSI_PERIOD = _env_positive_int("MOMENTUM_RSI_PERIOD", 14)
MOMENTUM_RSI_OVERBOUGHT = _env_float("MOMENTUM_RSI_OVERBOUGHT", 70.0, minimum=0.0)
MOMENTUM_RSI_OVERSOLD = _env_float("MOMENTUM_RSI_OVERSOLD", 30.0, minimum=0.0)
MOMENTUM_CAP = 1.5

# ---------------------------------------------------------------------------
# Telegram — read credentials once at import time (same token as ema.py)
# ---------------------------------------------------------------------------
TELEGRAM_BOT_TOKEN: str = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
_tg_chat_raw = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
TELEGRAM_CHAT_ID: int = 0
try:
    TELEGRAM_CHAT_ID = int(_tg_chat_raw) if _tg_chat_raw else 0
except ValueError:
    pass  # stays 0 → alerts disabled; warning emitted at dispatch time

# Proximity threshold: alert when price is within this % of an EMA.
TELEGRAM_PROXIMITY_PCT: float = _env_float("TELEGRAM_PROXIMITY_PCT", 0.0015)
# Imminent-cross threshold: alert when |EMA20 − EMA50| / EMA50 < this value.
TELEGRAM_CROSS_IMMINENT_PCT: float = _env_float("TELEGRAM_CROSS_IMMINENT_PCT", 0.0010)

# ---------------------------------------------------------------------------
# Groq AI — optional enrichment for H1 EMA Telegram alerts
# Fail-soft: if AI credentials/API/network fail, the alert still sends.
# Shares groq_ai_state.json with pivot.py for a unified cooldown.
# ---------------------------------------------------------------------------
GROQ_AI_ENABLED: bool = os.environ.get("GROQ_AI_ENABLED", "0").strip().lower() in ("1", "true", "yes", "on")
GROQ_API_KEY: str = os.environ.get("GROQ_API_KEY", "").strip()
GROQ_AI_MODEL: str = os.environ.get("GROQ_AI_MODEL", "llama-3.1-8b-instant").strip()
GROQ_AI_TIMEOUT: int = int(os.environ.get("GROQ_AI_TIMEOUT", "12"))
GROQ_AI_MAX_CHARS: int = int(os.environ.get("GROQ_AI_MAX_CHARS", "650"))
# Per-run cap shared across all Groq calls this process (default 8).
GROQ_AI_MAX_PER_RUN: int = max(1, int(os.environ.get("GROQ_AI_MAX_PER_RUN", "8")))
# Minutes to back off after receiving a 429 from Groq.
GROQ_AI_COOLDOWN_MINUTES: int = max(1, int(os.environ.get("GROQ_AI_COOLDOWN_MINUTES", "30")))
_groq_ai_calls_this_run: int = 0  # module-level per-run counter

_EMA_BIAS_MAP: Dict[str, float] = {
    "bullish_bias": 1.0,
    "bearish_bias": -1.0,
    "neutral_bias": 0.0,
}

PAIR_NORM: Dict[str, float] = {
    "eurusd": 0.4,
    "gbpusd": 0.5,
    "usdjpy": 0.4,
    "xauusd": 1.0,
}

SLOPE_THRESH: Dict[str, float] = {
    "eurusd": 0.00004,
    "gbpusd": 0.00004,
    "usdjpy": 0.00004,
    "xauusd": 0.00008,
}

TD_SYMBOLS: Dict[str, str] = {
    "eurusd": "EUR/USD",
    "gbpusd": "GBP/USD",
    "usdjpy": "USD/JPY",
    "xauusd": "XAU/USD",
}


STOOQ_SYMBOLS: Dict[str, str] = {
    "eurusd": "eurusd",
    "gbpusd": "gbpusd",
    "usdjpy": "usdjpy",
    "xauusd": "xauusd",
}

# Fallback symbol candidates tried in order when the primary symbol returns N/D.
# Stooq uses plain ticker names (no =x suffix); gold has several common aliases.
_STOOQ_FALLBACKS: Dict[str, List[str]] = {
    "xauusd": ["xauusd", "gold", "gc.f"],
}

DEFAULT_MAIN_PAIRS: Tuple[str, ...] = ("eurusd", "gbpusd", "usdjpy", "xauusd")

# Local TD daily seed written by scraper.py.  This is intentionally read-only here:
# scraper.py owns the Twelve Data request, momentum.py only consumes the local file.
SCRAPER_COMPONENT_FILE = Path(os.environ.get("MACRO_COMPONENTS_FILE", "public/macro_components.json"))
PREFER_SCRAPER_DAILY_EMA = _env_bool("MOMENTUM_PREFER_SCRAPER_DAILY_EMA", True)
SOURCE_SCRAPER_TD_DAILY = "scraper_td_daily"

TD_REQUEST_LIMIT_PER_DAY = _env_positive_int("MOMENTUM_TD_REQUEST_LIMIT_PER_DAY", 4)
TD_REQUESTS_PER_RUN_MAX = _env_positive_int("MOMENTUM_TD_REQUESTS_PER_RUN_MAX", 1)
TD_MIN_SECONDS_BETWEEN = _env_positive_int("MOMENTUM_TD_MIN_SECONDS_BETWEEN", 10)
TD_429_COOLDOWN_MINUTES = _env_positive_int("MOMENTUM_TD_429_COOLDOWN_MINUTES", 60)
TD_SEED_WINDOW_MINUTES = _env_positive_int("MOMENTUM_TD_SEED_WINDOW_MINUTES", 20)
SEED_CLOSES_TARGET = _env_positive_int("MOMENTUM_SEED_CLOSES_TARGET", 60)
MAX_H1_HISTORY = _env_positive_int("MOMENTUM_MAX_H1_HISTORY", 500)
REPAIR_GAP_HOURS = _env_positive_int("MOMENTUM_REPAIR_GAP_HOURS", 2)
SNAPSHOT_RETENTION_DAYS = _env_positive_int("MOMENTUM_SNAPSHOT_RETENTION_DAYS", 10)
DEFAULT_TIMEOUT = _env_positive_int("MOMENTUM_DEFAULT_TIMEOUT", 20)
STRICT_ONE_ATTEMPT_PER_WINDOW = _env_bool("MOMENTUM_STRICT_ONE_ATTEMPT_PER_WINDOW", True)
ALLOW_BOOTSTRAP_ANYTIME = _env_bool("MOMENTUM_ALLOW_BOOTSTRAP_ANYTIME", False)
BOOTSTRAP_ANYTIME_PER_DAY = _env_positive_int("MOMENTUM_BOOTSTRAP_ANYTIME_PER_DAY", 1)
# Per-run bootstrap cap. On CI (GitHub Actions) TD_USAGE_FILE does not persist between
# runs, so BOOTSTRAP_ANYTIME_PER_DAY resets every run and is not a reliable daily guard.
# BOOTSTRAP_PER_RUN_MAX is checked against a run-local counter instead.
# Default 4 = seed all pairs in one CI run. On Alwaysdata set to 1 (persistent file guards the rest).
BOOTSTRAP_PER_RUN_MAX = _env_positive_int("MOMENTUM_BOOTSTRAP_PER_RUN_MAX", 4)

# Friday close: hour >= this value means FX has closed for the weekend.
FX_WEEK_CLOSE_HOUR_UTC = _env_positive_int("MOMENTUM_FX_WEEK_CLOSE_HOUR_UTC", 22, minimum=0)
# Sunday reopen: hour >= this value means FX has reopened.
FX_WEEK_REOPEN_HOUR_UTC = _env_positive_int("MOMENTUM_FX_WEEK_REOPEN_HOUR_UTC", 22, minimum=0)

# Session windows: (IANA tz, local hour, local minute) at session open.
SESSION_STARTS: Dict[str, Tuple[str, int, int]] = {
    "sydney":   ("Australia/Sydney",  8, 0),
    "tokyo":    ("Asia/Tokyo",        9, 0),
    "london":   ("Europe/London",     8, 0),
    "new_york": ("America/New_York",  8, 0),
}

SOURCE_HOURLY = "stooq_h1"
SOURCE_TD_SEEDED = "stooq_h1+td_seeded"
SOURCE_UNAVAILABLE = "unavailable"

# CI detection: GitHub Actions sets CI=true. If MOMENTUM_DATA_DIR is not explicitly
# set but we are running in CI, default to public_html (matches workflow SCRAPER_OUTPUT_DIR).
# On Alwaysdata or local, falls back to the script directory.
def _default_base_dir() -> Path:
    explicit = os.environ.get("MOMENTUM_DATA_DIR", "").strip()
    if explicit:
        return Path(explicit)
    if os.environ.get("CI", "").lower() in ("true", "1"):
        return Path("public_html")
    return Path(__file__).resolve().parent

BASE_DIR = _default_base_dir()
BASE_DIR.mkdir(parents=True, exist_ok=True)
SNAPSHOT_FILE = BASE_DIR / "momentum_stooq_snapshots.jsonl"
H1_CLOSES_FILE = BASE_DIR / "momentum_closes.json"
TD_USAGE_FILE  = BASE_DIR / "momentum_td_usage.json"


# ---------------------------------------------------------------------------
# Startup logging
# ---------------------------------------------------------------------------

def _log_startup_once() -> None:
    global _MOMENTUM_STARTUP_LOGGED
    if _MOMENTUM_STARTUP_LOGGED:
        return
    _MOMENTUM_STARTUP_LOGGED = True
    log.info(
        "[startup][momentum][run_id=%s] td_key=%s lookback=%s ema_fast=%s "
        "persist=%s signal_floor=%.4f seed_day_limit=%s seed_run_limit=%s "
        "td_seed_window=%sm strict_window=%s bootstrap_anytime=%s "
        "bootstrap_per_run_max=%s sessions=%s",
        _run_id(),
        "present" if TWELVEDATA_API_KEY else "missing",
        LOOKBACK_BARS, EMA_FAST, PERSIST_WINDOW, SIGNAL_FLOOR,
        TD_REQUEST_LIMIT_PER_DAY, TD_REQUESTS_PER_RUN_MAX,
        TD_SEED_WINDOW_MINUTES, STRICT_ONE_ATTEMPT_PER_WINDOW,
        ALLOW_BOOTSTRAP_ANYTIME, BOOTSTRAP_PER_RUN_MAX,
        ",".join(SESSION_STARTS.keys()),
    )


# ---------------------------------------------------------------------------
# Pair helpers
# ---------------------------------------------------------------------------

def _normalize_pair_code(value: str) -> str:
    return "".join(ch for ch in (value or "").lower() if ch.isalnum())


def _parse_main_pairs(value: Any) -> List[str]:
    if isinstance(value, str):
        items = [v.strip() for v in value.split(",") if v.strip()]
    elif isinstance(value, (list, tuple, set)):
        items = [str(v).strip() for v in value if str(v).strip()]
    else:
        items = []
    out: List[str] = []
    seen: Set[str] = set()
    for item in items:
        code = _normalize_pair_code(item)
        if code and code not in seen:
            out.append(code)
            seen.add(code)
    return out


def _default_pairs() -> List[str]:
    env_pairs = _parse_main_pairs(os.environ.get("MOMENTUM_MAIN_PAIRS", ""))
    if env_pairs:
        return [p for p in env_pairs if p in TD_SYMBOLS]
    cfg_path = os.environ.get("SCRAPER_CONFIG", "").strip()
    if cfg_path:
        try:
            with open(cfg_path, encoding="utf-8") as f:
                cfg = json.load(f)
            cfg_pairs = _parse_main_pairs((cfg or {}).get("main_pairs", []))
            if cfg_pairs:
                return [p for p in cfg_pairs if p in TD_SYMBOLS]
        except Exception as exc:
            log.warning("momentum: main_pairs config load failed: %s", exc)
    return list(DEFAULT_MAIN_PAIRS)


def _min_required_bars() -> int:
    return max(LOOKBACK_BARS + 1, EMA_FAST + 1, PERSIST_WINDOW + 1, EMA_SIGNAL_SLOW + 2) + 1


# ---------------------------------------------------------------------------
# Time utilities
# ---------------------------------------------------------------------------

def _now_utc() -> datetime:
    return datetime.now(UTC)


def _hour_bucket(dt: datetime) -> datetime:
    return dt.astimezone(UTC).replace(minute=0, second=0, microsecond=0)


def _iso_z(dt: datetime) -> str:
    return dt.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _parse_dt(value: Any) -> Optional[datetime]:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    text = str(value).strip().replace("Z", "+00:00")
    if not text:
        return None
    fmts = [None, "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M",
            "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"]
    for fmt in fmts:
        try:
            dt = datetime.fromisoformat(text) if fmt is None else datetime.strptime(text, fmt)
            return dt if dt.tzinfo else dt.replace(tzinfo=UTC)
        except Exception:
            continue
    return None


# ---------------------------------------------------------------------------
# JSON helpers
# ---------------------------------------------------------------------------

def _load_json(path: Path, default: Any) -> Any:
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def _save_json(path: Path, data: Any) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        os.replace(tmp, path)
        try:
            os.chmod(path, 0o644)
        except OSError:
            pass
    except OSError as exc:
        log.error("momentum: _save_json failed (disk full?) path=%s err=%s", path, exc)
        try:
            tmp.unlink(missing_ok=True)
        except OSError:
            pass


# ---------------------------------------------------------------------------
# HTTP session
# ---------------------------------------------------------------------------

def _build_session() -> requests.Session:
    retry = Retry(
        total=1,
        backoff_factor=0.8,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=frozenset(["GET"]),
        respect_retry_after_header=True,
    )
    s = requests.Session()
    s.headers.update({
        "User-Agent": _USER_AGENT,
        "Accept": "application/json, text/csv, */*",
        "Accept-Language": "en-US,en;q=0.9",
    })
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s


# ---------------------------------------------------------------------------
# Market-hours / holiday guard
# ---------------------------------------------------------------------------

def _parse_holiday_dates(value: str) -> Set[date]:
    out: Set[date] = set()
    for token in (value or "").split(","):
        t = token.strip()
        if not t:
            continue
        try:
            out.add(datetime.strptime(t, "%Y-%m-%d").date())
        except ValueError:
            log.warning("momentum: ignoring invalid holiday entry=%r", t)
    return out


_HOLIDAY_MAP_CACHE: Optional[Dict[str, Set[date]]] = None


def _holiday_map() -> Dict[str, Set[date]]:
    """
    Two env styles supported:
      1) MOMENTUM_MARKET_HOLIDAYS=2026-12-25,2026-01-01  → applies to all pairs
      2) MOMENTUM_MARKET_HOLIDAYS_JSON={"all":["2026-12-25"],"xauusd":["2026-04-03"]}

    Result is module-level cached on first call.
    """
    global _HOLIDAY_MAP_CACHE
    if _HOLIDAY_MAP_CACHE is not None:
        return _HOLIDAY_MAP_CACHE
    out: Dict[str, Set[date]] = {
        "all": _parse_holiday_dates(os.environ.get("MOMENTUM_MARKET_HOLIDAYS", ""))
    }
    raw = os.environ.get("MOMENTUM_MARKET_HOLIDAYS_JSON", "").strip()
    if raw:
        try:
            data = json.loads(raw)
            if isinstance(data, dict):
                for key, values in data.items():
                    if isinstance(values, list):
                        joined = ",".join(str(v) for v in values)
                        out[_normalize_pair_code(key) or "all"] = _parse_holiday_dates(joined)
        except Exception as exc:
            log.warning("momentum: invalid MOMENTUM_MARKET_HOLIDAYS_JSON: %s", exc)
    _HOLIDAY_MAP_CACHE = out
    return out


def _is_market_holiday(dt: datetime, pair: str = "") -> bool:
    d = dt.astimezone(UTC).date()
    code = _normalize_pair_code(pair)
    hm = _holiday_map()
    if d in hm.get("all", set()):
        return True
    return d in hm.get(code, set())


def _hour_is_tradable(hour_start_utc: datetime, pair: str = "") -> bool:
    """Return True iff the given UTC hour belongs to an open FX session."""
    hour_start_utc = _hour_bucket(hour_start_utc)
    if _is_market_holiday(hour_start_utc, pair):
        return False
    wd = hour_start_utc.weekday()  # Mon=0 … Sun=6
    if wd == 5:  # Saturday – always closed
        return False
    if wd == 6:  # Sunday – open only at/after reopen hour
        return hour_start_utc.hour >= FX_WEEK_REOPEN_HOUR_UTC
    if wd == 4 and hour_start_utc.hour >= FX_WEEK_CLOSE_HOUR_UTC:  # Friday close
        return False
    return True


def _latest_expected_tradable_hour(now_utc: Optional[datetime] = None, pair: str = "") -> datetime:
    """Return the most recent completed tradable H1 close before *now*."""
    now = _hour_bucket(now_utc or _now_utc())
    candidate = now - timedelta(hours=1)
    for _ in range(24 * 10):
        if _hour_is_tradable(candidate, pair):
            return candidate
        candidate -= timedelta(hours=1)
    return candidate  # fallback – should never be reached


# ---------------------------------------------------------------------------
# Stooq live snapshots  —  yfinance-style via macro.StooqTicker
# ---------------------------------------------------------------------------
# Import the shared Stooq engine from macro.py so every module uses a single
# session, handshake, retry adapter, and CSV parser.  The fallback shim
# re-implements the bare minimum in case macro is not importable (e.g. tests).
# ---------------------------------------------------------------------------

try:
    # Canonical shared Stooq engine. Keep momentum.py aligned with macro.py,
    # scraper.py and signal_confirm.py after the latest macro updates.
    from macro import StooqTicker as _StooqTicker                  # shared engine
    _STOOQ_ENGINE = "macro"
except ImportError:
    try:
        from macro_rewritten_full import StooqTicker as _StooqTicker   # type: ignore[no-redef]
        _STOOQ_ENGINE = "macro_rewritten_full"
    except ImportError:
        try:
            from macro_rewritten import StooqTicker as _StooqTicker    # type: ignore[no-redef]
            _STOOQ_ENGINE = "macro_rewritten"
        except ImportError:                                            # pragma: no cover
            _StooqTicker = None                                        # type: ignore[assignment]
            _STOOQ_ENGINE = "shim"

log.debug("momentum: Stooq engine=%s", _STOOQ_ENGINE)


def _fetch_stooq_price(stooq_symbol: str) -> Tuple[Optional[float], str]:
    """
    Return (price, source_label) for *stooq_symbol*.

    yfinance-style lookup order:
      1. StooqTicker.fast_info["last_price"]  (real-time /q/l/ endpoint)
      2. StooqTicker.history(period="5d")[-1] (daily history fallback)

    Falls back to a direct HTTP shim when macro.StooqTicker is unavailable.
    """
    if _StooqTicker is not None:
        try:
            # Strip any legacy =x suffix — Stooq uses plain tickers, not Yahoo Finance format.
            clean_symbol = stooq_symbol.strip().lower().replace("=x", "")
            t = _StooqTicker(clean_symbol)
            price = t.fast_info.get("last_price")
            if price is not None:
                return float(price), "stooq_latest"
            # fast_info returned N/D — try daily history tail
            bars = t.history(period="5d")
            if bars:
                return float(bars[-1]["Close"]), "stooq_hist_fallback"
            return None, SOURCE_UNAVAILABLE
        except Exception as exc:
            log.debug("momentum: StooqTicker fetch failed symbol=%s err=%s", stooq_symbol, exc)
            return None, SOURCE_UNAVAILABLE

    # ── Shim: macro not importable ─────────────────────────────────────────
    # Strip any legacy =x suffix that stooq does not recognise.
    base_symbol = stooq_symbol.strip().lower().replace("=x", "")
    # Build candidate list: primary symbol first, then pair-specific fallbacks.
    candidates = [base_symbol] + [
        s for s in _STOOQ_FALLBACKS.get(base_symbol, []) if s != base_symbol
    ]
    for symbol in candidates:
        url = f"https://stooq.com/q/l/?s={symbol}&f=sd2t2ohlcv&h&e=csv"
        try:
            s = _build_session()
            resp = s.get(url, timeout=10)
            resp.raise_for_status()
            rows = list(csv.DictReader(StringIO(resp.text)))
            if not rows:
                log.debug("momentum: shim stooq no rows symbol=%s", symbol)
                continue
            row = {k.lower(): v for k, v in rows[-1].items()}
            close_str = (row.get("close") or "").strip().lower()
            if close_str and close_str != "n/d":
                return float(close_str), "stooq_latest"
            log.debug("momentum: shim stooq N/D symbol=%s trying next", symbol)
        except Exception as exc:
            log.debug("momentum: shim stooq fetch symbol=%s err=%s", symbol, exc)
    return None, SOURCE_UNAVAILABLE


# ---------------------------------------------------------------------------
# Snapshot log and local H1 rebuild
# ---------------------------------------------------------------------------

def _append_snapshot(pair: str, ts: datetime, price: float) -> None:
    payload = {"pair": pair, "ts": _iso_z(ts), "price": float(price)}
    try:
        with open(SNAPSHOT_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(payload) + "\n")
    except OSError as exc:
        log.error("momentum: _append_snapshot failed (disk full?) pair=%s err=%s", pair, exc)


def _load_snapshots(days: int = SNAPSHOT_RETENTION_DAYS) -> List[Dict[str, Any]]:
    cutoff = _now_utc() - timedelta(days=days)
    out: List[Dict[str, Any]] = []
    kept_lines: List[str] = []
    if not SNAPSHOT_FILE.exists():
        return out
    with open(SNAPSHOT_FILE, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except Exception:
                continue
            pair = str(row.get("pair", "")).lower().strip()
            dt = _parse_dt(row.get("ts"))
            try:
                price = float(row.get("price"))
            except (TypeError, ValueError):
                continue
            if pair not in STOOQ_SYMBOLS or dt is None or dt < cutoff:
                continue
            out.append({"pair": pair, "ts": dt.astimezone(UTC), "price": price})
            kept_lines.append(json.dumps({"pair": pair, "ts": _iso_z(dt), "price": price}))
    out.sort(key=lambda x: (x["pair"], x["ts"]))
    try:
        tmp = SNAPSHOT_FILE.with_suffix(SNAPSHOT_FILE.suffix + ".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            for ln in kept_lines:
                f.write(ln + "\n")
        os.replace(tmp, SNAPSHOT_FILE)
    except OSError as exc:
        log.warning("momentum: snapshot prune failed (disk full?): %s", exc)
    return out


def _rebuild_h1_from_snapshots(snapshots: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    completed_cutoff = _hour_bucket(_now_utc())
    grouped: Dict[str, Dict[str, Dict[str, Any]]] = {p: {} for p in STOOQ_SYMBOLS}
    for snap in snapshots:
        pair = snap["pair"]
        if pair not in grouped:
            continue
        bucket = _hour_bucket(snap["ts"])
        if bucket >= completed_cutoff:
            continue
        key = _iso_z(bucket)
        prev = grouped[pair].get(key)
        if prev is None or snap["ts"] >= _parse_dt(prev["ts"]):
            grouped[pair][key] = {
                "hour": key,
                "close": float(snap["price"]),
                "ts": _iso_z(snap["ts"]),
            }
    out: Dict[str, List[Dict[str, Any]]] = {}
    for pair in STOOQ_SYMBOLS:
        rows = [{"hour": v["hour"], "close": v["close"]}
                for _, v in sorted(grouped[pair].items())]
        out[pair] = rows[-MAX_H1_HISTORY:]
    return out


def _load_h1_closes() -> Dict[str, List[Dict[str, Any]]]:
    data = _load_json(H1_CLOSES_FILE, {})
    out: Dict[str, List[Dict[str, Any]]] = {p: [] for p in STOOQ_SYMBOLS}
    for pair, rows in (data or {}).items():
        if pair not in out or not isinstance(rows, list):
            continue
        clean: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            if not isinstance(row, dict):
                continue
            dt = _parse_dt(row.get("hour"))
            try:
                close = float(row.get("close"))
            except (TypeError, ValueError):
                continue
            if dt is None:
                continue
            key = _iso_z(_hour_bucket(dt))
            clean[key] = {"hour": key, "close": close}
        out[pair] = [clean[k] for k in sorted(clean.keys())][-MAX_H1_HISTORY:]
    return out


def _save_h1_closes(data: Dict[str, List[Dict[str, Any]]]) -> None:
    payload = {p: rows[-MAX_H1_HISTORY:] for p, rows in data.items() if p in STOOQ_SYMBOLS}
    _save_json(H1_CLOSES_FILE, payload)


def _merge_histories(
    base: List[Dict[str, Any]],
    overlay: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    merged: Dict[str, Dict[str, Any]] = {}
    for row in base:
        merged[row["hour"]] = {"hour": row["hour"], "close": float(row["close"])}
    for row in overlay:
        merged[row["hour"]] = {"hour": row["hour"], "close": float(row["close"])}
    return [merged[k] for k in sorted(merged.keys())][-MAX_H1_HISTORY:]


def _rows_to_closes(rows: List[Dict[str, Any]]) -> List[float]:
    return [float(r["close"]) for r in rows]


# ---------------------------------------------------------------------------
# Gap / seed detection
# ---------------------------------------------------------------------------

def _needs_seed_or_repair(rows: List[Dict[str, Any]], pair: str = "") -> Tuple[bool, str]:
    if not rows:
        return True, "missing_history"
    if len(rows) < SEED_CLOSES_TARGET:
        return True, f"history_too_short:{len(rows)}"
    last_hour = _parse_dt(rows[-1]["hour"])
    if last_hour is None:
        return True, "bad_last_hour"
    expected_last = _latest_expected_tradable_hour(_now_utc(), pair)
    gap_hours = int((expected_last - _hour_bucket(last_hour)).total_seconds() // 3600)
    if gap_hours < 0:
        return True, f"future_last_hour:{gap_hours}"
    if gap_hours >= REPAIR_GAP_HOURS:
        return True, f"gap_{gap_hours}h"
    return False, "ok"


def _repair_priority(pair: str, rows: List[Dict[str, Any]]) -> Tuple[int, int, int, str]:
    need, reason = _needs_seed_or_repair(rows, pair)
    if not need:
        return (99, 0, len(rows), reason)
    if reason == "missing_history":
        return (0, 9999, 0, reason)
    if reason.startswith("history_too_short:"):
        try:
            bars = int(reason.split(":", 1)[1])
        except Exception:
            bars = len(rows)
        return (1, max(0, SEED_CLOSES_TARGET - bars), bars, reason)
    if reason.startswith("gap_") and reason.endswith("h"):
        try:
            gap = int(reason[4:-1])
        except Exception:
            gap = 0
        return (2, gap, len(rows), reason)
    return (3, 0, len(rows), reason)


# ---------------------------------------------------------------------------
# Twelve Data quota tracking and session-window controls
# ---------------------------------------------------------------------------

def _load_td_usage() -> Dict[str, Any]:
    data = _load_json(TD_USAGE_FILE, {})
    today = _now_utc().date().isoformat()
    if data.get("date") != today:
        data = {
            "date": today,
            "count": 0,
            "last_request_ts": None,
            "cooldown_until": None,
            "last_error": None,
            "used_windows": [],
            "attempted_windows": [],
            "bootstrap_anytime_used": 0,
        }
    else:
        data.setdefault("count", 0)
        data.setdefault("last_request_ts", None)
        data.setdefault("cooldown_until", None)
        data.setdefault("last_error", None)
        data.setdefault("used_windows", [])
        data.setdefault("attempted_windows", [])
        data.setdefault("bootstrap_anytime_used", 0)
    return data


def _save_td_usage(data: Dict[str, Any]) -> None:
    _save_json(TD_USAGE_FILE, data)


def _set_td_cooldown(minutes: int, reason: str = "") -> None:
    usage = _load_td_usage()
    usage["cooldown_until"] = _iso_z(_now_utc() + timedelta(minutes=minutes))
    if reason:
        usage["last_error"] = reason
    _save_td_usage(usage)


def _throttle_td_if_needed() -> None:
    usage = _load_td_usage()
    last_ts = _parse_dt(usage.get("last_request_ts"))
    if last_ts is None:
        return
    elapsed = (_now_utc() - last_ts).total_seconds()
    wait = TD_MIN_SECONDS_BETWEEN - elapsed
    if wait > 0:
        time.sleep(wait)


def _seed_window_candidates(now_utc: Optional[datetime] = None) -> Iterable[Tuple[str, datetime]]:
    now = now_utc or _now_utc()
    for name, (tz_name, hour, minute) in SESSION_STARTS.items():
        tz = ZoneInfo(tz_name)
        local_now = now.astimezone(tz)
        for delta_days in (-1, 0, 1):
            d = local_now.date() + timedelta(days=delta_days)
            start_local = datetime(d.year, d.month, d.day, hour, minute, tzinfo=tz)
            start_utc = start_local.astimezone(UTC)
            if _hour_is_tradable(start_utc):
                yield name, start_utc


def _current_seed_window_key(now_utc: Optional[datetime] = None) -> Optional[str]:
    now = now_utc or _now_utc()
    width = timedelta(minutes=TD_SEED_WINDOW_MINUTES)
    candidates: List[Tuple[datetime, str]] = []
    for name, start_utc in _seed_window_candidates(now):
        if start_utc <= now < start_utc + width:
            key = f"{name}:{start_utc.strftime('%Y-%m-%dT%H:%MZ')}"
            candidates.append((start_utc, key))
    if not candidates:
        return None
    candidates.sort(key=lambda x: x[0], reverse=True)
    return candidates[0][1]


def _can_use_td(window_key: Optional[str] = None, allow_bootstrap_override: bool = False) -> bool:
    if not TWELVEDATA_API_KEY:
        return False
    usage = _load_td_usage()
    cooldown_until = _parse_dt(usage.get("cooldown_until"))
    if cooldown_until is not None and _now_utc() < cooldown_until:
        return False
    if int(usage.get("count", 0)) >= TD_REQUEST_LIMIT_PER_DAY:
        return False

    if allow_bootstrap_override and ALLOW_BOOTSTRAP_ANYTIME:
        # Re-derive window_key to close the TOCTOU race: if a window just
        # opened, let it handle the seed rather than burning a bootstrap slot.
        live_window = _current_seed_window_key()
        if live_window is not None:
            return False
        if int(usage.get("bootstrap_anytime_used", 0)) >= BOOTSTRAP_ANYTIME_PER_DAY:
            return False
        # Do not bootstrap if a window seed already succeeded today.
        if usage.get("used_windows"):
            return False
        return True

    window_key = window_key or _current_seed_window_key()
    if window_key is None:
        return False
    if window_key in set(usage.get("used_windows", [])):
        return False
    if STRICT_ONE_ATTEMPT_PER_WINDOW and window_key in set(usage.get("attempted_windows", [])):
        return False
    return True


def _mark_td_attempt(window_key: Optional[str]) -> None:
    usage = _load_td_usage()
    if window_key:
        attempted = list(usage.get("attempted_windows", []))
        if window_key not in attempted:
            attempted.append(window_key)
        usage["attempted_windows"] = attempted[-16:]
    usage["last_request_ts"] = _iso_z(_now_utc())
    _save_td_usage(usage)


def _mark_td_success(window_key: Optional[str], bootstrap_override: bool = False) -> None:
    usage = _load_td_usage()
    usage["count"] = int(usage.get("count", 0)) + 1
    if window_key:
        used = list(usage.get("used_windows", []))
        if window_key not in used:
            used.append(window_key)
        usage["used_windows"] = used[-16:]
    if bootstrap_override:
        usage["bootstrap_anytime_used"] = int(usage.get("bootstrap_anytime_used", 0)) + 1
    usage["last_request_ts"] = _iso_z(_now_utc())
    usage["last_error"] = None
    _save_td_usage(usage)


def _get_td_requests_today() -> int:
    return int(_load_td_usage().get("count", 0))


def _pick(row: Dict[str, Any], *keys: str) -> Any:
    """Return the first present value from a provider row."""
    for key in keys:
        if key in row:
            return row[key]
    return None


def _normalise_provider_date(value: Any) -> str:
    """Normalise compact dates such as 20260501 for the existing parser."""
    raw = str(value or "").strip()
    if len(raw) == 8 and raw.isdigit():
        return f"{raw[:4]}-{raw[4:6]}-{raw[6:]}"
    return raw


def _extract_provider_rows(payload: Any) -> List[Dict[str, Any]]:
    """Extract rows from common Twelve Data JSON shapes."""
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]
    if isinstance(payload, dict):
        for key in ("values", "Values", "Quotes", "quotes", "Data", "data", "Results", "results", "Items", "items"):
            rows = payload.get(key)
            if isinstance(rows, list):
                return [row for row in rows if isinstance(row, dict)]
        if any(k in payload for k in ("Close", "close", "Date", "date", "datetime", "Datetime")):
            return [payload]
    return []


def _row_datetime(row: Dict[str, Any]) -> Optional[datetime]:
    """Read datetime from Twelve Data-style fields."""
    date_part = _pick(row, "datetime", "Datetime", "DateTime", "Timestamp", "timestamp")
    if date_part is None:
        date_part = _pick(row, "Date", "date")
        time_part = _pick(row, "Time", "time")
        if date_part is not None and time_part not in (None, ""):
            date_part = f"{date_part} {time_part}"
    return _parse_dt(_normalise_provider_date(date_part))


def _provider_rows_to_h1_closes(rows: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], bool]:
    """Convert provider rows to completed H1 closes.

    The boolean indicates whether the response looks genuinely hourly.
    """
    completed_cutoff = _hour_bucket(_now_utc())
    dedup: Dict[str, Dict[str, Any]] = {}
    bars_per_day: Dict[str, int] = {}

    for row in rows:
        dt = _row_datetime(row)
        if dt is None:
            continue
        bucket = _hour_bucket(dt)
        if bucket >= completed_cutoff:
            continue
        raw_close = _pick(row, "close", "Close", "AdjClose", "adj_close", "Adj Close")
        try:
            close = float(raw_close)
        except (TypeError, ValueError):
            continue
        key = _iso_z(bucket)
        dedup[key] = {"hour": key, "close": close}
        day = bucket.date().isoformat()
        bars_per_day[day] = bars_per_day.get(day, 0) + 1

    clean = [dedup[k] for k in sorted(dedup.keys())][-MAX_H1_HISTORY:]
    looks_hourly = bool(clean) and (len(bars_per_day) <= 1 or max(bars_per_day.values(), default=0) > 1)
    return clean, looks_hourly


def _fetch_twelvedata_hourly_closes_direct(pair: str, lookback_hours: int) -> Tuple[List[Dict[str, Any]], str]:
    """Primary provider: Twelve Data H1 closes. Does not update quota itself."""
    if not TWELVEDATA_API_KEY:
        return [], "no_twelvedata_api_key"

    symbol = TD_SYMBOLS.get(pair)
    if not symbol:
        return [], "unsupported_pair"

    try:
        session = _build_session()
        resp = session.get(
            f"{TWELVEDATA_BASE}/time_series",
            params={
                "symbol": symbol,
                "interval": "1h",
                "outputsize": max(lookback_hours, _min_required_bars() + 10),
                "apikey": TWELVEDATA_API_KEY,
                "format": "JSON",
                "timezone": "UTC",
            },
            timeout=DEFAULT_TIMEOUT,
        )

        if resp.status_code == 429:
            _set_td_cooldown(TD_429_COOLDOWN_MINUTES, "429_twelvedata")
            return [], "twelvedata_rate_limited"

        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, dict) and data.get("status") == "error":
            code = str(data.get("code", "error"))
            msg = str(data.get("message", "unknown_error"))
            if code == "429":
                _set_td_cooldown(TD_429_COOLDOWN_MINUTES, msg)
                return [], "twelvedata_rate_limited"
            return [], f"twelvedata_api_error:{code}:{msg}"

        rows = _extract_provider_rows(data)
        clean, looks_hourly = _provider_rows_to_h1_closes(rows)
        if not clean:
            return [], "twelvedata_no_bars"
        if not looks_hourly:
            return [], "twelvedata_not_hourly"
        return clean[-MAX_H1_HISTORY:], "twelvedata_ok"

    except requests.RequestException as exc:
        return [], f"twelvedata_http_error:{exc}"
    except ValueError as exc:
        return [], f"twelvedata_decode_error:{exc}"
    except Exception as exc:
        return [], f"twelvedata_error:{exc}"




def _fetch_td_hourly_closes(
    pair: str,
    lookback_hours: int = 120,
    bootstrap_override: bool = False,
) -> Tuple[List[Dict[str, Any]], str]:
    """Fetch H1 closes using Twelve Data as the sole provider."""
    if not TWELVEDATA_API_KEY:
        return [], "no_api_key"

    window_key = _current_seed_window_key()
    if not _can_use_td(window_key, allow_bootstrap_override=bootstrap_override):
        return [], "budget_window_or_cooldown"

    if pair not in TD_SYMBOLS:
        return [], "unsupported_pair"

    _mark_td_attempt(window_key)
    _throttle_td_if_needed()

    fetched, status = _fetch_twelvedata_hourly_closes_direct(pair, lookback_hours)
    if fetched:
        _mark_td_success(window_key, bootstrap_override=bootstrap_override)
        suffix = "_bootstrap" if bootstrap_override and window_key is None else ""
        return fetched, f"{status}{suffix}"

    return [], f"failed:{status}"



def _repair_pair_history(
    pair: str,
    local_rows: List[Dict[str, Any]],
    td_requests_used_this_run: int = 0,
    bootstrap_used_this_run: int = 0,
) -> Tuple[List[Dict[str, Any]], str, int, int]:
    need, reason = _needs_seed_or_repair(local_rows, pair)
    if not need:
        return local_rows, "local_ok", td_requests_used_this_run, bootstrap_used_this_run

    _outside_window = _current_seed_window_key() is None
    _bootstrap_eligible = reason in ("missing_history",) or reason.startswith("history_too_short:")
    bootstrap_override = bool(ALLOW_BOOTSTRAP_ANYTIME and _bootstrap_eligible and _outside_window)

    if bootstrap_override:
        if bootstrap_used_this_run >= BOOTSTRAP_PER_RUN_MAX:
            return local_rows, f"repair_skipped:bootstrap_run_limit:{reason}", td_requests_used_this_run, bootstrap_used_this_run
    else:
        if td_requests_used_this_run >= TD_REQUESTS_PER_RUN_MAX:
            return local_rows, f"repair_skipped:run_limit:{reason}", td_requests_used_this_run, bootstrap_used_this_run

    fetched, status = _fetch_td_hourly_closes(
        pair,
        lookback_hours=max(SEED_CLOSES_TARGET * 2, _min_required_bars() + 10),
        bootstrap_override=bootstrap_override,
    )
    if fetched:
        merged = _merge_histories(fetched, local_rows)
        if bootstrap_override:
            return merged, f"td_seeded:{status}:{reason}", td_requests_used_this_run, bootstrap_used_this_run + 1
        return merged, f"td_seeded:{status}:{reason}", td_requests_used_this_run + 1, bootstrap_used_this_run
    if bootstrap_override:
        return local_rows, f"repair_skipped:{status}:{reason}", td_requests_used_this_run, bootstrap_used_this_run
    return local_rows, f"repair_skipped:{status}:{reason}", td_requests_used_this_run, bootstrap_used_this_run


# ---------------------------------------------------------------------------
# Scoring math
# ---------------------------------------------------------------------------

def ema(series: List[float], period: int) -> List[float]:
    if not series or period <= 0:
        return []
    k = 2.0 / (period + 1)
    out = [series[0]]
    for price in series[1:]:
        out.append(price * k + out[-1] * (1 - k))
    return out


def rsi_wilder(series: List[float], period: int = MOMENTUM_RSI_PERIOD) -> Optional[float]:
    """Return latest RSI using Wilder smoothing from completed H1 closes."""
    if not series or period <= 0 or len(series) < period + 1:
        return None
    values = [float(x) for x in series if x is not None]
    if len(values) < period + 1:
        return None
    gains: List[float] = []
    losses: List[float] = []
    for i in range(1, period + 1):
        change = values[i] - values[i - 1]
        gains.append(max(change, 0.0))
        losses.append(max(-change, 0.0))
    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period
    for i in range(period + 1, len(values)):
        change = values[i] - values[i - 1]
        gain = max(change, 0.0)
        loss = max(-change, 0.0)
        avg_gain = ((avg_gain * (period - 1)) + gain) / period
        avg_loss = ((avg_loss * (period - 1)) + loss) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def rsi_label(value: Optional[float]) -> str:
    if value is None:
        return "unavailable"
    if value >= MOMENTUM_RSI_OVERBOUGHT:
        return "overbought"
    if value <= MOMENTUM_RSI_OVERSOLD:
        return "oversold"
    if value >= 55.0:
        return "bullish"
    if value <= 45.0:
        return "bearish"
    return "neutral"


def rsi_direction(value: Optional[float]) -> str:
    label = rsi_label(value)
    if label in ("bullish", "overbought"):
        return "bullish"
    if label in ("bearish", "oversold"):
        return "bearish"
    return "neutral"


def rsi_payload(closes: List[float], period: int = MOMENTUM_RSI_PERIOD) -> Dict[str, Any]:
    value = rsi_wilder(closes, period)
    label = rsi_label(value)
    rounded = round(value, 2) if value is not None else None
    return {
        "h1_rsi_period": period,
        "h1_rsi": rounded,
        "h1_rsi14": rounded if period == 14 else None,
        "rsi14": rounded if period == 14 else None,
        "h1_rsi_label": label,
        "h1_rsi_signal": rsi_direction(value),
        "h1_rsi_status": "ok" if value is not None else "warming_up",
        "h1_rsi_bars_needed": period + 1,
    }



def slope_sign(series: List[float], pair: str = "") -> float:
    e = ema(series, EMA_FAST)
    if len(e) < 2 or e[-2] == 0:
        return 0.0
    delta_frac = (e[-1] - e[-2]) / abs(e[-2])
    thresh = SLOPE_THRESH.get(pair.lower(), 0.00005)
    if delta_frac > thresh:
        return 1.0
    if delta_frac < -thresh:
        return -1.0
    return 0.0


def persistence(closes: List[float]) -> float:
    if len(closes) < PERSIST_WINDOW + 1 or PERSIST_WINDOW <= 0:
        return 0.5
    net = closes[-1] - closes[-(PERSIST_WINDOW + 1)]
    if net == 0:
        return 0.5
    direction = 1 if net > 0 else -1
    start = len(closes) - PERSIST_WINDOW
    aligned = sum(1 for i in range(start, len(closes)) if (closes[i] - closes[i - 1]) * direction > 0)
    return aligned / PERSIST_WINDOW


def trend_impulse_score(closes: List[float], pair: str = "") -> float:
    if len(closes) < _min_required_bars():
        return 0.0
    pair = pair.lower().strip()
    norm = PAIR_NORM.get(pair, 0.5)
    base = closes[-(LOOKBACK_BARS + 1)]
    if base == 0 or norm <= 0:
        return 0.0
    net_change = closes[-1] - base
    change_norm = (net_change / base) / norm
    slope = slope_sign(closes, pair)
    if slope == 0.0:
        return 0.0
    score = change_norm * slope * persistence(closes)
    score = max(-MOMENTUM_CAP, min(MOMENTUM_CAP, score))
    return round(score, 3)


# ---------------------------------------------------------------------------
# EMA cross / price state helpers
# ---------------------------------------------------------------------------

_POSITION_EPS = 1e-12   # matches ema.py relation() — handles exact float equality cleanly


def _position_vs_ref(price: float, ref: float, eps: float = _POSITION_EPS) -> str:
    if price > ref + eps:
        return "above"
    if price < ref - eps:
        return "below"
    return "at"


_CROSS_REL_EPS = 1e-6   # 0.0001 % of price — filters floating-point noise without masking real crosses


def _cross_state(prev_fast: float, prev_slow: float, fast_now: float, slow_now: float) -> str:
    # Relative epsilon scaled to slow EMA magnitude so that sub-pip noise on
    # high-value pairs (USDJPY ~150, XAUUSD ~2000) does not produce phantom
    # cross signals. Mirrors the fix applied to ema.py detect_cross().
    eps = max(abs(slow_now), abs(prev_slow)) * _CROSS_REL_EPS
    prev_diff = prev_fast - prev_slow
    curr_diff = fast_now - slow_now
    if prev_diff <= eps and curr_diff > eps:
        return "bullish_cross"
    if prev_diff >= -eps and curr_diff < -eps:
        return "bearish_cross"
    return "none"


def _structure_label(vs_fast: str, vs_slow: str, fast_key: str = "ema_fast", slow_key: str = "ema_slow") -> str:
    """Return a composite label describing position relative to both EMAs.
    Uses the caller-supplied key names so the label reflects the actual periods
    (e.g. 'above_ema20_ema50' when fast=20, slow=50).
    """
    if vs_fast == "above" and vs_slow == "above":
        return f"above_{fast_key}_{slow_key}"
    if vs_fast == "below" and vs_slow == "below":
        return f"below_{fast_key}_{slow_key}"
    if vs_fast == "below" and vs_slow == "above":
        return f"below_{fast_key}_above_{slow_key}"
    if vs_fast == "above" and vs_slow == "below":
        return f"above_{fast_key}_below_{slow_key}"
    return "at_ema"


def _trend_bias_label(fast_vs_slow: str) -> str:
    """Map EMA alignment to a trend bias string."""
    if fast_vs_slow == "above":
        return "bullish_bias"
    if fast_vs_slow == "below":
        return "bearish_bias"
    return "neutral_bias"


def _ema_suggestion(
    trend_bias: str,
    cross: str,
    current_vs_fast: Optional[str],
    current_vs_slow: Optional[str],  # noqa: ARG001 — reserved for future slow-EMA logic
) -> str:
    """Derive a plain-language trading suggestion from EMA state."""
    if cross == "bullish_cross":
        return "watch_long"
    if cross == "bearish_cross":
        return "watch_short"
    if trend_bias == "bullish_bias":
        if current_vs_fast == "above":
            return "bullish_aligned"
        if current_vs_fast == "below":
            return "bullish_pullback"
        # current_vs_fast == "at": price is testing the fast EMA in a bullish stack
        return "bullish_at_ema"
    if trend_bias == "bearish_bias":
        if current_vs_fast == "below":
            return "bearish_aligned"
        if current_vs_fast == "above":
            return "bearish_pullback"
        # current_vs_fast == "at": price is testing the fast EMA in a bearish stack
        return "bearish_at_ema"
    return "neutral"


# ---------------------------------------------------------------------------
# History update workflow
# ---------------------------------------------------------------------------

def _update_histories(
    pairs: List[str],
) -> Tuple[Dict[str, List[Dict[str, Any]]], Dict[str, Optional[float]], Dict[str, str], Dict[str, str]]:
    local_h1 = _load_h1_closes()
    current_prices: Dict[str, Optional[float]] = {p: None for p in pairs}
    current_sources: Dict[str, str] = {p: SOURCE_UNAVAILABLE for p in pairs}
    repair_notes: Dict[str, str] = {p: "" for p in pairs}

    now = _now_utc()
    for pair in pairs:
        stooq_symbol = STOOQ_SYMBOLS.get(pair, "")
        price, src = _fetch_stooq_price(stooq_symbol)
        current_prices[pair] = price
        current_sources[pair] = src
        if price is not None:
            _append_snapshot(pair, now, price)

    snapshots = _load_snapshots()
    rebuilt = _rebuild_h1_from_snapshots(snapshots)

    # Merge rebuilt snapshot data for ALL known pairs so unrequested pairs
    # don't have their fresher rebuilt rows discarded on save.
    merged_map: Dict[str, List[Dict[str, Any]]] = {}
    for pair in STOOQ_SYMBOLS:
        merged_map[pair] = _merge_histories(local_h1.get(pair, []), rebuilt.get(pair, []))

    # Repair only the requested pairs (quota-limited TD calls).
    ranked = sorted(pairs, key=lambda p: _repair_priority(p, merged_map.get(p, [])))
    td_used_this_run = 0
    bootstrap_used_this_run = 0
    for pair in ranked:
        repaired_rows, note, td_used_this_run, bootstrap_used_this_run = _repair_pair_history(
            pair, merged_map[pair], td_used_this_run, bootstrap_used_this_run
        )
        merged_map[pair] = repaired_rows
        repair_notes[pair] = note

    # Write the fully merged map (all pairs) back to disk.
    local_h1.update(merged_map)

    _save_h1_closes(local_h1)

    if not SNAPSHOT_FILE.exists():
        SNAPSHOT_FILE.touch()

    return local_h1, current_prices, current_sources, repair_notes


# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# scraper.py local daily TD seed reader
# ---------------------------------------------------------------------------

def _load_scraper_components() -> Dict[str, Any]:
    """Read scraper.py's local macro_components.json without making any API call."""
    paths = [SCRAPER_COMPONENT_FILE]
    if SCRAPER_COMPONENT_FILE.name != "macro_components.json":
        paths.append(Path("public/macro_components.json"))
    paths.append(Path("macro_components.json"))
    seen = set()
    for path in paths:
        try:
            p = Path(path)
            key = str(p.resolve()) if p.exists() else str(p)
            if key in seen:
                continue
            seen.add(key)
            if not p.exists():
                continue
            with p.open(encoding="utf-8") as handle:
                data = json.load(handle)
            return data if isinstance(data, dict) else {}
        except Exception:
            continue
    return {}


def _scraper_daily_rows(pair: str) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """Return completed D1 rows and indicators seeded by scraper.py for one pair."""
    data = _load_scraper_components()
    pair_key = _normalize_pair_code(pair)
    history_map = (
        data.get("price_history", {})
        or data.get("price_daily_history", {})
        or data.get("_price_history", {})
        or {}
    )
    indicators_map = (
        data.get("price_indicators", {})
        or data.get("price_daily_indicators", {})
        or data.get("_price_indicators", {})
        or {}
    )
    raw_hist = history_map.get(pair_key) if isinstance(history_map, dict) else None
    raw_ind = indicators_map.get(pair_key) if isinstance(indicators_map, dict) else {}
    rows: List[Dict[str, Any]] = []
    if isinstance(raw_hist, list):
        for item in raw_hist:
            if not isinstance(item, dict):
                continue
            close_raw = item.get("close", item.get("Close"))
            try:
                close = float(close_raw)
            except (TypeError, ValueError):
                continue
            date_raw = item.get("_date") or item.get("date") or item.get("datetime") or item.get("Datetime")
            date_text = str(date_raw or "").strip()[:10]
            if date_text:
                hour = f"{date_text}T00:00:00Z"
            else:
                hour = _iso_z(_now_utc())
            rows.append({"hour": hour, "close": close})
    return rows[-MAX_H1_HISTORY:], (raw_ind if isinstance(raw_ind, dict) else {})


def _apply_scraper_daily_ema_seed(
    pair: str,
    ema_state: Dict[str, Any],
    current_price: Optional[float],
    current_source: str,
) -> Dict[str, Any]:
    """Prefer scraper.py's TD-seeded D1 EMA20/50 when enabled or when H1 is warming.

    This avoids extra Twelve Data calls from momentum.py.  If scraper.py has not
    produced enough daily bars yet, the original H1 state is returned unchanged.
    """
    rows, indicators = _scraper_daily_rows(pair)
    if len(rows) < EMA_SIGNAL_SLOW:
        if indicators:
            ema_state.setdefault("scraper_daily_indicators", indicators)
        return ema_state

    should_prefer = PREFER_SCRAPER_DAILY_EMA or bool(ema_state.get("warming_up")) or not bool(ema_state.get("ok"))
    if not should_prefer:
        ema_state.setdefault("scraper_daily_indicators", indicators)
        ema_state.setdefault("scraper_daily_bars", len(rows))
        return ema_state

    seeded = _compute_ema_state(
        pair=pair,
        rows=rows,
        current_price=current_price,
        current_source=current_source,
        repair_note="scraper_td_daily_seeded",
        bars=len(rows),
    )
    seeded["timeframe"] = "D1"
    seeded["history_source"] = SOURCE_SCRAPER_TD_DAILY
    seeded["source"] = SOURCE_SCRAPER_TD_DAILY
    seeded["repair_note"] = "scraper_td_daily_seeded"
    seeded["scraper_daily_bars"] = len(rows)
    seeded["scraper_daily_seeded"] = True
    if indicators:
        seeded["scraper_daily_indicators"] = indicators
        if indicators.get("rsi14") is not None:
            seeded["rsi14"] = indicators.get("rsi14")
            seeded["rsi"] = indicators.get("rsi14")
        if indicators.get("last_bar_date"):
            seeded["last_bar_date"] = indicators.get("last_bar_date")
    return seeded

# EMA state computation (shared by fetch_price_momentum and detect_h1_ema_*)
# ---------------------------------------------------------------------------

def _compute_ema_state(
    pair: str,
    rows: List[Dict[str, Any]],
    current_price: Optional[float],
    current_source: str,
    repair_note: str,
    bars: Optional[int] = None,
) -> Dict[str, Any]:
    """Derive the fast/slow EMA structural state for one pair from pre-built rows.

    All JSON field names are derived from the configured EMA_SIGNAL_FAST and
    EMA_SIGNAL_SLOW values.  EMA_SIGNAL_FAST is now 20 by default; the
    output automatically uses 'ema20', 'close_vs_ema20', 'above_ema20_ema50',
    etc. — no code changes required.

    Always returns a dict (never None).  Three possible states, mirroring ema.py:

      ok=False, warming_up=True, suggestion="warming_up"
        • no_data   — fewer than 2 bars; EMA values are None.
        • warming   — bars < EMA_SIGNAL_SLOW; EMA values present but all
                      comparative fields (above/below, structure, bias) are None
                      because the slow EMA has not yet converged.

      ok=True,  warming_up=False
        • ready     — bars >= EMA_SIGNAL_SLOW; all fields populated.
    """
    fp = EMA_SIGNAL_FAST   # e.g. 20
    sp = EMA_SIGNAL_SLOW   # e.g. 50
    fast_key = f"ema{fp}"  # e.g. "ema20"
    slow_key = f"ema{sp}"  # e.g. "ema50"

    closes        = _rows_to_closes(rows)
    bars_available = len(closes)
    bars_needed    = sp          # slow EMA must have >= sp bars to be reliable
    bars_target    = int(bars or max(sp + 3, _min_required_bars()))

    _history_src = (
        SOURCE_TD_SEEDED if repair_note.startswith("td_seeded")
        else (SOURCE_HOURLY if rows else SOURCE_UNAVAILABLE)
    )

    # ------------------------------------------------------------------
    # Shared skeleton for warming-up states (no comparisons possible).
    # ------------------------------------------------------------------
    def _warming_skeleton(
        fast_val: Optional[float],
        slow_val: Optional[float],
        last_close: Optional[float],
        price_val: Optional[float],
        reason: str,
    ) -> Dict[str, Any]:
        result = {
            "pair":        pair,
            "timeframe":   "H1",
            "fast_period": fp,
            "slow_period": sp,
            # EMA values — None when not yet computable, otherwise the raw value
            fast_key:      round(fast_val, 6) if fast_val is not None else None,
            slow_key:      round(slow_val, 6) if slow_val is not None else None,
            "fast_ema":    round(fast_val, 6) if fast_val is not None else None,
            "slow_ema":    round(slow_val, 6) if slow_val is not None else None,
            # Cross — cannot be trusted while warming up
            "cross":            "none",
            "ema_cross_signal": "none",
            # Close vs EMA — suppressed (mirrors ema.py warming-up behaviour)
            "last_close":               round(last_close, 6) if last_close is not None else None,
            f"close_vs_{fast_key}":     None,
            f"close_vs_{slow_key}":     None,
            "close_structure":          None,
            # EMA alignment — suppressed
            f"{fast_key}_vs_{slow_key}": None,
            # Current price vs EMA — suppressed
            "price":                    round(price_val, 6) if price_val is not None else None,
            f"current_vs_{fast_key}":   None,
            f"current_vs_{slow_key}":   None,
            "price_vs_fast":            None,
            "price_vs_slow":            None,
            "current_structure":        None,
            # Summary
            "trend_bias":      "neutral_bias",
            "suggestion":      "warming_up",
            # Warm-up diagnostics
            "warming_up":      True,
            "bars_available":  bars_available,
            "bars_needed":     bars_needed,
            "warm_up_reason":  reason,
            # Last completed H1 candle timestamp (ISO-Z) for alert deduplication
            "last_completed_hour": (
                rows[-1]["hour"] if rows
                else _iso_z(_latest_expected_tradable_hour(_now_utc(), pair))
            ),
            # Source / metadata
            "current_source":  current_source,
            "history_source":  _history_src,
            "repair_note":     repair_note,
            "ok":              False,
        }
        result.update(rsi_payload(closes, MOMENTUM_RSI_PERIOD))
        _LAST_EMA_DIAGNOSTICS[pair] = dict(result)
        return result

    # ------------------------------------------------------------------
    # Stage 1 — no data: can't compute anything at all.
    # ------------------------------------------------------------------
    if bars_available < 2:
        _last_close_val = float(closes[-1]) if bars_available == 1 else None
        _price_val      = float(current_price) if current_price is not None else _last_close_val
        return _warming_skeleton(
            fast_val=None, slow_val=None,
            last_close=_last_close_val, price_val=_price_val,
            reason=f"no_data:bars={bars_available}",
        )

    # Compute EMAs with however many bars we have (trimmed to target if enough).
    _closes_used = closes[-bars_target:] if bars_available >= bars_target else closes
    fast_series  = ema(_closes_used, fp)
    slow_series  = ema(_closes_used, sp)

    if len(fast_series) < 2 or len(slow_series) < 2:
        # Degenerate — should never happen given bars_available >= 2, but guard.
        return _warming_skeleton(
            fast_val=None, slow_val=None,
            last_close=float(closes[-1]), price_val=float(current_price or closes[-1]),
            reason=f"ema_series_too_short:fast={len(fast_series)},slow={len(slow_series)}",
        )

    _fast_val   = float(fast_series[-1])
    _slow_val   = float(slow_series[-1])
    _last_close = float(_closes_used[-1])
    _price_f    = float(current_price) if current_price is not None else _last_close

    # ------------------------------------------------------------------
    # Stage 2 — warming up: EMAs computed but slow EMA not yet reliable.
    # Mirrors ema.py: MIN_EMA50_CLOSES gate suppresses all comparisons.
    # ------------------------------------------------------------------
    if bars_available < bars_needed:
        return _warming_skeleton(
            fast_val=_fast_val, slow_val=_slow_val,
            last_close=_last_close, price_val=_price_f,
            reason=f"warming:bars={bars_available}<needed={bars_needed}",
        )

    # ------------------------------------------------------------------
    # Stage 3 — ready: full output with all comparative fields.
    # ------------------------------------------------------------------
    _cross        = _cross_state(float(fast_series[-2]), float(slow_series[-2]), _fast_val, _slow_val)
    _price_vs_fast = _position_vs_ref(_price_f,    _fast_val)
    _price_vs_slow = _position_vs_ref(_price_f,    _slow_val)
    _close_vs_fast = _position_vs_ref(_last_close, _fast_val)
    _close_vs_slow = _position_vs_ref(_last_close, _slow_val)
    _fast_vs_slow  = _position_vs_ref(_fast_val,   _slow_val)
    _trend_bias    = _trend_bias_label(_fast_vs_slow)
    _close_struct  = _structure_label(_close_vs_fast, _close_vs_slow, fast_key, slow_key)
    _cur_struct    = _structure_label(_price_vs_fast, _price_vs_slow, fast_key, slow_key)
    _suggestion    = _ema_suggestion(_trend_bias, _cross, _price_vs_fast, _price_vs_slow)

    result: Dict[str, Any] = {
        "pair":        pair,
        "timeframe":   "H1",
        # Configured periods
        "fast_period": fp,
        "slow_period": sp,
        # EMA values — dynamic keys + static aliases
        fast_key:      round(_fast_val, 6),
        slow_key:      round(_slow_val, 6),
        "fast_ema":    round(_fast_val, 6),
        "slow_ema":    round(_slow_val, 6),
        # Cross signal
        "cross":            _cross,
        "ema_cross_signal": _cross,
        # Last completed close vs each EMA — dynamic keys
        "last_close":                     round(_last_close, 6),
        f"close_vs_{fast_key}":           _close_vs_fast,
        f"close_vs_{slow_key}":           _close_vs_slow,
        "close_structure":                _close_struct,
        # EMA alignment — dynamic key
        f"{fast_key}_vs_{slow_key}":      _fast_vs_slow,
        # Current price vs each EMA — dynamic keys + static aliases
        "price":                          round(_price_f, 6),
        f"current_vs_{fast_key}":         _price_vs_fast,
        f"current_vs_{slow_key}":         _price_vs_slow,
        "price_vs_fast":                  _price_vs_fast,
        "price_vs_slow":                  _price_vs_slow,
        "current_structure":              _cur_struct,
        # Summary
        "trend_bias":      _trend_bias,
        "suggestion":      _suggestion,
        # Warm-up diagnostics — present in all states for consistent schema
        "warming_up":      False,
        "bars_available":  bars_available,
        "bars_needed":     bars_needed,
        "warm_up_reason":  None,
        # Last completed H1 candle timestamp (ISO-Z) for alert deduplication
        "last_completed_hour": (
            rows[-1]["hour"] if rows
            else _iso_z(_latest_expected_tradable_hour(_now_utc(), pair))
        ),
        # Source / metadata
        "current_source":  current_source,
        "history_source": (
            SOURCE_TD_SEEDED if repair_note.startswith("td_seeded")
            else (SOURCE_HOURLY if rows else SOURCE_UNAVAILABLE)
        ),
        "repair_note":     repair_note,
        "ok":              True,
    }
    result.update(rsi_payload(closes, MOMENTUM_RSI_PERIOD))
    _LAST_EMA_DIAGNOSTICS[pair] = dict(result)
    return result


# ---------------------------------------------------------------------------
# Telegram alerts — Hourly EMA 20/50
# Mirrors ema.py's Daily alert style exactly; only "Daily" → "Hourly".
# ---------------------------------------------------------------------------

# Flag / icon emojis (lowercase pair codes to match momentum.py convention).
_H1_PAIR_FLAGS: Dict[str, Tuple[str, str]] = {
    "eurusd": ("🇪🇺", "🇺🇸"),
    "gbpusd": ("🇬🇧", "🇺🇸"),
    "usdjpy": ("🇺🇸", "🇯🇵"),
    "xauusd": ("🥇", "🇺🇸"),
}

# Alert state file — persists de-duplication keys across runs.
# Defined here (after BASE_DIR) so it is ready when the section is reached.
H1_ALERT_STATE_FILE: Path = BASE_DIR / "momentum_telegram_alerts.json"
# Groq AI cooldown state — shared with signal_confirm.py/pivot.py by default.
# Priority:
# 1) GROQ_AI_STATE_FILE explicit override
# 2) same directory as SIGNAL_ALERT_STATE_FILE (signal_confirm.py default)
# 3) MOMENTUM_DATA_DIR / groq_ai_state.json
_GROQ_AI_STATE_FILE: Path = Path(
    os.environ.get(
        "GROQ_AI_STATE_FILE",
        str(Path(os.environ.get("SIGNAL_ALERT_STATE_FILE", str(BASE_DIR / "signal_telegram_alerts.json"))).parent / "groq_ai_state.json"),
    )
)


def _tg_send(bot_token: str, chat_id: int, text: str, timeout: int = 10) -> bool:
    """Send a Telegram message via the Bot API.  Returns True on success."""
    if not bot_token or not chat_id:
        return False
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    try:
        s = _build_session()
        resp = s.post(
            url,
            json={"chat_id": chat_id, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True},
            timeout=timeout,
        )
        if resp.status_code == 200:
            return True
        log.warning("momentum: Telegram HTTP %s — %s", resp.status_code, resp.text[:200])
        return False
    except requests.exceptions.RequestException as exc:
        log.warning("momentum: Telegram send failed: %s", exc)
        return False


def _h1_load_alert_state() -> Dict[str, Any]:
    data = _load_json(H1_ALERT_STATE_FILE, {})
    return data if isinstance(data, dict) else {}


def _h1_save_alert_state(state: Dict[str, Any]) -> None:
    _save_json(H1_ALERT_STATE_FILE, state)


def _h1_prune_alert_state(state: Dict[str, Any], max_age_hours: int = 48) -> Dict[str, Any]:
    cutoff = _now_utc() - timedelta(hours=max_age_hours)
    pruned: Dict[str, Any] = {}
    for k, v in state.items():
        ts = _parse_dt(v)
        if ts is None:
            continue
        if ts >= cutoff:
            pruned[k] = v
    return pruned


def _h1_format_price(pair: str, value: float) -> str:
    """Format a price with pair-appropriate decimal places."""
    p = pair.upper()
    if "JPY" in p:
        return f"{value:.3f}"
    if "XAU" in p:
        return f"{value:.2f}"
    return f"{value:.5f}"


def _h1_side(price: float, ema: float) -> str:
    return "above" if price >= ema else "below"


def _h1_pair_header(pair: str, bias: str = "") -> str:
    """Return a decorated pair string with flags and directional bias arrow."""
    left, right = _H1_PAIR_FLAGS.get(pair.lower(), ("", ""))
    display = f"{pair[:3].upper()}/{pair[3:].upper()}"  # e.g. "EUR/USD"
    base = f"{left} {display} {right}" if (left and right) else display
    b = bias.lower()
    if "bullish" in b:
        return f"{base} 🔼"
    if "bearish" in b:
        return f"{base} 🔽"
    return base


def _h1_fmt_bucket(bucket: str) -> str:
    """Format an H1 ISO-Z bucket string as a readable MYT timestamp.

    Input : '2025-04-28T08:00:00Z'
    Output: '28 Apr 2025  16:00 MYT'
    """
    try:
        dt = datetime.strptime(bucket, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)
        myt = ZoneInfo("Asia/Kuala_Lumpur")
        return dt.astimezone(myt).strftime("%d %b %Y  %H:%M MYT")
    except (ValueError, TypeError):
        return bucket or "—"


def _h1_human_bias(trend_bias: str) -> str:
    """Convert internal trend_bias strings to human-readable labels."""
    mapping = {
        "bullish_bias":  "Bullish",
        "bearish_bias":  "Bearish",
        "neutral_bias":  "Neutral",
        "neutral":       "Neutral",
        "bullish":       "Bullish",
        "bearish":       "Bearish",
    }
    return mapping.get(trend_bias.lower(), trend_bias.replace("_", " ").title())


def _h1_proximity_pct(price: float, ema: float) -> float:
    """Return |price − ema| / ema.  Returns inf when ema is zero."""
    if ema == 0:
        return float("inf")
    return abs(price - ema) / abs(ema)


# ---------------------------------------------------------------------------
# Groq AI helpers
# ---------------------------------------------------------------------------

def _groq_load_state() -> Dict[str, Any]:
    try:
        with open(_GROQ_AI_STATE_FILE, encoding="utf-8") as _f:
            return json.load(_f)
    except Exception:
        return {}


def _groq_save_state(data: Dict[str, Any]) -> None:
    try:
        _GROQ_AI_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        tmp = _GROQ_AI_STATE_FILE.with_suffix(".tmp")
        with open(tmp, "w", encoding="utf-8") as _f:
            json.dump(data, _f)
            _f.flush(); os.fsync(_f.fileno())
        os.replace(tmp, _GROQ_AI_STATE_FILE)
    except Exception as _exc:
        log.warning("momentum: could not save Groq AI state file: %s", _exc)


def _groq_can_call() -> bool:
    """Return True if a Groq AI call is allowed (per-run cap + cooldown check)."""
    if _groq_ai_calls_this_run >= GROQ_AI_MAX_PER_RUN:
        log.debug(
            "momentum: Groq AI per-run cap reached (%d/%d) — skipping",
            _groq_ai_calls_this_run, GROQ_AI_MAX_PER_RUN,
        )
        return False
    state = _groq_load_state()
    cooldown_until = state.get("cooldown_until")
    if cooldown_until:
        try:
            _cu = datetime.fromisoformat(cooldown_until.replace("Z", "+00:00"))
            if datetime.now(UTC) < _cu:
                log.warning("momentum: Groq AI cooldown active until %s — skipping", cooldown_until)
                return False
        except Exception:
            pass
    return True


def _groq_mark_429() -> None:
    """Set a cooldown after receiving a 429 from Groq AI."""
    until = (datetime.now(UTC) + timedelta(minutes=GROQ_AI_COOLDOWN_MINUTES)).strftime("%Y-%m-%dT%H:%M:%SZ")
    _groq_save_state({"cooldown_until": until})
    log.warning("momentum: Groq AI 429 — cooldown set for %d min (until %s)", GROQ_AI_COOLDOWN_MINUTES, until)


def _groq_mark_call() -> None:
    global _groq_ai_calls_this_run
    _groq_ai_calls_this_run += 1


# Dedicated session for Groq AI — does NOT retry on 429.
_groq_session_instance: Optional[requests.Session] = None

def _groq_session() -> requests.Session:
    global _groq_session_instance
    if _groq_session_instance is None:
        s = requests.Session()
        s.headers.update({"User-Agent": _USER_AGENT})
        s.mount(
            "https://",
            HTTPAdapter(
                max_retries=Retry(
                    total=1,
                    backoff_factor=0.5,
                    status_forcelist=[500, 502, 503, 504],
                    allowed_methods=frozenset({"POST"}),
                )
            ),
        )
        _groq_session_instance = s
    return _groq_session_instance


def _groq_h1_prompt(ema_state: Dict[str, Any]) -> str:
    pair     = ema_state.get("pair", "").upper()
    cross    = ema_state.get("ema_cross_signal", "")
    bias     = ema_state.get("trend_bias", "")
    price    = ema_state.get("price", "")
    ema_f    = ema_state.get("fast_ema", "")
    ema_s    = ema_state.get("slow_ema", "")
    f_period = ema_state.get("fast_period", EMA_SIGNAL_FAST)
    s_period = ema_state.get("slow_period", EMA_SIGNAL_SLOW)
    notes    = ema_state.get("notes", "")
    direction = "bullish (cross up)" if cross == "cross_up" else "bearish (cross down)"
    return "\n".join([
        "You are a concise FX momentum assistant for Telegram alerts.",
        "Return exactly 2 short bullets only. No trade command. No guarantee. No markdown table.",
        "Focus on what the H1 EMA crossover means for near-term momentum and what to watch next.",
        f"Pair: {pair}",
        f"Signal: Hourly EMA{f_period}/EMA{s_period} crossover — {direction}",
        f"Trend bias: {bias}",
        f"Price: {price}",
        f"EMA{f_period}: {ema_f}",
        f"EMA{s_period}: {ema_s}",
        f"Notes: {notes}",
        "Style: punchy, trader-friendly, max 45 words total.",
    ])


def _groq_h1_note(ema_state: Dict[str, Any]) -> str:
    """Return a Groq AI enrichment note for an H1 EMA alert, or '' on any failure."""
    if not (GROQ_AI_ENABLED and GROQ_API_KEY and GROQ_AI_MODEL):
        return ""
    if not _groq_can_call():
        return ""
    url = "https://api.groq.com/openai/v1/chat/completions"
    payload = {
        "model": GROQ_AI_MODEL,
        "messages": [{"role": "user", "content": _groq_h1_prompt(ema_state)}],
        "max_tokens": 150,
    }
    headers = {"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"}
    try:
        resp = _groq_session().post(url, headers=headers, json=payload, timeout=GROQ_AI_TIMEOUT)
        if resp.status_code == 429:
            _groq_mark_429()
            return ""
        if resp.status_code != 200:
            log.warning("momentum: Groq AI HTTP %s — %s", resp.status_code, resp.text[:200])
            return ""
        _groq_mark_call()
        choices = resp.json().get("choices") or []
        ai_text = choices[0].get("message", {}).get("content", "") if choices else ""
        ai_text = " ".join(str(ai_text).strip().split())[:GROQ_AI_MAX_CHARS]
        # Escape HTML special chars for Telegram HTML parse mode
        ai_text = ai_text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        return ai_text
    except Exception as exc:
        log.warning("momentum: Groq AI enrichment skipped: %s", exc)
        return ""


def build_h1_alerts(ema_state: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Build Telegram alert dicts from a single H1 EMA state dict.

    Mirrors ema.py ``build_alerts`` exactly; substitutes "Hourly" for "Daily"
    throughout.  Each returned dict has ``key``, ``text``, and ``level``.
    """
    alerts: List[Dict[str, Any]] = []

    if ema_state.get("suggestion") == "warming_up":
        return alerts
    if not ema_state.get("ok"):
        return alerts

    price = ema_state.get("price")
    ema_f = ema_state.get("fast_ema")
    ema_s = ema_state.get("slow_ema")
    if price is None or ema_f is None or ema_s is None:
        return alerts

    price = float(price)
    ema_f = float(ema_f)
    ema_s = float(ema_s)

    pair   = ema_state.get("pair", "")
    f      = int(ema_state.get("fast_period", EMA_SIGNAL_FAST))
    s      = int(ema_state.get("slow_period", EMA_SIGNAL_SLOW))
    bucket = ema_state.get("last_completed_hour", "")
    trend  = ema_state.get("trend_bias", "neutral_bias")
    cross  = ema_state.get("ema_cross_signal", "none")
    notes  = ema_state.get("notes", "")

    bias     = _h1_human_bias(trend)
    side_f   = _h1_side(price, ema_f)
    side_s   = _h1_side(price, ema_s)
    header   = _h1_pair_header(pair, bias=trend)
    ts       = _h1_fmt_bucket(bucket)
    fp       = _h1_format_price

    def _clean(v: Any) -> str:
        if not v:
            return "No additional commentary."
        return " ".join(str(v).split()).strip() or "No additional commentary."

    def _structure_text() -> str:
        if side_f == "above" and side_s == "above":
            return f"Price above EMA{f} and EMA{s}"
        if side_f == "below" and side_s == "below":
            return f"Price below EMA{f} and EMA{s}"
        return f"Price {side_f} EMA{f} and {side_s} EMA{s}"

    def _proximity_signal(ema_period: int, side: str, dist_pct: float) -> Tuple[str, str]:
        b = bias.lower()
        if "bullish" in b:
            sig = (f"Bullish Hourly retest near EMA{ema_period} support"
                   if side == "above" else
                   f"Bullish Hourly pullback through EMA{ema_period}")
        elif "bearish" in b:
            sig = (f"Bearish Hourly retest near EMA{ema_period} resistance"
                   if side == "below" else
                   f"Bearish Hourly pullback through EMA{ema_period}")
        else:
            sig = f"Hourly price testing EMA{ema_period}"
        summary = (
            f"Price is trading within {dist_pct * 100:.3f}% of Hourly EMA{ema_period}. "
            f"Current price is {side} EMA{ema_period} while the broader Hourly bias remains {b}."
        )
        return sig, summary

    def _fmt_msg(*, signal: str, summary: str,
                 extra_lines: Optional[List[str]] = None) -> str:
        parts = [
            f"<b>Hourly EMA Alert | {header}</b>",
            "",
            f"<b>Signal:</b> {signal}",
            f"<b>Bias:</b> {bias}",
            "<b>Timeframe:</b> Hourly",
            f"<b>Last completed Hourly candle:</b> {ts}",
            "",
            f"<b>Price:</b> {fp(pair, price)}",
            f"<b>EMA{f}:</b> {fp(pair, ema_f)}",
            f"<b>EMA{s}:</b> {fp(pair, ema_s)}",
            f"<b>Structure:</b> {_structure_text()}",
        ]
        if extra_lines:
            parts.extend(extra_lines)
        parts.extend([
            "",
            f"<b>Summary:</b> {_clean(summary)}",
        ])
        return "\n".join(parts)

    # Crossover alerts only — proximity and imminent-cross alerts removed.
    # Alert fires on a confirmed EMA20/50 crossover (completed Hourly candle).
    if cross in ("cross_up", "cross_down"):
        is_bull = cross == "cross_up"
        relation = "above" if is_bull else "below"
        sig = ("🟢 Bullish crossover confirmed on Hourly" if is_bull
               else "🔴 Bearish crossover confirmed on Hourly")
        key = f"{pair}:h1:cross:{cross}:{bucket}"
        summary = (
            f"EMA{f} has crossed {relation} EMA{s} on the latest completed Hourly candle. "
            f"Current price is {side_f} EMA{f} and {side_s} EMA{s}. "
            f"{_clean(notes)}"
        )
        text = _fmt_msg(
            signal=sig,
            summary=summary,
            extra_lines=[f"<b>Cross status:</b> EMA{f} {relation} EMA{s}"],
        )
        ai_note = _groq_h1_note(ema_state)
        if ai_note:
            text += "\n─────────────────────\n<b>🤖 Groq AI</b>\n" + ai_note
        alerts.append({"key": key, "text": text, "level": "cross"})

    return alerts


def dispatch_h1_ema_alerts(
    ema_states: Dict[str, Dict[str, Any]],
    bot_token: str = TELEGRAM_BOT_TOKEN,
    chat_id: int = TELEGRAM_CHAT_ID,
    dry_run: bool = False,
) -> int:
    """Evaluate all H1 EMA states, fire new alerts, return count sent.

    De-duplication is keyed on (pair, signal-type, H1-bucket) so the same
    signal is never re-sent within the same hourly candle.  State is persisted
    in H1_ALERT_STATE_FILE between cron runs.
    """
    if not bot_token:
        log.debug("momentum: Telegram bot token not configured — H1 EMA alerts skipped")
        return 0
    if not dry_run and not chat_id:
        log.warning("momentum: TELEGRAM_CHAT_ID not set — H1 EMA alerts disabled")
        return 0

    state = _h1_prune_alert_state(_h1_load_alert_state())
    sent = 0

    for pair, ema_state in ema_states.items():
        for alert in build_h1_alerts(ema_state):
            key = alert["key"]
            if key in state:
                log.debug("momentum: skipping duplicate H1 alert key=%s", key)
                continue
            if dry_run:
                log.info("[DRY-RUN] Would send H1 EMA alert key=%s:\n%s", key, alert["text"])
                sent += 1
            else:
                ok = _tg_send(bot_token, chat_id, alert["text"])
                if ok:
                    state[key] = _iso_z(_now_utc())
                    sent += 1
                    log.info(
                        "momentum: sent H1 %s EMA alert for %s (key=%s)",
                        alert["level"], pair, key,
                    )
                else:
                    log.warning(
                        "momentum: failed to send H1 %s EMA alert for %s",
                        alert["level"], pair,
                    )

    if not dry_run:
        _h1_save_alert_state(state)
    return sent


# ---------------------------------------------------------------------------
# Public API — fetch_price_momentum
# ---------------------------------------------------------------------------

def fetch_price_momentum(
    pairs: Optional[List[str]] = None,
    api_key: str = "",
) -> Tuple[Dict[str, float], Dict[str, str]]:
    global TWELVEDATA_API_KEY
    _log_startup_once()
    if api_key:
        TWELVEDATA_API_KEY = api_key.strip()

    pair_list = [_normalize_pair_code(p) for p in (pairs or _default_pairs())]
    pair_list = [p for p in pair_list if p in TD_SYMBOLS]
    if not pair_list:
        pair_list = list(DEFAULT_MAIN_PAIRS)

    histories, current_prices, current_sources, repair_notes = _update_histories(pair_list)
    out: Dict[str, float] = {}
    source_map: Dict[str, str] = {}
    diagnostics: Dict[str, Dict[str, Union[str, float, bool]]] = {}

    for pair in pair_list:
        rows = histories.get(pair, [])
        closes = _rows_to_closes(rows)
        score = trend_impulse_score(closes, pair)
        out[pair] = score

        note = repair_notes.get(pair, "")
        if note.startswith("td_seeded"):
            src = SOURCE_TD_SEEDED
        elif rows:
            src = SOURCE_HOURLY
        else:
            src = SOURCE_UNAVAILABLE
        source_map[pair] = src

        needs_seed, reason = _needs_seed_or_repair(rows, pair)
        pr = _repair_priority(pair, rows)
        diagnostics[pair] = {
            "score": score,
            "bars": float(len(closes)),
            "last_close": float(closes[-1]) if closes else 0.0,
            "current_price": float(current_prices[pair]) if current_prices[pair] is not None else 0.0,
            "current_source": current_sources.get(pair, SOURCE_UNAVAILABLE),
            "history_source": src,
            "repair_note": note,
            "needs_seed": bool(needs_seed),
            "needs_seed_reason": reason,
            "repair_priority_class": float(pr[0]),
            "repair_priority_metric": float(pr[1]),
            "td_requests_today": float(_get_td_requests_today()),
            "td_window_active": bool(_current_seed_window_key() is not None),
            "strict_one_attempt_per_window": STRICT_ONE_ATTEMPT_PER_WINDOW,
            "bootstrap_anytime_enabled": ALLOW_BOOTSTRAP_ANYTIME,
            "expected_last_hour": _iso_z(_latest_expected_tradable_hour(_now_utc(), pair)),
        }

    _LAST_MOMENTUM_DIAGNOSTICS.clear()
    _LAST_MOMENTUM_DIAGNOSTICS.update(diagnostics)

    # Compute EMA states from the same histories — no second Stooq fetch needed.
    # _compute_ema_state always returns a dict (never None); warming-up pairs
    # are stored with ok=False so consumers can detect the state.
    _LAST_EMA_STATE.clear()
    _LAST_H1_EMA_STATE.clear()
    for pair in pair_list:
        _state = _compute_ema_state(
            pair,
            histories.get(pair, []),
            current_prices.get(pair),
            current_sources.get(pair, SOURCE_UNAVAILABLE),
            repair_notes.get(pair, ""),
        )
        # Preserve raw H1 state before scraper may replace it with D1 data.
        _LAST_H1_EMA_STATE[pair] = dict(_state)
        _state = _apply_scraper_daily_ema_seed(
            pair,
            _state,
            current_prices.get(pair),
            current_sources.get(pair, SOURCE_UNAVAILABLE),
        )
        _LAST_EMA_STATE[pair] = _state

    # Dispatch Hourly EMA 20/50 Telegram alerts (non-blocking; errors are logged).
    if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
        try:
            dispatch_h1_ema_alerts(_LAST_H1_EMA_STATE)
        except Exception as _tg_exc:
            log.warning("momentum: Telegram H1 alert dispatch failed: %s", _tg_exc)

    return out, source_map


# ---------------------------------------------------------------------------
# Public API — detect_h1_ema_cross_and_price_state
# ---------------------------------------------------------------------------

def detect_h1_ema_cross_and_price_state(
    pair: str,
    api_key: str = "",
    bars: Optional[int] = None,
) -> Optional[Dict[str, Any]]:
    global TWELVEDATA_API_KEY
    if api_key:
        TWELVEDATA_API_KEY = api_key.strip()

    code = _normalize_pair_code(pair)
    if not code or code not in TD_SYMBOLS:
        log.warning("momentum: unsupported pair=%s in detect_h1_ema_cross_and_price_state", pair)
        return None

    histories, current_prices, current_sources, repair_notes = _update_histories([code])
    result = _compute_ema_state(
        code,
        histories.get(code, []),
        current_prices.get(code),
        current_sources.get(code, SOURCE_UNAVAILABLE),
        repair_notes.get(code, ""),
        bars=bars,
    )
    # _compute_ema_state always returns a dict; update diagnostics regardless.
    _LAST_EMA_DIAGNOSTICS[code] = dict(result)
    return result


# ---------------------------------------------------------------------------
# Diagnostics accessors
# ---------------------------------------------------------------------------

def get_last_momentum_diagnostics() -> Dict[str, Dict[str, Union[str, float, bool]]]:
    return {k: dict(v) for k, v in _LAST_MOMENTUM_DIAGNOSTICS.items()}


def get_last_ema_diagnostics() -> Dict[str, Dict[str, Union[str, float, bool]]]:
    return {k: dict(v) for k, v in _LAST_EMA_DIAGNOSTICS.items()}


def get_last_ema_state() -> Dict[str, Dict[str, Any]]:
    """Return EMA states computed during the most recent fetch_price_momentum call.

    Callers (e.g. macro.py) can use this to obtain ema_20_50_state without
    triggering a second round of Stooq fetches.
    """
    return {k: dict(v) for k, v in _LAST_EMA_STATE.items()}


# ---------------------------------------------------------------------------
# Convenience helpers
# ---------------------------------------------------------------------------


def get_momentum(pair: str, macro_or_closes: Union[Dict[str, Any], List[float]]) -> float:
    pair_l = pair.lower().strip()
    if isinstance(macro_or_closes, list):
        return trend_impulse_score(macro_or_closes, pair_l)
    if isinstance(macro_or_closes, dict):
        price = macro_or_closes.get("price_momentum", {}).get(pair_l)
        if isinstance(price, (int, float)) and abs(price) >= SIGNAL_FLOOR:
            return float(price)
        ema_state = macro_or_closes.get("ema_20_50_state", {}).get(pair_l)
        if isinstance(ema_state, dict):
            return _EMA_BIAS_MAP.get(ema_state.get("trend_bias", "neutral_bias"), 0.0)
    return 0.0


def momentum_blind(macro: Dict[str, Any]) -> bool:
    def _has_signal(values: object) -> bool:
        if not isinstance(values, dict) or not values:
            return False
        for value in values.values():
            try:
                if abs(float(value)) >= SIGNAL_FLOOR:
                    return True
            except (TypeError, ValueError):
                pass
        return False

    if _has_signal(macro.get("price_momentum")):
        return False
    ema_states = macro.get("ema_20_50_state", {})
    if isinstance(ema_states, dict):
        for state in ema_states.values():
            if isinstance(state, dict) and state.get("trend_bias") in ("bullish_bias", "bearish_bias"):
                return False
    return True


def momentum_summary(macro: Dict[str, Any]) -> Dict[str, float]:
    out: Dict[str, float] = {}
    if not isinstance(macro, dict):
        return out
    price = macro.get("price_momentum", {})
    ema_states = macro.get("ema_20_50_state", {})
    for pair in TD_SYMBOLS:
        p = price.get(pair)
        if isinstance(p, (int, float)):
            out[pair] = round(float(p), 3)
        else:
            state = ema_states.get(pair)
            if isinstance(state, dict):
                out[pair] = round(_EMA_BIAS_MAP.get(state.get("trend_bias", "neutral_bias"), 0.0), 3)
            else:
                out[pair] = 0.0
    return out


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    momentum, sources = fetch_price_momentum()
    print(json.dumps(
        {"momentum": momentum, "sources": sources, "diagnostics": get_last_momentum_diagnostics()},
        indent=2,
    ))

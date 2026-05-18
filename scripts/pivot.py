#!/usr/bin/env python3

import json
import logging
import os
import sqlite3
import time
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone as _tz
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional, Tuple, Union

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    ZoneInfo = None  # type: ignore

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

try:
    from dotenv import load_dotenv
    _env_path = Path(__file__).resolve().parent / ".env"
    load_dotenv(dotenv_path=_env_path, override=False)
except ImportError:
    pass

log = logging.getLogger(__name__)

                                                                               
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
        log.warning("pivot: invalid %s=%r; using default=%s", name, raw, default)
        return default

def _coerce_component_path(component_file: Union[str, Path]) -> Path:
    return component_file if isinstance(component_file, Path) else Path(component_file)

                                                                               
DEFAULT_COMPONENT_FILE = Path(
    os.environ.get("MACRO_COMPONENTS_FILE", "public/macro_components.json")
)

# Output path for the pivot-levels JSON consumed by pivots.html.
# Defaults to the same directory as macro_components.json.
PIVOT_LEVELS_FILE = Path(
    os.environ.get(
        "PIVOT_LEVELS_FILE",
        str(DEFAULT_COMPONENT_FILE.parent / "pivot_levels.json"),
    )
)

BREAK_TOL = _env_float("PRICE_PIVOT_BREAK_TOL", 0.05, minimum=0.0)
REJECT_TOL = _env_float("PRICE_PIVOT_REJECT_TOL", 0.10, minimum=0.0)
BALANCE_BAND = _env_float("PRICE_PIVOT_BALANCE_BAND", 0.15, minimum=0.0)
ATR_CAP_MULT = _env_float("PRICE_PIVOT_ATR_CAP", 0.08, minimum=0.0)

_CONV_WITH_BREAKOUT = _env_float("PRICE_CONV_WITH_BREAKOUT", 1.10, minimum=0.0)
_CONV_WITH_ACCEPT = _env_float("PRICE_CONV_WITH_ACCEPT", 1.00, minimum=0.0)
_CONV_AGAINST_ACCEPT = _env_float("PRICE_CONV_AGAINST_ACCEPT", 0.85, minimum=0.0)
_CONV_AGAINST_BREAKOUT = _env_float("PRICE_CONV_AGAINST_BREAKOUT", 0.70, minimum=0.0)
_CONV_REJECTION = _env_float("PRICE_CONV_REJECTION", 0.75, minimum=0.0)
_CONV_BALANCE = _env_float("PRICE_CONV_BALANCE", 0.65, minimum=0.0)
_CONV_NEUTRAL_MACRO = _env_float("PRICE_CONV_NEUTRAL_MACRO", 1.00, minimum=0.0)

NEUTRAL_MACRO_THRESHOLD = _env_float("PRICE_PIVOT_NEUTRAL_MACRO", 0.30, minimum=0.0)

# ── RSI confluence settings ──────────────────────────────────────────────────
# RSI is calculated from completed daily closes whenever enough history is
# available from the same source that provides OHLC. It is added to every pivot
# result as extra context without replacing the existing pivot/macro state.
RSI_PERIOD: int = max(2, int(_env_float("PRICE_RSI_PERIOD", 14.0, minimum=2.0)))
RSI_OVERBOUGHT: float = _env_float("PRICE_RSI_OVERBOUGHT", 70.0, minimum=50.0, maximum=100.0)
RSI_OVERSOLD: float = _env_float("PRICE_RSI_OVERSOLD", 30.0, minimum=0.0, maximum=50.0)
RSI_BULLISH_LEVEL: float = _env_float("PRICE_RSI_BULLISH_LEVEL", 55.0, minimum=0.0, maximum=100.0)
RSI_BEARISH_LEVEL: float = _env_float("PRICE_RSI_BEARISH_LEVEL", 45.0, minimum=0.0, maximum=100.0)
RSI_WITH_MULT: float = _env_float("PRICE_RSI_WITH_MULT", 1.05, minimum=0.0)
RSI_AGAINST_MULT: float = _env_float("PRICE_RSI_AGAINST_MULT", 0.95, minimum=0.0)
RSI_NEUTRAL_MULT: float = _env_float("PRICE_RSI_NEUTRAL_MULT", 1.00, minimum=0.0)
STOOQ_RSI_HISTORY_PERIOD: str = os.environ.get("PIVOT_STOOQ_RSI_HISTORY_PERIOD", "3mo").strip() or "3mo"

# ── SQLite OHLC cache (mirrors 1pivot.py pivot_cache table) ─────────────────
# Shares the same pivot.db used by the pivot daemon so both modules draw from
# one consistent store.  Override the path with PIVOT_DB env var.
_PIVOT_DB_PATH: Path = Path(
    os.environ.get("PIVOT_DB", "./pivot.db")
).expanduser().resolve()
PIVOT_FORCE_REFRESH: bool = (
    os.environ.get("PIVOT_FORCE_REFRESH", "0").strip().lower() in ("1", "true", "yes", "on")
)

                                                                               
TWELVEDATA_API_KEY = os.environ.get("TWELVEDATA_API_KEY", "").strip()
TWELVEDATA_BASE = "https://api.twelvedata.com"

# ── Shared TwelveData quota ───────────────────────────────────────────────────
# Reads and writes the same momentum_td_usage.json that momentum.py uses so
# both modules draw from one combined per-minute and per-run budget.
# Hard limit: 8 calls/min → enforce ≥7.5 s between any two TD calls.
# Per-run pivot cap: PIVOT_TD_MAX_PER_RUN (default 2) so bootstrap in
# momentum.py can still use its own slots without the combined total
# bursting over the minutely ceiling.
_TD_USAGE_FILE = Path(
    os.environ.get(
        "PIVOT_TD_USAGE_FILE",
        os.environ.get(
            "TWELVEDATA_USAGE_FILE",
            str(Path(os.environ.get("MOMENTUM_DATA_DIR") or os.environ.get("SCRAPER_OUTPUT_DIR") or Path(__file__).resolve().parent) / "momentum_td_usage.json"),
        ),
    )
)
_TD_MIN_INTERVAL_S: float = 60.0 / 8          # 7.5 s between calls
TD_PIVOT_MAX_PER_RUN: int = int(os.environ.get("PIVOT_TD_MAX_PER_RUN", "4"))
_pivot_td_calls_this_run: int = 0              # module-level run counter


def _load_td_usage_shared() -> Dict[str, Any]:
    """Load the shared TD usage file (same one momentum.py maintains)."""
    try:
        with open(_TD_USAGE_FILE, encoding="utf-8") as _f:
            _data = json.load(_f)
    except Exception:
        _data = {}
    _today = datetime.now(_tz.utc).date().isoformat()
    if _data.get("date") != _today:
        return {
            "date": _today,
            "count": 0,
            "last_request_ts": None,
            "cooldown_until": None,
        }
    _data.setdefault("count", 0)
    _data.setdefault("last_request_ts", None)
    _data.setdefault("cooldown_until", None)
    return _data


def _save_td_usage_shared(_data: Dict[str, Any]) -> None:
    try:
        with open(_TD_USAGE_FILE, "w", encoding="utf-8") as _f:
            json.dump(_data, _f)
    except Exception as _exc:
        log.warning("pivot: could not save shared TD usage file: %s", _exc)


def _td_pivot_can_call() -> bool:
    """
    Return True and sleep if needed to honour the 8/min rate limit.
    Returns False immediately if the per-run cap or a cooldown blocks the call.
    """
    if _pivot_td_calls_this_run >= TD_PIVOT_MAX_PER_RUN:
        log.warning(
            "pivot: TD per-run cap reached (%d/%d) -- skipping TwelveData for this pair",
            _pivot_td_calls_this_run, TD_PIVOT_MAX_PER_RUN,
        )
        return False
    usage = _load_td_usage_shared()
    # Respect any active cooldown set by momentum.py (e.g. after a 429)
    _cooldown = usage.get("cooldown_until")
    if _cooldown:
        try:
            _cu = datetime.fromisoformat(_cooldown.replace("Z", "+00:00"))
            if datetime.now(_tz.utc) < _cu:
                log.warning("pivot: TD cooldown active until %s -- skipping", _cooldown)
                return False
        except Exception:
            pass
    # Throttle to enforce ≥7.5 s between calls across both modules
    _last = usage.get("last_request_ts")
    if _last:
        try:
            _lt = datetime.fromisoformat(_last.replace("Z", "+00:00"))
            _elapsed = (datetime.now(_tz.utc) - _lt).total_seconds()
            _wait = _TD_MIN_INTERVAL_S - _elapsed
            if _wait > 0:
                log.debug("pivot: throttling TD call by %.1fs to respect rate limit", _wait)
                time.sleep(_wait)
        except Exception:
            pass
    return True


def _mark_td_call_shared() -> None:
    """Increment the shared daily counter and update last_request_ts."""
    global _pivot_td_calls_this_run
    usage = _load_td_usage_shared()
    usage["count"] = int(usage.get("count", 0)) + 1
    usage["last_request_ts"] = datetime.now(_tz.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    _save_td_usage_shared(usage)
    _pivot_td_calls_this_run += 1
    log.debug(
        "pivot: TD call recorded -- daily total=%d run total=%d",
        usage["count"], _pivot_td_calls_this_run,
    )
# ─────────────────────────────────────────────────────────────────────────────


_DEFAULT_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/146.0.0.0 Safari/537.36 Edg/146.0.3856.62"
)
_USER_AGENT = os.environ.get("SCRAPER_UA", _DEFAULT_UA)

TD_SYMBOLS: Dict[str, str] = {
    "eurusd": "EUR/USD",
    "gbpusd": "GBP/USD",
    "xauusd": "XAU/USD",
}


# Stooq symbols (free, no API key) — used as a fallback OHLC source.
# All symbols are bare (no =x suffix). macro.StooqTicker.__init__ strips
# any =x suffix automatically, but we keep these canonical here for clarity.
STOOQ_SYMBOLS: Dict[str, str] = {
    "eurusd": "eurusd",
    "gbpusd": "gbpusd",
    "xauusd": "xauusd",
}

DEFAULT_MAIN_PAIRS: Tuple[str, ...] = ("eurusd", "gbpusd", "xauusd")

def _normalize_pair_code(value: str) -> str:
    return "".join(ch for ch in (value or "").lower() if ch.isalnum())

def _parse_main_pairs(value) -> List[str]:
    if isinstance(value, str):
        items = [v.strip() for v in value.split(",") if v.strip()]
    elif isinstance(value, (list, tuple, set)):
        items = [str(v).strip() for v in value if str(v).strip()]
    else:
        items = []
    out: List[str] = []
    seen = set()
    for item in items:
        code = _normalize_pair_code(item)
        if code and code not in seen:
            out.append(code)
            seen.add(code)
    return out

def _active_main_pairs() -> List[str]:
    # A pair is supported if it has a mapping in any price source.
    _KNOWN_PAIRS = set(TD_SYMBOLS) | set(STOOQ_SYMBOLS)

    def _filter(raw: List[str]) -> Tuple[List[str], List[str]]:
        return (
            [p for p in raw if p in _KNOWN_PAIRS],
            [p for p in raw if p not in _KNOWN_PAIRS],
        )

    env_pairs = _parse_main_pairs(os.environ.get("PIVOT_MAIN_PAIRS", ""))
    if env_pairs:
        filtered, unknown = _filter(env_pairs)
        if unknown:
            log.warning("pivot: ignoring pairs with no price-source mapping: %s", unknown)
        if filtered:
            return filtered
        log.warning("pivot: PIVOT_MAIN_PAIRS set but no valid pairs remain after filtering; falling back to defaults")
    cfg_path = os.environ.get("SCRAPER_CONFIG", "").strip()
    if cfg_path:
        try:
            with open(cfg_path, encoding="utf-8") as f:
                cfg = json.load(f)
            cfg_pairs = _parse_main_pairs((cfg or {}).get("main_pairs", []))
            if cfg_pairs:
                filtered, unknown = _filter(cfg_pairs)
                if unknown:
                    log.warning("pivot: ignoring pairs with no price-source mapping: %s", unknown)
                if filtered:
                    return filtered
                log.warning("pivot: config main_pairs has no valid pairs after filtering; falling back to defaults")
        except Exception as exc:
            log.warning("pivot: main_pairs config load failed: %s", exc)
    return list(DEFAULT_MAIN_PAIRS)

HTTP_TIMEOUT = int(_env_float("PRICE_PIVOT_HTTP_TIMEOUT", 10.0, minimum=1.0))

SOURCE_TD = "twelvedata"
SOURCE_STOOQ = "stooq"
SOURCE_COMPONENTS = "components"
SOURCE_INJECTED = "injected"
SOURCE_UNAVAILABLE = "unavailable"

STATE_UNAVAILABLE = "unavailable"

                                                                               
import threading as _threading
_thread_local = _threading.local()

def _get_session() -> requests.Session:
    if not hasattr(_thread_local, "session"):
        s = requests.Session()
        s.headers.update({"User-Agent": _USER_AGENT, "Accept": "application/json,*/*"})
        s.mount(
            "https://",
            HTTPAdapter(
                max_retries=Retry(
                    total=3,
                    backoff_factor=0.5,
                    status_forcelist=[429, 500, 502, 503, 504],
                    allowed_methods=frozenset({"GET", "POST"}),
                )
            ),
        )
        _thread_local.session = s
    return _thread_local.session

                                                                               
def _valid_ohlc(ohlc: Dict[str, float]) -> bool:
    try:
        o = float(ohlc["open"])
        h = float(ohlc["high"])
        l = float(ohlc["low"])
        c = float(ohlc["close"])
    except (KeyError, TypeError, ValueError):
        return False
    if h <= l or l <= 0:   # h == l means zero-range (synthetic flat bar) — reject
        return False
    if not (l <= o <= h):
        return False
    if not (l <= c <= h):
        return False
    # Sanity: reject bars where the H/L range exceeds 50% of the High.
    # A legitimate daily bar for any instrument should never have a Low that
    # is more than 50% below the High (e.g. High=4610, Low=0.01 is invalid).
    if (h - l) / h > 0.50:
        return False
    return True

def _clean_ohlc(ohlc: Dict[str, float]) -> Optional[Dict[str, float]]:
    try:
        cleaned: Dict[str, float] = {
            "open": float(ohlc["open"]),
            "high": float(ohlc["high"]),
            "low": float(ohlc["low"]),
            "close": float(ohlc["close"]),
        }
    except (KeyError, TypeError, ValueError):
        return None

    if not _valid_ohlc(cleaned):
        return None

    for key in ("current_price", "atr5", "session_high", "session_low", "rsi", "rsi14", "rsi_period", "rsi_source"):
        if key in ohlc and ohlc[key] is not None:
            try:
                cleaned[key] = float(ohlc[key])
            except (TypeError, ValueError):
                pass

    if "_date" in ohlc and ohlc["_date"] is not None:
        cleaned["_date"] = str(ohlc["_date"])

    return cleaned

def _normalize_live_context(ohlc: Dict[str, float]) -> Dict[str, Optional[float]]:
    return {
        "current_price": float(ohlc["current_price"]) if ohlc.get("current_price") is not None else None,
        "atr5": float(ohlc["atr5"]) if ohlc.get("atr5") is not None else None,
        "session_high": float(ohlc["session_high"]) if ohlc.get("session_high") is not None else None,
        "session_low": float(ohlc["session_low"]) if ohlc.get("session_low") is not None else None,
    }

def _unavailable_result(macro_score: float = 0.0) -> Dict[str, Union[str, float, Dict[str, Union[str, float]]]]:
    return {
        "price_state": STATE_UNAVAILABLE,
        "rejection_level": "",
        "macro_alignment": "unavailable",
        "conviction_mult": 0.0,
        "score_adj": 0.0,
        "price_used": 0.0,
        "tolerance": 0.0,
        "state_basis": "none",
        "state_quality": "none",
        "nearest_level": {"name": "", "value": 0.0, "distance": 0.0, "side": "at"},
        "PP": 0.0,
        "R1": 0.0,
        "R2": 0.0,
        "R3": 0.0,
        "S1": 0.0,
        "S2": 0.0,
        "S3": 0.0,
        "range": 0.0,
        "close": 0.0,
        "macro_score": round(float(macro_score), 3),
        "ohlc_source": SOURCE_UNAVAILABLE,
        "ohlc_date": "",
        "rsi": None,
        "rsi_period": RSI_PERIOD,
        "rsi_state": "unavailable",
        "rsi_bias": "unavailable",
        "rsi_alignment": "unavailable",
        "rsi_confluence_mult": 0.0,
        "combined_conviction_mult": 0.0,
    }

                                                                               
# ---------------------------------------------------------------------------
# RSI helpers
# ---------------------------------------------------------------------------

def _rsi_from_closes(closes: List[float], period: int = RSI_PERIOD) -> Optional[float]:
    """Return Wilder RSI from a chronological list of completed daily closes."""
    clean_closes: List[float] = []
    for close in closes:
        try:
            clean_closes.append(float(close))
        except (TypeError, ValueError):
            continue

    if len(clean_closes) < period + 1:
        return None

    deltas = [clean_closes[i] - clean_closes[i - 1] for i in range(1, len(clean_closes))]
    gains = [max(delta, 0.0) for delta in deltas]
    losses = [max(-delta, 0.0) for delta in deltas]

    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period

    for gain, loss in zip(gains[period:], losses[period:]):
        avg_gain = ((avg_gain * (period - 1)) + gain) / period
        avg_loss = ((avg_loss * (period - 1)) + loss) / period

    if avg_loss == 0:
        return 100.0 if avg_gain > 0 else 50.0

    rs = avg_gain / avg_loss
    return round(100.0 - (100.0 / (1.0 + rs)), 2)


def _rsi_state(rsi: Optional[float]) -> str:
    if rsi is None:
        return "unavailable"
    if rsi >= RSI_OVERBOUGHT:
        return "overbought"
    if rsi <= RSI_OVERSOLD:
        return "oversold"
    return "neutral"


def _rsi_bias(rsi: Optional[float]) -> str:
    if rsi is None:
        return "unavailable"
    if rsi >= RSI_BULLISH_LEVEL:
        return "bullish"
    if rsi <= RSI_BEARISH_LEVEL:
        return "bearish"
    return "neutral"


def _attach_rsi(ohlc: Dict[str, float], rsi: Optional[float], source: str) -> Dict[str, float]:
    """Attach RSI fields to an OHLC payload in-place and return it."""
    if rsi is None:
        return ohlc
    ohlc["rsi"] = float(rsi)
    # Backward-friendly alias for dashboards that prefer indicator-period keys.
    ohlc[f"rsi{RSI_PERIOD}"] = float(rsi)
    ohlc["rsi_period"] = float(RSI_PERIOD)
    ohlc["rsi_source"] = source
    return ohlc


def _extract_closes_from_history(raw_history: object) -> List[float]:
    """Extract chronological closes from list/dict history payloads."""
    if not isinstance(raw_history, list):
        return []

    rows = raw_history
    try:
        rows = sorted(
            raw_history,
            key=lambda row: str(row.get("date", row.get("Date", row.get("datetime", ""))))
            if isinstance(row, dict) else "",
        )
    except Exception:
        rows = raw_history

    closes: List[float] = []
    for row in rows:
        if isinstance(row, dict):
            value = row.get("close", row.get("Close"))
        else:
            value = row
        try:
            closes.append(float(value))
        except (TypeError, ValueError):
            continue
    return closes


def _rsi_alignment_for_state(rsi: Optional[float], price_state: str) -> Tuple[str, float]:
    """Map RSI bias to the current pivot state and return alignment + multiplier."""
    if rsi is None:
        return "unavailable", 0.0

    bias = _rsi_bias(rsi)
    bullish_states = {"breakout_up", "accept_above_pp"}
    bearish_states = {"breakout_down", "accept_below_pp"}

    if bias == "neutral" or price_state in {"balance", "rejection"}:
        return "neutral", RSI_NEUTRAL_MULT
    if price_state in bullish_states and bias == "bullish":
        return "with", RSI_WITH_MULT
    if price_state in bearish_states and bias == "bearish":
        return "with", RSI_WITH_MULT
    if price_state in bullish_states and bias == "bearish":
        return "against", RSI_AGAINST_MULT
    if price_state in bearish_states and bias == "bullish":
        return "against", RSI_AGAINST_MULT
    return "neutral", RSI_NEUTRAL_MULT


def _apply_rsi_to_result(result: Dict, ohlc: Dict[str, float]) -> Dict:
    """Add RSI context and a combined conviction multiplier to a pivot result."""
    rsi = ohlc.get("rsi", ohlc.get(f"rsi{RSI_PERIOD}"))
    try:
        rsi_val = float(rsi) if rsi is not None else None
    except (TypeError, ValueError):
        rsi_val = None

    alignment, rsi_mult = _rsi_alignment_for_state(rsi_val, str(result.get("price_state", "")))
    base_conv = float(result.get("conviction_mult", 0.0) or 0.0)

    result["rsi"] = round(rsi_val, 2) if rsi_val is not None else None
    result["rsi_period"] = int(float(ohlc.get("rsi_period", RSI_PERIOD) or RSI_PERIOD))
    result["rsi_state"] = _rsi_state(rsi_val)
    result["rsi_bias"] = _rsi_bias(rsi_val)
    result["rsi_alignment"] = alignment
    result["rsi_confluence_mult"] = round(rsi_mult, 3)
    result["combined_conviction_mult"] = round(base_conv * rsi_mult, 3) if rsi_val is not None else base_conv
    return result

# ---------------------------------------------------------------------------
# Stooq OHLC — yfinance-style, free, no API key
# ---------------------------------------------------------------------------
# Imports the shared StooqTicker engine from macro.py so the session, handshake,
# retry adapter, and CSV parser are all shared across the codebase.
# ---------------------------------------------------------------------------

try:
    # Canonical shared Stooq engine. Keep pivot.py aligned with macro.py,
    # momentum.py, scraper.py and signal_confirm.py.
    from macro import StooqTicker as _StooqTicker
    _PIVOT_STOOQ_ENGINE = "macro"
except ImportError:
    try:
        from macro_rewritten_full import StooqTicker as _StooqTicker  # type: ignore[no-redef]
        _PIVOT_STOOQ_ENGINE = "macro_rewritten_full"
    except ImportError:
        try:
            from macro_rewritten import StooqTicker as _StooqTicker  # type: ignore[no-redef]
            _PIVOT_STOOQ_ENGINE = "macro_rewritten"
        except ImportError:
            _StooqTicker = None                                      # type: ignore[assignment]
            _PIVOT_STOOQ_ENGINE = "unavailable"


def _fetch_stooq_ohlc(pair: str) -> Optional[Dict[str, float]]:
    """
    Fetch the previous completed daily OHLC bar for *pair* from Stooq.

    Uses the yfinance-style API (macro.StooqTicker / fetch_price):

        t = fetch_price("eurusd")             # StooqTicker instance
        bars = t.history(period="5d")         # list of daily OHLC dicts
        bar  = bars[-2]                       # last *completed* bar

    Falls back gracefully to None if the engine is unavailable or Stooq
    returns N/D for this pair.
    """
    stooq_symbol = STOOQ_SYMBOLS.get(pair.lower())
    if not stooq_symbol:
        log.debug("pivot: no Stooq symbol mapping for %r", pair)
        return None

    if _StooqTicker is None:
        log.debug("pivot: macro.StooqTicker not available -- Stooq OHLC skipped for %r", pair)
        return None

    try:
        t = _StooqTicker(stooq_symbol)
        bars = t.history(period=STOOQ_RSI_HISTORY_PERIOD)  # list[dict], oldest-first
        if not bars:
            log.warning("pivot: Stooq returned no bars for %r (%s)", pair, stooq_symbol)
            return None

        # Use the most-recent *completed* bar relative to the NY Close boundary.
        # Pivots reset at 22:00 UTC (5:00 PM EST / 06:00 MYT next day), so:
        #   • Before 22:00 UTC → today's bar is still live; use yesterday's bar.
        #   • At / after 22:00 UTC → today's bar has closed; include it.
        pivot_date = _pivot_session_date()
        completed = [b for b in bars if str(b.get("Date", "")) <= pivot_date]
        bar = completed[-1] if completed else bars[-1]

        ohlc = _clean_ohlc({
            "open":  bar.get("Open"),
            "high":  bar.get("High"),
            "low":   bar.get("Low"),
            "close": bar.get("Close"),
            "_date": bar.get("Date", ""),
        })
        if ohlc is None:
            log.debug("pivot: Stooq bar for %r failed _clean_ohlc validation", pair)
            return None

        closes = _extract_closes_from_history(completed or bars)
        _attach_rsi(ohlc, _rsi_from_closes(closes), SOURCE_STOOQ)

        # Attach current price from fast_info so pivot gets the live price too
        live_price = t.fast_info.get("last_price")
        if live_price is not None:
            ohlc["current_price"] = float(live_price)

        log.debug("pivot[%s]: Stooq OHLC date=%s O=%s H=%s L=%s C=%s",
                  pair, ohlc.get("_date"), ohlc.get("open"), ohlc.get("high"),
                  ohlc.get("low"), ohlc.get("close"))
        return ohlc
    except Exception as exc:
        log.warning("pivot: Stooq OHLC fetch failed for %r (%s): %s", pair, stooq_symbol, exc)
        return None


def _fetch_twelvedata_ohlc(pair: str) -> Optional[Dict[str, float]]:
    if not TWELVEDATA_API_KEY:
        log.debug("pivot: TWELVEDATA_API_KEY not set -- skipping TwelveData")
        return None

    symbol = TD_SYMBOLS.get(pair.lower())
    if not symbol:
        log.debug("pivot: no TwelveData mapping for %r", pair)
        return None

    if not _td_pivot_can_call():
        return None

    try:
        response = _get_session().get(
            f"{TWELVEDATA_BASE}/time_series",
            params={
                "symbol": symbol,
                "interval": "1day",
                "outputsize": max(2, RSI_PERIOD + 2),
                "apikey": TWELVEDATA_API_KEY,
            },
            timeout=HTTP_TIMEOUT,
        )
        if not response.ok:
            log.warning("pivot: TwelveData HTTP %s for %s", response.status_code, pair)
            return None

        data = response.json()
        if data.get("status") == "error":
            log.warning("pivot: TwelveData error for %s: %s", pair, data.get("message", "?"))
            return None

        values = data.get("values", [])
        if not values:
            return None

        # Select the pivot anchor bar using the NY Close boundary (22:00 UTC).
        # TwelveData returns bars newest-first: values[0] = most recent bar.
        #   • Before 22:00 UTC → values[0] is still live; use values[1].
        #   • At / after 22:00 UTC (or on weekends when markets are closed) →
        #     values[0] has closed; use it as the pivot anchor.
        pivot_date = _pivot_session_date()
        bar_date_0 = str(values[0].get("datetime", ""))[:10] if values else ""
        if bar_date_0 and bar_date_0 <= pivot_date:
            bar = values[0]
            _td_completed = values
        else:
            bar = values[1] if len(values) > 1 else values[0]
            _td_completed = values[1:] if len(values) > 1 else values
        ohlc = _clean_ohlc({
            "open":  bar.get("open"),
            "high":  bar.get("high"),
            "low":   bar.get("low"),
            "close": bar.get("close"),
            "_date": bar.get("datetime", ""),
        })
        if ohlc is not None:
            completed_values = list(reversed(_td_completed))
            closes = _extract_closes_from_history(completed_values)
            _attach_rsi(ohlc, _rsi_from_closes(closes), SOURCE_TD)
            _mark_td_call_shared()
        return ohlc
    except Exception as exc:
        log.warning("pivot: TwelveData fetch failed for %s: %s", pair, exc)
        return None


def _fetch_components_ohlc(
    pair: str,
    component_file: Union[str, Path] = DEFAULT_COMPONENT_FILE,
) -> Optional[Dict[str, float]]:
    component_path = _coerce_component_path(component_file)
    try:
        with component_path.open(encoding="utf-8") as handle:
            data = json.load(handle)
    except FileNotFoundError:
        log.debug("pivot: component file not found: %s", component_path)
        return None
    except Exception as exc:
        log.warning("pivot: failed to load component file: %s", exc)
        return None

    pair_l = pair.lower()
    daily_map = data.get("price_daily", {}) or {}
    session_map = data.get("price_session_ohlc", {}) or {}
    current_map = data.get("price_current", {}) or {}
    atr_map = data.get("price_atr5", {}) or {}
    sh_map = data.get("price_session_high", {}) or {}
    sl_map = data.get("price_session_low", {}) or {}
    rsi_map = (
        data.get(f"price_rsi{RSI_PERIOD}", {})
        or data.get("price_rsi", {})
        or data.get("rsi14", {})
        or data.get("rsi", {})
        or {}
    )
    indicator_map = data.get("price_indicators", {}) or data.get("price_daily_indicators", {}) or {}
    history_map = data.get("price_daily_history", {}) or data.get("price_history", {}) or {}

    daily = daily_map.get(pair_l)
    session_ohlc = session_map.get(pair_l)

    def _candidate(raw: object, label: str) -> Optional[Dict[str, float]]:
        if not isinstance(raw, dict):
            return None
        if raw.get("_synthetic"):
            log.debug(
                "pivot[%s]: skipping %s -- marked _synthetic (flat bar, no real OHLC available)",
                pair, label,
            )
            return None
        cleaned = _clean_ohlc({
            "open": raw.get("open"),
            "high": raw.get("high"),
            "low": raw.get("low"),
            "close": raw.get("close"),
            "_date": raw.get("date", raw.get("_date", "")),
        })
        if cleaned is None:
            log.warning("pivot[%s]: malformed %s entry in components", pair, label)
            return None
        return cleaned

    ohlc = _candidate(daily, "price_daily")
    base_label = "price_daily"
    if ohlc is None:
        ohlc = _candidate(session_ohlc, "price_session_ohlc")
        base_label = "price_session_ohlc"
    if ohlc is None:
        log.debug("pivot[%s]: no usable price_daily/price_session_ohlc entry in components", pair)
        return None

    def _maybe_float(value: object) -> Optional[float]:
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def _attach(name: str, *values: object) -> None:
        for value in values:
            fv = _maybe_float(value)
            if fv is not None:
                ohlc[name] = fv
                return

    session_dict = session_ohlc if isinstance(session_ohlc, dict) else {}

                                                                              
    _attach("current_price", current_map.get(pair_l), session_dict.get("close"), ohlc.get("close"))
    _attach("atr5", atr_map.get(pair_l))
    _attach("session_high", sh_map.get(pair_l), session_dict.get("high"))
    _attach("session_low", sl_map.get(pair_l), session_dict.get("low"))

                                                                                                 
    if not ohlc.get("_date") and isinstance(session_dict, dict):
        ohlc["_date"] = session_dict.get("date", session_dict.get("_date", "")) or ""

                                                                                   
    if ohlc.get("atr5") is not None and ohlc["atr5"] <= 0:
        ohlc.pop("atr5", None)
    sh = ohlc.get("session_high")
    sl = ohlc.get("session_low")
    if sh is not None and sl is not None and sh <= sl:
        ohlc.pop("session_high", None)
        ohlc.pop("session_low", None)
    cp = ohlc.get("current_price")
    sh = ohlc.get("session_high")
    sl = ohlc.get("session_low")
    if cp is not None and sh is not None and sl is not None and not (sl <= cp <= sh):
                                                                      
        log.debug(
            "pivot[%s]: dropping contradictory session bounds from %s (cp=%s sh=%s sl=%s)",
            pair,
            base_label,
            cp,
            sh,
            sl,
        )
        ohlc.pop("session_high", None)
        ohlc.pop("session_low", None)

    indicator_entry = indicator_map.get(pair_l) if isinstance(indicator_map, dict) else None
    direct_rsi = None
    if isinstance(indicator_entry, dict):
        direct_rsi = _maybe_float(indicator_entry.get("rsi14", indicator_entry.get("rsi")))
        if indicator_entry.get("ema20") is not None:
            ohlc["ema20"] = indicator_entry.get("ema20")
        if indicator_entry.get("ema50") is not None:
            ohlc["ema50"] = indicator_entry.get("ema50")
        if indicator_entry.get("last_bar_date") and not ohlc.get("_date"):
            ohlc["_date"] = str(indicator_entry.get("last_bar_date"))
    if direct_rsi is None:
        direct_rsi = _maybe_float(rsi_map.get(pair_l)) if isinstance(rsi_map, dict) else None
    if direct_rsi is not None:
        _attach_rsi(ohlc, direct_rsi, SOURCE_COMPONENTS)
    elif isinstance(history_map, dict):
        closes = _extract_closes_from_history(history_map.get(pair_l))
        _attach_rsi(ohlc, _rsi_from_closes(closes), SOURCE_COMPONENTS)

    return ohlc

_COMPONENTS_TTL_SECONDS = int(
    float(os.environ.get("PIVOT_COMPONENTS_TTL", "1800"))  # 30 min default
)


# ===========================================================================
# SQLite OHLC CACHE  (same table layout as 1pivot.py)
# ===========================================================================

def _pivot_db_connect() -> sqlite3.Connection:
    """Open a WAL-mode SQLite connection to the shared pivot.db."""
    con = sqlite3.connect(_PIVOT_DB_PATH, timeout=10, check_same_thread=False)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=NORMAL")
    con.row_factory = sqlite3.Row
    return con


@contextmanager
def _pivot_db() -> "Generator[sqlite3.Connection, None, None]":
    """Write connection — commits on exit, rolls back on exception."""
    con = _pivot_db_connect()
    try:
        yield con
        con.commit()
    except Exception:
        con.rollback()
        raise
    finally:
        con.close()


@contextmanager
def _pivot_db_read() -> "Generator[sqlite3.Connection, None, None]":
    """Read-only connection — no commit."""
    con = _pivot_db_connect()
    try:
        yield con
    finally:
        con.close()


def _init_pivot_ohlc_cache() -> None:
    """Create pivot_cache table if it does not already exist.

    Identical schema to 1pivot.py so both can share the same pivot.db.
    """
    _PIVOT_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    try:
        with _pivot_db() as con:
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS pivot_cache (
                    pair         TEXT NOT NULL,
                    cache_key    TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    fetched_at   TEXT NOT NULL,
                    PRIMARY KEY (pair, cache_key)
                )
                """
            )
    except Exception as exc:
        log.warning("pivot: could not init SQLite pivot_cache table: %s", exc)


def _load_ohlc_cache_entry(pair: str) -> "Optional[Dict[str, Any]]":
    """Return the cached OHLC payload for *pair*, or None if absent."""
    try:
        with _pivot_db_read() as con:
            row = con.execute(
                "SELECT payload_json FROM pivot_cache "
                "WHERE pair=? AND cache_key='pivot_ohlc'",
                (pair.lower(),),
            ).fetchone()
        if row:
            return json.loads(row["payload_json"])
    except Exception as exc:
        log.warning("pivot: SQLite cache load failed pair=%s: %s", pair, exc)
    return None


def _save_ohlc_cache_entry(
    pair: str,
    session_date: str,
    ohlc: "Dict[str, float]",
    source: str,
) -> None:
    """Upsert an OHLC cache row for *pair* tagged with *session_date*."""
    now_iso = datetime.now(_tz.utc).isoformat().replace("+00:00", "Z")
    payload: "Dict[str, Any]" = {
        "session_date": session_date,
        "fetched_at":   now_iso,
        "ohlc":         ohlc,
        "source":       source,
    }
    try:
        with _pivot_db() as con:
            con.execute(
                "INSERT OR REPLACE INTO pivot_cache "
                "(pair, cache_key, payload_json, fetched_at) VALUES (?,?,?,?)",
                (
                    pair.lower(),
                    "pivot_ohlc",
                    json.dumps(payload, sort_keys=True),
                    now_iso,
                ),
            )
    except Exception as exc:
        log.warning("pivot: SQLite cache save failed pair=%s: %s", pair, exc)


# Initialise the cache table once at import time (no-op if already exists).
_init_pivot_ohlc_cache()


# ---------------------------------------------------------------------------
# NY Close pivot reset boundary
# ---------------------------------------------------------------------------
# Forex pivots reset at the New York Common Close: 5:00 PM America/New_York.
# This follows US daylight saving time automatically via zoneinfo.
_NY_CLOSE_TZ_NAME: str = os.environ.get("PIVOT_NY_CLOSE_TZ", "America/New_York")
_NY_CLOSE_HOUR: int = int(os.environ.get("PIVOT_NY_CLOSE_HOUR", "17"))
_NY_CLOSE_MINUTE: int = int(os.environ.get("PIVOT_NY_CLOSE_MINUTE", "0"))
_NY_CLOSE_UTC_HOUR: int = int(os.environ.get("PIVOT_NY_CLOSE_UTC_HOUR", "22"))


def _previous_fx_trading_date(date_obj):
    prev = date_obj - timedelta(days=1)
    while prev.weekday() >= 5:
        prev -= timedelta(days=1)
    return prev


def _pivot_session_date(now_utc: Optional[datetime] = None) -> str:
    now_utc = now_utc or datetime.now(_tz.utc)
    if now_utc.tzinfo is None:
        now_utc = now_utc.replace(tzinfo=_tz.utc)
    if ZoneInfo is not None:
        try:
            ny_tz = ZoneInfo(_NY_CLOSE_TZ_NAME)
            now_ny = now_utc.astimezone(ny_tz)
            close_today_ny = now_ny.replace(hour=_NY_CLOSE_HOUR, minute=_NY_CLOSE_MINUTE, second=0, microsecond=0)
            anchor_date = now_ny.date() if now_ny.weekday() < 5 and now_ny >= close_today_ny else _previous_fx_trading_date(now_ny.date())
            return anchor_date.isoformat()
        except Exception as exc:
            log.warning("pivot: timezone-aware NY close failed (%s); falling back to fixed UTC hour", exc)
    bar_date = now_utc.date() if now_utc.hour >= _NY_CLOSE_UTC_HOUR else (now_utc - timedelta(days=1)).date()
    while bar_date.weekday() >= 5:
        bar_date -= timedelta(days=1)
    return bar_date.isoformat()


def _components_are_fresh(component_file: Union[str, Path]) -> bool:
    """Return True if the component file exists and was modified within the TTL window."""
    try:
        mtime = Path(component_file).stat().st_mtime
        age = time.time() - mtime
        if age <= _COMPONENTS_TTL_SECONDS:
            return True
        log.debug(
            "pivot: component file age=%.0fs exceeds TTL=%ss -- will fall through to API",
            age,
            _COMPONENTS_TTL_SECONDS,
        )
        return False
    except OSError:
        return False

def _resolve_ohlc(
    pair: str,
    component_file: Union[str, Path] = DEFAULT_COMPONENT_FILE,
) -> Tuple[Optional[Dict[str, float]], str]:
    # 1. Component cache (written by scraper.py) — preferred, zero API cost.
    #    Only use it when the file is fresh (within TTL) so stale data doesn't
    #    silently mask a broken scraper run.
    if _components_are_fresh(component_file):
        ohlc = _fetch_components_ohlc(pair, component_file)
        if ohlc is not None:
            log.debug("pivot[%s]: using %s (fresh cache)", pair, SOURCE_COMPONENTS)
            return ohlc, SOURCE_COMPONENTS
        log.debug(
            "pivot[%s]: component file is fresh but pair data missing -- falling through to API",
            pair,
        )

    # 2. Stooq — free, no API key, yfinance-style via macro.StooqTicker.
    #    Tried before paid APIs so TwelveData quota is preserved.
    ohlc = _fetch_stooq_ohlc(pair)
    if ohlc is not None:
        log.debug("pivot[%s]: using %s (free OHLC)", pair, SOURCE_STOOQ)
        return ohlc, SOURCE_STOOQ

    # 3. TwelveData — paid API, only reached when Stooq returns nothing.
    ohlc = _fetch_twelvedata_ohlc(pair)
    if ohlc is not None:
        log.debug("pivot[%s]: using %s (API fallback)", pair, SOURCE_TD)
        return ohlc, SOURCE_TD

    # 4. Stale component file is better than nothing — try it unconditionally.
    ohlc = _fetch_components_ohlc(pair, component_file)
    if ohlc is not None:
        log.warning(
            "pivot[%s]: using stale component cache (all APIs failed)", pair
        )
        return ohlc, SOURCE_COMPONENTS

    return None, SOURCE_UNAVAILABLE


def _resolve_ohlc_cached(
    pair: str,
    component_file: Union[str, Path] = DEFAULT_COMPONENT_FILE,
) -> Tuple[Optional[Dict[str, float]], str]:
    """Cache-first OHLC resolver — mirrors fetch_previous_daily_ohlc() in 1pivot.py.

    On a session-date cache hit the REST/Stooq request is skipped entirely.
    At most **one** live fetch is made per pair per pivot session (typically
    24 hours), after which all calls in the same session are served from the
    SQLite pivot_cache table.

    Set PIVOT_FORCE_REFRESH=1 to bypass the cache and always re-fetch.
    """
    pair_l = pair.lower().strip()
    session_date = _pivot_session_date()

    if not PIVOT_FORCE_REFRESH:
        cached = _load_ohlc_cache_entry(pair_l)
        if cached and cached.get("session_date") == session_date:
            raw_ohlc = cached.get("ohlc")
            cached_source = str(cached.get("source", SOURCE_COMPONENTS))
            if isinstance(raw_ohlc, dict):
                ohlc = _clean_ohlc(raw_ohlc)
                if ohlc is not None:
                    log.info(
                        "pivot[%s]: SQLite OHLC cache hit session=%s source=%s",
                        pair_l, session_date, cached_source,
                    )
                    return ohlc, cached_source
            log.debug("pivot[%s]: cached OHLC invalid — re-fetching", pair_l)

    # Cache miss or stale — resolve live and persist the result.
    ohlc, source = _resolve_ohlc(pair_l, component_file)
    if ohlc is not None:
        _save_ohlc_cache_entry(pair_l, session_date, ohlc, source)
        log.info(
            "pivot[%s]: OHLC fetched live and cached session=%s source=%s",
            pair_l, session_date, source,
        )
    return ohlc, source


                                                                               
def daily_pivots(ohlc: Dict[str, float]) -> Dict[str, float]:
    clean = _clean_ohlc(ohlc)
    if clean is None:
        raise ValueError("invalid OHLC payload for daily_pivots")

    h, l, c = clean["high"], clean["low"], clean["close"]
    rng = h - l

    pp = (h + l + c) / 3.0
    r1 = 2 * pp - l
    s1 = 2 * pp - h
    r2 = pp + rng
    s2 = pp - rng
    r3 = h + 2 * (pp - l)
    s3 = l - 2 * (h - pp)

    return {
        "PP": round(pp, 6),
        "R1": round(r1, 6),
        "R2": round(r2, 6),
        "R3": round(r3, 6),
        "S1": round(s1, 6),
        "S2": round(s2, 6),
        "S3": round(s3, 6),
        "range": round(rng, 6),
        "close": round(c, 6),
    }

                                                                               
def _resolve_conviction(alignment: str, state: str) -> float:
    if state == "rejection":
        return _CONV_REJECTION
    if state == "balance":
        return _CONV_BALANCE

    if alignment == "with":
        if state in ("breakout_up", "breakout_down"):
            return _CONV_WITH_BREAKOUT
        return _CONV_WITH_ACCEPT

    if alignment == "against":
        if state in ("breakout_up", "breakout_down"):
            return _CONV_AGAINST_BREAKOUT
        return _CONV_AGAINST_ACCEPT

    return _CONV_NEUTRAL_MACRO

                                                                               
def classify_price_structure(
    pivots: Dict[str, float],
    macro_score: float,
    current_price: Optional[float] = None,
    atr5: Optional[float] = None,
    session_high: Optional[float] = None,
    session_low: Optional[float] = None,
) -> Dict[str, Union[str, float, Dict[str, Union[str, float]]]]:

    c = float(current_price) if current_price is not None else float(pivots["close"])
    pp = float(pivots["PP"])
    r1 = float(pivots["R1"])
    r2 = float(pivots["R2"])
    r3 = float(pivots["R3"])
    s1 = float(pivots["S1"])
    s2 = float(pivots["S2"])
    s3 = float(pivots["S3"])
    rng = max(float(pivots["range"]), 0.0)

    raw_tol = rng * BREAK_TOL
    if atr5 is not None and atr5 > 0:
        atr_cap = float(atr5) * ATR_CAP_MULT
        tol = min(raw_tol, atr_cap)
    else:
        tol = raw_tol

    bal = rng * BALANCE_BAND

    # Rejection checks run FIRST so a session-tagged level that price has
    # since pulled back from is not mis-classified as a live breakout/accept.
    # Upper rejections: session_high touched a resistance level, price retreated.
    # Lower rejections: session_low touched a support level, price bounced.
    rejection_level: str = ""

    if session_high is not None:
        if session_high >= r3 and r2 < c < r3:
            rejection_level = "R3"
        elif session_high >= r2 and r1 < c < r2:
            rejection_level = "R2"
        elif session_high >= r1 and pp < c < r1:
            rejection_level = "R1"

    if not rejection_level and session_low is not None:
        if session_low <= s3 and s3 < c < s2:
            rejection_level = "S3"
        elif session_low <= s2 and s2 < c < s1:
            rejection_level = "S2"
        elif session_low <= s1 and s1 < c < pp:
            rejection_level = "S1"

    if rejection_level:
        state = "rejection"
    elif c > r1 + tol:
        state = "breakout_up"
    elif c < s1 - tol:
        state = "breakout_down"
    elif c > pp + bal:
        state = "accept_above_pp"
    elif c < pp - bal:
        state = "accept_below_pp"
    else:
        state = "balance"

                                                  
    if abs(macro_score) < NEUTRAL_MACRO_THRESHOLD:
        alignment = "neutral"
    else:
        macro_dir = "bullish" if macro_score > 0 else "bearish"
        bullish_states = {"breakout_up", "accept_above_pp"}
        bearish_states = {"breakout_down", "accept_below_pp"}

        if state in bullish_states and macro_dir == "bullish":
            alignment = "with"
        elif state in bearish_states and macro_dir == "bearish":
            alignment = "with"
        elif state in bullish_states and macro_dir == "bearish":
            alignment = "against"
        elif state in bearish_states and macro_dir == "bullish":
            alignment = "against"
        else:
            alignment = "neutral"

    conviction_mult = _resolve_conviction(alignment, state)

                                       
    approx_adj = macro_score * (conviction_mult - 1.0)
    score_adj = round(max(min(approx_adj, 0.3), -0.3), 3)

    named_levels = {
        "PP": pp,
        "R1": r1,
        "R2": r2,
        "R3": r3,
        "S1": s1,
        "S2": s2,
        "S3": s3,
    }
    nearest_name = min(named_levels, key=lambda lvl: abs(c - named_levels[lvl]))
    nearest_val = named_levels[nearest_name]

    has_current = current_price is not None
    has_rejection_evidence = session_high is not None or session_low is not None
    if has_current and has_rejection_evidence:
        state_quality = "high"
    elif has_current:
        state_quality = "medium"
    else:
        state_quality = "low"
    state_basis = "live" if has_current else "prior_close"

    return {
        "price_state": state,
        "rejection_level": rejection_level,
        "macro_alignment": alignment,
        "conviction_mult": round(conviction_mult, 3),
        "score_adj": score_adj,
        "price_used": round(c, 6),
        "tolerance": round(tol, 6),
        "state_basis": state_basis,
        "state_quality": state_quality,
        "nearest_level": {
            "name": nearest_name,
            "value": round(nearest_val, 6),
            "distance": round(abs(c - nearest_val), 6),
            "side": "above" if c > nearest_val else "below" if c < nearest_val else "at",
        },
        "PP": pivots["PP"],
        "R1": pivots["R1"],
        "R2": pivots["R2"],
        "R3": pivots["R3"],
        "S1": pivots["S1"],
        "S2": pivots["S2"],
        "S3": pivots["S3"],
        "range": pivots["range"],
        "close": pivots["close"],
        "macro_score": round(float(macro_score), 3),
    }

                                                                               
# ---------------------------------------------------------------------------
# pivot_levels.json writer
# ---------------------------------------------------------------------------

def write_pivot_levels_json(
    results: Dict[str, Dict],
    scrape_time: Optional[str] = None,
    output_path: Union[str, Path, None] = None,
) -> None:
    """Write pre-computed pivot levels to disk for frontend consumption.

    Produces a ``pivot_levels.json`` that pivots.html reads as its primary
    source, eliminating client-side OHLC recalculation and making
    ``price_state`` / ``nearest_level`` authoritative.

    The file is written atomically (tmp → os.replace) so the frontend never
    reads a partial write.

    Env var ``PIVOT_LEVELS_FILE`` overrides the default output path
    (``<macro_components dir>/pivot_levels.json``).

    Args:
        results:     First return value of ``fetch_price_structure()``.
        scrape_time: ISO-8601 timestamp (defaults to UTC now).
        output_path: Override path; falls back to ``PIVOT_LEVELS_FILE``.
    """
    path = Path(output_path) if output_path else PIVOT_LEVELS_FILE
    now_iso = scrape_time or (
        datetime.now(_tz.utc).isoformat().replace("+00:00", "Z")
    )
    payload: Dict[str, Any] = {
        "generated_at": now_iso,
        "pairs": results,
    }
    tmp = path.with_suffix(".tmp")
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, indent=2)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, path)
        try:
            os.chmod(path, 0o644)
        except OSError:
            pass
        log.info("pivot: wrote pivot_levels.json — %d pair(s) → %s", len(results), path)
    except OSError as exc:
        log.warning("pivot: failed to write pivot_levels.json: %s", exc)
        try:
            Path(str(tmp)).unlink(missing_ok=True)
        except OSError:
            pass


def fetch_price_structure(
    pairs: Optional[List[str]] = None,
    macro_scores: Optional[Dict[str, float]] = None,
    component_file: Union[str, Path] = DEFAULT_COMPONENT_FILE,
    ohlc_map: Optional[Dict[str, Dict[str, float]]] = None,
) -> Tuple[Dict[str, Dict], Dict[str, str]]:
    pairs = pairs or _active_main_pairs()
    macro_scores = macro_scores or {}
    ohlc_map = ohlc_map or {}

    results: Dict[str, Dict] = {}
    sources: Dict[str, str] = {}

    for pair in pairs:
        pair_l = pair.lower().strip()
        macro_score = float(macro_scores.get(pair_l, 0.0))

        injected = ohlc_map.get(pair_l)
        if injected is not None:
            ohlc = _clean_ohlc(injected)
            source = SOURCE_INJECTED if ohlc is not None else SOURCE_UNAVAILABLE
        else:
            ohlc, source = _resolve_ohlc_cached(pair_l, component_file)

        if ohlc is None:
            results[pair_l] = _unavailable_result(macro_score)
            sources[pair_l] = SOURCE_UNAVAILABLE
            continue

        pivots = daily_pivots(ohlc)
        live = _normalize_live_context(ohlc)

        result = classify_price_structure(
            pivots=pivots,
            macro_score=macro_score,
            current_price=live["current_price"],
            atr5=live["atr5"],
            session_high=live["session_high"],
            session_low=live["session_low"],
        )
        result["ohlc_source"] = source
        result["ohlc_date"] = ohlc.get("_date", "")
        _apply_rsi_to_result(result, ohlc)

        if result["state_basis"] == "prior_close":
            log.debug(
                "pivot[%s]: using prior-close-only structure (source=%s date=%s)",
                pair_l,
                source,
                result["ohlc_date"],
            )

        log.debug(
            "pivot[%s]: state=%s quality=%s alignment=%s conv=%.2f source=%s price=%s PP=%s",
            pair_l,
            result["price_state"],
            result["state_quality"],
            result["macro_alignment"],
            result["conviction_mult"],
            source,
            result["price_used"],
            result["PP"],
        )

        results[pair_l] = result
        sources[pair_l] = source

    # Log a single INFO summary so production logs show which source served each pair.
    if results:
        src_summary = "  ".join(
            f"{p}={sources.get(p, 'unavailable')}"
            for p in pairs
        )
        log.info("pivot: OHLC sources — %s", src_summary)

    # Fire Telegram alerts. Reset alerts run first; normal price-state alerts run after.
    # Optional Groq AI enrichment is fail-soft.
    if TELEGRAM_BOT_TOKEN:
        try:
            n_reset = dispatch_pivot_reset_alerts(results)
            if n_reset:
                log.info("pivot reset alerts: %d alert(s) dispatched", n_reset)
        except RuntimeError as _alert_exc:
            log.warning("pivot reset alerts: dispatch skipped: %s", _alert_exc)
        except Exception as _alert_exc:
            log.warning("pivot reset alerts: unexpected error during dispatch: %s", _alert_exc)
        try:
            n = dispatch_pivot_alerts(results)
            if n:
                log.info("pivot alerts: %d alert(s) dispatched", n)
        except RuntimeError as _alert_exc:
            log.warning("pivot alerts: dispatch skipped: %s", _alert_exc)
        except Exception as _alert_exc:
            log.warning("pivot alerts: unexpected error during dispatch: %s", _alert_exc)

    # Persist pre-computed levels for frontend consumption (pivots.html).
    # Non-fatal: a write failure never aborts the scraper run.
    try:
        write_pivot_levels_json(results)
    except Exception as _write_exc:
        log.warning("pivot: write_pivot_levels_json raised unexpectedly: %s", _write_exc)

    return results, sources

def fetch_price_context(
    pair: str,
    macro_score: float,
    component_file: Union[str, Path] = DEFAULT_COMPONENT_FILE,
    ohlc: Optional[Dict[str, float]] = None,
) -> Optional[Dict[str, Union[str, float, Dict[str, Union[str, float]]]]]:
    pair_l = pair.lower().strip()

    if ohlc is not None:
        clean_ohlc = _clean_ohlc(ohlc)
        source = SOURCE_INJECTED if clean_ohlc is not None else SOURCE_UNAVAILABLE
    else:
        clean_ohlc, source = _resolve_ohlc_cached(pair_l, component_file)

    if clean_ohlc is None:
        result = _unavailable_result(macro_score)
        result["ohlc_source"] = SOURCE_UNAVAILABLE
        return result

    pivots = daily_pivots(clean_ohlc)
    live = _normalize_live_context(clean_ohlc)
    result = classify_price_structure(
        pivots=pivots,
        macro_score=float(macro_score),
        current_price=live["current_price"],
        atr5=live["atr5"],
        session_high=live["session_high"],
        session_low=live["session_low"],
    )
    result["ohlc_source"] = source
    result["ohlc_date"] = clean_ohlc.get("_date", "")
    _apply_rsi_to_result(result, clean_ohlc)
    return result

# ===========================================================================
# Telegram alert system
# ===========================================================================
# Environment variables (all optional):
#
#   TELEGRAM_BOT_TOKEN   Telegram bot token.  Leave unset to disable.
#   TELEGRAM_CHAT_ID     Target chat ID (integer).  Required when alerts
#                              are enabled; validated lazily so the module can
#                              be imported and used without it.
#   PIVOT_ALERT_STATE_FILE     Path to the de-duplication state JSON file.
#                              Default: pivot_telegram_alerts.json in the same
#                              directory as DEFAULT_COMPONENT_FILE.
#   PIVOT_ALERT_MIN_QUALITY    Minimum state_quality to trigger an alert.
#                              One of: "low", "medium", "high".  Default "medium".
#   PIVOT_ALERT_MIN_CONVICTION Minimum conviction_mult to trigger an alert.
#                              Default 0.80 — suppresses pure balance/neutral noise.
#   PIVOT_ALERT_STATE_PRUNE_H  Hours after which a de-dup entry expires.
#                              Default 26 (slightly over one trading day).
# ===========================================================================

_UTC = _tz.utc

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
TELEGRAM_BOT_TOKEN: str = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
_chat_id_raw: str       = os.environ.get("TELEGRAM_CHAT_ID",   "").strip()
TELEGRAM_CHAT_ID: int   = 0   # 0 = unconfigured; validated lazily before use

_alert_state_dir = DEFAULT_COMPONENT_FILE.parent
PIVOT_ALERT_STATE_FILE = Path(
    os.environ.get(
        "PIVOT_ALERT_STATE_FILE",
        str(_alert_state_dir / "pivot_telegram_alerts.json"),
    )
)

PIVOT_RESET_ALERT_STATE_FILE = Path(
    os.environ.get("PIVOT_RESET_ALERT_STATE_FILE", str(_alert_state_dir / "pivot_reset_telegram_alerts.json"))
)
PIVOT_RESET_ALERTS_ENABLED: bool = os.environ.get("PIVOT_RESET_ALERTS", "1").strip().lower() not in ("0", "false", "no", "off")

# Optional Groq AI enrichment for Telegram pivot/reset alerts.
# Fail-soft: if AI credentials/API/network fail, the normal Telegram alert still sends.
GROQ_AI_ENABLED: bool = os.environ.get("GROQ_AI_ENABLED", "0").strip().lower() in ("1", "true", "yes", "on")
GROQ_API_KEY: str = os.environ.get("GROQ_API_KEY", "").strip()
GROQ_AI_MODEL: str = os.environ.get("GROQ_AI_MODEL", "llama-3.1-8b-instant").strip()
GROQ_AI_TIMEOUT: int = int(os.environ.get("GROQ_AI_TIMEOUT", "12"))
GROQ_AI_MAX_CHARS: int = int(os.environ.get("GROQ_AI_MAX_CHARS", "650"))
# Max Groq AI calls per workflow run (default 3) to stay inside free-tier limits.
GROQ_AI_MAX_PER_RUN: int = max(1, int(os.environ.get("GROQ_AI_MAX_PER_RUN", "3")))
# How long (minutes) to back off after receiving a 429 from Groq AI.
GROQ_AI_COOLDOWN_MINUTES: int = max(1, int(os.environ.get("GROQ_AI_COOLDOWN_MINUTES", "30")))
# Persistent Groq cooldown state file — shared with signal_confirm.py and momentum.py.
# Priority:
# 1) GROQ_AI_STATE_FILE explicit override
# 2) same directory as SIGNAL_ALERT_STATE_FILE (signal_confirm.py default)
# 3) DEFAULT_COMPONENT_FILE parent / groq_ai_state.json
_CF_AI_STATE_FILE = Path(
    os.environ.get(
        "GROQ_AI_STATE_FILE",
        str(Path(os.environ.get("SIGNAL_ALERT_STATE_FILE", str(DEFAULT_COMPONENT_FILE.parent / "signal_telegram_alerts.json"))).parent / "groq_ai_state.json"),
    )
)
_cf_ai_calls_this_run: int = 0  # module-level per-run counter


def _cf_ai_load_state() -> Dict[str, Any]:
    try:
        with open(_CF_AI_STATE_FILE, encoding="utf-8") as _f:
            return json.load(_f)
    except Exception:
        return {}


def _cf_ai_save_state(data: Dict[str, Any]) -> None:
    try:
        _CF_AI_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        tmp = _CF_AI_STATE_FILE.with_suffix(".tmp")
        with open(tmp, "w", encoding="utf-8") as _f:
            json.dump(data, _f)
            _f.flush(); os.fsync(_f.fileno())
        os.replace(tmp, _CF_AI_STATE_FILE)
    except Exception as _exc:
        log.warning("pivot: could not save Groq AI state file: %s", _exc)


def _cf_ai_can_call() -> bool:
    """Return True if a Groq AI call is allowed (per-run cap + cooldown check)."""
    if _cf_ai_calls_this_run >= GROQ_AI_MAX_PER_RUN:
        log.debug(
            "pivot: Groq AI per-run cap reached (%d/%d) — skipping",
            _cf_ai_calls_this_run, GROQ_AI_MAX_PER_RUN,
        )
        return False
    state = _cf_ai_load_state()
    cooldown_until = state.get("cooldown_until")
    if cooldown_until:
        try:
            _cu = datetime.fromisoformat(cooldown_until.replace("Z", "+00:00"))
            if datetime.now(_tz.utc) < _cu:
                log.warning(
                    "pivot: Groq AI cooldown active until %s — skipping", cooldown_until
                )
                return False
        except Exception:
            pass
    return True


def _cf_ai_mark_429() -> None:
    """Set a cooldown after receiving a 429 from Groq AI."""
    until = (
        datetime.now(_tz.utc) + timedelta(minutes=GROQ_AI_COOLDOWN_MINUTES)
    ).strftime("%Y-%m-%dT%H:%M:%SZ")
    _cf_ai_save_state({"cooldown_until": until})
    log.warning(
        "pivot: Groq AI 429 — cooldown set for %d min (until %s)",
        GROQ_AI_COOLDOWN_MINUTES, until,
    )


def _cf_ai_mark_call() -> None:
    global _cf_ai_calls_this_run
    _cf_ai_calls_this_run += 1


# Dedicated session for Groq AI — does NOT retry on 429 to avoid
# amplifying rate-limit errors the way the shared session would.
# Cached as a module-level singleton to reuse connection pooling.
_cf_ai_session_instance: Optional[requests.Session] = None

def _cf_ai_session() -> requests.Session:
    global _cf_ai_session_instance
    if _cf_ai_session_instance is None:
        s = requests.Session()
        s.headers.update({"User-Agent": _USER_AGENT})
        s.mount(
            "https://",
            HTTPAdapter(
                max_retries=Retry(
                    total=1,
                    backoff_factor=0.5,
                    status_forcelist=[500, 502, 503, 504],  # intentionally excludes 429
                    allowed_methods=frozenset({"POST"}),
                )
            ),
        )
        _cf_ai_session_instance = s
    return _cf_ai_session_instance

_QUALITY_RANK: Dict[str, int] = {"none": 0, "low": 1, "medium": 2, "high": 3}
_raw_min_quality = os.environ.get("PIVOT_ALERT_MIN_QUALITY", "medium").strip().lower()
PIVOT_ALERT_MIN_QUALITY: int = _QUALITY_RANK.get(_raw_min_quality, _QUALITY_RANK["medium"])

PIVOT_ALERT_MIN_CONVICTION: float = _env_float("PIVOT_ALERT_MIN_CONVICTION", 0.80, minimum=0.0)
PIVOT_ALERT_STATE_PRUNE_H: int = max(
    1, int(_env_float("PIVOT_ALERT_STATE_PRUNE_H", 26.0, minimum=1.0))
)

# States that can trigger an alert, and their minimum required alignment.
# "any" means the state fires regardless of macro_alignment.
# "with_or_neutral" means fires unless macro is explicitly against.
_ALERTABLE_STATES: Dict[str, str] = {
    "breakout_up":      "any",
    "breakout_down":    "any",
    "rejection":        "any",
    "accept_above_pp":  "with_or_neutral",
    "accept_below_pp":  "with_or_neutral",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _resolve_chat_id(raw: str) -> int:
    """Parse and validate TELEGRAM_CHAT_ID.  Raises RuntimeError on bad input.

    Called lazily (not at import time) so the module can be imported and used
    without setting the env var when Telegram alerts are not needed.
    """
    if not raw:
        raise RuntimeError(
            "TELEGRAM_CHAT_ID environment variable is required when pivot "
            "Telegram alerts are enabled.  Set it to your chat ID (integer)."
        )
    try:
        return int(raw)
    except ValueError:
        raise RuntimeError(
            f"TELEGRAM_CHAT_ID must be an integer, got: {raw!r}"
        )


try:
    TELEGRAM_CHAT_ID = _resolve_chat_id(_chat_id_raw) if _chat_id_raw else 0
except RuntimeError:
    TELEGRAM_CHAT_ID = 0  # surfaced at dispatch time


def _pivot_format_price(pair: str, value: float) -> str:
    """Format a price with pair-appropriate decimal places."""
    p = pair.lower()
    if "xau" in p:
        return f"{value:.2f}"
    return f"{value:.5f}"


def _pivot_now_utc() -> datetime:
    return datetime.now(_UTC)


def _pivot_iso(dt: datetime) -> str:
    return dt.astimezone(_UTC).isoformat().replace("+00:00", "Z")


def _pivot_parse_dt(value: Any) -> Optional[datetime]:
    if not value:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=_UTC)
    try:
        text = str(value).strip().replace("Z", "+00:00")
        dt = datetime.fromisoformat(text)
        return dt if dt.tzinfo else dt.replace(tzinfo=_UTC)
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# State (de-duplication) persistence
# ---------------------------------------------------------------------------

def _pivot_load_alert_state() -> Dict[str, str]:
    """Load de-dup state from disk.  Keys are fingerprints; values are ISO timestamps."""
    try:
        with open(PIVOT_ALERT_STATE_FILE, encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except FileNotFoundError:
        return {}
    except (json.JSONDecodeError, OSError, ValueError) as exc:
        log.warning("pivot alerts: failed to load state file: %s", exc)
        return {}


def _pivot_save_alert_state(state: Dict[str, str]) -> None:
    """Persist de-dup state atomically."""
    tmp = PIVOT_ALERT_STATE_FILE.with_suffix(".tmp")
    try:
        PIVOT_ALERT_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, PIVOT_ALERT_STATE_FILE)
    except OSError as exc:
        log.warning("pivot alerts: failed to save state file: %s", exc)
        try:
            tmp.unlink(missing_ok=True)
        except OSError:
            pass


def _pivot_prune_alert_state(state: Dict[str, str]) -> Dict[str, str]:
    """Drop entries older than PIVOT_ALERT_STATE_PRUNE_H hours."""
    cutoff = _pivot_now_utc() - timedelta(hours=PIVOT_ALERT_STATE_PRUNE_H)
    pruned: Dict[str, str] = {}
    for key, val in state.items():
        ts = _pivot_parse_dt(val)
        if ts is not None and ts >= cutoff:
            pruned[key] = val
        else:
            log.debug("pivot alerts: pruning stale key=%s", key)
    return pruned


# ---------------------------------------------------------------------------
# Telegram send
# ---------------------------------------------------------------------------

def _pivot_send_telegram(
    bot_token: str,
    chat_id: int,
    text: str,
    timeout: int = 10,
) -> bool:
    """POST a Telegram message.  Returns True on success."""
    if not bot_token:
        log.debug("pivot alerts: no bot token — skipping send")
        return False
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    try:
        s = _get_session()
        resp = s.post(
            url,
            json={"chat_id": chat_id, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True},
            timeout=timeout,
        )
        if resp.status_code == 200:
            log.debug("pivot alerts: sent OK (chat_id=%s)", chat_id)
            return True
        log.warning(
            "pivot alerts: Telegram HTTP %s — %s",
            resp.status_code,
            resp.text[:200],
        )
        return False
    except requests.exceptions.RequestException as exc:
        log.warning("pivot alerts: send failed: %s", exc)
        return False


# ---------------------------------------------------------------------------
# Groq AI enrichment
# ---------------------------------------------------------------------------

def _telegram_html_escape(value: Any) -> str:
    return str(value).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _pivot_groq_ai_enabled() -> bool:
    return bool(GROQ_AI_ENABLED and GROQ_API_KEY and GROQ_AI_MODEL)


def _pivot_groq_ai_prompt(pair: str, result: Dict, alert_label: str, context: str = "pivot alert") -> str:
    nearest = result.get("nearest_level", {}) or {}
    return "\n".join([
        "You are a concise FX pivot-market assistant for Telegram alerts.",
        "Return exactly 2 short bullets only. No trade command. No guarantee. No markdown table.",
        "Focus on pivot context, macro alignment, and what level matters next.",
        f"Context: {context}", f"Pair: {pair.upper()}", f"Alert: {alert_label}",
        f"Price state: {result.get('price_state', '')}", f"State quality: {result.get('state_quality', '')}",
        f"Macro alignment: {result.get('macro_alignment', '')}", f"Macro score: {result.get('macro_score', '')}",
        f"Conviction multiplier: {result.get('conviction_mult', '')}", f"Basis: {result.get('state_basis', '')}",
        f"Price: {result.get('price_used', result.get('close', ''))}", f"PP: {result.get('PP', '')}",
        f"R1/R2/R3: {result.get('R1', '')}/{result.get('R2', '')}/{result.get('R3', '')}",
        f"S1/S2/S3: {result.get('S1', '')}/{result.get('S2', '')}/{result.get('S3', '')}",
        f"Nearest level: {nearest.get('name', '')} {nearest.get('value', nearest.get('price', ''))}",
        "Style: punchy, trader-friendly, max 45 words total.",
    ])


def _pivot_groq_ai_note(pair: str, result: Dict, alert_label: str, context: str = "pivot alert") -> str:
    if not _pivot_groq_ai_enabled():
        return ""
    # Check per-run cap and persistent cooldown before making any network call.
    if not _cf_ai_can_call():
        return ""
    url = "https://api.groq.com/openai/v1/chat/completions"
    payload = {
        "model": GROQ_AI_MODEL,
        "messages": [{"role": "user", "content": _pivot_groq_ai_prompt(pair, result, alert_label, context)}],
        "max_tokens": 150,
    }
    headers = {"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"}
    try:
        # Use a dedicated session that does NOT retry on 429.
        resp = _cf_ai_session().post(url, headers=headers, json=payload, timeout=GROQ_AI_TIMEOUT)
        if resp.status_code == 429:
            _cf_ai_mark_429()
            return ""
        if resp.status_code != 200:
            log.warning("pivot alerts: Groq AI HTTP %s — %s", resp.status_code, resp.text[:200])
            return ""
        _cf_ai_mark_call()
        data = resp.json()
        choices = data.get("choices") or []
        ai_text = choices[0].get("message", {}).get("content", "") if choices else ""
        ai_text = " ".join(str(ai_text).strip().split())[:GROQ_AI_MAX_CHARS]
        return _telegram_html_escape(ai_text) if ai_text else ""
    except Exception as exc:
        log.warning("pivot alerts: Groq AI enrichment skipped: %s", exc)
        return ""


# ---------------------------------------------------------------------------
# Alert construction
# ---------------------------------------------------------------------------

_STATE_EMOJI: Dict[str, str] = {
    "breakout_up":      "⚡",
    "breakout_down":    "⚡",
    "rejection":        "↩️",
    "accept_above_pp":  "🔼",
    "accept_below_pp":  "🔽",
}

_STATE_LABEL: Dict[str, str] = {
    "breakout_up":      "BREAKOUT UP",
    "breakout_down":    "BREAKOUT DOWN",
    "rejection":        "REJECTION",
    "accept_above_pp":  "ACCEPT ABOVE PP",
    "accept_below_pp":  "ACCEPT BELOW PP",
}

_ALIGNMENT_TAG: Dict[str, str] = {
    "with":        "✅ With Macro",
    "against":     "⚠️ Against Macro",
    "neutral":     "➖ Macro Neutral",
    "unavailable": "❓ Macro Unavailable",
}

# Flag emoji(s) for each supported pair.
# For FX: base-currency flag first, quote-currency flag second.
# For commodities/crypto: a single representative icon.
_PAIR_FLAG: Dict[str, str] = {
    # Major FX
    "eurusd":  "🇪🇺🇺🇸",
    "gbpusd":  "🇬🇧🇺🇸",
    "usdchf":  "🇺🇸🇨🇭",
    "audusd":  "🇦🇺🇺🇸",
    "nzdusd":  "🇳🇿🇺🇸",
    "usdcad":  "🇺🇸🇨🇦",
    # Minor / cross FX
    "eurgbp":  "🇪🇺🇬🇧",
    "eurchf":  "🇪🇺🇨🇭",
    "gbpchf":  "🇬🇧🇨🇭",
    "audnzd":  "🇦🇺🇳🇿",
    "audcad":  "🇦🇺🇨🇦",
    "audchf":  "🇦🇺🇨🇭",
    "nzdcad":  "🇳🇿🇨🇦",
    "nzdchf":  "🇳🇿🇨🇭",
    "cadchf":  "🇨🇦🇨🇭",
    "gbpaud":  "🇬🇧🇦🇺",
    "gbpcad":  "🇬🇧🇨🇦",
    "gbpnzd":  "🇬🇧🇳🇿",
    "eurcad":  "🇪🇺🇨🇦",
    "euraud":  "🇪🇺🇦🇺",
    "eurnzd":  "🇪🇺🇳🇿",
    # MYR pairs
    "usdmyr":  "🇺🇸🇲🇾",
    "eurmyr":  "🇪🇺🇲🇾",
    "gbpmyr":  "🇬🇧🇲🇾",
    "audmyr":  "🇦🇺🇲🇾",
    "nzdmyr":  "🇳🇿🇲🇾",
    "cadmyr":  "🇨🇦🇲🇾",
    "chfmyr":  "🇨🇭🇲🇾",
    "sgdmyr":  "🇸🇬🇲🇾",
    # SGD pairs
    "usdsgd":  "🇺🇸🇸🇬",
    # Commodities & crypto
    "xauusd":  "🥇",        # Gold
    "xagusd":  "🥈",        # Silver
    "xptusd":  "⚪",        # Platinum
    "usoil":   "🛢️",        # WTI Crude
    "ukoil":   "🛢️",        # Brent Crude
    "btcusd":  "₿",         # Bitcoin
    "ethusd":  "🔷",        # Ethereum
    "btcusdt": "₿",
}


def _pivot_quality_ok(result: Dict) -> bool:
    """Return True if state_quality meets the minimum threshold."""
    quality = result.get("state_quality", "none")
    return _QUALITY_RANK.get(quality, 0) >= PIVOT_ALERT_MIN_QUALITY


def _pivot_alignment_ok(state: str, alignment: str) -> bool:
    """Return True if the macro alignment is acceptable for this state."""
    rule = _ALERTABLE_STATES.get(state)
    if rule is None:
        return False
    if rule == "any":
        return True
    if rule == "with_or_neutral":
        return alignment in ("with", "neutral")
    return False


def _pivot_signal_price_ladder(
    pair: str,
    price: float,
    levels: Dict[str, float],
    live_label: str = "Live Price",
) -> str:
    """Build Signal Confirm style daily pivot ladder text.

    Returned text is plain mono text and should be wrapped once using:
      <code>{_telegram_html_escape(ladder)}</code>
    """
    fp = _pivot_format_price

    def _num(value: Any) -> Optional[float]:
        try:
            v = float(value)
            if v <= 0.0:
                return None
            return v
        except (TypeError, ValueError):
            return None

    named: List[tuple[str, float]] = []
    for key in ("R3", "R2", "R1", "PP", "S1", "S2", "S3"):
        value = _num(levels.get(key) if key in levels else levels.get(key.lower()))
        if value is not None:
            named.append((key, value))

    price_value = _num(price)
    price_inserted = False
    rows: List[str] = []

    for label, value in named:
        if price_value is not None and not price_inserted and price_value >= value:
            rows.append(f"  ►  {fp(pair, price_value):<13} ← {live_label}")
            price_inserted = True
        rows.append(f"  {label:<3} {fp(pair, value)}")

    if price_value is not None and not price_inserted:
        rows.append(f"  ►  {fp(pair, price_value):<13} ← {live_label}")

    return "\n".join(rows)

def build_pivot_alerts(pair: str, result: Dict) -> List[Dict[str, str]]:
    """Inspect a classify_price_structure result and return alert dicts.

    Each dict has:
      - ``key``   : stable de-dup fingerprint  (pair + state + ohlc_date)
      - ``text``  : HTML-formatted Telegram message
      - ``level`` : "breakout" | "rejection" | "accept"
    """
    alerts: List[Dict[str, str]] = []

    state     = result.get("price_state", "")
    alignment = result.get("macro_alignment", "neutral")
    quality   = result.get("state_quality", "none")
    conv      = float(result.get("conviction_mult", 0.0))
    ohlc_date = result.get("ohlc_date", "")
    basis     = result.get("state_basis", "")

    # Gate: state must be alertable
    if state not in _ALERTABLE_STATES:
        return alerts

    # Gate: quality threshold
    if not _pivot_quality_ok(result):
        log.debug(
            "pivot alerts[%s]: skipping %s — quality=%s below threshold",
            pair, state, quality,
        )
        return alerts

    # Gate: alignment rule for this state
    if not _pivot_alignment_ok(state, alignment):
        log.debug(
            "pivot alerts[%s]: skipping %s — alignment=%s not permitted for this state",
            pair, state, alignment,
        )
        return alerts

    # Gate: conviction threshold (avoids spamming on noise near PP)
    if conv < PIVOT_ALERT_MIN_CONVICTION:
        log.debug(
            "pivot alerts[%s]: skipping %s — conviction=%.3f below threshold %.3f",
            pair, state, conv, PIVOT_ALERT_MIN_CONVICTION,
        )
        return alerts

    price  = float(result.get("price_used", 0.0))
    pp     = float(result.get("PP",    0.0))
    r1     = float(result.get("R1",    0.0))
    r2     = float(result.get("R2",    0.0))
    r3     = float(result.get("R3",    0.0))
    s1     = float(result.get("S1",    0.0))
    s2     = float(result.get("S2",    0.0))
    s3     = float(result.get("S3",    0.0))
    mscore = float(result.get("macro_score", 0.0))

    emoji = _STATE_EMOJI.get(state, "🔔")
    # Pair flag(s) — falls back to empty string for unlisted pairs.
    pair_flag = _PAIR_FLAG.get(pair.lower(), "")

    # For breakout states, compute the highest resistance / lowest support
    # level that price has actually cleared, and embed it in the label.
    if state == "breakout_up":
        if r3 > 0 and price > r3:
            broken_level = "R3"
        elif r2 > 0 and price > r2:
            broken_level = "R2"
        else:
            broken_level = "R1"
        label = f"BREAKOUT ABOVE {broken_level}"
    elif state == "breakout_down":
        if s3 > 0 and price < s3:
            broken_level = "S3"
        elif s2 > 0 and price < s2:
            broken_level = "S2"
        else:
            broken_level = "S1"
        label = f"BREAKOUT BELOW {broken_level}"
    elif state == "rejection":
        # rejection_level is set authoritatively by classify_price_structure.
        rej_level = result.get("rejection_level", "")
        label = f"REJECTION AT {rej_level}" if rej_level else "REJECTION"
    else:
        label = _STATE_LABEL.get(state, state.upper())

    align_tag = _ALIGNMENT_TAG.get(alignment, alignment)
    basis_tag = "Live" if basis == "live" else "Prior Close"

    # Always format with explicit sign so 0.0 shows as "+0.000" rather than
    # the inconsistent bare "0.000" that the old "if mscore" branch produced.
    # (if mscore) is False for 0.0, which is a valid, meaningful macro score.
    mscore_str = f"{mscore:+.3f}"

    # RSI context for Telegram alert. RSI is optional: keep the alert readable
    # even when historical closes are unavailable or the RSI calculation failed.
    rsi_raw = result.get("rsi")
    try:
        rsi_val = float(rsi_raw) if rsi_raw is not None else None
    except (TypeError, ValueError):
        rsi_val = None

    try:
        rsi_period = int(float(result.get("rsi_period", RSI_PERIOD) or RSI_PERIOD))
    except (TypeError, ValueError):
        rsi_period = RSI_PERIOD

    rsi_state = str(result.get("rsi_state", "unavailable") or "unavailable")
    rsi_bias = str(result.get("rsi_bias", "unavailable") or "unavailable")
    rsi_alignment = str(result.get("rsi_alignment", "unavailable") or "unavailable")

    if rsi_val is None:
        rsi_value_str = "n/a"
        rsi_signal_str = "Unavailable"
    else:
        rsi_value_str = f"{rsi_val:.2f} ({rsi_period})"
        rsi_signal_str = f"{rsi_bias.title()} / {rsi_state.title()}"

    rsi_alignment_str = rsi_alignment.title()
    try:
        combined_conv = float(result.get("combined_conviction_mult", conv) or conv)
    except (TypeError, ValueError):
        combined_conv = conv
    combined_conv_str = f"{combined_conv:.2f}×"

    # MYT timestamp (UTC+8) for the date line.
    _MYT = _tz(timedelta(hours=8))
    now_myt = datetime.now(_MYT)
    # Tidy date: "Tue, 28 Apr 2026  14:35 MYT"
    myt_str = now_myt.strftime("%a, %d %b %Y  %H:%M MYT")
    # OHLC bar date formatted as "28 Apr 2026" when it parses, else raw.
    if ohlc_date:
        try:
            _ohlc_dt = datetime.strptime(ohlc_date[:10], "%Y-%m-%d")
            ohlc_date_fmt = _ohlc_dt.strftime("%d %b %Y")
        except ValueError:
            ohlc_date_fmt = ohlc_date
    else:
        ohlc_date_fmt = ""

    # Header: flag(s) + state emoji + label + pair
    flag_prefix = f"{pair_flag} " if pair_flag else ""
    text = (
        f"<b>{flag_prefix}{emoji} {label}</b>\n"
        f"<b>{pair.upper()}</b>  ·  {basis_tag}\n"
        f"─────────────────────\n"
        f"{align_tag}\n"
        f"Quality : {quality.title()}\n"
        f"─────────────────────\n"
        f"<b>Pivot Levels</b>  <i>(Daily)</i>\n"
        f"<code>{_telegram_html_escape(_pivot_signal_price_ladder(pair, price, {'R3': r3, 'R2': r2, 'R1': r1, 'PP': pp, 'S1': s1, 'S2': s2, 'S3': s3}))}</code>\n"
        f"─────────────────────\n"
        f"<code>{'Score':<10} {mscore_str:>8}</code>\n"
        f"<code>{'Conv':<10} {conv:.2f}×{' ':>5}</code>\n"
        f"<code>{'RSI':<10} {rsi_value_str:>12}</code>\n"
        f"<code>{'RSI Signal':<10} {rsi_signal_str:>12}</code>\n"
        f"<code>{'RSI Align':<10} {rsi_alignment_str:>12}</code>\n"
        f"<code>{'Comb':<10} {combined_conv_str:>8}</code>"
        + (f"\n─────────────────────\n"
           f"<code>{'Signal':<10} {myt_str}</code>\n"
           f"<code>{'Bar':<10} {ohlc_date_fmt:>11}</code>" if ohlc_date_fmt else "")
    )

    # De-dup key: stable within one daily OHLC bar per pair+state.
    # ohlc_date is the date string from the OHLC source; when a new bar
    # rolls in, the key changes and a fresh alert fires.
    rej_level_key = result.get("rejection_level", "") if state == "rejection" else ""
    key = f"{pair.lower()}:{state}:{rej_level_key}:{ohlc_date}"

    level_map = {
        "breakout_up": "breakout", "breakout_down": "breakout",
        "rejection": "rejection",
        "accept_above_pp": "accept", "accept_below_pp": "accept",
    }
    ai_note = _pivot_groq_ai_note(pair, result, label, "price-state pivot alert")
    if ai_note:
        text += "\n─────────────────────\n<b>🤖 Groq AI</b>\n" + ai_note
    alerts.append({"key": key, "text": text, "level": level_map.get(state, state)})
    return alerts


# ---------------------------------------------------------------------------
# New York close reset alerts
# ---------------------------------------------------------------------------

def _pivot_load_state_file(path: Path) -> Dict[str, str]:
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except FileNotFoundError:
        return {}
    except (json.JSONDecodeError, OSError, ValueError) as exc:
        log.warning("pivot reset alerts: failed to load state file %s: %s", path, exc)
        return {}


def _pivot_save_state_file(path: Path, state: Dict[str, str]) -> None:
    tmp = path.with_suffix(".tmp")
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2)
            f.flush(); os.fsync(f.fileno())
        os.replace(tmp, path)
    except OSError as exc:
        log.warning("pivot reset alerts: failed to save state file %s: %s", path, exc)
        try: tmp.unlink(missing_ok=True)
        except OSError: pass


def _build_pivot_reset_alert(pair: str, result: Dict, session_date: str) -> Optional[Dict[str, str]]:
    if result.get("price_state") == STATE_UNAVAILABLE:
        return None
    try:
        pp = float(result.get("PP", 0.0)); r1 = float(result.get("R1", 0.0)); r2 = float(result.get("R2", 0.0)); r3 = float(result.get("R3", 0.0))
        s1 = float(result.get("S1", 0.0)); s2 = float(result.get("S2", 0.0)); s3 = float(result.get("S3", 0.0))
        close = float(result.get("close", result.get("price_used", 0.0))); rng = float(result.get("range", 0.0))
    except (TypeError, ValueError):
        return None
    if pp <= 0.0:
        return None

    fp = _pivot_format_price
    pair_l = pair.lower().strip()
    pair_flag = _PAIR_FLAG.get(pair_l, "")
    ohlc_date = str(result.get("ohlc_date", "") or session_date)
    source = str(result.get("ohlc_source", "") or "unknown")

    try:
        session_fmt = datetime.strptime(session_date[:10], "%Y-%m-%d").strftime("%d %b %Y")
    except ValueError:
        session_fmt = session_date
    try:
        ohlc_fmt = datetime.strptime(ohlc_date[:10], "%Y-%m-%d").strftime("%d %b %Y")
    except ValueError:
        ohlc_fmt = ohlc_date

    now_myt = datetime.now(_tz(timedelta(hours=8))).strftime("%a, %d %b %Y  %H:%M MYT")
    pivot_ladder = _pivot_signal_price_ladder(
        pair_l,
        close,
        {"R3": r3, "R2": r2, "R1": r1, "PP": pp, "S1": s1, "S2": s2, "S3": s3},
        live_label="NY Close",
    )

    text = (
        f"<b>{(pair_flag + ' ') if pair_flag else ''}🔄 NY CLOSE PIVOT RESET</b>\n"
        f"<b>{pair_l.upper()}</b>  ·  New daily pivots active\n"
        f"─────────────────────\n"
        f"Session : {session_fmt}\n"
        f"OHLC Bar: {ohlc_fmt}\n"
        f"Source  : {source}\n"
        f"─────────────────────\n"
        f"<code>{'Close':<6} {fp(pair_l, close):>12}</code>\n"
        f"<code>{'Range':<6} {fp(pair_l, rng):>12}</code>\n"
        f"─────────────────────\n"
        f"<b>Pivot Levels</b>  <i>(Daily)</i>\n"
        f"<code>{_telegram_html_escape(pivot_ladder)}</code>\n"
        f"─────────────────────\n"
        f"<code>{'Reset':<6} {now_myt}</code>"
    )

    ai_note = _pivot_groq_ai_note(pair_l, result, "NY CLOSE PIVOT RESET", "daily pivot reset")
    if ai_note:
        text += "\n─────────────────────\n<b>🤖 Groq AI</b>\n" + ai_note
    return {"key": f"reset|{pair_l}|{session_date}", "text": text, "level": "reset"}


def dispatch_pivot_reset_alerts(results: Dict[str, Dict], bot_token: str = TELEGRAM_BOT_TOKEN, chat_id: int = TELEGRAM_CHAT_ID, dry_run: bool = False) -> int:
    if not PIVOT_RESET_ALERTS_ENABLED or not bot_token:
        return 0
    if not chat_id:
        if not _chat_id_raw:
            raise RuntimeError("TELEGRAM_CHAT_ID is required when Telegram alerts are enabled")
        chat_id = _resolve_chat_id(_chat_id_raw)
    session_date = _pivot_session_date(); state = _pivot_prune_alert_state(_pivot_load_state_file(PIVOT_RESET_ALERT_STATE_FILE)); sent = 0; now_iso = _pivot_iso(_pivot_now_utc())
    for pair, result in sorted(results.items()):
        alert = _build_pivot_reset_alert(pair, result, session_date)
        if alert is None or alert["key"] in state:
            continue
        if dry_run:
            log.info("[DRY-RUN] pivot reset alert key=%s:\n%s", alert["key"], alert["text"]); sent += 1
        elif _pivot_send_telegram(bot_token, chat_id, alert["text"]):
            state[alert["key"]] = now_iso; sent += 1
    if not dry_run:
        _pivot_save_state_file(PIVOT_RESET_ALERT_STATE_FILE, state)
    return sent


# ---------------------------------------------------------------------------
# Explicit pivot level-cross alerts: PP / R1 / R2 / R3 / S1 / S2 / S3
# ---------------------------------------------------------------------------
# These alerts are separate from the structural alerts above.  Structural alerts
# classify the market state (breakout, rejection, accept_above_pp, etc.).  Level
# cross alerts are mechanical: if the latest price crosses from one side of a
# pivot level to the other, send exactly one Telegram alert, then cool down.

PIVOT_LEVEL_CROSS_ALERTS_ENABLED: bool = os.environ.get(
    "PIVOT_LEVEL_CROSS_ALERTS", "1"
).strip().lower() not in ("0", "false", "no", "off")

PIVOT_LEVEL_CROSS_STATE_FILE: Path = Path(
    os.environ.get(
        "PIVOT_LEVEL_CROSS_STATE_FILE",
        str(DEFAULT_COMPONENT_FILE.parent / "pivot_level_cross_alerts.json"),
    )
)

PIVOT_LEVEL_CROSS_COOLDOWN_MINUTES: int = max(
    1, int(_env_float("PIVOT_LEVEL_CROSS_COOLDOWN_MINUTES", 60.0, minimum=1.0))
)

PIVOT_LEVEL_CROSS_MIN_PIPS: float = _env_float(
    "PIVOT_LEVEL_CROSS_MIN_PIPS", 0.0, minimum=0.0
)

_PIVOT_LEVEL_CROSS_LEVELS: Tuple[str, ...] = ("R3", "R2", "R1", "PP", "S1", "S2", "S3")


def _pivot_level_cross_load_state() -> Dict[str, Any]:
    """Load level-cross state from disk.

    Shape:
      {
        "pairs": {"eurusd": {"last_price": 1.12345, "sides": {"R1": "below"}}},
        "alerts": {"eurusd:R1:above:2026-05-13": "2026-05-14T01:02:03Z"}
      }
    """
    try:
        with open(PIVOT_LEVEL_CROSS_STATE_FILE, encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            data.setdefault("pairs", {})
            data.setdefault("alerts", {})
            return data
    except FileNotFoundError:
        pass
    except (json.JSONDecodeError, OSError, ValueError) as exc:
        log.warning("pivot level-cross alerts: failed to load state: %s", exc)
    return {"pairs": {}, "alerts": {}}


def _pivot_level_cross_save_state(state: Dict[str, Any]) -> None:
    """Persist level-cross state atomically."""
    tmp = PIVOT_LEVEL_CROSS_STATE_FILE.with_suffix(".tmp")
    try:
        PIVOT_LEVEL_CROSS_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, PIVOT_LEVEL_CROSS_STATE_FILE)
    except OSError as exc:
        log.warning("pivot level-cross alerts: failed to save state: %s", exc)
        try:
            tmp.unlink(missing_ok=True)
        except OSError:
            pass


def _pivot_level_cross_prune_alerts(state: Dict[str, Any]) -> Dict[str, Any]:
    """Prune old alert cooldown keys so the state file does not grow forever."""
    cutoff = _pivot_now_utc() - timedelta(minutes=PIVOT_LEVEL_CROSS_COOLDOWN_MINUTES)
    alerts = state.setdefault("alerts", {})
    if not isinstance(alerts, dict):
        state["alerts"] = {}
        return state
    kept: Dict[str, str] = {}
    for key, ts_raw in alerts.items():
        ts = _pivot_parse_dt(ts_raw)
        if ts is not None and ts >= cutoff:
            kept[key] = str(ts_raw)
    state["alerts"] = kept
    return state


def _pivot_level_side(price: float, level: float) -> str:
    """Return side of price relative to a level."""
    if price > level:
        return "above"
    if price < level:
        return "below"
    return "at"


def _pivot_level_cross_key(pair: str, level_name: str, direction: str, ohlc_date: str) -> str:
    """Stable cooldown key for one pair/level/direction/day."""
    return f"level_cross:{pair.lower()}:{level_name}:{direction}:{ohlc_date or 'no_date'}"


def _pivot_level_cross_text(
    pair: str,
    level_name: str,
    level_value: float,
    previous_price: float,
    current_price: float,
    direction: str,
    result: Dict[str, Any],
) -> str:
    """Build Telegram text for a mechanical pivot level cross."""
    pair_l = pair.lower()
    pair_display = f"{pair_l[:3].upper()}/{pair_l[3:].upper()}" if len(pair_l) == 6 else pair.upper()
    pair_flag = _PAIR_FLAG.get(pair_l, "")
    arrow = "⬆️" if direction == "above" else "⬇️"
    verb = "crossed above" if direction == "above" else "crossed below"
    pips_from_level = abs(_pivot_pips(pair_l, current_price - level_value))
    ohlc_date = result.get("ohlc_date", "")
    state = str(result.get("price_state", ""))
    alignment = str(result.get("macro_alignment", "neutral"))
    rsi = result.get("rsi")
    rsi_line = ""
    try:
        if rsi is not None:
            rsi_line = f"\nRSI: <b>{float(rsi):.2f}</b> ({_telegram_html_escape(result.get('rsi_bias', ''))})"
    except (TypeError, ValueError):
        rsi_line = ""

    ladder = _pivot_signal_price_ladder(
        pair_l,
        current_price,
        {
            "R3": float(result.get("R3", 0.0) or 0.0),
            "R2": float(result.get("R2", 0.0) or 0.0),
            "R1": float(result.get("R1", 0.0) or 0.0),
            "PP": float(result.get("PP", 0.0) or 0.0),
            "S1": float(result.get("S1", 0.0) or 0.0),
            "S2": float(result.get("S2", 0.0) or 0.0),
            "S3": float(result.get("S3", 0.0) or 0.0),
        },
        live_label="Current",
    )

    return (
        f"{arrow} <b>PIVOT LEVEL CROSS</b>\n"
        f"{pair_flag} <b>{pair_display}</b> {verb} <b>{level_name}</b>\n\n"
        f"Level: <code>{_pivot_format_price(pair_l, level_value)}</code>\n"
        f"Prev: <code>{_pivot_format_price(pair_l, previous_price)}</code> → "
        f"Now: <code>{_pivot_format_price(pair_l, current_price)}</code>\n"
        f"Distance: <b>{pips_from_level:.1f}p</b> from {level_name}\n"
        f"State: <b>{_telegram_html_escape(state.replace('_', ' '))}</b> · "
        f"Macro: <b>{_telegram_html_escape(alignment)}</b>"
        f"{rsi_line}\n"
        f"OHLC: <code>{_telegram_html_escape(ohlc_date)}</code> · "
        f"Source: <code>{_telegram_html_escape(result.get('ohlc_source', ''))}</code>\n\n"
        f"<pre>{_telegram_html_escape(ladder)}</pre>"
    )


def build_pivot_level_cross_alerts(
    pair: str,
    result: Dict[str, Any],
    state: Dict[str, Any],
) -> List[Dict[str, str]]:
    """Build explicit PP/R/S level-cross alerts and update last-side tracking.

    The first run only seeds the side state and does not alert.  Alerts start
    from the second run onward when a true side change is detected.
    """
    alerts: List[Dict[str, str]] = []
    if not PIVOT_LEVEL_CROSS_ALERTS_ENABLED:
        return alerts
    if result.get("price_state") == STATE_UNAVAILABLE:
        return alerts

    pair_l = pair.lower().strip()
    try:
        current_price = float(result.get("price_used"))
    except (TypeError, ValueError):
        return alerts
    if current_price <= 0:
        return alerts

    pairs_state = state.setdefault("pairs", {})
    pair_state = pairs_state.setdefault(pair_l, {})
    prev_price_raw = pair_state.get("last_price")
    try:
        previous_price = float(prev_price_raw) if prev_price_raw is not None else None
    except (TypeError, ValueError):
        previous_price = None

    previous_sides = pair_state.get("sides", {})
    if not isinstance(previous_sides, dict):
        previous_sides = {}

    new_sides: Dict[str, str] = {}
    alerts_state = state.setdefault("alerts", {})
    now_iso = _pivot_iso(_pivot_now_utc())
    ohlc_date = str(result.get("ohlc_date", ""))

    for level_name in _PIVOT_LEVEL_CROSS_LEVELS:
        try:
            level_value = float(result.get(level_name, 0.0) or 0.0)
        except (TypeError, ValueError):
            continue
        if level_value <= 0:
            continue

        current_side = _pivot_level_side(current_price, level_value)
        new_sides[level_name] = current_side
        previous_side = str(previous_sides.get(level_name, ""))

        # First run / no previous side: seed only, no alert.
        if previous_price is None or not previous_side:
            continue

        # Ignore exact-at transitions until price resolves to above/below.
        if current_side == "at" or previous_side == "at" or current_side == previous_side:
            continue

        direction = current_side  # current side after crossing: "above" or "below"
        min_pips = abs(_pivot_pips(pair_l, current_price - level_value))
        if min_pips < PIVOT_LEVEL_CROSS_MIN_PIPS:
            log.debug(
                "pivot level-cross[%s]: skip %s %s — %.1fp below min %.1fp",
                pair_l, level_name, direction, min_pips, PIVOT_LEVEL_CROSS_MIN_PIPS,
            )
            continue

        key = _pivot_level_cross_key(pair_l, level_name, direction, ohlc_date)
        if key in alerts_state:
            continue

        alerts.append({
            "key": key,
            "level": "level_cross",
            "text": _pivot_level_cross_text(
                pair_l, level_name, level_value, previous_price, current_price, direction, result
            ),
        })
        alerts_state[key] = now_iso

    pair_state["last_price"] = current_price
    pair_state["sides"] = new_sides
    pair_state["updated_at"] = now_iso
    pairs_state[pair_l] = pair_state
    return alerts


def dispatch_pivot_level_cross_alerts(
    results: Dict[str, Dict],
    bot_token: str = TELEGRAM_BOT_TOKEN,
    chat_id: int = TELEGRAM_CHAT_ID,
    dry_run: bool = False,
) -> int:
    """Fire mechanical PP/R/S level-cross Telegram alerts.

    This function can be called directly, but dispatch_pivot_alerts() also calls
    it so existing integrations automatically get level-cross alerts.
    """
    if not PIVOT_LEVEL_CROSS_ALERTS_ENABLED:
        return 0
    if not dry_run and bot_token and not chat_id:
        chat_id = _resolve_chat_id(_chat_id_raw)

    state = _pivot_level_cross_prune_alerts(_pivot_level_cross_load_state())
    sent = 0

    for pair, result in results.items():
        for alert in build_pivot_level_cross_alerts(pair, result, state):
            if dry_run:
                log.info("[DRY-RUN] pivot level-cross alert key=%s:\n%s", alert["key"], alert["text"])
                sent += 1
            else:
                if _pivot_send_telegram(bot_token, chat_id, alert["text"]):
                    sent += 1
                    log.info("pivot level-cross alerts: sent for %s key=%s", pair, alert["key"])
                else:
                    log.warning("pivot level-cross alerts: failed for %s key=%s", pair, alert["key"])

    if not dry_run:
        _pivot_level_cross_save_state(state)
    return sent

# ---------------------------------------------------------------------------
# Dispatcher
# ---------------------------------------------------------------------------

def dispatch_pivot_alerts(
    results: Dict[str, Dict],
    bot_token: str = TELEGRAM_BOT_TOKEN,
    chat_id: int   = TELEGRAM_CHAT_ID,
    dry_run: bool  = False,
) -> int:
    """Fire new pivot alerts for all pairs and return the count sent.

    De-duplication is keyed on (pair, price_state, ohlc_date) so the same
    structural event is never re-sent for the same daily bar.

    Args:
        results:   Output of ``fetch_price_structure()`` (first return value).
        bot_token: Telegram bot token (defaults to TELEGRAM_BOT_TOKEN).
        chat_id:   Telegram chat ID  (defaults to TELEGRAM_CHAT_ID).
        dry_run:   Log alerts without sending or persisting state.

    Returns:
        Number of alerts sent (or that would have been sent in dry-run mode).

    Raises:
        RuntimeError: If ``chat_id`` is 0/unset and ``dry_run`` is False and
                      ``bot_token`` is set (i.e. sending is actually attempted).
    """
    # Validate credentials before doing any work.
    if not dry_run and bot_token and not chat_id:
        try:
            chat_id = _resolve_chat_id(_chat_id_raw)
        except RuntimeError as exc:
            raise RuntimeError(
                f"pivot alerts: {exc}  "
                "Set TELEGRAM_CHAT_ID or disable alerts."
            ) from exc

    state = _pivot_prune_alert_state(_pivot_load_alert_state())
    sent = 0

    for pair, result in results.items():
        if result.get("price_state") == STATE_UNAVAILABLE:
            continue
        for alert in build_pivot_alerts(pair, result):
            key = alert["key"]
            if key in state:
                log.debug("pivot alerts: skipping duplicate key=%s", key)
                continue
            if dry_run:
                log.info("[DRY-RUN] pivot alert key=%s:\n%s", key, alert["text"])
                # Do NOT write state in dry-run: would suppress real alerts next run.
                sent += 1
            else:
                ok = _pivot_send_telegram(bot_token, chat_id, alert["text"])
                if ok:
                    state[key] = _pivot_iso(_pivot_now_utc())
                    sent += 1
                    log.info(
                        "pivot alerts: sent %s alert for %s (key=%s)",
                        alert["level"], pair, key,
                    )
                else:
                    log.warning(
                        "pivot alerts: failed to send %s alert for %s",
                        alert["level"], pair,
                    )

    # Mechanical level-cross alerts (PP/R1/R2/R3/S1/S2/S3) are tracked in
    # a separate state file so they do not interfere with structural alerts.
    sent += dispatch_pivot_level_cross_alerts(
        results,
        bot_token=bot_token,
        chat_id=chat_id,
        dry_run=dry_run,
    )

    if not dry_run:
        _pivot_save_alert_state(state)
    return sent

#!/usr/bin/env python3
"""
ema.py

Robust EMA engine for EURUSD, GBPUSD, XAUUSD.

Data sources:
- Twelve Data API: main Daily correction + gap repair source.
  Set TWELVEDATA_API_KEY env var.
  Session windows: once per FX session open (Sydney / Tokyo / London / New York)
  a broker-grade Daily history is fetched from Twelve Data and merged over the local
  snapshot-reconstructed history. This reduces cumulative reconstruction error
  from missed spikes, delayed quotes and stale Stooq data.
  Outside session windows, gap repair still fires when REPAIR_GAP_HOURS is exceeded.
  Set EMA_SESSION_WINDOW_MINUTES to control how long after session open the window stays open (default 20).
- Stooq: live snapshots every run, builds completed Daily closes locally between Twelve Data corrections.

Output files (written into EMA_DATA_DIR, default: script directory):
- ema_stooq_snapshots.jsonl
- ema_closes.json
- ema_state.json
- ema_twelvedata_usage.json (Twelve Data quota + session-window tracking)

Required environment variables:
  TELEGRAM_CHAT_ID         Your Telegram chat ID (integer). Always required —
                           there is no default to prevent accidental alert leakage.
                           Pass --no-alerts if Telegram is not needed.

Optional environment variables:
  TELEGRAM_BOT_TOKEN       Telegram bot token. Leave unset to disable sending.
  TWELVEDATA_API_KEY       Twelve Data API key for Daily seeding / repair.
  EMA_DATA_DIR             Directory for all data files (default: script dir).
  EMA_SESSION_WINDOW_MINUTES  Minutes after session open to allow broker correction (default: 20).
  EMA_WARMUP_MULTIPLIER    Multiplier applied to slow period for EMA warm-up gate
                           (default: 2 → require slow*2 bars before emitting signals).
                           Set to 1 to restore the minimal single-pass behaviour.

CLI flags:
  --pair EURUSD        Single pair (default: all pairs)
  --all                Scan all supported pairs
  --fast INT           Fast EMA period (default: 20)
  --slow INT           Slow EMA period (default: 50)
  --json               Print JSON output
  --session-info       Print current session window status and exit
  --no-alerts          Disable Telegram alerts for this run
  --dry-run-alerts     Print alerts that would be sent without actually sending
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import platform
import time
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from io import StringIO
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional, Tuple

fcntl = None  # will be replaced by real module on POSIX when filelock is absent
try:
    from filelock import FileLock as _FileLock  # type: ignore
    _LOCK_BACKEND = "filelock"
except ImportError:
    _FileLock = None
    if platform.system() != "Windows":
        import fcntl  # type: ignore
        _LOCK_BACKEND = "fcntl"
    else:
        _LOCK_BACKEND = "none"
        logging.getLogger(__name__).warning(
            "ema.py: file locking is unavailable on Windows without the 'filelock' package. "
            "Install it with: pip install filelock"
        )

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    try:
        from backports.zoneinfo import ZoneInfo  # type: ignore
    except ImportError:
        raise ImportError(
            "ema.py requires the 'tzdata' package on environments without system "
            "timezone data (e.g. GitHub Actions Ubuntu runners). "
            "Add 'tzdata' to requirements.txt."
        )

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Load .env file before reading any environment variables.
# python-dotenv is optional — if absent, only real env vars are used.
# ---------------------------------------------------------------------------
try:
    from dotenv import load_dotenv  # type: ignore

    try:
        _script_dir = Path(__file__).resolve().parent
    except NameError:
        _script_dir = Path.cwd()

    _env_path = _script_dir / ".env"
    if _env_path.exists():
        load_dotenv(dotenv_path=_env_path, override=False)
        log.info("ema: loaded env from %s", _env_path)
    else:
        _loaded = load_dotenv(override=False)
        if _loaded:
            log.info("ema: loaded env via dotenv search (no .env found at %s)", _env_path)
        else:
            log.warning(
                "ema: no .env file found at %s or in parent directories — "
                "env vars must be set in the shell environment",
                _env_path,
            )
except ImportError:
    log.warning(
        "ema: python-dotenv is not installed — .env file will NOT be loaded. "
        "Install it with:  pip install python-dotenv"
    )

UTC = timezone.utc
SUPPORTED_PAIRS = ["EURUSD", "GBPUSD", "XAUUSD"]
PAIR_TO_TD_SYMBOL = {
    "EURUSD": "EUR/USD",
    "GBPUSD": "GBP/USD",
    "XAUUSD": "XAU/USD",
}
PAIR_TO_STOOQ_SYMBOL = {
    "EURUSD": "eurusd",
    "GBPUSD": "gbpusd",
    "XAUUSD": "xauusd",
}

# Flag / icon emojis for each supported pair.
# Format: (left_flag, right_flag) — for XAUUSD the "flags" are commodity icons.
PAIR_FLAGS: Dict[str, Tuple[str, str]] = {
    "EURUSD": ("🇪🇺", "🇺🇸"),
    "GBPUSD": ("🇬🇧", "🇺🇸"),
    "XAUUSD": ("🥇", "🇺🇸"),
}

TWELVEDATA_BASE = "https://api.twelvedata.com"
TWELVEDATA_API_KEY = os.environ.get("TWELVEDATA_API_KEY", "").strip()


USER_AGENT = os.environ.get(
    "EMA_ANALYSIS_UA",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
)

def _env_positive_int(name: str, default: int, minimum: int = 1) -> int:
    """Read a positive integer env var with a safe fallback."""
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
        if value < minimum:
            raise ValueError
        return value
    except (TypeError, ValueError):
        log.warning("ema: invalid %s=%r — using default %s", name, raw, default)
        return default


# Keep Twelve Data controlled by default. scraper.py can run frequently, so the
# EMA engine should not be able to burn several requests per symbol unless you
# explicitly raise these env vars.
TWELVEDATA_REQUEST_LIMIT_PER_DAY = _env_positive_int("EMA_TWELVEDATA_REQUEST_LIMIT_PER_DAY", 8)
TWELVEDATA_REQUESTS_PER_SYMBOL_PER_DAY = _env_positive_int("EMA_TWELVEDATA_REQUESTS_PER_SYMBOL_PER_DAY", 1)
TWELVEDATA_MIN_SECONDS_BETWEEN_REQUESTS = _env_positive_int("EMA_TWELVEDATA_MIN_SECONDS_BETWEEN_REQUESTS", 10)
TWELVEDATA_429_COOLDOWN_MINUTES = _env_positive_int("EMA_TWELVEDATA_429_COOLDOWN_MINUTES", 60)
SEED_CLOSES_TARGET = _env_positive_int("EMA_SEED_CLOSES_TARGET", 60)
MAX_CLOSES_HISTORY = _env_positive_int("EMA_MAX_CLOSES_HISTORY", 500)
REPAIR_GAP_HOURS = _env_positive_int("EMA_REPAIR_GAP_HOURS", 26)
SNAPSHOT_RETENTION_DAYS = _env_positive_int("EMA_SNAPSHOT_RETENTION_DAYS", 10)
DEFAULT_TIMEOUT = _env_positive_int("EMA_DEFAULT_TIMEOUT", 20)

# EMA warm-up: require at least slow * EMA_WARMUP_MULTIPLIER completed bars
# before emitting signals.  The default of 2 gives the slow EMA a full
# second pass worth of data to stabilise, which reduces false signals on
# volatile instruments.  Set EMA_WARMUP_MULTIPLIER=1 to restore the
# previous (minimal) behaviour.
_wmul_raw = os.environ.get("EMA_WARMUP_MULTIPLIER", "2")
try:
    EMA_WARMUP_MULTIPLIER: int = max(1, int(_wmul_raw))
except (ValueError, TypeError):
    log.warning("ema: invalid EMA_WARMUP_MULTIPLIER=%r — using default 2", _wmul_raw)
    EMA_WARMUP_MULTIPLIER = 2

# Session → (IANA tz, local open hour, local open minute).
# One broker Daily correction is allowed per window per pair per day, anchored at
# the highest-liquidity open moments to reduce Stooq snapshot reconstruction error.
SESSION_STARTS: Dict[str, Tuple[str, int, int]] = {
    "sydney":   ("Australia/Sydney",  8, 0),
    "tokyo":    ("Asia/Tokyo",        9, 0),
    "london":   ("Europe/London",     8, 0),
    "new_york": ("America/New_York",  8, 0),
}
_swm_raw = os.environ.get("EMA_SESSION_WINDOW_MINUTES", "20")
try:
    SESSION_WINDOW_MINUTES = max(1, int(_swm_raw))
except (ValueError, TypeError):
    log.warning("ema: invalid EMA_SESSION_WINDOW_MINUTES=%r — using default 20", _swm_raw)
    SESSION_WINDOW_MINUTES = 20
# Keep at most this many used/attempted window keys in the usage file.
_SESSION_WINDOW_HISTORY = 16

# ---------------------------------------------------------------------------
# Telegram alert configuration
# ---------------------------------------------------------------------------
# Both TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID are required when Telegram
# alerts are enabled.  Neither has a hardcoded default — validation is
# deferred to the call sites that need them (dispatch_alerts / main) so that
# the module can be safely imported and used with --no-alerts without setting
# these variables.
TELEGRAM_BOT_TOKEN: str = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
_chat_id_raw = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
TELEGRAM_CHAT_ID: int = 0   # 0 = unconfigured; validated before use in dispatch_alerts/main


def _resolve_telegram_chat_id(raw: str) -> int:
    """Parse and validate TELEGRAM_CHAT_ID.  Raises RuntimeError on bad input.

    Called lazily (not at import time) so the module can be imported and run
    with --no-alerts even when TELEGRAM_CHAT_ID is absent from the environment.
    """
    if not raw:
        raise RuntimeError(
            "TELEGRAM_CHAT_ID environment variable is required when Telegram alerts "
            "are enabled.  Set it to your Telegram chat ID (integer), or pass "
            "--no-alerts on the command line to disable alerts entirely."
        )
    try:
        return int(raw)
    except ValueError:
        raise RuntimeError(
            f"TELEGRAM_CHAT_ID must be an integer, got: {raw!r}"
        )


try:
    TELEGRAM_CHAT_ID = _resolve_telegram_chat_id(_chat_id_raw) if _chat_id_raw else 0
except RuntimeError:
    TELEGRAM_CHAT_ID = 0  # will be caught at dispatch_alerts / main if alerts are enabled

# Proximity threshold: alert when price is within this % of an EMA.
# e.g. 0.0015 = 0.15% — roughly 15 pips on EURUSD near 1.10.
TELEGRAM_PROXIMITY_PCT: float = float(os.environ.get("TELEGRAM_PROXIMITY_PCT", "0.0015"))

# Imminent-cross threshold: alert when |EMA20 − EMA50| / EMA50 < this value.
TELEGRAM_CROSS_IMMINENT_PCT: float = float(os.environ.get("TELEGRAM_CROSS_IMMINENT_PCT", "0.0010"))

# Optional Groq AI enrichment for Daily EMA Telegram alerts.
# Fail-soft: if credentials/API/network fail, the normal alert still sends.
GROQ_AI_ENABLED: bool = os.environ.get("GROQ_AI_ENABLED", "0").strip().lower() in ("1", "true", "yes", "on")
GROQ_API_KEY: str = os.environ.get("GROQ_API_KEY", "").strip()
GROQ_AI_MODEL: str = os.environ.get("GROQ_AI_MODEL", "llama-3.1-8b-instant").strip()
GROQ_AI_TIMEOUT: int = int(os.environ.get("GROQ_AI_TIMEOUT", "12"))
GROQ_AI_MAX_CHARS: int = int(os.environ.get("GROQ_AI_MAX_CHARS", "650"))
GROQ_AI_MAX_PER_RUN: int = max(1, int(os.environ.get("GROQ_AI_MAX_PER_RUN", "8")))
GROQ_AI_COOLDOWN_MINUTES: int = max(1, int(os.environ.get("GROQ_AI_COOLDOWN_MINUTES", "30")))
_ema_groq_ai_calls_this_run: int = 0

# Alert state file — persists de-duplication keys across runs.
TELEGRAM_ALERT_STATE_FILE_NAME = "ema_telegram_alerts.json"

BASE_DIR = Path(os.environ.get("EMA_DATA_DIR", Path(__file__).resolve().parent))
BASE_DIR.mkdir(parents=True, exist_ok=True)
SNAPSHOT_FILE = BASE_DIR / "ema_stooq_snapshots.jsonl"
TELEGRAM_ALERT_STATE_FILE = BASE_DIR / TELEGRAM_ALERT_STATE_FILE_NAME
CLOSES_FILE = BASE_DIR / "ema_closes.json"
STATE_FILE = BASE_DIR / "ema_state.json"
TWELVEDATA_USAGE_FILE = Path(
    os.environ.get(
        "EMA_TWELVEDATA_USAGE_FILE",
        os.environ.get("TWELVEDATA_USAGE_FILE", str(BASE_DIR / "ema_twelvedata_usage.json")),
    )
)
_GROQ_AI_STATE_FILE: Path = Path(
    os.environ.get(
        "GROQ_AI_STATE_FILE",
        str(Path(os.environ.get("SIGNAL_ALERT_STATE_FILE", str(BASE_DIR / "signal_telegram_alerts.json"))).parent / "groq_ai_state.json"),
    )
)


def _default_closes_payload() -> Dict[str, List[Dict[str, Any]]]:
    return {pair: [] for pair in SUPPORTED_PAIRS}


def _default_twelvedata_usage_payload(now: Optional[datetime] = None) -> Dict[str, Any]:
    current = (now or now_utc()).date().isoformat()
    return {
        "date": current,
        "count": 0,
        "per_symbol": {},
        "last_request_ts": None,
        "cooldown_until": None,
        "used_windows": [],
        "attempted_windows": [],
    }


def ensure_required_output_files() -> None:
    """Create required dashboard output files when they do not exist yet.

    This keeps first-run / no-data runs compatible with deployment checks that
    validate file presence in public_html. JSON files are initialised with valid
    empty payloads so later readers can consume them safely.
    """
    defaults = {
        SNAPSHOT_FILE: None,  # JSONL: empty file is valid
        CLOSES_FILE: _default_closes_payload(),
        STATE_FILE: {},
        TWELVEDATA_USAGE_FILE: _default_twelvedata_usage_payload(),
    }
    for target, payload in defaults.items():
        if target.exists():
            continue
        target.parent.mkdir(parents=True, exist_ok=True)
        if payload is None:
            target.touch()
            try:
                os.chmod(target, 0o644)
            except OSError:
                pass
            continue
        tmp = target.with_suffix(target.suffix + ".tmp")
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(payload, f, indent=2)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, target)
            try:
                os.chmod(target, 0o644)
            except OSError:
                pass
        finally:
            try:
                tmp.unlink(missing_ok=True)
            except OSError:
                pass

_twelvedata_usage_cache: Optional[Dict[str, Any]] = None

LOCK_TIMEOUT_SECONDS = 30


@contextmanager
def file_lock(path: Path) -> Generator[None, None, None]:
    """Cross-platform advisory file lock.

    Uses 'filelock' when installed (recommended; works on all platforms).
    Falls back to POSIX fcntl on Unix if filelock is absent.
    On Windows without filelock, logs a one-time warning and proceeds unlocked.
    """
    lock_path = path.with_suffix(path.suffix + ".lock")

    if _LOCK_BACKEND == "filelock":
        assert _FileLock is not None
        lock = _FileLock(str(lock_path), timeout=LOCK_TIMEOUT_SECONDS)
        try:
            lock.acquire()
        except Exception:
            log.warning("file_lock(%s): could not acquire within %ds — proceeding unlocked",
                        path.name, LOCK_TIMEOUT_SECONDS)
        try:
            yield
        finally:
            try:
                lock.release()
            except Exception:
                pass

    elif _LOCK_BACKEND == "fcntl":
        deadline = time.monotonic() + LOCK_TIMEOUT_SECONDS
        lock_fd: Optional[int] = None
        try:
            lock_fd = os.open(str(lock_path), os.O_CREAT | os.O_WRONLY, 0o644)
            while True:
                try:
                    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    if time.monotonic() >= deadline:
                        log.warning("file_lock(%s): could not acquire within %ds — proceeding unlocked",
                                    path.name, LOCK_TIMEOUT_SECONDS)
                        break
                    time.sleep(0.1)
            yield
        finally:
            if lock_fd is not None:
                try:
                    fcntl.flock(lock_fd, fcntl.LOCK_UN)
                except OSError:
                    pass
                os.close(lock_fd)

    else:
        # No locking available (Windows without filelock) — proceed unlocked.
        yield


@dataclass
class EMAAnalysisResult:
    pair: str
    timeframe: str
    completed_closes: int
    last_completed_hour: str
    last_close: Optional[float]
    fast_period: int          # which --fast value was used
    slow_period: int          # which --slow value was used
    ema_fast: Optional[float]
    ema_slow: Optional[float]
    close_vs_fast: Optional[str]
    close_vs_slow: Optional[str]
    close_structure: Optional[str]
    ema_fast_vs_slow: Optional[str]
    ema_cross_signal: Optional[str]
    current_price: Optional[float]
    current_vs_fast: Optional[str]
    current_vs_slow: Optional[str]
    current_structure: Optional[str]
    trend_bias: str
    suggestion: str
    notes: str
    twelvedata_requests_today: int
    source_basis: str


# ---------------------------------------------------------------------------
# Telegram alerts
# ---------------------------------------------------------------------------

def send_telegram_message(
    bot_token: str,
    chat_id: int,
    text: str,
    timeout: int = 10,
) -> bool:
    """Send a Telegram message via the Bot API.  Returns True on success."""
    if not bot_token:
        log.debug("send_telegram_message: no bot token configured — skipping")
        return False
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    try:
        # Use a plain session (not the Stooq retry session) with a standard UA.
        s = requests.Session()
        s.headers.update({"User-Agent": USER_AGENT})
        resp = s.post(
            url,
            json={"chat_id": chat_id, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True},
            timeout=timeout,
        )
        if resp.status_code == 200:
            log.debug("send_telegram_message: sent OK (chat_id=%s)", chat_id)
            return True
        log.warning(
            "send_telegram_message: HTTP %s — %s",
            resp.status_code,
            resp.text[:200],
        )
        return False
    except requests.exceptions.RequestException as exc:
        log.warning("send_telegram_message: request failed: %s", exc)
        return False


def _load_alert_state() -> Dict[str, Any]:
    """Load de-duplication state from disk.  Keys are alert fingerprints; values are ISO timestamps."""
    data = load_json(TELEGRAM_ALERT_STATE_FILE, {})
    if not isinstance(data, dict):
        return {}
    return data


def _save_alert_state(state: Dict[str, Any]) -> None:
    save_json(TELEGRAM_ALERT_STATE_FILE, state)


def _prune_alert_state(state: Dict[str, Any], max_age_hours: int = 48) -> Dict[str, Any]:
    """Remove old one-shot alert keys while preserving clean-alignment regime state."""
    cutoff = now_utc() - timedelta(hours=max_age_hours)
    pruned: Dict[str, Any] = {}
    for k, v in state.items():
        if str(k).endswith(":clean_alignment_state") and isinstance(v, dict):
            pruned[k] = v
            continue
        ts = parse_dt(v)
        if ts is None:
            log.debug("_prune_alert_state: dropping corrupt entry key=%s", k)
            continue
        if ts >= cutoff:
            pruned[k] = v
    return pruned

def _proximity_pct(price: float, ema: float) -> float:
    """Return |price − ema| / ema as a fraction.

    Returns ``float('inf')`` when *ema* is zero so that proximity and
    imminent-cross threshold comparisons (``< TELEGRAM_PROXIMITY_PCT``) always
    evaluate to False, preventing spurious alerts when an EMA has not yet
    been computed from real data.
    """
    if ema == 0:
        return float("inf")
    return abs(price - ema) / abs(ema)


def _format_price(pair: str, value: float) -> str:
    """Format a price with pair-appropriate decimal places."""
    # XAUUSD gets different precision
    if "XAU" in pair.upper():
        return f"{value:.2f}"
    return f"{value:.5f}"


def _side(price: float, ema: float) -> str:
    return "above" if price >= ema else "below"


def _pair_header(pair: str, bias: str = "") -> str:
    """Return a decorated pair string with country/commodity flags and bias arrow.

    A coloured directional arrow is appended when the bias is bullish or bearish:
      - Bullish → 🔼  (up arrow, bullish)
      - Bearish → 🔽  (down arrow, bearish)

    Examples:
      EURUSD (bullish) →  🇪🇺 EUR/USD 🇺🇸 🔼
      GBPUSD (bearish) →  🇬🇧 GBP/USD 🇺🇸 🔽
      XAUUSD (bullish) →  🥇 XAU/USD 🇺🇸 🔼
    """
    left, right = PAIR_FLAGS.get(pair.upper(), ("", ""))
    display = f"{pair[:3]}/{pair[3:]}"  # e.g. "EUR/USD"
    base = f"{left} {display} {right}" if (left and right) else display

    bias_lower = bias.lower()
    if "bullish" in bias_lower:
        return f"{base} 🔼"
    if "bearish" in bias_lower:
        return f"{base} 🔽"
    return base


def _fmt_bucket(bucket: str) -> str:
    """Format the Daily bucket ISO string into a clean, readable timestamp (MYT).

    Input example : '2025-04-28T08:00:00Z'
    Output example: '28 Apr 2025  16:00 MYT'
    """
    try:
        dt = datetime.strptime(bucket, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)
        myt = ZoneInfo("Asia/Kuala_Lumpur")
        dt_myt = dt.astimezone(myt)
        return dt_myt.strftime("%d %b %Y  %H:%M MYT")
    except (ValueError, TypeError):
        return bucket or "—"


# ---------------------------------------------------------------------------
# Groq AI helpers for Daily EMA alerts
# ---------------------------------------------------------------------------

_ema_groq_session_instance: Optional[requests.Session] = None


def _ema_groq_load_state() -> Dict[str, Any]:
    try:
        with open(_GROQ_AI_STATE_FILE, encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _ema_groq_save_state(data: Dict[str, Any]) -> None:
    try:
        _GROQ_AI_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        tmp = _GROQ_AI_STATE_FILE.with_suffix(".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, _GROQ_AI_STATE_FILE)
    except Exception as exc:
        log.warning("ema: could not save Groq AI state file: %s", exc)


def _ema_groq_can_call() -> bool:
    global _ema_groq_ai_calls_this_run
    if _ema_groq_ai_calls_this_run >= GROQ_AI_MAX_PER_RUN:
        log.debug("ema: Groq AI per-run cap reached (%d/%d) — skipping", _ema_groq_ai_calls_this_run, GROQ_AI_MAX_PER_RUN)
        return False
    cooldown_until = _ema_groq_load_state().get("cooldown_until")
    if cooldown_until:
        try:
            cu = datetime.fromisoformat(str(cooldown_until).replace("Z", "+00:00"))
            if datetime.now(UTC) < cu:
                log.warning("ema: Groq AI cooldown active until %s — skipping", cooldown_until)
                return False
        except Exception:
            pass
    return True


def _ema_groq_mark_429() -> None:
    until = (datetime.now(UTC) + timedelta(minutes=GROQ_AI_COOLDOWN_MINUTES)).strftime("%Y-%m-%dT%H:%M:%SZ")
    _ema_groq_save_state({"cooldown_until": until})
    log.warning("ema: Groq AI 429 — cooldown set for %d min (until %s)", GROQ_AI_COOLDOWN_MINUTES, until)


def _ema_groq_mark_call() -> None:
    global _ema_groq_ai_calls_this_run
    _ema_groq_ai_calls_this_run += 1


def _ema_groq_session() -> requests.Session:
    global _ema_groq_session_instance
    if _ema_groq_session_instance is None:
        s = requests.Session()
        s.headers.update({"User-Agent": USER_AGENT})
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
        _ema_groq_session_instance = s
    return _ema_groq_session_instance


def _ema_groq_prompt(res: "EMAAnalysisResult", signal: str, summary: str) -> str:
    return "\n".join([
        "You are a concise FX EMA momentum assistant for Telegram alerts.",
        "Return exactly 2 short bullets only. No trade command. No guarantee. No markdown table.",
        "Explain what the Daily EMA signal means and what to watch next.",
        f"Pair: {res.pair}",
        f"Signal: {signal}",
        f"Trend bias: {res.trend_bias}",
        f"Suggestion: {res.suggestion}",
        f"Last close: {res.last_close}",
        f"Current price: {res.current_price}",
        f"EMA{res.fast_period}: {res.ema_fast}",
        f"EMA{res.slow_period}: {res.ema_slow}",
        f"Close structure: {res.close_structure}",
        f"Current structure: {res.current_structure}",
        f"Summary: {summary}",
        "Style: punchy, trader-friendly, max 45 words total.",
    ])


def _ema_groq_ai_note(res: "EMAAnalysisResult", signal: str, summary: str) -> str:
    """Return Groq AI enrichment note for a Daily EMA alert, or ''."""
    if not (GROQ_AI_ENABLED and GROQ_API_KEY and GROQ_AI_MODEL):
        return ""
    if not _ema_groq_can_call():
        return ""
    url = "https://api.groq.com/openai/v1/chat/completions"
    payload = {
        "model": GROQ_AI_MODEL,
        "messages": [{"role": "user", "content": _ema_groq_prompt(res, signal, summary)}],
        "max_tokens": 150,
    }
    headers = {"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"}
    try:
        resp = _ema_groq_session().post(url, headers=headers, json=payload, timeout=GROQ_AI_TIMEOUT)
        if resp.status_code == 429:
            _ema_groq_mark_429()
            return ""
        if resp.status_code != 200:
            log.warning("ema: Groq AI HTTP %s — %s", resp.status_code, resp.text[:200])
            return ""
        _ema_groq_mark_call()
        choices = resp.json().get("choices") or []
        ai_text = choices[0].get("message", {}).get("content", "") if choices else ""
        ai_text = "\n".join(line.strip() for line in str(ai_text).strip().splitlines() if line.strip())[:GROQ_AI_MAX_CHARS]
        return ai_text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    except Exception as exc:
        log.warning("ema: Groq AI enrichment skipped: %s", exc)
        return ""

def build_alerts(res: "EMAAnalysisResult") -> List[Dict[str, Any]]:
    """Inspect an EMAAnalysisResult and return Telegram alerts with a
    professional Daily-focused message layout.

    Each dict has:
      - ``key``  : stable de-dup fingerprint for this Daily bucket + signal
      - ``text`` : polished HTML Telegram message
      - ``level``: "cross" or "clean_alignment"
      - ``state_key`` / ``state_value``: optional regime state used so clean
        alignment alerts fire only when the alignment first appears or flips.
    """
    alerts: List[Dict[str, Any]] = []

    if res.current_price is None or res.ema_fast is None or res.ema_slow is None:
        return alerts

    price = res.current_price
    ema_f = res.ema_fast
    ema_s = res.ema_slow
    pair = res.pair
    f = res.fast_period
    s = res.slow_period
    bucket = res.last_completed_hour  # stable per Daily bar

    fp = _format_price
    bias = human_bias_label(res.trend_bias)
    side_f = _side(price, ema_f).lower()
    side_s = _side(price, ema_s).lower()
    header = _pair_header(pair, bias=res.trend_bias)
    ts = _fmt_bucket(bucket)

    def _clean_text(value: Optional[str]) -> str:
        if not value:
            return "No additional commentary."
        cleaned = " ".join(str(value).split()).strip()
        return cleaned or "No additional commentary."

    def _structure_text() -> str:
        if side_f == "above" and side_s == "above":
            return f"Price above EMA{f} and EMA{s}"
        if side_f == "below" and side_s == "below":
            return f"Price below EMA{f} and EMA{s}"
        return f"Price {side_f} EMA{f} and {side_s} EMA{s}"

    def _format_message(*, signal: str, summary: str, extra_lines: Optional[List[str]] = None) -> str:
        parts = [
            f"<b>Daily EMA Alert | {header}</b>",
            "",
            f"<b>Signal:</b> {signal}",
            f"<b>Bias:</b> {bias}",
            "<b>Timeframe:</b> Daily",
            f"<b>Last completed Daily candle:</b> {ts}",
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
            f"<b>Summary:</b> {_clean_text(summary)}",
        ])
        return "\n".join(parts)

    # Crossover alerts only — proximity and imminent-cross alerts removed.
    # Alert fires on a confirmed EMA20/50 crossover (completed Daily candle).
    if res.ema_cross_signal in ("cross_up", "cross_down"):
        is_bullish = res.ema_cross_signal == "cross_up"
        signal = ("🟢 Bullish crossover confirmed on Daily" if is_bullish
                  else "🔴 Bearish crossover confirmed on Daily")
        relation_word = "above" if is_bullish else "below"
        key = f"{pair}:cross:{res.ema_cross_signal}:{bucket}"
        summary = (
            f"EMA{f} has crossed {relation_word} EMA{s} on the latest completed Daily candle. "
            f"Current price is {side_f} EMA{f} and {side_s} EMA{s}. "
            f"{_clean_text(res.notes)}"
        )
        text = _format_message(
            signal=signal,
            summary=summary,
            extra_lines=[
                f"<b>Cross status:</b> EMA{f} {relation_word} EMA{s}",
            ],
        )
        ai_note = _ema_groq_ai_note(res, signal, summary)
        if ai_note:
            text += f"\n{'─' * 32}\n<b>🤖 Groq AI</b>\n{ai_note}"
        alerts.append({"key": key, "text": text, "level": "cross"})

    # Clean alignment alerts — full bullish/bearish stack confirmation.
    # Bullish: EMA fast > EMA slow, completed Daily close above both EMAs,
    # and current price above both EMAs. Bearish is the mirror image.
    clean_alignment: Optional[str] = None
    if (
        res.ema_fast_vs_slow == "above"
        and res.close_vs_fast == "above"
        and res.close_vs_slow == "above"
        and res.current_vs_fast == "above"
        and res.current_vs_slow == "above"
    ):
        clean_alignment = "bullish"
    elif (
        res.ema_fast_vs_slow == "below"
        and res.close_vs_fast == "below"
        and res.close_vs_slow == "below"
        and res.current_vs_fast == "below"
        and res.current_vs_slow == "below"
    ):
        clean_alignment = "bearish"

    if clean_alignment:
        is_bullish = clean_alignment == "bullish"
        signal = ("🟢 Clean bullish EMA alignment" if is_bullish else "🔴 Clean bearish EMA alignment")
        stack_word = "above" if is_bullish else "below"
        key = f"{pair}:clean_alignment:{clean_alignment}:{bucket}"
        state_key = f"{pair}:clean_alignment_state"
        summary = (
            f"Clean {clean_alignment} alignment confirmed: EMA{f} is {stack_word} EMA{s}, "
            f"the completed Daily close is {stack_word} both EMAs, and current price is also "
            f"{stack_word} both EMAs. {_clean_text(res.notes)}"
        )
        text = _format_message(
            signal=signal,
            summary=summary,
            extra_lines=[
                f"<b>Alignment:</b> EMA{f} {stack_word} EMA{s}; close and current price {stack_word} both EMAs",
            ],
        )
        ai_note = _ema_groq_ai_note(res, signal, summary)
        if ai_note:
            text += f"\n{'─' * 32}\n<b>🤖 Groq AI</b>\n{ai_note}"
        alerts.append({
            "key": key,
            "text": text,
            "level": "clean_alignment",
            "state_key": state_key,
            "state_value": clean_alignment,
        })

    return alerts

def dispatch_alerts(
    results: List["EMAAnalysisResult"],
    bot_token: str = TELEGRAM_BOT_TOKEN,
    chat_id: int = TELEGRAM_CHAT_ID,
    dry_run: bool = False,
) -> int:
    """Evaluate all results, fire new alerts, and return the count sent.

    De-duplication is keyed on (pair, signal-type, Daily-bucket) so that the
    same signal is never re-sent within the same Daily candle.  State is
    persisted in TELEGRAM_ALERT_STATE_FILE between cron runs.

    Raises RuntimeError if TELEGRAM_CHAT_ID is missing or invalid and
    ``dry_run`` is False (i.e. alerts would actually be sent).
    """
    # Validate credentials before doing any work so failures are loud & early.
    # Skip validation in dry-run mode since no messages are actually sent.
    if not dry_run and bot_token and not chat_id:
        raise RuntimeError(
            "TELEGRAM_CHAT_ID is required when Telegram alerts are enabled. "
            "Set the environment variable or pass --no-alerts."
        )
    state = _prune_alert_state(_load_alert_state())
    sent = 0

    for res in results:
        if res.suggestion == "warming_up":
            continue

        alerts = build_alerts(res)
        alignment_state_key = f"{res.pair}:clean_alignment_state"
        has_clean_alignment_alert = any(a.get("level") == "clean_alignment" for a in alerts)

        # Reset when clean alignment disappears, so a future fresh same-direction
        # alignment can alert again.
        if not dry_run and not has_clean_alignment_alert and alignment_state_key in state:
            state[alignment_state_key] = {"direction": "none", "ts": iso_z(now_utc())}

        for alert in alerts:
            key = alert["key"]
            state_key = alert.get("state_key")
            state_value = alert.get("state_value")

            if state_key and state_value:
                previous_state = state.get(state_key)
                previous_direction = previous_state.get("direction") if isinstance(previous_state, dict) else None
                if previous_direction == state_value:
                    log.debug("dispatch_alerts: skipping unchanged clean alignment for %s (%s)", res.pair, state_value)
                    continue
            elif key in state:
                log.debug("dispatch_alerts: skipping duplicate key=%s", key)
                continue

            if dry_run:
                log.info("[DRY-RUN] Would send alert key=%s:\n%s", key, alert["text"])
                sent += 1
            else:
                ok = send_telegram_message(bot_token, chat_id, alert["text"])
                if ok:
                    now_iso = iso_z(now_utc())
                    state[key] = now_iso
                    if state_key and state_value:
                        state[state_key] = {"direction": state_value, "ts": now_iso}
                    sent += 1
                    log.info("dispatch_alerts: sent %s alert for %s (key=%s)", alert["level"], res.pair, key)
                else:
                    log.warning("dispatch_alerts: failed to send %s alert for %s", alert["level"], res.pair)

    if not dry_run:
        _save_alert_state(state)
    return sent


# ---------------------------------------------------------------------------
# HTTP session
# ---------------------------------------------------------------------------
def build_session() -> requests.Session:
    retry = Retry(
        total=1,
        backoff_factor=0.8,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=frozenset(["GET"]),
        respect_retry_after_header=True,
    )
    s = requests.Session()
    s.headers.update({
        "User-Agent": USER_AGENT,
        "Accept": "application/json, text/csv, */*",
        "Accept-Language": "en-US,en;q=0.9",
    })
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s


_SESSION: Optional[requests.Session] = None


def get_session() -> requests.Session:
    global _SESSION
    if _SESSION is None:
        _SESSION = build_session()
    return _SESSION


# ---------------------------------------------------------------------------
# Time helpers
# ---------------------------------------------------------------------------
def now_utc() -> datetime:
    return datetime.now(UTC)


def four_hour_bucket(dt: datetime) -> datetime:
    dt = dt.astimezone(UTC)
    bucket_hour = (dt.hour // 4) * 4
    return dt.replace(hour=bucket_hour, minute=0, second=0, microsecond=0)


def iso_z(dt: datetime) -> str:
    return dt.astimezone(UTC).isoformat().replace("+00:00", "Z")


def parse_dt(value: Any) -> Optional[datetime]:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    text = str(value).strip().replace("Z", "+00:00")
    if not text:
        return None
    # Try fromisoformat first (handles most cases), then explicit strptime formats.
    for fmt in [None, "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%dT%H:%M:%S%z",
                "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"]:
        try:
            dt = datetime.fromisoformat(text) if fmt is None else datetime.strptime(text, fmt)
            return dt if dt.tzinfo else dt.replace(tzinfo=UTC)
        except (ValueError, TypeError):
            continue
    log.warning("parse_dt: could not parse %r", value)
    return None


# ---------------------------------------------------------------------------
# JSON I/O
# ---------------------------------------------------------------------------
def load_json(path: Path, default: Any) -> Any:
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return default
    except (json.JSONDecodeError, UnicodeDecodeError, OSError, ValueError) as exc:
        log.warning("load_json(%s): %s — returning default", path.name, exc)
        return default


def save_json(path: Path, data: Any) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    with file_lock(path):
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, path)
            try:
                os.chmod(path, 0o644)
            except OSError:
                pass
        except Exception:
            try:
                tmp.unlink(missing_ok=True)
            except OSError:
                pass
            raise


# ---------------------------------------------------------------------------
# Signal helpers
# ---------------------------------------------------------------------------
def relation(a: float, b: float, eps: float = 1e-12) -> str:
    if a > b + eps:
        return "above"
    if a < b - eps:
        return "below"
    return "at"


def structure_label(vs_fast: str, vs_slow: str) -> str:
    if vs_fast == "above" and vs_slow == "above":
        return "above_fast_slow"
    if vs_fast == "below" and vs_slow == "below":
        return "below_fast_slow"
    if vs_fast == "below" and vs_slow == "above":
        return "below_fast_above_slow"
    if vs_fast == "above" and vs_slow == "below":
        return "above_fast_below_slow"
    return "mixed"


def trend_bias_label(fast_vs_slow: Optional[str]) -> str:
    if fast_vs_slow == "above":
        return "bullish_bias"
    if fast_vs_slow == "below":
        return "bearish_bias"
    return "neutral_bias"


_CROSS_REL_EPS = 1e-6   # 0.0001 % of price — filters floating-point noise without masking real crosses


def human_bias_label(trend_bias: Optional[str]) -> str:
    if not trend_bias:
        return "Neutral"
    label = str(trend_bias).replace("_bias", "").replace("_", " ").strip()
    return label.title() if label else "Neutral"


def detect_cross(prev_fast: float, prev_slow: float, curr_fast: float, curr_slow: float) -> str:
    # Use a relative epsilon scaled to the magnitude of the slow EMA so that
    # sub-pip noise on high-value pairs (XAUUSD ~2000) does not
    # produce phantom cross signals.
    eps = max(abs(curr_slow), abs(prev_slow)) * _CROSS_REL_EPS
    prev_diff = prev_fast - prev_slow
    curr_diff = curr_fast - curr_slow
    # cross_up:   was at/below slow (prev_diff < eps)  and now clearly above (curr_diff > eps)
    # cross_down: was at/above slow (prev_diff > -eps) and now clearly below (curr_diff < -eps)
    if prev_diff < eps and curr_diff > eps:
        return "cross_up"
    if prev_diff > -eps and curr_diff < -eps:
        return "cross_down"
    return "no_cross"


def suggest_label(
    close_vs_fast: Optional[str],
    close_vs_slow: Optional[str],
    fast_vs_slow: Optional[str],
    cross_signal: Optional[str],
    current_vs_fast: Optional[str],
    current_vs_slow: Optional[str],
    ready: bool,
    fast: int = 20,
    slow: int = 50,
) -> Tuple[str, str]:
    f, s = fast, slow
    if not ready:
        return "warming_up", f"EMA{s} is still warming up; not enough completed Daily closes yet."
    if cross_signal == "cross_up":
        note = f"Fresh EMA{f} crossed above EMA{s} on the latest completed Daily close."
        if current_vs_fast == "above" and current_vs_slow == "above":
            note += " Current price confirms above both EMAs."
        elif current_vs_fast is not None and current_vs_slow is not None:
            note += f" Current price is {current_vs_fast} EMA{f} and {current_vs_slow} EMA{s}."
        return "bullish_cross", note
    if cross_signal == "cross_down":
        note = f"Fresh EMA{f} crossed below EMA{s} on the latest completed Daily close."
        if current_vs_fast == "below" and current_vs_slow == "below":
            note += " Current price confirms below both EMAs."
        elif current_vs_fast is not None and current_vs_slow is not None:
            note += f" Current price is {current_vs_fast} EMA{f} and {current_vs_slow} EMA{s}."
        return "bearish_cross", note
    if fast_vs_slow == "above":
        if close_vs_fast == "above" and close_vs_slow == "above":
            notes = [f"Bullish structure: completed Daily close is above EMA{f} and EMA{s}."]
            if current_vs_fast == "above" and current_vs_slow == "above":
                notes.append("Current price also holds above both EMAs.")
            return "bullish", " ".join(notes)
        if close_vs_fast == "below" and close_vs_slow == "above":
            return "bullish_pullback", f"Bullish pullback: completed Daily close is below EMA{f} but above EMA{s}."
        # "at" one or both EMAs while fast > slow — close is testing EMA support
        return "bullish_at_ema", f"Close is at or between EMA{f}/EMA{s} in a bullish EMA stack; price testing EMA level."
    if fast_vs_slow == "below":
        if close_vs_fast == "below" and close_vs_slow == "below":
            notes = [f"Bearish structure: completed Daily close is below EMA{f} and EMA{s}."]
            if current_vs_fast == "below" and current_vs_slow == "below":
                notes.append("Current price also remains below both EMAs.")
            return "bearish", " ".join(notes)
        if close_vs_fast == "above" and close_vs_slow == "below":
            return "bearish_pullback", f"Bearish pullback: completed Daily close is above EMA{f} but below EMA{s}."
        # "at" one or both EMAs while fast < slow — close is testing EMA resistance
        return "bearish_at_ema", f"Close is at or between EMA{f}/EMA{s} in a bearish EMA stack; price testing EMA level."
    # fast_vs_slow == "at": EMAs are converging / essentially equal
    return "neutral", f"EMAs are converging (EMA{f} ≈ EMA{s}); no directional confirmation from EMA{f}/EMA{s} structure."


# ---------------------------------------------------------------------------
# Twelve Data usage budget / cooldown  (cached per-run)
# ---------------------------------------------------------------------------
def _load_twelvedata_usage_fresh() -> Dict[str, Any]:
    data = load_json(TWELVEDATA_USAGE_FILE, {})
    today = now_utc().date().isoformat()
    if data.get("date") != today:
        data = {
            "date": today,
            "count": 0,
            "per_symbol": {},
            "last_request_ts": None,
            "cooldown_until": None,
            "used_windows": [],
            "attempted_windows": [],
        }
    else:
        data.setdefault("count", 0)
        data.setdefault("per_symbol", {})
        data.setdefault("last_request_ts", None)
        data.setdefault("cooldown_until", None)
        data.setdefault("used_windows", [])
        data.setdefault("attempted_windows", [])
    return data


def load_twelvedata_usage() -> Dict[str, Any]:
    global _twelvedata_usage_cache
    if _twelvedata_usage_cache is None:
        _twelvedata_usage_cache = _load_twelvedata_usage_fresh()
    return _twelvedata_usage_cache


def save_twelvedata_usage(data: Dict[str, Any]) -> None:
    global _twelvedata_usage_cache
    _twelvedata_usage_cache = data
    save_json(TWELVEDATA_USAGE_FILE, data)


def _save_twelvedata_usage_locked(data: Dict[str, Any]) -> None:
    """Write usage to disk and update the in-process cache.

    Must be called while already holding file_lock(TWELVEDATA_USAGE_FILE).
    Unlike save_twelvedata_usage this writes directly (atomic tmp+replace) without
    re-acquiring the lock, since save_json itself calls file_lock internally.
    We bypass that by replicating the atomic write inline.
    """
    global _twelvedata_usage_cache
    _twelvedata_usage_cache = data
    tmp = TWELVEDATA_USAGE_FILE.with_suffix(TWELVEDATA_USAGE_FILE.suffix + ".tmp")
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, TWELVEDATA_USAGE_FILE)
        try:
            os.chmod(TWELVEDATA_USAGE_FILE, 0o644)
        except OSError:
            pass
    except Exception:
        try:
            tmp.unlink(missing_ok=True)
        except OSError:
            pass
        raise


def get_twelvedata_requests_today() -> int:
    return int(load_twelvedata_usage().get("count", 0))


def twelvedata_cooldown_active() -> bool:
    usage = load_twelvedata_usage()
    cooldown_until = parse_dt(usage.get("cooldown_until"))
    return cooldown_until is not None and now_utc() < cooldown_until


def get_api_quota_summary() -> Dict[str, Any]:
    """Provider-agnostic quota snapshot for external callers (e.g. scraper.py).

    Returns a flat dict that can be unpacked directly into a JSON payload with
    ``**get_api_quota_summary()``.  The keys are intentionally generic so that
    callers remain decoupled from the underlying provider name.
    """
    return {
        "requests_today":  get_twelvedata_requests_today(),
        "cooldown_active": twelvedata_cooldown_active(),
    }


def set_twelvedata_cooldown(minutes: int, extra_reason: str = "") -> None:
    global _twelvedata_usage_cache
    with file_lock(TWELVEDATA_USAGE_FILE):
        _twelvedata_usage_cache = _load_twelvedata_usage_fresh()
        usage = _twelvedata_usage_cache
        usage["cooldown_until"] = iso_z(now_utc() + timedelta(minutes=minutes))
        if extra_reason:
            usage["last_error"] = extra_reason
        _save_twelvedata_usage_locked(usage)


def _try_reserve_twelvedata_quota(pair: str = "", count: int = 1) -> bool:
    """Atomically check quota and reserve it under the file lock.

    Unlike the read-then-act pattern of ``can_use_twelvedata`` + ``mark_twelvedata_request``,
    this performs both the eligibility check and the increment in a single critical
    section, preventing two concurrent processes from each passing the check and
    both making requests against the same budget slot.

    Returns True if quota was successfully reserved, False otherwise.
    The caller must NOT call ``mark_twelvedata_request`` separately when using this function.
    """
    global _twelvedata_usage_cache
    if not TWELVEDATA_API_KEY:
        return False
    n = max(count, 1)
    with file_lock(TWELVEDATA_USAGE_FILE):
        _twelvedata_usage_cache = _load_twelvedata_usage_fresh()
        usage = _twelvedata_usage_cache
        # Re-check all guards inside the lock so no concurrent process can slip through.
        cooldown_until = parse_dt(usage.get("cooldown_until"))
        if cooldown_until is not None and now_utc() < cooldown_until:
            return False
        if int(usage.get("count", 0)) + n > TWELVEDATA_REQUEST_LIMIT_PER_DAY:
            return False
        if pair:
            per_symbol = int(usage["per_symbol"].get(pair, 0))
            if per_symbol + n > TWELVEDATA_REQUESTS_PER_SYMBOL_PER_DAY:
                return False
        # Reserve by incrementing now — before the HTTP request is made.
        usage["count"] = int(usage.get("count", 0)) + n
        if pair:
            usage["per_symbol"][pair] = int(usage["per_symbol"].get(pair, 0)) + n
        usage["last_request_ts"] = iso_z(now_utc())
        _save_twelvedata_usage_locked(usage)
    return True


def throttle_twelvedata_if_needed() -> None:
    usage = _load_twelvedata_usage_fresh()
    last_ts = parse_dt(usage.get("last_request_ts"))
    if last_ts is None:
        return
    elapsed = (now_utc() - last_ts).total_seconds()
    wait = TWELVEDATA_MIN_SECONDS_BETWEEN_REQUESTS - elapsed
    if wait > 0:
        log.debug("throttle_twelvedata: sleeping %.1fs", wait)
        time.sleep(wait)


# ---------------------------------------------------------------------------
# Session-window helpers
# ---------------------------------------------------------------------------

def _fx_is_open(dt: datetime) -> bool:
    """Return True if dt falls within normal FX trading hours (no holiday check)."""
    d = dt.astimezone(UTC)
    # Weekend: Saturday all day; Sunday before 22:00 UTC
    if d.weekday() == 5:   # Saturday
        return False
    if d.weekday() == 6 and d.hour < 22:  # Sunday pre-open
        return False
    # Friday 22:00 UTC onward is closed
    if d.weekday() == 4 and d.hour >= 22:
        return False
    return True


def current_session_window_key(now: Optional[datetime] = None) -> Optional[str]:
    """Return a unique key for the session window that is open right now, or None.

    A window is open for SESSION_WINDOW_MINUTES after each session's UTC open
    time, but only on FX trading days.  The key is 'session:YYYY-MM-DDTHH:MMZ'
    so it is stable across runs within the same window.
    """
    now = now or now_utc()
    width = timedelta(minutes=SESSION_WINDOW_MINUTES)
    best: Optional[Tuple[datetime, str]] = None
    for name, (tz_name, local_hour, local_minute) in SESSION_STARTS.items():
        tz = ZoneInfo(tz_name)
        local_now = now.astimezone(tz)
        # Check today and yesterday in local time to handle UTC-day boundaries
        for delta_days in (0, -1):
            local_date = local_now.date() + timedelta(days=delta_days)
            local_open = datetime(
                local_date.year, local_date.month, local_date.day,
                local_hour, local_minute, tzinfo=tz,
            )
            utc_open = local_open.astimezone(UTC)
            if not _fx_is_open(utc_open):
                continue
            if utc_open <= now < utc_open + width:
                key = f"{name}:{utc_open.strftime('%Y-%m-%dT%H:%MZ')}"
                if best is None or utc_open > best[0]:  # pylint: disable=unsubscriptable-object
                    best = (utc_open, key)
    return best[1] if best else None


def _pair_window_key(window_key: str, pair: str) -> str:
    """Embed the pair into a window key so each pair gets its own slot.

    e.g. 'london:2026-04-23T07:00Z' + 'EURUSD'
      -> 'london:EURUSD:2026-04-23T07:00Z'

    This allows all 4 pairs to be corrected within the same session window
    without the first pair's attempt blocking the rest.
    """
    parts = window_key.split(":", 1)
    return f"{parts[0]}:{pair.upper()}:{parts[1]}" if len(parts) == 2 else f"{window_key}:{pair.upper()}"


def can_use_session_window(window_key: Optional[str] = None, pair: str = "") -> bool:
    """Return True if a broker correction request is permitted right now for this pair.

    Requires:
      - API key present
      - Not in 429 cooldown
      - Daily count < TWELVEDATA_REQUEST_LIMIT_PER_DAY
      - We are inside an active session window
      - This (window, pair) combination has not already been used or attempted today
    """
    if not TWELVEDATA_API_KEY:
        return False
    usage = _load_twelvedata_usage_fresh()
    cooldown_until = parse_dt(usage.get("cooldown_until"))
    if cooldown_until is not None and now_utc() < cooldown_until:
        return False
    if int(usage.get("count", 0)) + 1 > TWELVEDATA_REQUEST_LIMIT_PER_DAY:
        return False
    wk = window_key or current_session_window_key()
    if wk is None:
        return False
    pk = _pair_window_key(wk, pair) if pair else wk
    if pk in set(usage.get("used_windows", [])):
        return False
    if pk in set(usage.get("attempted_windows", [])):
        return False
    return True


def mark_session_attempt(window_key: str, pair: str = "") -> None:
    global _twelvedata_usage_cache
    with file_lock(TWELVEDATA_USAGE_FILE):
        _twelvedata_usage_cache = _load_twelvedata_usage_fresh()
        usage = _twelvedata_usage_cache
        pk = _pair_window_key(window_key, pair) if pair else window_key
        attempted = list(usage.get("attempted_windows", []))
        if pk not in attempted:
            attempted.append(pk)
        usage["attempted_windows"] = attempted[-_SESSION_WINDOW_HISTORY:]
        usage["last_request_ts"] = iso_z(now_utc())
        _save_twelvedata_usage_locked(usage)


def mark_session_success(window_key: str, pair: str) -> None:
    """Mark a session window as successfully corrected.

    Request quota is reserved by fetch_twelvedata_daily_closes() for each actual
    provider request. This function only tracks the session-window de-dup key.
    """
    global _twelvedata_usage_cache
    with file_lock(TWELVEDATA_USAGE_FILE):
        _twelvedata_usage_cache = _load_twelvedata_usage_fresh()
        usage = _twelvedata_usage_cache
        pk = _pair_window_key(window_key, pair)
        used = list(usage.get("used_windows", []))
        if pk not in used:
            used.append(pk)
        usage["used_windows"] = used[-_SESSION_WINDOW_HISTORY:]
        usage["last_request_ts"] = iso_z(now_utc())
        _save_twelvedata_usage_locked(usage)


# ---------------------------------------------------------------------------
# Local history / snapshots
# ---------------------------------------------------------------------------
def load_closes() -> Dict[str, List[Dict[str, Any]]]:
    data = load_json(CLOSES_FILE, {})
    out: Dict[str, List[Dict[str, Any]]] = {p: [] for p in SUPPORTED_PAIRS}
    for pair, rows in (data or {}).items():
        pair_up = str(pair).upper()
        if pair_up not in out or not isinstance(rows, list):
            continue
        clean: Dict[str, Dict[str, Any]] = {}
        seen_keys: Dict[str, int] = {}   # key → first-seen index, for dup detection
        for idx, row in enumerate(rows):
            if not isinstance(row, dict):
                continue
            dt = parse_dt(row.get("hour"))
            raw_close = row.get("close")
            if raw_close is None:
                continue
            try:
                close = float(raw_close)
            except (TypeError, ValueError):
                continue
            if dt is None:
                continue
            key = iso_z(four_hour_bucket(dt))
            if key in seen_keys:
                log.warning(
                    "load_closes(%s): duplicate Daily bucket %s at rows %d and %d — keeping later value",
                    pair_up, key, seen_keys[key], idx,
                )
            seen_keys[key] = idx
            clean[key] = {"hour": key, "close": close}
        out[pair_up] = [clean[k] for k in sorted(clean.keys())][-MAX_CLOSES_HISTORY:]
    return out


def save_closes(data: Dict[str, List[Dict[str, Any]]]) -> None:
    save_json(CLOSES_FILE, {p: rows[-MAX_CLOSES_HISTORY:] for p, rows in data.items() if p in SUPPORTED_PAIRS})


def append_snapshot(pair: str, ts: datetime, price: float) -> None:
    payload = {"pair": pair.upper(), "ts": iso_z(ts), "price": float(price)}
    with file_lock(SNAPSHOT_FILE):
        with open(SNAPSHOT_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(payload) + "\n")


def trim_snapshot_file(max_days: int = SNAPSHOT_RETENTION_DAYS + 2) -> None:
    """Rewrite SNAPSHOT_FILE keeping only lines within max_days.

    Uses a time-based cutoff on every run (not a file-size gate) so the
    file cannot grow unboundedly on low-traffic deployments.
    """
    if not SNAPSHOT_FILE.exists():
        return
    cutoff = now_utc() - timedelta(days=max_days)
    kept: List[str] = []
    with file_lock(SNAPSHOT_FILE):
        with open(SNAPSHOT_FILE, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    row = json.loads(line)
                    ts = parse_dt(row.get("ts"))
                    if ts and ts >= cutoff:
                        kept.append(line)
                except (json.JSONDecodeError, KeyError, TypeError, ValueError) as exc:
                    log.warning("trim_snapshot_file: skipping malformed line: %s", exc)
                    continue
        tmp = SNAPSHOT_FILE.with_suffix(".tmp")
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                f.write("\n".join(kept))
                if kept:
                    f.write("\n")
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, SNAPSHOT_FILE)
            try:
                os.chmod(SNAPSHOT_FILE, 0o644)
            except OSError:
                pass
        except Exception:
            try:
                tmp.unlink(missing_ok=True)
            except OSError:
                pass
            raise


def load_snapshots(days: int = SNAPSHOT_RETENTION_DAYS) -> List[Dict[str, Any]]:
    cutoff = now_utc() - timedelta(days=days)
    out: List[Dict[str, Any]] = []
    if not SNAPSHOT_FILE.exists():
        return out
    with open(SNAPSHOT_FILE, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError as exc:
                log.warning("load_snapshots: skipping malformed line: %s", exc)
                continue
            pair = str(row.get("pair", "")).upper()
            dt = parse_dt(row.get("ts"))
            try:
                price = float(row.get("price"))
            except (TypeError, ValueError):
                continue
            if pair not in SUPPORTED_PAIRS or dt is None or dt < cutoff:
                continue
            out.append({"pair": pair, "ts": dt.astimezone(UTC), "price": price})
    out.sort(key=lambda x: (x["pair"], x["ts"]))
    return out


def rebuild_stooq_closes(snapshots: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    completed_cutoff = four_hour_bucket(now_utc())
    grouped: Dict[str, Dict[str, Dict[str, Any]]] = {p: {} for p in SUPPORTED_PAIRS}
    for snap in snapshots:
        pair = snap["pair"]
        bucket = four_hour_bucket(snap["ts"])
        if bucket >= completed_cutoff:
            continue
        key = iso_z(bucket)
        prev = grouped[pair].get(key)
        if prev is None or (parse_dt(prev["ts"]) or datetime.min.replace(tzinfo=UTC)) <= snap["ts"]:
            grouped[pair][key] = {"hour": key, "close": float(snap["price"]), "ts": iso_z(snap["ts"])}
    out = {p: [] for p in SUPPORTED_PAIRS}
    for pair in SUPPORTED_PAIRS:
        rows = [{"hour": v["hour"], "close": v["close"]} for _, v in sorted(grouped[pair].items())]
        out[pair] = rows[-MAX_CLOSES_HISTORY:]
    return out


def merge_close_histories(base_rows: List[Dict[str, Any]], overlay_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    merged: Dict[str, Dict[str, Any]] = {}
    for row in base_rows:
        merged[row["hour"]] = {"hour": row["hour"], "close": float(row["close"])}
    for row in overlay_rows:
        merged[row["hour"]] = {"hour": row["hour"], "close": float(row["close"])}
    return [merged[k] for k in sorted(merged.keys())][-MAX_CLOSES_HISTORY:]


# ---------------------------------------------------------------------------
# Stooq — yfinance-compatible interface
#
# Drop-in replacement for yfinance.Ticker / yf.download() backed by Stooq.
#
# Usage (mirrors yfinance):
#
#   ticker = StooqTicker("eurusd")          # yf.Ticker("EURUSD=X")
#   df     = ticker.history(period="5d")    # → pd.DataFrame OHLCV, tz-aware UTC index
#   price  = ticker.fast_info["last_price"] # → float
#   info   = ticker.info                    # → dict with regularMarketPrice, etc.
#
#   # bulk download (mirrors yf.download)
#   df = StooqDownloader.download("eurusd", period="1mo", interval="1d")
#   df = StooqDownloader.download(["eurusd","gbpusd"], period="5d")
# ---------------------------------------------------------------------------

STOOQ_MAX_QUOTE_AGE_DAYS = 2  # reject quotes older than this many calendar days

# Map yfinance-style period strings → approximate calendar days to fetch
_PERIOD_DAYS: Dict[str, int] = {
    "1d": 1, "5d": 5, "1wk": 7, "1mo": 35, "3mo": 95,
    "6mo": 185, "1y": 370, "2y": 740, "5y": 1830,
    "10y": 3660, "ytd": 370, "max": 3660,
}

# Map yfinance-style interval strings → Stooq interval param
_INTERVAL_MAP: Dict[str, str] = {
    "1m": "1",   "5m": "5",   "15m": "15",  "30m": "30",
    "1h": "60",  "4h": "240", "1d": "d",    "1wk": "w",
    "1mo": "m",
}


class StooqFastInfo:
    """Mirrors yfinance.FastInfo with lazy Stooq-backed attributes."""

    def __init__(self, ticker: "StooqTicker") -> None:
        self._ticker = ticker
        self._cache: Dict[str, Any] = {}

    def _ensure(self) -> None:
        if self._cache:
            return
        row = self._ticker._fetch_quote_row()
        if row is None:
            return
        for yf_key, stooq_key in [
            ("last_price",  "Close"),
            ("open",        "Open"),
            ("day_high",    "High"),
            ("day_low",     "Low"),
            ("volume",      "Volume"),
        ]:
            raw = row.get(stooq_key)
            if raw not in (None, "", "N/D"):
                try:
                    self._cache[yf_key] = float(raw)
                except (TypeError, ValueError):
                    pass
        raw_date = row.get("Date", "")
        raw_time = row.get("Time", "")
        if raw_date:
            ts_str = f"{raw_date} {raw_time}".strip()
            dt = parse_dt(ts_str)
            if dt:
                self._cache["last_fetch_time"] = dt

    def __getitem__(self, key: str) -> Any:
        self._ensure()
        if key not in self._cache:
            raise KeyError(key)
        return self._cache[key]

    def get(self, key: str, default: Any = None) -> Any:
        self._ensure()
        return self._cache.get(key, default)

    def __contains__(self, key: str) -> bool:
        self._ensure()
        return key in self._cache

    def __repr__(self) -> str:  # pragma: no cover
        self._ensure()
        return f"StooqFastInfo({self._cache})"


class StooqTicker:
    """
    Mirrors ``yfinance.Ticker`` backed by Stooq.

    Parameters
    ----------
    symbol : str
        Stooq symbol, e.g. ``"eurusd"``, ``"gbpusd"``, ``"btc.v"``.
    """

    def __init__(self, symbol: str) -> None:
        self.ticker = symbol.lower().strip()
        self._quote_row: Optional[Dict[str, str]] = None
        self._quote_fetched: bool = False
        self._info_cache: Optional[Dict[str, Any]] = None
        self.fast_info: StooqFastInfo = StooqFastInfo(self)

    # ── raw quote (lazy, cached per instance) ─────────────────────────────

    def _fetch_quote_row(self, retries: int = 2) -> Optional[Dict[str, str]]:
        if self._quote_fetched:
            return self._quote_row
        url = f"https://stooq.com/q/l/?s={self.ticker}&f=sd2t2ohlcv&h&e=csv"
        for attempt in range(retries):
            try:
                session = get_session()
                resp = session.get(url, timeout=10)
                resp.raise_for_status()
                rows = list(csv.DictReader(StringIO(resp.text)))
                row = rows[-1] if rows else None
                if row and row.get("Close") not in (None, "", "N/D"):
                    raw_date = row.get("Date", "")
                    if raw_date:
                        quote_dt = parse_dt(raw_date)
                        if quote_dt is not None:
                            age = (now_utc() - quote_dt).total_seconds() / 86400
                            if age > STOOQ_MAX_QUOTE_AGE_DAYS:
                                log.warning(
                                    "StooqTicker(%s): stale quote date=%s (%.1fd) — skipping",
                                    self.ticker, raw_date, age,
                                )
                                self._quote_fetched = True
                                self._quote_row = None
                                return None
                    self._quote_row = row
                    self._quote_fetched = True
                    return row
            except requests.exceptions.RequestException as exc:
                log.warning("StooqTicker(%s): network error: %s", self.ticker, exc)
            except (csv.Error, UnicodeDecodeError, IndexError, ValueError) as exc:
                log.warning("StooqTicker(%s): unexpected error: %s", self.ticker, exc)
            if attempt < retries - 1:
                time.sleep(1.5)
        log.info("StooqTicker(%s): quote unavailable after %d attempts", self.ticker, retries)
        self._quote_fetched = True
        self._quote_row = None
        return None

    # ── .info — mirrors yf.Ticker.info ────────────────────────────────────

    @property
    def info(self) -> Dict[str, Any]:
        """Return a dict resembling ``yfinance.Ticker.info``."""
        if self._info_cache is not None:
            return self._info_cache
        row = self._fetch_quote_row()
        out: Dict[str, Any] = {
            "symbol":               self.ticker.upper(),
            "exchange":             "STOOQ",
            "quoteType":            "CURRENCY",
            "regularMarketPrice":   None,
            "regularMarketOpen":    None,
            "regularMarketDayHigh": None,
            "regularMarketDayLow":  None,
            "regularMarketVolume":  None,
            "regularMarketTime":    None,
            "previousClose":        None,
        }
        if row:
            def _f(key: str) -> Optional[float]:
                v = row.get(key)
                if v in (None, "", "N/D"):
                    return None
                try:
                    return float(v)
                except (TypeError, ValueError):
                    return None

            out["regularMarketPrice"]   = _f("Close")
            out["regularMarketOpen"]    = _f("Open")
            out["regularMarketDayHigh"] = _f("High")
            out["regularMarketDayLow"]  = _f("Low")
            out["regularMarketVolume"]  = _f("Volume")
            raw_date = row.get("Date", "")
            raw_time = row.get("Time", "")
            if raw_date:
                ts_str = f"{raw_date} {raw_time}".strip()
                dt = parse_dt(ts_str)
                if dt:
                    out["regularMarketTime"] = dt.isoformat()
        self._info_cache = out
        return out

    # ── .history() — mirrors yf.Ticker.history() ──────────────────────────

    def history(
        self,
        period: str = "1mo",
        interval: str = "1d",
        start: Optional[str] = None,
        end: Optional[str] = None,
        timeout: int = DEFAULT_TIMEOUT,
    ) -> "pd.DataFrame":
        """
        Fetch OHLCV bars from Stooq.

        Parameters mirror ``yfinance.Ticker.history()``:
          period   : "1d","5d","1mo","3mo","6mo","1y","2y","5y","max"
          interval : "1m","5m","15m","30m","1h","4h","1d","1wk","1mo"
          start    : "YYYY-MM-DD"  (overrides period)
          end      : "YYYY-MM-DD"  (overrides period)

        Returns
        -------
        pd.DataFrame
            Columns: Open, High, Low, Close, Volume
            Index  : DatetimeIndex (UTC, tz-aware), named "Datetime"
        """
        stooq_interval = _INTERVAL_MAP.get(interval, "d")
        days = _PERIOD_DAYS.get(period, 35)

        # Build date range
        end_dt   = parse_dt(end)   if end   else now_utc()
        start_dt = parse_dt(start) if start else (end_dt - timedelta(days=days))

        url = (
            f"https://stooq.com/q/d/l/"
            f"?s={self.ticker}"
            f"&d1={start_dt.strftime('%Y%m%d')}"
            f"&d2={end_dt.strftime('%Y%m%d')}"
            f"&i={stooq_interval}"
        )
        try:
            session = get_session()
            resp = session.get(url, timeout=timeout)
            resp.raise_for_status()
            df = pd.read_csv(StringIO(resp.text))
        except requests.exceptions.RequestException as exc:
            log.warning("StooqTicker(%s).history: network error: %s", self.ticker, exc)
            return _empty_ohlcv_df()
        except (pd.errors.ParserError, UnicodeDecodeError, ValueError) as exc:
            log.warning("StooqTicker(%s).history: parse error: %s", self.ticker, exc)
            return _empty_ohlcv_df()

        return _normalise_stooq_df(df, self.ticker)

    # ── convenience: current price (mirrors fast_info["last_price"]) ───────

    def current_price(self) -> Tuple[Optional[float], str]:
        """
        Return ``(price, source_label)`` — convenience wrapper used by the
        pipeline.  Equivalent to ``fast_info.get("last_price")``.
        """
        price = self.fast_info.get("last_price")
        if price is not None:
            return price, "stooq_latest"
        return None, "unavailable"


class StooqDownloader:
    """
    Mirrors ``yfinance.download()`` as a class-method interface.

    Examples
    --------
    >>> df = StooqDownloader.download("eurusd", period="1mo", interval="1d")
    >>> df = StooqDownloader.download(["eurusd", "gbpusd"], period="5d")
    """

    @staticmethod
    def download(
        tickers: "str | List[str]",
        period: str = "1mo",
        interval: str = "1d",
        start: Optional[str] = None,
        end: Optional[str] = None,
        timeout: int = DEFAULT_TIMEOUT,
    ) -> "pd.DataFrame":
        """
        Download OHLCV data for one or more Stooq symbols.

        Single ticker  → DataFrame with columns Open/High/Low/Close/Volume.
        Multiple tickers → DataFrame with MultiIndex columns (field, ticker),
                           matching the ``yf.download(group_by="ticker")`` layout.
        """
        if isinstance(tickers, str):
            tickers = [tickers]
        frames: Dict[str, "pd.DataFrame"] = {}
        for sym in tickers:
            t = StooqTicker(sym)
            frames[sym.upper()] = t.history(
                period=period, interval=interval,
                start=start, end=end, timeout=timeout,
            )
        if len(frames) == 1:
            return next(iter(frames.values()))
        # Multi-ticker: build MultiIndex columns (field, ticker) like yf.download
        combined = pd.concat(frames, axis=1)
        combined.columns.names = ["Ticker", "Price"]
        return combined.swaplevel(axis=1).sort_index(axis=1)


# ── internal helpers ───────────────────────────────────────────────────────

def _empty_ohlcv_df() -> "pd.DataFrame":
    """Return an empty DataFrame with the standard OHLCV column schema."""
    df = pd.DataFrame(columns=["Open", "High", "Low", "Close", "Volume"])
    df.index = pd.DatetimeIndex([], tz="UTC", name="Datetime")
    return df


def _normalise_stooq_df(df: "pd.DataFrame", symbol: str) -> "pd.DataFrame":
    """
    Normalise a raw Stooq CSV DataFrame to the yfinance column/index schema.

    Stooq columns: Date, Open, High, Low, Close, Volume
    Output  : DatetimeIndex (UTC) named "Datetime", columns Open/High/Low/Close/Volume
    """
    if df.empty or "Close" not in df.columns:
        log.warning("_normalise_stooq_df(%s): empty or missing Close column", symbol)
        return _empty_ohlcv_df()

    # Stooq intraday CSVs include a "Time" column; daily CSVs do not.
    if "Time" in df.columns:
        df["_dt"] = pd.to_datetime(
            df["Date"].astype(str) + " " + df["Time"].astype(str), utc=True, errors="coerce"
        )
    else:
        df["_dt"] = pd.to_datetime(df["Date"], utc=True, errors="coerce")

    df = df.dropna(subset=["_dt"]).copy()
    df = df.set_index("_dt")
    df.index.name = "Datetime"
    df = df.sort_index()

    # Keep only OHLCV; coerce to numeric, drop entirely-NaN rows
    ohlcv_cols = [c for c in ["Open", "High", "Low", "Close", "Volume"] if c in df.columns]
    df = df[ohlcv_cols].apply(pd.to_numeric, errors="coerce")
    df = df.dropna(subset=["Close"])

    # Reject stale data (last bar older than STOOQ_MAX_QUOTE_AGE_DAYS)
    if not df.empty:
        last_bar_age = (now_utc() - df.index[-1].to_pydatetime()).total_seconds() / 86400
        if last_bar_age > STOOQ_MAX_QUOTE_AGE_DAYS + 3:  # allow weekend gap
            log.warning(
                "_normalise_stooq_df(%s): last bar is %.1f days old — data may be stale",
                symbol, last_bar_age,
            )
    return df


# ── pipeline-facing wrapper (unchanged call signature) ────────────────────

def fetch_stooq_current_price(symbol: str) -> Tuple[Optional[float], str]:
    """
    Thin wrapper kept for pipeline compatibility.
    Delegates to ``StooqTicker.current_price()``.
    """
    return StooqTicker(symbol).current_price()


# ---------------------------------------------------------------------------
# Twelve Data fetch (safe / throttled / non-fatal)
# ---------------------------------------------------------------------------
def _pick(row: Dict[str, Any], *keys: str) -> Any:
    """Return the first present value from row for any of keys."""
    for key in keys:
        if key in row:
            return row[key]
    return None


def _normalise_market_date(value: Any) -> str:
    """Normalise Twelve Data date values to YYYY-MM-DD where possible."""
    raw = str(value or "").strip()
    if len(raw) == 8 and raw.isdigit():
        return f"{raw[:4]}-{raw[4:6]}-{raw[6:]}"
    if len(raw) >= 10 and raw[4:5] == "-" and raw[7:8] == "-":
        return raw[:10]
    return raw


def _extract_market_rows(payload: Any) -> List[Dict[str, Any]]:
    """Extract quote rows from common Twelve Data JSON shapes."""
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]
    if isinstance(payload, dict):
        for key in ("Quotes", "quotes", "values", "Values", "Data", "data", "Results", "results", "Items", "items"):
            rows = payload.get(key)
            if isinstance(rows, list):
                return [row for row in rows if isinstance(row, dict)]
        if any(k in payload for k in ("Close", "close", "Date", "date", "datetime")):
            return [payload]
    return []


def _rows_to_daily_closes(rows: List[Dict[str, Any]], lookback_bars: int) -> List[Dict[str, Any]]:
    """Convert provider rows into the script's [{'hour': iso_z, 'close': float}] format.

    Today's daily candle is excluded even if a provider returns an in-progress row.
    """
    today = now_utc().date()
    closes: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        date_value = _pick(row, "Date", "date", "Datetime", "datetime", "Timestamp", "timestamp")
        date_text = _normalise_market_date(date_value)
        dt = parse_dt(date_text)
        if dt is None:
            continue
        dt = dt.astimezone(UTC)
        if dt.date() >= today:
            continue
        raw_close = _pick(row, "Close", "close", "AdjClose", "adj_close", "Adj Close")
        if raw_close is None:
            continue
        try:
            close = float(raw_close)
        except (TypeError, ValueError):
            continue
        bucket = four_hour_bucket(dt)
        closes[iso_z(bucket)] = {"hour": iso_z(bucket), "close": close}

    return [closes[k] for k in sorted(closes.keys())][-max(lookback_bars, SEED_CLOSES_TARGET):]


def _fetch_twelvedata_daily_closes_direct(pair: str, lookback_bars: int, timeout: int) -> Tuple[List[Dict[str, Any]], str]:
    """Fetch daily closes from Twelve Data without doing quota bookkeeping."""
    pair_up = pair.upper()
    if pair_up not in PAIR_TO_TD_SYMBOL:
        return [], f"unsupported_pair:{pair_up}"
    if not TWELVEDATA_API_KEY:
        return [], "no_twelvedata_api_key"

    td_symbol = PAIR_TO_TD_SYMBOL[pair_up]
    outputsize = max(lookback_bars, SEED_CLOSES_TARGET, 120)
    session = get_session()
    try:
        resp = session.get(
            f"{TWELVEDATA_BASE}/time_series",
            params={
                "symbol": td_symbol,
                "interval": "1day",
                "outputsize": outputsize,
                "timezone": "UTC",
                "apikey": TWELVEDATA_API_KEY,
            },
            timeout=timeout,
        )
        if resp.status_code == 429:
            set_twelvedata_cooldown(TWELVEDATA_429_COOLDOWN_MINUTES, f"429_twelvedata:{pair_up}")
            return [], "twelvedata_429_cooldown"
        resp.raise_for_status()
        payload = resp.json()
    except requests.exceptions.RequestException as exc:
        msg = str(exc)
        if "429" in msg:
            set_twelvedata_cooldown(TWELVEDATA_429_COOLDOWN_MINUTES, f"retry429_twelvedata:{pair_up}")
            return [], "twelvedata_429_retry_cooldown"
        log.warning("_fetch_twelvedata_daily_closes_direct(%s): %s", pair_up, msg[:120])
        return [], f"twelvedata_request_error:{msg[:120]}"
    except (json.JSONDecodeError, ValueError) as exc:
        log.warning("_fetch_twelvedata_daily_closes_direct(%s): unexpected: %s", pair_up, exc)
        return [], f"twelvedata_unexpected_error:{str(exc)[:120]}"

    if isinstance(payload, dict) and payload.get("status") == "error":
        msg = str(payload.get("message", "unknown"))
        log.warning("_fetch_twelvedata_daily_closes_direct(%s): API error: %s", pair_up, msg)
        return [], f"twelvedata_api_error:{msg[:120]}"

    rows = _extract_market_rows(payload)
    ordered = _rows_to_daily_closes(rows, lookback_bars)
    if not ordered:
        return [], "twelvedata_no_bars"
    return ordered, "twelvedata_ok"


def fetch_twelvedata_daily_closes(pair: str, lookback_bars: int = 120, timeout: int = DEFAULT_TIMEOUT) -> Tuple[List[Dict[str, Any]], str]:
    """Fetch daily closes from Twelve Data as the only broker-grade source."""
    pair_up = pair.upper()
    if pair_up not in SUPPORTED_PAIRS:
        return [], f"unsupported_pair:{pair_up}"
    if not TWELVEDATA_API_KEY:
        return [], "no_twelvedata_api_key"

    throttle_twelvedata_if_needed()
    if not _try_reserve_twelvedata_quota(pair_up, 1):
        return [], "twelvedata_budget_or_cooldown"

    rows, status = _fetch_twelvedata_daily_closes_direct(pair_up, lookback_bars, timeout)
    if rows:
        return rows, status
    return [], status


def needs_seed_or_repair(rows: List[Dict[str, Any]]) -> Tuple[bool, str]:
    if not rows:
        return True, "missing_history"
    if len(rows) < SEED_CLOSES_TARGET:
        return True, f"history_too_short:{len(rows)}"
    last_hour = parse_dt(rows[-1]["hour"])
    if last_hour is None:
        return True, "bad_last_hour"
    expected_last_completed = four_hour_bucket(now_utc()) - timedelta(hours=4)
    gap_hours = max(0, int((expected_last_completed - last_hour).total_seconds() // 3600))
    if gap_hours >= REPAIR_GAP_HOURS:
        return True, f"gap_{gap_hours}h"
    return False, "ok"


def fetch_session_correction(pair: str, window_key: str, timeout: int = DEFAULT_TIMEOUT) -> Tuple[List[Dict[str, Any]], str]:
    """Fetch a fresh Daily history during an active session window.

    Twelve Data is the only broker-grade source.
    The session-window gate prevents repeated corrections for the same pair/window.
    """
    pair_up = pair.upper()
    if pair_up not in SUPPORTED_PAIRS:
        return [], f"unsupported_pair:{pair_up}"
    if not TWELVEDATA_API_KEY:
        return [], "no_twelvedata_api_key"
    if not can_use_session_window(window_key, pair_up):
        return [], "window_unavailable"

    mark_session_attempt(window_key, pair_up)
    fetched, status = fetch_twelvedata_daily_closes(pair_up, lookback_bars=max(SEED_CLOSES_TARGET * 2, 120), timeout=timeout)
    if fetched:
        mark_session_success(window_key, pair_up)
        log.info("fetch_session_correction(%s): session=%s source=%s bars=%d", pair_up, window_key, status, len(fetched))
        return fetched[-MAX_CLOSES_HISTORY:], status

    log.warning("fetch_session_correction(%s): no usable Daily bars for session=%s (%s)", pair_up, window_key, status)
    return [], status


def repair_pair_history(pair: str, local_rows: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], str]:
    """Repair or correct Daily history for one pair.

    Priority:
      1. Session-window correction — if a session window is open, always fetch
         broker-grade candles regardless of whether the history looks healthy.
         This is the main mechanism for reducing snapshot reconstruction error.
      2. Gap repair — if outside all windows but history has a significant gap,
         fall back to the existing Twelve Data fetch (uses the broader can_use_twelvedata
         budget check, not the window gate).
    """
    window_key = current_session_window_key()
    if window_key and can_use_session_window(window_key, pair.upper()):
        fetched, status = fetch_session_correction(pair, window_key)
        if fetched:
            # Twelve Data is overlay so broker-grade candles win over Stooq snapshots on the same bucket.
            return merge_close_histories(local_rows, fetched), f"session_corrected:{window_key}:{status}"
        # Window fetch failed — log but don't fall through to gap repair in the
        # same run (avoid double-spending quota on a transient error).
        log.warning("repair_pair_history(%s): session fetch failed (%s), skipping gap repair this run", pair, status)
        return local_rows, f"session_fetch_failed:{status}"

    # Outside all session windows — only repair genuine gaps.
    need, reason = needs_seed_or_repair(local_rows)
    if not need:
        return local_rows, "local_ok"
    fetched, status = fetch_twelvedata_daily_closes(pair)
    if fetched:
        # Twelve Data is overlay so broker-grade candles win over Stooq snapshots on the same bucket.
        return merge_close_histories(local_rows, fetched), f"twelvedata_{reason}"
    return local_rows, f"repair_skipped:{reason}:{status}"


# ---------------------------------------------------------------------------
# EMA computation  (dynamic fast/slow periods)
# ---------------------------------------------------------------------------
def compute_ema_state(
    pair: str,
    rows: List[Dict[str, Any]],
    current_price: Optional[float],
    source_basis: str,
    notes_prefix: str = "",
    fast: int = 20,
    slow: int = 50,
) -> EMAAnalysisResult:
    pair_up = pair.upper()

    def _result_stub(**kwargs) -> EMAAnalysisResult:
        """Shared defaults for early-return paths."""
        defaults = dict(
            pair=pair_up, timeframe="Daily", completed_closes=0,
            last_completed_hour="", last_close=None,
            fast_period=fast, slow_period=slow,
            ema_fast=None, ema_slow=None,
            close_vs_fast=None, close_vs_slow=None, close_structure=None,
            ema_fast_vs_slow=None, ema_cross_signal=None,
            current_price=round(float(current_price), 6) if current_price is not None else None,
            current_vs_fast=None, current_vs_slow=None, current_structure=None,
            trend_bias="neutral_bias", suggestion="warming_up", notes="",
            twelvedata_requests_today=get_twelvedata_requests_today(),
            source_basis=source_basis,
        )
        defaults.update(kwargs)
        return EMAAnalysisResult(**defaults)

    if not rows:
        suggestion, warming_note = suggest_label(None, None, None, None, None, None, ready=False, fast=fast, slow=slow)
        note = (notes_prefix + " ").strip() + "No completed Daily close history available yet."
        full_note = (note + " " + warming_note).strip()
        return _result_stub(suggestion=suggestion, notes=full_note)

    df = pd.DataFrame(rows)
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df = df.dropna(subset=["close"]).copy()
    df["hour"] = pd.to_datetime(df["hour"], utc=True)
    df = df.sort_values("hour").reset_index(drop=True)

    df["ema_fast"] = df["close"].ewm(span=fast, adjust=False).mean()
    df["ema_slow"] = df["close"].ewm(span=slow, adjust=False).mean()

    last = df.iloc[-1]
    last_close = float(last["close"])
    last_hour_text = iso_z(last["hour"].to_pydatetime())

    # Warm-up: require slow * EMA_WARMUP_MULTIPLIER bars so the slow EMA has
    # had enough history to stabilise.  Default multiplier is 2 (configurable
    # via EMA_WARMUP_MULTIPLIER env var).
    ready = len(df) >= slow * EMA_WARMUP_MULTIPLIER

    if not ready:
        suggestion, note = suggest_label(None, None, None, None, None, None, ready=False, fast=fast, slow=slow)
        full_note = (notes_prefix + " " + note).strip()
        return _result_stub(
            completed_closes=int(len(df)),
            last_completed_hour=last_hour_text,
            last_close=round(last_close, 6),
            ema_fast=round(float(last["ema_fast"]), 6),
            ema_slow=round(float(last["ema_slow"]), 6),
            suggestion=suggestion,
            notes=full_note,
        )

    prev = df.iloc[-2] if len(df) >= 2 else None
    ema_fast = float(last["ema_fast"])
    ema_slow = float(last["ema_slow"])

    close_vs_fast = relation(last_close, ema_fast)
    close_vs_slow = relation(last_close, ema_slow)
    close_structure = structure_label(close_vs_fast, close_vs_slow)
    fast_vs_slow = relation(ema_fast, ema_slow)
    cross_signal = (
        detect_cross(float(prev["ema_fast"]), float(prev["ema_slow"]), ema_fast, ema_slow)
        if prev is not None else "no_cross"
    )
    current_vs_fast = relation(float(current_price), ema_fast) if current_price is not None else None
    current_vs_slow = relation(float(current_price), ema_slow) if current_price is not None else None
    current_structure = structure_label(current_vs_fast, current_vs_slow) if current_vs_fast and current_vs_slow else None

    suggestion, note = suggest_label(
        close_vs_fast, close_vs_slow, fast_vs_slow, cross_signal,
        current_vs_fast, current_vs_slow, ready=True, fast=fast, slow=slow,
    )
    full_note = (notes_prefix + " " + note).strip()

    return EMAAnalysisResult(
        pair=pair_up,
        timeframe="Daily",
        completed_closes=int(len(df)),
        last_completed_hour=last_hour_text,
        last_close=round(last_close, 6),
        fast_period=fast,
        slow_period=slow,
        ema_fast=round(ema_fast, 6),
        ema_slow=round(ema_slow, 6),
        close_vs_fast=close_vs_fast,
        close_vs_slow=close_vs_slow,
        close_structure=close_structure,
        ema_fast_vs_slow=fast_vs_slow,
        ema_cross_signal=cross_signal,
        current_price=round(float(current_price), 6) if current_price is not None else None,
        current_vs_fast=current_vs_fast,
        current_vs_slow=current_vs_slow,
        current_structure=current_structure,
        trend_bias=trend_bias_label(fast_vs_slow),
        suggestion=suggestion,
        notes=full_note,
        twelvedata_requests_today=get_twelvedata_requests_today(),
        source_basis=source_basis,
    )


# ---------------------------------------------------------------------------
# Per-run workflow
# ---------------------------------------------------------------------------
def update_histories_with_stooq_snapshots(
    pairs: List[str],
) -> Tuple[Dict[str, List[Dict[str, Any]]], Dict[str, Optional[float]], Dict[str, str], Dict[str, str]]:
    local_closes = load_closes()
    current_prices: Dict[str, Optional[float]] = {p: None for p in pairs}
    current_sources: Dict[str, str] = {p: "unavailable" for p in pairs}
    repair_notes: Dict[str, str] = {p: "" for p in pairs}

    for pair in pairs:
        fetch_ts = now_utc()
        price, src = fetch_stooq_current_price(PAIR_TO_STOOQ_SYMBOL[pair])
        current_prices[pair] = price
        current_sources[pair] = src
        if price is not None:
            append_snapshot(pair, fetch_ts, price)
            log.info("%s stooq price: %s", pair, price)
        else:
            log.warning("%s stooq price: unavailable", pair)

    # Bucket logic uses a single consistent "now" captured after all fetches so
    # that prev_bucket and cur_bucket reflect the wall-clock time at which
    # history merging begins, not the start of the (potentially slow) fetch loop.
    now = now_utc()

    # Merge Stooq Daily closes into local history.
    # Fast-path: if local history already has a completed Daily close for the
    # *previous* bucket we only need to inject the snapshot for that bucket
    # rather than scanning the full SNAPSHOT_RETENTION_DAYS file.  This
    # avoids an O(N) file scan on every 30-min cron run once seeded.
    prev_bucket = four_hour_bucket(now) - timedelta(hours=4)
    prev_bucket_key = iso_z(prev_bucket)
    all_pairs_have_prev = all(
        any(r["hour"] == prev_bucket_key for r in local_closes.get(p, []))
        for p in pairs
    )

    if all_pairs_have_prev:
        # Staleness guard: if the newest local Daily close is more than 26 hours
        # behind the previous completed bucket, a run was skipped or delayed.
        # Force a full snapshot rebuild so those gaps are not silently dropped.
        def _newest_close_dt(rows: List[Dict[str, Any]]) -> Optional[datetime]:
            dts = [parse_dt(r["hour"]) for r in rows if parse_dt(r["hour"]) is not None]
            return max(dts) if dts else None

        for p in pairs:
            newest = _newest_close_dt(local_closes.get(p, []))
            if newest is not None and (now - newest).total_seconds() > 4.5 * 3600:
                log.info(
                    "%s fast-path aborted: newest Daily close is %.1fh old — forcing full snapshot rebuild",
                    p, (now - newest).total_seconds() / 3600,
                )
                all_pairs_have_prev = False
                break

    if all_pairs_have_prev:
        # Lightweight: only record the snapshot for the *current* (incomplete)
        # bucket — it will become a completed close in the next run.
        cur_bucket = four_hour_bucket(now)
        cur_key = iso_z(cur_bucket)
        for pair in pairs:
            price = current_prices[pair]
            if price is not None:
                candidate = {"hour": cur_key, "close": price}
                existing = {r["hour"]: r for r in local_closes.get(pair, [])}
                existing[cur_key] = candidate
                local_closes[pair] = [existing[k] for k in sorted(existing.keys())][-MAX_CLOSES_HISTORY:]
    else:
        # Full rebuild from snapshots (seeding or gap recovery).
        snapshots = load_snapshots()
        stooq_closes = rebuild_stooq_closes(snapshots)
        for pair in pairs:
            local_closes[pair] = merge_close_histories(local_closes.get(pair, []), stooq_closes.get(pair, []))

    # Repair / session-correct Daily history. Session corrections fire once per
    # session window per pair; gap repair is limited to one Twelve Data fetch per run.
    #
    # Startup-seed exception: pairs with no usable history at all
    # ("missing_history" or "history_too_short") are seeded unconditionally on
    # the first run regardless of twelvedata_used_this_run.  The per-request throttle
    # (TWELVEDATA_MIN_SECONDS_BETWEEN_REQUESTS) and the daily budget check inside
    # _try_reserve_twelvedata_quota still apply, so quota is never over-spent.
    # Gap repairs (history exists but has holes) remain capped at one per run
    # to avoid burning quota on every cron tick during steady state.
    twelvedata_used_this_run = False
    active_window = current_session_window_key()
    for pair in pairs:
        in_session = active_window is not None and can_use_session_window(active_window, pair)
        if not in_session:
            need, why = needs_seed_or_repair(local_closes.get(pair, []))
            if not need:
                repair_notes[pair] = "local_ok"
                continue
            is_cold_start = why == "missing_history" or why.startswith("history_too_short")
            if twelvedata_used_this_run and not is_cold_start:
                repair_notes[pair] = f"deferred:{why}"
                log.info("%s repair deferred (Twelve Data already used this run): %s", pair, why)
                continue
        repaired, note = repair_pair_history(pair, local_closes.get(pair, []))
        local_closes[pair] = repaired
        repair_notes[pair] = note
        log.info("%s repair: %s", pair, note)
        # Charge the Twelve Data run-limit only for gap-repair fetches, not session
        # corrections and not cold-start seeds (those must all complete).
        if not in_session and not note.startswith("repair_skipped") and note != "local_ok":
            if not is_cold_start:
                twelvedata_used_this_run = True
        if twelvedata_cooldown_active():
            break

    save_closes(local_closes)
    return local_closes, current_prices, current_sources, repair_notes


def run_analysis(
    pairs: List[str],
    fast: int = 20,
    slow: int = 50,
) -> Tuple[List[EMAAnalysisResult], Dict[str, str]]:
    histories, current_prices, current_sources, repair_notes = update_histories_with_stooq_snapshots(pairs)
    results: List[EMAAnalysisResult] = []
    errors: Dict[str, str] = {}
    for pair in pairs:
        try:
            notes_prefix = repair_notes.get(pair, "")
            if "session_corrected" in notes_prefix:
                source_basis = "local_closes + broker_session_correction + stooq_latest"
            elif current_sources.get(pair) != "unavailable":
                source_basis = "local_closes + stooq_latest"
            else:
                source_basis = "local_closes"
            results.append(
                compute_ema_state(
                    pair, histories.get(pair, []), current_prices.get(pair),
                    source_basis, notes_prefix=notes_prefix, fast=fast, slow=slow,
                )
            )
        except Exception as exc:
            log.error("compute_ema_state(%s): %s", pair, exc, exc_info=True)
            errors[pair] = str(exc)
    return results, errors


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------
def print_result(res: EMAAnalysisResult) -> None:
    f, s = res.fast_period, res.slow_period
    print("=" * 84)
    print(f"PAIR                  : {res.pair}")
    print(f"TIMEFRAME             : {res.timeframe}")
    print(f"COMPLETED DAILY CLOSES: {res.completed_closes}")
    print(f"LAST COMPLETED HOUR   : {res.last_completed_hour}")
    print(f"LAST DAILY CLOSE      : {res.last_close}")
    print(f"EMA {f:<4} (fast)       : {res.ema_fast}")
    print(f"EMA {s:<4} (slow)       : {res.ema_slow}")
    print("-" * 84)
    print(f"CLOSE vs EMA{f:<4}      : {res.close_vs_fast}")
    print(f"CLOSE vs EMA{s:<4}      : {res.close_vs_slow}")
    print(f"CLOSE STRUCTURE       : {res.close_structure}")
    print(f"EMA{f} vs EMA{s}         : {res.ema_fast_vs_slow}")
    print(f"EMA CROSS SIGNAL      : {res.ema_cross_signal}")
    print("-" * 84)
    print(f"CURRENT PRICE         : {res.current_price}")
    print(f"CURRENT vs EMA{f:<4}    : {res.current_vs_fast}")
    print(f"CURRENT vs EMA{s:<4}    : {res.current_vs_slow}")
    print(f"CURRENT STRUCTURE     : {res.current_structure}")
    print("-" * 84)
    print(f"TREND BIAS            : {res.trend_bias}")
    print(f"SUGGESTION            : {res.suggestion}")
    print(f"NOTES                 : {res.notes}")
    print(f"TWELVEDATA REQUESTS TODAY     : {res.twelvedata_requests_today}/{TWELVEDATA_REQUEST_LIMIT_PER_DAY}")
    print(f"SOURCE BASIS          : {res.source_basis}")
    print("=" * 84)


def print_summary(results: List[EMAAnalysisResult]) -> None:
    print("=" * 116)
    print(f"{'PAIR':<10} {'SUGGESTION':<18} {'TREND_BIAS':<15} {'D_CLOSES':<10} "
          f"{'CLOSE_vs':<22} {'CROSS':<12} {'CURRENT_vs':<18} {'TD_USED':<10}")
    print("-" * 116)
    for res in results:
        f, s = res.fast_period, res.slow_period
        close_vs = (
            f"{f}:{res.close_vs_fast},{s}:{res.close_vs_slow}"
            if res.close_vs_fast and res.close_vs_slow
            else "warming_up"
        )
        current_vs = (
            f"{f}:{res.current_vs_fast},{s}:{res.current_vs_slow}"
            if res.current_vs_fast and res.current_vs_slow
            else "unavailable"
        )
        print(
            f"{res.pair:<10} {res.suggestion:<18} {res.trend_bias:<15} "
            f"{res.completed_closes:<10} {close_vs:<22} "
            f"{(res.ema_cross_signal or 'n/a'):<12} {current_vs:<18} "
            f"{res.twelvedata_requests_today:<10}"
        )
    print("=" * 116)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main() -> None:
    ensure_required_output_files()
    parser = argparse.ArgumentParser(
        description="Robust EMA engine (Twelve Data broker daily corrections + Stooq live snapshots)."
    )
    parser.add_argument("--pair", default=None, help="Single pair: EURUSD, GBPUSD, XAUUSD")
    parser.add_argument("--all", action="store_true", help="Scan all supported pairs")
    parser.add_argument("--fast", type=int, default=20, help="Fast EMA period (default: 20)")
    parser.add_argument("--slow", type=int, default=50, help="Slow EMA period (default: 50)")
    parser.add_argument("--json", action="store_true", help="Print JSON output")
    parser.add_argument("--session-info", action="store_true", help="Print session window status and exit")
    parser.add_argument("--no-alerts", action="store_true", help="Disable Telegram alerts for this run")
    parser.add_argument("--dry-run-alerts", action="store_true",
                        help="Print alerts that would be sent without actually sending them")
    args = parser.parse_args()

    if args.session_info:
        now = now_utc()
        wk = current_session_window_key(now)
        usage = load_twelvedata_usage()
        per_pair = {
            p: can_use_session_window(wk, p) if wk else False
            for p in SUPPORTED_PAIRS
        }
        print(json.dumps({
            "now_utc": iso_z(now),
            "active_window": wk,
            "can_use_per_pair": per_pair,
            "used_windows": usage.get("used_windows", []),
            "attempted_windows": usage.get("attempted_windows", []),
            "td_requests_today": get_twelvedata_requests_today(),
            "cooldown_active": twelvedata_cooldown_active(),
            "sessions": {
                name: {"tz": tz, "local_open": f"{h:02d}:{m:02d}"}
                for name, (tz, h, m) in SESSION_STARTS.items()
            },
        }, indent=2))
        return

    if args.fast <= 0 or args.slow <= 0 or args.fast >= args.slow:
        raise SystemExit("Error: --fast must be > 0 and strictly less than --slow")

    # Validate Telegram credentials early so a misconfigured TELEGRAM_CHAT_ID
    # causes a clean error before any network I/O, unless --no-alerts was passed.
    resolved_chat_id = TELEGRAM_CHAT_ID
    if not args.no_alerts:
        try:
            resolved_chat_id = _resolve_telegram_chat_id(_chat_id_raw)
        except RuntimeError as exc:
            raise SystemExit(f"Configuration error: {exc}") from exc

    if args.all or not args.pair:
        pairs = list(SUPPORTED_PAIRS)
    else:
        pair = args.pair.replace("/", "").replace("-", "").upper()
        if pair not in SUPPORTED_PAIRS:
            raise SystemExit(f"Unsupported pair: {pair}. Supported: {', '.join(SUPPORTED_PAIRS)}")
        pairs = [pair]

    results, errors = run_analysis(pairs, fast=args.fast, slow=args.slow)

    # ── Telegram alerts ────────────────────────────────────────────────────
    if not args.no_alerts:
        n_sent = dispatch_alerts(
            results,
            bot_token=TELEGRAM_BOT_TOKEN,
            chat_id=resolved_chat_id,
            dry_run=args.dry_run_alerts,
        )
        if n_sent:
            log.info("Telegram: %d alert(s) dispatched", n_sent)
    # ──────────────────────────────────────────────────────────────────────
    state_payload = {
        "generated_at": iso_z(now_utc()),
        "ema_periods": {"fast": args.fast, "slow": args.slow},
        "twelvedata_requests_today": get_twelvedata_requests_today(),
        "twelvedata_cooldown_active": twelvedata_cooldown_active(),
        "results": {res.pair: asdict(res) for res in results},
        "errors": errors,
    }
    save_json(STATE_FILE, state_payload)
    trim_snapshot_file()

    if args.json:
        print(json.dumps(state_payload, indent=2))
    else:
        if len(results) == 1:
            print_result(results[0])
        else:
            print_summary(results)
        if errors:
            print("\nErrors:")
            for pair, msg in errors.items():
                print(f"  {pair}: {msg}")


if __name__ == "__main__":
    main()

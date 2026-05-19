#!/usr/bin/env python3

import copy
import csv
import json
import logging
import math
import os
import re
import threading
import time
import uuid
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, wait as _futures_wait, ALL_COMPLETED
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import requests
from bs4 import BeautifulSoup

try:
    from dotenv import load_dotenv
    _env_path = Path(__file__).resolve().parent / ".env"
    load_dotenv(dotenv_path=_env_path, override=False)
except ImportError:
    pass                                                                          

def _missing_momentum_diagnostics():
    return {}


try:
    # Canonical file updated in this workflow:
    # momentum.py uses Twelve Data for H1 momentum data.
    from momentum import (
        fetch_price_momentum as _fetch_price_momentum,
        get_momentum as _get_momentum,
        momentum_blind as _momentum_blind,
        momentum_summary as _momentum_summary,
        detect_h1_ema_cross_and_price_state as _detect_h1_ema,
        get_last_ema_state as _get_last_ema_state,
    )
    try:
        from momentum import get_last_momentum_diagnostics as _get_last_momentum_diagnostics
    except ImportError:
        _get_last_momentum_diagnostics = _missing_momentum_diagnostics
    _MOMENTUM_OK = True
    _MOMENTUM_MODULE = "momentum"
except ImportError:
    try:
        # Backward compatibility only. Prefer momentum.py above.
        from momentum_updated import (
            fetch_price_momentum as _fetch_price_momentum,
            get_momentum as _get_momentum,
            momentum_blind as _momentum_blind,
            momentum_summary as _momentum_summary,
            detect_h1_ema_cross_and_price_state as _detect_h1_ema,
            get_last_ema_state as _get_last_ema_state,
        )
        try:
            from momentum_updated import get_last_momentum_diagnostics as _get_last_momentum_diagnostics
        except ImportError:
            _get_last_momentum_diagnostics = _missing_momentum_diagnostics
        _MOMENTUM_OK = True
        _MOMENTUM_MODULE = "momentum_updated"
    except ImportError:
        _MOMENTUM_OK = False
        _MOMENTUM_MODULE = "unavailable"
        def _fetch_price_momentum(pairs=None, api_key=""):
            return {}, {}
        def _detect_h1_ema(pair, api_key="", bars=None):
            return None
        def _get_last_ema_state():
            return {}
        def _get_momentum(pair, macro):
            return macro.get("price_momentum", {}).get(pair, 0.0)
        def _momentum_blind(macro):
            return macro.get("data_quality", {}).get("penalties", {}).get("momentum_blind", False)
        def _momentum_summary(macro):
            return {}
        _get_last_momentum_diagnostics = _missing_momentum_diagnostics


try:
    # Canonical file updated in this workflow:
    # pivot.py uses the modern Twelve Data API for OHLC data.
    from pivot import fetch_price_context as _fetch_price_context
    _PRICE_OK = True
    _PIVOT_MODULE = "pivot"
except ImportError:
    try:
        # Backward compatibility only. Prefer pivot.py above.
        from pivot_updated import fetch_price_context as _fetch_price_context
        _PRICE_OK = True
        _PIVOT_MODULE = "pivot_updated"
    except ImportError:
        _PRICE_OK = False
        _PIVOT_MODULE = "unavailable"
        def _fetch_price_context(pair: str, macro_score: float, **kw):
            return None

def _pivot_ohlc_from_macro(pair: str, macro: Dict) -> Optional[Dict]:
    if _normalize_pair_code(pair) not in ACTIVE_MAIN_PAIRS:
        return None

    pair_l = pair.lower().strip()
    daily = macro.get("_price_daily", {}).get(pair_l)
    session_ohlc = macro.get("_price_session_ohlc", {}).get(pair_l)

    def _ohlc_valid(d: object) -> bool:
        """Reject bars with a near-zero or impossible Low (e.g. corrupted Stooq xauusd bar)."""
        if not isinstance(d, dict):
            return False
        try:
            o = float(d["open"]); h = float(d["high"])
            l = float(d["low"]);  c = float(d["close"])
        except (KeyError, TypeError, ValueError):
            return False
        if h <= 0 or l <= 0:
            return False
        if h < l or not (l <= o <= h) or not (l <= c <= h):
            return False
        # Reject bars where H/L range exceeds 50 % of High — same rule as
        # pivot.py _valid_ohlc — so corrupted data is caught here before it
        # reaches _fetch_price_context and causes the whole context to fall
        # back to "unavailable" (e.g. Stooq returning Low≈0.00022 for xauusd).
        if (h - l) / h > 0.50:
            log.warning(
                "_pivot_ohlc_from_macro[%s]: rejecting OHLC with implausible "
                "H/L range (H=%.5f L=%.5f ratio=%.2f) — will fall through to API",
                pair, h, l, (h - l) / h,
            )
            return False
        return True

    base = daily if _ohlc_valid(daily) else None
    if base is None:
        base = session_ohlc if _ohlc_valid(session_ohlc) else None
    if base is None:
        return None

    ohlc = dict(base)
    session_dict = session_ohlc if isinstance(session_ohlc, dict) else {}

    def _attach(out_key: str, *values) -> None:
        for value in values:
            if value is not None:
                ohlc[out_key] = value
                return

    _attach(
        "current_price",
        macro.get("_price_current", {}).get(pair_l),
        session_dict.get("close"),
        ohlc.get("close"),
    )
    _attach(
        "session_high",
        macro.get("_price_session_high", {}).get(pair_l),
        session_dict.get("high"),
    )
    _attach(
        "session_low",
        macro.get("_price_session_low", {}).get(pair_l),
        session_dict.get("low"),
    )

    if "_date" not in ohlc:
        _attach(
            "_date",
            macro.get("_price_date", {}).get(pair_l),
            session_dict.get("_date"),
            session_dict.get("date"),
        )

    return ohlc

try:
    import lxml as _lxml
    _BS4_PARSER = "lxml"  # _lxml import succeeded; ref retained to silence pyflakes
except ImportError:
    _BS4_PARSER = "html.parser"

UTC = timezone.utc
log = logging.getLogger(__name__)

_MACRO_STARTUP_LOGGED = False

def _run_id() -> str:
    return os.environ.get("SCRAPER_RUN_ID") or os.environ.get("RUN_ID") or "local"

def _log_macro_startup_once() -> None:
    global _MACRO_STARTUP_LOGGED
    if _MACRO_STARTUP_LOGGED:
        return
    _MACRO_STARTUP_LOGGED = True
    log.info("[startup][macro][run_id=%s] keys={fred:%s twelvedata:%s} modules={momentum:%s pivot:%s} ok={momentum:%s pivot:%s} cache_file=%s component_file=%s calendar=%s",
             _run_id(),
             "present" if bool(FRED_API_KEY) else "missing",
             "present" if os.environ.get("TWELVEDATA_API_KEY") else "missing",
             _MOMENTUM_MODULE, _PIVOT_MODULE,
             _MOMENTUM_OK, _PRICE_OK, CACHE_FILE, COMPONENT_CACHE_FILE, CALENDAR_JSON)

MAX_WORKERS = 6

MACRO_DEBUG = os.environ.get("MACRO_DEBUG", "0") == "1"

MACRO_VIX_DEAD = float(os.environ.get("MACRO_VIX_DEAD", "12"))
MACRO_VIX_RISK_OFF = float(os.environ.get("MACRO_VIX_RISK_OFF", "24"))

MACRO_GOLD_ENHANCED_ENABLED = os.environ.get("MACRO_GOLD_ENHANCED_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")

MACRO_SCORE_MIN = float(os.environ.get("MACRO_SCORE_MIN", "0.8"))
MACRO_SCORE_STRONG = float(os.environ.get("MACRO_SCORE_STRONG", "1.2"))
MACRO_DQ_MIN = float(os.environ.get("MACRO_DQ_MIN", "0.4"))

_RE_SAFE_NUMBER = re.compile(r"[-+]?(?:\d[\d,]*\.?\d*|\.\d+)(?:[eE][-+]?\d+)?")

CACHE_FILE = os.environ.get("MACRO_CACHE_FILE", "./macro_cache.json")
COMPONENT_CACHE_FILE = os.environ.get("MACRO_COMPONENT_CACHE", "./macro_components.json")
CALENDAR_JSON = os.environ.get("MACRO_CALENDAR_JSON", "./calendar.json")
CACHE_TTL = int(os.environ.get("MACRO_CACHE_TTL", "1800"))
HTTP_TIMEOUT = 10                                       
HTTP_TIMEOUT_SLOW = 15                                                            

def configure(output_dir: str) -> None:
    global CACHE_FILE, COMPONENT_CACHE_FILE, CALENDAR_JSON, ACTIVE_MAIN_PAIRS
    d = output_dir.rstrip("/")
    CACHE_FILE = os.environ.get(
        "MACRO_CACHE_FILE", f"{d}/macro_cache.json"
    )
    COMPONENT_CACHE_FILE = os.environ.get(
        "MACRO_COMPONENT_CACHE", f"{d}/macro_components.json"
    )
    CALENDAR_JSON = os.environ.get(
        "MACRO_CALENDAR_JSON", f"{d}/calendar.json"
    )
    ACTIVE_MAIN_PAIRS = _load_active_main_pairs()
    log.info(
        f"macro configured: output_dir={d} cache={CACHE_FILE} "
        f"components={COMPONENT_CACHE_FILE} calendar={CALENDAR_JSON} "
        f"main_pairs={sorted(ACTIVE_MAIN_PAIRS)}"
    )

FRED_API_KEY = os.environ.get("FRED_API_KEY", "")

if not FRED_API_KEY:
    logging.getLogger(__name__).warning(
        "macro: FRED_API_KEY is not set -- dependent fetchers may use fallbacks"
    )

PMI_NEUTRAL: float = 50.0
PMI_SCALE: float = 5.0
VIX_NEUTRAL_FALLBACK: float = 20.0
SCORE_MAX: float = 3.0

DEFAULT_MAIN_PAIRS: Tuple[str, ...] = ("eurusd", "gbpusd", "xauusd")
ACTIVE_MAIN_PAIRS: set[str] = set(DEFAULT_MAIN_PAIRS)

def _normalize_pair_code(value: str) -> str:
    return re.sub(r"[^a-z0-9]", "", (value or "").lower())

def _parse_main_pairs(value: Any) -> List[str]:
    if isinstance(value, str):
        raw_items = [v.strip() for v in value.split(",") if v.strip()]
    elif isinstance(value, (list, tuple, set)):
        raw_items = [str(v).strip() for v in value if str(v).strip()]
    else:
        raw_items = []

    seen = set()
    pairs: List[str] = []
    for item in raw_items:
        code = _normalize_pair_code(item)
        if code and code not in seen:
            pairs.append(code)
            seen.add(code)
    return pairs

def _load_active_main_pairs() -> set[str]:
    env_pairs = _parse_main_pairs(os.environ.get("MACRO_MAIN_PAIRS", ""))
    if env_pairs:
        return set(env_pairs)

    cfg_path = os.environ.get("SCRAPER_CONFIG", "").strip()
    if cfg_path:
        try:
            with open(cfg_path, encoding="utf-8") as f:
                cfg = json.load(f)
            cfg_pairs = _parse_main_pairs((cfg or {}).get("main_pairs", []))
            if cfg_pairs:
                return set(cfg_pairs)
        except Exception as exc:
            log.warning(f"macro main_pairs config load failed: {exc}")

    return set(DEFAULT_MAIN_PAIRS)

_INFLATION_PROXIES: Dict[str, float] = {
    "aggressive_hiking": 3.0, "hiking": 3.0, "neutral": 2.5,
    "cutting": 2.0, "aggressive_cutting": 2.0,
}

_FACTOR_WEIGHTS_DEFAULT: Dict[str, float] = {
    "rate": 1.0, "curve": 0.0, "risk": 0.8, "diff": 0.0,
    "momentum": 1.0, "real_yield": 0.0, "growth": 0.15,
    "usd": 0.7, "liquidity": 0.0,
}

_WEIGHT_CLAMP = (-2.0, 2.0)

def _load_factor_weights() -> Dict[str, float]:
    raw = os.environ.get("MACRO_FACTOR_WEIGHTS", "").strip()
    if not raw:
        return dict(_FACTOR_WEIGHTS_DEFAULT)
    try:
        overrides = json.loads(raw)
        if not isinstance(overrides, dict):
            raise ValueError("must be a JSON object")
    except (json.JSONDecodeError, ValueError) as exc:
        log.warning(f"MACRO_FACTOR_WEIGHTS invalid JSON ({exc}) -- using defaults")
        return dict(_FACTOR_WEIGHTS_DEFAULT)

    weights = dict(_FACTOR_WEIGHTS_DEFAULT)
    for key, val in overrides.items():
        if key not in _FACTOR_WEIGHTS_DEFAULT:
            log.warning(f"MACRO_FACTOR_WEIGHTS: unknown key {key!r} ignored")
            continue
        try:
            fval = float(val)
        except (TypeError, ValueError):
            log.warning(f"MACRO_FACTOR_WEIGHTS: non-numeric value for {key!r} ignored")
            continue
        clamped = max(_WEIGHT_CLAMP[0], min(_WEIGHT_CLAMP[1], fval))
        if clamped != fval:
            log.warning(f"MACRO_FACTOR_WEIGHTS: {key}={fval} clamped to {clamped}")
        weights[key] = clamped

    log.info(f"MACRO_FACTOR_WEIGHTS loaded from env: {weights}")
    return weights

FACTOR_WEIGHTS: Dict[str, float] = _load_factor_weights()

COMPONENT_TTLS: Dict[str, int] = {
    "us_fed_rate_pair": 21600,                                        
    "us_yield_data": 7200,                                          
    "tips_yield": 21600,                                    
    "pmi": 3600,                                             
    "eu_pmi": 3600,                                                                 
    "uk_pmi": 3600,                                                                 
    "eu_ecb_rate": 21600,                                            
    "gb_boe_rate": 21600,                                            
    "vix": 900,                                                    
    "spx": 900,
    # Manufacturing PMI calendar enrichment (Stooq → calendar.json in-memory)
    "pmi_stooq_cal_usd": 21600,
    "pmi_stooq_cal_eur": 21600,
    "pmi_stooq_cal_gbp": 21600,
}

_DEFAULT_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/146.0.0.0 Safari/537.36 Edg/146.0.3856.62"
)
_MACRO_UA = os.environ.get("SCRAPER_UA", _DEFAULT_UA)

_BROWSER_HEADERS = {
    "User-Agent": _MACRO_UA,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Cache-Control": "no-cache",
    "Sec-Ch-Ua": '"Microsoft Edge";v="146", "Chromium";v="146", "Not_A Brand";v="99"',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": '"Windows"',
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
}

_ATOM_NS = "http://www.w3.org/2005/Atom"
_TREAS_NS = "http://schemas.microsoft.com/ado/2007/08/dataservices"

_RATE_BOUNDS = (-1.0, 20.0)
_VIX_BOUNDS = (3.0, 90.0)
_YIELD_BOUNDS = (-3.0, 25.0)
_PMI_BOUNDS = (20.0, 70.0)

_RE_PMI_VALUE = re.compile(r"\b(\d{2}(?:\.\d{1,2})?)(?!\d)")
_RE_PREV_PRIOR = re.compile(r"\b(previous|prior|last month|revised)\b", re.IGNORECASE)
_RE_RATE_RANGE = re.compile(
    r"(\d+(?:\.\d+)?)\s*(?:to|-)\s*(\d+(?:\.\d+)?)\s*percent", re.IGNORECASE
)
_RE_RATE_TE = re.compile(r"^(\d{1,2}\.\d{1,2})%?$")
_RE_RATE_CONTEXT = re.compile(
    r"(?:interest|bank|base|policy|key).*?rate[^.]{0,80}?(\d{1,2}\.\d{1,2})\s*%",
    re.IGNORECASE,
)
_RE_BOE_RATE = re.compile(r"^(\d+\.\d{1,4})$")
_RE_NUMERIC_CLEAN = re.compile(r"[^0-9.\-]")
_RE_PMI_TE = re.compile(r"^(\d{2}(?:\.\d{1,2})?)$")

_PMI_KIND_PRIORITY = {
    "composite": 0,
    "manufacturing": 1,
    "services": 2,
    "generic": 3,
}

_PMI_STOOQ_SYMBOLS: Dict[str, Dict[str, str]] = {
    "USD": {
        "composite": "PMCPUS",
        "manufacturing": "PMMNUS",
    },
    "EUR": {
        "manufacturing": "PMMNEU",
        "services": "PMSREU",
    },
    "GBP": {
        "manufacturing": "PMMNUK",
        "services": "PMSRUK",
    },
}

_PMI_STOOQ_PREFERENCE: Dict[str, Tuple[str, ...]] = {
    "USD": ("composite", "manufacturing"),
    "EUR": ("manufacturing", "services"),
    "GBP": ("manufacturing", "services"),
}

# ---------------------------------------------------------------------------
# Manufacturing PMI calendar enrichment — Stooq symbols
#
# These three symbols are fetched once per build cycle (TTL = 6 h via
# component cache) and used to backfill blank *actual* fields in the
# in-memory calendar before fetch_eu_pmi / fetch_uk_pmi / fetch_pmi read it.
# Distinct from _PMI_STOOQ_SYMBOLS above (which drives bias-scoring fallback).
# ---------------------------------------------------------------------------
_PMI_CAL_STOOQ_SYMBOLS: Dict[str, str] = {
    "USD": "pmmnus.m",
    "EUR": "pmmneu.m",
    "GBP": "pmmnuk.m",
}

PMI_CAL_STOOQ_ENABLED: bool = (
    os.environ.get("PMI_CAL_STOOQ_ENABLED", "1").strip().lower()
    in ("1", "true", "yes", "on")
)

# Country-level Eurozone PMI inputs from Stooq.
# These complement headline EUR PMI and are blended into EURUSD growth scoring.
_EU_COUNTRY_PMI_STOOQ: Dict[str, Dict[str, Any]] = {
    "DE": {"country": "Germany",     "symbol": "PMMNDE", "sector": "Manufacturing & exports", "weight": 0.32},
    "FR": {"country": "France",      "symbol": "PMMNFR", "sector": "Services & consumer demand", "weight": 0.20},
    "IT": {"country": "Italy",       "symbol": "PMMNIT", "sector": "Manufacturing", "weight": 0.15},
    "ES": {"country": "Spain",       "symbol": "PMMNES", "sector": "Services / volatile swings", "weight": 0.12},
    "IE": {"country": "Ireland",     "symbol": "PMMNIE", "sector": "Tech & pharma exports", "weight": 0.08},
    "AT": {"country": "Austria",     "symbol": "PMMNAT", "sector": "Manufacturing / supply chains", "weight": 0.05},
    "NL": {"country": "Netherlands", "symbol": "PMMNNL", "sector": "Trade & logistics", "weight": 0.08},
}
EU_COUNTRY_PMI_SCORE_SCALE: float = 5.0
EU_COUNTRY_PMI_GROWTH_WEIGHT: float = float(os.environ.get("EU_COUNTRY_PMI_GROWTH_WEIGHT", "0.60"))

def _normalize_stooq_macro_symbol(symbol: str) -> str:
    symbol = (symbol or "").strip().upper()
    if not symbol:
        return ""
    if "." not in symbol:
        symbol = f"{symbol}.M"
    return symbol.lower()

def _classify_pmi_event_title(title: str) -> Tuple[str, bool, bool]:
    tl = (title or "").lower()
    is_flash = "flash" in tl
    is_final = "final" in tl
    if "composite" in tl or "output index" in tl:
        kind = "composite"
    elif "services" in tl or "service" in tl or "non-manufacturing" in tl or "non manufacturing" in tl:
        kind = "services"
    elif "manufacturing" in tl or "mfg" in tl or "factory" in tl or "ism manufacturing" in tl:
        kind = "manufacturing"
    else:
        kind = "generic"
    return kind, is_flash, is_final

def _pmi_event_score(kind: str, is_flash: bool, is_final: bool) -> Tuple[int, int, int]:
    release_rank = 0 if is_final else 1 if is_flash else 2
    kind_rank = _PMI_KIND_PRIORITY.get(kind, 9)
    return (release_rank, kind_rank, 0)

def _fetch_pmi_from_stooq(currency: str, label: str) -> Optional[Tuple[float, str, str]]:
    curr = (currency or "").upper()
    symbols = _PMI_STOOQ_SYMBOLS.get(curr, {})
    prefs = _PMI_STOOQ_PREFERENCE.get(curr, tuple(symbols.keys()))
    for kind in prefs:
        base_symbol = symbols.get(kind)
        if not base_symbol:
            continue
        symbol = _normalize_stooq_macro_symbol(base_symbol)
        val, ok = _fetch_stooq(symbol, f"{label}_{kind}", _PMI_BOUNDS)
        if ok and _PMI_BOUNDS[0] <= val <= _PMI_BOUNDS[1]:
            log.info(f"[API OK] {label}: {val} (Stooq/{symbol}, kind={kind})")
            return round(val, 1), kind, symbol.upper()
    return None


def fetch_eu_country_pmi() -> Dict[str, Any]:
    """Fetch country-level Eurozone PMI values from Stooq and aggregate them."""
    countries: Dict[str, Dict[str, Any]] = {}
    weighted_sum = 0.0
    valid_weight = 0.0
    for code, cfg in _EU_COUNTRY_PMI_STOOQ.items():
        symbol = _normalize_stooq_macro_symbol(str(cfg.get("symbol", "")))
        val, ok = _fetch_stooq(symbol, f"eu_country_pmi_{code.lower()}", _PMI_BOUNDS)
        weight = float(cfg.get("weight", 0.0) or 0.0)
        countries[code] = {
            "country": cfg.get("country"), "sector": cfg.get("sector"),
            "symbol": symbol.upper(), "weight": weight,
            "value": round(float(val), 1) if ok else None, "valid": bool(ok),
        }
        if ok:
            weighted_sum += float(val) * weight
            valid_weight += weight
    if valid_weight > 0:
        weighted_pmi = round(weighted_sum / valid_weight, 2)
        score = round(max(min((weighted_pmi - PMI_NEUTRAL) / EU_COUNTRY_PMI_SCORE_SCALE, 1.5), -1.5), 3)
    else:
        weighted_pmi = PMI_NEUTRAL
        score = 0.0
    result = {
        "countries": countries, "weighted_pmi": weighted_pmi, "score": score,
        "valid": valid_weight > 0, "valid_weight": round(valid_weight, 3),
        "source": "Stooq/country_pmi" if valid_weight > 0 else "unavailable",
    }
    if result["valid"]:
        log.info("[API OK] eu_country_pmi: weighted=%.2f score=%+.3f valid_weight=%.2f", weighted_pmi, score, valid_weight)
    else:
        log.warning("[API FAIL] eu_country_pmi: all Stooq country PMI sources unavailable")
    return result

_calendar_run_cache: Dict = {}
# TTL for the in-process calendar cache (seconds).  Matches CACHE_TTL so that
# a long-running process (e.g. daemon mode) re-reads calendar.json after each
# macro rebuild cycle rather than serving permanently stale event data.
_CALENDAR_RUN_CACHE_TTL: int = CACHE_TTL

def _get_calendar_data() -> Dict:
    now_t = time.time()
    cached_at = _calendar_run_cache.get("cached_at", 0.0)
    if "data" in _calendar_run_cache and (now_t - cached_at) < _CALENDAR_RUN_CACHE_TTL:
        return _calendar_run_cache["data"]
    # Cache miss or expired — (re)load from disk.
    try:
        with open(CALENDAR_JSON, encoding="utf-8") as f:
            _calendar_run_cache["data"] = json.load(f)
        _calendar_run_cache["cached_at"] = now_t
        log.info(f"[API OK] calendar.json: loaded from {CALENDAR_JSON} "
                 f"({len(_calendar_run_cache['data'].get('events', []))} events)")
    except FileNotFoundError:
        log.debug(f"calendar.json not found at {CALENDAR_JSON}")
        _calendar_run_cache["data"] = {}
        _calendar_run_cache["cached_at"] = now_t
    except Exception as exc:
        log.warning(f"calendar.json load error: {exc}")
        _calendar_run_cache["data"] = {}
        _calendar_run_cache["cached_at"] = now_t
    return _calendar_run_cache["data"]

def _safe(v: float, bounds: Tuple[float, float],
          fallback: float = 0.0, name: str = "") -> float:
    if bounds[0] <= v <= bounds[1]:
        return v
    log.warning(f"safe: {name!r} {v} outside {bounds} -> {fallback}")
    return fallback

def _safe_rate(v, n=""): return _safe(float(v), _RATE_BOUNDS, 0.0, n)

def _safe_yield(v, n=""): return _safe(float(v), _YIELD_BOUNDS, 0.0, n)

def _safe_pmi(v, n=""): return _safe(float(v), _PMI_BOUNDS, PMI_NEUTRAL, n)


def _apply_vix_scaling(factors: Dict[str, float],
                       vix: float = VIX_NEUTRAL_FALLBACK,
                       ) -> Dict[str, float]:
    result = dict(factors)
    if vix > 30:
        if "risk" in result:
            result["risk"] = round(result["risk"] * 1.2, 3)
        if "momentum" in result:
            result["momentum"] = round(result["momentum"] * 0.5, 3)
        log.debug(f"vix_scaling: vix={vix:.1f} > 30 -> risk amplified, momentum dampened")
    return result


_HIGH_STRESS_VIX: float = 35.0
_STRESS_SUPPRESS_KEYS: Tuple[str, ...] = ("growth", "liquidity")

def _stress_deactivate(factors: Dict[str, float],
                       vix: float) -> Dict[str, float]:
    if vix <= _HIGH_STRESS_VIX:
        return factors
    stress_mult = max(0.1, 1.0 - (vix - _HIGH_STRESS_VIX) / 30.0)
    result = dict(factors)
    for key in _STRESS_SUPPRESS_KEYS:
        if key in result:
            result[key] = round(result[key] * stress_mult, 3)
    log.debug(f"stress_deactivate: vix={vix:.1f} mult={stress_mult:.2f}")
    return result

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

_thread_local = threading.local()

def _make_macro_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(_BROWSER_HEADERS)
    s.mount("https://", HTTPAdapter(max_retries=Retry(
        total=3, backoff_factor=0.8,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset(["GET"]),
        respect_retry_after_header=True,
    )))
    s.mount("http://", HTTPAdapter(max_retries=Retry(
        total=2, backoff_factor=0.5,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=frozenset(["GET"]),
    )))
    return s

def _get_thread_session() -> requests.Session:
    if not hasattr(_thread_local, "session"):
        _thread_local.session = _make_macro_session()
    return _thread_local.session

def _get(url, timeout=HTTP_TIMEOUT, max_retries=2):
    headers = {}
    if "api." in url or "format=json" in url or "file_type=json" in url:
        headers["Accept"] = "application/json, text/plain, */*"
    session = _get_thread_session()
    try:
        for attempt in range(max_retries + 1):
            try:
                r = session.get(url, timeout=timeout, headers=headers)
                if r.ok:
                    _ = r.content
                    return r
                log.debug(f"_get({url}): HTTP {r.status_code}")
                if r.status_code < 500:
                    return None
                if attempt < max_retries:
                    log.debug(f"_get({url}) attempt {attempt+1}: HTTP {r.status_code}, retrying")
                    time.sleep(1.5)
            except Exception as exc:
                if attempt < max_retries:
                    log.debug(f"_get({url}) attempt {attempt+1}: {exc}")
                    time.sleep(1.5)
                else:
                    log.debug(f"_get({url}): {exc}")
    except Exception:
        pass
    return None

def _get_json(url, timeout=HTTP_TIMEOUT):
    r = _get(url, timeout)
    if r is None:
        return None
    try:
        return json.loads(r.content)
    except Exception:
        return None

def _load_component_cache():
    try:
        with open(COMPONENT_CACHE_FILE, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def _save_component_cache(cache):
    tmp = COMPONENT_CACHE_FILE + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(cache, f)
        os.replace(tmp, COMPONENT_CACHE_FILE)
    except Exception as exc:
        log.warning(f"component cache save: {exc}")
        try: os.remove(tmp)
        except OSError: pass

def _get_cached_component(cache, key):
    entry = cache.get(key)
    if not entry:
        return None
    if time.time() - entry.get("t", 0) < COMPONENT_TTLS.get(key, CACHE_TTL):
        return entry.get("v")
    return None

def _get_stale_component(cache, key, max_age=None):
    entry = cache.get(key)
    if not entry:
        return None
    if max_age is None:
        max_age = max(COMPONENT_TTLS.get(key, CACHE_TTL) * 4, 3600)
    if time.time() - entry.get("t", 0) < max_age:
        log.warning(f"component stale fallback: {key}")
        return entry.get("v")
    return None

def _set_component(cache, key, value):
    cache[key] = {"v": value, "t": time.time()}

def _fetch_pmi_from_calendar(currency, label):
    cal = _get_calendar_data()
    if not cal:
        log.debug(f"{label}: calendar.json not found or empty")
        return None

    now = datetime.now(UTC)
    best_actual = None
    best_prev = None

    for ev in cal.get("events", []):
        curr = (ev.get("currency", "") or "").upper()
        if curr != currency:
            continue

        title = ev.get("title", "") or ""
        title_lower = title.lower()
        if "pmi" not in title_lower and "purchasing manager" not in title_lower and "ism" not in title_lower:
            continue

        kind, is_flash, is_final = _classify_pmi_event_title(title)
        if currency == "USD" and "ism" in title_lower and kind == "generic":
            kind = "manufacturing"

        event_time = ev.get("event_time")
        et = None
        if event_time:
            try:
                et = datetime.fromisoformat(event_time)
                if et.tzinfo is None:
                    et = et.replace(tzinfo=UTC)
                if (now - et).days > 60:
                    continue
            except Exception:
                et = None

        actual_str = (ev.get("actual", "") or "").strip()
        prev_str = (ev.get("previous", "") or "").strip()
        score = _pmi_event_score(kind, is_flash, is_final)
        sort_time = et or datetime(1970, 1, 1, tzinfo=UTC)

        if actual_str:
            m = _RE_PMI_VALUE.search(actual_str)
            if m:
                try:
                    val = _safe_pmi(float(m.group(1)), f"{label}-calendar-actual")
                    cand = (score, -int(sort_time.timestamp()), val, kind, "actual")
                    if best_actual is None or cand < best_actual:
                        best_actual = cand
                except Exception:
                    pass

        if prev_str:
            m = _RE_PMI_VALUE.search(prev_str)
            if m:
                try:
                    val = _safe_pmi(float(m.group(1)), f"{label}-calendar-prev")
                    cand = (score, -int(sort_time.timestamp()), val, kind, "previous")
                    if best_prev is None or cand < best_prev:
                        best_prev = cand
                except Exception:
                    pass

    if best_actual is not None:
        _, _, val, kind, src_kind = best_actual
        log.info(f"[API OK] {label}: {val} (calendar/forexfactory, kind={kind}, source={src_kind})")
        return val
    if best_prev is not None:
        _, _, val, kind, src_kind = best_prev
        log.warning(f"{label}: using calendar previous PMI {val} (kind={kind}) because actual is unavailable")
        return val

    log.debug(f"{label}: no usable PMI found in calendar.json for {currency}")
    return None

def fetch_eu_pmi():
    cal_val = _fetch_pmi_from_calendar("EUR", "eu_pmi")
    if cal_val is not None:
        return cal_val, True, "calendar/forexfactory"
    stooq_val = _fetch_pmi_from_stooq("EUR", "eu_pmi")
    if stooq_val is not None:
        val, kind, symbol = stooq_val
        return val, True, f"Stooq/{symbol}:{kind}"
    log.warning("EU PMI: calendar and Stooq fallback unavailable -> returning neutral (pmi_valid=False)")
    return PMI_NEUTRAL, False, "unavailable"

def fetch_uk_pmi():
    cal_val = _fetch_pmi_from_calendar("GBP", "uk_pmi")
    if cal_val is not None:
        return cal_val, True, "calendar/forexfactory"
    stooq_val = _fetch_pmi_from_stooq("GBP", "uk_pmi")
    if stooq_val is not None:
        val, kind, symbol = stooq_val
        return val, True, f"Stooq/{symbol}:{kind}"
    log.warning("UK PMI: calendar and Stooq fallback unavailable -> returning neutral (pmi_valid=False)")
    return PMI_NEUTRAL, False, "unavailable"

# ---------------------------------------------------------------------------
# Manufacturing PMI calendar enrichment
#
# Fetches Manufacturing PMI index values for USD (pmmnus.m), EUR (pmmneu.m),
# and GBP (pmmnuk.m) from Stooq and backfills blank *actual* fields in the
# in-memory calendar events.  Called from build_macro() before the parallel
# PMI fetchers run so that _fetch_pmi_from_calendar() finds real values.
#
# Results are component-cached (TTL = 6 h) so repeated calls within one run
# are free.  A synthetic event row is appended when no matching event exists,
# giving fetch_eu_pmi / fetch_uk_pmi / fetch_pmi a guaranteed calendar hit.
# ---------------------------------------------------------------------------

def _blank_calendar_value(value: object) -> bool:
    """True when a calendar actual/forecast field carries no usable data."""
    text = str(value or "").strip()
    return not text or text.upper() in {"N/A", "NA", "N/D", "TBD", "-"}


def _pmi_region_matches(title: str, currency: str) -> bool:
    """True when *title* describes a Manufacturing PMI for *currency*'s region."""
    t = (title or "").lower()
    if "pmi" not in t or "manufactur" not in t:
        return False
    if currency == "USD":
        return not any(x in t for x in ("canada", "mexico"))
    if currency == "GBP":
        return (
            any(x in t for x in ("uk", "u.k", "britain", "british"))
            or "manufacturing pmi" in t
        )
    if currency == "EUR":
        return any(x in t for x in ("eurozone", "euro area", "eu ", "european"))
    return False


def enrich_calendar_with_pmi_stooq(
    events: List[Dict],
    comp_cache: Optional[Dict] = None,
) -> Tuple[List[Dict], int]:
    """Fetch Manufacturing PMI from Stooq and enrich the in-memory calendar.

    For each of the three currencies (USD, EUR, GBP):
      1. Fetch the latest close for the symbol via ``_fetch_stooq()``,
         backed by the component cache (TTL = 6 h).
      2. Backfill any calendar event whose *actual* field is blank and whose
         title matches the currency's region.
      3. Append a synthetic event row when no matching event was found, so
         ``_fetch_pmi_from_calendar()`` is guaranteed a calendar hit.

    Returns ``(enriched_events, changed_count)``.
    """
    if not PMI_CAL_STOOQ_ENABLED:
        return events, 0

    cache = comp_cache if isinstance(comp_cache, dict) else _load_component_cache()
    dirty = False
    pmi_values: Dict[str, Dict[str, object]] = {}

    for currency, symbol in _PMI_CAL_STOOQ_SYMBOLS.items():
        cache_key = f"pmi_stooq_cal_{currency.lower()}"

        # 1a. Fresh component-cache hit.
        cached = _get_cached_component(cache, cache_key)
        if cached and isinstance(cached, dict) and cached.get("value") is not None:
            pmi_values[currency] = cached
            continue

        # 1b. Live Stooq fetch.
        val, ok = _fetch_stooq(symbol, f"pmi_stooq_cal_{currency}", _PMI_BOUNDS)
        if ok:
            entry: Dict[str, object] = {
                "value":  round(val, 1),
                "symbol": symbol.upper(),
                "source": "Stooq",
            }
            pmi_values[currency] = entry
            _set_component(cache, cache_key, entry)
            dirty = True
            log.info(
                "PMI Stooq→cal: %s %s=%.1f cached for %ds",
                currency, symbol.upper(), val,
                COMPONENT_TTLS.get(cache_key, 21600),
            )
        else:
            # 1c. Stale component-cache fallback.
            stale = _get_stale_component(cache, cache_key)
            if stale and isinstance(stale, dict) and stale.get("value") is not None:
                pmi_values[currency] = stale
                log.warning(
                    "PMI Stooq→cal: %s live fetch failed -- using stale cached value=%.1f",
                    currency, stale["value"],
                )
            else:
                log.warning(
                    "PMI Stooq→cal: %s no data for %s -- skipped", currency, symbol.upper()
                )

    if dirty:
        _save_component_cache(cache)

    if not pmi_values:
        return events, 0

    _CURRENCY_PAIRS: Dict[str, List[str]] = {
        "EUR": ["eurusd"],
        "GBP": ["gbpusd"],
        "USD": ["eurusd", "gbpusd", "xauusd"],
    }
    _LABEL_MAP: Dict[str, str] = {"USD": "U.S.", "EUR": "Eurozone", "GBP": "U.K."}
    changed = 0

    for currency, item in pmi_values.items():
        actual_str = f"{item['value']:.1f}"
        updated = 0

        # 2. Backfill blank actual fields in matching existing events.
        for ev in events:
            if str(ev.get("currency", "")).upper() != currency:
                continue
            if not _blank_calendar_value(ev.get("actual")):
                continue
            if not _pmi_region_matches(str(ev.get("title", "")), currency):
                continue
            ev["actual"] = actual_str
            ev["actual_source"] = "Stooq"
            ev["actual_source_symbol"] = item.get("symbol")
            updated += 1
            changed += 1

        # 3. Append a synthetic row when no event could be backfilled.
        symbol_upper = str(item.get("symbol", "")).upper()
        exists = any(
            str(ev.get("currency", "")).upper() == currency
            and str(ev.get("actual_source_symbol", "")).upper() == symbol_upper
            for ev in events
        )
        if not exists:
            events.append({
                "title":               f"Stooq {_LABEL_MAP.get(currency, currency)} Manufacturing PMI",
                "currency":            currency,
                "impact":              "Medium",
                "event_time":          None,
                "actual":              actual_str,
                "forecast":            "",
                "previous":            "",
                "pairs":               _CURRENCY_PAIRS.get(currency, []),
                "source":              "Stooq",
                "actual_source":       "Stooq",
                "actual_source_symbol": symbol_upper,
                "note": (
                    "Synthetic PMI row added by macro.py "
                    "to enrich calendar before PMI fetchers run"
                ),
            })
            changed += 1
            log.info(
                "PMI Stooq→cal: appended synthetic %s calendar row actual=%s",
                currency, actual_str,
            )
        elif updated:
            log.info(
                "PMI Stooq→cal: updated %d %s calendar PMI row(s) actual=%s",
                updated, currency, actual_str,
            )

    return events, changed

def _fetch_fred_series(series_id, label, validator=_safe_yield):
    def _parse_values(values_iter, source_name):
        try:
            for raw_value in values_iter:
                if raw_value in (None, '', '.'):
                    continue
                raw = float(raw_value)
                val = validator(raw, label)
                if not math.isclose(val, raw, rel_tol=1e-9, abs_tol=1e-9):
                    log.warning(f"{label}: {source_name} returned {raw}, validator clamped to {val} -- skipping")
                    continue
                log.info(f"{label}: {val} ({source_name})")
                return val
        except Exception as exc:
            log.warning(f"{label} {source_name}: {exc}")
        return None

    if FRED_API_KEY:
        data = _get_json(
            f"https://api.stlouisfed.org/fred/series/observations"
            f"?series_id={series_id}&api_key={FRED_API_KEY}"
            f"&file_type=json&sort_order=desc&limit=5"
        )
        if data is None:
            log.warning(f"[API FAIL] {label}: FRED/{series_id} returned None")
        else:
            log.info(f"[API OK] {label}: FRED/{series_id} responded")
            parsed = _parse_values((obs.get('value', '.') for obs in data.get('observations', [])), f"FRED/{series_id}")
            if parsed is not None:
                return parsed
    else:
        log.warning(f"[API FAIL] {label}: no FRED_API_KEY configured -- trying fredgraph CSV fallback")

    r = _get(f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={series_id}", timeout=HTTP_TIMEOUT_SLOW)
    if r is None:
        log.warning(f"[API FAIL] {label}: FREDCSV/{series_id} returned None")
        return None
    try:
        rows = list(csv.DictReader(StringIO(r.text)))
        if not rows:
            log.warning(f"[API FAIL] {label}: FREDCSV/{series_id} returned empty CSV")
            return None
        parsed = _parse_values((row.get(series_id, '.') for row in reversed(rows)), f"FREDCSV/{series_id}")
        if parsed is not None:
            return parsed
    except Exception as exc:
        log.warning(f"{label} FREDCSV/{series_id}: {exc}")
    return None

def fetch_yield_data():
    month = datetime.now(UTC).strftime("%Y%m")
    r = _get(
        f"https://home.treasury.gov/resource-center/data-chart-center/"
        f"interest-rates/pages/xml?data=daily_treasury_yield_curve"
        f"&field_tdr_date_value={month}"
    )
    if r is not None:
        log.info("[API OK] yield_data: Treasury XML responded")
        try:
            root = ET.fromstring(r.text)
            entries = root.findall(f".//{{{_ATOM_NS}}}entry") or root.findall(".//entry")
            if entries:
                latest = entries[-1]
                def _val(tag):
                    el = latest.find(f".//{{{_TREAS_NS}}}{tag}") or latest.find(f".//{tag}")
                    return float(el.text) if (el is not None and el.text) else 0.0
                y2 = _safe_yield(_val("BC_2YEAR"), "Treasury-2Y")
                y10 = _safe_yield(_val("BC_10YEAR"), "Treasury-10Y")
                if y2 != 0.0 or y10 != 0.0:
                    _set_fetch_source("yield_data", "Treasury/XML")
                    return round(y10 - y2, 4), y10
        except Exception as exc:
            log.warning(f"yield_data XML: {exc}")
    y2 = _fetch_fred_series("DGS2", "FRED-DGS2") or 0.0
    y10 = _fetch_fred_series("DGS10", "FRED-DGS10") or 0.0
    if y2 != 0.0 or y10 != 0.0:
        _set_fetch_source("yield_data", "FRED")
        return round(y10 - y2, 4), y10
    return 0.0, 0.0

def fetch_tips_yield():
    val = _fetch_fred_series("DFII10", "DFII10")
    if val is not None:
        _set_fetch_source("tips", "FRED/DFII10")
        return (val, True)
    return (0.0, False)


def _safe_index_level(value: float, label: str) -> float:
    """Generic validator for index/price-style macro series."""
    if not math.isfinite(float(value)):
        raise ValueError(f"{label}: non-finite value {value}")
    return float(value)


def _fetch_fred_recent_values(series_id: str, label: str, limit: int = 10, validator=_safe_index_level) -> List[float]:
    """Fetch recent valid FRED observations, latest first."""
    def _consume(raw_values, source_name: str) -> List[float]:
        out: List[float] = []
        for raw_value in raw_values:
            if raw_value in (None, "", "."):
                continue
            try:
                out.append(float(validator(float(raw_value), label)))
                if len(out) >= limit:
                    break
            except Exception as exc:
                log.debug("%s %s skipped value %r: %s", label, source_name, raw_value, exc)
        if out:
            log.info("%s: latest=%s count=%s (%s)", label, round(out[0], 4), len(out), source_name)
        return out

    if FRED_API_KEY:
        data = _get_json(
            f"https://api.stlouisfed.org/fred/series/observations"
            f"?series_id={series_id}&api_key={FRED_API_KEY}"
            f"&file_type=json&sort_order=desc&limit={max(limit * 3, 15)}"
        )
        if data is not None:
            out = _consume((obs.get("value", ".") for obs in data.get("observations", [])), f"FRED/{series_id}")
            if out:
                return out
        else:
            log.warning("[API FAIL] %s: FRED/%s returned None", label, series_id)

    r = _get(f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={series_id}", timeout=HTTP_TIMEOUT_SLOW)
    if r is None:
        log.warning("[API FAIL] %s: FREDCSV/%s returned None", label, series_id)
        return []
    try:
        rows = list(csv.DictReader(StringIO(r.text)))
        return _consume((row.get(series_id, ".") for row in reversed(rows)), f"FREDCSV/{series_id}")
    except Exception as exc:
        log.warning("%s FREDCSV/%s: %s", label, series_id, exc)
        return []


def _series_state_from_values(values: List[float], source: str) -> Dict[str, Any]:
    if len(values) >= 2:
        current, prev = float(values[0]), float(values[1])
        return {"current": round(current, 4), "prev": round(prev, 4), "change": round(current - prev, 4), "valid": True, "source": source}
    if len(values) == 1:
        current = float(values[0])
        return {"current": round(current, 4), "prev": round(current, 4), "change": 0.0, "valid": True, "source": source}
    return {"current": 0.0, "prev": 0.0, "change": 0.0, "valid": False, "source": "fallback"}


_YAHOO_GVZ_URL = (
    "https://query1.finance.yahoo.com/v8/finance/chart/%5EGVZ?interval=1d&range=1d"
)

# ---------------------------------------------------------------------------
# Generic Yahoo Finance v8 chart helpers
# ---------------------------------------------------------------------------

def _yahoo_v8_url(ticker: str, range_: str = "5d") -> str:
    """Build a Yahoo Finance v8 chart URL, percent-encoding special chars."""
    from urllib.parse import quote
    encoded = quote(ticker, safe="")
    return (
        f"https://query1.finance.yahoo.com/v8/finance/chart/{encoded}"
        f"?interval=1d&range={range_}"
    )


def _yahoo_v8_state(
    yahoo_ticker: str,
    label: str,
    bounds: Tuple[float, float],
) -> Dict[str, Any]:
    """Fetch current + previous close for *yahoo_ticker* via the v8 chart API.

    Mirrors the pattern of ``_yahoo_gvz_state`` but accepts any ticker and
    bounds.  Returns the same ``_series_state_from_values``-shaped dict.
    """
    source = f"Yahoo/{yahoo_ticker}"
    _fallback = {"current": 0.0, "prev": 0.0, "change": 0.0, "valid": False, "source": "fallback"}
    try:
        data = _get_json(_yahoo_v8_url(yahoo_ticker, "5d"), timeout=HTTP_TIMEOUT_SLOW)
        result = (data or {}).get("chart", {}).get("result") or []
        if not result:
            log.warning("[API FAIL] %s: Yahoo/%s chart returned no result", label, yahoo_ticker)
            _set_fetch_source(label, "fallback")
            return _fallback

        meta = result[0].get("meta", {})
        current = meta.get("regularMarketPrice")
        prev    = meta.get("chartPreviousClose")

        # Fallback: read from indicators.quote[0].close if meta fields missing
        if current is None:
            closes = (
                (result[0].get("indicators", {}).get("quote") or [{}])[0].get("close") or []
            )
            valid_closes = [c for c in closes if c is not None]
            current = valid_closes[-1] if valid_closes else None
            if prev is None and len(valid_closes) >= 2:
                prev = valid_closes[-2]

        if current is None:
            log.warning("[API FAIL] %s: Yahoo/%s could not extract price", label, yahoo_ticker)
            _set_fetch_source(label, "fallback")
            return _fallback

        current = float(current)
        prev    = float(prev) if prev is not None else current

        if not (bounds[0] <= current <= bounds[1]):
            log.warning(
                "[API FAIL] %s: Yahoo/%s value %.4f outside bounds %s",
                label, yahoo_ticker, current, bounds,
            )
            _set_fetch_source(label, "fallback")
            return _fallback

        log.info("[API OK] %s: current=%.4f prev=%.4f (%s)", label, current, prev, source)
        _set_fetch_source(label, source)
        return _series_state_from_values([current, prev], source)

    except Exception as exc:
        log.debug("%s Yahoo/%s failed: %s", label, yahoo_ticker, exc)
        _set_fetch_source(label, "fallback")
        return _fallback


def _fetch_yahoo_recent_values(
    yahoo_ticker: str,
    label: str,
    bounds: Tuple[float, float],
    limit: int = 6,
) -> List[float]:
    """Fetch up to *limit* recent daily closes (newest first) for *yahoo_ticker*.

    Uses a 30-day range so we get enough history for delta calculations even
    with weekends/holidays stripped out.  Returns an empty list on failure.
    """
    try:
        data = _get_json(_yahoo_v8_url(yahoo_ticker, "30d"), timeout=HTTP_TIMEOUT_SLOW)
        result = (data or {}).get("chart", {}).get("result") or []
        if not result:
            log.warning("[API FAIL] %s: Yahoo/%s 30d chart returned no result", label, yahoo_ticker)
            return []
        closes = (
            (result[0].get("indicators", {}).get("quote") or [{}])[0].get("close") or []
        )
        # Reverse so newest is first; filter Nones and out-of-bounds values.
        valid = [
            float(c)
            for c in reversed(closes)
            if c is not None and bounds[0] <= float(c) <= bounds[1]
        ]
        valid = valid[:limit]
        if valid:
            log.info(
                "[API OK] %s: Yahoo/%s latest=%.4f count=%d",
                label, yahoo_ticker, valid[0], len(valid),
            )
        return valid
    except Exception as exc:
        log.debug("%s Yahoo/%s 30d failed: %s", label, yahoo_ticker, exc)
        return []


# ---------------------------------------------------------------------------
# Per-series Yahoo tickers used as FRED fallbacks in fetch_gold_macro_data()
# ---------------------------------------------------------------------------
# DFII10 (10Y TIPS / real yield)  → ^TNX (10Y nominal) — directional proxy
# DTWEXBGS (broad USD index)      → DX-Y.NYB (ICE DXY futures, close enough)
# T5YIFR (5Y5Y inflation swap)    → ^TNX used as inflation-pressure proxy;
#                                   scale kept small so delta contribution is
#                                   bounded (no direct Yahoo equivalent exists)
# DCOILWTICO (WTI crude)          → CL=F (front-month WTI futures — exact match)
# ^MOVE (CBOE MOVE treasury vol)  → Yahoo ^MOVE — sole source

_YAHOO_FALLBACKS: Dict[str, Dict] = {
    "DFII10":     {"ticker": "^TNX",     "bounds": (-2.0, 10.0)},   # nominal proxy
    "DTWEXBGS":   {"ticker": "DX-Y.NYB", "bounds": (80.0, 140.0)},  # DXY
    "T5YIFR":     {"ticker": "^TNX",     "bounds": (-2.0, 10.0)},   # inflation proxy
    "DCOILWTICO": {"ticker": "CL=F",     "bounds": (20.0, 200.0)},  # WTI exact
}


def _gold_macro_series_state(
    fred_series: str,
    label: str,
    limit: int,
    validator,
    fred_source: str,
) -> Dict[str, Any]:
    """Fetch a gold-macro series, trying FRED first then a Yahoo Finance fallback.

    Returns a ``_series_state_from_values``-shaped dict.
    """
    vals = _fetch_fred_recent_values(fred_series, label, limit=limit, validator=validator)
    if vals:
        return _series_state_from_values(vals, fred_source)

    # FRED unavailable — try Yahoo fallback
    yf_cfg = _YAHOO_FALLBACKS.get(fred_series)
    if yf_cfg:
        ticker, bounds = yf_cfg["ticker"], yf_cfg["bounds"]
        y_vals = _fetch_yahoo_recent_values(ticker, label, bounds, limit=limit)
        if y_vals:
            yahoo_source = f"Yahoo/{ticker}(proxy)"
            log.info(
                "%s: FRED/%s unavailable — using %s as fallback",
                label, fred_series, yahoo_source,
            )
            return _series_state_from_values(y_vals, yahoo_source)

    # Both sources failed
    log.warning("%s: FRED/%s and Yahoo fallback both failed", label, fred_series)
    return _series_state_from_values([], "fallback")


def _yahoo_gvz_state(label: str = "gold_vol", bounds: Tuple[float, float] = (5.0, 150.0)) -> Dict[str, Any]:
    """Fetch GVZ (CBOE Gold Volatility Index) from Yahoo Finance chart API.

    Uses ``regularMarketPrice`` as the current value and
    ``chartPreviousClose`` as the previous value — both are present in the
    v8 chart meta even with ``range=1d``, so no extra round-trip is needed.
    Falls back to the first close in the ``indicators`` array if meta fields
    are absent.  Returns a fallback state dict on any error.
    """
    source = "Yahoo/^GVZ"
    try:
        data = _get_json(_YAHOO_GVZ_URL, timeout=HTTP_TIMEOUT_SLOW)
        result = (data or {}).get("chart", {}).get("result") or []
        if not result:
            log.warning("[API FAIL] %s: Yahoo chart returned no result", label)
            _set_fetch_source(label, "fallback")
            return {"current": 0.0, "prev": 0.0, "change": 0.0, "valid": False, "source": "fallback"}

        meta = result[0].get("meta", {})
        current = meta.get("regularMarketPrice")
        prev    = meta.get("chartPreviousClose")

        # Fallback: read from indicators.quote[0].close if meta fields missing
        if current is None:
            closes = (result[0].get("indicators", {}).get("quote") or [{}])[0].get("close") or []
            valid_closes = [c for c in closes if c is not None]
            current = valid_closes[-1] if valid_closes else None
            if prev is None and len(valid_closes) >= 2:
                prev = valid_closes[-2]

        if current is None:
            log.warning("[API FAIL] %s: could not extract price from Yahoo response", label)
            _set_fetch_source(label, "fallback")
            return {"current": 0.0, "prev": 0.0, "change": 0.0, "valid": False, "source": "fallback"}

        current = float(current)
        prev    = float(prev) if prev is not None else current

        if not (bounds[0] <= current <= bounds[1]):
            log.warning("[API FAIL] %s: Yahoo value %.3f outside bounds %s", label, current, bounds)
            _set_fetch_source(label, "fallback")
            return {"current": 0.0, "prev": 0.0, "change": 0.0, "valid": False, "source": "fallback"}

        log.info("[API OK] %s: current=%.3f prev=%.3f (%s)", label, current, prev, source)
        _set_fetch_source(label, source)
        return _series_state_from_values([current, prev], source)

    except Exception as exc:
        log.debug("%s Yahoo/^GVZ failed: %s", label, exc)
        _set_fetch_source(label, "fallback")
        return {"current": 0.0, "prev": 0.0, "change": 0.0, "valid": False, "source": "fallback"}


def fetch_gold_macro_data() -> Dict[str, Any]:
    """Fetch gold-only macro inputs used only for XAUUSD bias scoring.

    All 6 sources use Yahoo Finance or FRED (with Yahoo as fallback).
    Stooq is not used for any gold source.

      FRED DFII10     -> Yahoo ^TNX   (10-Y nominal; direction proxy for real yield)
      FRED DTWEXBGS   -> Yahoo DX-Y.NYB (ICE DXY, close proxy for broad USD)
      FRED T5YIFR     -> Yahoo ^TNX   (inflation-pressure proxy; no direct ticker)
      Yahoo ^MOVE     -> sole source  (CBOE MOVE Index)
      Yahoo ^GVZ      -> sole source  (CBOE Gold Volatility Index)
      FRED DCOILWTICO -> Yahoo CL=F   (WTI front-month futures; exact match)
    """
    if not MACRO_GOLD_ENHANCED_ENABLED:
        return dict(_NESTED_DEFAULTS.get("gold", {}))

    # -- 1. Real-yield momentum (DFII10) -------------------------------------
    real_yield_momentum = _gold_macro_series_state(
        "DFII10", "gold_real_yield_momentum",
        limit=6, validator=_safe_yield, fred_source="FRED/DFII10",
    )

    # -- 2. Broad USD (DTWEXBGS) ---------------------------------------------
    broad_usd = _gold_macro_series_state(
        "DTWEXBGS", "gold_broad_usd",
        limit=6, validator=_safe_index_level, fred_source="FRED/DTWEXBGS",
    )

    # -- 3. Inflation expectations (T5YIFR) ----------------------------------
    inflation_expectations = _gold_macro_series_state(
        "T5YIFR", "gold_inflation_expectations",
        limit=6, validator=_safe_yield, fred_source="FRED/T5YIFR",
    )

    # -- 4. Treasury vol / MOVE index ----------------------------------------
    # Yahoo Finance ^MOVE (CBOE MOVE Index) is the sole source.
    _MOVE_BOUNDS = (20.0, 300.0)
    treasury_vol = _yahoo_v8_state("^MOVE", "gold_treasury_vol", _MOVE_BOUNDS)

    # -- 5. Gold vol / GVZ (already Yahoo) -----------------------------------
    gold_vol = _yahoo_gvz_state("gold_vol", (5.0, 150.0))

    # -- 6. Energy inflation / WTI (DCOILWTICO) ------------------------------
    energy_inflation = _gold_macro_series_state(
        "DCOILWTICO", "gold_energy_inflation",
        limit=6, validator=_safe_index_level, fred_source="FRED/DCOILWTICO",
    )

    result = {
        "enabled": True,
        "real_yield_momentum":    real_yield_momentum,
        "broad_usd":              broad_usd,
        "inflation_expectations": inflation_expectations,
        "treasury_vol":           treasury_vol,
        "gold_vol":               gold_vol,
        "energy_inflation":       energy_inflation,
    }
    valid_count = sum(1 for v in result.values() if isinstance(v, dict) and v.get("valid"))
    result["valid_count"] = valid_count
    result["quality"] = round(min(valid_count / 6.0, 1.0), 3)
    _set_fetch_source("gold_macro", f"enhanced(valid={valid_count}/6)")
    log.info(
        "[gold_macro] sources: ry=%s usd=%s infl=%s move=%s gvz=%s wti=%s valid=%d/6",
        real_yield_momentum.get("source", "?"),
        broad_usd.get("source", "?"),
        inflation_expectations.get("source", "?"),
        treasury_vol.get("source", "?"),
        gold_vol.get("source", "?"),
        energy_inflation.get("source", "?"),
        valid_count,
    )
    return result


def fetch_pmi():
    cal_val = _fetch_pmi_from_calendar("USD", "us_pmi")
    if cal_val is not None:
        return cal_val, True, "calendar/forexfactory"
    stooq_val = _fetch_pmi_from_stooq("USD", "us_pmi")
    if stooq_val is not None:
        val, kind, symbol = stooq_val
        return val, True, f"Stooq/{symbol}:{kind}"
    log.warning("US PMI: calendar and Stooq fallback unavailable -> returning neutral (pmi_valid=False)")
    return PMI_NEUTRAL, False, "unavailable"

def fetch_fed_rate_with_prev():
    if FRED_API_KEY:
        for series in ("FEDFUNDS", "DFF"):
            data = _get_json(
                f"https://api.stlouisfed.org/fred/series/observations"
                f"?series_id={series}&api_key={FRED_API_KEY}"
                f"&file_type=json&sort_order=desc&limit=5"
            )
            if data is not None:
                log.info(f"[API OK] fed_rate: FRED/{series} responded")
                try:
                    valid = [o for o in data.get("observations", []) if o.get("value", ".") != "."]
                    if valid:
                        curr = _safe_rate(valid[0]["value"], f"FRED/{series}")
                        prev = curr
                        if len(valid) > 1 and series == "FEDFUNDS":
                            try:
                                prev = _safe_rate(valid[1]["value"], f"FRED/{series}-prev")
                            except (ValueError, TypeError):
                                pass
                        _set_fetch_source("fed_rate", f"FRED/{series}")
                        return curr, prev
                except Exception as exc:
                    log.warning(f"fed_rate {series}: {exc}")
            else:
                log.warning(f"[API FAIL] fed_rate: FRED/{series} returned None")

    r = _get(
        "https://www.federalreserve.gov/datadownload/Output.aspx"
        "?rel=H15&series=bcb44e57fb57efbe1c1819a37cbd7604"
        "&lastobs=1&startdate=&enddate=&filetype=json"
        "&label=include&layout=seriescolumn"
    )
    if r is not None:
        try:
            for row in reversed(r.json().get("data", [])):
                for cell in row[1:]:
                    try:
                        rate = _safe_rate(cell, "H15-JSON")
                        if rate > 0.0:
                            _set_fetch_source("fed_rate", "FederalReserve/H15")
                            return rate, rate
                    except (ValueError, TypeError):
                        continue
        except Exception:
            pass

    r2 = _get("https://www.federalreserve.gov/releases/h15/")
    if r2 is not None:
        try:
            m = _RE_RATE_RANGE.search(r2.text)
            if m:
                avg = _safe_rate((float(m.group(1)) + float(m.group(2))) / 2, "H15")
                _set_fetch_source("fed_rate", "FederalReserve/H15")
                return avg, avg
        except Exception:
            pass
    return 0.0, 0.0

def fetch_ecb_rate():
    data = _get_json("https://data-api.ecb.europa.eu/service/data/"
                     "FM/B.U2.EUR.4F.KR.DFR.LEV"
                     "?format=jsondata&detail=dataonly&lastNObservations=1")
    if data is None:
        log.warning("[API FAIL] ecb_rate: ECB data API returned None")
        return 0.0
    log.info("[API OK] ecb_rate: ECB data API responded")
    try:
        datasets = data.get("dataSets", [])
        if not datasets:
            return 0.0
        best, best_idx = None, -1
        for series in datasets[0].get("series", {}).values():
            obs = series.get("observations", {})
            if not obs:
                continue
            last_key = max(obs.keys(), key=int)
            val = obs[last_key][0]
            if val is not None and int(last_key) > best_idx:
                best, best_idx = float(val), int(last_key)
        if best is not None:
            _set_fetch_source("ecb_rate", "ECB/DFR")
            return _safe_rate(best, "ECB-DFR")
        return 0.0
    except Exception:
        return 0.0

def fetch_boe_rate():
    r = _get("https://www.bankofengland.co.uk/boeapps/database/Bank-Rate.asp", timeout=HTTP_TIMEOUT_SLOW)
    if r is not None:
        for td in BeautifulSoup(r.text, _BS4_PARSER).find_all("td"):
            m = _RE_BOE_RATE.match(td.get_text(strip=True))
            if m and 0.0 <= float(m.group(1)) <= 15.0:
                log.info(f"[API OK] boe_rate: {m.group(1)} (BoE official)")
                _set_fetch_source("boe_rate", "BoE/official")
                return _safe_rate(m.group(1), "BoE-page")
    log.warning("[API FAIL] boe_rate: BoE official page failed")
    if FRED_API_KEY:
        data = _get_json(f"https://api.stlouisfed.org/fred/series/observations"
                         f"?series_id=BOERUKM&api_key={FRED_API_KEY}"
                         f"&file_type=json&sort_order=desc&limit=5")
        if data is not None:
            log.info("[API OK] boe_rate: FRED/BOERUKM responded")
        else:
            log.warning("[API FAIL] boe_rate: FRED/BOERUKM returned None")
        try:
            for obs in (data or {}).get("observations", []):
                v = obs.get("value", ".")
                if v != "." and float(v) >= 0.0:
                    _set_fetch_source("boe_rate", "FRED/BOERUKM")
                    return _safe_rate(v, "FRED-BOERUKM")
        except Exception:
            pass
    log.warning("[API FAIL] boe_rate: all sources failed -> 0.0")
    return 0.0

# ---------------------------------------------------------------------------
# Stooq — yfinance-style fetch module
#
# Mirrors the pattern used in momentum.py:
#   • Dedicated session with browser UA + Retry adapter (separate from the
#     macro thread-local session so Stooq cookies don't bleed into other APIs)
#   • _stooq_fetch_latest()  → latest OHLCV quote  (/q/l/ endpoint)
#   • _stooq_fetch_daily()   → daily OHLCV history  (REMOVED_STOOQ_HISTORICAL_PATH?i=d endpoint)
#   • _stooq_fetch_hourly()  → intraday H1 history  (REMOVED_STOOQ_HISTORICAL_PATH?i=h endpoint)
#   • Ticker class            → yfinance-compatible  .history() / .fast_info
#   • _fetch_stooq()         → drop-in for all existing macro.py callers
# ---------------------------------------------------------------------------

_STOOQ_UA = _MACRO_UA   # reuse the same browser UA string

_stooq_session_lock = threading.Lock()
_stooq_session: Optional[requests.Session] = None
_stooq_handshook = False


def _get_stooq_session() -> requests.Session:
    """Return a shared, lazily-created Stooq session (thread-safe)."""
    global _stooq_session, _stooq_handshook
    with _stooq_session_lock:
        if _stooq_session is None:
            s = requests.Session()
            s.headers.update({
                "User-Agent": _STOOQ_UA,
                "Accept": "text/csv,application/json,*/*",
                "Accept-Language": "en-US,en;q=0.9",
                "Accept-Encoding": "gzip, deflate, br",
                "Cache-Control": "no-cache",
            })
            s.mount("https://", HTTPAdapter(max_retries=Retry(
                total=2,
                backoff_factor=0.8,
                status_forcelist=[429, 500, 502, 503, 504],
                allowed_methods=frozenset(["GET"]),
                respect_retry_after_header=True,
            )))
            s.mount("http://", HTTPAdapter(max_retries=Retry(
                total=1,
                backoff_factor=0.5,
                status_forcelist=[500, 502, 503, 504],
                allowed_methods=frozenset(["GET"]),
            )))
            _stooq_session = s
        # One-time cookie handshake so subsequent CSV requests aren't blocked.
        if not _stooq_handshook:
            try:
                _stooq_session.get("https://stooq.com", timeout=8)
            except Exception:
                pass
            _stooq_handshook = True
        return _stooq_session


def _stooq_parse_csv(text: str) -> List[Dict[str, str]]:
    """Parse Stooq CSV text → list of dicts with lowercase-stripped keys."""
    rows = list(csv.DictReader(StringIO(text)))
    return [{k.lower().strip(): v.strip() for k, v in row.items()} for row in rows]


def _stooq_is_nd(value: str) -> bool:
    return value.strip().lower() in ("", "n/d", "null", "none", "-")


def _stooq_safe_float(value: str) -> Optional[float]:
    if _stooq_is_nd(value):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _stooq_fetch_latest(symbol: str, timeout: int = HTTP_TIMEOUT_SLOW) -> Optional[Dict[str, str]]:
    """
    Fetch the latest OHLCV quote row from Stooq's real-time endpoint.
    Returns a dict with lowercase keys, or None on failure / N/D.

    Endpoint: https://stooq.com/q/l/?s={symbol}&f=sd2t2ohlcv&h&e=csv
    """
    from urllib.parse import quote_plus
    sym = symbol.strip().lower()
    url = f"https://stooq.com/q/l/?s={quote_plus(sym)}&f=sd2t2ohlcv&h&e=csv"
    try:
        resp = _get_stooq_session().get(url, timeout=timeout)
        resp.raise_for_status()
        rows = _stooq_parse_csv(resp.text)
        if not rows:
            return None
        row = rows[-1]
        if _stooq_is_nd(row.get("close", "")):
            return None
        return row
    except Exception as exc:
        log.debug("stooq_latest: symbol=%s err=%s", sym, exc)
        return None


def _stooq_fetch_daily(
    symbol: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    timeout: int = HTTP_TIMEOUT_SLOW,
) -> List[Dict[str, str]]:
    """Strict build: Stooq historical daily OHLCV is completely removed."""
    log.debug("stooq_daily removed in strict build for symbol=%s", symbol)
    return []


def _stooq_fetch_hourly(
    symbol: str,
    timeout: int = HTTP_TIMEOUT_SLOW,
) -> List[Dict[str, str]]:
    """Strict build: Stooq historical hourly OHLCV is completely removed."""
    log.debug("stooq_hourly removed in strict build for symbol=%s", symbol)
    return []


def _stooq_row_to_bar(row: Dict[str, str]) -> Optional[Dict[str, Any]]:
    """
    Convert a raw Stooq CSV row to a canonical OHLCV bar dict.

    Output keys (yfinance-compatible):
        Date (str "YYYY-MM-DD"), Open, High, Low, Close (float), Volume (int|None)
    Returns None if the row is unparseable.
    """
    date_val = row.get("date", "") or row.get("d", "")
    close_val = _stooq_safe_float(row.get("close", "") or row.get("c", ""))
    if not date_val or close_val is None:
        return None
    open_v  = _stooq_safe_float(row.get("open",   "") or row.get("o", "")) or close_val
    high_v  = _stooq_safe_float(row.get("high",   "") or row.get("h", "")) or close_val
    low_v   = _stooq_safe_float(row.get("low",    "") or row.get("l", "")) or close_val
    vol_raw = _stooq_safe_float(row.get("volume", "") or row.get("v", ""))
    return {
        "Date":   date_val.strip(),
        "Open":   open_v,
        "High":   high_v,
        "Low":    low_v,
        "Close":  close_val,
        "Volume": int(vol_raw) if vol_raw is not None else None,
    }


# yfinance-style period → approximate day count
_STOOQ_PERIOD_DAYS: Dict[str, int] = {
    "1d": 1,   "5d": 5,   "1wk": 7,
    "1mo": 30, "3mo": 90, "6mo": 180,
    "1y": 365, "2y": 730, "5y": 1825,
    "ytd": 0,  "max": 0,
}


class StooqTicker:
    """
    yfinance-compatible Stooq ticker.

    Usage (drop-in replacement for yf.Ticker):

        t = StooqTicker("vi.c")               # VIX on Stooq
        bars = t.history(period="3mo")        # list[dict] OHLCV, oldest-first
        price = t.fast_info["last_price"]     # float | None

        t2 = StooqTicker("^spx")
        bars = t2.history(period="1y", interval="1d")

    Supported intervals: "1d" (daily, default) | "1h" (hourly, limited depth)
    Supported periods:   "1d" "5d" "1wk" "1mo" "3mo" "6mo" "1y" "2y" "5y" "ytd" "max"
    """

    def __init__(self, symbol: str) -> None:
        # Strip yfinance-style "=X" suffix — Stooq uses bare symbols (eurusd, not eurusd=x).
        self.symbol = re.sub(r"=x$", "", symbol.strip().lower())
        self._info_cache: Optional[Dict[str, Any]] = None

    def history(
        self,
        period: str = "3mo",
        interval: str = "1d",
        start: Optional[str] = None,   # "YYYY-MM-DD"
        end: Optional[str] = None,     # "YYYY-MM-DD"
        auto_adjust: bool = True,      # accepted for API compat, no-op
    ) -> List[Dict[str, Any]]:
        """
        Return historical OHLCV bars sorted oldest-first.
        Each bar: {"Date": "YYYY-MM-DD", "Open": float, "High": float,
                   "Low": float, "Close": float, "Volume": int|None}
        """
        if interval == "1h":
            raw_rows = _stooq_fetch_hourly(self.symbol)
        else:
            # Derive start_date from period if not explicitly given
            start_date = None
            if start:
                start_date = start.replace("-", "")
            elif period:
                days = _STOOQ_PERIOD_DAYS.get(period.lower(), 365)
                if days > 0:
                    from datetime import timedelta
                    start_date = (datetime.now(UTC).date() - timedelta(days=days)).strftime("%Y%m%d")
            end_date = end.replace("-", "") if end else None
            raw_rows = _stooq_fetch_daily(self.symbol, start_date=start_date, end_date=end_date)

        bars: List[Dict[str, Any]] = []
        for row in raw_rows:
            bar = _stooq_row_to_bar(row)
            if bar is not None:
                bars.append(bar)
        bars.sort(key=lambda b: b["Date"])
        return bars

    @property
    def fast_info(self) -> Dict[str, Any]:
        """
        Lightweight property returning a dict with:
            last_price     : float | None   (latest close)
            previous_close : float | None   (previous close if available)
            open           : float | None
            high           : float | None
            low            : float | None
        Mirrors yf.Ticker.fast_info keys used in macro.py callers.
        """
        if self._info_cache is not None:
            return self._info_cache
        row = _stooq_fetch_latest(self.symbol)
        if row:
            self._info_cache = {
                "last_price":     _stooq_safe_float(row.get("close", "")),
                "previous_close": _stooq_safe_float(row.get("open", "")),  # Stooq latest: prev≈open
                "open":           _stooq_safe_float(row.get("open", "")),
                "high":           _stooq_safe_float(row.get("high", "")),
                "low":            _stooq_safe_float(row.get("low",  "")),
            }
        else:
            self._info_cache = {
                "last_price": None, "previous_close": None,
                "open": None, "high": None, "low": None,
            }
        return self._info_cache


def stooq_download(
    symbols: List[str],
    period: str = "3mo",
    interval: str = "1d",
) -> Dict[str, List[Dict[str, Any]]]:
    """
    yfinance-style multi-ticker download.

        data = stooq_download(["vi.c", "^spx"], period="1mo")
        # → {"vi.c": [...bars...], "^spx": [...]}

    A short inter-request sleep avoids Stooq rate-limiting on large batches.
    """
    _SLEEP = float(os.environ.get("STOOQ_BATCH_SLEEP", "0.35"))
    result: Dict[str, List[Dict[str, Any]]] = {}
    for i, sym in enumerate(symbols):
        if i > 0:
            time.sleep(_SLEEP)
        result[sym] = StooqTicker(sym).history(period=period, interval=interval)
    return result


def _fetch_stooq(symbol: str, label: str, bounds: Tuple[float, float]) -> Tuple[float, bool]:
    """
    Drop-in replacement for the original _fetch_stooq().

    Fetches the latest close via StooqTicker.fast_info, falls back to the
    most-recent daily bar if the live quote returns N/D.
    Validates against *bounds* and logs the outcome exactly as before.
    """
    ticker = StooqTicker(symbol)
    price = ticker.fast_info.get("last_price")

    # Fallback: pull the tail of daily history if live quote was N/D
    if price is None:
        bars = ticker.history(period="5d")
        if bars:
            price = bars[-1].get("Close")

    if price is None:
        return 0.0, False
    try:
        val = float(price)
        if bounds[0] <= val <= bounds[1]:
            log.info(f"[API OK] {label}: {val} (Stooq/{symbol})")
            return round(val, 3), True
    except (TypeError, ValueError):
        pass
    return 0.0, False

# ---------------------------------------------------------------------------
# fetch_price — public yfinance-style price API backed by Stooq
#
# Drop-in usage that mirrors the two most common yfinance patterns:
#
#   # Pattern 1 — single ticker (yf.Ticker equivalent)
#   t = fetch_price("eurusd")
#   price  = t.fast_info["last_price"]          # float | None
#   bars   = t.history(period="3mo")            # list[dict] OHLCV
#   bars_h = t.history(period="5d", interval="1h")
#
#   # Pattern 2 — batch download (yf.download equivalent)
#   data = fetch_price(["eurusd", "vi.c", "^spx"], period="1mo")
#   # → {"eurusd": [...bars...], "vi.c": [...], "^spx": [...]}
#
# When a single string is passed, returns a StooqTicker instance.
# When a list/tuple is passed, returns a dict[symbol → list[bar]].
#
# Supported intervals : "1d" (daily, default) | "1h" (hourly, ~5-day depth)
# Supported periods   : "1d" "5d" "1wk" "1mo" "3mo" "6mo" "1y" "2y" "5y"
#                       "ytd" "max"
#
# Canonical Stooq symbols for the pairs used throughout macro.py:
#   FX      eurusd    gbpusd    xauusd   (bare — no =x suffix)
#   Indices ^spx      vi.c
#   Rates   none
# ---------------------------------------------------------------------------

def fetch_price(
    symbols: Union[str, List[str]],
    period: str = "3mo",
    interval: str = "1d",
    start: Optional[str] = None,   # "YYYY-MM-DD"
    end: Optional[str] = None,     # "YYYY-MM-DD"
) -> Union["StooqTicker", Dict[str, List[Dict[str, Any]]]]:
    """
    yfinance-style price fetcher backed by Stooq (no API key required).

    Single symbol  → StooqTicker  (access .history() and .fast_info)
    List of symbols → dict[symbol → list[OHLCV bar dicts]]

    Each bar dict keys: Date (str), Open, High, Low, Close (float), Volume (int|None).

    Examples
    --------
    # Latest price for EURUSD
    price = fetch_price("eurusd").fast_info["last_price"]

    # 3-month daily bars for gold
    bars = fetch_price("xauusd").history(period="3mo")

    # Batch download: VIX + SPX, last month of daily bars
    data = fetch_price(["vi.c", "^spx"], period="1mo")
    vix_bars = data["vi.c"]
    spx_bars = data["^spx"]

    # 1-hour intraday bars (last ~5 days, Stooq limitation)
    h1 = fetch_price("eurusd", interval="1h")   # StooqTicker
    bars_h1 = h1.history(period="5d", interval="1h")
    """
    if isinstance(symbols, str):
        t = StooqTicker(symbols)
        # Pre-warm history so callers that chain .history() immediately pay
        # no extra latency when period/interval are the common defaults.
        if interval == "1d" and period == "3mo" and start is None and end is None:
            # history is lazily fetched on first call; nothing extra to do.
            pass
        return t

    # Batch path — mirrors yf.download(tickers, period=..., interval=...)
    _SLEEP = float(os.environ.get("STOOQ_BATCH_SLEEP", "0.35"))
    result: Dict[str, List[Dict[str, Any]]] = {}
    sym_list = list(symbols)
    for i, sym in enumerate(sym_list):
        if i > 0:
            time.sleep(_SLEEP)
        t = StooqTicker(sym)
        result[sym] = t.history(period=period, interval=interval,
                                start=start, end=end)
    return result


def fetch_price_latest(symbol: str) -> Optional[float]:
    """
    Convenience one-liner: return the latest close price for *symbol*,
    or None if Stooq returns N/D or the request fails.

    Example
    -------
    vix = fetch_price_latest("vi.c")      # → 18.34 or None
    eurusd = fetch_price_latest("eurusd")
    """
    return StooqTicker(symbol).fast_info.get("last_price")


def fetch_vix():
    val, ok = _fetch_stooq("vi.c", "vix", _VIX_BOUNDS)
    if ok:
        _set_fetch_source("vix", "Stooq/vi.c")
        return round(val, 2), True
    log.warning("[API FAIL] vix: Stooq unavailable -> neutral fallback (valid=False)")
    _set_fetch_source("vix", "fallback")
    return VIX_NEUTRAL_FALLBACK, False

_SPX_BOUNDS = (1000.0, 20000.0)

def fetch_spx() -> Tuple[float, float, bool]:
    curr_val, curr_ok = _fetch_stooq("^spx", "spx", _SPX_BOUNDS)
    if curr_ok:
        _set_fetch_source("spx", "Stooq/^spx")
        return round(curr_val, 2), round(curr_val, 2), True

    log.warning("[API FAIL] spx: Stooq latest quote failed -> trying FRED/SP500")
    _spx_validator = lambda v, lbl: max(_SPX_BOUNDS[0], min(v, _SPX_BOUNDS[1]))
    val = _fetch_fred_series("SP500", "spx", validator=_spx_validator)
    if val is not None:
        _set_fetch_source("spx", "FRED/SP500")
        log.info(f"[API OK] spx: {val} prev={val} (FRED/SP500, trend=0.0 this run)")
        return round(val, 2), round(val, 2), True

    log.warning("[API FAIL] spx: all sources exhausted")
    _set_fetch_source("spx", "fallback")
    return 0.0, 0.0, False

def _spx_trend_factor(macro: Dict) -> float:
    spx = macro.get("spx", {})
    if not spx.get("valid", False):
        return 0.0
    curr = spx.get("current", 0.0)
    prev = spx.get("prev", curr)
    if prev == 0.0:
        return 0.0
    pct_change = (curr - prev) / prev * 100
    return max(min(pct_change / 1.5, 1.0), -1.0)

def _load_calendar_surprises() -> Dict[str, float]:
    surprises: Dict[str, List[float]] = {}
    cal = _get_calendar_data()
    if not cal:
        return {}

    now = datetime.now(UTC)
    for ev in cal.get("events", []):
        actual_str = (ev.get("actual") or "").strip()
        forecast_str = (ev.get("forecast") or "").strip()
        if not actual_str or not forecast_str:
            continue

        try:
            actual = float(_RE_NUMERIC_CLEAN.sub("", actual_str))
            forecast = float(_RE_NUMERIC_CLEAN.sub("", forecast_str))
        except (ValueError, TypeError):
            continue

        event_time = ev.get("event_time")
        if event_time:
            try:
                et = datetime.fromisoformat(event_time)
                if et.tzinfo is None:
                    et = et.replace(tzinfo=UTC)
                if (now - et).days > 7:
                    continue
            except Exception:
                continue                                                     

        currency = (ev.get("currency", "") or "").upper()
        if currency not in ("USD", "EUR", "GBP"):
            continue

        if abs(forecast) > 0.01:
            surprise = (actual - forecast) / abs(forecast)
        else:
            surprise = actual - forecast

        title_lower = (ev.get("title", "") or "").lower()
        indicator_weight = 0.4                                     
        if any(kw in title_lower for kw in ("cpi", "inflation", "consumer price")):
            indicator_weight = 1.0
        elif any(kw in title_lower for kw in ("nfp", "non-farm", "payroll", "employment")):
            indicator_weight = 1.0
        elif any(kw in title_lower for kw in ("gdp", "gross domestic")):
            indicator_weight = 0.8
        elif any(kw in title_lower for kw in ("pmi", "purchasing manager")):
            indicator_weight = 0.6
        elif any(kw in title_lower for kw in ("retail", "trade balance")):
            indicator_weight = 0.5

        surprise = max(min(surprise * indicator_weight, 2.0), -2.0)
        surprises.setdefault(currency, []).append(surprise)

    result = {}
    for curr, vals in surprises.items():
        if vals:
            avg = sum(vals) / len(vals)
            result[curr] = round(max(min(avg, 1.0), -1.0), 3)
            log.info(f"surprise[{curr}]: {result[curr]} (from {len(vals)} events)")

    return result

def _load_cache():
    try:
        with open(CACHE_FILE, encoding="utf-8") as f:
            data = json.load(f)
        ts = datetime.fromisoformat(data["timestamp"])
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=UTC)
        if (datetime.now(UTC) - ts).total_seconds() < CACHE_TTL:
            return data["macro"]
    except FileNotFoundError:
        pass
    except Exception as exc:
        log.warning(f"cache load: {exc}")
    return None

def _load_prev_from_cache():
    try:
        with open(CACHE_FILE, encoding="utf-8") as f:
            macro = json.load(f).get("macro", {})
        return {
            "us_rate": macro.get("us", {}).get("rate"),
            "eu_rate": macro.get("eu", {}).get("rate"),
            "gb_rate": macro.get("gb", {}).get("rate"),
            "rate_history": macro.get("rate_history", {}),
            "prev_vix": macro.get("vix"),
            "prev_yield_spread": macro.get("us", {}).get("yield_spread"),
            "prev_pmi": {
                "us": macro.get("pmi"), "eu": macro.get("eu", {}).get("pmi"),
                "gb": macro.get("gb", {}).get("pmi"),
            },
            "prev_scores": macro.get("_prev_scores", {}),
            "factor_history": macro.get("_factor_history", {}),
        }
    except Exception:
        return {
            "us_rate": None, "eu_rate": None, "gb_rate": None,
            "rate_history": {},
            "prev_vix": None, "prev_yield_spread": None, "prev_pmi": {},
            "prev_scores": {}, "factor_history": {},
        }

def _save_cache(macro):
    tmp = CACHE_FILE + ".tmp"
    def _json_default(obj):
        # Guard against datetime objects leaking in from price-context dicts or
        # any other source; json.dump raises TypeError without this handler.
        if isinstance(obj, datetime):
            return obj.isoformat()
        raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump({"timestamp": datetime.now(UTC).isoformat(), "macro": macro}, f,
                      indent=2, default=_json_default)
        os.replace(tmp, CACHE_FILE)
        try: os.chmod(CACHE_FILE, 0o644)
        except OSError: pass
    except Exception as exc:
        log.warning(f"cache save: {exc}")
        try: os.remove(tmp)
        except OSError: pass

def compute_data_quality(macro):
    score = 1.0
    penalties = {}
    categories = {"major": [], "moderate": [], "minor": [], "anomaly": []}

    def _p(key, amt, cond, category="moderate"):
        nonlocal score
        if cond:
            penalties[key] = amt
            categories[category].append(key)
            score -= amt

    us = macro.get("us", {})

    _p("fed_rate", 0.25, us.get("rate", 0.0) == 0.0, "major")
    _p("yield_10y", 0.20, us.get("yield_10y", 0.0) == 0.0, "major")

    _p("tips", 0.10, not us.get("tips_valid", False), "moderate")
    _p("vix", 0.10, not macro.get("vix_valid", False), "moderate")
    _p("ecb_rate", 0.10, macro.get("eu", {}).get("rate", 0.0) == 0.0, "moderate")
    _p("boe_rate", 0.08, macro.get("gb", {}).get("rate", 0.0) == 0.0, "moderate")

    _p("us_pmi", 0.05, not macro.get("pmi_valid", False), "minor")
    _p("eu_pmi", 0.03, not macro.get("eu", {}).get("pmi_valid", False), "minor")
    _p("gb_pmi", 0.03, not macro.get("gb", {}).get("pmi_valid", False), "minor")

    for region in ("us", "eu", "gb"):
        series = macro.get("rate_history", {}).get(region, [])
        if len(series) < 2:
            _p(f"{region}_hist_thin", 0.03, True, "minor")
            _p(f"{region}_hist_stale", 0.015, True, "minor")                

    _p("curve_anomaly", 0.10, abs(us.get("yield_spread", 0.0)) > 5.0, "anomaly")
    _p("vix_anomaly", 0.10, macro.get("vix", 20) < 8 or macro.get("vix", 20) > 80, "anomaly")
    for pair, dv in macro.get("diff", {}).items():
        _p(f"diff_{pair}", 0.05, abs(dv) > 10.0, "anomaly")

    prev = macro.get("prev", {})
    _is_blind = _momentum_blind(macro)
    _p("momentum_blind", 0.08, _is_blind, "moderate")
    if not _is_blind:
        nones = sum(1 for k in ("us_rate", "eu_rate", "gb_rate") if prev.get(k) is None)
        _p("momentum_partial", 0.04, nones >= 2, "minor")

    score = max(0.0, min(score, 1.0))
    return {
        "score": round(score, 3),
        "grade": "A" if score >= 0.85 else "B" if score >= 0.70 else "C" if score >= 0.50 else "D",
        "penalties": penalties,
        "categories": {k: v for k, v in categories.items() if v},
    }

_REQUIRED_KEYS = ("us", "eu", "gb", "vix", "diff")

_FIELD_DEFAULTS: Dict[str, Any] = {
    "vix": VIX_NEUTRAL_FALLBACK,
    "vix_valid": False,
    "vix_trend": 0.0,
    "pmi": PMI_NEUTRAL,
    "pmi_valid": False,
    "pmi_deltas": {"us": 0.0, "eu": 0.0, "gb": 0.0},
    "surprises": {},
    "price_momentum": {},
    "price_momentum_sources": {},
    "price_momentum_diagnostics": {},
}

_NESTED_DEFAULTS: Dict[str, Dict[str, Any]] = {
    "us": {
        "rate": 0.0,
        "yield_spread": 0.0,
        "yield_10y": 0.0,
        "tips_10y": 0.0,
        "tips_valid": False,
    },
    "eu": {"rate": 0.0, "pmi": PMI_NEUTRAL, "pmi_valid": False, "pmi_source": "unavailable"},
    "gb": {"rate": 0.0, "pmi": PMI_NEUTRAL, "pmi_valid": False, "pmi_source": "unavailable"},
    "diff": {"eurusd": 0.0, "gbpusd": 0.0},
    "spx": {"current": 0.0, "prev": 0.0, "valid": False, "source": "unavailable"},
    "gold": {
        "enabled": False,
        "real_yield_momentum": {"current": 0.0, "prev": 0.0, "change": 0.0, "valid": False, "source": "unavailable"},
        "broad_usd": {"current": 0.0, "prev": 0.0, "change": 0.0, "valid": False, "source": "unavailable"},
        "inflation_expectations": {"current": 0.0, "prev": 0.0, "change": 0.0, "valid": False, "source": "unavailable"},
        "treasury_vol": {"current": 0.0, "prev": 0.0, "change": 0.0, "valid": False, "source": "unavailable"},
        "gold_vol": {"current": 0.0, "prev": 0.0, "change": 0.0, "valid": False, "source": "unavailable"},
        "energy_inflation": {"current": 0.0, "prev": 0.0, "change": 0.0, "valid": False, "source": "unavailable"},
    },
    "prev": {
        "us_rate": None,
        "eu_rate": None,
        "gb_rate": None,
        "prev_pmi": {"us": None, "eu": None, "gb": None},
        "us_yield_spread": None,
        "prev_vix": None,
    },
    "rate_history": {"us": [], "eu": [], "gb": []},
}

def _clone_default(value: Any) -> Any:
    return copy.deepcopy(value)

def _validate_macro_schema(macro: Dict, log_ref) -> Dict:
    warnings_list = []

    for key in _REQUIRED_KEYS:
        if key not in macro:
            warnings_list.append(key)
            if key in _NESTED_DEFAULTS:
                macro[key] = _clone_default(_NESTED_DEFAULTS[key])
            elif key in _FIELD_DEFAULTS:
                macro[key] = _clone_default(_FIELD_DEFAULTS[key])

    for key, default in _FIELD_DEFAULTS.items():
        if key not in macro:
            macro[key] = _clone_default(default)

    for key, defaults in _NESTED_DEFAULTS.items():
        if key not in macro or not isinstance(macro[key], dict):
            if key not in warnings_list:
                warnings_list.append(key)
            macro[key] = _clone_default(defaults)
            continue
        for subkey, subdefault in defaults.items():
            if subkey not in macro[key]:
                macro[key][subkey] = _clone_default(subdefault)

    if warnings_list:
        log_ref.warning(f"schema_validate: missing/invalid keys filled with defaults: {warnings_list}")

    return macro

def _smooth_vix(current_vix: float, prev_vix: Optional[float],
                alpha: float = 0.3) -> float:
    if prev_vix is None:
        return current_vix
    return round(alpha * current_vix + (1 - alpha) * prev_vix, 2)

def _rotate(series, val, maxlen=3):
    return (series + [val])[-maxlen:]

def _sanitize(series, fresh):
    if fresh == 0.0:
        return series
    return [fresh if v == 0.0 or abs(v - fresh) > 2.0 else v
            for v in series]

_FETCH_SOURCES: Dict[str, str] = {}
_FETCH_SOURCES_LOCK = threading.Lock()

def _set_fetch_source(key: str, value: str) -> None:
    with _FETCH_SOURCES_LOCK:
        _FETCH_SOURCES[key] = value

_FETCH_SOURCE_ALIASES: Dict[str, str] = {
    "us_fed_rate_pair": "fed_rate",
    "us_yield_data": "yield_data",
    "tips_yield": "tips",
    "pmi": "us_pmi",
    "eu_pmi": "eu_pmi",
    "eu_country_pmi": "eu_country_pmi",
    "uk_pmi": "uk_pmi",
    "eu_ecb_rate": "ecb_rate",
    "gb_boe_rate": "boe_rate",
    "vix": "vix",
    "spx": "spx",
}

def _source_label_for_component(key: str, value: Any = None) -> str:
    if key in ("pmi", "eu_pmi", "uk_pmi") and isinstance(value, (tuple, list)) and len(value) >= 3:
        source_name = str(value[2])
        if source_name:
            return source_name
    alias = _FETCH_SOURCE_ALIASES.get(key, key)
    return _FETCH_SOURCES.get(alias, _FETCH_SOURCES.get(key, "unknown"))

def _is_component_cacheworthy(key: str, value: Any) -> bool:
    if value is None:
        return False

    if key in ("pmi", "eu_pmi", "uk_pmi"):
        return (
            isinstance(value, (tuple, list))
            and len(value) >= 3
            and bool(value[1])
            and str(value[2]).lower() not in {"fallback", "unavailable", "unknown"}
        )

    if key in ("vix", "tips_yield"):
        return isinstance(value, (tuple, list)) and len(value) >= 2 and bool(value[1])

    if key in ("spx",):
        return isinstance(value, (tuple, list)) and len(value) >= 3 and bool(value[2])

    if key == "gold_macro":
        return isinstance(value, dict) and int(value.get("valid_count", 0) or 0) > 0
    if key == "eu_country_pmi":
        return isinstance(value, dict) and bool(value.get("valid"))

    if key == "us_yield_data":
        return (
            isinstance(value, (tuple, list))
            and len(value) >= 2
            and (abs(float(value[0])) > 1e-12 or abs(float(value[1])) > 1e-12)
        )

    if key == "us_fed_rate_pair":
        return isinstance(value, (tuple, list)) and len(value) >= 1 and abs(float(value[0])) > 1e-12

    if key in ("eu_ecb_rate", "gb_boe_rate"):
        try:
            return abs(float(value)) > 1e-12
        except (TypeError, ValueError):
            return False

    return True

def _macro_pair_key(value: Any) -> str:
    return _normalize_pair_code(str(value or ""))


def _macro_cross_canonical(raw: Any) -> str:
    value = str(raw or "").strip().lower()
    if value in ("cross_up", "bullish_cross", "golden_cross", "watch_long"):
        return "golden_cross"
    if value in ("cross_down", "bearish_cross", "death_cross", "watch_short"):
        return "death_cross"
    return "none"


def _load_scraper_td_seed_for_macro() -> Dict[str, Any]:
    """Load scraper.py's locally shared TD daily seed from macro_components.json."""
    candidates = [Path(COMPONENT_CACHE_FILE), Path("public/macro_components.json"), Path("macro_components.json")]
    seen = set()
    for path in candidates:
        try:
            key = str(path.resolve()) if path.exists() else str(path)
            if key in seen:
                continue
            seen.add(key)
            if not path.exists():
                continue
            with path.open(encoding="utf-8") as handle:
                data = json.load(handle)
            return data if isinstance(data, dict) else {}
        except Exception:
            continue
    return {}


def _scraper_indicator_to_macro_ema_state(pair: str, indicator: Dict[str, Any], current_price: Any = None) -> Dict[str, Any]:
    """Build an ema_20_50_state-compatible dict from scraper.py daily indicators."""
    if not isinstance(indicator, dict):
        return {}
    try:
        ema20 = float(indicator.get("ema20"))
        ema50 = float(indicator.get("ema50"))
    except (TypeError, ValueError):
        return {}
    try:
        price = float(current_price) if current_price is not None else None
    except (TypeError, ValueError):
        price = None
    fast_vs_slow = "above" if ema20 > ema50 else "below" if ema20 < ema50 else "at"
    bias = "bullish_bias" if fast_vs_slow == "above" else "bearish_bias" if fast_vs_slow == "below" else "neutral_bias"
    def _pos(v: Optional[float], ref: float) -> Optional[str]:
        if v is None:
            return None
        return "above" if v > ref else "below" if v < ref else "at"
    price_vs_fast = _pos(price, ema20)
    price_vs_slow = _pos(price, ema50)
    return {
        "pair": pair,
        "timeframe": "D1",
        "fast_period": 20,
        "slow_period": 50,
        "ema20": round(ema20, 6),
        "ema50": round(ema50, 6),
        "fast_ema": round(ema20, 6),
        "slow_ema": round(ema50, 6),
        "ema20_vs_ema50": fast_vs_slow,
        "trend_bias": bias,
        "price": round(price, 6) if price is not None else None,
        "current_vs_ema20": price_vs_fast,
        "current_vs_ema50": price_vs_slow,
        "price_vs_fast": price_vs_fast,
        "price_vs_slow": price_vs_slow,
        "cross": "none",
        "ema_cross_signal": "none",
        "warming_up": False,
        "ok": True,
        "source": "scraper_td_daily",
        "history_source": "scraper_td_daily",
        "scraper_daily_seeded": True,
        "bars_available": indicator.get("bars"),
        "bars_needed": indicator.get("required_bars", 50),
        "rsi14": indicator.get("rsi14"),
        "last_bar_date": indicator.get("last_bar_date"),
    }

def _normalise_ema_state_for_macro(pair: str, result: Dict[str, Any]) -> Dict[str, Any]:
    """Preserve upstream EMA state while adding stable aliases for downstream use."""
    if not isinstance(result, dict):
        return {}
    out = dict(result)
    pair_key = _macro_pair_key(out.get("pair") or pair)
    out["pair"] = pair_key
    fast = out.get("ema_fast", out.get("fast_ema", out.get("ema20", out.get("ema_20"))))
    slow = out.get("ema_slow", out.get("slow_ema", out.get("ema50", out.get("ema_50"))))
    price = out.get("last_close", out.get("price", out.get("close", out.get("current_price"))))
    if fast is not None:
        out.setdefault("ema_fast", fast); out.setdefault("fast_ema", fast)
    if slow is not None:
        out.setdefault("ema_slow", slow); out.setdefault("slow_ema", slow)
    if price is not None:
        out.setdefault("last_close", price); out.setdefault("price", price)
    bias = out.get("trend_bias") or out.get("bias")
    if bias not in ("bullish_bias", "bearish_bias", "neutral_bias"):
        try:
            f = float(fast); s = float(slow); p = float(price)
            if f > s and p >= f:
                bias = "bullish_bias"
            elif f < s and p <= f:
                bias = "bearish_bias"
            elif f > s or p > f:
                bias = "bullish_bias"
            elif f < s or p < f:
                bias = "bearish_bias"
            else:
                bias = "neutral_bias"
        except (TypeError, ValueError):
            bias = "neutral_bias"
    out["trend_bias"] = bias
    cross = _macro_cross_canonical(out.get("ema_cross_signal", out.get("cross", out.get("suggestion"))))
    out["cross"] = cross
    out["ema_cross_signal"] = cross
    if "current_vs_fast" not in out and "price_vs_fast" in out:
        out["current_vs_fast"] = out.get("price_vs_fast")
    if "current_vs_slow" not in out and "price_vs_slow" in out:
        out["current_vs_slow"] = out.get("price_vs_slow")
    out.setdefault("source", out.get("data_source", out.get("history_source", "momentum")))
    return out


def build_macro() -> Dict:
    global _FETCH_SOURCES
    _FETCH_SOURCES = {}
    _calendar_run_cache.clear()                                
    run_id = os.environ.get("SCRAPER_RUN_ID") or uuid.uuid4().hex[:8]
    os.environ.setdefault("SCRAPER_RUN_ID", run_id)
    _log_macro_startup_once()
    log.info(f"[{run_id}] build_macro start")

    cached_prev = _load_prev_from_cache()
    comp_cache = _load_component_cache()

    # ── Enrich calendar.json with Stooq Manufacturing PMI ────────────────────
    # Fetch pmmnus.m / pmmneu.m / pmmnuk.m and backfill blank *actual* fields
    # in the in-memory calendar BEFORE the parallel PMI fetchers run, so that
    # _fetch_pmi_from_calendar() inside fetch_pmi / fetch_eu_pmi / fetch_uk_pmi
    # finds real values instead of falling back to Stooq bias-scoring symbols.
    _cal_data = _get_calendar_data()                    # loads disk → _calendar_run_cache
    _cal_events = list(_cal_data.get("events", []))
    _cal_events, _pmi_cal_n = enrich_calendar_with_pmi_stooq(_cal_events, comp_cache)
    if _pmi_cal_n:
        _calendar_run_cache["data"] = {**_cal_data, "events": _cal_events}
        log.info(
            "[%s] PMI Stooq→cal: %d change(s) applied to in-memory calendar",
            run_id, _pmi_cal_n,
        )
    # ─────────────────────────────────────────────────────────────────────────

    fetchers = {
        "us_fed_rate_pair": fetch_fed_rate_with_prev,
        "us_yield_data": fetch_yield_data,
        "tips_yield": fetch_tips_yield,
        "gold_macro": fetch_gold_macro_data,
        "pmi": fetch_pmi,
        "eu_pmi": fetch_eu_pmi,
        "eu_country_pmi": fetch_eu_country_pmi,
        "uk_pmi": fetch_uk_pmi,
        "eu_ecb_rate": fetch_ecb_rate,
        "gb_boe_rate": fetch_boe_rate,
        "vix": fetch_vix,
        "spx": fetch_spx,
    }

    fallbacks = {
        "us_fed_rate_pair": (0.0, None), "us_yield_data": (0.0, 0.0),
        "tips_yield": (0.0, False), "gold_macro": dict(_NESTED_DEFAULTS.get("gold", {})), "pmi": (PMI_NEUTRAL, False, "fallback"),
        "eu_pmi": (PMI_NEUTRAL, False, "fallback"), "eu_country_pmi": {}, "uk_pmi": (PMI_NEUTRAL, False, "fallback"),
        "eu_ecb_rate": 0.0, "gb_boe_rate": 0.0,
        "vix": (VIX_NEUTRAL_FALLBACK, False),
        "spx": (0.0, 0.0, False),
    }

    to_fetch, cached_results = {}, {}
    for key in fetchers:
        cv = _get_cached_component(comp_cache, key)
        if cv is not None:
            cached_results[key] = cv
        else:
            to_fetch[key] = fetchers[key]

    results = dict(cached_results)
    if cached_results:
        log.info(f"[{run_id}] component cache hits: {list(cached_results.keys())}")
    if to_fetch:
        log.info(f"[{run_id}] fetching live: {list(to_fetch.keys())}")
        _BUILD_TIMEOUT = 45                                                      
        with ThreadPoolExecutor(max_workers=min(MAX_WORKERS, len(to_fetch))) as ex:
            futs = {ex.submit(fn): key for key, fn in to_fetch.items()}
            done, not_done = _futures_wait(futs, timeout=_BUILD_TIMEOUT, return_when=ALL_COMPLETED)
            for fut in not_done:
                key = futs[fut]
                fut.cancel()
                log.warning(f"[{run_id}] fetcher [{key}]: timed out after {_BUILD_TIMEOUT}s")
                stale = _get_stale_component(comp_cache, key)
                if stale is not None:
                    results[key] = stale
                    log.info(f"[{run_id}] source[{key}]: stale cache fallback (timeout)")
                else:
                    results[key] = fallbacks[key]
                    log.info(f"[{run_id}] source[{key}]: hardcoded fallback (timeout)")
            for fut in done:
                key = futs[fut]
                try:
                    val = fut.result()
                    results[key] = val
                    source_label = _source_label_for_component(key, val)
                    if _is_component_cacheworthy(key, val):
                        _set_component(comp_cache, key, val)
                        cache_note = "cached"
                    else:
                        cache_note = "not cached (fallback/invalid)"
                    log.info(f"[{run_id}] source[{key}]: live fetch OK "
                             f"(src={source_label}; {cache_note})")
                except Exception as exc:
                    log.warning(f"[{run_id}] fetcher [{key}]: {exc}")
                    stale = _get_stale_component(comp_cache, key)
                    if stale is not None:
                        results[key] = stale
                        log.info(f"[{run_id}] source[{key}]: stale cache fallback")
                    else:
                        results[key] = fallbacks[key]
                        log.info(f"[{run_id}] source[{key}]: hardcoded fallback")
        _save_component_cache(comp_cache)

    def _tup(key, default, n=2):
        val = results.get(key, default)
        if isinstance(val, (tuple, list)):
            if len(val) == n:
                return tuple(val)
            log.warning(f"_tup: key={key!r} expected len={n}, got len={len(val)} -- using default")
        return default

    def _scalar(key, default=0.0):
        v = results.get(key, default)
        return v[0] if isinstance(v, (tuple, list)) else float(v)

    fed, fed_prev = _tup("us_fed_rate_pair", (0.0, None))
    if fed_prev is None:
        fed_prev = cached_prev.get("us_rate")
    yield_spread, yield_10y = _tup("us_yield_data", (0.0, 0.0))
    tips_10y, tips_valid = _tup("tips_yield", (0.0, False))
    pmi_value, pmi_valid, pmi_src = _tup("pmi", (PMI_NEUTRAL, False, "fallback"), 3)
    eu_pmi_v, eu_pmi_valid, eu_pmi_src = _tup("eu_pmi", (PMI_NEUTRAL, False, "fallback"), 3)
    uk_pmi_v, uk_pmi_valid, uk_pmi_src = _tup("uk_pmi", (PMI_NEUTRAL, False, "fallback"), 3)
    eu_country_pmi = results.get("eu_country_pmi", {})
    if not isinstance(eu_country_pmi, dict):
        eu_country_pmi = {}
    ecb, boe = _scalar("eu_ecb_rate"), _scalar("gb_boe_rate")
    vix_value, vix_valid = _tup("vix", (VIX_NEUTRAL_FALLBACK, False))

    spx_raw = results.get("spx", (0.0, 0.0, False))
    if isinstance(spx_raw, (tuple, list)) and len(spx_raw) == 3:
        spx_curr, spx_prev, spx_valid = spx_raw
    else:
        spx_curr, spx_prev, spx_valid = 0.0, 0.0, False

    gold_macro = results.get("gold_macro")
    if not isinstance(gold_macro, dict):
        gold_macro = dict(_NESTED_DEFAULTS.get("gold", {}))

    prev_vix = cached_prev.get("prev_vix")
    vix_ema = _smooth_vix(vix_value, prev_vix)
    vix_trend = round(vix_ema - prev_vix, 2) if prev_vix is not None else 0.0

    prev_pmi = cached_prev.get("prev_pmi", {})
    pmi_deltas = {
        "us": round(pmi_value - (prev_pmi.get("us") or pmi_value), 2),
        "eu": round(eu_pmi_v - (prev_pmi.get("eu") or eu_pmi_v), 2),
        "gb": round(uk_pmi_v - (prev_pmi.get("gb") or uk_pmi_v), 2),
    }

    surprises = _load_calendar_surprises()

    macro = {
        "timestamp": datetime.now(UTC).isoformat(),
        "run_id": run_id,
        "us": {
            "rate": fed, "yield_spread": yield_spread,
            "yield_10y": yield_10y, "tips_10y": tips_10y, "tips_valid": tips_valid,
        },
        "eu": {
            "rate": ecb, "pmi": eu_pmi_v, "pmi_valid": eu_pmi_valid, "pmi_source": eu_pmi_src,
            "country_pmi": eu_country_pmi,
            "country_pmi_score": float(eu_country_pmi.get("score", 0.0) or 0.0),
            "country_pmi_valid": bool(eu_country_pmi.get("valid", False)),
            "country_pmi_source": eu_country_pmi.get("source", "unavailable"),
        },
        "gb": {"rate": boe, "pmi": uk_pmi_v, "pmi_valid": uk_pmi_valid, "pmi_source": uk_pmi_src},
        "vix": vix_value, "vix_valid": vix_valid, "vix_trend": vix_trend,
        "pmi": pmi_value, "pmi_valid": pmi_valid, "pmi_source": pmi_src, "pmi_deltas": pmi_deltas,
        "spx": {"current": spx_curr, "prev": spx_prev, "valid": spx_valid, "source": _source_label_for_component("spx")},
        "gold": gold_macro,
        "surprises": surprises,
        "prev": {
            "us_rate":        fed_prev,
            "eu_rate":        cached_prev.get("eu_rate"),
            "gb_rate":        cached_prev.get("gb_rate"),
            "prev_pmi": {
                "us": cached_prev.get("prev_pmi", {}).get("us"),
                "eu": cached_prev.get("prev_pmi", {}).get("eu"),
                "gb": cached_prev.get("prev_pmi", {}).get("gb"),
            },
            "prev_vix":        cached_prev.get("prev_vix"),
        },
        "diff": {
            "eurusd": round(ecb - fed, 4),
            "gbpusd": round(boe - fed, 4),
        },
    }

    macro["prev"]["us_yield_spread"] = cached_prev.get("prev_yield_spread")

    prev_hist = cached_prev.get("rate_history", {})
    macro["rate_history"] = {
        r: _rotate(_sanitize(prev_hist.get(r, []), v), v)
        for r, v in [("us", fed), ("eu", ecb), ("gb", boe)]
    }

    macro["ema_20_50_state"] = {}

    macro["_regime"] = simple_regime(macro)
    regime_str = macro["_regime"]                           
    macro["_prev_scores"] = cached_prev.get("prev_scores", {})
    macro["_factor_history"] = cached_prev.get("factor_history", {})
    macro["vix_ema"] = vix_ema
    macro["main_pairs"] = sorted(ACTIVE_MAIN_PAIRS)

    _scraper_td_seed = _load_scraper_td_seed_for_macro()
    macro["price_history"] = _scraper_td_seed.get("price_history", {}) or _scraper_td_seed.get("price_daily_history", {}) or {}
    macro["price_indicators"] = _scraper_td_seed.get("price_indicators", {}) or _scraper_td_seed.get("price_daily_indicators", {}) or {}

    if _MOMENTUM_OK:
        price_mom, price_mom_sources = _fetch_price_momentum(
            pairs=sorted(ACTIVE_MAIN_PAIRS),
        )
        macro["price_momentum"] = price_mom
        macro["price_momentum_sources"] = price_mom_sources
        macro["price_momentum_diagnostics"] = _get_last_momentum_diagnostics()
        if MACRO_DEBUG and price_mom:
            log.info(
                f"[{run_id}] price_momentum (trend-impulse): {price_mom} "
                f"sources={price_mom_sources}"
            )
            if macro["price_momentum_diagnostics"]:
                log.info(
                    f"[{run_id}] price_momentum diagnostics: "
                    f"{macro['price_momentum_diagnostics']}"
                )
        # EMA states are already computed inside _fetch_price_momentum from the
        # same H1 histories — retrieve them without another provider request.
        # _compute_ema_state always returns a dict, so all pairs are present;
        # warming-up pairs have ok=False and all comparative fields set to None.
        ema_state: Dict[str, Any] = {}
        for _pair, _result in _get_last_ema_state().items():
            if not isinstance(_result, dict):
                continue
            _pair_key = _macro_pair_key(_pair)
            if _pair_key not in ACTIVE_MAIN_PAIRS:
                continue
            ema_state[_pair_key] = _normalise_ema_state_for_macro(_pair_key, _result)
        # Fill any missing/warming EMA states from scraper.py's local TD daily seed.
        for _pair_key, _indicator in (macro.get("price_indicators", {}) or {}).items():
            _pair_key = _macro_pair_key(_pair_key)
            if _pair_key not in ACTIVE_MAIN_PAIRS:
                continue
            _existing = ema_state.get(_pair_key, {})
            if not _existing or _existing.get("warming_up") or not _existing.get("ok"):
                _seeded_state = _scraper_indicator_to_macro_ema_state(
                    _pair_key,
                    _indicator if isinstance(_indicator, dict) else {},
                    (macro.get("_price_current", {}) or {}).get(_pair_key),
                )
                if _seeded_state:
                    ema_state[_pair_key] = _normalise_ema_state_for_macro(_pair_key, _seeded_state)
        macro["ema_20_50_state"] = ema_state
        if MACRO_DEBUG and ema_state:
            log.info(f"[{run_id}] ema_20_50_state: {ema_state}")
    else:
        macro.setdefault("price_momentum", {})
        macro.setdefault("price_momentum_sources", {})
        macro.setdefault("price_momentum_diagnostics", {})
        seeded_ema_state: Dict[str, Any] = {}
        for _pair_key, _indicator in (macro.get("price_indicators", {}) or {}).items():
            _pair_key = _macro_pair_key(_pair_key)
            if _pair_key in ACTIVE_MAIN_PAIRS:
                _seeded_state = _scraper_indicator_to_macro_ema_state(
                    _pair_key,
                    _indicator if isinstance(_indicator, dict) else {},
                    (macro.get("_price_current", {}) or {}).get(_pair_key),
                )
                if _seeded_state:
                    seeded_ema_state[_pair_key] = _normalise_ema_state_for_macro(_pair_key, _seeded_state)
        macro.setdefault("ema_20_50_state", seeded_ema_state)

    macro["data_quality"] = compute_data_quality(macro)

    _pm_sources = macro.get("price_momentum_sources", {})
    _pm_vals = sorted(set(v for v in _pm_sources.values() if v))
    _mom_source = "/".join(_pm_vals) if _pm_vals else ("blind" if _MOMENTUM_OK else "unavailable")
    macro["sources"] = {
        "fed_rate":   _FETCH_SOURCES.get("fed_rate", "fallback"),
        "ecb_rate":   _FETCH_SOURCES.get("ecb_rate", "fallback"),
        "boe_rate":   _FETCH_SOURCES.get("boe_rate", "fallback"),
        "yield_data": _FETCH_SOURCES.get("yield_data", "fallback"),
        "tips":       _FETCH_SOURCES.get("tips", "fallback"),
        "vix":        _FETCH_SOURCES.get("vix", "fallback"),
        "spx":        _FETCH_SOURCES.get("spx", "fallback"),
        "gold_macro": _FETCH_SOURCES.get("gold_macro", "fallback"),
        "us_pmi":     pmi_src,
        "eu_pmi":     eu_pmi_src,
        "uk_pmi":     uk_pmi_src,
        "momentum":   _mom_source,
        "momentum_module": _MOMENTUM_MODULE,
        "pivot_module": _PIVOT_MODULE,
        "provider_policy": "momentum: Twelve Data; pivot: Twelve Data",
    }

    macro = _validate_macro_schema(macro, log)

    dq = macro["data_quality"]

    if MACRO_DEBUG:
        macro["debug"] = {
            "run_id": run_id,
            "rate_inputs": {"fed": fed, "fed_prev": fed_prev, "ecb": ecb, "boe": boe},
            "yield_inputs": {"spread": yield_spread, "y10": yield_10y, "tips": tips_10y, "tips_valid": tips_valid},
            "risk_inputs": {"vix": vix_value, "vix_ema": vix_ema, "vix_trend": vix_trend, "vix_valid": vix_valid},
            "growth_inputs": {"pmi": pmi_value, "pmi_valid": pmi_valid, "pmi_source": pmi_src, "pmi_deltas": pmi_deltas},
            "spx_inputs": {"current": spx_curr, "prev": spx_prev, "valid": spx_valid},
            "surprises": surprises,
            "momentum_summary": _momentum_summary(macro) if _MOMENTUM_OK else {},
            "momentum_diagnostics": macro.get("price_momentum_diagnostics", {}),
            "regime": {"string": regime_str},
            "dq": dq,
        }
        log.info(f"[{run_id}] MACRO_DEBUG: debug block attached ({len(macro['debug'])} keys)")

    log.info(
        f"[{run_id}] macro built -- fed={fed} ecb={ecb} boe={boe} "
        f"spread={yield_spread} vix={vix_value}(ema={vix_ema})(t={vix_trend:+.1f})(valid={vix_valid}) "
        f"pmi={pmi_value}(d={pmi_deltas['us']:+.1f})(valid={pmi_valid}) "
        f"spx={spx_curr}(v={spx_valid}) "
        f"surprises={surprises} "
        f"regime={regime_str} dq={dq['score']}({dq['grade']})"
    )
    try:
        log.info("[macro][run_id=%s] summary regime=%s dq=%.3f/%s momentum=%s sources=%s",
                 run_id, macro.get("regime", "unknown"), float(dq.get("score", 0.0)), dq.get("grade", "na"), _MOMENTUM_OK, dict(sorted(_FETCH_SOURCES.items())))
    except Exception as exc:
        log.warning("[macro][run_id=%s] summary logging failed: %s", run_id, exc)
    return macro

def get_macro_context() -> Dict:
    cached = _load_cache()
    if isinstance(cached, dict) and cached:
        log.info("[macro][run_id=%s] cache_hit file=%s", _run_id(), CACHE_FILE)
        return cached
    try:
        macro = build_macro()
        _save_cache(macro)
        macro["_needs_resave"] = True
        return macro
    except Exception as exc:
        log.error(f"build_macro() FAILED: {exc}")
        macro = _emergency_fallback(exc)
        if macro:
            _save_cache(macro)
            macro["_needs_resave"] = True
        return macro

def _emergency_fallback(exc: Exception) -> Dict:
    try:
        with open(CACHE_FILE, encoding="utf-8") as f:
            data = json.load(f)
        old_macro = data.get("macro", {})
        if not old_macro:
            raise FileNotFoundError("empty cache")

        decay = 0.5                                        
        log.warning(f"emergency_fallback: using cached macro with {decay}x decay")

        for pair, score in old_macro.get("_prev_scores", {}).items():
            if isinstance(score, (int, float)):
                old_macro["_prev_scores"][pair] = round(score * decay, 3)

        old_macro["_emergency"] = True
        old_macro["_emergency_reason"] = str(exc)
                                                                                  
                                                                                    
        old_macro["data_quality"] = {
            "score": 0.3, "grade": "D",
            "penalties": {"emergency_fallback": 0.7},
            "categories": {"major": ["emergency_fallback"]},
        }
        return old_macro
    except Exception:
        log.error("emergency_fallback: no usable cache ---- returning empty macro")
        return {}

def _dynamic_inflation_proxy(macro):
    us = macro.get("us", {})
    if us.get("tips_valid") and us.get("tips_10y", 0.0) != 0.0:
        return max(min(us["yield_10y"] - us["tips_10y"], 6.0), 0.0)
    us_rate = us.get("rate", 0.0)
    us_prev = macro.get("prev", {}).get("us_rate")
    if us_prev is None:
        return 2.5
    d = us_rate - us_prev
    cycle = ("aggressive_hiking" if d >= 0.50 else "hiking" if d > 0.05
             else "aggressive_cutting" if d <= -0.50 else "cutting" if d < -0.05
             else "neutral")
    return _INFLATION_PROXIES.get(cycle, 2.5)

def _inflation_regime(macro):
    p = _dynamic_inflation_proxy(macro)
    return "high_inflation" if p > 3.5 else "low_inflation" if p < 1.5 else "neutral_inflation"

def _rate_cycle(macro):
    us_rate = macro.get("us", {}).get("rate", 0.0)
    us_prev = macro.get("prev", {}).get("us_rate")
    if us_prev is None:
        return "neutral"
    d = us_rate - us_prev
    return ("aggressive_cutting" if d <= -0.50 else "cutting" if d < -0.05
            else "aggressive_hiking" if d >= 0.50 else "hiking" if d > 0.05
            else "neutral")

def _rate_stance(macro):
    r = macro.get("us", {}).get("rate", 0.0)
    return "restrictive" if r >= 4.5 else "accommodative" if r <= 2.0 else "neutral_stance"

def simple_regime(macro: Dict) -> str:
    vix = macro.get("vix_ema", macro.get("vix", VIX_NEUTRAL_FALLBACK))
    rate = macro.get("us", {}).get("rate", 0.0)
    if vix > 24:
        return "risk_off"
    if rate > 4.0:
        return "tight_policy"
    return "neutral"

def compute_regime_confidence(macro: Dict) -> float:
    spread = macro.get("us", {}).get("yield_spread", 0.0)
    curve_risk = "risk_off" if spread < -0.3 else "risk_on" if spread > 0.5 else "neutral"

    vix = macro.get("vix_ema", macro.get("vix", VIX_NEUTRAL_FALLBACK))
    vix_risk = "risk_off" if vix > 25 else "risk_on" if vix < 15 else "neutral"

    spx = macro.get("spx", {})
    spx_risk = "neutral"
    if spx.get("valid") and spx.get("prev", 0) > 0:
        pct = (spx["current"] - spx["prev"]) / spx["prev"] * 100
        spx_risk = "risk_on" if pct > 0.5 else "risk_off" if pct < -0.5 else "neutral"

    votes = [curve_risk, vix_risk, spx_risk]
    risk_off_n = sum(1 for v in votes if v == "risk_off")
    risk_on_n  = sum(1 for v in votes if v == "risk_on")
    dominant   = max(risk_off_n, risk_on_n)
    total      = len(votes)
    return round(0.33 + 0.67 * (dominant / total), 3)

def compute_signal_horizon(macro: Dict) -> str:
    vix = macro.get("vix", VIX_NEUTRAL_FALLBACK)
    spx = macro.get("spx", {})
    spx_valid = spx.get("valid", False) and spx.get("prev", 0) > 0
    spx_move = abs((spx["current"] - spx["prev"]) / spx["prev"] * 100) if spx_valid else 0.0

    if vix > 22 or spx_move > 1.0:
        return "short"

    pmi = macro.get("pmi", PMI_NEUTRAL)
    spread = macro.get("us", {}).get("yield_spread", 0.0)
    if abs(pmi - PMI_NEUTRAL) > 2.5 or abs(spread) > 0.5:
        return "long"

    return "medium"

def detect_regime(macro):
    cycle = _rate_cycle(macro)
    vix = macro.get("vix", VIX_NEUTRAL_FALLBACK)
    risk = "risk_off" if vix > 25 else "risk_on" if vix < 15 else "neutral"
    return f"{cycle}_{risk}_{_inflation_regime(macro)}_{_rate_stance(macro)}"

def regime_weights(regime):
    w = dict(FACTOR_WEIGHTS)

    if regime == "risk_off" or "risk_off" in regime:
        w["risk"] = round(w["risk"] * 1.5, 4)
        w["usd"] = round(w["usd"] * 1.3, 4)
    elif regime == "tight_policy" or "hiking" in regime or "restrictive" in regime:
        w["rate"] = round(w["rate"] * 1.3, 4)
    elif "risk_on" in regime:
        w["risk"] = round(w["risk"] * 0.7, 4)

    return {k: round(v, 4) for k, v in w.items()}

def _rate_factor_usd(macro):
    return max(min((macro["us"]["rate"] - 2.5) / 2.0, 1.5), -1.5)

def _usd_factor_clean(macro):
    rate = _rate_factor_usd(macro)
    risk = _risk_factor_fx(macro)
    return round(0.6 * rate + 0.4 * risk, 3)

def _rate_trend_strength(series):
    if len(series) < 2:
        return 0.0
    deltas = [series[i] - series[i-1] for i in range(1, len(series))]
    return max(min(sum(deltas) / len(deltas) / 0.25, 1.0), -1.0)

def _rate_factor_spread(base_rate, quote_rate, base_hist, quote_hist, scale=2.0):
    spot = max(min((base_rate - quote_rate) / scale, 1.5), -1.5)
    trend = (_rate_trend_strength(base_hist) - _rate_trend_strength(quote_hist)) * 0.3
    return round(max(min(spot + trend, 1.5), -1.5), 3)

def _curve_factor(macro):
    s = macro["us"]["yield_spread"]
    if s < 0:
        return round(-1.5 * min(abs(s) / 1.0, 1.0), 3)
    return round(1.0 * min(s / 2.0, 1.0), 3)

def _risk_factor_fx(macro):
    vix = macro.get("vix", VIX_NEUTRAL_FALLBACK)
    raw = max(min((vix - 20.0) / 5.0, 2.0), -2.0)
    dampen = 0.7 if macro["us"]["rate"] > 2.0 else 0.5
    vix_comp = raw * dampen + max(min(macro.get("vix_trend", 0.0) / 5.0, 0.3), -0.3)
    curve_comp = -_curve_factor(macro) * 0.5
    return round(0.7 * vix_comp + 0.3 * curve_comp, 3)

def _risk_factor_gold_raw(macro):
    vix = macro.get("vix", VIX_NEUTRAL_FALLBACK)
    raw = max(min((vix - 20.0) / 5.0, 2.0), -2.0)
    return raw + max(min(macro.get("vix_trend", 0.0) / 5.0, 0.3), -0.3)

def _risk_concave(r):
    return 0.0 if r == 0.0 else math.copysign(abs(r) ** 0.8, r)

def _rate_momentum(curr, prev):
    if prev is None: return 0.0
    return max(min((curr - prev) / 0.25, 2.0), -2.0)

def _real_yield_factor(macro):
    us = macro.get("us", {})
    if us.get("tips_valid") and us.get("tips_10y", 0.0) != 0.0:
        real = us["tips_10y"]
    else:
        real = us.get("yield_10y", 0.0) - _dynamic_inflation_proxy(macro)
    return max(min(-real / 1.5, 1.5), -1.5)

def _growth_factor(macro):
    pmi = macro.get("pmi", PMI_NEUTRAL)
    level = (pmi - PMI_NEUTRAL) / PMI_SCALE
    delta = macro.get("pmi_deltas", {}).get("us", 0.0) / PMI_SCALE
    return max(min(level + 0.5 * delta, 1.5), -1.5)

def _growth_factor_regional(base_pmi, quote_pmi, base_delta=0.0, quote_delta=0.0):
    level = (base_pmi - quote_pmi) / PMI_SCALE
    delta = (base_delta - quote_delta) / PMI_SCALE
    return max(min(level + 0.5 * delta, 1.5), -1.5)

def _growth_factor_fx_usd(macro):
    return round((_growth_factor(macro) - 0.3 * _rate_factor_usd(macro)) * 0.5, 3)

def _liquidity_factor_gold(macro):
    return round(-_rate_momentum(macro["us"]["rate"],
                                  macro.get("prev", {}).get("us_rate")) * 0.5, 3)

def _tanh_score(raw): return math.tanh(raw / 2.0) * SCORE_MAX

def _clamp(score): return max(min(score, SCORE_MAX), -SCORE_MAX)

def _conviction(score: float) -> float:
    abs_score = abs(score)
    if abs_score == 0.0:
        return 0.0
    if abs_score >= MACRO_SCORE_STRONG:
        return score
    if abs_score < MACRO_SCORE_MIN:
        return round(score * 0.5, 3)

    span = max(MACRO_SCORE_STRONG - MACRO_SCORE_MIN, 1e-9)
    frac = (abs_score - MACRO_SCORE_MIN) / span
    mult = 0.75 + 0.25 * frac
    return round(score * mult, 3)

def _surprise_adjustment(macro, base_currency, quote_currency="USD"):
    surprises = macro.get("surprises", {})
    if not surprises:
        return 0.0
    base_s = surprises.get(base_currency, 0.0)
    quote_s = surprises.get(quote_currency, 0.0)
    return round(max(min((base_s - quote_s) * 0.5, 0.5), -0.5), 3)

def bias_confidence(score, vix_valid=True, pmi_valid=True, data_quality=1.0):
    tier = "high" if abs(score) >= 1.5 else "medium" if abs(score) >= 0.8 else "low"
    if tier == "high" and not (vix_valid and pmi_valid):
        tier = "medium"
    if data_quality < 0.55:
        tier = "low"
    elif data_quality < 0.75 and tier == "high":
        tier = "medium"
    return tier

def _weighted_score(factors, weights):
    return sum(v * weights.get(k, 1.0) for k, v in factors.items())

def _apply_data_quality_scaling(score, dq_score):
    return score * math.sqrt(max(dq_score, 0.01))

def _smooth_score_double_ema(current, pair, macro):
    prev_scores = macro.get("_prev_scores", {})
    prev = prev_scores.get(pair)
    if prev is None:
        return current
    smoothed = 0.8 * current + 0.2 * prev
    return round(smoothed, 3)

def _smooth_score(current, pair, macro):
    return _smooth_score_double_ema(current, pair, macro)

_FACTOR_KEYS = ("rate", "curve", "risk", "diff", "momentum", "real_yield",
                "growth", "usd", "liquidity")

_NEUTRAL_BIAS = {
    "score": 0.0, "confidence": "low", "regime": "unknown",
    "factors": {k: 0.0 for k in _FACTOR_KEYS},
    "weighted_factors": {k: 0.0 for k in _FACTOR_KEYS},
    "regime_confidence": 0.5,
    "signal_horizon": "medium",
}

def vol_adjust(score: float, pair_vol: float) -> float:
    return round(score / max(pair_vol, 0.5), 3)

def should_trade(score: float, macro: Dict) -> bool:
    if abs(score) < MACRO_SCORE_MIN:
        return False

    vix = macro.get("vix", 20)
    if vix < MACRO_VIX_DEAD:
        return False

    if vix > MACRO_VIX_RISK_OFF and abs(score) < MACRO_SCORE_STRONG:
        return False

    dq = macro.get("data_quality", {}).get("score", 1.0)
    if dq < MACRO_DQ_MIN:
        return False

    if _momentum_blind(macro):
        return False

    return True


def _gold_state(macro: Dict, key: str) -> Dict[str, Any]:
    val = macro.get("gold", {}).get(key, {}) if isinstance(macro.get("gold"), dict) else {}
    return val if isinstance(val, dict) else {}


def _gold_change_factor(macro: Dict, key: str, scale: float, sign: float = 1.0, max_abs: float = 1.5) -> float:
    state = _gold_state(macro, key)
    if not state.get("valid"):
        return 0.0
    try:
        change = float(state.get("change", 0.0))
    except (TypeError, ValueError):
        return 0.0
    if abs(scale) < 1e-12:
        return 0.0
    return round(max(min(sign * change / scale, max_abs), -max_abs), 3)


def _gold_real_yield_momentum_factor(macro: Dict) -> float:
    return _gold_change_factor(macro, "real_yield_momentum", scale=0.12, sign=-1.0)


def _gold_broad_usd_factor(macro: Dict) -> float:
    return _gold_change_factor(macro, "broad_usd", scale=0.35, sign=-1.0)


def _gold_inflation_expectations_factor(macro: Dict) -> float:
    return _gold_change_factor(macro, "inflation_expectations", scale=0.08, sign=1.0)


def _gold_treasury_vol_factor(macro: Dict) -> float:
    state = _gold_state(macro, "treasury_vol")
    if not state.get("valid"):
        return 0.0
    try:
        current = float(state.get("current", 0.0))
        change = float(state.get("change", 0.0))
    except (TypeError, ValueError):
        return 0.0
    level_impulse = max(0.0, (current - 110.0) / 50.0)
    change_impulse = change / 7.0
    return round(max(min(level_impulse + change_impulse, 1.5), -1.0), 3)


def _gold_vol_factor(macro: Dict) -> float:
    return round(0.5 * _gold_change_factor(macro, "gold_vol", scale=2.0, sign=1.0, max_abs=1.0), 3)


def _gold_energy_inflation_factor(macro: Dict) -> float:
    return _gold_change_factor(macro, "energy_inflation", scale=2.0, sign=1.0, max_abs=1.0)


def get_pair_bias(pair: str, macro: Dict) -> Dict:
    pair = _normalize_pair_code(pair)
    if pair not in ACTIVE_MAIN_PAIRS:
        result = dict(_NEUTRAL_BIAS)
        result["regime"] = simple_regime(macro) if isinstance(macro, dict) and macro else "unknown"
        result["enabled"] = False
        result["main_pair_only"] = True
        return result

    try:
        _valid = (macro and isinstance(macro.get("us"), dict)
                  and "rate" in macro["us"] and "yield_spread" in macro["us"]
                  and isinstance(macro.get("eu"), dict) and "rate" in macro["eu"]
                  and isinstance(macro.get("gb"), dict) and "rate" in macro["gb"]
                  and "vix" in macro and "diff" in macro)
    except Exception:
        _valid = False
    if not _valid:
        return dict(_NEUTRAL_BIAS)

    vix_valid = macro.get("vix_valid", True)
    dq = macro.get("data_quality", {}).get("score", 1.0)
    regime = simple_regime(macro)
    w = dict(FACTOR_WEIGHTS)
    _vix_for_growth = macro.get("vix", VIX_NEUTRAL_FALLBACK)
    w["growth"] = 0.0 if _vix_for_growth > 24 else 0.05
    curve = _curve_factor(macro)
    risk_fx = _risk_factor_fx(macro)
    mom = _get_momentum(pair, macro)
    if _momentum_blind(macro):
        log.debug(f"get_pair_bias[{pair}]: momentum blind -- momentum=0.0, fundamentals still scored")
    usd_f = _usd_factor_clean(macro)
    rh = macro.get("rate_history", {})
    pmi_d = macro.get("pmi_deltas", {})
    spx_f = _spx_trend_factor(macro)

    us_rate, eu_rate = macro["us"]["rate"], macro["eu"]["rate"]
    gb_rate = macro["gb"]["rate"]

    if pair == "xauusd":
        pmi_valid = macro.get("pmi_valid", True)
        gwm = 1.0 if pmi_valid else 0.3
        w_g = dict(w)
        w_g["rate"] = 0.0
        w_g["growth"] = round(w_g["growth"] * gwm, 4)
        w_g["liquidity"] = w_g.get("liquidity", FACTOR_WEIGHTS["liquidity"])
        # Gold-only macro extensions. These do not affect FX pairs.
        w_g.update({
            "real_yield": 1.25,
            "real_yield_momentum": 1.00,
            "broad_usd": 1.10,
            "inflation_expectations": 0.70,
            "treasury_vol": 0.45,
            "gold_vol": 0.25,
            "energy_inflation": 0.35,
            "usd": 0.85,
            "momentum": 1.00,
        })

        _usd_surprise = macro.get("surprises", {}).get("USD", 0.0)
        surprise_adj = round(max(min(-_usd_surprise * 0.5, 0.5), -0.5), 3)
        factors = {
            "rate": 0.0,
            "curve": round(-curve, 3),
            "risk": round(_risk_concave(_risk_factor_gold_raw(macro)) + 0.2 * spx_f * -1, 3),
            "diff": 0.0,
            "momentum": mom,
            "real_yield": round(_real_yield_factor(macro), 3),
            "real_yield_momentum": _gold_real_yield_momentum_factor(macro),
            "growth": round(-_growth_factor(macro) + surprise_adj, 3),
            "usd": round(-_rate_factor_usd(macro), 3),
            "broad_usd": _gold_broad_usd_factor(macro),
            "inflation_expectations": _gold_inflation_expectations_factor(macro),
            "treasury_vol": _gold_treasury_vol_factor(macro),
            "gold_vol": _gold_vol_factor(macro),
            "energy_inflation": _gold_energy_inflation_factor(macro),
            "liquidity": _liquidity_factor_gold(macro),
        }
        vix_now = macro.get("vix", VIX_NEUTRAL_FALLBACK)
        factors = _stress_deactivate(factors, vix_now)
        factors = _apply_vix_scaling(factors, vix=vix_now)
        macro.setdefault("_factor_history", {}).setdefault(pair, {})
        raw_f = {k: round(v, 3) for k, v in factors.items()}
        weighted = {k: round(v * w_g.get(k, 1.0), 3) for k, v in raw_f.items()}
        raw_score = _clamp(_tanh_score(_weighted_score(raw_f, w_g)))
        score = _apply_data_quality_scaling(raw_score, dq)
        score = _smooth_score(score, pair, macro)
        score = _conviction(score)
        score = round(_clamp(score), 3)
        macro.setdefault("_prev_scores", {})[pair] = score
        _reg_conf  = compute_regime_confidence(macro)
        _sig_horiz = compute_signal_horizon(macro)
        _price_ctx = None
        if _PRICE_OK:
            try:
                _price_ctx = _fetch_price_context(pair, score, ohlc=_pivot_ohlc_from_macro(pair, macro))
                if _price_ctx:
                    _cm = _price_ctx.get("conviction_mult", 1.0)
                    score = round(_clamp(score * _cm), 3)
            except Exception as _pex:
                log.debug(f"get_pair_bias[{pair}]: price_context fetch failed: {_pex}")
        return {
            "score": score, "regime": regime,
            "confidence": bias_confidence(score, vix_valid, pmi_valid, dq),
            "factors": raw_f, "weighted_factors": weighted,
            "regime_confidence": _reg_conf,
            "signal_horizon":    _sig_horiz,
            "price_context":     _price_ctx,
        }

    def _fx_growth(region_key, base_ccy):
        bp = macro[region_key].get("pmi", PMI_NEUTRAL)
        bv = macro[region_key].get("pmi_valid", False)
        up = macro.get("pmi", PMI_NEUTRAL)
        uv = macro.get("pmi_valid", False)
        both = bv and uv; either = bv or uv
        bd = pmi_d.get({"eu": "eu", "gb": "gb"}.get(region_key, "us"), 0.0)
        ud = pmi_d.get("us", 0.0)
        growth = (_growth_factor_regional(bp, up, bd, ud) if both
                  else _growth_factor_fx_usd(macro))
        growth += _surprise_adjustment(macro, base_ccy, "USD")
        gwm = 1.0 if both else (0.5 if either else 0.3)
        return growth, gwm, both

    if pair == "eurusd":
        gf, gwm, ppv = _fx_growth("eu", "EUR")
        eu_country_score = 0.0
        if macro.get("eu", {}).get("country_pmi_valid"):
            eu_country_score = float(macro.get("eu", {}).get("country_pmi_score", 0.0) or 0.0)
            gf = round(gf + EU_COUNTRY_PMI_GROWTH_WEIGHT * eu_country_score, 3)
            ppv = True
        w_fx = dict(w); w_fx["growth"] = round(w_fx["growth"] * gwm, 4)
        diff = macro["diff"].get("eurusd", 0.0)
        risk_sign = -risk_fx if diff < 0 else -0.5 * risk_fx
        factors = {
            "rate": _rate_factor_spread(eu_rate, us_rate, rh.get("eu", []), rh.get("us", [])),
            "curve": round(curve * 0.05, 3),
            "risk": round(risk_sign + 0.15 * spx_f, 3),                              
            "diff": 0.0, "momentum": mom, "real_yield": 0.0,
            "growth": round(gf, 3), "usd": round(-usd_f, 3), "liquidity": 0.0,
        }
    elif pair == "gbpusd":
        gf, gwm, ppv = _fx_growth("gb", "GBP")
        w_fx = dict(w); w_fx["growth"] = round(w_fx["growth"] * gwm, 4)
        diff = macro["diff"].get("gbpusd", 0.0)
        risk_sign = -risk_fx if diff < 0 else -0.5 * risk_fx
        factors = {
            "rate": _rate_factor_spread(gb_rate, us_rate, rh.get("gb", []), rh.get("us", [])),
            "curve": round(curve * 0.05, 3),
            "risk": round(risk_sign + 0.15 * spx_f, 3),
            "diff": 0.0, "momentum": mom, "real_yield": 0.0,
            "growth": round(gf, 3), "usd": round(-usd_f, 3), "liquidity": 0.0,
        }
    else:
        factors = {k: 0.0 for k in _FACTOR_KEYS}
        w_fx = dict(w); ppv = False

    vix_now = macro.get("vix", VIX_NEUTRAL_FALLBACK)
    factors = _stress_deactivate(factors, vix_now)
    factors = _apply_vix_scaling(factors, vix=vix_now)
    macro.setdefault("_factor_history", {}).setdefault(pair, {})
    raw_f = {k: round(v, 3) for k, v in factors.items()}
    weighted = {k: round(v * w_fx.get(k, 1.0), 3) for k, v in raw_f.items()}
    raw_score = _clamp(_tanh_score(_weighted_score(raw_f, w_fx)))
    score = _apply_data_quality_scaling(raw_score, dq)
    score = _smooth_score(score, pair, macro)
    score = _conviction(score)
    score = round(_clamp(score), 3)
    macro.setdefault("_prev_scores", {})[pair] = score

    _reg_conf  = compute_regime_confidence(macro)
    _sig_horiz = compute_signal_horizon(macro)
    _price_ctx = None
    if _PRICE_OK:
        try:
            _price_ctx = _fetch_price_context(pair, score, ohlc=_pivot_ohlc_from_macro(pair, macro))
            if _price_ctx:
                _cm = _price_ctx.get("conviction_mult", 1.0)
                score = round(_clamp(score * _cm), 3)
        except Exception as _pex:
            log.debug(f"get_pair_bias[{pair}]: price_context fetch failed: {_pex}")
    return {
        "score": score, "regime": regime,
        "confidence": bias_confidence(score, vix_valid, ppv, dq),
        "factors": raw_f, "weighted_factors": weighted,
        "regime_confidence": _reg_conf,
        "signal_horizon":    _sig_horiz,
        "price_context":     _price_ctx,
    }


# ---------------------------------------------------------------------------
# Pair-bias attachment patch for dashboard MARKET REGIME
# ---------------------------------------------------------------------------
def _attach_pair_biases_to_macro(macro: Dict) -> Dict:
    """Ensure macro has scored pair_biases/biases for dashboard aggregate scoring."""
    if not isinstance(macro, dict) or not macro:
        return macro

    existing = macro.get("pair_biases") or macro.get("biases") or {}
    biases: Dict[str, Dict[str, Any]] = dict(existing) if isinstance(existing, dict) else {}

    def _has_numeric_score(value: Any) -> bool:
        if isinstance(value, (int, float)):
            return math.isfinite(float(value))
        if isinstance(value, dict):
            for key in ("score", "macro_score", "bias_score", "value"):
                try:
                    return math.isfinite(float(value.get(key)))
                except (TypeError, ValueError):
                    continue
        return False

    # Fill missing or scoreless entries. Previously an existing empty/stub
    # pair_biases dict caused an early return, leaving MARKET REGIME at 0.00.
    for pair in sorted(ACTIVE_MAIN_PAIRS):
        current = biases.get(pair)
        if _has_numeric_score(current):
            continue
        try:
            bias = get_pair_bias(pair, macro)
            if isinstance(bias, dict):
                biases[pair] = bias
        except Exception as exc:
            log.debug("attach_pair_biases[%s]: %s", pair, exc)

    if biases:
        macro["pair_biases"] = biases
        macro["biases"] = biases
        macro.setdefault("price_context", {})
        for pair, bias in biases.items():
            ctx = bias.get("price_context") if isinstance(bias, dict) else None
            if isinstance(ctx, dict):
                macro["price_context"][pair] = ctx
    return macro

_original_build_macro = build_macro

def build_macro() -> Dict:
    return _attach_pair_biases_to_macro(_original_build_macro())

_original_get_macro_context = get_macro_context

def get_macro_context() -> Dict:
    macro = _attach_pair_biases_to_macro(_original_get_macro_context())
    try:
        if isinstance(macro, dict) and macro.get("pair_biases"):
            _save_cache(macro)
    except Exception as exc:
        log.debug("get_macro_context: pair_bias cache save skipped: %s", exc)
    return macro


# ===========================================================================
# REPAIR PATCH: Rich technical context for macro_components.json
# ===========================================================================
# Purpose:
#   macro_components.json was poor because macro.py returned useful H1 EMA data,
#   while pivot.py generated useful D1 pivot/RSI data in pivot_levels.json, but
#   both were not merged into one stable structure.  This patch enriches macro
#   output and get_pair_bias() with:
#     - D1 RSI from pivot_levels.json
#     - D1 pivots / price_state / nearest level from pivot_levels.json
#     - H1 RSI + H1 EMA 20/50 from momentum.py's ema_20_50_state
#     - D1 EMA 20/50 from price_indicators, price_history, ema_state.json, or
#       ema_closes.json when those files are available
# ===========================================================================

MACRO_TECHNICAL_CONTEXT_ENABLED: bool = os.environ.get(
    "MACRO_TECHNICAL_CONTEXT", "1"
).strip().lower() not in ("0", "false", "no", "off")

MACRO_TECHNICAL_LOOKBACK: int = max(60, int(float(os.environ.get("MACRO_TECHNICAL_LOOKBACK", "160") or 160)))


def _mc_pair(pair: Any) -> str:
    try:
        return _macro_pair_key(pair)
    except Exception:
        return re.sub(r"[^a-z0-9]", "", str(pair or "").lower())


def _mc_float(value: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        if value is None or value == "":
            return default
        x = float(value)
        return x if math.isfinite(x) else default
    except (TypeError, ValueError):
        return default


def _mc_round_price(pair: str, value: Any) -> Optional[float]:
    x = _mc_float(value)
    if x is None:
        return None
    pair_l = _mc_pair(pair)
    digits = 2 if "xau" in pair_l else 6
    return round(x, digits)


def _mc_public_dir() -> Path:
    try:
        return Path(COMPONENT_CACHE_FILE).expanduser().resolve().parent
    except Exception:
        return Path(".").resolve()


def _mc_json_load(path: Path) -> Dict[str, Any]:
    try:
        if path.exists():
            with path.open(encoding="utf-8") as f:
                data = json.load(f)
            return data if isinstance(data, dict) else {}
    except Exception as exc:
        log.debug("macro technical: load failed %s: %s", path, exc)
    return {}


def _mc_load_first(env_name: str, default_name: str) -> Dict[str, Any]:
    candidates: List[Path] = []
    env_value = os.environ.get(env_name, "").strip()
    if env_value:
        candidates.append(Path(env_value).expanduser())
    base = _mc_public_dir()
    candidates.extend([base / default_name, Path(default_name), Path("public") / default_name])
    seen = set()
    for path in candidates:
        key = str(path)
        if key in seen:
            continue
        seen.add(key)
        data = _mc_json_load(path)
        if data:
            data["_source_file"] = str(path)
            return data
    return {}


def _mc_ema(values: List[float], period: int) -> Optional[float]:
    clean = [float(v) for v in values if _mc_float(v) is not None]
    if len(clean) < period:
        return None
    k = 2.0 / (period + 1.0)
    ema = sum(clean[:period]) / period
    for price in clean[period:]:
        ema = price * k + ema * (1.0 - k)
    return ema


def _mc_closes_from_rows(rows: Any) -> List[float]:
    if not isinstance(rows, list):
        return []
    closes: List[float] = []
    for row in rows:
        if isinstance(row, dict):
            raw = row.get("close", row.get("Close", row.get("c", row.get("last_close"))))
        else:
            raw = row
        val = _mc_float(raw)
        if val is not None:
            closes.append(val)
    return closes[-MACRO_TECHNICAL_LOOKBACK:]


def _mc_rsi_state(value: Optional[float]) -> str:
    if value is None:
        return "unavailable"
    if value >= 70:
        return "overbought"
    if value <= 30:
        return "oversold"
    return "neutral"


def _mc_rsi_bias(value: Optional[float]) -> str:
    if value is None:
        return "unavailable"
    if value >= 55:
        return "bullish"
    if value <= 45:
        return "bearish"
    return "neutral"


def _mc_build_ema_state(pair: str, timeframe: str, ema20: float, ema50: float, price: Optional[float], source: str, extra: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    pair_l = _mc_pair(pair)
    def side(v: Optional[float], ref: float) -> Optional[str]:
        if v is None:
            return None
        return "above" if v > ref else "below" if v < ref else "at"
    fast_vs_slow = "above" if ema20 > ema50 else "below" if ema20 < ema50 else "at"
    trend_bias = "bullish_bias" if fast_vs_slow == "above" else "bearish_bias" if fast_vs_slow == "below" else "neutral_bias"
    out = {
        "pair": pair_l,
        "timeframe": timeframe,
        "fast_period": 20,
        "slow_period": 50,
        "ema20": _mc_round_price(pair_l, ema20),
        "ema50": _mc_round_price(pair_l, ema50),
        "ema_fast": _mc_round_price(pair_l, ema20),
        "ema_slow": _mc_round_price(pair_l, ema50),
        "fast_ema": _mc_round_price(pair_l, ema20),
        "slow_ema": _mc_round_price(pair_l, ema50),
        "ema20_vs_ema50": fast_vs_slow,
        "trend_bias": trend_bias,
        "price": _mc_round_price(pair_l, price) if price is not None else None,
        "current_vs_ema20": side(price, ema20),
        "current_vs_ema50": side(price, ema50),
        "price_vs_fast": side(price, ema20),
        "price_vs_slow": side(price, ema50),
        "cross": "none",
        "ema_cross_signal": "none",
        "warming_up": False,
        "ok": True,
        "source": source,
        "history_source": source,
    }
    if extra:
        out.update({k: v for k, v in extra.items() if v is not None})
    return out


def _mc_external_ema(pair: str, data: Dict[str, Any], timeframe: str) -> Dict[str, Any]:
    pair_l = _mc_pair(pair)
    containers = [data.get("results"), data.get("pairs"), data.get("ema_20_50_state"), data]
    for container in containers:
        if not isinstance(container, dict):
            continue
        raw = container.get(pair_l) or container.get(pair_l.upper())
        if not isinstance(raw, dict):
            continue
        raw_tf = str(raw.get("timeframe", timeframe)).upper()
        if timeframe.upper() == "D1" and raw_tf not in ("D1", "DAILY", "1D"):
            continue
        ema20 = _mc_float(raw.get("ema20", raw.get("ema_20", raw.get("ema_fast", raw.get("fast_ema")))))
        ema50 = _mc_float(raw.get("ema50", raw.get("ema_50", raw.get("ema_slow", raw.get("slow_ema")))))
        price = _mc_float(raw.get("price", raw.get("current_price", raw.get("last_close", raw.get("close")))))
        if ema20 is not None and ema50 is not None:
            return _mc_build_ema_state(pair_l, timeframe, ema20, ema50, price, str(raw.get("source", data.get("_source_file", "external_ema"))), raw)
    return {}


def _mc_daily_ema_from_available_history(pair: str, macro: Dict[str, Any], current_price: Any = None) -> Dict[str, Any]:
    pair_l = _mc_pair(pair)

    # 1) scraper/macro price_indicators, if available.
    indicators = (macro.get("price_indicators", {}) or {}).get(pair_l, {})
    if isinstance(indicators, dict):
        ema20 = _mc_float(indicators.get("ema20", indicators.get("ema_20")))
        ema50 = _mc_float(indicators.get("ema50", indicators.get("ema_50")))
        if ema20 is not None and ema50 is not None:
            price = _mc_float(current_price, _mc_float(indicators.get("close", indicators.get("last_close"))))
            return _mc_build_ema_state(pair_l, "D1", ema20, ema50, price, "macro_price_indicators", indicators)

    # 2) macro price_history, if populated by scraper.
    closes = _mc_closes_from_rows((macro.get("price_history", {}) or {}).get(pair_l))
    if len(closes) >= 50:
        ema20, ema50 = _mc_ema(closes, 20), _mc_ema(closes, 50)
        if ema20 is not None and ema50 is not None:
            return _mc_build_ema_state(pair_l, "D1", ema20, ema50, _mc_float(current_price, closes[-1]), "macro_price_history", {"bars_available": len(closes), "last_close": closes[-1]})

    # 3) ema.py close cache, if available.
    ema_closes = _mc_load_first("EMA_CLOSES_FILE", "ema_closes.json")
    for key in (pair_l.upper(), pair_l):
        rows = ema_closes.get(key)
        closes = _mc_closes_from_rows(rows)
        if len(closes) >= 50:
            ema20, ema50 = _mc_ema(closes, 20), _mc_ema(closes, 50)
            if ema20 is not None and ema50 is not None:
                return _mc_build_ema_state(pair_l, "D1", ema20, ema50, _mc_float(current_price, closes[-1]), "ema_closes", {"bars_available": len(closes), "last_close": closes[-1]})

    return {"pair": pair_l, "timeframe": "D1", "ok": False, "source": "unavailable", "reason": "no_daily_ema_history"}


def _mc_load_pivot_levels() -> Dict[str, Any]:
    return _mc_load_first("PIVOT_LEVELS_FILE", "pivot_levels.json")


def _mc_pair_technical_context(pair: str, macro: Dict[str, Any], pivot_pair: Dict[str, Any], ema_state_file: Dict[str, Any]) -> Dict[str, Any]:
    pair_l = _mc_pair(pair)
    h1_state = (macro.get("ema_20_50_state", {}) or {}).get(pair_l, {})
    if isinstance(h1_state, dict) and h1_state:
        try:
            h1_state = _normalise_ema_state_for_macro(pair_l, h1_state)
        except Exception:
            h1_state = dict(h1_state)
        h1_state["timeframe"] = "H1"
    else:
        h1_state = {"pair": pair_l, "timeframe": "H1", "ok": False, "source": "unavailable"}

    current_price = (
        _mc_float(pivot_pair.get("price_used"))
        or _mc_float((macro.get("price_current", {}) or {}).get(pair_l))
        or _mc_float((macro.get("_price_current", {}) or {}).get(pair_l))
        or _mc_float(h1_state.get("price"))
    )

    d1_ema = _mc_external_ema(pair_l, ema_state_file, "D1") or _mc_daily_ema_from_available_history(pair_l, macro, current_price)

    d1_rsi = _mc_float(pivot_pair.get("rsi"))
    d1_rsi_state = {
        "pair": pair_l,
        "timeframe": "D1",
        "period": int(_mc_float(pivot_pair.get("rsi_period"), 14) or 14),
        "value": round(d1_rsi, 2) if d1_rsi is not None else None,
        "state": str(pivot_pair.get("rsi_state") or _mc_rsi_state(d1_rsi)),
        "bias": str(pivot_pair.get("rsi_bias") or _mc_rsi_bias(d1_rsi)),
        "alignment": str(pivot_pair.get("rsi_alignment") or "unavailable"),
        "source": str(pivot_pair.get("rsi_source") or pivot_pair.get("ohlc_source") or "pivot_levels"),
        "ok": d1_rsi is not None,
    }

    h1_rsi = _mc_float(h1_state.get("h1_rsi", h1_state.get("h1_rsi14", h1_state.get("rsi14")))) if isinstance(h1_state, dict) else None
    h1_rsi_state = {
        "pair": pair_l,
        "timeframe": "H1",
        "period": int(_mc_float(h1_state.get("h1_rsi_period"), 14) or 14) if isinstance(h1_state, dict) else 14,
        "value": round(h1_rsi, 2) if h1_rsi is not None else None,
        "state": str(h1_state.get("h1_rsi_label") or _mc_rsi_state(h1_rsi)) if isinstance(h1_state, dict) else "unavailable",
        "bias": str(h1_state.get("h1_rsi_signal") or _mc_rsi_bias(h1_rsi)) if isinstance(h1_state, dict) else "unavailable",
        "source": str(h1_state.get("source") or h1_state.get("history_source") or "momentum") if isinstance(h1_state, dict) else "unavailable",
        "ok": h1_rsi is not None,
    }

    pivots = {k: _mc_round_price(pair_l, pivot_pair.get(k)) for k in ("PP", "R1", "R2", "R3", "S1", "S2", "S3") if _mc_float(pivot_pair.get(k)) is not None}
    daily = {
        "timeframe": "D1",
        "ohlc": {
            "date": pivot_pair.get("ohlc_date"),
            "source": pivot_pair.get("ohlc_source"),
            "close": _mc_round_price(pair_l, pivot_pair.get("close")),
            "range": _mc_round_price(pair_l, pivot_pair.get("range")),
        },
        "pivots": pivots,
        "price_state": pivot_pair.get("price_state"),
        "nearest_level": pivot_pair.get("nearest_level"),
        "rsi14": d1_rsi_state,
        "ema_20_50": d1_ema,
    }
    h1 = {"timeframe": "H1", "rsi14": h1_rsi_state, "ema_20_50": h1_state}

    votes = []
    for item in (daily["ema_20_50"], h1["ema_20_50"]):
        if isinstance(item, dict):
            votes.append(str(item.get("trend_bias", "")))
    votes += [d1_rsi_state.get("bias", ""), h1_rsi_state.get("bias", "")]
    bull = sum(1 for v in votes if "bull" in str(v))
    bear = sum(1 for v in votes if "bear" in str(v))
    summary_bias = "bullish" if bull > bear else "bearish" if bear > bull else "neutral"

    return {
        "pair": pair_l,
        "current_price": _mc_round_price(pair_l, current_price),
        "daily": daily,
        "h1": h1,
        "summary": {
            "bias": summary_bias,
            "bullish_votes": bull,
            "bearish_votes": bear,
            "d1_rsi_ok": bool(d1_rsi_state.get("ok")),
            "h1_rsi_ok": bool(h1_rsi_state.get("ok")),
            "d1_ema_ok": bool(isinstance(d1_ema, dict) and d1_ema.get("ok")),
            "h1_ema_ok": bool(isinstance(h1_state, dict) and h1_state.get("ok")),
        },
        "sources": {
            "pivot": pivot_pair.get("ohlc_source") or "pivot_levels",
            "h1": h1_rsi_state.get("source"),
            "d1_ema": d1_ema.get("source") if isinstance(d1_ema, dict) else "unavailable",
        },
    }


def _mc_enrich_macro(macro: Dict[str, Any]) -> Dict[str, Any]:
    if not MACRO_TECHNICAL_CONTEXT_ENABLED or not isinstance(macro, dict):
        return macro
    pivot_levels = _mc_load_pivot_levels()
    pivot_pairs = pivot_levels.get("pairs", {}) if isinstance(pivot_levels, dict) else {}
    if not isinstance(pivot_pairs, dict):
        pivot_pairs = {}
    ema_state_file = _mc_load_first("EMA_STATE_FILE", "ema_state.json")

    technical: Dict[str, Any] = {}
    daily_ema: Dict[str, Any] = {}
    h1_ema: Dict[str, Any] = {}
    daily_rsi: Dict[str, Any] = {}
    h1_rsi: Dict[str, Any] = {}

    for pair in sorted(ACTIVE_MAIN_PAIRS):
        pair_l = _mc_pair(pair)
        pivot_pair = pivot_pairs.get(pair_l) or pivot_pairs.get(pair_l.upper()) or {}
        if not isinstance(pivot_pair, dict):
            pivot_pair = {}
        ctx = _mc_pair_technical_context(pair_l, macro, pivot_pair, ema_state_file)
        technical[pair_l] = ctx
        daily_ema[pair_l] = ctx["daily"]["ema_20_50"]
        h1_ema[pair_l] = ctx["h1"]["ema_20_50"]
        daily_rsi[pair_l] = ctx["daily"]["rsi14"]
        h1_rsi[pair_l] = ctx["h1"]["rsi14"]
        if pivot_pair:
            macro.setdefault("price_context", {})[pair_l] = pivot_pair
            if isinstance(macro.get("pair_biases", {}).get(pair_l), dict):
                macro["pair_biases"][pair_l]["price_context"] = pivot_pair
                macro["pair_biases"][pair_l]["technical_summary"] = ctx["summary"]

    macro["technical_context"] = technical
    macro["daily_ema_20_50_state"] = daily_ema
    macro["h1_ema_20_50_state"] = h1_ema
    macro["daily_rsi_state"] = daily_rsi
    macro["h1_rsi_state"] = h1_rsi
    if h1_ema:
        macro["ema_20_50_state"] = h1_ema
    macro.setdefault("sources", {})
    macro["sources"]["technical_context"] = "pivot_levels+momentum+ema_state+ema_closes"
    macro["sources"]["pivot_levels_file"] = pivot_levels.get("_source_file", "unavailable") if isinstance(pivot_levels, dict) else "unavailable"
    macro["sources"]["ema_state_file"] = ema_state_file.get("_source_file", "unavailable") if isinstance(ema_state_file, dict) else "unavailable"
    return macro


# Wrap get_pair_bias so scraper's per-pair call gets authoritative pivot.py data,
# not stale/unavailable price_context from macro_components.json.
_mc_original_get_pair_bias = get_pair_bias

def get_pair_bias(pair: str, macro: Dict) -> Dict:
    result = _mc_original_get_pair_bias(pair, macro)
    pair_l = _mc_pair(pair)
    try:
        enriched_macro = _mc_enrich_macro(macro if isinstance(macro, dict) else {})
        tech = (enriched_macro.get("technical_context", {}) or {}).get(pair_l, {})
        pivot_ctx = (enriched_macro.get("price_context", {}) or {}).get(pair_l)
        if isinstance(result, dict):
            if isinstance(pivot_ctx, dict) and pivot_ctx:
                result["price_context"] = pivot_ctx
            if isinstance(tech, dict) and tech:
                result["technical_summary"] = tech.get("summary", {})
                result["technical_context"] = tech
    except Exception as exc:
        log.debug("macro technical get_pair_bias wrapper skipped for %s: %s", pair_l, exc)
    return result


# Wrap build/get context as final layer. This is intentionally appended after
# existing macro.py wrappers, so cache hits and live builds are both enriched.
_mc_previous_build_macro = build_macro

def build_macro() -> Dict:
    return _mc_enrich_macro(_mc_previous_build_macro())

_mc_previous_get_macro_context = get_macro_context

def get_macro_context() -> Dict:
    macro = _mc_enrich_macro(_mc_previous_get_macro_context())
    try:
        if isinstance(macro, dict):
            _save_cache(macro)
    except Exception as exc:
        log.debug("macro technical cache save skipped: %s", exc)
    return macro

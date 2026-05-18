#!/usr/bin/env python3

__version__ = "3.8"

import os
import re
import sys
import json
import time
import copy
import random
import hashlib
import logging
import threading
import requests
try:
    import feedparser                
    _FEEDPARSER_OK = True
except ImportError:
    _FEEDPARSER_OK = False

    class _FeedParserStub:
        FeedParserDict = dict

        @staticmethod
        def parse(_text):
            class _EmptyFeed:
                entries = []
                bozo = 1
                bozo_exception = RuntimeError("feedparser not installed")
            return _EmptyFeed()

    feedparser = _FeedParserStub()                
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timedelta, timezone
from calendar import timegm

from pathlib import Path
from urllib.parse import urlparse
import csv as _csv
from io import StringIO as _StringIO

from bs4 import BeautifulSoup
from dateutil import parser as dateparser, tz as _dateutil_tz
try:
    from feedgen.feed import FeedGenerator
    _FEEDGEN_OK = True
except ImportError:
    _FEEDGEN_OK = False

    class FeedGenerator:                
        def __init__(self, *args, **kwargs):
            raise RuntimeError("feedgen not installed")

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from logging.handlers import RotatingFileHandler

try:
    from dotenv import load_dotenv
    _env_path = Path(__file__).resolve().parent / ".env"
    load_dotenv(dotenv_path=_env_path, override=False)
except ImportError:
    pass                                                                          
from concurrent.futures import Future, ThreadPoolExecutor, as_completed

UTC = timezone.utc
_ET = _dateutil_tz.gettz("America/New_York") or UTC

try:
    from filelock import FileLock as _FileLock
    _FILELOCK_OK = True
except ImportError:
    _FILELOCK_OK = False

    class _FileLock:                          
        def __init__(self, path, timeout=10):
            pass

        def __enter__(self):
            logging.getLogger(__name__).warning(
                "filelock not installed -- calendar cache unprotected; "
                "run: pip install filelock"
            )
            return self

        def __exit__(self, *_):
            pass

try:
    import cloudscraper as _cloudscraper_mod
    _CLOUDSCRAPER_OK = True
except ImportError:
    _CLOUDSCRAPER_OK = False


try:
    # Canonical macro.py updated in this workflow. It prefers momentum.py and
    # pivot.py internally, so scraper follows the same stack.
    from macro import (
        configure as _configure_macro,
        get_macro_context,
        get_pair_bias as _get_pair_bias,
        compute_regime_confidence as _compute_regime_confidence,
        compute_signal_horizon as _compute_signal_horizon,
    )
    _MACRO_OK = True
    _MACRO_MODULE = "macro"
except ImportError:
    try:
        from macro_rewritten_full import (
            configure as _configure_macro,
            get_macro_context,
            get_pair_bias as _get_pair_bias,
            compute_regime_confidence as _compute_regime_confidence,
            compute_signal_horizon as _compute_signal_horizon,
        )
        _MACRO_OK = True
        _MACRO_MODULE = "macro_rewritten_full"
    except ImportError:
        try:
            from macro_rewritten import (
                configure as _configure_macro,
                get_macro_context,
                get_pair_bias as _get_pair_bias,
                compute_regime_confidence as _compute_regime_confidence,
                compute_signal_horizon as _compute_signal_horizon,
            )
            _MACRO_OK = True
            _MACRO_MODULE = "macro_rewritten"
        except ImportError:
            _MACRO_OK = False
            _MACRO_MODULE = "unavailable"
            def _configure_macro(output_dir: str) -> None:
                pass
            def get_macro_context() -> Dict:
                return {}
            def _get_pair_bias(pair: str, macro: Dict) -> Dict:
                return {"score": 0.0, "confidence": "low", "factors": {}}
            def _compute_regime_confidence(macro: Dict) -> float:
                return 0.5
            def _compute_signal_horizon(macro: Dict) -> str:
                return "medium"

try:
    import signal_confirm as _signal_confirm
    _SIGNAL_CONFIRM_OK = True
except ImportError:
    _SIGNAL_CONFIRM_OK = False

try:
    from pivot import fetch_price_structure as _fetch_price_structure  # type: ignore
    _PIVOT_OK = True
except ImportError:
    _PIVOT_OK = False

    def _fetch_price_structure() -> Tuple[Dict, str]:  # type: ignore[misc]
        return {}, "pivot_unavailable"

try:
    from ema import (
        run_analysis                  as _ema_run_analysis,
        save_json                     as _ema_save_json,
        trim_snapshot_file            as _ema_trim_snapshots,
        ensure_required_output_files  as _ema_ensure_files,
        get_api_quota_summary         as _ema_quota_summary,
        iso_z                         as _ema_iso_z,
        now_utc                       as _ema_now_utc,
        STATE_FILE                    as _EMA_STATE_FILE,
        SUPPORTED_PAIRS               as _EMA_PAIRS,
    )
    from dataclasses import asdict as _asdict
    _EMA_OK = True
except ImportError:
    _EMA_OK = False

DEFAULT_CONFIG: Dict = {
    "log_file":         os.environ.get("SCRAPER_LOG_FILE", str(Path(__file__).resolve().parent / "scraper.log")),
    "output_dir":       os.environ.get("SCRAPER_OUTPUT_DIR", str(Path(__file__).resolve().parent / "public_html")),
    "output_file_mode": 0o644,
    "output_dir_mode":  0o755,
    "max_items":        5,
    "max_items_fx":     10,
    "http_retries":     3,
    "http_backoff":     1,
    "http_timeout":     30,
    "seen_hashes_max":  5000,
    "log_retention_days": 1,
    "log_max_bytes":      5_000_000,
    "log_backup_count":   3,
    "headline_max_age_days": 3,
    "ff_throttle_delay":  10.0,
    "calendar_cache_ttl": 1800,

    "enabled_sources": ["inv", "sec", "kit", "mining", "tegold", "cal", "stooq"],

    "investinglive_feeds": {
        "news":   "https://investinglive.com/feed/news/",
        "ta":     "https://investinglive.com/feed/technicalanalysis/",
        "orders": "https://investinglive.com/feed/forexorders/",
        "cb":     "https://investinglive.com/feed/centralbank/",
    },

    "secondary_feeds": [
        ["ForexLive-news",    "ForexLive",    "https://www.forexlive.com/feed/news"],
        ["ForexLive-ta",      "ForexLive",    "https://www.forexlive.com/feed/technicalanalysis"],
        ["ForexCrunch",       "ForexCrunch",  "https://feeds.feedburner.com/ForexCrunch"],
        ["ActionForex",       "ActionForex",  "https://www.actionforex.com/feed"],
        ["FXStreet-news",     "FXStreet",     "https://www.fxstreet.com/rss/news"],
        ["FXStreet-analysis", "FXStreet",     "https://www.fxstreet.com/rss/analysis"],
        ["InvestingCom",      "Investing.com","https://investing.com/rss/news_1.rss"],
        ["DailyForex",        "DailyForex",   "https://www.dailyforex.com/rss/forexnews.xml"],
    ],

    "kitco_url": "https://www.kitco.com/news/",
    # Kitco is frequently slow.  12 s is generous for a news page; the stale
    # cache kicks in immediately on timeout rather than after 3 × 30 s.
    "kitco_timeout": 12,

    "mining_url":        "https://www.mining.com/wp-json/wp/v2/posts",
    "mining_per_page":   10,
    "mining_categories": "gold",
    "mining_endpoints": [
        "https://www.mining.com/wp-json/wp/v2/posts?categories_slug=gold",
        "https://www.mining.com/wp-json/wp/v2/posts?tags_slug=gold",
        "https://www.mining.com/wp-json/wp/v2/posts?categories_slug=commodity-gold",
    ],
    "mining_fields": "id,title,link,date,excerpt",
    "mining_gold_rss": "https://www.mining.com/commodity/gold/feed/",
    "mining_gold_archive": "https://www.mining.com/commodity/gold/",
    "tradingeconomics_gold_url": "https://tradingeconomics.com/commodity/gold",
    "tradingeconomics_gold_max_items": 8,

    "pair_entities": {
        "eurusd": {
            "required": [["eur", "euro", "ecb", "european"],
                         ["usd", "dollar", "fed", "greenback", "dxy"]],
            "boost":    ["eur/usd", "eurusd"],
        },
        "gbpusd": {
            "required": [["gbp", "pound", "sterling", "cable", "boe"],
                         ["usd", "dollar", "fed", "greenback", "dxy"]],
            "boost":    ["gbp/usd", "gbpusd"],
        },
        "usdjpy": {
            "required": [["usd", "dollar", "fed", "greenback", "dxy"],
                         ["jpy", "yen", "boj", "japan"]],
            "boost":    ["usd/jpy", "usdjpy"],
        },
        "xauusd": {
            "required": [["gold", "xau", "bullion", "precious metal", "spot gold"],
                         []],
            "boost":    ["xau/usd", "xauusd", "gold price"],
        },
    },

    "gold_keywords": [
        "gold", "xauusd", "xau/usd", "precious metal",
        "bullion", "spot gold", "gold prices",
    ],

    "fx_pairs": ["eurusd", "gbpusd", "usdjpy"],

    "ff_calendar_urls": [
        "https://nfs.faireconomy.media/ff_calendar_thisweek.xml",
    ],
    "ff_min_impact":        "Low",
    "ff_wanted_currencies": ["EUR", "GBP", "USD", "JPY"],
    "ff_currency_pairs": {
        "EUR": ["eurusd"],
        "GBP": ["gbpusd"],
        "USD": ["eurusd", "gbpusd", "usdjpy", "xauusd"],
        "JPY": ["usdjpy"],
    },
    "ff_max_events": 20,

    "rates_attribution": "https://stooq.com",

    "rates_cache_ttl":          900,                                          

    "rates_change_thresholds": {
                  
        "EUR/USD": 0.00005,
        "GBP/USD": 0.00005,
        "USD/JPY": 0.005,
        "GBP/JPY": 0.005,
        "AUD/USD": 0.00005,
        "USD/CAD": 0.00005,
        "USD/CHF": 0.00005,
        "XAU/USD": 0.05,
                        
        "USD/MYR": 0.0005,
        "USD/SGD": 0.0001,
                     
        "SGD/MYR": 0.0005,
        "EUR/MYR": 0.0005,
        "GBP/MYR": 0.0005,
        "JPY/MYR": 0.000005,
        "CNY/MYR": 0.0005,
        "THB/MYR": 0.0001,
        "IDR/MYR": 0.000001,
        "AUD/MYR": 0.0005,
        "NZD/MYR": 0.0005,
                
        "BTC/USD": 10.0,
        "_default": 0.00005,
    },

    "rates_staleness_brackets": [
        [60,   "fresh"],
        [180,  "warm"],
        [300,  "aging"],
        [600,  "stale"],
        [3600, "old"],
    ],

    "rates_trading_windows": [
        [700,  1200, 60],                                           
        [1200, 1700, 60],                                           
        [1700, 2000, 120],                                            
        [2000, 2300, 180],                                            
        [0,    300,  120],                                            
    ],
    "rates_cache_ttl_offpeak": 300,                                    
    "price_component_cache_file": ".price_component_cache.json",
}

def _deep_merge(base: Dict, override: Dict) -> Dict:
    result = copy.deepcopy(base)
    for k, v in override.items():
        if k in result and isinstance(result[k], dict) and isinstance(v, dict):
            result[k] = _deep_merge(result[k], v)
        else:
            result[k] = copy.deepcopy(v)
    return result

def load_config() -> Dict:
    cfg = copy.deepcopy(DEFAULT_CONFIG)
    path = os.environ.get("SCRAPER_CONFIG", "")
    if not path:
        return cfg
    try:
        with open(path, encoding="utf-8") as f:
            overrides = json.load(f)
        for k, v in overrides.items():
            if k not in DEFAULT_CONFIG:
                logging.getLogger(__name__).warning(
                    f"Config: unknown key {k!r} ignored -- check for typos"
                )
                continue
            if isinstance(cfg[k], dict) and isinstance(v, dict):
                cfg[k] = _deep_merge(cfg[k], v)
            else:
                cfg[k] = v
        logging.getLogger(__name__).info(f"Config loaded from {path}")
    except Exception as exc:
        logging.getLogger(__name__).warning(
            f"Cannot load config {path}: {exc} -- using defaults"
        )
    return cfg

def _deployment_context() -> Dict[str, str]:
    if os.environ.get("GITHUB_ACTIONS", "").lower() == "true":
        return {
            "platform": "GitHub Actions",
            "run_id": os.environ.get("GITHUB_RUN_ID", "local"),
            "trigger": os.environ.get("GITHUB_EVENT_NAME", "workflow_dispatch"),
            "workflow": os.environ.get("GITHUB_WORKFLOW", "scraper"),
        }
    if os.environ.get("CI_JOB_ID") or os.environ.get("CI_PIPELINE_SOURCE"):
        return {
            "platform": "CI",
            "run_id": os.environ.get("CI_JOB_ID", "local"),
            "trigger": os.environ.get("CI_PIPELINE_SOURCE", "pipeline"),
            "workflow": os.environ.get("CI_PROJECT_PATH", "scraper"),
        }
    return {
        "platform": "alwaysdata",
        "run_id": "local",
        "trigger": "local",
        "workflow": "scraper",
    }


def _set_perms(path: str, mode: int) -> None:
    """Set file permissions, silently ignoring errors (e.g. on read-only FS)."""
    try:
        os.chmod(path, mode)
    except OSError:
        pass


def _hash_item(title: str, link: str) -> str:
    key = title.strip().lower() + "|" + link.split("?")[0].rstrip("/")
    return hashlib.sha1(key.encode()).hexdigest()[:16]

def load_seen_hashes(path: str) -> List[str]:
    try:
        with open(path, encoding="utf-8") as f:
            return list(json.load(f).get("hashes", []))
    except FileNotFoundError:
        return []
    except Exception as exc:
        logging.getLogger(__name__).warning(f"seen_hashes load: {exc}")
        return []

def save_seen_hashes(path: str, hashes: List[str], max_entries: int,
                     log: logging.Logger) -> None:
    tmp = path + ".tmp"
    try:
        lst = hashes[-max_entries:] if len(hashes) > max_entries else hashes
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump({"hashes": lst, "count": len(lst)}, f)
        os.replace(tmp, path)
        _set_perms(path, 0o644)
    except Exception as exc:
        log.warning(f"seen_hashes save: {exc}")
        try:
            os.remove(tmp)
        except OSError:
            pass

def mark_new_and_update(
    items: List[Dict], seen: List[str]
) -> Tuple[List[Dict], List[str]]:
    seen_set = set(seen)
    updated  = list(seen)
    for item in items:
        h = _hash_item(item.get("title", ""), item.get("link", ""))
        item["is_new"] = h not in seen_set
        if h not in seen_set:
            seen_set.add(h)
            updated.append(h)
    return items, updated

_RE_TOKEN_CLEAN = re.compile(r"[^a-z0-9]+")


def _pregroup_by_pair(items: List[Dict], pairs: List[str],
                      cfg: Dict) -> Dict[str, List[Dict]]:
    pair_map: Dict[str, List[Dict]] = {p: [] for p in pairs}
    for item in items:
        clean_title = f" {_RE_TOKEN_CLEAN.sub(' ', item.get('title', '').lower())} "
        for pair in pairs:
            if _entity_match_precleaned(clean_title, pair, cfg):
                pair_map[pair].append(dict(item))
    return pair_map

def _entity_match_precleaned(clean_title: str,
                             pair: str, cfg: Dict) -> bool:
    entity_cfg = cfg.get("pair_entities", {}).get(pair)
    if not entity_cfg:
        return False

    for token in entity_cfg.get("boost", []):
        needle = _RE_TOKEN_CLEAN.sub(" ", token.lower()).strip()
        if not needle:
            continue
        if f" {needle} " in clean_title or needle in clean_title:
            return True

    for token_list in entity_cfg.get("required", [[], []]):
        if token_list and not any(
            (lambda n: f" {n} " in clean_title or n in clean_title)(
                _RE_TOKEN_CLEAN.sub(" ", token.lower()).strip()
            )
            for token in token_list
        ):
            return False
    return True


_POS_WORDS = [
    "rise", "rises", "rising", "rally", "rallies", "gain", "gains",
    "surge", "surges", "jump", "jumps", "climb", "climbs", "soar",
    "bullish", "upbeat", "recovery", "rebound", "beat", "beats",
    "strong", "strength", "boost", "boosts", "high", "highest",
    "rate cut", "easing", "dovish", "stimulus", "breakout", "upgrade",
]

_NEG_WORDS = [
    "fall", "falls", "falling", "drop", "drops", "decline", "declines",
    "slump", "slumps", "plunge", "plunges", "crash", "crashes",
    "bearish", "downbeat", "weak", "weakness", "selloff", "sell-off",
    "miss", "misses", "low", "lowest", "loss", "losses", "crisis",
    "recession", "contraction", "downgrade", "fears", "tumble",
    "rate hike", "tightening", "hawkish",
]

def _score_to_sentiment(score: float) -> str:
    if score > 0.1:
        return "bullish"
    if score < -0.1:
        return "bearish"
    return "neutral"

def score_sentiment(title: str) -> Dict:
    t   = " " + title.lower() + " "
    pos = len({w for w in _POS_WORDS if w in t})
    neg = len({w for w in _NEG_WORDS if w in t})
    net = pos - neg
    if net > 0:
        return {"sentiment": "bullish", "score": net}
    if net < 0:
        return {"sentiment": "bearish", "score": net}
    return {"sentiment": "neutral", "score": 0}

_SYMBOL_TOKENS: Dict[str, List[str]] = {
    "EUR": ["eur", "euro", "ecb", "european central bank"],
    "GBP": ["gbp", "pound", "sterling", "cable", "boe", "bank of england"],
    "USD": ["usd", "dollar", "fed", "fomc", "federal reserve", "greenback", "dxy"],
    "JPY": ["jpy", "yen", "boj", "bank of japan"],
    "XAU": ["gold", "xau", "bullion", "precious metal"],
    "AUD": ["aud", "aussie", "rba", "reserve bank of australia"],
    "CAD": ["cad", "loonie", "boc", "bank of canada"],
    "CHF": ["chf", "franc", "snb", "swiss national bank"],
}

def tag_symbols(title: str) -> List[str]:
    t = " " + title.lower() + " "
    return [sym for sym, tokens in _SYMBOL_TOKENS.items()
            if any(tok in t for tok in tokens)]

_HIGH_IMPACT_KW = [
    "rate decision", "rate cut", "rate hike", "fomc", "fomc statement",
    "interest rate", "emergency", "intervention", "quantitative easing",
    "quantitative tightening", "nfp", "non-farm payroll", "cpi",
    "boj decision", "ecb decision", "boe decision", "boc decision",
]
_MED_IMPACT_KW = [
    "gdp", "pmi", "retail sales", "trade balance", "payroll",
    "consumer confidence", "unemployment", "jobs report", "ism",
    "manufacturing data", "housing data",
]

def infer_impact(title: str) -> str:
    t = " " + title.lower() + " "
    if any(kw in t for kw in _HIGH_IMPACT_KW):
        return "high"
    if any(kw in t for kw in _MED_IMPACT_KW):
        return "med"
    return "low"

def combine_signal(sentiment: float, macro_bias: Dict) -> Dict:
    macro_score = macro_bias.get("score", 0.0)
    confidence  = macro_bias.get("confidence", "low")

    w_macro = min(abs(macro_score) / 3.0, 0.70)
    w_sent  = 1.0 - w_macro

    sent_clamped = max(min(float(sentiment), 3.0), -3.0)
    fused = round(sent_clamped * w_sent + macro_score * w_macro, 3)

    return {
        "fused_score": fused,
        "confidence":  confidence,
        "components": {
            "sentiment":    sentiment,
            "sent_clamped": sent_clamped,
            "macro_score":  macro_score,
            "weights": {"macro": round(w_macro, 3), "sent": round(w_sent, 3)},
        },
    }

def _apply_macro_to_feed(headlines: List[Dict], pair: str,
                         macro: Dict,
                         log: Optional[logging.Logger] = None
                         ) -> Tuple[List[Dict], Dict]:
    def _null_bias() -> Dict:
        return {"score": 0.0, "confidence": "low", "factors": {}}

    if not macro:
        return headlines, _null_bias()

    try:
        bd = _get_pair_bias(pair, macro)
    except Exception as exc:
        if log:
            log.warning(f"_get_pair_bias({pair}): {exc} -- using neutral bias")
        bd = _null_bias()

    for h in headlines:
        h["pair_macro_bias"]   = bd["score"]
        h["macro_conf"]        = bd["confidence"]
        h["macro_factors"]     = dict(bd.get("factors", {}))
        fusion                 = combine_signal(h.get("sent_score", 0), bd)
        h["fused_score"]       = fusion["fused_score"]
        h["fused_sentiment"]   = _score_to_sentiment(fusion["fused_score"])
        h["fusion_components"] = fusion["components"]
        h.pop("final_score", None)

    return headlines, bd

@dataclass
class SourceStatus:
    name:       str
    success:    bool  = False
    items:      int   = 0
    error:      str   = ""
    duration:   float = 0.0
    latency_ms: int   = 0

@dataclass
class RunStatus:
    scrape_time: str = ""
    sources: Dict[str, SourceStatus] = field(default_factory=dict)

    def record(self, name: str, success: bool, items: int = 0,
               error: str = "", duration: float = 0.0) -> None:
        self.sources[name] = SourceStatus(
            name=name, success=success, items=items,
            error=error, duration=duration,
            latency_ms=round(duration * 1000),
        )

    def to_dict(self) -> Dict:
        return {
            "scrape_time":    self.scrape_time,
            "total_sources":  len(self.sources),
            "failed_sources": sum(1 for v in self.sources.values() if not v.success),
            "sources": {
                k: {
                    "success":    v.success,
                    "items":      v.items,
                    "error":      v.error or None,
                    "duration":   round(v.duration, 2),
                    "latency_ms": v.latency_ms,
                }
                for k, v in self.sources.items()
            },
        }

def _trim_log_file(log_file: str, max_age_days: int) -> str:
    if not os.path.isfile(log_file):
        return ""
    cutoff    = datetime.now(UTC) - timedelta(days=max_age_days)
    ts_re     = re.compile(r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z)")
    kept = dropped = 0
    keep_line = True
    tmp = log_file + ".trim.tmp"
    try:
        with (open(log_file, "r", encoding="utf-8", errors="replace") as src,
              open(tmp, "w", encoding="utf-8") as dst):
            for line in src:
                m = ts_re.match(line)
                if m:
                    try:
                        line_dt = datetime.strptime(
                            m.group(1), "%Y-%m-%dT%H:%M:%SZ"
                        ).replace(tzinfo=UTC)
                        keep_line = line_dt >= cutoff
                    except ValueError:
                        keep_line = True
                if keep_line:
                    dst.write(line); kept += 1
                else:
                    dropped += 1
        os.replace(tmp, log_file)
        try:
            os.chmod(log_file, 0o644)
        except OSError:
            pass
        return (f"Log trim: removed {dropped} line(s) older than {max_age_days}d "
                f"({kept} kept)" if dropped
                else f"Log trim: no lines older than {max_age_days}d")
    except Exception as exc:
        try:
            os.remove(tmp)
        except OSError:
            pass
        return f"Log trim failed: {exc}"

def setup_logging(log_file: str, log_retention_days: int = 0,
                  cfg: Optional[Dict] = None) -> logging.Logger:
    log_dir = os.path.dirname(log_file)
    if log_dir:
        os.makedirs(log_dir, exist_ok=True)

    trim_msg = ""
    use_rotating = cfg is not None and "log_max_bytes" in cfg

    if not use_rotating and log_retention_days > 0:
        trim_msg = _trim_log_file(log_file, log_retention_days)

    logger = logging.getLogger(__name__)
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s",
                            datefmt="%Y-%m-%dT%H:%M:%SZ")

    stdout_h = logging.StreamHandler(sys.stdout)
    stdout_h.setFormatter(fmt)
    logger.addHandler(stdout_h)

    try:
        if use_rotating:
            file_h = RotatingFileHandler(
                log_file,
                maxBytes=cfg.get("log_max_bytes", 5_000_000),
                backupCount=cfg.get("log_backup_count", 3),
                encoding="utf-8",
            )
        else:
            file_h = logging.FileHandler(log_file, encoding="utf-8")
        file_h.setFormatter(fmt)
        logger.addHandler(file_h)
    except OSError as exc:
        logger.warning(f"Cannot open log file {log_file}: {exc} -- stdout only")

    if trim_msg:
        (logger.warning if "failed" in trim_msg else logger.info)(trim_msg)

    return logger

RSS_ACCEPT  = ("application/rss+xml, application/atom+xml, "
               "application/xml;q=0.9, text/xml;q=0.8")
HTML_ACCEPT = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
JSON_ACCEPT = "application/json, */*;q=0.8"
_DEFAULT_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/146.0.0.0 Safari/537.36 Edg/146.0.3856.62"
)
USER_AGENT  = os.environ.get("SCRAPER_UA", _DEFAULT_UA)

_EDGE_HINTS: Dict[str, str] = {
    "Sec-Ch-Ua":          '"Microsoft Edge";v="146", "Chromium";v="146", "Not_A Brand";v="99"',
    "Sec-Ch-Ua-Mobile":   "?0",
    "Sec-Ch-Ua-Platform": '"Windows"',
}

def _make_session(accept: str, cfg: Dict,
                  retries: Optional[int] = None) -> requests.Session:
    retry_count = retries if retries is not None else cfg["http_retries"]
    retry = Retry(
        total=retry_count,
        backoff_factor=cfg["http_backoff"],
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=frozenset(["GET"]),
        respect_retry_after_header=True,
    )
    s = requests.Session()
    s.headers.update({
        "User-Agent":      USER_AGENT,
        "Accept":          accept,
        "Accept-Language": "en-US,en;q=0.9",
        **_EDGE_HINTS,
    })
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("http://",  adapter)
    s.mount("https://", adapter)
    return s

_THROTTLE_LOCK  = threading.Lock()
_THROTTLE_STATE: Dict[str, float] = {}

def _throttle(url: str, delay: float = 0.5) -> None:
    try:
        domain = urlparse(url).netloc
    except Exception:
        return

    with _THROTTLE_LOCK:
        now       = time.time()
        base_wait = max(delay - (now - _THROTTLE_STATE.get(domain, 0.0)), 0.0)
        jitter    = random.uniform(0.3, 1.0) if base_wait > 0 else 0.0
        total     = base_wait + jitter
        # Record the time at which this thread's window will EXPIRE so that
        # concurrent threads computing their own base_wait see the correct
        # "last access" value and don't slip through the throttle gate.
        _THROTTLE_STATE[domain] = now + total
    if total > 0:
        time.sleep(total)

_FF_429_BACKOFF: Tuple[int, ...] = (30, 60, 60)
_FF_5XX_BACKOFF: Tuple[int, ...] = (5, 15, 30)

def _fetch_with_429_retry(url: str, session: requests.Session,
                          cfg: Dict, log: logging.Logger,
                          max_retries: int = 3) -> requests.Response:
    last_status: int = 0
    for attempt in range(max_retries):
        resp = session.get(url, timeout=cfg["http_timeout"])
        last_status = resp.status_code
        if resp.status_code == 429:
            if attempt == max_retries - 1:
                break
            ra = resp.headers.get("Retry-After")
            if ra:
                try:
                    wait = min(int(ra), 120)
                except ValueError:
                    wait = _FF_429_BACKOFF[min(attempt, len(_FF_429_BACKOFF) - 1)]
            else:
                wait = _FF_429_BACKOFF[min(attempt, len(_FF_429_BACKOFF) - 1)]
            log.warning(f"429 from {url} -- backing off {wait}s "
                        f"(attempt {attempt+1}/{max_retries})")
            time.sleep(wait)
            continue
        if resp.status_code >= 500:
            if attempt < max_retries - 1:
                wait = _FF_5XX_BACKOFF[min(attempt, len(_FF_5XX_BACKOFF) - 1)]
                log.warning(f"{resp.status_code} from {url} -- retrying in {wait}s "
                            f"(attempt {attempt+1}/{max_retries})")
                time.sleep(wait)
                continue
            # Last attempt — raise a consistent RuntimeError (same as 429 path)
            # rather than letting raise_for_status() produce an HTTPError.
            break
        resp.raise_for_status()
        return resp
    raise RuntimeError(
        f"ForexFactory: HTTP {last_status} after {max_retries} retries for {url}"
    )

_CAL_CACHE_LOCK = threading.Lock()
_CAL_CACHE: Dict = {}

def _load_calendar_cache(ttl: int, log: logging.Logger) -> Optional[List[Dict]]:
    with _CAL_CACHE_LOCK:
        if not _CAL_CACHE:
            return None
        age = time.time() - _CAL_CACHE.get("fetched_at", 0)
        if age < ttl:
            log.info(f"FF: in-memory calendar cache ({int(age)}s old, TTL={ttl}s)")
            return _CAL_CACHE.get("events")
        log.info(f"FF: cache expired ({int(age)}s) -- fetching fresh")
        return None

def _save_calendar_cache(events: List[Dict], log: logging.Logger) -> None:
    with _CAL_CACHE_LOCK:
        _CAL_CACHE["events"]     = events
        _CAL_CACHE["fetched_at"] = time.time()
    log.info(f"FF: calendar cached in memory ({len(events)} events)")

def _save_calendar_cache_file(cache_path: str, events: List[Dict],
                              log: logging.Logger) -> None:
    lock = cache_path + ".lock"
    tmp  = cache_path + ".tmp"
    try:
        with _FileLock(lock, timeout=10):
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump({"fetched_at": time.time(), "events": events}, f)
            os.replace(tmp, cache_path)
        log.info(f"FF: calendar cached to disk ({len(events)} events)")
    except Exception as exc:
        log.warning(f"FF: disk cache write failed ({exc})")
        try:
            os.remove(tmp)
        except OSError:
            pass

def _load_calendar_cache_file(cache_path: str, ttl: int,
                              log: logging.Logger) -> Optional[List[Dict]]:
    lock = cache_path + ".lock"
    try:
        with _FileLock(lock, timeout=10):
            if not os.path.exists(cache_path):
                return None
            with open(cache_path, encoding="utf-8") as f:
                data = json.load(f)
            age = time.time() - data.get("fetched_at", 0)
            if age < ttl:
                log.info(f"FF: disk calendar cache ({int(age)}s old, TTL={ttl}s)")
                return data.get("events")
            log.info(f"FF: disk cache expired ({int(age)}s) -- fetching fresh")
        return None
    except Exception as exc:
        log.warning(f"FF: disk cache read failed ({exc}) -- fetching fresh")
        return None

def _load_stale_calendar_cache_file(cache_path: str,
                                    log: logging.Logger) -> Optional[List[Dict]]:
    lock = cache_path + ".lock"
    try:
        with _FileLock(lock, timeout=10):
            if not os.path.exists(cache_path):
                return None
            with open(cache_path, encoding="utf-8") as f:
                data = json.load(f)
        events = data.get("events")
        if events is not None:
            age = int(time.time() - data.get("fetched_at", 0))
            log.warning(f"FF: stale disk cache fallback ({age}s old, {len(events)} events)")
            return events
    except Exception as exc:
        log.warning(f"FF: stale disk cache read failed ({exc})")
    return None


def _is_bot_blocked(text: str) -> bool:
    head = text[:2000].lower()
    return any(m in head for m in [
        "<title>access denied", "<title>attention required",
        "<title>just a moment", "please enable javascript",
        "checking your browser",
    ])

def _is_xml(text: str) -> bool:
    snippet = text.lstrip()[:200].lower()
    return snippet.startswith("<?xml") or "<rss" in snippet or "<feed" in snippet

def _fetch_rss(url: str, cfg: Dict,
               session: Optional[requests.Session] = None
               ) -> Tuple[Optional[feedparser.FeedParserDict], str]:
    if not _FEEDPARSER_OK:
        return None, "feedparser not installed"
    _throttle(url)
    s = session or _make_session(RSS_ACCEPT, cfg)
    close_after = session is None
    try:
        resp = s.get(url, timeout=cfg["http_timeout"])
        resp.raise_for_status()
        if _is_bot_blocked(resp.text):
            return None, "bot-blocked"
        if not _is_xml(resp.text):
            return None, "returned HTML not XML"
        feed = feedparser.parse(resp.text)
        if feed.bozo and not feed.entries:
            return None, str(feed.bozo_exception)
        return feed, ""
    except Exception as exc:
        return None, str(exc)
    finally:
        if close_after:
            s.close()

def parse_date(raw: str) -> Optional[datetime]:
    try:
        dt = dateparser.parse(raw)
    except (ValueError, TypeError, OverflowError):
        return None
    if dt is None:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=UTC)

def parse_struct_time(st) -> Optional[datetime]:
    try:
        return datetime.fromtimestamp(timegm(st), tz=UTC)
    except Exception:
        return None

def make_headline(title: str, link: str, pub_date: Optional[datetime],
                  source: str, scrape_time: str = "", summary: str = "") -> Dict:
    sent    = score_sentiment(title)
    symbols = tag_symbols(title)
    impact  = infer_impact(title)

    pair_macro_bias:   float = 0.0
    macro_conf:        str   = "low"
    macro_factors:     Dict  = {}
    fused_score:       float = float(sent["score"])
    fusion_components: Dict  = {}

    return {
        "title":             title,
        "link":              link,
        "published_at":      pub_date,
        "scraped_at":        scrape_time,
        "source":            source,
        "summary":           summary,
        "sentiment":         sent["sentiment"],
        "sent_score":        sent["score"],
        "symbols":           symbols,
        "impact":            impact,
        "pair_macro_bias":   pair_macro_bias,
        "macro_conf":        macro_conf,
        "macro_factors":     macro_factors,
        "fused_score":       fused_score,
        "fused_sentiment":   _score_to_sentiment(fused_score),
        "fusion_components": fusion_components,
        "is_new":            True,
    }

def deduplicate(items: List[Dict]) -> List[Dict]:
    seen: set        = set()
    out:  List[Dict] = []
    for item in items:
        key = (
            (item.get("title") or "").strip().lower(),
            (item.get("link")  or "").split("?")[0].rstrip("/"),
        )
        if key not in seen:
            seen.add(key)
            out.append(item)
    return out

def sort_newest(items: List[Dict]) -> List[Dict]:
    return sorted(
        items,
        key=lambda h: h.get("published_at") or datetime.min.replace(tzinfo=UTC),
        reverse=True,
    )

def drop_old_headlines(items: List[Dict], max_age_days: int,
                       log: Optional[logging.Logger] = None) -> List[Dict]:
    if max_age_days <= 0:
        return items
    cutoff = datetime.now(UTC) - timedelta(days=max_age_days)
    kept, dropped = [], 0
    for h in items:
        pub = h.get("published_at")
        if pub is None:
            kept.append(h)
            continue
        if isinstance(pub, datetime) and pub.tzinfo is None:
            pub = pub.replace(tzinfo=UTC)
            h["published_at"] = pub
        if pub >= cutoff:
            kept.append(h)
        else:
            dropped += 1
    if dropped and log:
        log.info(f"drop_old_headlines: removed {dropped} item(s) older than {max_age_days}d")
    return kept

def parse_rss_feed(feed: feedparser.FeedParserDict,
                   scrape_time: str, source: str) -> List[Dict]:
    out: List[Dict] = []
    for entry in feed.entries:
        title = entry.get("title", "").strip()
        link  = entry.get("link",  "").strip()
        if not title or not link:
            continue

        pub_date = None
        if entry.get("published_parsed"):
            pub_date = parse_struct_time(entry["published_parsed"])
        elif entry.get("updated_parsed"):
            pub_date = parse_struct_time(entry["updated_parsed"])
        elif entry.get("published"):
            pub_date = parse_date(entry["published"])

        raw = entry.get("summary") or entry.get("description") or ""
        if not isinstance(raw, str):
            raw = ""
        summary = (
            BeautifulSoup(raw, "html.parser").get_text(strip=True)[:300]
            if raw and "<" in raw
            else raw[:300].strip()
        )
        out.append(make_headline(title, link, pub_date, source, scrape_time, summary))
    return out

def _load_stale_json(path: str, log: logging.Logger,
                     label: str) -> Optional[List[Dict]]:
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        items = data.get("items", [])
        if not items:
            age = int(time.time() - data.get("t", 0))
            log.warning(
                f"{label}: stale cache exists but is empty "
                f"({age}s old) -- no fallback available"
            )
            return None
        age = int(time.time() - data.get("t", 0))
        log.warning(f"{label}: stale cache fallback ({age}s old, {len(items)} items)")
        for h in items:
            if h.get("published_at") and isinstance(h["published_at"], str):
                h["published_at"] = parse_date(h["published_at"])
        return items
    except Exception:
        pass
    return None

def _save_stale_json(path: str, items: List[Dict]) -> None:
    tmp = path + ".tmp"
    try:
        serialised = []
        for h in items:
            hc = dict(h)
            if isinstance(hc.get("published_at"), datetime):
                hc["published_at"] = hc["published_at"].isoformat()
            serialised.append(hc)
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump({"t": time.time(), "items": serialised}, f)
        os.replace(tmp, path)
    except Exception:
        try:
            os.remove(tmp)
        except OSError:
            pass
def _extract_date_from_node(node, max_depth: int = 5) -> Optional[datetime]:
    current = node
    for _ in range(max_depth):
        if current is None:
            break
        try:
            time_tag = current.find("time")
            if time_tag:
                raw_dt = time_tag.get("datetime") or time_tag.get_text(strip=True)
                if raw_dt:
                    parsed = parse_date(raw_dt)
                    if parsed is not None:
                        return parsed
        except Exception:
            pass
        try:
            text = current.get_text(" ", strip=True)
            match = re.search(r"\b(20\d{2}-\d{2}-\d{2})\b", text)
            if match:
                parsed = parse_date(match.group(1))
                if parsed is not None:
                    return parsed
        except Exception:
            pass
        current = getattr(current, "parent", None)
    return None


class SourceHandler(ABC):
    name: str = "unknown"

    def __init__(self, cfg: Dict, status: RunStatus, log: logging.Logger) -> None:
        self.cfg    = cfg
        self.status = status
        self.log    = log
        self._session: Optional[requests.Session] = None

    def _get_session(self, accept: str = RSS_ACCEPT) -> requests.Session:
        if self._session is None:
            self._session = _make_session(accept, self.cfg)
            self._session_accept = accept
        elif getattr(self, "_session_accept", None) != accept:
            self._session.close()
            self._session = _make_session(accept, self.cfg)
            self._session_accept = accept
        return self._session

    @abstractmethod
    def fetch(self, scrape_time: str) -> List[Dict]: ...

    def run(self, scrape_time: str) -> List[Dict]:
        t0 = time.monotonic()
        try:
            items    = self.fetch(scrape_time)
            duration = time.monotonic() - t0
            self.status.record(self.name, success=True, items=len(items),
                               duration=duration)
            return items
        except Exception as exc:
            duration = time.monotonic() - t0
            self.log.error(f"[{self.name}] {exc}")
            self.status.record(self.name, success=False, error=str(exc),
                               duration=duration)
            return []
        finally:
            if self._session:
                self._session.close()
                self._session = None

class InvestingLiveHandler(SourceHandler):
    name = "investingLive"

    def fetch(self, scrape_time: str) -> List[Dict]:
        feeds = self.cfg["investinglive_feeds"]
        names = list(feeds.keys())
        urls  = list(feeds.values())

        def _fetch_one(url):
            return _fetch_rss(url, self.cfg)

        with ThreadPoolExecutor(max_workers=max(1, len(urls))) as ex:
            results = list(ex.map(_fetch_one, urls))

        headlines: List[Dict] = []
        all_ok = True
        for feed_name, (feed, err) in zip(names, results):
            if feed is None:
                self.log.warning(f"investingLive [{feed_name}]: {err}")
                all_ok = False
                continue
            items = parse_rss_feed(feed, scrape_time, source="investingLive")
            self.log.info(f"investingLive [{feed_name}]: {len(items)} articles")
            headlines.extend(items)

        headlines = deduplicate(headlines)
        self.log.info(f"investingLive total unique: {len(headlines)}")
        if not all_ok and not headlines:
            raise RuntimeError("all investingLive feeds failed or returned no articles")
        return headlines

class SecondaryFeedsHandler(SourceHandler):
    name = "secondary"

    def fetch(self, scrape_time: str) -> List[Dict]:
        rows = [tuple(r) for r in self.cfg["secondary_feeds"]]
        if not rows:
            return []

        valid_rows = []
        for r in rows:
            if len(r) < 3:
                self.log.warning(
                    f"Secondary: skipping malformed feed row {r!r} "
                    f"(expected [label, source, url], got {len(r)} field(s))"
                )
                continue
            valid_rows.append(r)

        if not valid_rows:
            return []

        def _fetch_one(r):
            return _fetch_rss(r[2], self.cfg)

        with ThreadPoolExecutor(max_workers=max(1, len(valid_rows))) as ex:
            results = list(ex.map(_fetch_one, valid_rows))

        headlines: List[Dict] = []
        for (label, source, _url), (feed, err) in zip(valid_rows, results):
            if feed is None:
                self.log.warning(f"Secondary [{label}]: {err}")
                continue
            items = parse_rss_feed(feed, scrape_time, source=source)
            self.log.info(f"Secondary [{label}]: {len(items)} articles")
            headlines.extend(items)

        headlines = deduplicate(headlines)
        self.log.info(f"Secondary total: {len(headlines)}")
        return headlines

class KitcoHandler(SourceHandler):
    name = "Kitco"

    def _stale_path(self) -> str:
        return os.path.join(self.cfg["output_dir"], ".kitco_stale.json")

    def fetch(self, scrape_time: str) -> List[Dict]:
        _throttle(self.cfg["kitco_url"])
        # Kitco is frequently slow.  A dedicated no-retry session with a short
        # timeout means we reach the stale-cache fallback in ~12 s instead of
        # blocking for 3 × 30 s while the retry adapter exhausts its attempts.
        kitco_timeout = self.cfg.get("kitco_timeout", 12)
        session = _make_session(HTML_ACCEPT, self.cfg, retries=0)
        try:
            resp = session.get(self.cfg["kitco_url"], timeout=kitco_timeout)
            resp.raise_for_status()
            if _is_bot_blocked(resp.text):
                cached = _load_stale_json(self._stale_path(), self.log, "Kitco")
                if cached is not None:
                    return cached
                raise RuntimeError("bot-blocked (no cache)")
            soup = BeautifulSoup(resp.text, "html.parser")
        except RuntimeError:
            raise
        except Exception as exc:
            cached = _load_stale_json(self._stale_path(), self.log, "Kitco")
            if cached is not None:
                return cached
            raise RuntimeError(f"fetch failed: {exc}") from exc
        finally:
            session.close()

        gold_kws   = self.cfg["gold_keywords"]
        limit      = self.cfg["max_items"]
        headlines: List[Dict] = []
        seen:      set        = set()

        for a in soup.select("a[href*='/news/article/']"):
            title = a.get_text(strip=True)
            link  = a.get("href", "")
            if not title or not link:
                continue
            if not any(kw in title.lower() for kw in gold_kws):
                continue
            if link.startswith("/"):
                link = "https://www.kitco.com" + link

            norm = link.split("?")[0].rstrip("/")
            if norm in seen:
                continue
            seen.add(norm)

            pub_date = None
            for ancestor in a.parents:
                if ancestor.name in ("body", "[document]", None):
                    break
                t = ancestor.find("time", recursive=False)
                if t and t.get("datetime"):
                    pub_date = parse_date(t["datetime"])
                    break
            if pub_date is None:
                m = re.search(r"/news/article/(\d{4}-\d{2}-\d{2})/", link)
                if m:
                    pub_date = parse_date(m.group(1))

            headlines.append(
                make_headline(title, link, pub_date, "Kitco", scrape_time)
            )
            if len(headlines) >= limit:
                break

        if headlines:
            self.log.info(f"Kitco: {len(headlines)} gold articles")
            _save_stale_json(self._stale_path(), headlines)
        else:
            self.log.warning("Kitco: no articles matched")
        return headlines


class MiningComHandler(SourceHandler):
    name = "Mining.com"

    def _stale_path(self) -> str:
        return os.path.join(self.cfg["output_dir"], ".mining_stale.json")

    @staticmethod
    def _is_expected_forbidden(err: object) -> bool:
        msg = str(err)
        return "403" in msg or "Forbidden" in msg

    def _log_fetch_issue(self, label: str, err: object) -> None:
        msg = str(err)
        if self._is_expected_forbidden(msg):
            self.log.info(f"{label}: 403/Forbidden - live fetch blocked, trying next fallback")
        else:
            self.log.warning(f"{label}: {msg}")

    def _load_stale_after_block(self, err: object):
        cached = _load_stale_json(self._stale_path(), self.log, "Mining.com")
        if cached is not None and self._is_expected_forbidden(err):
            self.log.info("Mining.com: using stale cache because live endpoints returned 403/Forbidden")
        return cached

    def fetch(self, scrape_time: str) -> List[Dict]:
        gold_kws = self.cfg["gold_keywords"]

        rss_items = self._fetch_gold_rss(scrape_time, gold_kws)
        if rss_items:
            return rss_items

        archive_items = self._fetch_gold_archive_html(scrape_time, gold_kws)
        if archive_items:
            return archive_items

        if _CLOUDSCRAPER_OK:
            return self._fetch_cloudscraper(scrape_time, gold_kws)
        return self._fetch_wp_rest(scrape_time, gold_kws)

    def _fetch_gold_rss(self, scrape_time: str, gold_kws: List[str]) -> List[Dict]:
        rss_url = self.cfg.get("mining_gold_rss", "https://www.mining.com/commodity/gold/feed/")
        feed, err = _fetch_rss(rss_url, self.cfg, session=self._get_session(RSS_ACCEPT))
        if feed is None:
            self._log_fetch_issue("Mining.com RSS", err)
            return []
        items = parse_rss_feed(feed, scrape_time, source="Mining.com")
        items = [h for h in items if any(kw in h.get("title", "").lower() for kw in gold_kws)]
        items = deduplicate(sort_newest(items))
        if items:
            self.log.info(f"Mining.com RSS: {len(items)} gold articles")
            _save_stale_json(self._stale_path(), items)
        else:
            self.log.info("Mining.com RSS: no gold items matched")
        return items

    def _fetch_gold_archive_html(self, scrape_time: str, gold_kws: List[str]) -> List[Dict]:
        url = self.cfg.get("mining_gold_archive", "https://www.mining.com/commodity/gold/")
        _throttle(url)
        session = self._get_session(HTML_ACCEPT)
        try:
            resp = session.get(url, timeout=self.cfg["http_timeout"])
            resp.raise_for_status()
            if _is_bot_blocked(resp.text):
                self.log.warning("Mining.com archive HTML: bot-blocked")
                return []
        except Exception as exc:
            self._log_fetch_issue("Mining.com archive HTML fetch failed", exc)
            return []
    
        soup = BeautifulSoup(resp.text, "html.parser")
        headlines: List[Dict] = []
        seen: set = set()
        limit = self.cfg.get("max_items_fx", 10)
    
        for anchor in soup.select("a[href]"):
            title = anchor.get_text(" ", strip=True)
            link = (anchor.get("href") or "").strip()
            if not title or not link:
                continue
            if "/web/" not in link and "/gold/" not in link and "/commodity/gold/" not in link:
                continue
            if not any(kw in title.lower() for kw in gold_kws):
                continue
            if link.startswith("/"):
                link = "https://www.mining.com" + link
    
            norm = link.split("?")[0].rstrip("/")
            if norm in seen:
                continue
            seen.add(norm)
    
            pub_date = _extract_date_from_node(anchor)
            headlines.append(make_headline(title, link, pub_date, "Mining.com", scrape_time))
            if len(headlines) >= limit:
                break
    
        headlines = deduplicate(sort_newest(headlines))
        if headlines:
            self.log.info(f"Mining.com archive HTML: {len(headlines)} gold articles")
            _save_stale_json(self._stale_path(), headlines)
        else:
            self.log.info("Mining.com archive HTML: no articles matched")
        return headlines

    def _fetch_cloudscraper(self, scrape_time: str, gold_kws: List[str]) -> List[Dict]:
        scraper = _cloudscraper_mod.create_scraper(
            browser={"browser": "chrome", "platform": "windows", "mobile": False}
        )
        fields = self.cfg.get("mining_fields", "id,title,link,date,excerpt")
        per_page = self.cfg.get("mining_per_page", 10)
        posts: List[Dict] = []
        last_err = ""
        endpoints = self.cfg.get("mining_endpoints", [
            f"{self.cfg.get('mining_url','https://www.mining.com/wp-json/wp/v2/posts')}"
            f"?categories_slug={self.cfg.get('mining_categories','gold')}"
        ])
        for base_url in endpoints:
            url = (f"{base_url}&per_page={per_page}"
                   f"&_fields={fields}&orderby=date&order=desc")
            param = base_url.split("?")[1][:45] if "?" in base_url else base_url
            try:
                resp = scraper.get(url, timeout=self.cfg["http_timeout"])
                resp.raise_for_status()
                data = resp.json()
                if isinstance(data, list) and data:
                    posts = data
                    self.log.info(f"Mining.com API fallback: {len(posts)} posts via [{param}]")
                    break
                self.log.warning(f"Mining.com API fallback: empty from [{param}]")
            except Exception as exc:
                last_err = str(exc)
                self._log_fetch_issue(f"Mining.com API fallback [{param}]", exc)
        if not posts:
            cached = self._load_stale_after_block(last_err)
            if cached is not None:
                return cached
            raise RuntimeError(last_err or "all endpoints empty")
        return self._parse_posts(posts, scrape_time, gold_kws)

    def _fetch_wp_rest(self, scrape_time: str, gold_kws: List[str]) -> List[Dict]:
        base_url = self.cfg.get("mining_url", "https://www.mining.com/wp-json/wp/v2/posts")
        per_page = self.cfg.get("mining_per_page", 10)
        cat_slug = self.cfg.get("mining_categories", "gold")
        url = (f"{base_url}?categories_slug={cat_slug}"
               f"&per_page={per_page}&_embed=false")
        _throttle(url)
        session = self._get_session(JSON_ACCEPT)
        try:
            resp = session.get(url, timeout=self.cfg["http_timeout"])
            resp.raise_for_status()
            posts = resp.json()
            if not isinstance(posts, list):
                raise RuntimeError(f"unexpected response type: {type(posts)}")
        except Exception as exc:
            cached = self._load_stale_after_block(exc)
            if cached is not None:
                return cached
            raise RuntimeError(f"Mining.com WP REST: {exc}") from exc
        self.log.info(f"Mining.com API fallback: {len(posts)} posts via [categories_slug={cat_slug}]")
        return self._parse_posts(posts, scrape_time, gold_kws)

    def _parse_posts(self, posts: List[Dict], scrape_time: str, gold_kws: List[str]) -> List[Dict]:
        headlines: List[Dict] = []
        for post in posts:
            raw_title = post.get("title", {})
            title = (raw_title.get("rendered", "") if isinstance(raw_title, dict) else str(raw_title)).strip()
            title = BeautifulSoup(title, "html.parser").get_text(strip=True)
            link = post.get("link", "").strip()
            if not title or not link:
                continue
            if not any(kw in title.lower() for kw in gold_kws):
                continue
            pub_date = None
            date_raw = post.get("date_gmt") or post.get("date") or ""
            if date_raw:
                pub_date = parse_date(date_raw)
            raw_exc = post.get("excerpt", {})
            excerpt_html = raw_exc.get("rendered", "") if isinstance(raw_exc, dict) else str(raw_exc)
            summary = BeautifulSoup(excerpt_html, "html.parser").get_text(strip=True)[:300]
            headlines.append(make_headline(title, link, pub_date, "Mining.com", scrape_time, summary))
        headlines = deduplicate(sort_newest(headlines))
        self.log.info(f"Mining.com API fallback: {len(headlines)} gold articles")
        if headlines:
            _save_stale_json(self._stale_path(), headlines)
        return headlines


class TradingEconomicsGoldHandler(SourceHandler):
    name = "TradingEconomics-Gold"

    def _stale_path(self) -> str:
        return os.path.join(self.cfg["output_dir"], ".tradingeconomics_gold_stale.json")

    def fetch(self, scrape_time: str) -> List[Dict]:
        url = self.cfg.get("tradingeconomics_gold_url", "https://tradingeconomics.com/commodity/gold")
        limit = int(self.cfg.get("tradingeconomics_gold_max_items", 8))
        _throttle(url, delay=1.0)
        session = self._get_session(HTML_ACCEPT)
        try:
            resp = session.get(url, timeout=self.cfg["http_timeout"])
            resp.raise_for_status()
            if _is_bot_blocked(resp.text):
                cached = _load_stale_json(self._stale_path(), self.log, "TradingEconomics-Gold")
                if cached is not None:
                    return cached
                raise RuntimeError("bot-blocked (no cache)")
        except Exception as exc:
            cached = _load_stale_json(self._stale_path(), self.log, "TradingEconomics-Gold")
            if cached is not None:
                return cached
            raise RuntimeError(f"fetch failed: {exc}") from exc
    
        soup = BeautifulSoup(resp.text, "html.parser")
        headlines: List[Dict] = []
        seen: set = set()
    
        for anchor in soup.select("a[href*='/commodity/gold/news/']"):
            title = anchor.get_text(" ", strip=True)
            link = (anchor.get("href") or "").strip()
            if not title or not link:
                continue
            if link.startswith("/"):
                link = "https://tradingeconomics.com" + link
    
            norm = link.split("?")[0].rstrip("/")
            if norm in seen:
                continue
            seen.add(norm)
    
            pub_date = _extract_date_from_node(anchor)
            headlines.append(make_headline(title, link, pub_date, "TradingEconomics", scrape_time))
            if len(headlines) >= limit:
                break
    
        headlines = deduplicate(sort_newest(headlines))
        if headlines:
            self.log.info(f"TradingEconomics-Gold: {len(headlines)} articles")
            _save_stale_json(self._stale_path(), headlines)
        else:
            self.log.warning("TradingEconomics-Gold: no matching gold article links found")
        return headlines
_STOOQ_PAIRS: List[Tuple[str, str, int]] = [
                                                     
              
    ("EUR/USD",  "eurusd",   5),
    ("GBP/USD",  "gbpusd",   5),
    ("USD/JPY",  "usdjpy",   3),
    ("AUD/USD",  "audusd",   5),
    ("USD/CAD",  "usdcad",   5),
    ("USD/CHF",  "usdchf",   5),
    ("XAU/USD",  "xauusd",   2),
                      
    ("USD/MYR",  "usdmyr",   4),
    ("USD/SGD",  "usdsgd",   4),
                 
    ("SGD/MYR",  "sgdmyr",   4),
    ("EUR/MYR",  "eurmyr",   4),
    ("GBP/MYR",  "gbpmyr",   4),
    ("JPY/MYR",  "jpymyr",   6),                 
    ("CNY/MYR",  "cnymyr",   4),
    ("THB/MYR",  "thbmyr",   4),
    ("IDR/MYR",  "idrmyr",   6),                    
    ("AUD/MYR",  "audmyr",   4),
    ("NZD/MYR",  "nzdmyr",   4),
            
    ("BTC/USD",  "btc.v",    2),                             
]

_PRICE_CACHE_KEY_PREFIX = "stooq_price_"

PRICE_COMPONENT_TTL: int = 900

_STOOQ_LATEST_TPL = "https://stooq.com/q/l/?s={symbol}&f=sd2t2ohlcv&h&e=csv"

# ---------------------------------------------------------------------------
# Stooq engine — yfinance-style via macro.StooqTicker
# ---------------------------------------------------------------------------
# Import the shared StooqTicker from macro.py so every module uses one session,
# handshake, retry adapter, and CSV parser.  Falls back to a shim when macro
# is not yet importable (e.g. during testing or circular-import bootstrap).
# ---------------------------------------------------------------------------

try:
    from macro import StooqTicker as _StooqTicker  # type: ignore
    _SCRAPER_STOOQ_ENGINE = "macro"
except ImportError:
    try:
        from macro_rewritten_full import StooqTicker as _StooqTicker
        _SCRAPER_STOOQ_ENGINE = "macro_rewritten_full"
    except ImportError:
        try:
            from macro_rewritten import StooqTicker as _StooqTicker  # type: ignore[no-redef]
            _SCRAPER_STOOQ_ENGINE = "macro_rewritten"
        except ImportError:
            _StooqTicker = None  # type: ignore[assignment]
            _SCRAPER_STOOQ_ENGINE = "shim"

import weakref as _weakref
_stooq_handshake_done: "_weakref.WeakSet[requests.Session]" = _weakref.WeakSet()

def _stooq_handshake(session: requests.Session) -> None:
    """One-time cookie handshake — used only by the shim fallback path."""
    if session in _stooq_handshake_done:
        return
    try:
        session.get("https://stooq.com", timeout=10)
    except Exception:
        pass
    _stooq_handshake_done.add(session)


def _prices_cache_path(cfg: Dict) -> str:
    filename = cfg.get("price_component_cache_file", ".price_component_cache.json")
    return os.path.join(cfg.get("output_dir", "."), filename)

def _load_prices_component_cache(cfg: Dict) -> Dict:
    try:
        with open(_prices_cache_path(cfg), encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def _save_prices_component_cache(cfg: Dict, cache: Dict,
                                  log: Optional[logging.Logger] = None) -> None:
    path = _prices_cache_path(cfg)
    tmp  = path + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(cache, f, indent=2)
        os.replace(tmp, path)
        try:
            os.chmod(path, 0o644)
        except OSError:
            pass
    except Exception as exc:
        if log:
            log.warning(f"prices component cache save: {exc}")
        try:
            os.remove(tmp)
        except OSError:
            pass

def _get_price_component(cache: Dict, label: str, ttl: int = PRICE_COMPONENT_TTL
                         ) -> Optional[Dict]:
    key   = _PRICE_CACHE_KEY_PREFIX + label.replace("/", "")
    entry = cache.get(key)
    if not entry:
        return None
    if time.time() - entry.get("t", 0) < ttl:
        return entry
    return None

def _get_stale_price_component(cache: Dict, label: str,
                                max_age: int = PRICE_COMPONENT_TTL * 4
                                ) -> Optional[Dict]:
    key   = _PRICE_CACHE_KEY_PREFIX + label.replace("/", "")
    entry = cache.get(key)
    if not entry:
        return None
    age = time.time() - entry.get("t", 0)
    if age < max_age:
        return entry
    return None


def _stooq_fetch_one(symbol: str, cfg: Dict,
                     session: Optional[requests.Session] = None
                     ) -> Optional[float]:
    """
    Fetch the latest close price for *symbol* from Stooq.

    yfinance-style lookup order (via macro.StooqTicker when available):
      1. StooqTicker.fast_info["last_price"]  — real-time /q/l/ endpoint
      2. StooqTicker.history(period="5d")[-1] — daily history fallback on N/D

    Falls back to the original direct-HTTP shim when macro.StooqTicker is
    not importable (e.g. circular-import bootstrap or test isolation).
    """
    if _StooqTicker is not None:
        try:
            t = _StooqTicker(symbol)
            price = t.fast_info.get("last_price")
            if price is not None:
                return float(price)
            bars = t.history(period="5d")
            if bars:
                return float(bars[-1]["Close"])
            return None
        except Exception:
            return None

    # ── Shim: macro not importable ─────────────────────────────────────────
    url = _STOOQ_LATEST_TPL.format(symbol=symbol)
    s = session
    close_after = False
    if s is None:
        s = requests.Session()
        s.headers.update({"User-Agent": USER_AGENT, "Accept": "text/csv,*/*"})
        close_after = True
    _stooq_handshake(s)
    try:
        resp = s.get(url, timeout=cfg["http_timeout"])
        if not resp.ok:
            return None
        rows = list(_csv.DictReader(_StringIO(resp.text)))
        if not rows:
            return None
        close_str = (rows[-1].get("Close") or "").strip()
        if not close_str or close_str == "N/D":
            return None
        return float(close_str)
    except Exception:
        return None
    finally:
        if close_after:
            s.close()

def _build_session_snapshot(price: float, when: Optional[datetime] = None) -> Dict[str, float]:
    dt = when or datetime.now(UTC)
    return {
        "open": price,
        "high": price,
        "low": price,
        "close": price,
        "_date": dt.date().isoformat(),
        "_synthetic": True,
    }



def _row_pick(row: Dict[str, object], *keys: str) -> object:
    for key in keys:
        if key in row and row[key] not in (None, ""):
            return row[key]
    return None


def _normalise_provider_date(raw: object) -> str:
    text = str(raw or "").strip()
    if len(text) == 8 and text.isdigit():
        return f"{text[:4]}-{text[4:6]}-{text[6:]}"
    return text[:10] if len(text) >= 10 else text


def _extract_ohlc_rows(data: object) -> List[Dict[str, object]]:
    if isinstance(data, list):
        return [r for r in data if isinstance(r, dict)]
    if isinstance(data, dict):
        for key in ("values", "Values", "Quotes", "quotes", "Data", "data", "Results", "results", "Items", "items"):
            rows = data.get(key)
            if isinstance(rows, list):
                return [r for r in rows if isinstance(r, dict)]
        if any(k in data for k in ("Open", "open", "High", "high", "Low", "low", "Close", "close")):
            return [data]
    return []


def _staleness_label(age_seconds: float, cfg: Dict) -> str:
    for bracket in cfg.get("rates_staleness_brackets", []):
        if len(bracket) >= 2 and age_seconds <= float(bracket[0]):
            return str(bracket[1])
    return "very-old"

def _staleness_pct(age_seconds: float, ttl: float) -> float:
    if ttl <= 0:
        return 0.0
    return round(min((age_seconds / ttl) * 100.0, 999.9), 1)

def _price_changed(label: str, new_price: float,
                   prev_prices: Dict[str, float], cfg: Dict) -> bool:
    thresholds = cfg.get("rates_change_thresholds", {})
    threshold  = float(thresholds.get(label, thresholds.get("_default", 0.00005)))
    prev       = prev_prices.get(label)
    if prev is None:
        return True
    return abs(new_price - prev) >= threshold

def _trading_window_ttl(cfg: Dict) -> float:
    now_utc  = datetime.now(UTC)
    hhmm     = now_utc.hour * 100 + now_utc.minute
    windows  = cfg.get("rates_trading_windows", [])
    offpeak  = float(cfg.get("rates_cache_ttl_offpeak", 300))
    for entry in windows:
        if len(entry) < 3:
            continue
        start, end, ttl = int(entry[0]), int(entry[1]), float(entry[2])
        if start <= end:
            if start <= hhmm < end:
                return ttl
        else:
            if hhmm >= start or hhmm < end:
                return ttl
    return offpeak
def _effective_ttl(cfg: Dict) -> float:
    raw = cfg.get("rates_cache_ttl", PRICE_COMPONENT_TTL)
    if raw == "auto":
        return max(_trading_window_ttl(cfg), 30.0)
    return max(float(raw), 30.0)

def _ohlc_plausible(ohlc: object, label: str = "", log: Optional[logging.Logger] = None) -> bool:
    """Return True only when the OHLC dict passes basic sanity checks.

    Stooq occasionally returns a near-zero Low for XAU/USD (e.g. Low~0.00022
    against a High~4610). That corrupted bar destroys pivot-level maths and
    causes the entire price context to fall back to unavailable.
    The 50 % H/L-range guard matches the rule in pivot.py _valid_ohlc so both
    modules apply a consistent quality gate.
    """
    if not isinstance(ohlc, dict):
        return False
    try:
        o = float(ohlc["open"]); h = float(ohlc["high"])
        l = float(ohlc["low"]);  c = float(ohlc["close"])
    except (KeyError, TypeError, ValueError):
        return False
    if h <= 0 or l <= 0:
        return False
    if h < l or not (l <= o <= h) or not (l <= c <= h):
        return False
    if (h - l) / h > 0.50:
        if log:
            log.warning(
                "StooqHandler: discarding implausible OHLC for %s "
                "(H=%.5f L=%.5f H/L-ratio=%.2f) — corrupted bar ignored",
                label or "?", h, l, (h - l) / h,
            )
        return False
    return True


class StooqHandler(SourceHandler):
    name = "Stooq"

    def __init__(self, cfg: Dict, status: RunStatus, log: logging.Logger) -> None:
        super().__init__(cfg, status, log)
                                                                                  
        self.rates_changed:      bool                         = False
        self.price_daily:        Dict[str, Dict[str, float]] = {}
        self.price_current:      Dict[str, float]            = {}
        self.price_session_ohlc: Dict[str, Dict[str, float]] = {}
        self.price_session_high: Dict[str, float]            = {}
        self.price_session_low:  Dict[str, float]            = {}
        self.price_date:         Dict[str, str]              = {}
        self.price_history:      Dict[str, List[Dict[str, float]]] = {}
        self.price_indicators:   Dict[str, Dict[str, object]] = {}

    def _load_prev_lookup(self) -> Dict[str, float]:
        rates_path = os.path.join(self.cfg["output_dir"], "rates.json")
        prev: Dict[str, float] = {}
        try:
            with open(rates_path, encoding="utf-8") as f:
                cached = json.load(f)
            for r in cached.get("rates", []):
                try:
                    prev[r["label"]] = float(r["rate"])
                except (KeyError, TypeError, ValueError):
                    pass
            if prev:
                self.log.info(f"Stooq: loaded {len(prev)} prev rates for chg_pct")
        except FileNotFoundError:
            self.log.info("Stooq: no cached rates.json -- chg_pct=0.0 this run")
        except Exception as exc:
            self.log.warning(f"Stooq: prev rates load: {exc}")
        return prev

    def _fetch_live(self, label: str, symbol: str,
                    session: requests.Session,
                    ) -> Tuple[Optional[float], str]:
        """Fetch live price from Stooq. OHLC is handled entirely by pivot.py."""
        _throttle(_STOOQ_LATEST_TPL.format(symbol=symbol), delay=0.3)
        price_f = _stooq_fetch_one(symbol, self.cfg, session)
        if price_f is not None and price_f > 0:
            return price_f, "Stooq"
        self.log.warning(f"Stooq: no data for {label} ({symbol})")
        return None, "none"

    def fetch(self, scrape_time: str) -> List[Dict]:
        self.rates_changed      = False
        self.price_daily        = {}   # not populated — pivot.py fetches its own OHLC
        self.price_current      = {}
        self.price_session_ohlc = {}
        self.price_session_high = {}
        self.price_session_low  = {}
        self.price_date         = {}
        self.price_history      = {}   # not populated — pivot.py fetches its own OHLC
        self.price_indicators   = {}   # not populated — pivot.py fetches its own OHLC

        prev_lookup  = self._load_prev_lookup()
        session      = self._get_session(JSON_ACCEPT)
        ttl          = _effective_ttl(self.cfg)
        now_wall     = time.time()
        comp_cache   = _load_prices_component_cache(self.cfg)
        window_label = (
            f"auto/{ttl:.0f}s" if self.cfg.get("rates_cache_ttl") == "auto"
            else f"fixed/{ttl:.0f}s"
        )
        self.log.info(
            f"Stooq: TTL={window_label} component_ttl={PRICE_COMPONENT_TTL}s "
            f"({datetime.now(UTC).strftime('%H:%M')} UTC)"
        )

        rates: List[Dict] = []
        cached_count = live_count = changed_count = 0
        cache_dirty = False

        for label, symbol, dec in _STOOQ_PAIRS:
            pair_key  = label.replace("/", "").lower()
            cache_key = _PRICE_CACHE_KEY_PREFIX + label.replace("/", "")
            cached_entry = _get_price_component(comp_cache, label, PRICE_COMPONENT_TTL)

            price_f: Optional[float] = None
            src   = "none"
            age_s = 0.0

            if cached_entry:
                price_f = cached_entry["price"]
                src     = cached_entry["source"]
                age_s   = now_wall - cached_entry["t"]
                cached_count += 1
                self.log.debug(
                    f"Stooq cache hit: {label} price={price_f} src={src} age={age_s:.0f}s"
                )
            else:
                price_f, src = self._fetch_live(label, symbol, session)
                age_s = 0.0
                if price_f is None:
                    stale = _get_stale_price_component(comp_cache, label)
                    if stale:
                        price_f = stale["price"]
                        src     = stale["source"] + "(stale)"
                        age_s   = now_wall - stale["t"]
                        self.log.warning(
                            f"Stooq: using stale cache for {label} (age={age_s:.0f}s)"
                        )
                    else:
                        self.log.warning(f"Stooq: no data at all for {label} -- skipped")
                        continue
                else:
                    _now = time.time()
                    comp_cache[cache_key] = {
                        "price":  price_f,
                        "source": src,
                        "t":      _now,
                    }
                    cache_dirty = True
                    live_count += 1

            changed = _price_changed(label, price_f, prev_lookup, self.cfg)
            if changed:
                changed_count += 1
                self.rates_changed = True

            self.price_current[pair_key] = price_f

            # Always write a synthetic session snapshot — pivot.py rejects
            # _synthetic bars and fetches its own OHLC via Stooq/TwelveData.
            session_ctx = _build_session_snapshot(float(price_f))
            self.price_session_ohlc[pair_key] = dict(session_ctx)

            hi = session_ctx.get("high")
            lo = session_ctx.get("low")
            dt = session_ctx.get("_date")
            if hi is not None:
                self.price_session_high[pair_key] = hi
            if lo is not None:
                self.price_session_low[pair_key]  = lo
            if dt:
                self.price_date[pair_key] = str(dt)

            prev_f  = prev_lookup.get(label, price_f)
            chg_pct = round(((price_f - prev_f) / prev_f) * 100, 4) if prev_f else 0.0
            stale_pct = _staleness_pct(age_s, ttl)
            stale_lbl = _staleness_label(age_s, self.cfg)
            rates.append({
                "label":       label,
                "rate":        f"{price_f:.{dec}f}",
                "prev":        f"{prev_f:.{dec}f}",
                "chg_pct":     chg_pct,
                "source":      src,
                "stale_age_s": round(age_s, 1),
                "stale_pct":   stale_pct,
                "stale_label": stale_lbl,
                "changed":     changed,
            })

        if cache_dirty:
            _save_prices_component_cache(self.cfg, comp_cache, self.log)

        self.log.info(
            f"Stooq: {len(rates)}/{len(_STOOQ_PAIRS)} pairs filled "
            f"live={live_count} cached={cached_count} "
            f"changed={changed_count} "
            f"price_daily_pairs={len(self.price_daily)} price_session_pairs={len(self.price_session_ohlc)}"
        )
        return rates

_IMPACT_ORDER: Dict[str, int] = {"Low": 1, "Medium": 2, "High": 3}
_IMPACT_STARS: Dict[str, str] = {"Low": "*", "Medium": "**", "High": "***"}
_IMPACT_ALIAS: Dict[str, str] = {
    "Moderate": "Medium", "Med": "Medium",
    "Hi": "High", "Lo": "Low",
}

# ── Stooq kalendarium fallback ───────────────────────────────────────────────
# Used when all ForexFactory XML feeds are unavailable (bot-block, HTTP error,
# or zero events).  One page per region per week; two weeks fetched to mirror
# the FF thisweek+nextweek pattern.

# Stooq economic-calendar fallback regions.
# Format: (Stooq ?r= code, mapped FX currency).  EU member-country calendars
# are intentionally mapped to EUR so EUR/USD receives German/French/Italian/etc.
# events when ForexFactory is unavailable.
_STOOQ_CAL_REGIONS: List[Tuple[str, str]] = [
    ("us", "USD"),   # United States
    ("uk", "GBP"),   # United Kingdom
    ("de", "EUR"),   # Germany
    ("it", "EUR"),   # Italy
    ("fr", "EUR"),   # France
    ("es", "EUR"),   # Spain
    ("ie", "EUR"),   # Ireland
    ("at", "EUR"),   # Austria
    ("nl", "EUR"),   # Netherlands
    ("eu", "EUR"),   # Eurozone
    ("jp", "JPY"),   # Japan
]

_STOOQ_CAL_REGION_LABELS: Dict[str, str] = {
    "us": "US",
    "uk": "UK",
    "de": "Germany",
    "it": "Italy",
    "fr": "France",
    "es": "Spain",
    "ie": "Ireland",
    "at": "Austria",
    "nl": "Netherlands",
    "eu": "Eurozone",
    "jp": "Japan",
}

_STOOQ_CAL_BASE = "https://stooq.com/kalendarium/"


def _stooq_cal_week(dt: datetime) -> str:
    """Return ISO year+week string matching Stooq's ?w= parameter, e.g. '202619'."""
    return dt.strftime("%G%V")   # %G = ISO year, %V = zero-padded 2-digit ISO week


def _stooq_impact(stars_text: str, cells) -> str:
    """Map Stooq star characters, numeric values, or CSS classes to Low/Medium/High."""
    count = stars_text.count("\u2605") + stars_text.count("*")
    if count == 0:
        try:
            count = int(stars_text.strip())
        except (ValueError, TypeError):
            count = 0
    if count == 0:
        for cell in cells:
            cls = " ".join(cell.get("class", [])).lower()
            if "high" in cls:
                return "High"
            if "med" in cls:
                return "Medium"
    if count >= 3:
        return "High"
    if count == 2:
        return "Medium"
    return "Low"


def _stooq_parse_datetime(date_str: str, time_str: str) -> Optional[datetime]:
    """Parse Stooq date/time strings (DD.MM.YYYY and HH:MM) into UTC."""
    combined = f"{date_str} {time_str}".strip()
    # Normalise European dot-separated dates: "13.05.2026" → "2026-05-13"
    m = re.match(r"(\d{1,2})\.(\d{1,2})\.(\d{4})", combined)
    if m:
        d, mo, y = m.groups()
        combined = f"{y}-{mo.zfill(2)}-{d.zfill(2)}" + combined[m.end():]
    try:
        dt = dateparser.parse(combined)
        if dt:
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=UTC)
            return dt.astimezone(UTC)
    except Exception:
        pass
    return None


def _parse_stooq_kalendarium(
    html: str, currency: str, currency_pairs: Dict,
    min_order: int, log: logging.Logger,
) -> List["CalendarEvent"]:
    """
    Parse one Stooq kalendarium HTML page.

    Expected table columns (class/id 'fth1'):
      0: date  1: time  2: title  3: actual  4: forecast  5: previous  6: impact
    Date cells may use rowspan — the last seen non-empty value is reused.
    """
    soup   = BeautifulSoup(html, "html.parser")
    pairs  = currency_pairs.get(currency, [])
    events: List["CalendarEvent"] = []

    table = (soup.find("table", id="fth1")
             or soup.find("table", class_="fth1"))
    if table is None:
        for t in soup.find_all("table"):
            rows = t.find_all("tr")
            if len(rows) > 2 and len(rows[1].find_all("td")) >= 5:
                table = t
                break

    if table is None:
        log.debug(f"stooq-cal [{currency}]: no data table found")
        return events

    current_date = ""
    for row in table.find_all("tr"):
        cells = row.find_all("td")
        if len(cells) < 5:
            continue

        date_raw = cells[0].get_text(strip=True)
        if date_raw:
            current_date = date_raw

        time_str  = cells[1].get_text(strip=True) if len(cells) > 1 else ""
        title_raw = cells[2].get_text(strip=True) if len(cells) > 2 else ""
        actual    = cells[3].get_text(strip=True) if len(cells) > 3 else ""
        forecast  = cells[4].get_text(strip=True) if len(cells) > 4 else ""
        previous  = cells[5].get_text(strip=True) if len(cells) > 5 else ""
        impact_raw = cells[6].get_text(strip=True) if len(cells) > 6 else ""

        if not title_raw or not current_date:
            continue

        impact = _stooq_impact(impact_raw, cells)
        if _IMPACT_ORDER.get(impact, 0) < min_order:
            continue

        event_time = _stooq_parse_datetime(current_date, time_str)
        events.append(CalendarEvent(
            title=title_raw, country=currency, impact=impact,
            event_time=event_time,
            actual=actual, forecast=forecast, previous=previous,
            pairs=pairs,
        ))

    return events


def _fetch_stooq_calendar(
    cfg: Dict, log: logging.Logger,
    wanted_currencies: set, currency_pairs: Dict, min_order: int,
) -> List["CalendarEvent"]:
    """
    Fetch all configured Stooq kalendarium regions for this week and next week.
    EUR regions (Eurozone + DE/IT/FR/ES/IE/AT/NL) are deduplicated by
    (title, event_time) so shared releases do not appear repeatedly.
    """
    session    = _make_session(HTML_ACCEPT, cfg)
    now_utc    = datetime.now(UTC)
    all_events: List["CalendarEvent"] = []
    seen_keys:  set = set()

    for week_dt in (now_utc, now_utc + timedelta(weeks=1)):
        week_str = _stooq_cal_week(week_dt)
        for region, currency in _STOOQ_CAL_REGIONS:
            if currency not in wanted_currencies:
                continue
            url = f"{_STOOQ_CAL_BASE}?r={region}&w={week_str}"
            try:
                _throttle(url, delay=1.5)
                resp = session.get(url, timeout=cfg.get("http_timeout", 15))
                resp.raise_for_status()
                events = _parse_stooq_kalendarium(
                    resp.text, currency, currency_pairs, min_order, log
                )
                region_label = _STOOQ_CAL_REGION_LABELS.get(region, region.upper())
                log.info(f"stooq-cal [{region_label}:{region}/{week_str}]: {len(events)} event(s)")
                for ev in events:
                    key = (ev.title, ev.country,
                           ev.event_time.isoformat() if ev.event_time else None)
                    if key not in seen_keys:
                        seen_keys.add(key)
                        all_events.append(ev)
            except Exception as exc:
                region_label = _STOOQ_CAL_REGION_LABELS.get(region, region.upper())
                log.warning(f"stooq-cal [{region_label}:{region}/{week_str}]: {exc}")

    session.close()
    return all_events
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class CalendarEvent:
    title:      str
    country:    str
    impact:     str
    event_time: Optional[datetime]
    actual:     str
    forecast:   str
    previous:   str
    pairs:      List[str]

    def to_dict(self) -> Dict:
        return {
            "title":      self.title,
            "currency":   self.country,
            "impact":     self.impact,
            "event_time": self.event_time.isoformat() if self.event_time else None,
            "actual":     self.actual,
            "forecast":   self.forecast,
            "previous":   self.previous,
            "pairs":      self.pairs,
        }

def _child_text(node, tag: str) -> Optional[str]:
    for child in node.children:
        if getattr(child, "name", None) == tag:
            text = child.get_text(strip=True)
            if text:
                return text
    return None

class ForexFactoryCalendarHandler(SourceHandler):
    name = "ForexFactory-Calendar"

    def fetch(self, scrape_time: str) -> List[Dict]:
        ttl   = self.cfg.get("calendar_cache_ttl", 1800)
        delay = self.cfg.get("ff_throttle_delay", 10.0)
        out_dir    = self.cfg["output_dir"]
        cache_path = os.path.join(out_dir, ".ff_calendar_cache.json")

        cached = _load_calendar_cache(ttl, self.log)
        if cached is not None:
            return cached
        disk_cached = _load_calendar_cache_file(cache_path, ttl, self.log)
        if disk_cached is not None:
            _save_calendar_cache(disk_cached, self.log)                            
            return disk_cached

        min_impact     = self.cfg["ff_min_impact"]
        wanted         = set(self.cfg["ff_wanted_currencies"])
        currency_pairs = self.cfg["ff_currency_pairs"]
        min_order      = _IMPACT_ORDER.get(min_impact, 1)

        session       = self._get_session(RSS_ACCEPT)
        all_events:   List[CalendarEvent] = []
        fetch_errors: List[str]           = []

        for url in self.cfg["ff_calendar_urls"]:
            label = "thisweek" if "thisweek" in url else "nextweek"
            try:
                _throttle(url, delay=delay)
                resp = _fetch_with_429_retry(url, session, self.cfg, self.log)
                if _is_bot_blocked(resp.text):
                    fetch_errors.append(f"{label}: bot-blocked")
                    self.log.warning(f"FF [{label}]: bot-blocked")
                    continue
                events = self._parse_xml(resp.text, wanted, min_order, currency_pairs)
                self.log.info(
                    f"FF [{label}]: {len(events)} events "
                    f"(impact>={min_impact}, currencies={sorted(wanted)})"
                )
                all_events.extend(events)
            except Exception as exc:
                fetch_errors.append(f"{label}: {exc}")
                self.log.error(f"FF [{label}]: {exc}")

        if not all_events:
            stale = _load_stale_calendar_cache_file(cache_path, self.log)
            if stale is not None:
                return stale

            # ── Stooq kalendarium fallback ────────────────────────────────────
            self.log.warning(
                "FF: primary feeds unavailable — trying Stooq kalendarium fallback"
            )
            stooq_events = _fetch_stooq_calendar(
                self.cfg, self.log,
                wanted_currencies=wanted,
                currency_pairs=currency_pairs,
                min_order=min_order,
            )
            if stooq_events:
                stooq_events.sort(
                    key=lambda e: e.event_time or datetime.max.replace(tzinfo=UTC)
                )
                self.log.info(
                    f"stooq-cal: {len(stooq_events)} event(s) used as FF fallback"
                )
                serialised = [e.to_dict() for e in stooq_events]
                _save_calendar_cache(serialised, self.log)
                _save_calendar_cache_file(cache_path, serialised, self.log)
                return serialised
            # ─────────────────────────────────────────────────────────────────

            detail = ("; ".join(fetch_errors) if fetch_errors
                      else f"zero events (impact>={min_impact!r})")
            raise RuntimeError(f"ForexFactory: {detail}")

        seen:   set                 = set()
        unique: List[CalendarEvent] = []
        for ev in all_events:
            key = (ev.title, ev.country, ev.event_time)
            if key not in seen:
                seen.add(key)
                unique.append(ev)
        unique.sort(
            key=lambda e: e.event_time or datetime.max.replace(tzinfo=UTC)
        )
        self.log.info(f"FF calendar total: {len(unique)} events")

        serialised = [e.to_dict() for e in unique]
        _save_calendar_cache(serialised, self.log)
        _save_calendar_cache_file(cache_path, serialised, self.log)
        return serialised

    def _parse_xml(self, xml_text: str, wanted: set,
                   min_order: int, currency_pairs: Dict) -> List[CalendarEvent]:
        soup: Optional[BeautifulSoup] = None
        for parser in ("xml", "lxml-xml"):
            try:
                soup = BeautifulSoup(xml_text, parser)
                break
            except Exception:
                continue
        if soup is None:
            logging.getLogger(__name__).warning(
                "_parse_xml: xml/lxml-xml parsers unavailable -- "
                "falling back to html.parser (tag names will be lowercased)"
            )
            soup = BeautifulSoup(xml_text, "html.parser")

        et_tz  = _ET
        events: List[CalendarEvent] = []

        for node in soup.find_all("event"):
            country = (_child_text(node, "country") or "").upper().strip()
            if country not in wanted:
                continue

            impact_raw = (_child_text(node, "impact") or "").strip()
            impact = _IMPACT_ALIAS.get(impact_raw.title(), impact_raw.title())
            if impact not in _IMPACT_ORDER or _IMPACT_ORDER[impact] < min_order:
                continue

            title = _child_text(node, "title") or ""
            if not title:
                continue

            date_raw = _child_text(node, "date") or ""
            time_raw = (_child_text(node, "time") or "").strip().lower()
            event_time: Optional[datetime] = None

            if date_raw:
                try:
                    base = dateparser.parse(date_raw)
                except Exception:
                    base = None

                if base and time_raw not in ("all day", "tentative", ""):
                    try:
                        combined = dateparser.parse(f"{date_raw} {time_raw}")
                        if combined:
                            combined   = combined.replace(tzinfo=et_tz)
                            combined   = _dateutil_tz.resolve_imaginary(combined)
                            event_time = combined.astimezone(UTC)
                    except Exception:
                        pass

                if event_time is None and base:
                    base_aware = base.replace(tzinfo=et_tz)
                    event_time = _dateutil_tz.resolve_imaginary(
                        base_aware
                    ).astimezone(UTC)

            events.append(CalendarEvent(
                title=title, country=country, impact=impact,
                event_time=event_time,
                actual=_child_text(node, "actual")    or "",
                forecast=_child_text(node, "forecast") or "",
                previous=_child_text(node, "previous") or "",
                pairs=currency_pairs.get(country, []),
            ))
        return events

def group_calendar_by_pair(
    events: List[Dict], pairs: List[str]
) -> Dict[str, List[Dict]]:
    result: Dict[str, List[Dict]] = {p: [] for p in pairs}
    for ev in events:
        for pair in ev.get("pairs", []):
            if pair in result:
                result[pair].append(ev)
    return result

def build_rss(title: str, link: str, description: str, headlines: List[Dict],
              path: str, file_mode: int, log: logging.Logger) -> None:
    if not headlines:
        log.warning(f"Writing empty RSS feed: {path}")

    fg = FeedGenerator()
    fg.title(title)
    fg.link(href=link)
    fg.description(description)
    fg.lastBuildDate(datetime.now(UTC))

    for h in headlines:
        e = fg.add_entry()
        e.id(f"{h['link']}#{h.get('source', '')}")
        e.title(f"[{h['source']}] {h['title']}")
        e.link(href=h["link"])
        if h.get("summary"):
            e.description(h["summary"])
        if h["published_at"] is not None:
            e.pubDate(h["published_at"])

    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    fg.rss_file(path)
    _set_perms(path, file_mode)
    log.info(f"RSS -> {path} ({len(headlines)} items)")

def build_calendar_rss(pair: str, events: List[Dict], path: str,
                       file_mode: int, log: logging.Logger) -> None:
    if not events:
        log.warning(f"Writing empty calendar RSS: {path}")

    fg = FeedGenerator()
    fg.title(f"{pair.upper()} Economic Calendar")
    fg.link(href="https://www.forexfactory.com/calendar")
    fg.description(
        f"Upcoming economic events affecting {pair.upper()} -- Forex Factory"
    )
    fg.lastBuildDate(datetime.now(UTC))

    for ev in events:
        impact     = ev.get("impact", "")
        stars      = _IMPACT_STARS.get(impact, impact)
        currency   = ev.get("currency", "")
        title_text = ev.get("title", "")
        event_time = ev.get("event_time") or ""
        time_str   = event_time[:16] if event_time else "TBD"

        desc_parts = [f"Currency: {currency}", f"Impact: {impact} {stars}"]
        if ev.get("forecast"):
            desc_parts.append(f"Forecast: {ev['forecast']}")
        if ev.get("previous"):
            desc_parts.append(f"Previous: {ev['previous']}")
        if ev.get("actual"):
            desc_parts.append(f"Actual: {ev['actual']}")
        if event_time:
            desc_parts.append(f"Time (UTC): {time_str}")
            desc_parts.append(
                "Note: times are UTC (converted from US Eastern source)"
            )

        e = fg.add_entry()
        e.id(re.sub(r"[^\w\-]", "-",
                    f"ff-{pair}-{currency}-{title_text}-{time_str}"))
        e.title(f"[{currency}] {title_text} ({stars}) @ {time_str} UTC")
        e.link(href="https://www.forexfactory.com/calendar")
        e.description("\n".join(desc_parts))

        if event_time:
            try:
                pub_dt = dateparser.parse(event_time)
                if pub_dt:
                    if not pub_dt.tzinfo:
                        pub_dt = pub_dt.replace(tzinfo=UTC)
                    e.pubDate(pub_dt)
            except Exception:
                pass

    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    fg.rss_file(path)
    _set_perms(path, file_mode)
    log.info(f"Calendar RSS -> {path} ({len(events)} events)")

def _write_json(path: str, data: Dict, file_mode: int,
                log: logging.Logger) -> None:
    def _default(obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        raise TypeError(f"Not serializable: {type(obj)}")

    tmp = path + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, default=_default)
        os.replace(tmp, path)
        _set_perms(path, file_mode)
        log.info(f"JSON -> {path}")
    except Exception as exc:
        log.error(f"Failed to write {path}: {exc}")
        try:
            os.remove(tmp)
        except OSError:
            pass

_CRITICAL_SOURCES = {"investingLive", "secondary"}

def _should_fail(run_status: RunStatus, all_feeds: Dict) -> bool:
    all_news_empty = all(len(v) == 0 for v in all_feeds.values())

    active_critical = [
        s for s in _CRITICAL_SOURCES if s in run_status.sources
    ]

    if not active_critical:
        return False

    critical_all_failed = all(
        not run_status.sources[s].success for s in active_critical
    )
    critical_all_empty = all(
        run_status.sources[s].items == 0 for s in active_critical
    )

    return all_news_empty and (critical_all_failed or critical_all_empty)



def _normalise_ema_state_map(raw: object) -> Dict[str, Dict[str, Any]]:
    """Normalise an EMA state mapping to lowercase pair keys.

    momentum.py/macro.py already compute the H1 EMA20/50 state from shared H1
    closes. This helper makes the shape predictable for scraper outputs and
    signal_confirm.py without forcing pivot.py to import momentum.py directly.
    """
    if not isinstance(raw, dict):
        return {}
    out: Dict[str, Dict[str, Any]] = {}
    for key, value in raw.items():
        if not isinstance(value, dict):
            continue
        pair = str(value.get("pair") or key or "").replace("/", "").lower().strip()
        if not pair:
            continue
        item = dict(value)
        item["pair"] = pair
        out[pair] = item
    return out


def _get_shared_h1_ema_states(
    cfg: Dict,
    pairs: List[str],
    macro: Optional[Dict] = None,
    log: Optional[logging.Logger] = None,
) -> Dict[str, Dict[str, Any]]:
    """Return H1 EMA20/50 states from the shared momentum.py pipeline.

    Priority:
    1. Use macro["ema_20_50_state"] if macro.py already populated it.
    2. Use momentum.get_last_ema_state() without another data fetch.
    3. For missing pairs only, lazily call momentum.detect_h1_ema_cross_and_price_state().

    This keeps pivot.py focused on pivot structure and avoids a top-level
    pivot -> momentum import cycle. Normal Telegram/pivot signals still run if
    EMA state is unavailable; signal_confirm.py simply skips confirmation.
    """
    wanted = [str(p).replace("/", "").lower().strip() for p in pairs if str(p).strip()]
    states: Dict[str, Dict[str, Any]] = {}

    # 1) Preferred: macro already sourced this from momentum.py.
    if isinstance(macro, dict):
        states.update(_normalise_ema_state_map(macro.get("ema_20_50_state", {})))

    missing = [p for p in wanted if p not in states]
    if not missing:
        if log:
            log.info("H1 EMA state: using macro/momentum shared states for %s", sorted(states.keys()))
        return states

    try:
        from momentum import (  # lazy import avoids scraper -> macro -> pivot -> momentum cycles
            get_last_ema_state as _momentum_get_last_ema_state,
            detect_h1_ema_cross_and_price_state as _momentum_detect_h1_state,
        )
    except Exception as exc:
        if log:
            log.warning("H1 EMA state: momentum.py unavailable (%s)", exc)
        return states

    # 2) Reuse last states if macro.py/momentum.py already computed them.
    try:
        last_states = _normalise_ema_state_map(_momentum_get_last_ema_state())
        if last_states:
            for pair in missing:
                if pair in last_states:
                    states[pair] = last_states[pair]
            missing = [p for p in wanted if p not in states]
            if log and last_states:
                log.info("H1 EMA state: reused momentum.get_last_ema_state() pairs=%s", sorted(last_states.keys()))
    except Exception as exc:
        if log:
            log.warning("H1 EMA state: get_last_ema_state failed (%s)", exc)

    # 3) Fetch only missing pairs as a fallback. This should be rare because
    # macro.py normally calls momentum.py before scraper signal confirmation.
    if missing:
        bars_raw = os.environ.get("SIGNAL_H1_BARS") or os.environ.get("MOMENTUM_H1_BARS", "")
        try:
            bars = int(bars_raw) if bars_raw else None
        except ValueError:
            bars = None
        for pair in missing:
            try:
                state = _momentum_detect_h1_state(pair, bars=bars)
                norm = _normalise_ema_state_map({pair: state})
                if pair in norm:
                    states[pair] = norm[pair]
            except Exception as exc:
                if log:
                    log.warning("H1 EMA state: fallback detect failed for %s (%s)", pair.upper(), exc)

    if log:
        ready = [p for p, s in states.items() if s.get("ok")]
        warming = [p for p, s in states.items() if s and not s.get("ok")]
        still_missing = [p for p in wanted if p not in states]
        log.info(
            "H1 EMA state: ready=%s warming_or_unready=%s missing=%s",
            ready,
            warming,
            still_missing,
        )
    return states

def main() -> None:
    cfg = load_config()

    os.makedirs(cfg["output_dir"], exist_ok=True)
    try:
        os.chmod(cfg["output_dir"], cfg["output_dir_mode"])
    except OSError:
        pass

    log = setup_logging(cfg["log_file"], cfg.get("log_retention_days", 0), cfg=cfg)

    deployment = _deployment_context()

    log.info("=" * 60)
    log.info(f"scraper.py v{__version__}  --  ForexFlow")
    log.info(f"macro.py          : {_MACRO_OK}")
    log.info(f"deployment        : {deployment['platform']}")
    log.info(f"run_id            : {deployment['run_id']}")
    log.info(f"trigger           : {deployment['trigger']}")
    log.info(f"workflow          : {deployment['workflow']}")
    log.info(f"SCRAPER_CONFIG    : {os.environ.get('SCRAPER_CONFIG', '(default config)')}")
    log.info(f"Python            : {sys.version.split()[0]}")
    log.info(f"output_dir        : {cfg['output_dir']}")
    log.info("=" * 60)

    scrape_time    = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    run_status     = RunStatus(scrape_time=scrape_time)
    file_mode      = cfg["output_file_mode"]
    out            = cfg["output_dir"]
    seen_hash_path = os.path.join(out, "seen_hashes.json")
    rates_path     = os.path.join(out, "rates.json")
    seen_hash_max  = cfg["seen_hashes_max"]

    seen_hashes = load_seen_hashes(seen_hash_path)
    log.info(f"Loaded {len(seen_hashes)} seen hashes from cache")

    all_pairs  = cfg["fx_pairs"] + ["xauusd"]

    enabled = set(cfg.get("enabled_sources",
                          ["inv", "sec", "kit", "mining", "cal", "stooq"]))
    all_handler_defs = {
        "inv":    InvestingLiveHandler,
        "sec":    SecondaryFeedsHandler,
        "kit":    KitcoHandler,
        "mining": MiningComHandler,
        "tegold": TradingEconomicsGoldHandler,
        "cal":    ForexFactoryCalendarHandler,
        "stooq":  StooqHandler,
    }

    handler_map = {
        k: cls(cfg, run_status, log)
        for k, cls in all_handler_defs.items()
        if k in enabled
    }

    disabled = set(all_handler_defs.keys()) - enabled
    if disabled:
        log.info(f"Disabled sources: {sorted(disabled)}")

    calendar_events: List[Dict] = []
    calendar_by_pair: Dict[str, List[Dict]] = {p: [] for p in all_pairs}
    if "cal" in handler_map:
                                                                                 
                                                                                 
        log.info("Fetching economic calendar (before macro) ...")
        try:
            calendar_events = handler_map["cal"].run(scrape_time)
        except Exception as exc:
            log.warning("Calendar fetch raised unexpectedly: %s", exc)
        calendar_by_pair = group_calendar_by_pair(calendar_events, all_pairs)
        _write_json(f"{out}/calendar.json", {
            "scrape_time":    scrape_time,
            "events":         calendar_events,
            "events_by_pair": calendar_by_pair,
        }, file_mode, log)
        log.info(
            "calendar.json written early (%d events) -- macro can now read PMI",
            len(calendar_events),
                                                                                               
        )
    else:
        log.info("Calendar handler disabled -- macro will use fallback PMI sources")

    macro: Dict = {}
    if _MACRO_OK:
        _configure_macro(out)
        log.info("Fetching macro context ...")
        try:
            macro = get_macro_context()
            us  = macro.get("us", {})
            dq  = macro.get("data_quality", {})
            spx = macro.get("spx", {})
            log.info(
                f"Macro ready -- fed={us.get('rate','?')} "
                f"ecb={macro.get('eu',{}).get('rate','?')} "
                f"boe={macro.get('gb',{}).get('rate','?')} "
                f"boj={macro.get('jp',{}).get('rate','?')} "
                f"spread={us.get('yield_spread','?')} "
                f"spx={spx.get('current','?')}(v={spx.get('valid','?')}) "
                f"surprises={macro.get('surprises',{})} "
                f"dq={dq.get('score','?')}({dq.get('grade','?')})"
            )
        except Exception as exc:
            log.warning(f"Macro failed ({exc}) -- running without enrichment")
    else:
        log.info("macro.py not found -- running without macro enrichment")

    remaining_handlers = {k: h for k, h in handler_map.items() if k != "cal"}
    log.info(f"Launching {len(remaining_handlers)} source handlers concurrently ...")
    results: Dict[str, List[Dict]] = {}
    if remaining_handlers:
        with ThreadPoolExecutor(max_workers=max(1, len(remaining_handlers))) as ex:
            fut_to_key: Dict[Future, str] = {
                ex.submit(h.run, scrape_time): k
                for k, h in remaining_handlers.items()
            }
            for fut in as_completed(fut_to_key):
                key = fut_to_key[fut]
                try:
                    results[key] = fut.result()
                except Exception as exc:
                    log.error(f"Handler [{key}] raised unexpectedly: {exc}")
                    results[key] = []

    inv             = results.get("inv",    [])
    sec             = results.get("sec",    [])
    kitco           = results.get("kit",    [])
    mining          = results.get("mining", [])
    te_gold         = results.get("tegold", [])
    rates           = results.get("stooq",  [])

    pair_items = _pregroup_by_pair(inv + sec, all_pairs, cfg)

                                                                                 
    if macro:
        _sh = handler_map.get("stooq")
        macro["_price_daily"]        = getattr(_sh, "price_daily",        {})
        macro["_price_current"]      = getattr(_sh, "price_current",      {})
        macro["_price_session_ohlc"] = getattr(_sh, "price_session_ohlc", {})
        macro["_price_session_high"] = getattr(_sh, "price_session_high", {})
        macro["_price_session_low"]  = getattr(_sh, "price_session_low",  {})
        macro["_price_date"]         = getattr(_sh, "price_date",         {})
        macro["_price_history"]      = getattr(_sh, "price_history",      {})
        macro["_price_indicators"]   = getattr(_sh, "price_indicators",   {})

    pair_bias_cache: Dict[str, Dict] = {}

    all_feeds: Dict[str, List[Dict]] = {}
    max_age = cfg.get("headline_max_age_days", 3)
    for pair in cfg["fx_pairs"]:
        merged = sort_newest(deduplicate(pair_items.get(pair, [])))
        merged = drop_old_headlines(merged, max_age, log)
        merged = merged[:cfg["max_items_fx"]]
        merged, bd = _apply_macro_to_feed(merged, pair, macro, log)
        pair_bias_cache[pair] = bd
        merged, seen_hashes = mark_new_and_update(merged, seen_hashes)
        if len(seen_hashes) > seen_hash_max:
            seen_hashes = seen_hashes[-seen_hash_max:]
        all_feeds[pair] = merged

        src_counts: Dict[str, int] = {}
        for h in merged:
            src_counts[h["source"]] = src_counts.get(h["source"], 0) + 1
        new_count = sum(1 for h in merged if h.get("is_new"))
        log.info(
            f"{pair.upper()}: {len(merged)} items ({new_count} new) "
            f"bias={bd['score']:+.2f} conf={bd['confidence']} -- {src_counts}"
        )

        build_rss(
            title=f"{pair.upper()} News Feed",
            link="https://investinglive.com/forex/",
            description=(
                f"Latest {pair.upper()} headlines from "
                "investingLive, ForexLive, ForexCrunch, ActionForex, Investing.com and DailyForex"
            ),
            headlines=merged,
            path=f"{out}/{pair}.xml",
            file_mode=file_mode, log=log,
        )

    log.info("Building XAUUSD feed ...")
    xau_from_rss = pair_items.get("xauusd", [])
    log.info(
        f"XAUUSD -- rss: {len(xau_from_rss)}, "
        f"kitco: {len(kitco)}, mining: {len(mining)}, te: {len(te_gold)}"
    )

    xau = sort_newest(deduplicate(
        xau_from_rss + copy.deepcopy(kitco) + copy.deepcopy(mining) + copy.deepcopy(te_gold)
    ))
    xau = drop_old_headlines(xau, max_age, log)
    xau = xau[:cfg["max_items_fx"]]                           
    xau, xbd = _apply_macro_to_feed(xau, "xauusd", macro, log)
    pair_bias_cache["xauusd"] = xbd
    xau, seen_hashes = mark_new_and_update(xau, seen_hashes)
    if len(seen_hashes) > seen_hash_max:
        seen_hashes = seen_hashes[-seen_hash_max:]
    all_feeds["xauusd"] = xau

    new_xau = sum(1 for h in xau if h.get("is_new"))
    log.info(
        f"XAUUSD: {len(xau)} items ({new_xau} new) "
        f"bias={xbd['score']:+.2f} conf={xbd['confidence']}"
    )

    build_rss(
        title="XAUUSD News Feed (Merged)",
        link="https://investinglive.com/forex/",
        description=(
            "Latest XAUUSD headlines from "
            "investingLive, ForexLive, ForexCrunch, ActionForex, "
            "Investing.com, DailyForex, Kitco, Mining.com and Trading Economics"
        ),
        headlines=xau,
        path=f"{out}/xauusd.xml",
        file_mode=file_mode, log=log,
    )

    save_seen_hashes(seen_hash_path, seen_hashes, seen_hash_max, log)

    for pair in all_pairs:
        pair_events = calendar_by_pair.get(pair, [])[:cfg["ff_max_events"]]
        build_calendar_rss(
            pair=pair, events=pair_events,
            path=f"{out}/{pair}_calendar.xml",
            file_mode=file_mode, log=log,
        )
        log.info(f"{pair.upper()} calendar: {len(pair_events)} events")

    macro_summary: Dict = {}
    if macro:
        pair_biases: Dict[str, Dict] = {}
        for pair in all_pairs:
            bd = pair_bias_cache.get(pair)
            if bd is None:
                try:
                    bd = _get_pair_bias(pair, macro)
                except Exception as exc:
                    log.warning("get_pair_bias(%s): %s -- using neutral bias", pair, exc)
                    bd = {"score": 0.0, "confidence": "low", "factors": {}}
            pair_biases[pair] = bd

        # ── EMA state: sourced from macro.py → momentum.py pipeline ──────────
        # All pairs are always present when momentum has enough data; warming-up
        # pairs have ok=False and suggestion="warming_up".  This intentionally
        # shares the same H1 closes/EMA state used by momentum.py instead of
        # asking pivot.py to recalculate H1 EMA independently.
        ema_20_50_state: Dict = _get_shared_h1_ema_states(cfg, all_pairs, macro, log)
        log.info("ema_20_50_state pairs: %s", list(ema_20_50_state.keys()))

        macro_summary = {
            "pair_biases":       pair_biases,
            "ema_20_50_state":   ema_20_50_state,
            "data_quality":      macro.get("data_quality", {}),
            "regime":            macro.get("_regime", ""),
            "regime_duration":   macro.get("regime_duration", 0),
            "regime_confidence": _compute_regime_confidence(macro),
            "signal_horizon":    _compute_signal_horizon(macro),
            "prev_rates":        macro.get("prev", {}),
            "indicators": {
                "fed_rate":     macro.get("us", {}).get("rate"),
                "yield_spread": macro.get("us", {}).get("yield_spread"),
                "yield_10y":    macro.get("us", {}).get("yield_10y"),
                "tips_10y":     macro.get("us", {}).get("tips_10y"),
                "tips_valid":   macro.get("us", {}).get("tips_valid"),
                "ecb_rate":     macro.get("eu", {}).get("rate"),
                "boe_rate":     macro.get("gb", {}).get("rate"),
                "boj_rate":     macro.get("jp", {}).get("rate"),
                "eu_pmi":       macro.get("eu", {}).get("pmi"),
                "eu_pmi_valid": macro.get("eu", {}).get("pmi_valid"),
                "uk_pmi":       macro.get("gb", {}).get("pmi"),
                "uk_pmi_valid": macro.get("gb", {}).get("pmi_valid"),
                "us_pmi":       macro.get("pmi"),
                "us_pmi_valid": macro.get("pmi_valid"),
                "vix":          macro.get("vix"),
                "vix_valid":    macro.get("vix_valid"),
                "vix_trend":    macro.get("vix_trend"),
                "pmi_deltas":   macro.get("pmi_deltas", {}),
            },
            "spx":       macro.get("spx", {}),
            "nikkei":    macro.get("nikkei", {}),
            "surprises": macro.get("surprises", {}),
            "sources":   macro.get("sources", {}),
            "technical_context":       macro.get("technical_context", {}),
            "daily_ema_20_50_state":   macro.get("daily_ema_20_50_state", {}),
            "h1_ema_20_50_state":      macro.get("h1_ema_20_50_state", ema_20_50_state),
            "daily_rsi_state":         macro.get("daily_rsi_state", {}),
            "h1_rsi_state":            macro.get("h1_rsi_state", {}),
            "price_context":           macro.get("price_context", {}),
        }

    if macro and macro.get("_needs_resave"):
        try:
            try:
                from macro import _save_cache as _macro_save_cache
            except ImportError:
                try:
                    from macro_rewritten_full import _save_cache as _macro_save_cache
                except ImportError:
                    from macro_rewritten import _save_cache as _macro_save_cache
            _macro_save_cache(macro)
            log.info("macro cache re-saved with updated _prev_scores")
        except Exception as _exc:
            log.warning(f"macro re-save failed: {_exc}")

    _write_json(f"{out}/forex.json", {
        "scrape_time": scrape_time,
        "status":      run_status.to_dict(),
        "macro":       macro_summary,
        "feeds":       all_feeds,
    }, file_mode, log)

    if macro_summary:
        _stooq_handler       = handler_map.get("stooq")
        _price_daily         = getattr(_stooq_handler, "price_daily",        {})
        _price_current       = getattr(_stooq_handler, "price_current",      {})
        _price_session_ohlc  = getattr(_stooq_handler, "price_session_ohlc", {})
        _price_session_high  = getattr(_stooq_handler, "price_session_high", {})
        _price_session_low   = getattr(_stooq_handler, "price_session_low",  {})
        _price_date          = getattr(_stooq_handler, "price_date",         {})
        _price_history       = getattr(_stooq_handler, "price_history",      {})
        _price_indicators    = getattr(_stooq_handler, "price_indicators",   {})

        _write_json(f"{out}/macro_components.json", {
            "scrape_time":        scrape_time,
            "indicators":         macro_summary.get("indicators", {}),
            "pair_biases":        macro_summary.get("pair_biases", {}),
            "ema_20_50_state":    macro_summary.get("ema_20_50_state", {}),
            "h1_ema_20_50_state": macro_summary.get("h1_ema_20_50_state", macro_summary.get("ema_20_50_state", {})),
            "daily_ema_20_50_state": macro_summary.get("daily_ema_20_50_state", {}),
            "h1_rsi_state":       macro_summary.get("h1_rsi_state", {}),
            "daily_rsi_state":    macro_summary.get("daily_rsi_state", {}),
            "technical_context":  macro_summary.get("technical_context", {}),
            "sources":            macro_summary.get("sources", {}),
            "data_quality":       macro_summary.get("data_quality", {}),
            "spx":                macro_summary.get("spx", {}),
            "nikkei":             macro_summary.get("nikkei", {}),
            "surprises":          macro_summary.get("surprises", {}),
            "regime":             macro_summary.get("regime", ""),
            "regime_duration":    macro_summary.get("regime_duration", 0),
            "regime_confidence":  macro_summary.get("regime_confidence", 0.5),
            "signal_horizon":     macro_summary.get("signal_horizon", "medium"),
            "prev_rates":         macro_summary.get("prev_rates", {}),
            "price_daily":        _price_daily,
            "price_current":      _price_current,
            "price_session_ohlc": _price_session_ohlc,
            "price_session_high": _price_session_high,
            "price_session_low":  _price_session_low,
            "price_date":         _price_date,
            "price_history":      _price_history,
            "price_indicators":   _price_indicators,
            "price_context": {
                **macro_summary.get("price_context", {}),
                **{
                    pair: bias.get("price_context")
                    for pair, bias in macro_summary.get("pair_biases", {}).items()
                    if isinstance(bias, dict) and bias.get("price_context")
                },
            },
        }, file_mode, log)
    else:
        log.info("macro_components.json skipped -- no macro data this run")

    # ── Signal confirmation (signal_confirm.py) ──────────────────────────
    if _SIGNAL_CONFIRM_OK:
        _ema_states = macro_summary.get("ema_20_50_state", {}) if macro_summary else {}
        if not _ema_states:
            _ema_states = _get_shared_h1_ema_states(cfg, all_pairs, macro, log)
        if _PIVOT_OK and _ema_states:
            log.info("signal_confirm: running signal confirmation ...")
            try:
                _pivot_results, _pivot_note = _fetch_price_structure()
                log.info("signal_confirm: pivot fetch note=%s pairs=%s",
                         _pivot_note, list(_pivot_results.keys()))
                _signals = _signal_confirm.batch_combine(_pivot_results, _ema_states)
                _write_json(f"{out}/signals.json", {
                    "scrape_time": scrape_time,
                    "signals": _signals,
                }, file_mode, log)
                _n_alerts = _signal_confirm.dispatch_signal_alerts(
                    _pivot_results, _ema_states
                )
                log.info("signal_confirm: %d Telegram alert(s) dispatched", _n_alerts)
            except Exception as _sc_exc:
                log.warning("signal_confirm: error during signal run: %s", _sc_exc)
        elif not _PIVOT_OK:
            log.info("signal_confirm: pivot.py not importable -- signals skipped this run")
        else:
            log.info("signal_confirm: no ema_20_50_state available -- signals skipped")
    else:
        log.info("signal_confirm: module not found -- signals skipped this run")
    # ──────────────────────────────────────────────────────────────────────

    rates_stale   = False
    rates_fetched = scrape_time

    sources_seen  = {r.get("source", "Stooq") for r in rates if r.get("source") not in ("none", "")}
    rates_source  = "+".join(sorted(sources_seen)) if sources_seen else "Stooq"

    stooq_handler = handler_map.get("stooq")
    rates_ttl     = _effective_ttl(cfg)
    rates_changed_flag = getattr(stooq_handler, "rates_changed", True)

    if not rates:
        log.warning("Stooq returned no rates -- falling back to cached rates.json")
        try:
            with open(rates_path, encoding="utf-8") as f:
                cached_rates_file = json.load(f)
            rates         = cached_rates_file.get("rates", [])
            rates_fetched = cached_rates_file.get("fetched_at", scrape_time)
            rates_source  = cached_rates_file.get("source", "cache")
            rates_stale   = True
            rates_changed_flag = False                                 
            log.info(f"Using {len(rates)} stale cached rates (no rewrite)")
        except Exception:
            log.warning("No cached rates.json -- ticker empty this run")

    if rates_changed_flag:
        _write_json(rates_path, {
            "fetched_at":   rates_fetched,
            "source":       rates_source,
            "stale":        rates_stale,
            "ttl_s":        rates_ttl,
            "attribution":  cfg.get("rates_attribution", ""),
            "rates":        rates,
        }, file_mode, log)
        changed_labels = [r["label"] for r in rates if r.get("changed")]
        log.info(f"rates.json written -- {len(rates)} rates, "
                 f"{len(changed_labels)} changed: {changed_labels}")
    else:
        log.info(
            f"rates.json SKIPPED (no price change above threshold) -- "
            f"{len(rates)} rates, TTL={rates_ttl:.0f}s"
        )

    st = run_status.to_dict()
    total_items    = sum(info["items"] for info in st["sources"].values())
    total_enabled  = len(all_handler_defs)
    total_active   = st["total_sources"]
    total_disabled = total_enabled - total_active
    total_failed   = st["failed_sources"]
    total_ok       = total_active - total_failed

    feed_item_counts = {pair: len(v) for pair, v in all_feeds.items()}
    total_feed_items = sum(feed_item_counts.values())

    disabled_str = f", {total_disabled} disabled" if total_disabled else ""
    log.info("=" * 60)
    log.info(
        f"Run complete -- {total_active}/{total_enabled} sources active{disabled_str}  |  "
        f"{total_ok} succeeded, {total_failed} failed  |  "
        f"{total_items} raw items fetched  |  "
        f"{total_feed_items} headlines in feeds  |  "
        f"scrape_time={scrape_time}"
    )
    log.info("-" * 60)
    for src, info in sorted(st["sources"].items(),
                            key=lambda kv: kv[1]["latency_ms"]):
        icon = "[OK]" if info["success"] else "[FAIL]--"
        log.info(
            f"  {icon} {src:<28} "
            f"items={info['items']:>4}  "
            f"latency={info['latency_ms']:>5}ms"
            + (f"  ERR: {info['error']}" if info["error"] else "")
        )
    if disabled:
        log.info(f"  -- disabled: {', '.join(sorted(disabled))}")
    log.info("-" * 60)
    log.info(
        "  Feed totals: "
        + "  ".join(
            f"{pair.upper()}={feed_item_counts.get(pair, 0)}"
            for pair in all_pairs
        )
    )
    log.info("=" * 60)

    # ── EMA engine (ema.py) ──────────────────────────────────────────────────
    if _EMA_OK:
        log.info("ema: starting EMA analysis ...")
        try:
            _ema_ensure_files()
            _ema_results, _ema_errors = _ema_run_analysis(_EMA_PAIRS, fast=20, slow=50)
            _ema_save_json(_EMA_STATE_FILE, {
                "generated_at": _ema_iso_z(_ema_now_utc()),
                "ema_periods":  {"fast": 20, "slow": 50},
                **_ema_quota_summary(),
                "results": {r.pair: _asdict(r) for r in _ema_results},
                "errors":  _ema_errors,
            })
            _ema_trim_snapshots()
            log.info("ema: complete — %d pair(s), %d error(s)",
                     len(_ema_results), len(_ema_errors))
        except Exception as _ema_exc:
            log.warning("ema: run failed — %s", _ema_exc, exc_info=True)
    else:
        log.info("ema: module not found — skipped this run")
    # ─────────────────────────────────────────────────────────────────────────

    if _should_fail(run_status, all_feeds):
        log.error(
            "FATAL: all critical sources failed and no feed data produced. "
            "Exiting with code 1 -- GitHub Actions or your scheduler can mark this run as failed."
        )
        sys.exit(1)

if __name__ == "__main__":
    main()

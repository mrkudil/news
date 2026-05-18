"""
signal_confirm.py
-----------------
Combines pivot.py price-structure output with ema.py EMA-state output
to produce a single confirmed/conflicted/weak signal for each pair.

Usage
-----
    from signal_confirm import combine_pivot_ema_signal, batch_combine

    # Single pair
    sig = combine_pivot_ema_signal(pivot_result, ema_state)
    print(sig["signal"], sig["confidence"], sig["reason"])

    # All pairs at once
    signals = batch_combine(pivot_results, ema_states)
    # signals["eurusd"]["signal"] == "confirmed_long" etc.

Inputs
------
pivot_result  : dict returned by pivot.classify_price_structure() (or one entry
                from fetch_price_structure()[0])
ema_state     : dict returned by ema.py / EMAAnalysisResult asdict() (or one
                entry from the "ema_20_50_state" block in macro_components.json)

Output keys
-----------
signal        : str  — see SIGNAL_* constants below
confidence    : float 0.0–1.0  — composite confidence derived from
                conviction_mult × ema weight
direction     : "long" | "short" | "neutral"
reason        : str  — human-readable explanation of the ruling
pivot_state   : str  — passthrough from pivot_result
ema_bias      : str  — passthrough from ema_state
ema_cross     : str  — passthrough from ema_state
macro_align   : str  — passthrough from pivot_result
warming_up    : bool — True when EMA data is insufficient
ok            : bool — False when either input is unavailable/warming-up
"""

from __future__ import annotations

from typing import Any, Dict, Optional

# ---------------------------------------------------------------------------
# Small compatibility helpers
# ---------------------------------------------------------------------------

_MISSING = object()


def _get_any(data: Dict[str, Any], *keys: str, default: Any = None) -> Any:
    """Return the first non-empty value from data for any of the supplied keys."""
    if not isinstance(data, dict):
        return default
    for key in keys:
        value = data.get(key, _MISSING)
        if value is not _MISSING and value not in (None, ""):
            return value
    return default


def _as_float(value: Any, default: float = 0.0) -> float:
    """Best-effort float coercion for mixed pivot/EMA JSON fields."""
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _norm_pair(pair: Any) -> str:
    """Normalise pair keys to the lower-case convention used by pivot.py."""
    return str(pair or "").strip().lower()


# ---------------------------------------------------------------------------
# Signal constants
# ---------------------------------------------------------------------------

SIGNAL_CONFIRMED_LONG       = "confirmed_long"
SIGNAL_CONFIRMED_SHORT      = "confirmed_short"
SIGNAL_HIGH_CONVICTION_LONG  = "high_conviction_long"
SIGNAL_HIGH_CONVICTION_SHORT = "high_conviction_short"
SIGNAL_CONFLICTED           = "conflicted"
SIGNAL_WEAK                 = "weak"
SIGNAL_UNCONFIRMED          = "unconfirmed"   # EMA warming up
SIGNAL_UNAVAILABLE          = "unavailable"   # pivot data missing


# ---------------------------------------------------------------------------
# EMA trend-bias → direction helpers
# ---------------------------------------------------------------------------

_BULLISH_BIASES = {"bullish_bias"}
_BEARISH_BIASES = {"bearish_bias"}

def _ema_is_bullish(ema: Dict[str, Any]) -> bool:
    return _get_any(ema, "trend_bias", "bias", default="") in _BULLISH_BIASES

def _ema_is_bearish(ema: Dict[str, Any]) -> bool:
    return _get_any(ema, "trend_bias", "bias", default="") in _BEARISH_BIASES

def _price_above_both(ema: Dict[str, Any]) -> bool:
    # momentum.py uses "price_vs_fast/slow"; ema.py uses "current_vs_fast/slow".
    # Fall back to the ema.py key so both sources work correctly.
    vs_fast = _get_any(ema, "price_vs_fast", "current_vs_fast", "close_vs_fast")
    vs_slow = _get_any(ema, "price_vs_slow", "current_vs_slow", "close_vs_slow")
    return vs_fast == "above" and vs_slow == "above"

def _price_below_both(ema: Dict[str, Any]) -> bool:
    vs_fast = _get_any(ema, "price_vs_fast", "current_vs_fast", "close_vs_fast")
    vs_slow = _get_any(ema, "price_vs_slow", "current_vs_slow", "close_vs_slow")
    return vs_fast == "below" and vs_slow == "below"


# ---------------------------------------------------------------------------
# Cross-signal canonicalisation
# ---------------------------------------------------------------------------
# Different upstream modules emit different strings for the same EMA-cross event:
#   older modules → "bullish_cross" / "bearish_cross" / "none"
#   ema.py       → "cross_up"      / "cross_down"    / "no_cross"
#   macro JSON   → "golden_cross"  / "death_cross"   / "none"  (canonical)
#
# Rules L2 and S2 (high-conviction signals) compare against the canonical form.
# Normalising here keeps the rule code clean and source-agnostic.
# ---------------------------------------------------------------------------

_CROSS_CANONICAL: Dict[str, str] = {
    # older modules / suggestion strings
    "bullish_cross": "golden_cross",
    "bearish_cross": "death_cross",
    "watch_long": "golden_cross",
    "watch_short": "death_cross",
    # ema.py
    "cross_up":      "golden_cross",
    "cross_down":    "death_cross",
    # already canonical
    "golden_cross":  "golden_cross",
    "death_cross":   "death_cross",
}


def _normalise_cross(raw: Any) -> str:
    """Map any upstream cross-signal string to 'golden_cross', 'death_cross', or 'none'."""
    return _CROSS_CANONICAL.get(str(raw or "").strip().lower(), "none")


def _ema_cross_value(ema: Dict[str, Any]) -> str:
    """Read the EMA cross from either ema.py or older macro/momentum schemas."""
    return _normalise_cross(
        _get_any(ema, "ema_cross_signal", "cross", "cross_signal", "suggestion", default="none")
    )


# ---------------------------------------------------------------------------
# Confidence calculation
# ---------------------------------------------------------------------------

# Base confidence weights by EMA quality
_EMA_CONFIDENCE_WEIGHTS: Dict[str, float] = {
    "bullish_bias":  0.75,
    "bearish_bias":  0.75,
    "neutral_bias":  0.40,
}

def _base_confidence(pivot_result: Dict[str, Any], ema: Dict[str, Any]) -> float:
    """
    Blend pivot conviction_mult (already 0–1.1) with EMA weight to produce
    a normalised 0–1 confidence score.

    conviction_mult is capped at 1.0 before blending so a breakout WITH macro
    (mult=1.10) doesn't push confidence above the EMA weight ceiling.
    """
    conv   = min(float(pivot_result.get("conviction_mult", 1.0)), 1.0)
    ema_w  = _EMA_CONFIDENCE_WEIGHTS.get(_get_any(ema, "trend_bias", "bias", default="neutral_bias"), 0.40)
    # Normalise the raw cross string here too — _base_confidence is called with the
    # raw ema_state dict so it would otherwise miss "bullish_cross" (momentum.py)
    # and "cross_up" (ema.py), causing the 5pp cross bonus to never fire.
    cross  = _ema_cross_value(ema)
    # Small cross bonus: a fresh golden/death cross adds 5 pp to confidence.
    cross_bonus = 0.05 if cross in ("golden_cross", "death_cross") else 0.0
    raw = (conv * 0.55) + (ema_w * 0.45) + cross_bonus
    return round(min(raw, 1.0), 3)


# ---------------------------------------------------------------------------
# Core combiner
# ---------------------------------------------------------------------------

def combine_pivot_ema_signal(
    pivot_result: Dict[str, Any],
    ema_state: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Combine one pair's pivot structure result with its EMA state.

    This accepts the current pivot.py / ema.py dictionaries and remains backward
    compatible with older macro_components / momentum-style field names.
    """
    pivot_result = pivot_result if isinstance(pivot_result, dict) else {}
    ema_state = ema_state if isinstance(ema_state, dict) else {}

    # ---- passthrough / normalised fields ------------------------------------
    pivot_state  = _get_any(pivot_result, "price_state", "state", default="unavailable")
    macro_align  = _get_any(pivot_result, "macro_alignment", "macro_align", default="neutral")
    ema_bias     = _get_any(ema_state, "trend_bias", "bias", default="neutral_bias")
    ema_cross    = _ema_cross_value(ema_state)
    ema_ok       = bool(_get_any(ema_state, "ok", "is_valid", default=False))
    warming_up   = bool(_get_any(ema_state, "warming_up", "warmup", default=not ema_ok))

    def _build(
        signal: str,
        direction: str,
        reason: str,
        ok: bool = True,
    ) -> Dict[str, Any]:
        confidence = _base_confidence(pivot_result, ema_state) if ok else 0.0
        return {
            "signal":      signal,
            "confidence":  confidence,
            "direction":   direction,
            "reason":      reason,
            "pivot_state": pivot_state,
            "ema_bias":    ema_bias,
            "ema_cross":   ema_cross,
            "macro_align": macro_align,
            "warming_up":  warming_up,
            "ok":          ok,
            # Useful downstream/debug passthroughs from the updated modules.
            "ohlc_date":   _get_any(pivot_result, "ohlc_date", "date", default=""),
            "pivot_source": _get_any(pivot_result, "source", "data_source", "ohlc_source", default=""),
            "ema_source":  _get_any(ema_state, "source", "data_source", "history_source", default=""),
            "h1_ema_alignment": _get_any(pivot_result, "h1_ema_alignment", default=""),
            "h1_ema_direction": _get_any(pivot_result, "h1_ema_direction", default=""),
            "pivot_direction": _get_any(pivot_result, "pivot_direction", default=""),
            "h1_ema_conviction_mult": _get_any(pivot_result, "h1_ema_conviction_mult", default=""),
            "h1_rsi": _get_any(pivot_result, "h1_rsi", default=None),
            "h1_rsi_label": _get_any(pivot_result, "h1_rsi_label", default=""),
            "h1_rsi_signal": _get_any(pivot_result, "h1_rsi_signal", default=""),
            "h1_ema": pivot_result.get("h1_ema") if isinstance(pivot_result.get("h1_ema"), dict) else {},
            # New macro.py technical-context passthroughs.  These are optional,
            # so older pivot.py / macro_components.json payloads still work.
            "d1_rsi": _get_any(pivot_result, "rsi", "d1_rsi", "rsi14", default=None),
            "d1_rsi_state": _get_any(pivot_result, "rsi_state", "d1_rsi_state", default=""),
            "d1_rsi_bias": _get_any(pivot_result, "rsi_bias", "d1_rsi_bias", default=""),
            "technical_summary": pivot_result.get("technical_summary") if isinstance(pivot_result.get("technical_summary"), dict) else {},
            "technical_context": pivot_result.get("technical_context") if isinstance(pivot_result.get("technical_context"), dict) else {},
        }

    # ---- guards --------------------------------------------------------------
    if pivot_state == "unavailable":
        return _build(SIGNAL_UNAVAILABLE, "neutral", "Pivot data unavailable", ok=False)

    if warming_up or not ema_ok:
        return _build(
            SIGNAL_UNCONFIRMED,
            "neutral",
            f"EMA Not Ready (warming_up={warming_up}, ok={ema_ok})",
            ok=False,
        )

    if pivot_state == "balance":
        return _build(SIGNAL_WEAK, "neutral", "Price In Balance Zone Around PP — No Edge")

    # ========================================================================
    # LONG-side rules
    # ========================================================================

    if pivot_state == "breakout_up" and _ema_is_bullish(ema_state) and _price_above_both(ema_state):
        cross_note = f" + {ema_cross}" if ema_cross == "golden_cross" else ""
        return _build(SIGNAL_CONFIRMED_LONG, "long", f"Breakout_Up Confirmed By Bullish EMA Stack{cross_note}")

    if pivot_state in ("accept_above_pp", "breakout_up") and ema_cross == "golden_cross" and _ema_is_bullish(ema_state):
        return _build(SIGNAL_HIGH_CONVICTION_LONG, "long", f"Golden_Cross During {pivot_state} — Elevated Conviction")

    if pivot_state == "accept_above_pp" and _ema_is_bullish(ema_state):
        return _build(SIGNAL_CONFIRMED_LONG, "long", "Accept_Above_PP With Bullish EMA Trend")

    # ========================================================================
    # SHORT-side rules
    # ========================================================================

    if pivot_state == "breakout_down" and _ema_is_bearish(ema_state) and _price_below_both(ema_state):
        cross_note = f" + {ema_cross}" if ema_cross == "death_cross" else ""
        return _build(SIGNAL_CONFIRMED_SHORT, "short", f"Breakout_Down Confirmed By Bearish EMA Stack{cross_note}")

    if pivot_state in ("accept_below_pp", "breakout_down") and ema_cross == "death_cross" and _ema_is_bearish(ema_state):
        return _build(SIGNAL_HIGH_CONVICTION_SHORT, "short", f"Death_Cross During {pivot_state} — Elevated Conviction")

    if pivot_state == "accept_below_pp" and _ema_is_bearish(ema_state):
        return _build(SIGNAL_CONFIRMED_SHORT, "short", "Accept_Below_PP With Bearish EMA Trend")

    # ========================================================================
    # REJECTION rules
    # ========================================================================

    if pivot_state == "rejection" and _ema_is_bearish(ema_state):
        return _build(SIGNAL_CONFIRMED_SHORT, "short", "Rejection From R-Level Confirmed By Bearish EMA")

    if pivot_state == "rejection" and _ema_is_bullish(ema_state):
        return _build(SIGNAL_CONFIRMED_LONG, "long", "Rejection (Bounce) From S-Level Confirmed By Bullish EMA")

    if pivot_state == "rejection":
        return _build(SIGNAL_WEAK, "neutral", "Rejection Signal But EMA Trend Is Neutral — Unconfirmed")

    # ========================================================================
    # CONFLICTED / fallback
    # ========================================================================

    pivot_is_bullish = pivot_state in ("breakout_up", "accept_above_pp")
    pivot_is_bearish = pivot_state in ("breakout_down", "accept_below_pp")

    if pivot_is_bullish and _ema_is_bearish(ema_state):
        return _build(SIGNAL_CONFLICTED, "neutral", f"{pivot_state} But EMA Trend Is Bearish — Conflicted")

    if pivot_is_bearish and _ema_is_bullish(ema_state):
        return _build(SIGNAL_CONFLICTED, "neutral", f"{pivot_state} But EMA Trend Is Bullish — Conflicted")

    direction = "long" if pivot_is_bullish else "short" if pivot_is_bearish else "neutral"
    return _build(SIGNAL_WEAK, direction, f"{pivot_state} With Neutral / Non-Confirming EMA")


# ---------------------------------------------------------------------------


def _seeded_indicator_to_ema_state(pair: str, indicator: Dict[str, Any], pivot_result: Dict[str, Any]) -> Dict[str, Any]:
    """Accept scraper.py price_indicators as a fallback EMA state."""
    if not isinstance(indicator, dict):
        return {}
    try:
        ema20 = float(indicator.get("ema20"))
        ema50 = float(indicator.get("ema50"))
    except (TypeError, ValueError):
        return {}
    price = _sc_h1_float(_get_any(pivot_result if isinstance(pivot_result, dict) else {}, "current_price", "price", "close", default=None))
    fast_vs_slow = "above" if ema20 > ema50 else "below" if ema20 < ema50 else "at"
    trend_bias = "bullish_bias" if fast_vs_slow == "above" else "bearish_bias" if fast_vs_slow == "below" else "neutral_bias"
    def _pos(v, ref):
        if v is None:
            return None
        return "above" if v > ref else "below" if v < ref else "at"
    price_vs_fast = _pos(price, ema20)
    price_vs_slow = _pos(price, ema50)
    return {
        "pair": pair,
        "timeframe": "D1",
        "ema20": round(ema20, 6),
        "ema50": round(ema50, 6),
        "fast_ema": round(ema20, 6),
        "slow_ema": round(ema50, 6),
        "ema20_vs_ema50": fast_vs_slow,
        "trend_bias": trend_bias,
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
        "rsi14": indicator.get("rsi14"),
        "last_bar_date": indicator.get("last_bar_date"),
    }

# ---------------------------------------------------------------------------
# H1 momentum context from momentum_closes.json
# ---------------------------------------------------------------------------

def _sc_clamp(value: float, low: float = 0.0, high: float = 1.0) -> float:
    return max(low, min(high, value))


def _sc_h1_float(value: Any) -> Optional[float]:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _sc_h1_ema(values: list, period: int) -> Optional[float]:
    """Simple EMA for completed H1 closes; no provider call is made here."""
    if len(values) < period:
        return None
    k = 2.0 / (period + 1.0)
    ema = sum(values[:period]) / period
    for value in values[period:]:
        ema = (value * k) + (ema * (1.0 - k))
    return ema


def _sc_h1_candidate_files() -> list:
    """Candidate paths for momentum_closes.json, in safest priority order."""
    candidates = []
    explicit = SIGNAL_H1_CLOSES_FILE or _os.environ.get("MOMENTUM_H1_CLOSES_FILE", "").strip()
    if explicit:
        candidates.append(_Path(explicit))
    data_dir = _os.environ.get("MOMENTUM_DATA_DIR", "").strip()
    if data_dir:
        candidates.append(_Path(data_dir) / "momentum_closes.json")
    candidates.extend([
        _Path("public_html/momentum_closes.json"),
        _Path("public/momentum_closes.json"),
        _Path("momentum_closes.json"),
        _Path(__file__).resolve().parent / "momentum_closes.json",
    ])
    out = []
    seen = set()
    for candidate in candidates:
        try:
            key = str(candidate.resolve()) if candidate.exists() else str(candidate)
        except Exception:
            key = str(candidate)
        if key not in seen:
            seen.add(key)
            out.append(candidate)
    return out


def _load_h1_closes_for_signal(pair: str) -> tuple:
    """Load completed H1 close rows for one pair from momentum_closes.json."""
    pair_key = _norm_pair(pair)
    for path in _sc_h1_candidate_files():
        try:
            if not path.exists():
                continue
            with path.open(encoding="utf-8") as handle:
                data = _json.load(handle)
            if not isinstance(data, dict):
                continue
            raw_rows = data.get(pair_key) or data.get(pair_key.upper()) or data.get(pair)
            if not isinstance(raw_rows, list):
                continue
            rows = []
            for row in raw_rows:
                if not isinstance(row, dict):
                    continue
                close = _sc_h1_float(row.get("close"))
                if close is None:
                    continue
                hour = str(row.get("hour") or row.get("datetime") or row.get("ts") or "")
                rows.append({"hour": hour, "close": close})
            rows.sort(key=lambda r: r.get("hour", ""))
            return rows, str(path)
        except Exception as exc:
            _log.debug("signal H1 momentum: failed reading %s: %s", path, exc)
            continue
    return [], ""


def classify_h1_movement(
    pair: str,
    ema_state: Optional[Dict[str, Any]] = None,
    signal_direction: str = "neutral",
) -> Dict[str, Any]:
    """Classify current H1 movement using the H1 EMA stack.

    Uses only local momentum_closes.json. It never calls Twelve Data.

    Fix notes:
    - H1 confirmation is now based on H1 EMA20/EMA50, not only the Daily EMA bias.
    - Confidence adjustment is applied against the signal direction when available:
      LONG wants bullish H1 EMA, SHORT wants bearish H1 EMA.
    - Daily EMA bias is retained only as a fallback when the signal direction is neutral.
    """
    if not SIGNAL_USE_H1_MOMENTUM:
        return {"h1_status": "disabled", "h1_movement": "disabled", "h1_score": 0}

    ema_state = ema_state if isinstance(ema_state, dict) else {}
    rows, path = _load_h1_closes_for_signal(pair)
    closes = [r["close"] for r in rows]

    fast_period = int(globals().get("SIGNAL_H1_FAST_EMA", 20))
    slow_period = int(globals().get("SIGNAL_H1_SLOW_EMA", 50))
    min_required = max(SIGNAL_H1_MIN_BARS, slow_period, 7)

    if len(closes) < min_required:
        return {
            "h1_status": "insufficient_data" if rows else "unavailable",
            "h1_movement": "insufficient_data" if rows else "unavailable",
            "h1_bias_alignment": "neutral_or_mixed",
            "h1_ema_bias": "neutral_bias",
            "h1_score": 0,
            "h1_bars": len(closes),
            "h1_file": path,
            "h1_reason": f"Need at least {min_required} completed H1 closes for EMA{fast_period}/EMA{slow_period}",
        }

    last = closes[-1]
    close_3 = closes[-4]
    close_6 = closes[-7]
    fast_now = _sc_h1_ema(closes, fast_period)
    fast_prev = _sc_h1_ema(closes[:-1], fast_period)
    slow_now = _sc_h1_ema(closes, slow_period)
    slow_prev = _sc_h1_ema(closes[:-1], slow_period)

    score = 0
    reasons = []

    # Short-term impulse checks.
    if last > close_3:
        score += 1; reasons.append("last close above 3H ago")
    elif last < close_3:
        score -= 1; reasons.append("last close below 3H ago")

    if last > close_6:
        score += 1; reasons.append("last close above 6H ago")
    elif last < close_6:
        score -= 1; reasons.append("last close below 6H ago")

    # H1 EMA stack checks.
    if fast_now is not None:
        if last > fast_now:
            score += 1; reasons.append(f"last close above H1 EMA{fast_period}")
        elif last < fast_now:
            score -= 1; reasons.append(f"last close below H1 EMA{fast_period}")

    if slow_now is not None:
        if last > slow_now:
            score += 1; reasons.append(f"last close above H1 EMA{slow_period}")
        elif last < slow_now:
            score -= 1; reasons.append(f"last close below H1 EMA{slow_period}")

    if fast_now is not None and slow_now is not None:
        if fast_now > slow_now:
            score += 1; reasons.append(f"H1 EMA{fast_period} above EMA{slow_period}")
        elif fast_now < slow_now:
            score -= 1; reasons.append(f"H1 EMA{fast_period} below EMA{slow_period}")

    if fast_now is not None and fast_prev is not None:
        if fast_now > fast_prev:
            score += 1; reasons.append(f"H1 EMA{fast_period} rising")
        elif fast_now < fast_prev:
            score -= 1; reasons.append(f"H1 EMA{fast_period} falling")

    if slow_now is not None and slow_prev is not None:
        if slow_now > slow_prev:
            score += 1; reasons.append(f"H1 EMA{slow_period} rising")
        elif slow_now < slow_prev:
            score -= 1; reasons.append(f"H1 EMA{slow_period} falling")

    # EMA trend bias used for final confirmation.
    if fast_now is not None and slow_now is not None and last > fast_now > slow_now:
        h1_ema_bias = "bullish_bias"
    elif fast_now is not None and slow_now is not None and last < fast_now < slow_now:
        h1_ema_bias = "bearish_bias"
    else:
        h1_ema_bias = "neutral_bias"

    if score >= 3 or h1_ema_bias == "bullish_bias":
        movement = "h1_bullish"
    elif score <= -3 or h1_ema_bias == "bearish_bias":
        movement = "h1_bearish"
    else:
        movement = "h1_mixed"

    signal_direction = str(signal_direction or "neutral").lower()
    if signal_direction == "long":
        if h1_ema_bias == "bullish_bias" or movement == "h1_bullish":
            alignment = "with_h1_ema"
        elif h1_ema_bias == "bearish_bias" or movement == "h1_bearish":
            alignment = "against_h1_ema"
        else:
            alignment = "neutral_or_mixed"
    elif signal_direction == "short":
        if h1_ema_bias == "bearish_bias" or movement == "h1_bearish":
            alignment = "with_h1_ema"
        elif h1_ema_bias == "bullish_bias" or movement == "h1_bullish":
            alignment = "against_h1_ema"
        else:
            alignment = "neutral_or_mixed"
    else:
        # Backward-compatible fallback for neutral signals.
        daily_bias = _get_any(ema_state, "trend_bias", "bias", default="neutral_bias")
        if daily_bias == "bullish_bias" and movement == "h1_bullish":
            alignment = "with_bias"
        elif daily_bias == "bullish_bias" and movement == "h1_bearish":
            alignment = "against_bias"
        elif daily_bias == "bearish_bias" and movement == "h1_bearish":
            alignment = "with_bias"
        elif daily_bias == "bearish_bias" and movement == "h1_bullish":
            alignment = "against_bias"
        else:
            alignment = "neutral_or_mixed"

    if alignment in ("with_h1_ema", "with_bias"):
        adjustment = SIGNAL_H1_WITH_BIAS_BONUS
    elif alignment in ("against_h1_ema", "against_bias"):
        adjustment = -SIGNAL_H1_AGAINST_BIAS_PENALTY
    else:
        adjustment = 0.0

    return {
        "h1_status": "ok",
        "h1_movement": movement,
        "h1_bias_alignment": alignment,
        "h1_ema_bias": h1_ema_bias,
        "h1_score": score,
        "h1_confidence_adjustment": round(adjustment, 3),
        "h1_last_close": round(last, 6),
        "h1_ema20": round(fast_now, 6) if fast_now is not None and fast_period == 20 else None,
        "h1_ema50": round(slow_now, 6) if slow_now is not None and slow_period == 50 else None,
        "h1_fast_ema": round(fast_now, 6) if fast_now is not None else None,
        "h1_slow_ema": round(slow_now, 6) if slow_now is not None else None,
        "h1_fast_ema_period": fast_period,
        "h1_slow_ema_period": slow_period,
        "h1_bars": len(closes),
        "h1_last_hour": rows[-1].get("hour", "") if rows else "",
        "h1_file": path,
        "h1_reason": "; ".join(reasons),
    }


def _h1_alignment_compact(value: Any) -> str:
    v = str(value or "").strip().lower()
    if v in ("with", "with_h1_ema", "with_bias", "aligned", "confirmed"):
        return "WITH"
    if v in ("against", "against_h1_ema", "against_bias", "conflict", "blocked"):
        return "AGAINST"
    if v in ("missing", "unavailable", "insufficient_data"):
        return "MISSING"
    return "NEUTRAL"


def _ema_stack_text(fast: Any, slow: Any, fast_p: Any = 20, slow_p: Any = 50) -> str:
    try:
        f = float(fast); s = float(slow); fp = int(fast_p or 20); sp = int(slow_p or 50)
    except Exception:
        return "EMA n/a"
    return f"EMA{fp}>EMA{sp}" if f > s else f"EMA{fp}<EMA{sp}" if f < s else f"EMA{fp}=EMA{sp}"


def _close_vs_ema_text(close: Any, fast: Any, slow: Any, fast_p: Any = 20, slow_p: Any = 50) -> str:
    try:
        c = float(close); f = float(fast); s = float(slow); fp = int(fast_p or 20); sp = int(slow_p or 50)
    except Exception:
        return "Close n/a"
    if c > f:
        return f"Close>EMA{fp}"
    if c < s:
        return f"Close<EMA{sp}"
    return f"Close between EMA{fp}/{sp}"


def _rsi_text(h1: Dict[str, Any]) -> str:
    rsi = _get_any(h1, "h1_rsi", "h1_rsi14", "rsi14", default=None)
    label = str(_get_any(h1, "h1_rsi_label", "h1_rsi_signal", default="") or "").replace("_", " ")
    try:
        return f"RSI {float(rsi):.1f}" + (f" {label}" if label else "")
    except Exception:
        return "RSI n/a"


def _build_h1_summary(h1: Dict[str, Any]) -> str:
    h1 = h1 if isinstance(h1, dict) else {}
    align = _h1_alignment_compact(h1.get("h1_ema_alignment") or h1.get("h1_bias_alignment") or h1.get("alignment"))
    icon = "✅" if align == "WITH" else "⚠️" if align == "AGAINST" else "➖" if align == "NEUTRAL" else "…"
    fast = _get_any(h1, "h1_fast_ema", "h1_ema20", "fast_ema", "ema20", default=None)
    slow = _get_any(h1, "h1_slow_ema", "h1_ema50", "slow_ema", "ema50", default=None)
    close = _get_any(h1, "h1_last_close", "last_close", "close", default=None)
    fast_p = _get_any(h1, "h1_fast_ema_period", "fast_period", default=20)
    slow_p = _get_any(h1, "h1_slow_ema_period", "slow_period", default=50)
    direction = str(h1.get("h1_ema_direction") or h1.get("trend_bias") or h1.get("h1_ema_bias") or "").replace("_", " ").strip()
    dir_txt = f" · {direction}" if direction and direction not in ("neutral bias", "neutral") else ""
    return f"{icon} H1 {align}{dir_txt} · {_ema_stack_text(fast, slow, fast_p, slow_p)} · {_close_vs_ema_text(close, fast, slow, fast_p, slow_p)} · {_rsi_text(h1)}"


def _h1_adjustment_text(adj: Any) -> str:
    try: value = float(adj or 0.0)
    except Exception: value = 0.0
    if value > 0: return f"boost {value:+.2f}"
    if value < 0: return f"drag {value:+.2f}"
    return "no change"



def apply_h1_momentum_context(pair: str, signal: Dict[str, Any], ema_state: Dict[str, Any]) -> Dict[str, Any]:
    """Attach concise H1 context and adjust confidence slightly."""
    signal = dict(signal or {})
    h1 = classify_h1_movement(pair, ema_state, signal.get("direction", "neutral"))
    for key in ("h1_ema_alignment", "h1_ema_direction", "pivot_direction", "h1_ema_conviction_mult", "h1_rsi", "h1_rsi_label", "h1_rsi_signal"):
        if signal.get(key) not in (None, ""):
            h1[key] = signal.get(key)
    if isinstance(signal.get("h1_ema"), dict):
        src = signal["h1_ema"]
        h1.setdefault("fast_ema", _get_any(src, "fast_ema", "ema20", default=None))
        h1.setdefault("slow_ema", _get_any(src, "slow_ema", "ema50", default=None))
        h1.setdefault("last_close", _get_any(src, "last_close", default=None))
        h1.setdefault("trend_bias", _get_any(src, "trend_bias", default=""))
        h1.setdefault("h1_rsi", _get_any(src, "h1_rsi", "h1_rsi14", "rsi14", default=None))
        h1.setdefault("h1_rsi_label", _get_any(src, "h1_rsi_label", default=""))
        h1.setdefault("h1_rsi_signal", _get_any(src, "h1_rsi_signal", default=""))
    signal.update(h1)
    signal["h1_summary"] = _build_h1_summary(signal)
    signal["h1_adjustment_text"] = _h1_adjustment_text(signal.get("h1_confidence_adjustment", 0.0))
    if signal.get("ok") and h1.get("h1_status") == "ok":
        before = float(signal.get("confidence", 0.0) or 0.0)
        adj = float(h1.get("h1_confidence_adjustment", 0.0) or 0.0)
        signal["confidence_before_h1"] = round(before, 3)
        signal["confidence"] = round(_sc_clamp(before + adj), 3)
    return signal

# Batch helper
# ---------------------------------------------------------------------------



def _sc_extract_h1_ema_states(payload: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """Return pair -> H1 EMA state from either a direct EMA dict or full macro dict.

    New macro_components.json exposes h1_ema_20_50_state and keeps
    ema_20_50_state as a backward-compatible H1 alias.  Older callers still
    pass the direct ema_20_50_state dict.  This helper supports both.
    """
    if not isinstance(payload, dict):
        return {}

    # Prefer explicit H1 block from the new macro.py.  Fall back to legacy alias.
    if isinstance(payload.get("h1_ema_20_50_state"), dict):
        source = payload.get("h1_ema_20_50_state") or {}
    elif isinstance(payload.get("ema_20_50_state"), dict):
        source = payload.get("ema_20_50_state") or {}
    else:
        # Direct dict case: {"eurusd": {...}, "gbpusd": {...}}
        source = payload

    out: Dict[str, Dict[str, Any]] = {}
    for k, v in source.items():
        if not isinstance(v, dict):
            continue
        pair_key = _norm_pair(v.get("pair") or k)
        if not pair_key:
            continue
        vv = dict(v)
        vv.setdefault("pair", pair_key)
        vv.setdefault("timeframe", "H1")
        out[pair_key] = vv
    return out


def _sc_extract_seeded_daily_indicators(payload: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """Return pair -> daily indicator fallback from old scraper price_indicators."""
    if not isinstance(payload, dict):
        return {}
    raw = payload.get("price_indicators") or payload.get("price_daily_indicators") or {}
    out: Dict[str, Dict[str, Any]] = {}
    if isinstance(raw, dict):
        for k, v in raw.items():
            if isinstance(v, dict):
                out[_norm_pair(k)] = v
    return out


def _sc_merge_technical_context(pair: str, pivot_result: Dict[str, Any], macro_payload: Dict[str, Any]) -> Dict[str, Any]:
    """Merge new macro technical_context[pair] into the pivot result.

    This lets signal_confirm.py benefit from macro_fixed.py without requiring
    every caller to reshape payloads.  D1 pivot/RSI remains in the normal
    pivot_result fields, while H1 context is attached as h1_ema/h1_rsi fields.
    """
    pair_key = _norm_pair(pair)
    pr = dict(pivot_result or {}) if isinstance(pivot_result, dict) else {}
    if not isinstance(macro_payload, dict):
        return pr

    tech = (macro_payload.get("technical_context") or {}).get(pair_key)
    if not isinstance(tech, dict):
        return pr

    daily = tech.get("daily") if isinstance(tech.get("daily"), dict) else {}
    h1 = tech.get("h1") if isinstance(tech.get("h1"), dict) else {}
    d1_rsi = daily.get("rsi14") if isinstance(daily.get("rsi14"), dict) else {}
    h1_rsi = h1.get("rsi14") if isinstance(h1.get("rsi14"), dict) else {}
    h1_ema = h1.get("ema_20_50") if isinstance(h1.get("ema_20_50"), dict) else {}

    # Fill missing D1 pivot/RSI fields from technical_context.daily.
    pivots = daily.get("pivots") if isinstance(daily.get("pivots"), dict) else {}
    for level in ("PP", "R1", "R2", "R3", "S1", "S2", "S3"):
        if pr.get(level) in (None, "", 0, 0.0) and pivots.get(level) is not None:
            pr[level] = pivots.get(level)
    if pr.get("price_state") in (None, "", "unavailable") and daily.get("price_state"):
        pr["price_state"] = daily.get("price_state")
    if not isinstance(pr.get("nearest_level"), dict) and isinstance(daily.get("nearest_level"), dict):
        pr["nearest_level"] = daily.get("nearest_level")
    if pr.get("rsi") in (None, "") and d1_rsi.get("value") is not None:
        pr["rsi"] = d1_rsi.get("value")
    pr.setdefault("rsi_state", d1_rsi.get("state", ""))
    pr.setdefault("rsi_bias", d1_rsi.get("bias", ""))
    pr.setdefault("rsi_alignment", d1_rsi.get("alignment", ""))

    # Attach compact H1 fields expected by existing alert text helpers.
    if h1_ema:
        pr["h1_ema"] = h1_ema
    if h1_rsi.get("value") is not None:
        pr.setdefault("h1_rsi", h1_rsi.get("value"))
        pr.setdefault("h1_rsi_label", h1_rsi.get("state", ""))
        pr.setdefault("h1_rsi_signal", h1_rsi.get("bias", ""))
    if isinstance(tech.get("summary"), dict):
        pr["technical_summary"] = tech.get("summary")
    pr["technical_context"] = tech
    return pr

def batch_combine(
    pivot_results: Dict[str, Dict[str, Any]],
    ema_states: Dict[str, Dict[str, Any]],
) -> Dict[str, Dict[str, Any]]:
    """
    Run combine_pivot_ema_signal for every pair present in pivot_results.

    Compatible with both old and new payloads:
      - old: ema_states is direct ema_20_50_state
      - new: ema_states is full macro_components.json with
        h1_ema_20_50_state / daily_rsi_state / technical_context

    The signal decision still uses the H1 EMA 20/50 stack by default, because
    macro_fixed.py keeps ema_20_50_state as the H1 alias.  D1 RSI/pivots and
    technical summaries are attached for richer reasons/Telegram text.
    """
    out: Dict[str, Dict[str, Any]] = {}
    macro_payload = ema_states if isinstance(ema_states, dict) else {}
    ema_lookup = _sc_extract_h1_ema_states(macro_payload)
    seeded_indicator_lookup = _sc_extract_seeded_daily_indicators(macro_payload)

    for pair, pr in (pivot_results or {}).items():
        pair_key = _norm_pair(pair)
        merged_pr = _sc_merge_technical_context(pair_key, pr if isinstance(pr, dict) else {}, macro_payload)
        ema = ema_lookup.get(pair_key, {})

        if (not ema or ema.get("warming_up") or not ema.get("ok")) and pair_key in seeded_indicator_lookup:
            seeded = _seeded_indicator_to_ema_state(pair_key, seeded_indicator_lookup[pair_key], merged_pr)
            if seeded:
                ema = seeded

        sig = combine_pivot_ema_signal(merged_pr, ema)
        out[pair_key] = apply_h1_momentum_context(pair_key, sig, ema)
    return out

# ---------------------------------------------------------------------------
# Telegram dispatcher
# ---------------------------------------------------------------------------
# Reuses pivot.py's _pivot_send_telegram() and de-dup state file pattern.
# Controlled entirely by environment variables — zero code changes needed
# to enable/disable per deployment.
#
# Environment variables
# ---------------------
# TELEGRAM_BOT_TOKEN          Telegram bot token. Leave unset to disable.
#                             Shared with momentum.py, ema.py and pivot.py.
# TELEGRAM_CHAT_ID            Target chat/channel ID (integer). Required when
#                             alerts are enabled.
# SIGNAL_ALERT_STATE_FILE     Path to de-dup state JSON.
#                             Default: public/signal_telegram_alerts.json
# SIGNAL_ALERT_MIN_CONFIDENCE Minimum confidence (0.0–1.0) to fire an alert.
#                             Default: 0.65. Raise to reduce noise.
# SIGNAL_ALERT_PRUNE_H        Hours after which a de-dup entry expires.
#                             Default: 26 (one trading day).
# SIGNAL_ALERT_SIGNALS        Comma-separated list of signal values that will
#                             trigger an alert. Default fires on confirmed and
#                             high-conviction only (ignores conflicted/weak).
#                             Example: "confirmed_long,confirmed_short,
#                                       high_conviction_long,high_conviction_short"
# SIGNAL_USE_H1_MOMENTUM      true/false. If true, reads momentum_closes.json
#                             and adds H1 with/against Daily EMA bias context.
# SIGNAL_H1_CLOSES_FILE       Optional explicit path to momentum_closes.json.
# SIGNAL_H1_WITH_BIAS_BONUS   Confidence bonus when H1 agrees with Daily bias.
# SIGNAL_H1_AGAINST_BIAS_PENALTY Confidence penalty when H1 moves against bias.
# ---------------------------------------------------------------------------

import os as _os
import json as _json
import logging as _logging
import html as _html
from datetime import datetime as _datetime, timedelta as _timedelta, timezone as _tz
from pathlib import Path as _Path

_log = _logging.getLogger(__name__)
_UTC = _tz.utc
_MYT = _tz(offset=_timedelta(hours=8), name="MYT")   # Malaysia Time — UTC+8, no DST

# ---- config ----------------------------------------------------------------

SIGNAL_BOT_TOKEN: str = _os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
_signal_chat_id_raw: str = _os.environ.get("TELEGRAM_CHAT_ID", "").strip()
SIGNAL_CHAT_ID: int = 0   # resolved lazily

_DEFAULT_ALERT_STATE_FILE = _Path(
    _os.environ.get("SIGNAL_ALERT_STATE_FILE", "public/signal_telegram_alerts.json")
)
SIGNAL_ALERT_MIN_CONFIDENCE: float = float(
    _os.environ.get("SIGNAL_ALERT_MIN_CONFIDENCE", "0.65")
)
SIGNAL_ALERT_PRUNE_H: int = max(
    1, int(float(_os.environ.get("SIGNAL_ALERT_PRUNE_H", "26")))
)

_DEFAULT_ALERT_SIGNALS = {
    SIGNAL_CONFIRMED_LONG,
    SIGNAL_CONFIRMED_SHORT,
    SIGNAL_HIGH_CONVICTION_LONG,
    SIGNAL_HIGH_CONVICTION_SHORT,
}
_env_signals_raw = _os.environ.get("SIGNAL_ALERT_SIGNALS", "").strip()
SIGNAL_ALERT_SIGNALS: set = (
    {s.strip() for s in _env_signals_raw.split(",") if s.strip()}
    if _env_signals_raw
    else _DEFAULT_ALERT_SIGNALS
)

SIGNAL_USE_H1_MOMENTUM: bool = _os.environ.get("SIGNAL_USE_H1_MOMENTUM", "true").strip().lower() not in {"0", "false", "no", "off"}
SIGNAL_H1_CLOSES_FILE: str = _os.environ.get("SIGNAL_H1_CLOSES_FILE", "").strip()
SIGNAL_H1_FAST_EMA: int = max(2, int(float(_os.environ.get("SIGNAL_H1_FAST_EMA", "20"))))
SIGNAL_H1_SLOW_EMA: int = max(SIGNAL_H1_FAST_EMA + 1, int(float(_os.environ.get("SIGNAL_H1_SLOW_EMA", "50"))))
SIGNAL_H1_MIN_BARS: int = max(7, SIGNAL_H1_SLOW_EMA, int(float(_os.environ.get("SIGNAL_H1_MIN_BARS", "50"))))
SIGNAL_H1_WITH_BIAS_BONUS: float = float(_os.environ.get("SIGNAL_H1_WITH_BIAS_BONUS", "0.05"))
SIGNAL_H1_AGAINST_BIAS_PENALTY: float = float(_os.environ.get("SIGNAL_H1_AGAINST_BIAS_PENALTY", "0.08"))

# Session News/Event Calendar alerts — sent once per MYT day at FX session start.
SIGNAL_CALENDAR_ALERTS_ENABLED: bool = _os.environ.get("SIGNAL_CALENDAR_ALERTS_ENABLED", "true").strip().lower() not in {"0", "false", "no", "off"}
SIGNAL_CALENDAR_JSON: str = _os.environ.get("SIGNAL_CALENDAR_JSON", "").strip()
SIGNAL_CALENDAR_SESSION_WINDOW_MIN: int = max(1, int(float(_os.environ.get("SIGNAL_CALENDAR_SESSION_WINDOW_MIN", "45"))))
SIGNAL_CALENDAR_MAX_EVENTS: int = max(1, int(float(_os.environ.get("SIGNAL_CALENDAR_MAX_EVENTS", "14"))))
SIGNAL_CALENDAR_CURRENCIES: set = {c.strip().upper() for c in _os.environ.get("SIGNAL_CALENDAR_CURRENCIES", "EUR,GBP,USD,JPY").split(",") if c.strip()}
SIGNAL_CALENDAR_SESSIONS_RAW: str = _os.environ.get("SIGNAL_CALENDAR_SESSIONS_MYT", "Sydney=06:00,Tokyo=08:00,London=15:00,New York=20:00").strip()
SIGNAL_CALENDAR_GROQ_ENABLED: bool = _os.environ.get("SIGNAL_CALENDAR_GROQ_ENABLED", "true").strip().lower() not in {"0", "false", "no", "off"}
# Impact filter: "high" → only 🔴 events; "medium" → 🔴+🟠; "" / "all" → everything.
# When the session window opens but no events survive the filter, the alert is
# silently skipped (no "no events" message sent) unless SIGNAL_CALENDAR_SKIP_IF_NO_EVENTS=false.
_SIGNAL_CALENDAR_MIN_IMPACT_RAW: str = _os.environ.get("SIGNAL_CALENDAR_MIN_IMPACT", "").strip().lower()
SIGNAL_CALENDAR_MIN_IMPACT: str = _SIGNAL_CALENDAR_MIN_IMPACT_RAW if _SIGNAL_CALENDAR_MIN_IMPACT_RAW in {"high", "medium"} else ""
SIGNAL_CALENDAR_SKIP_IF_NO_EVENTS: bool = _os.environ.get("SIGNAL_CALENDAR_SKIP_IF_NO_EVENTS", "true").strip().lower() not in {"0", "false", "no", "off"}

# ---------------------------------------------------------------------------
# Groq AI — optional enrichment for signal Telegram alerts
# Fail-soft: if AI credentials/API/network fail, the alert still sends.
# Shares groq_ai_state.json with pivot.py and momentum.py.
# ---------------------------------------------------------------------------
_GROQ_AI_ENABLED: bool = _os.environ.get("GROQ_AI_ENABLED", "0").strip().lower() in ("1", "true", "yes", "on")
_GROQ_API_KEY: str = _os.environ.get("GROQ_API_KEY", "").strip()
_GROQ_AI_MODEL: str = _os.environ.get("GROQ_AI_MODEL", "llama-3.1-8b-instant").strip()
_GROQ_AI_TIMEOUT: int = int(_os.environ.get("GROQ_AI_TIMEOUT", "12"))
_GROQ_AI_MAX_CHARS: int = int(_os.environ.get("GROQ_AI_MAX_CHARS", "650"))
_GROQ_AI_MAX_PER_RUN: int = max(1, int(_os.environ.get("GROQ_AI_MAX_PER_RUN", "8")))
_GROQ_AI_COOLDOWN_MINUTES: int = max(1, int(_os.environ.get("GROQ_AI_COOLDOWN_MINUTES", "30")))
_groq_ai_calls_this_run: int = 0  # module-level per-run counter
_GROQ_AI_STATE_FILE: _Path = _Path(
    _os.environ.get(
        "GROQ_AI_STATE_FILE",
        str(_Path(_os.environ.get("SIGNAL_ALERT_STATE_FILE", "public/signal_telegram_alerts.json")).parent / "groq_ai_state.json"),
    )
)

# ---- emoji / label maps ----------------------------------------------------

_SIGNAL_EMOJI: Dict[str, str] = {
    SIGNAL_HIGH_CONVICTION_LONG:  "🚀",
    SIGNAL_HIGH_CONVICTION_SHORT: "💥",
    SIGNAL_CONFIRMED_LONG:        "✅",
    SIGNAL_CONFIRMED_SHORT:       "🔴",
    SIGNAL_CONFLICTED:            "⚠️",
    SIGNAL_WEAK:                  "〰️",
    SIGNAL_UNCONFIRMED:           "⏳",
    SIGNAL_UNAVAILABLE:           "❓",
}

_SIGNAL_LABEL: Dict[str, str] = {
    SIGNAL_HIGH_CONVICTION_LONG:  "High Conviction Long",
    SIGNAL_HIGH_CONVICTION_SHORT: "High Conviction Short",
    SIGNAL_CONFIRMED_LONG:        "Confirmed Long",
    SIGNAL_CONFIRMED_SHORT:       "Confirmed Short",
    SIGNAL_CONFLICTED:            "Conflicted",
    SIGNAL_WEAK:                  "Weak",
    SIGNAL_UNCONFIRMED:           "Unconfirmed (EMA Warming Up)",
    SIGNAL_UNAVAILABLE:           "Unavailable",
}

_BIAS_TAG: Dict[str, str] = {
    "bullish_bias": "📈 Bullish EMA",
    "bearish_bias": "📉 Bearish EMA",
    "neutral_bias": "➖ Neutral EMA",
}

_ALIGN_TAG: Dict[str, str] = {
    "with":        "✅ Macro: With",
    "against":     "⚠️ Macro: Against",
    "neutral":     "➖ Macro: Neutral",
    "unavailable": "❓ Macro: N/A",
}

_CROSS_TAG: Dict[str, str] = {
    "golden_cross": "⚡ Golden Cross",
    "death_cross":  "⚡ Death Cross",
}

# ---- helpers ----------------------------------------------------------------

def _sc_resolve_chat_id(raw: str) -> int:
    if not raw:
        raise RuntimeError(
            "TELEGRAM_CHAT_ID is required when signal alerts are enabled."
        )
    try:
        return int(raw)
    except ValueError:
        raise RuntimeError(
            f"TELEGRAM_CHAT_ID must be an integer, got: {raw!r}"
        )

try:
    SIGNAL_CHAT_ID = _sc_resolve_chat_id(_signal_chat_id_raw) if _signal_chat_id_raw else 0
except RuntimeError:
    SIGNAL_CHAT_ID = 0


def _sc_fmt_price(pair: str, value: float) -> str:
    p = pair.lower()
    if "jpy" in p:
        return f"{value:.3f}"
    if "xau" in p:
        return f"{value:.2f}"
    return f"{value:.5f}"


def _sc_now() -> _datetime:
    return _datetime.now(_UTC)


def _sc_iso(dt: _datetime) -> str:
    return dt.astimezone(_UTC).isoformat().replace("+00:00", "Z")


def _sc_parse_dt(value: Any) -> Optional[_datetime]:
    if not value:
        return None
    if isinstance(value, _datetime):
        return value if value.tzinfo else value.replace(tzinfo=_UTC)
    try:
        text = str(value).strip().replace("Z", "+00:00")
        dt = _datetime.fromisoformat(text)
        return dt if dt.tzinfo else dt.replace(tzinfo=_UTC)
    except (ValueError, TypeError):
        return None


# ---- de-dup state ----------------------------------------------------------


def _sc_escape(text: Any) -> str:
    """Escape dynamic text for Telegram HTML payloads."""
    return _html.escape("" if text is None else str(text), quote=False)

def _sc_load_state(state_file: _Path) -> Dict[str, str]:
    try:
        with open(state_file, encoding="utf-8") as f:
            data = _json.load(f)
        return data if isinstance(data, dict) else {}
    except FileNotFoundError:
        return {}
    except (_json.JSONDecodeError, OSError, ValueError) as exc:
        _log.warning("signal alerts: failed to load state file: %s", exc)
        return {}


def _sc_save_state(state_file: _Path, state: Dict[str, str]) -> None:
    tmp = state_file.with_suffix(".tmp")
    try:
        state_file.parent.mkdir(parents=True, exist_ok=True)
        with open(tmp, "w", encoding="utf-8") as f:
            _json.dump(state, f, indent=2)
            f.flush()
            _os.fsync(f.fileno())
        _os.replace(tmp, state_file)
    except OSError as exc:
        _log.warning("signal alerts: failed to save state file: %s", exc)
        try:
            tmp.unlink(missing_ok=True)
        except OSError:
            pass


def _sc_prune_state(state: Dict[str, str], prune_h: int) -> Dict[str, str]:
    cutoff = _sc_now() - _timedelta(hours=prune_h)
    return {
        k: v for k, v in state.items()
        if (ts := _sc_parse_dt(v)) is not None and ts >= cutoff
    }


# ---- message builder -------------------------------------------------------

def _sc_confidence_bar(confidence: float, width: int = 10) -> str:
    """Render confidence as a filled bar, e.g. '████████░░ 80%'."""
    filled = round(confidence * width)
    return "█" * filled + "░" * (width - filled) + f" {confidence:.0%}"


def _sc_macro_label(mscore: float) -> str:
    """Convert macro_score to a concise human label with directional emoji."""
    if mscore >= 0.5:
        return f"⬆️  Strongly Supportive ({mscore:+.3f})"
    if mscore >= 0.2:
        return f"↗️  Moderately Supportive ({mscore:+.3f})"
    if mscore >= -0.2:
        return f"➡️  Neutral ({mscore:+.3f})"
    if mscore >= -0.5:
        return f"↘️  Moderately Opposing ({mscore:+.3f})"
    return f"⬇️  Strongly Opposing ({mscore:+.3f})"


# Maps ema.py suggestion strings → plain-English trader label + action note.
_SC_SUGGESTION_LABEL: Dict[str, tuple[str, str]] = {
    # ── ema.py suggestion strings ────────────────────────────────────────────
    "bullish_cross":    ("Bullish Cross",    "Momentum Turning Up — Consider Long Entries"),
    "bearish_cross":    ("Bearish Cross",    "Momentum Turning Down — Consider Short Entries"),
    "bullish":          ("Bullish Stack",    "Price Above EMA20 & EMA50 — Trend Intact"),
    "bearish":          ("Bearish Stack",    "Price Below EMA20 & EMA50 — Trend Intact"),
    "bullish_pullback": ("Bullish Pullback", "Close < EMA20, > EMA50 — Watch For Daily Reclaim"),
    "bearish_pullback": ("Bearish Pullback", "Close > EMA20, < EMA50 — Watch For Daily Failure"),
    "bullish_at_ema":   ("Testing Support",  "Price At EMA In Bullish Stack — Potential Bounce"),
    "bearish_at_ema":   ("Testing Resistance","Price At EMA In Bearish Stack — Potential Rejection"),
    "neutral":          ("Neutral / Coiling","EMAs Converging — No Directional Edge Yet"),
    "warming_up":       ("Warming Up",       "Insufficient History — Signal Pending"),
    # ── momentum.py suggestion strings ─────────────────────────────────────
    # momentum.py uses different labels for the same underlying market conditions.
    "watch_long":       ("Watch Long",       "Bullish EMA Cross Forming — Await Daily Confirmation"),
    "watch_short":      ("Watch Short",      "Bearish EMA Cross Forming — Await Daily Confirmation"),
    "bullish_aligned":  ("Bullish Aligned",  "Price Above EMA20 & EMA50 — Daily Trend Intact"),
    "bearish_aligned":  ("Bearish Aligned",  "Price Below EMA20 & EMA50 — Daily Trend Intact"),
    # current ema.py cross/suggestion variants
    "cross_up":         ("Golden Cross",      "EMA20 Crossed Above EMA50 — Momentum Turning Up"),
    "cross_down":       ("Death Cross",       "EMA20 Crossed Below EMA50 — Momentum Turning Down"),
    "golden_cross":     ("Golden Cross",      "EMA20 Above EMA50 — Bullish Momentum Confirmation"),
    "death_cross":      ("Death Cross",       "EMA20 Below EMA50 — Bearish Momentum Confirmation"),
    "no_cross":         ("No Fresh Cross",    "Trend Bias Depends On EMA Stack And Price Location"),
}


def _sc_price_ladder(
    pair: str,
    price: float,
    levels: Dict[str, float],
) -> str:
    """
    Build a visual price ladder showing R3 → R2 → R1 → PP → S1 → S2 → S3 with
    a '►' price marker inserted at the correct position.

    Returns a multi-line string for wrapping in <code>...</code>.
    """
    fp    = _sc_fmt_price
    named = []
    if levels.get("r3"):
        named.append(("R3", levels["r3"]))
    named += [
        ("R2", levels["r2"]),
        ("R1", levels["r1"]),
        ("PP", levels["pp"]),
        ("S1", levels["s1"]),
        ("S2", levels["s2"]),
    ]
    if levels.get("s3"):
        named.append(("S3", levels["s3"]))
    price_inserted = False
    rows: list[str] = []
    for lbl, val in named:
        if not price_inserted and price >= val:
            rows.append(f"  ►  {fp(pair, price):<13} ← Live Price")
            price_inserted = True
        rows.append(f"  {lbl:<3} {fp(pair, val)}")
    if not price_inserted:
        rows.append(f"  ►  {fp(pair, price):<13} ← Live Price")
    return "\n".join(rows)


def _sc_ema_panel(
    pair: str,
    ema_result: Dict[str, Any],
) -> str:
    """
    Build the EMA panel block for embedding in a signal alert.

    Accepts either a plain dict (from ema_states) or one produced by
    dataclasses.asdict(EMAAnalysisResult).  All fields are accessed via
    .get() so missing keys degrade gracefully.

    Returns a formatted string ready to be appended to the message body.
    The caller is responsible for surrounding dividers.
    """
    fp = _sc_fmt_price

    ema_f_val   = _get_any(ema_result, "ema_fast", "ema20", "ema_20")
    ema_s_val   = _get_any(ema_result, "ema_slow", "ema50", "ema_50")
    fast_p      = int(_get_any(ema_result, "fast_period", default=20))
    slow_p      = int(_get_any(ema_result, "slow_period", default=50))
    trend_bias  = _get_any(ema_result, "trend_bias", "bias", default="")
    suggestion  = _get_any(ema_result, "suggestion", "ema_cross_signal", "cross", default="")
    cross_sig   = _ema_cross_value(ema_result)
    last_close  = _get_any(ema_result, "last_close", "close", "current_price")
    close_str   = _get_any(ema_result, "close_structure", default="")
    cur_vs_fast = _get_any(ema_result, "current_vs_fast", "price_vs_fast", "close_vs_fast", default="")
    cur_vs_slow = _get_any(ema_result, "current_vs_slow", "price_vs_slow", "close_vs_slow", default="")
    timeframe   = _get_any(ema_result, "timeframe", default="Daily")
    completed_closes = _get_any(ema_result, "completed_closes", "completed_daily_closes", "bars", default=None)
    ema_source  = _get_any(ema_result, "source", "data_source", "history_source", default="")

    # Stack direction arrow
    fvs = _get_any(ema_result, "ema_fast_vs_slow", "fast_vs_slow", default="")
    stack_arrow = "▲" if fvs == "above" else ("▼" if fvs == "below" else "↔")

    # EMA value lines — skip if not computed yet
    ema_lines = ""
    if ema_f_val is not None and ema_s_val is not None:
        ema_lines = (
            f"  EMA{fast_p:<3} {fp(pair, ema_f_val)}\n"
            f"  EMA{slow_p:<3} {fp(pair, ema_s_val)}\n"
        )

    # Cross badge
    cross_tag = ""
    if cross_sig == "golden_cross":
        cross_tag = "  ⚡ Golden Cross"
    elif cross_sig == "death_cross":
        cross_tag = "  ⚡ Death Cross"

    # Daily close line
    _CLOSE_STR_LABEL = {
        # canonical keys (produced by current ema.py)
        "above_fast_slow":         "Above_Ema20_Ema50",
        "below_fast_slow":         "Below_Ema20_Ema50",
        "above_fast_below_slow":   "Above_Ema20_Below_Ema50",
        "below_fast_above_slow":   "Below_Ema20_Above_Ema50",
        # canonical keys from EMA20/50
        # forward-compatible new literal keys
        "above_ema20_ema50":       "Above_Ema20_Ema50",
        "below_ema20_ema50":       "Below_Ema20_Ema50",
        "above_ema50_below_ema20": "Above_Ema50_Below_Ema20",
        "below_ema50_above_ema20": "Below_Ema50_Above_Ema20",
    }
    close_line = ""
    if last_close is not None:
        close_lbl = _CLOSE_STR_LABEL.get(close_str, close_str)
        close_line = f"  Close  {fp(pair, last_close)}  ({close_lbl})\n" if close_str else f"  Close  {fp(pair, last_close)}\n"

    # Live price position relative to EMAs
    pos_parts = []
    if cur_vs_fast:
        pos_parts.append(f"{cur_vs_fast.title()} EMA{fast_p}")
    if cur_vs_slow:
        pos_parts.append(f"{cur_vs_slow.title()} EMA{slow_p}")
    pos_line = f"  Price  {'  ·  '.join(pos_parts)}\n" if pos_parts else ""

    # Suggestion — human label + action note
    sugg_label, sugg_note = _SC_SUGGESTION_LABEL.get(
        suggestion, (suggestion, "")
    )
    sugg_line = f"  Signal  <b>{_sc_escape(sugg_label)}</b>"
    if sugg_note:
        sugg_line += f"\n          <i>{_sc_escape(sugg_note)}</i>"

    # Bars count for transparency
    bars_note = f"  ({completed_closes} completed {timeframe} bars)" if completed_closes else ""
    source_note = f"  Source  {_sc_escape(ema_source)}\n" if ema_source else ""

    # Trend bias line — uses the same _BIAS_TAG lookup as the badges row
    bias_line = ""
    if trend_bias:
        bias_line = f"  Bias   {_BIAS_TAG.get(trend_bias, trend_bias)}\n"

    panel = (
        f"<b>Daily EMA  ({fast_p} / {slow_p})</b>{bars_note}\n"
        f"<code>"
        f"  Stack  EMA{fast_p} {stack_arrow} EMA{slow_p}{cross_tag}\n"
        f"{ema_lines}"
        f"{close_line}"
        f"{pos_line}"
        f"{bias_line}"
        f"{source_note}"
        f"</code>"
        f"{sugg_line}"
    )
    return panel


# ---------------------------------------------------------------------------
# Groq AI helpers
# ---------------------------------------------------------------------------

def _groq_sc_load_state() -> Dict[str, Any]:
    try:
        with open(_GROQ_AI_STATE_FILE, encoding="utf-8") as _f:
            return _json.load(_f)
    except Exception:
        return {}


def _groq_sc_save_state(data: Dict[str, Any]) -> None:
    try:
        _GROQ_AI_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        tmp = _GROQ_AI_STATE_FILE.with_suffix(".tmp")
        with open(tmp, "w", encoding="utf-8") as _f:
            _json.dump(data, _f)
            _f.flush()
            import os as _os2; _os2.fsync(_f.fileno())
        import os as _os3; _os3.replace(tmp, _GROQ_AI_STATE_FILE)
    except Exception as _exc:
        _log.warning("signal alerts: could not save Groq AI state file: %s", _exc)


def _groq_sc_can_call() -> bool:
    """Return True if a Groq AI call is allowed (per-run cap + cooldown check)."""
    if _groq_ai_calls_this_run >= _GROQ_AI_MAX_PER_RUN:
        _log.debug(
            "signal alerts: Groq AI per-run cap reached (%d/%d) — skipping",
            _groq_ai_calls_this_run, _GROQ_AI_MAX_PER_RUN,
        )
        return False
    state = _groq_sc_load_state()
    cooldown_until = state.get("cooldown_until")
    if cooldown_until:
        try:
            _cu = _datetime.fromisoformat(cooldown_until.replace("Z", "+00:00"))
            if _datetime.now(_UTC) < _cu:
                _log.warning("signal alerts: Groq AI cooldown active until %s — skipping", cooldown_until)
                return False
        except Exception:
            pass
    return True


def _groq_sc_mark_429() -> None:
    """Set a cooldown after receiving a 429 from Groq AI."""
    until = (_datetime.now(_UTC) + _timedelta(minutes=_GROQ_AI_COOLDOWN_MINUTES)).strftime("%Y-%m-%dT%H:%M:%SZ")
    _groq_sc_save_state({"cooldown_until": until})
    _log.warning("signal alerts: Groq AI 429 — cooldown set for %d min (until %s)", _GROQ_AI_COOLDOWN_MINUTES, until)


def _groq_sc_mark_call() -> None:
    global _groq_ai_calls_this_run
    _groq_ai_calls_this_run += 1


# Dedicated session singleton — does NOT retry on 429.
_groq_sc_session_instance = None

def _groq_sc_session():
    global _groq_sc_session_instance
    if _groq_sc_session_instance is None:
        import requests as _req
        from requests.adapters import HTTPAdapter as _HTTPAdapter
        from urllib3.util.retry import Retry as _Retry
        s = _req.Session()
        s.headers.update({"User-Agent": "Mozilla/5.0"})
        s.mount(
            "https://",
            _HTTPAdapter(
                max_retries=_Retry(
                    total=1,
                    backoff_factor=0.5,
                    status_forcelist=[500, 502, 503, 504],
                    allowed_methods=frozenset({"POST"}),
                )
            ),
        )
        _groq_sc_session_instance = s
    return _groq_sc_session_instance


def _groq_sc_prompt(pair: str, signal: Dict[str, Any], pivot_result: Dict[str, Any]) -> str:
    """Build a concise prompt summarising the confirmed signal for Groq."""
    sig_val    = signal.get("signal", "")
    direction  = signal.get("direction", "neutral")
    confidence = float(signal.get("confidence", 0.0))
    reason     = signal.get("reason", "")
    ema_bias   = signal.get("ema_bias", "")
    ema_cross  = signal.get("ema_cross", "none")
    macro_align = signal.get("macro_align", "neutral")
    pivot_state = signal.get("pivot_state", "")
    h1_align   = signal.get("h1_bias_alignment", "")
    h1_move    = signal.get("h1_movement", "")
    pp  = _as_float(_get_any(pivot_result, "PP", "pp", default=0.0))
    r1  = _as_float(_get_any(pivot_result, "R1", "r1", default=0.0))
    s1  = _as_float(_get_any(pivot_result, "S1", "s1", default=0.0))
    return "\n".join([
        "You are a concise FX signal assistant for Telegram alerts.",
        "Return 2 short bullets only. No trade command. No guarantee. No markdown table.",
        "Synthesise the confirmed signal: what the confluence means and what level to watch.",
        f"Pair: {pair.upper()}",
        f"Signal: {sig_val} ({direction})",
        f"Confidence: {confidence:.0%}",
        f"Trigger: {reason}",
        f"Daily EMA bias: {ema_bias}  EMA cross: {ema_cross}",
        f"Macro alignment: {macro_align}",
        f"Pivot state: {pivot_state}",
        f"H1 bias alignment: {h1_align}  H1 movement: {h1_move}",
        f"Key levels — PP: {pp}  R1: {r1}  S1: {s1}",
        "Style: punchy, trader-friendly, max 45 words total.",
    ])


def _groq_sc_note(pair: str, signal: Dict[str, Any], pivot_result: Dict[str, Any]) -> str:
    """Return a Groq AI enrichment note for a signal alert, or \'\' on any failure."""
    if not (_GROQ_AI_ENABLED and _GROQ_API_KEY and _GROQ_AI_MODEL):
        return ""
    if not _groq_sc_can_call():
        return ""
    url = "https://api.groq.com/openai/v1/chat/completions"
    payload = {
        "model": _GROQ_AI_MODEL,
        "messages": [{"role": "user", "content": _groq_sc_prompt(pair, signal, pivot_result)}],
        "max_tokens": 150,
    }
    headers = {"Authorization": f"Bearer {_GROQ_API_KEY}", "Content-Type": "application/json"}
    try:
        resp = _groq_sc_session().post(url, headers=headers, json=payload, timeout=_GROQ_AI_TIMEOUT)
        if resp.status_code == 429:
            _groq_sc_mark_429()
            return ""
        if resp.status_code != 200:
            _log.warning("signal alerts: Groq AI HTTP %s — %s", resp.status_code, resp.text[:200])
            return ""
        _groq_sc_mark_call()
        choices = resp.json().get("choices") or []
        ai_text = choices[0].get("message", {}).get("content", "") if choices else ""
        ai_text = " ".join(str(ai_text).strip().split())[:_GROQ_AI_MAX_CHARS]
        return ai_text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    except Exception as exc:
        _log.warning("signal alerts: Groq AI enrichment skipped: %s", exc)
        return ""



def _groq_sc_calendar_prompt(session_name: str, events: list, now_myt: _datetime) -> str:
    """Build a Groq prompt for session calendar commentary."""
    compact = []
    for ev in events[:SIGNAL_CALENDAR_MAX_EVENTS]:
        dt = ev.get("_dt")
        compact.append({
            "time_myt": dt.astimezone(_MYT).strftime("%H:%M") if isinstance(dt, _datetime) else "TBD",
            "currency": ev.get("currency") or ev.get("country") or "",
            "impact": ev.get("impact") or "",
            "title": ev.get("title") or "",
            "forecast": ev.get("forecast") or "",
            "previous": ev.get("previous") or "",
            "actual": ev.get("actual") or "",
        })
    return "\n".join([
        "You are a concise FX macro calendar assistant for Telegram alerts.",
        "Return exactly 2 short bullets only. No table. No trade command. No guarantee.",
        "Explain what today's listed events may mean for intraday FX/gold sentiment.",
        f"Session starting now: {session_name}",
        f"Date/time context: {now_myt.strftime('%Y-%m-%d %H:%M')} MYT",
        "Main watched markets: EURUSD, GBPUSD, USDJPY, XAUUSD.",
        "Events JSON:",
        _json.dumps(compact, ensure_ascii=False),
        "Style: punchy, trader-friendly, max 45 words total.",
    ])


def _groq_sc_calendar_note(session_name: str, events: list, now_myt: _datetime) -> str:
    """Return 2-bullet Groq AI commentary for a session calendar alert, or ''."""
    if not (SIGNAL_CALENDAR_GROQ_ENABLED and _GROQ_AI_ENABLED and _GROQ_API_KEY and _GROQ_AI_MODEL):
        return ""
    if not _groq_sc_can_call():
        return ""
    url = "https://api.groq.com/openai/v1/chat/completions"
    payload = {
        "model": _GROQ_AI_MODEL,
        "messages": [{"role": "user", "content": _groq_sc_calendar_prompt(session_name, events, now_myt)}],
        "max_tokens": 150,
    }
    headers = {"Authorization": f"Bearer {_GROQ_API_KEY}", "Content-Type": "application/json"}
    try:
        resp = _groq_sc_session().post(url, headers=headers, json=payload, timeout=_GROQ_AI_TIMEOUT)
        if resp.status_code == 429:
            _groq_sc_mark_429()
            return ""
        if resp.status_code != 200:
            _log.warning("session calendar: Groq AI HTTP %s — %s", resp.status_code, resp.text[:200])
            return ""
        _groq_sc_mark_call()
        choices = resp.json().get("choices") or []
        ai_text = choices[0].get("message", {}).get("content", "") if choices else ""
        ai_text = "\n".join(line.strip() for line in str(ai_text).strip().splitlines() if line.strip())
        ai_text = ai_text[:_GROQ_AI_MAX_CHARS]
        return ai_text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    except Exception as exc:
        _log.warning("session calendar: Groq AI commentary skipped: %s", exc)
        return ""

def build_signal_alert_text(
    pair: str,
    signal: Dict[str, Any],
    pivot_result: Dict[str, Any],
    ema_result: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Build a professional HTML Telegram message for one confirmed signal.

    Parameters
    ----------
    pair         : lowercase pair code e.g. "eurusd"
    signal       : output of combine_pivot_ema_signal()
    pivot_result : matching pivot.classify_price_structure() dict
    ema_result   : optional — dataclasses.asdict(EMAAnalysisResult) or plain
                   ema_states dict.  When supplied, adds a Daily EMA panel
                   between the pivot ladder and the context section.

    Message layout
    --------------
    HEADER      Signal tier · Pair · Direction
    ────────
    BADGES      Macro alignment │ EMA bias │ cross
    REASON      Plain-English trigger explanation
    CONFIDENCE  Visual bar + quality tier
    ────────
    PIVOT       Price ladder: R2 / R1 / ► price / PP / S1 / S2
    ────────
    Daily EMA   Stack direction, EMA values, close structure,
    (optional)  live price position, suggestion label + note
    ────────
    CONTEXT     Macro score, pivot state, cross
    FOOTER      Bar date · Generated timestamp
    """
    sig_val     = signal.get("signal", "")
    direction   = signal.get("direction", "neutral")
    reason      = _sc_escape(signal.get("reason", ""))
    confidence  = float(signal.get("confidence", 0.0))
    ema_bias    = signal.get("ema_bias", "neutral_bias")
    ema_cross   = signal.get("ema_cross", "none")
    macro_align = signal.get("macro_align", "neutral")
    pivot_state = _sc_escape(signal.get("pivot_state", ""))

    levels    = pivot_result.get("levels") if isinstance(pivot_result.get("levels"), dict) else {}
    price     = _as_float(_get_any(pivot_result, "price_used", "current_price", "last_price", "price", default=0.0))
    pp        = _as_float(_get_any(pivot_result, "PP", "pp", default=levels.get("pp", levels.get("PP", 0.0))))
    r1        = _as_float(_get_any(pivot_result, "R1", "r1", default=levels.get("r1", levels.get("R1", 0.0))))
    r2        = _as_float(_get_any(pivot_result, "R2", "r2", default=levels.get("r2", levels.get("R2", 0.0))))
    r3        = _as_float(_get_any(pivot_result, "R3", "r3", default=levels.get("r3", levels.get("R3", 0.0))))
    s1        = _as_float(_get_any(pivot_result, "S1", "s1", default=levels.get("s1", levels.get("S1", 0.0))))
    s2        = _as_float(_get_any(pivot_result, "S2", "s2", default=levels.get("s2", levels.get("S2", 0.0))))
    s3        = _as_float(_get_any(pivot_result, "S3", "s3", default=levels.get("s3", levels.get("S3", 0.0))))
    mscore    = _as_float(_get_any(pivot_result, "macro_score", default=0.0))
    ohlc_date = _sc_escape(_get_any(pivot_result, "ohlc_date", "date", default=""))
    quality   = _sc_escape(_get_any(pivot_result, "state_quality", "quality", default=""))
    pivot_src = _sc_escape(_get_any(pivot_result, "source", "data_source", "ohlc_source", default=""))

    emoji     = _SIGNAL_EMOJI.get(sig_val, "🔔")
    label     = _sc_escape(_SIGNAL_LABEL.get(sig_val, sig_val.upper()))
    dir_arrow = {"long": "▲ Long", "short": "▼ Short", "neutral": "◆ Neutral"}.get(direction, "◆")

    align_badge = _sc_escape(_ALIGN_TAG.get(macro_align, macro_align))
    bias_badge  = _sc_escape(_BIAS_TAG.get(ema_bias, ema_bias))
    cross_badge = f"  │  {_CROSS_TAG[ema_cross]}" if ema_cross in _CROSS_TAG else ""
    conf_bar    = _sc_confidence_bar(confidence)
    quality_str = f"  │  Quality: {quality.title()}" if quality else ""
    ladder      = _sc_price_ladder(pair, price, {"r3": r3, "r2": r2, "r1": r1, "pp": pp, "s1": s1, "s2": s2, "s3": s3})
    macro_label = _sc_escape(_sc_macro_label(mscore))
    pivot_str   = f"pivot: <code>{pivot_state}</code>" if pivot_state else ""
    cross_ctx   = f"  │  Cross: {_sc_escape(ema_cross)}" if ema_cross not in ("none", "") else ""
    h1_status   = str(signal.get("h1_status", ""))
    h1_move     = str(signal.get("h1_movement", ""))
    h1_align    = str(signal.get("h1_bias_alignment", ""))
    h1_score    = signal.get("h1_score", "")
    h1_adj      = float(signal.get("h1_confidence_adjustment", 0.0) or 0.0)
    h1_ema_bias = str(signal.get("h1_ema_bias", ""))
    h1_fast     = signal.get("h1_fast_ema", signal.get("h1_ema20", None))
    h1_slow     = signal.get("h1_slow_ema", signal.get("h1_ema50", None))
    h1_fast_p   = signal.get("h1_fast_ema_period", 20)
    h1_slow_p   = signal.get("h1_slow_ema_period", 50)
    h1_reason   = _sc_escape(str(signal.get("h1_reason", "")))
    h1_last_hr  = _sc_escape(str(signal.get("h1_last_hour", "")))
    h1_summary  = _sc_escape(str(signal.get("h1_summary", "") or _build_h1_summary(signal)))
    h1_adj_text = _sc_escape(str(signal.get("h1_adjustment_text", "") or _h1_adjustment_text(h1_adj)))
    generated   = _sc_now().astimezone(_MYT).strftime("%Y-%m-%d %H:%M MYT")
    date_str    = f"Bar: {ohlc_date}  │  " if ohlc_date else ""

    DIV = "─" * 32

    msg = (
        # ── Header ──────────────────────────────────────────────────────
        f"{emoji} <b>{label}</b>  ·  <b>{pair.upper()}</b>  <b>{dir_arrow}</b>\n"
        f"{DIV}\n"
        # ── Badges ──────────────────────────────────────────────────────
        f"{align_badge}  │  {bias_badge}{cross_badge}\n"
        # ── Reason ──────────────────────────────────────────────────────
        f"📋 {reason}\n"
        # ── Confidence ──────────────────────────────────────────────────
        f"<code>Confidence  {conf_bar}{quality_str}</code>\n"
        f"{DIV}\n"
        # ── Pivot ladder ────────────────────────────────────────────────
        f"<b>Pivot Levels</b>  <i>(Daily)</i>\n"
        f"<code>{_sc_escape(ladder)}</code>\n"
        f"{DIV}\n"
    )

    # ── Daily EMA panel (optional) ───────────────────────────────────────────
    if ema_result:
        msg += _sc_ema_panel(pair, ema_result) + f"\n{DIV}\n"

    # ── Compact H1 intraday movement panel ────────────────────────────────
    if h1_status:
        msg += "<b>H1 Intraday</b>\n"
        msg += f"<code>{h1_summary}</code>\n"
        meta = []
        if h1_adj:
            meta.append(f"Confidence {h1_adj_text}")
        if h1_score not in (None, ""):
            meta.append(f"Score {h1_score}")
        if h1_last_hr:
            meta.append(f"Last {h1_last_hr}")
        if meta:
            msg += f"<code>{_sc_escape(' · '.join(str(x) for x in meta))}</code>\n"
        msg += f"{DIV}\n"

    # ── Context ───────────────────────────────────────────────────────────
    msg += "<b>Context</b>\n"
    msg += f"Macro   {macro_label}\n"
    if pivot_src:
        msg += f"Pivot   Source: {pivot_src}\n"
    if pivot_str:
        msg += f"State   {pivot_str}{cross_ctx}\n"

    # ── Footer ────────────────────────────────────────────────────────────
    msg += f"<i>{date_str}Generated: {generated}</i>"

    return msg


# ---- send (reuses pivot.py's session if available) -------------------------

def _sc_send_telegram(bot_token: str, chat_id: int, text: str, timeout: int = 10) -> bool:
    """Send a Telegram message. Falls back to a plain requests.post if pivot
    module is not importable (so signal_confirm.py stays self-contained)."""
    if not bot_token:
        _log.warning("signal alerts: Telegram bot token is empty; set TELEGRAM_BOT_TOKEN")
        return False
    if not chat_id:
        _log.warning("signal alerts: Telegram chat ID is 0 or unset; set TELEGRAM_CHAT_ID")
        return False

    # Try to reuse pivot's session (shared retry adapter + UA header).
    try:
        from pivot import _get_session  # type: ignore
        session_post = _get_session().post
    except ImportError:
        import requests as _requests
        session_post = _requests.post

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    try:
        resp = session_post(
            url,
            json={"chat_id": chat_id, "text": text, "parse_mode": "HTML"},
            timeout=timeout,
        )
        if resp.status_code == 200:
            return True
        _log.warning("signal alerts: Telegram HTTP %s — %s", resp.status_code, resp.text[:200])
        return False
    except Exception as exc:
        _log.warning("signal alerts: send failed: %s", exc)
        return False



# ---- session calendar alerts -----------------------------------------------

def _sc_calendar_candidate_files() -> list:
    """Candidate calendar.json paths, safest priority first."""
    candidates = []
    if SIGNAL_CALENDAR_JSON:
        candidates.append(_Path(SIGNAL_CALENDAR_JSON))
    if _os.environ.get("SCRAPER_OUTPUT_DIR"):
        candidates.append(_Path(_os.environ.get("SCRAPER_OUTPUT_DIR", "")) / "calendar.json")
    base = _Path(__file__).resolve().parent
    candidates.extend([
        _Path("public_html/calendar.json"),
        _Path("public/calendar.json"),
        _Path("calendar.json"),
        base / "public_html" / "calendar.json",
        base / "public" / "calendar.json",
        base / "calendar.json",
    ])
    out, seen = [], set()
    for candidate in candidates:
        try:
            key = str(candidate.resolve()) if candidate.exists() else str(candidate)
        except Exception:
            key = str(candidate)
        if key not in seen:
            seen.add(key)
            out.append(candidate)
    return out


def _sc_load_calendar_events() -> tuple:
    """Load scraper.py calendar.json events."""
    for path in _sc_calendar_candidate_files():
        try:
            if not path.exists():
                continue
            with path.open(encoding="utf-8") as handle:
                data = _json.load(handle)
            events = data.get("events", []) if isinstance(data, dict) else []
            if isinstance(events, list):
                return events, str(path)
        except Exception as exc:
            _log.warning("session calendar: failed reading %s: %s", path, exc)
    return [], ""


def _sc_parse_event_time(value: Any) -> Optional[_datetime]:
    if not value:
        return None
    try:
        dt = _datetime.fromisoformat(str(value).strip().replace("Z", "+00:00"))
        return dt if dt.tzinfo else dt.replace(tzinfo=_UTC)
    except Exception:
        return None


def _sc_parse_calendar_sessions(raw: str) -> Dict[str, tuple]:
    sessions: Dict[str, tuple] = {}
    for part in (raw or "").split(","):
        if "=" not in part:
            continue
        name, hhmm = part.split("=", 1)
        try:
            hh, mm = [int(x) for x in hhmm.strip().split(":", 1)]
            if 0 <= hh <= 23 and 0 <= mm <= 59 and name.strip():
                sessions[name.strip()] = (hh, mm)
        except Exception:
            continue
    return sessions or {"Sydney": (6, 0), "Tokyo": (8, 0), "London": (15, 0), "New York": (20, 0)}


def _sc_due_calendar_sessions(now_myt: Optional[_datetime] = None) -> list:
    """Return sessions whose MYT start time falls within the current run window."""
    now_myt = now_myt or _sc_now().astimezone(_MYT)
    due = []
    for name, (hh, mm) in _sc_parse_calendar_sessions(SIGNAL_CALENDAR_SESSIONS_RAW).items():
        start = now_myt.replace(hour=hh, minute=mm, second=0, microsecond=0)
        delta_min = (now_myt - start).total_seconds() / 60.0
        if 0 <= delta_min < SIGNAL_CALENDAR_SESSION_WINDOW_MIN:
            due.append((name, start))
    return due


def _sc_filter_today_events(events: list, today_myt) -> list:
    """Keep today's configured-currency events and attach parsed UTC datetime as _dt."""
    # Build the allowed impact set based on SIGNAL_CALENDAR_MIN_IMPACT.
    # "high"   → {"high"}
    # "medium" → {"high", "medium"}
    # ""       → None (no filter — all impact levels pass)
    if SIGNAL_CALENDAR_MIN_IMPACT == "high":
        _allowed_impacts: Optional[set] = {"high"}
    elif SIGNAL_CALENDAR_MIN_IMPACT == "medium":
        _allowed_impacts = {"high", "medium"}
    else:
        _allowed_impacts = None  # no filter

    out = []
    for ev in events:
        if not isinstance(ev, dict):
            continue
        curr = str(ev.get("currency") or ev.get("country") or "").upper().strip()
        if SIGNAL_CALENDAR_CURRENCIES and curr and curr not in SIGNAL_CALENDAR_CURRENCIES:
            continue
        # Impact filter
        if _allowed_impacts is not None:
            ev_impact = str(ev.get("impact") or "").strip().lower()
            if ev_impact not in _allowed_impacts:
                continue
        et = _sc_parse_event_time(ev.get("event_time"))
        if et is None or et.astimezone(_MYT).date() != today_myt:
            continue
        item = dict(ev)
        item["_dt"] = et
        out.append(item)
    out.sort(key=lambda e: e.get("_dt") or _datetime.max.replace(tzinfo=_UTC))
    return out


def _sc_impact_icon(impact: Any) -> str:
    v = str(impact or "").strip().lower()
    return "🔴" if v == "high" else "🟠" if v == "medium" else "🟡"


def _sc_build_session_calendar_text(session_name: str, events: list, source_path: str, now_myt: Optional[_datetime] = None) -> str:
    now_myt = now_myt or _sc_now().astimezone(_MYT)
    impact_label = {"high": "🔴 High only", "medium": "🔴🟠 Medium+"}.get(SIGNAL_CALENDAR_MIN_IMPACT, "All")
    lines = [
        f"<b>🗓️ {session_name} Session News/Event Calendar</b>",
        f"<b>Date:</b> {now_myt.strftime('%Y-%m-%d')} MYT",
        f"<b>Scope:</b> {', '.join(sorted(SIGNAL_CALENDAR_CURRENCIES))}  |  <b>Impact:</b> {impact_label}",
    ]
    if source_path:
        lines.append(f"<b>Source:</b> {_sc_escape(_Path(source_path).name)}")
    lines.append("─" * 32)
    if not events:
        lines.append("No scheduled calendar events found for today in calendar.json.")
        return "\n".join(lines)
    for ev in events[:SIGNAL_CALENDAR_MAX_EVENTS]:
        dt = ev.get("_dt")
        time_txt = dt.astimezone(_MYT).strftime("%H:%M") if isinstance(dt, _datetime) else "TBD"
        curr = _sc_escape(ev.get("currency") or ev.get("country") or "")
        impact = _sc_escape(ev.get("impact") or "")
        title = _sc_escape(ev.get("title") or "")
        bits = []
        if ev.get("forecast") not in (None, ""):
            bits.append(f"F:{_sc_escape(ev.get('forecast'))}")
        if ev.get("previous") not in (None, ""):
            bits.append(f"P:{_sc_escape(ev.get('previous'))}")
        if ev.get("actual") not in (None, ""):
            bits.append(f"A:{_sc_escape(ev.get('actual'))}")
        detail = f" — {' | '.join(bits)}" if bits else ""
        lines.append(f"{_sc_impact_icon(impact)} <b>{time_txt}</b> {curr} · {impact} · {title}{detail}")
    remaining = len(events) - SIGNAL_CALENDAR_MAX_EVENTS
    if remaining > 0:
        lines.append(f"… plus {remaining} more event(s).")
    ai_note = _groq_sc_calendar_note(session_name, events, now_myt)
    if ai_note:
        lines.extend(["─" * 32, "<b>🤖 Groq AI Calendar Commentary</b>", ai_note])
    return "\n".join(lines)


def _dispatch_session_calendar_alerts(state: Dict[str, str], bot_token: str, chat_id: int, dry_run: bool = False) -> int:
    """Send today's News/Event Calendar once per configured session start."""
    if not SIGNAL_CALENDAR_ALERTS_ENABLED:
        return 0
    now_myt = _sc_now().astimezone(_MYT)
    due = _sc_due_calendar_sessions(now_myt)
    if not due:
        return 0
    events, source_path = _sc_load_calendar_events()
    today_events = _sc_filter_today_events(events, now_myt.date())
    # Skip silently when no events survive the impact filter (and skip-guard is on).
    if not today_events and SIGNAL_CALENDAR_SKIP_IF_NO_EVENTS:
        _log.debug(
            "session calendar: no events match impact filter=%r — skipping all session alerts",
            SIGNAL_CALENDAR_MIN_IMPACT or "all",
        )
        return 0
    sent = 0
    for session_name, _start in due:
        key = f"session_calendar:{now_myt.strftime('%Y-%m-%d')}:{session_name.lower().replace(' ', '_')}"
        if key in state:
            _log.debug("session calendar: duplicate key=%s — skipping", key)
            continue
        text = _sc_build_session_calendar_text(session_name, today_events, source_path, now_myt)
        if dry_run:
            _log.info("[DRY-RUN] session calendar alert key=%s:\n%s", key, text)
            state[key] = _sc_iso(_sc_now())
            sent += 1
        else:
            ok = _sc_send_telegram(bot_token, chat_id, text)
            if ok:
                state[key] = _sc_iso(_sc_now())
                sent += 1
                _log.info("session calendar: sent %s alert key=%s", session_name, key)
            else:
                _log.warning("session calendar: failed to send %s alert", session_name)
    return sent


# ---- main dispatcher -------------------------------------------------------

def dispatch_signal_alerts(
    pivot_results:  Dict[str, Dict[str, Any]],
    ema_states:     Dict[str, Dict[str, Any]],
    bot_token:      str  = SIGNAL_BOT_TOKEN,
    chat_id:        int  = SIGNAL_CHAT_ID,
    min_confidence: float = SIGNAL_ALERT_MIN_CONFIDENCE,
    alert_signals:  Optional[set] = None,
    state_file:     Optional[_Path] = None,
    dry_run:        bool = False,
) -> int:
    """
    Compute combined signals for all pairs and fire Telegram alerts for
    actionable ones.  De-duplicates by (pair, signal, ohlc_date) so the
    same signal is never re-sent for the same daily bar.

    Parameters
    ----------
    pivot_results   : first return value of pivot.fetch_price_structure()
    ema_states      : macro["ema_20_50_state"] or ema.py output dictionaries
    bot_token       : Telegram bot token (defaults to TELEGRAM_BOT_TOKEN)
    chat_id         : Telegram chat ID (defaults to TELEGRAM_CHAT_ID)
    min_confidence  : suppress alerts below this confidence floor
    alert_signals   : set of signal strings that are allowed to fire
    state_file      : override path for de-dup state JSON
    dry_run         : log alerts without sending or persisting state

    Returns
    -------
    int — number of alerts sent (or that would have been sent in dry-run mode)
    """
    _sf = state_file or _DEFAULT_ALERT_STATE_FILE
    _allowed = alert_signals if alert_signals is not None else SIGNAL_ALERT_SIGNALS

    # Validate credentials before doing any work.
    if not dry_run and not bot_token:
        _log.warning("signal alerts: disabled because Telegram bot token is empty; set TELEGRAM_BOT_TOKEN")
        return 0
    if not dry_run and bot_token and not chat_id:
        try:
            chat_id = _sc_resolve_chat_id(_signal_chat_id_raw)
        except RuntimeError as exc:
            raise RuntimeError(
                f"signal alerts: {exc}  Set TELEGRAM_CHAT_ID or disable alerts."
            ) from exc

    state = _sc_prune_state(_sc_load_state(_sf), SIGNAL_ALERT_PRUNE_H)
    sent = _dispatch_session_calendar_alerts(state, bot_token, chat_id, dry_run=dry_run)
    signals = batch_combine(pivot_results, ema_states)

    for pair, sig in signals.items():
        if not sig.get("ok"):
            _log.debug("signal alerts[%s]: skipping — ok=False (%s)", pair, sig.get("signal"))
            continue

        sig_val    = sig.get("signal", "")
        confidence = float(sig.get("confidence", 0.0))
        pivot_raw = next((v for k, v in pivot_results.items() if _norm_pair(k) == _norm_pair(pair)), {})
        ema_raw = next((v for k, v in ema_states.items() if _norm_pair(k) == _norm_pair(pair)), {}) if ema_states else {}
        ohlc_date  = _get_any(pivot_raw if isinstance(pivot_raw, dict) else {}, "ohlc_date", "date", default="")

        # Gate: must be an alertable signal type
        if sig_val not in _allowed:
            _log.debug("signal alerts[%s]: skipping %s — not in allowed set", pair, sig_val)
            continue

        # Gate: confidence floor
        if confidence < min_confidence:
            _log.debug(
                "signal alerts[%s]: skipping %s — confidence=%.3f < %.3f",
                pair, sig_val, confidence, min_confidence,
            )
            continue

        # De-dup key: stable for (pair, signal_type, daily bar)
        key = f"{pair}:{sig_val}:{ohlc_date or _sc_now().strftime('%Y-%m-%d')}"
        if key in state:
            _log.debug("signal alerts[%s]: duplicate key=%s — skipping", pair, key)
            continue

        text = build_signal_alert_text(
            pair, sig,
            pivot_raw if isinstance(pivot_raw, dict) else {},
            ema_result=ema_raw if isinstance(ema_raw, dict) else None,
        )

        ai_note = _groq_sc_note(pair, sig, pivot_raw if isinstance(pivot_raw, dict) else {})
        if ai_note:
            DIV = "─" * 32
            text += f"\n{DIV}\n<b>🤖 Groq AI</b>\n{ai_note}"

        if dry_run:
            _log.info("[DRY-RUN] signal alert key=%s:\n%s", key, text)
            sent += 1
        else:
            ok = _sc_send_telegram(bot_token, chat_id, text)
            if ok:
                state[key] = _sc_iso(_sc_now())
                sent += 1
                _log.info(
                    "signal alerts: sent %s for %s (confidence=%.2f key=%s)",
                    sig_val, pair, confidence, key,
                )
            else:
                _log.warning("signal alerts: failed to send %s for %s", sig_val, pair)

    if not dry_run:
        _sc_save_state(_sf, state)

    return sent


# ---------------------------------------------------------------------------
# Quick self-test / demo  (python signal_confirm.py)
# ---------------------------------------------------------------------------

if __name__ == "__main__":

    _PIVOT_MOCK: Dict[str, Any] = {
        "price_state":    "accept_above_pp",
        "macro_alignment": "with",
        "conviction_mult": 1.0,
        "price_used":     1.08520,
        "PP": 1.08400, "R1": 1.08700, "R2": 1.09000, "R3": 1.09300,
        "S1": 1.08100, "S2": 1.07800, "S3": 1.07500,
        "macro_score": 0.62,
        "state_quality": "high",
        "state_basis": "live",
        "ohlc_date": "2026-04-25",
    }

    # ema_state dict (as consumed by combine_pivot_ema_signal)
    _EMA_STATE_MOCK: Dict[str, Any] = {
        "pair":          "eurusd",
        "trend_bias":    "bullish_bias",
        "suggestion":    "bullish_cross",
        "ema_cross_signal": "cross_up",
        "current_vs_fast":  "above",
        "current_vs_slow":  "above",
        "ok":            True,
        "warming_up":    False,
    }

    # ema_result dict (as produced by dataclasses.asdict(EMAAnalysisResult))
    _EMA_RESULT_MOCK: Dict[str, Any] = {
        "pair":                 "eurusd",
        "timeframe":            "Daily",
        "fast_period":          20,
        "slow_period":          50,
        "ema_fast":             1.08480,
        "ema_slow":             1.08310,
        "ema_fast_vs_slow":     "above",
        "ema_cross_signal":     "cross_up",
        "last_close":           1.08510,
        "close_structure":      "above_fast_slow",
        "current_vs_fast":      "above",
        "current_vs_slow":      "above",
        "trend_bias":           "bullish_bias",
        "suggestion":           "bullish_cross",
        "completed_closes":     112,
    }

    print("=" * 60)
    print("DEMO 1 — HIGH CONVICTION LONG  (with Daily EMA panel)")
    print("=" * 60)
    result = combine_pivot_ema_signal(_PIVOT_MOCK, _EMA_STATE_MOCK)
    print(_json.dumps(result, indent=2))
    print()
    print(build_signal_alert_text("eurusd", result, _PIVOT_MOCK, ema_result=_EMA_RESULT_MOCK))

    print()
    print("=" * 60)
    print("DEMO 2 — CONFIRMED SHORT  (with Daily EMA panel)")
    print("=" * 60)
    _PIVOT_MOCK2 = {**_PIVOT_MOCK, "price_used": 1.08050, "macro_score": -0.38,
                    "price_state": "accept_below_pp", "macro_alignment": "against",
                    "state_quality": "medium"}
    _EMA_STATE2  = {**_EMA_STATE_MOCK, "trend_bias": "bearish_bias",
                    "cross": "death_cross", "suggestion": "bearish_cross",
                    "price_vs_fast": "below", "price_vs_slow": "below"}
    _EMA_RESULT2 = {**_EMA_RESULT_MOCK, "ema_fast_vs_slow": "below",
                    "ema_cross_signal": "cross_down", "close_structure": "below_fast_slow",
                    "current_vs_fast": "below", "current_vs_slow": "below",
                    "trend_bias": "bearish_bias", "suggestion": "bearish_cross"}
    result2 = combine_pivot_ema_signal(_PIVOT_MOCK2, _EMA_STATE2)
    print(build_signal_alert_text("eurusd", result2, _PIVOT_MOCK2, ema_result=_EMA_RESULT2))

    print()
    print("=" * 60)
    print("DEMO 3 — message WITHOUT Daily panel (backward-compat)")
    print("=" * 60)
    print(build_signal_alert_text("eurusd", result, _PIVOT_MOCK))

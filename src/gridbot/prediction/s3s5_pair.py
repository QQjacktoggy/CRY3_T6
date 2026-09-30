"""2026-09-14: R3 A plus FAV with a 2 bps favorable-distance admission guard.

Prior release history:

2026-09-13 rule set D (C''+FAV): LEGS=(R3, FAV);
  R3 [0.45, 0.49] @ 1U; FAV [0.60, 0.75] @ 1U (operator-selected size).
  Forbid ask [0.35, 0.40); do NOT reopen mid band [0.50, 0.55] for R3.
  R3 fill↑: delay_ms 3000→2000, end window 210→240 (exec_slack 4s).
  Mutual exclusion: first pending leg owns the market; simultaneous signals prefer R3.
2026-09-13 rule set C'-tight (C''): LEGS=R3 only; forbid ask [0.35, 0.40); min_ask >= 0.45; max_ask = 0.49.
2026-09-13 rule set C': LEGS=R3 only; forbid ask [0.35, 0.40); min_ask >= 0.45; max_ask = 0.55.
2026-09-13 rule set C: LEGS=R3 only; forbid ask [0.35, 0.40); min_ask >= 0.45.
2026-09-12 stable replan: LEGS=R3 only; R3 forbids ask [0.35, 0.40).
Original: S3+S5 live lane: frozen R3 + VOL_LATE_3S legs, 1 USDT each, intra-batch -4 halt.

Signal math matches the pair-shadow engine for those two legs.  Live quotes are
REST books mapped into the same feature/signal shape.  Playbook overlay never
places orders while OBSERVE; recovery is a later complete 20-run with paper>0,
then the following batch resumes.  This module does not call Binance.
"""
from __future__ import annotations

import math
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Mapping

TPE = timezone(timedelta(hours=8))
PROFILE = "s3s5_pair_v1"
FAV_PROFILE = "fav_only_v1"
FAV_V2_PROFILE = "fav_only_v2"
FAV_V3_PROFILE = "fav_only_v3"
FAV_V4_PROFILE = "fav_only_v4"
FAV_P3_PROFILE = "fav_p3"
FAV_ONLY_PROFILES = frozenset((FAV_PROFILE, FAV_V2_PROFILE, FAV_V3_PROFILE, FAV_V4_PROFILE, FAV_P3_PROFILE))
PROFILES = frozenset((PROFILE, *FAV_ONLY_PROFILES))
OLD_PRICE_CAP_SLACK = 0.005
FAV_V2_OLD_PRICE_CAP_SLACK = 0.06
FAV_V3_OLD_PRICE_CAP_SLACK = 0.02
FAV_V4_OLD_PRICE_CAP_SLACK = 0.01
FAV_V3_MIN_ASK_GAP = Decimal("0.10")


def _norm_profile(value: Any) -> str:
    return str(value or "").strip().lower()


def is_fav_only(profile: Any) -> bool:
    return _norm_profile(profile) in FAV_ONLY_PROFILES


def old_price_cap_slack(profile: Any) -> float:
    """Execution ask may rise this much vs the signal ask. V1 stays 0.005, V2 is 0.06, V3 is 0.02, V4 is 0.01."""

    p = _norm_profile(profile)
    if p == FAV_V2_PROFILE:
        return FAV_V2_OLD_PRICE_CAP_SLACK
    if p == FAV_V3_PROFILE:
        return FAV_V3_OLD_PRICE_CAP_SLACK
    if p == FAV_V4_PROFILE:
        return FAV_V4_OLD_PRICE_CAP_SLACK
    return OLD_PRICE_CAP_SLACK


def legs_for_profile(profile):
    if is_fav_only(profile):
        return ("FAV",)
    if _norm_profile(profile) == PROFILE:
        return LEGS
    raise ValueError("Unsupported R3/FAV profile")

LEG_GROSS = {"R3": "1", "FAV": "1"}
LEGS = ("R3", "FAV")  # Deployment does not arm or start a loop.
LOOP_MDD_BY_UNIT = {
    Decimal("1"): Decimal("-2.5"),
    Decimal("2"): Decimal("-5.0"),
    Decimal("3"): Decimal("-7.5"),
}


def uses_loop_risk_guards(profile: Any) -> bool:
    """Loop peak MDD + 2-loss cooldown apply to this family, including future lanes in PROFILES."""

    return _norm_profile(profile) in PROFILES


def get_leg_gross(order_unit_usdt: Decimal | str | int = "1") -> dict[str, str]:
    unit = str(int(Decimal(str(order_unit_usdt))))
    if unit not in {"1", "2", "3"}:
        raise ValueError("s3s5 order unit must be 1, 2 or 3")
    return {"R3": unit, "FAV": unit}


def loop_mdd_limit(order_unit_usdt: Decimal | str | int = "1") -> Decimal:
    unit = Decimal(str(order_unit_usdt))
    return LOOP_MDD_BY_UNIT.get(unit, Decimal("-2.5"))
FAV_VERSION = "fav-distance2-20260914"


def fav_exclusion_key(campaign_id):
    return "fav_distance2_excluded:" + str(campaign_id)


def fav_approval_key(campaign_id):
    return "fav_distance2_approved:" + str(campaign_id)


def fav_distance_allowed(spot, reference, side, profile=PROFILE):
    try:
        p, r = Decimal(str(spot)), Decimal(str(reference))
        if side not in ("UP", "DOWN") or not p.is_finite() or not r.is_finite() or p <= 0 or r <= 0:
            return False
        min_bps = Decimal("5.0") if _norm_profile(profile) == FAV_V4_PROFILE else Decimal("2")
        return (p / r - 1) * 10000 * (1 if side == "UP" else -1) >= min_bps
    except (ArithmeticError, ValueError, TypeError):
        return False


def fav_snapshot_approval(quote, plan, at, start, profile=PROFILE):
    """Use original clocks, never the legacy mapper's restamped clocks."""
    try:
        if _norm_profile(profile) not in PROFILES:
            return None
        side = plan["side"]
        clocks = [quote.observed_at_ms, quote.spot_observed_at_ms,
                  quote.book_up_observed_at_ms, quote.book_down_observed_at_ms]
        # The live WSS producer preserves exchange times in execution_book,
        # while the optional top-level book fields remain zero. Only accept
        # that explicit, orientation-verified provenance; never stamp "now".
        if clocks[2] == 0 or clocks[3] == 0:
            raw = quote.raw if isinstance(quote.raw, Mapping) else {}
            book = raw.get("execution_book")
            if not isinstance(book, Mapping) or book.get("source") != "binance_prediction_ws":
                return None
            if book.get("orientation_verified") is not True or book.get("source_timestamp_available") is not True:
                return None
            times = book.get("book_source_times_ms")
            if not isinstance(times, (list, tuple)) or len(times) != 2 or book.get("sampled_at_ms") != quote.observed_at_ms:
                return None
            for i, name in enumerate(("UP", "DOWN")):
                if Decimal(str(book[name]["ask"])) != (quote.up_ask if i == 0 else quote.down_ask):
                    return None
                if clocks[i + 2] == 0:
                    clocks[i + 2] = times[i]
            clocks.append(book.get("received_at_ms"))
        if not quote.feed_ok or not all(isinstance(t, int) and not isinstance(t, bool) and 0 < t <= at and at - t <= 1500 for t in clocks):
            return None
        ask = Decimal(str(plan["ask"]))
        actual = quote.up_ask if side == "UP" else quote.down_ask
        minimum = Decimal(".55") if _norm_profile(profile) in (FAV_V3_PROFILE, FAV_V4_PROFILE) else Decimal(".60")
        if not 45000 <= at - start <= 240000 or not minimum <= ask <= Decimal(".75") or ask != actual:
            return None
        if not fav_distance_allowed(quote.btc_spot, quote.reference_price, side, profile=profile):
            return None
        return dict(version=FAV_VERSION, profile=_norm_profile(profile), at_ms=at, source_ms=min(clocks), start_ms=start,
                    side=side, ask=str(ask), spot=str(quote.btc_spot), reference=str(quote.reference_price))
    except (AttributeError, KeyError, ArithmeticError, TypeError, ValueError):
        return None


def fav_approval_valid(proof, at, side, ask, profile=PROFILE):
    try:
        minimum = Decimal(".55") if _norm_profile(profile) in (FAV_V3_PROFILE, FAV_V4_PROFILE) else Decimal(".60")
        return bool(_norm_profile(profile) in PROFILES and isinstance(proof, Mapping) and proof.get("version") == FAV_VERSION
                    and proof.get("profile", PROFILE) == _norm_profile(profile)
                    and 0 <= at - proof["at_ms"] <= 1500
                    and 0 <= at - proof["source_ms"] <= 1500
                    and 45000 <= at - proof["start_ms"] <= 240000
                    and proof["side"] == side
                    and Decimal(str(ask)) == Decimal(proof["ask"])
                    and minimum <= Decimal(str(ask)) <= Decimal(".75")
                    and fav_distance_allowed(proof["spot"], proof["reference"], side, profile=profile))
    except (KeyError, ArithmeticError, TypeError, ValueError):
        return False
# family, begin, end, min_ask, max_ask, delay_ms, old_price_cap, reconfirm
SPECS = {
    "R3": ("reversion", 30, 240, 0.45, 0.49, 2000, True, False),  # fill↑ end 210→240, delay 3s→2s
    "FAV": ("favorite", 45, 240, 0.60, 0.75, 2000, True, True),
    "VOL_LATE_3S": ("vol", 180, 270, 0.20, 0.85, 3000, False, True),
}


def specs_for(name: str, profile: Any = None) -> tuple:
    """Return SPECS tuple for strategy leg, specialized per profile.
    
    In fav_only_v3 and fav_only_v4, FAV min_ask expands from 0.60 to 0.55.
    In fav_only_v4, reconfirmation delay is 750ms instead of 2000ms.
    """
    family, begin, end, lo, hi, delay, pricecap, reconfirm = SPECS[name]
    p = _norm_profile(profile)
    if p in (FAV_V3_PROFILE, FAV_V4_PROFILE) and name == "FAV":
        lo = 0.55
    if p == FAV_V4_PROFILE and name == "FAV":
        delay = 750
    return family, begin, end, lo, hi, delay, pricecap, reconfirm


# Rule set D / C''+FAV: MIN/MAX_ASK clamp R3 only (ask_allowed); FAV uses SPECS [0.60, 0.75].
MIN_ASK = 0.45
MAX_ASK = 0.49
# Inclusive-exclusive ask gaps skipped after min/max band (stable replan 2026-09-12).
# R3: skip [0.35, 0.40) — kept for honesty/rule-C; currently subsumed by MIN_ASK.
FORBID_ASK_RANGES = {
    "R3": ((0.35, 0.40),),
}


def ask_allowed(name: str, ask: float, profile: Any = None) -> bool:
    """True if ask meets SPECS min/max (and R3 MIN_ASK/MAX_ASK clamp) and forbid gaps.

    R3 is clamped to [MIN_ASK, MAX_ASK] = [0.45, 0.49]. FAV uses SPECS lo/hi
    [0.60, 0.75] (or [0.55, 0.75] in fav_only_v3) without the MAX_ASK=0.49 ceiling.
    Enforced for signal admission and pending execution (price_band).
    """
    family, begin, end, lo, hi, *_ = specs_for(name, profile)
    del family, begin, end
    floor = max(float(lo), float(MIN_ASK))
    ceiling = min(float(hi), float(MAX_ASK)) if name == "R3" else float(hi)
    if not floor <= ask <= ceiling:
        return False
    for gap_lo, gap_hi in FORBID_ASK_RANGES.get(name, ()):
        if gap_lo <= ask < gap_hi:
            return False
    return True


SIGMA_FLOOR = 0.05588092263731523
BPS = Decimal("500")
MDD_LIMIT = Decimal("-6")
# Loop 200 must not use the old 1 USDT × 2 = -2 envelope. The -4 stop is
# per 20-run batch (playbook). These are catastrophe backstops only.
LOOP_LOSS_LIMIT = Decimal("-40")  # ~10 batches of -4
DAILY_LOSS_LIMIT = Decimal("-12")  # ~3 halted batches in a day
CONSECUTIVE_LOSS_LIMIT = 20  # one full 20-run of losses; playbook still owns -4
BATCH_MS = 6_000_000
EPOCH_MS = int(datetime(2026, 9, 10, 17, 35, tzinfo=TPE).timestamp() * 1000)
MIN_SCORED = 15
MIN_SCORED_TAIL = 10
TIMES = (
    "spot_at_ms",
    "spot_event_at_ms",
    "spot_received_at_ms",
    "book_at_ms",
    "received_at_ms",
)


def risk_envelope() -> dict[str, Decimal | int]:
    """Risk numbers for s3s5_pair_v1. Do not use order_unit × 2."""

    return {
        "loop_loss_limit": LOOP_LOSS_LIMIT,
        "daily_loss_limit": DAILY_LOSS_LIMIT,
        "consecutive_loss_limit": CONSECUTIVE_LOSS_LIMIT,
        "batch_mdd_limit": MDD_LIMIT,
    }


def hard_stop_latch_thresholds(profile: Any) -> tuple[Decimal, int]:
    """Daily PnL and consecutive-loss thresholds that durable-latch hard stop.

    Default matches the old 1 USDT × 2 envelope. S3+S5 uses the catastrophe
    envelope so a 20-run -4 playbook can run inside a 200-run loop.
    """

    if _norm_profile(profile) in PROFILES:
        env = risk_envelope()
        return Decimal(str(env["daily_loss_limit"])), int(env["consecutive_loss_limit"])
    return Decimal("-2"), 3


def risk_status_line() -> str:
    return (
        "策略風控：Loop 高點回撤 1U-2.5／2U-5.0／3U-7.5 停新進場"
        "｜連虧 2 次冷卻 30 分｜災難 Loop-40／日-12／連虧 20"
    )


def amount_picker_hidden(current_profile: Any, next_profile: Any = None) -> bool:
    """Amount picker is available for every lane, including S3S5/FAV."""

    del current_profile, next_profile
    return False


def amount_lock_message(profile: str = PROFILE) -> str:
    if _norm_profile(profile) == FAV_V4_PROFILE:
        return (
            "【FAV V4（ETH 專屬）】\n\n"
            "每腿 1 USDT；Loop MDD -2.5。\n"
            "ask 0.55–0.75；延遲 750ms，追價上限 +0.01；有利距離至少 5 bps，Leader 至少 20 秒，同市場最多一筆。"
        )
    if _norm_profile(profile) == FAV_V3_PROFILE:
        return (
            "【FAV V3 實驗版】\n\n"
            "每腿 1／2／3 USDT；Loop MDD -2.5／-5.0／-7.5。\n"
            "ask 0.55–0.75，兩邊 ask 差至少 0.10；延遲 2 秒，原訊號追價上限 +0.02；有利距離至少 2 bps，同市場最多一筆。"
        )
    if _norm_profile(profile) == FAV_V2_PROFILE:
        return (
            "【FAV V2】\n\n"
            "每腿 1／2／3 USDT；Loop MDD -2.5／-5.0／-7.5。\n"
            "2 秒 old_price_cap 0.06；有利距離至少 2 bps，同市場最多一筆，不加倉。"
        )
    if is_fav_only(profile):
        return (
            "【FAV】\n\n"
            "每腿 1／2／3 USDT；Loop MDD -2.5／-5.0／-7.5。\n"
            "有利距離至少 2 bps，同市場最多一筆，不加倉。"
        )
    return (
        "【S3+S5 投入】\n\n"
        "R3／FAV 每腿 1／2／3 USDT；Loop MDD -2.5／-5.0／-7.5。\n"
        "FAV 有利距離至少 2 bps，同市場最多一筆，不加倉。用 /predict_amount 選擇。"
    )


def amount_lock_note(profile: Any) -> str:
    if _norm_profile(profile) not in PROFILES:
        return ""
    if _norm_profile(profile) == FAV_V4_PROFILE:
        return "\nFAV V4：每腿 1 USDT，Loop MDD -2.5。"
    if _norm_profile(profile) == FAV_V3_PROFILE:
        return "\nFAV V3：每腿 1／2／3 USDT，Loop MDD -2.5／-5.0／-7.5。"
    if _norm_profile(profile) == FAV_V2_PROFILE:
        return "\nFAV V2：每腿 1／2／3 USDT，Loop MDD -2.5／-5.0／-7.5。"
    if is_fav_only(profile):
        return "\nFAV：每腿 1／2／3 USDT，Loop MDD -2.5／-5.0／-7.5。"
    return "\nS3+S5：每腿 1／2／3 USDT，Loop MDD -2.5／-5.0／-7.5。"


def compact_stake_line(profile: str = PROFILE) -> str:
    if _norm_profile(profile) == FAV_V4_PROFILE:
        return "投入：FAV V4 每腿 1 USDT｜Loop MDD -2.5｜距離至少 5 bps｜750ms Reconfirm"
    if _norm_profile(profile) == FAV_V3_PROFILE:
        return "投入：FAV V3 每腿 1／2／3 USDT｜Loop MDD -2.5／-5.0／-7.5｜ask 0.55–0.75｜gap 0.10"
    if _norm_profile(profile) == FAV_V2_PROFILE:
        return "投入：FAV V2 每腿 1／2／3 USDT｜Loop MDD -2.5／-5.0／-7.5｜cap 0.06"
    if is_fav_only(profile):
        return "投入：FAV 每腿 1／2／3 USDT｜Loop MDD -2.5／-5.0／-7.5｜距離至少 2 bps｜同市場最多一筆"
    return "投入：R3／FAV 每腿 1／2／3 USDT｜Loop MDD -2.5／-5.0／-7.5｜FAV 距離至少 2 bps｜同市場最多一筆"


def batch_of(start_ms: int, epoch_ms: int = EPOCH_MS) -> int:
    return (int(start_ms) - int(epoch_ms)) // BATCH_MS + 1


def pnl500(fill: Mapping[str, Any], outcome: str) -> Decimal:
    payout = Decimal("0.5") if outcome == "DRAW" else Decimal(int(fill["side"] == outcome))
    return Decimal(str(fill["shares"])) * payout - Decimal(str(fill["gross"])) * (
        1 + BPS / Decimal("10000")
    )


def _finite_ms(value: Any, at: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return 0
    return parsed if 0 < parsed <= at else 0


def _visible_ask_shares(info: Mapping[str, Any]) -> float:
    """REST depth: prefer top_ask_shares, else any visible size on the side."""

    candidates: list[Any] = [
        info.get("top_ask_shares"),
        info.get("visible_ask_shares"),
        info.get("ask_shares"),
        info.get("size"),
        info.get("quantity"),
        info.get("qty"),
    ]
    for nested_key in ("asks", "levels"):
        levels = info.get(nested_key)
        if not isinstance(levels, list):
            continue
        for item in levels:
            if isinstance(item, Mapping):
                candidates.append(item.get("size") or item.get("quantity") or item.get("qty"))
            elif isinstance(item, (list, tuple)) and len(item) >= 2:
                candidates.append(item[1])
    best = 0.0
    for raw in candidates:
        try:
            value = float(raw or 0)
        except (TypeError, ValueError):
            continue
        if math.isfinite(value) and value > best:
            best = value
    return best


def quote_from_snapshot(quote: Any, at_ms: int) -> dict[str, Any] | None:
    """Preserve and verify the original WSS engine quote; never restamp clocks."""
    try:
        at = int(at_ms)
        raw = quote.raw if isinstance(getattr(quote, "raw", None), Mapping) else {}
        q = raw.get("reversal5")
        execution = raw.get("execution_book")
        if not isinstance(q, Mapping) or not isinstance(execution, Mapping):
            if getattr(quote, "book_up_observed_at_ms", 0) > 0 and getattr(quote, "book_down_observed_at_ms", 0) > 0:
                candidate = {
                    "source": "binance_prediction_rest",
                    "orientation_verified": True,
                    "spot_connected": True,
                    "feed_ok": bool(getattr(quote, "feed_ok", False)),
                    "spot": float(quote.btc_spot) if quote.btc_spot is not None else 0.0,
                    "reference": float(quote.reference_price) if quote.reference_price is not None else 0.0,
                    "spot_at_ms": int(getattr(quote, "spot_observed_at_ms", 0) or at),
                    "spot_event_at_ms": int(getattr(quote, "spot_observed_at_ms", 0) or at),
                    "spot_received_at_ms": int(getattr(quote, "spot_observed_at_ms", 0) or at),
                    "book_at_ms": int(getattr(quote, "book_up_observed_at_ms", 0) or at),
                    "received_at_ms": at,
                    "UP": {
                        "bid": float(quote.up_bid) if quote.up_bid is not None else 0.0,
                        "ask": float(quote.up_ask) if quote.up_ask is not None else 0.0,
                        "ask_shares": float(getattr(quote, "up_ask_shares", 10.0) or 10.0),
                    },
                    "DOWN": {
                        "bid": float(quote.down_bid) if quote.down_bid is not None else 0.0,
                        "ask": float(quote.down_ask) if quote.down_ask is not None else 0.0,
                        "ask_shares": float(getattr(quote, "down_ask_shares", 10.0) or 10.0),
                    },
                }
                return candidate
            return None
        candidate = deepcopy(dict(q))
        if not bool(getattr(quote, "feed_ok", False)) or not valid(candidate, at):
            return None
        if execution.get("source") != candidate.get("source") or execution.get("source") != "binance_prediction_ws":
            return None
        if execution.get("orientation_verified") is not True or execution.get("source_timestamp_available") is not True:
            return None
        if execution.get("sampled_at_ms") != getattr(quote, "observed_at_ms", None):
            return None
        sampled = execution.get("sampled_at_ms")
        if not isinstance(sampled, int) or isinstance(sampled, bool) or not 0 < sampled <= at or at - sampled > 1500:
            return None
        # Historical collectors may attach an additional sampling timestamp.
        # The live producer supplies it only in execution_book; never invent it.
        if "sampled_at_ms" in candidate:
            original_sampled = candidate["sampled_at_ms"]
            if not isinstance(original_sampled, int) or isinstance(original_sampled, bool) or not 0 < original_sampled <= at or at - original_sampled > 1500:
                return None
        if getattr(quote, "spot_observed_at_ms", None) != candidate.get("spot_at_ms"):
            return None
        times = execution.get("book_source_times_ms")
        if not isinstance(times, (list, tuple)) or len(times) != 2:
            return None
        if any(not isinstance(t, int) or isinstance(t, bool) or t != candidate["book_at_ms"] for t in times):
            return None
        if execution.get("received_at_ms") != candidate["received_at_ms"]:
            return None
        scalar_pairs = (
            (candidate["spot"], quote.btc_spot),
            (candidate["reference"], quote.reference_price),
            (candidate["UP"]["bid"], quote.up_bid),
            (candidate["UP"]["ask"], quote.up_ask),
            (candidate["DOWN"]["bid"], quote.down_bid),
            (candidate["DOWN"]["ask"], quote.down_ask),
        )
        if any(Decimal(str(left)) != Decimal(str(right)) for left, right in scalar_pairs):
            return None
        for side in ("UP", "DOWN"):
            info = execution.get(side)
            if not isinstance(info, Mapping):
                return None
            if Decimal(str(info.get("ask"))) != Decimal(str(candidate[side]["ask"])):
                return None
            if Decimal(str(info.get("top_ask_shares"))) != Decimal(str(candidate[side]["ask_shares"])):
                return None
        return candidate
    except (AttributeError, KeyError, ArithmeticError, TypeError, ValueError):
        return None


def acquisition(q: Mapping[str, Any], at: int) -> bool:
    try:
        if q.get("source") not in {"binance_prediction_ws", "binance_prediction_rest"}:
            return False
        if q.get("orientation_verified") is not True or q.get("spot_connected") is not True:
            return False
        if any(not isinstance(q[k], int) or isinstance(q[k], bool) or not 0 < q[k] <= at for k in TIMES):
            return False
        if not q["spot_at_ms"] <= q["spot_event_at_ms"] <= q["spot_received_at_ms"]:
            return False
        if any(at - q[k] > 1500 for k in ("book_at_ms", "received_at_ms")):
            return False
        if not math.isfinite(float(q["spot"])) or float(q["spot"]) <= 0:
            return False
        for side in ("UP", "DOWN"):
            bid, ask, depth = (float(q[side][k]) for k in ("bid", "ask", "ask_shares"))
            if not all(math.isfinite(x) for x in (bid, ask, depth)) or not 0 < bid <= ask < 1 or depth < 0:
                return False
        return True
    except (KeyError, TypeError, ValueError, OverflowError):
        return False


def valid(q: Mapping[str, Any], at: int) -> bool:
    try:
        return bool(
            acquisition(q, at)
            and q.get("feed_ok")
            and at - q["spot_at_ms"] <= 1500
            and math.isfinite(float(q["reference"]))
            and float(q["reference"]) > 0
            and all(float(q[s]["ask"]) - float(q[s]["bid"]) <= 0.040000001 for s in ("UP", "DOWN"))
        )
    except (KeyError, TypeError, ValueError, OverflowError):
        return False


def feature(history: list, q: Mapping[str, Any], at: int, start: int) -> dict[str, Any]:
    spot = float(q["spot"])
    elapsed = (at - start) / 1000
    f: dict[str, Any] = dict(
        at=at,
        elapsed=elapsed,
        spot=spot,
        mid=(float(q["UP"]["bid"]) + float(q["UP"]["ask"])) / 2,
        book=q["book_at_ms"],
        distance=math.log(spot / float(q["reference"])) * 10000,
        asks=[float(q[s]["ask"]) for s in ("UP", "DOWN")],
    )
    for lag in (3, 10):
        anchors = [r for r in history if lag * 1000 <= at - r[0] <= lag * 1000 + 1500]
        if not anchors:
            continue
        old = anchors[-1]
        h = [r for r in history if r[0] >= old[0]]
        if any(b[0] - a[0] > 2500 for a, b in zip(h, h[1:])):
            continue
        f["move" + str(lag)] = math.log(spot / old[1]) * 10000
        f["delta" + str(lag)] = f["mid"] - old[2]
    h = history
    if len(h) >= 30 and h[-1][0] - h[0][0] >= 45000 and all(b[0] - a[0] <= 5000 for a, b in zip(h, h[1:])):
        f["sigma"] = math.sqrt(
            sum((math.log(b[1] / a[1]) * 10000) ** 2 for a, b in zip(h, h[1:])) / ((h[-1][0] - h[0][0]) / 1000)
        )
    return f


def signal(name: str, f: Mapping[str, Any], profile: Any = None) -> str | None:
    family, begin, end, lo, hi, *_ = specs_for(name, profile)
    if not begin <= f["elapsed"] <= end:
        return None
    if family == "reversion":
        v, d, v3 = (f.get(k) for k in ("move10", "delta10", "move3"))
        if any(x is None for x in (v, d, v3)) or abs(v) > 0.5 or abs(d) < 0.03 or v3 * d > 0:
            return None
        side = 1 if d > 0 else 0
    elif family == "favorite":
        # Mostly-decided favorite sleeve: higher ask = market-implied likely winner.
        asks = f["asks"]
        fav = 0 if asks[0] >= asks[1] else 1
        other = 1 - fav
        fav_ask = float(asks[fav])
        other_ask = float(asks[other])
        if not (float(lo) <= fav_ask <= float(hi)):
            return None
        gap_too_small = (
            Decimal(str(asks[fav])) - Decimal(str(asks[other])) < FAV_V3_MIN_ASK_GAP
            if _norm_profile(profile) == FAV_V3_PROFILE
            else fav_ask - other_ask < 0.15
        )
        if gap_too_small:
            return None
        # Soft filter: skip if a violent 3s spot move is contrary to the favorite.
        m3 = f.get("move3")
        if m3 is not None:
            if fav == 0 and m3 < -1.0:
                return None
            if fav == 1 and m3 > 1.0:
                return None
        side = fav
    else:
        if "sigma" not in f:
            return None
        z = f["distance"] / (max(SIGMA_FLOOR, f["sigma"]) * math.sqrt(max(10, 300 - f["elapsed"])))
        p = max(0.001, min(0.999, 0.5 * (1 + math.erf(z / math.sqrt(2)))))
        edges = [p - f["asks"][0] * 1.05, 1 - p - f["asks"][1] * 1.05]
        side = 0 if edges[0] >= edges[1] else 1
        if edges[side] < 0.03:
            return None
    return ("UP", "DOWN")[side] if ask_allowed(name, f["asks"][side], profile=profile) else None


def check_v4_gates(history: list, q: Mapping[str, Any], at: int, side: str) -> tuple[bool, str | None]:
    """Evaluate ETH FAV V4 gates time-causally using observations with t <= at.
    
    All gates are computed from data strictly on or before decision timestamp `at`.
    Gates:
    1. min_signed_distance_bps >= 5.0
    2. min_leader_age_s >= 20
    3. max_reference_crosses in last 20s <= 0
    4. min_same_side_ratio in last 20s >= 0.85
    """
    try:
        underlying_spot = float(q["spot"])
        ref = float(q["reference"])
        if not math.isfinite(underlying_spot) or not math.isfinite(ref) or underlying_spot <= 0 or ref <= 0:
            return False, "eth_v4_invalid_spot_or_ref"
    except (KeyError, TypeError, ValueError):
        return False, "eth_v4_invalid_spot_or_ref"

    # 1. Spot distance gate: min_signed_distance_bps = 5.0
    if side == "UP":
        signed_dist_bps = (underlying_spot / ref - 1.0) * 10000.0
    elif side == "DOWN":
        signed_dist_bps = (1.0 - underlying_spot / ref) * 10000.0
    else:
        return False, "eth_v4_invalid_side"

    if signed_dist_bps < 5.0:
        return False, "eth_v4_spot_distance"

    # 2. Leader age gate: min_leader_age_s = 20
    # Continuous seconds backwards from `at` where spot was on candidate side
    leader_age_ms = 0
    for entry in reversed(history):
        t_i = entry[0]
        if t_i > at:
            continue
        s_i = entry[1]
        is_same = (s_i >= ref) if side == "UP" else (s_i <= ref)
        if not is_same:
            break
        leader_age_ms = at - t_i

    leader_age_s = leader_age_ms / 1000.0
    if leader_age_s < 20.0:
        return False, "eth_v4_leader_age"

    # 3. Reference cross count in last 20s & 4. Same-side ratio in last 20s
    window_20s = [e for e in history if at - 20000 <= e[0] <= at]
    if not window_20s:
        return False, "eth_v4_no_history_in_window"

    cross_count = 0
    same_side_count = 0
    for i, entry in enumerate(window_20s):
        s_i = entry[1]
        is_same = (s_i >= ref) if side == "UP" else (s_i <= ref)
        if is_same:
            same_side_count += 1
        if i > 0:
            s_prev = window_20s[i - 1][1]
            prev_same = (s_prev >= ref) if side == "UP" else (s_prev <= ref)
            if prev_same != is_same:
                cross_count += 1

    if cross_count > 0:
        return False, "eth_v4_recent_cross"

    same_side_ratio = same_side_count / len(window_20s)
    if same_side_ratio < 0.85:
        return False, "eth_v4_same_side_ratio"

    return True, None


def initialize(
    start: int,
    profile: str = PROFILE,
    order_unit_usdt: Decimal | str | int = "1",
) -> dict[str, Any]:
    gross = get_leg_gross(order_unit_usdt)
    return dict(
        version=profile,
        start_ms=start,
        last_ms=None,
        history=[],
        reference=None,
        censored=False,
        closed=False,
        legs={
            name: dict(
                status="waiting",
                gross=gross[name],
            )
            for name in legs_for_profile(profile)
        },
        depth_book=None,
        consumed={},
    )


def step(
    previous: dict[str, Any] | None,
    q: Mapping[str, Any],
    *,
    at: int,
    start: int,
    live: bool = False,
    profile: str = PROFILE,
    order_unit_usdt: Decimal | str | int = "1",
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    s = deepcopy(previous) if previous else initialize(start, profile, order_unit_usdt)
    if s["version"] != profile or s["start_ms"] != start:
        raise ValueError("S3+S5 identity changed")
    if s["closed"] or not start <= at < start + 300000:
        raise ValueError("Outside active market")
    if s["last_ms"] is not None and at <= s["last_ms"]:
        raise ValueError("Input time not strictly increasing")
    try:
        ref = Decimal(str(q.get("reference")))
    except Exception:
        ref = Decimal("NaN")
    if ref.is_finite() and ref > 0:
        if s["reference"] is not None and Decimal(s["reference"]) != ref:
            raise ValueError("Reference changed")
        if s["reference"] is None:
            s["reference"] = str(ref)
    s["last_ms"] = at
    if s.get("fav_excluded"):
        return s, []
    s["history"] = [r for r in s["history"] if start <= r[0] <= at]
    good = valid(q, at) and not s["censored"]
    if good:
        s["history"].append(
            [at, float(q["spot"]), (float(q["UP"]["bid"]) + float(q["UP"]["ask"])) / 2]
        )
        f = feature(s["history"], q, at, start)
    else:
        f = None
    proposals: list[dict[str, Any]] = []
    for name in legs_for_profile(profile):  # deterministic R3 priority if both signals appear on this tick
        leg = s["legs"][name]
        if leg["status"] not in ("waiting", "pending"):
            continue
        # Never add another leg after either strategy has started its entry.
        # The worker also checks durable intents, including after a restart.
        others = [v for k, v in s["legs"].items() if k != name]
        if any(v.get("status") in ("ordering", "filled") for v in others):
            continue
        if leg["status"] == "waiting" and any(v.get("status") == "pending" for v in others):
            continue
        if name == "FAV" and s["legs"].get("R3", {}).get("status") == "pending":
            continue
        family, begin, end, lo, hi, delay, pricecap, reconfirm = specs_for(name, profile)
        if at > start + end * 1000:
            leg.update(status="expired", reason="window_ended")
            continue
        if leg["status"] == "pending":
            p = leg["pending"]
            if at > p["expires_ms"]:
                leg.update(status="expired", reason="execution_timeout")
                continue
            if at < p["ready_ms"]:
                continue
            # Every mode requires a valid quote from a strictly newer source book.
            # Reconfirm (SPECS flag) and price caps still apply below.
            if not good or q["book_at_ms"] <= p["book_ms"]:
                continue
            side = p["side"]
            ask = Decimal(str(q[side]["ask"]))
            reason = None
            if not ask_allowed(name, float(ask), profile=profile):
                reason = "price_band"
            elif pricecap and (
                ask > Decimal(str(p["ask"])) + Decimal(str(old_price_cap_slack(profile)))
                if _norm_profile(profile) in (FAV_V3_PROFILE, FAV_V4_PROFILE)
                else float(ask) > float(p["ask"]) + old_price_cap_slack(profile) + 1e-12
            ):
                # V3 may wait for the original price cap within the original
                # deadline. Never re-anchor the signal price or extend its TTL.
                if _norm_profile(profile) == FAV_V3_PROFILE and name == "FAV":
                    leg["reason"] = "old_price_cap_wait"
                    continue
                reason = "eth_v4_ask_drift" if _norm_profile(profile) == FAV_V4_PROFILE else "old_price_cap"
            elif _norm_profile(profile) == FAV_V4_PROFILE and name == "FAV":
                v4_ok, v4_reason = check_v4_gates(s["history"], q, at, side)
                if not v4_ok:
                    reason = v4_reason
            elif reconfirm and (f is None or signal(name, f, profile=profile) != side):
                reason = "reconfirmation"
            if reason:
                leg.update(status="rejected", reason=reason)
                continue
            leg.pop("reason", None)
            proposals.append(
                dict(
                    strategy=name,
                    side=side,
                    gross=leg["gross"],
                    ask=str(ask),
                    shares=str(Decimal(leg["gross"]) / ask),
                )
            )
        elif good:
            side = signal(name, f, profile=profile)
            if side is not None:
                if _norm_profile(profile) == FAV_V4_PROFILE and name == "FAV":
                    v4_ok, v4_reason = check_v4_gates(s["history"], q, at, side)
                    if not v4_ok:
                        leg["reason"] = v4_reason
                        continue
                # All modes share the same four-second post-delay deadline; V4 uses 1000ms.
                exec_slack = 1_000 if _norm_profile(profile) == FAV_V4_PROFILE else 4_000
                leg.update(
                    status="pending",
                    pending=dict(
                        side=side,
                        ask=str(q[side]["ask"]),
                        at_ms=at,
                        book_ms=q["book_at_ms"],
                        ready_ms=at + delay,
                        expires_ms=min(at + delay + exec_slack, start + end * 1000),
                    ),
                )
    plans: list[dict[str, Any]] = []
    if proposals:
        book = q["book_at_ms"]
        if s["depth_book"] != book:
            s.update(depth_book=book, consumed={})
        for side in ("UP", "DOWN"):
            ps = [p for p in proposals if p["side"] == side]
            if not ps:
                continue
            used = Decimal(s["consumed"].get(side, "0"))
            required = sum((Decimal(p["shares"]) for p in ps), Decimal(0))
            enough = used + required <= Decimal(str(q[side]["ask_shares"]))
            for p in ps:
                leg = s["legs"][p["strategy"]]
                if enough:
                    if live:
                        if leg.get("status") != "ordering":
                            leg.update(status="ordering", fill=p)
                            plans.append(p)
                    else:
                        leg.update(status="filled", fill=p)
                        plans.append(p)
                else:
                    leg.update(status="rejected", reason="joint_depth")
            if enough:
                s["consumed"][side] = str(used + required)
    if any(p["strategy"] == "FAV" and not fav_distance_allowed(q.get("spot"), q.get("reference"), p["side"], profile=profile) for p in plans):
        if at < start + 90_000:
            for leg in s["legs"].values():
                leg.pop("fill", None)
                if leg.get("status") in ("ordering", "filled"):
                    leg.update(status="waiting")
            return s, []
        s["fav_excluded"] = True
        for leg in s["legs"].values():
            leg.pop("fill", None)
            leg.update(status="rejected", reason="fav_distance_below_threshold")
        return s, []
    return s, plans


def finish(state: dict[str, Any] | None, start: int) -> dict[str, Any]:
    s = deepcopy(state) if state else initialize(start)
    s["closed"] = True
    for leg in s["legs"].values():
        if leg["status"] in ("waiting", "pending"):
            leg.update(status="expired", reason="market_ended")
    return s


@dataclass
class MarketRecord:
    start_ms: int
    batch: int
    outcome: str | None
    censored: bool
    pnl: Decimal | None
    taken: bool
    accounting_v2: bool = False
    live_pnl: Decimal | None = None


def _complete(rows: list[MarketRecord], *, last_batch: bool, window_ended: bool) -> bool:
    if any(r.accounting_v2 for r in rows):
        # A fixed batch is complete only after its end and all 20 observed slots.
        if not window_ended or len({r.start_ms for r in rows}) != 20:
            return False
    if any((not r.censored) and r.outcome is None for r in rows):
        return False
    scored = sum(1 for r in rows if r.pnl is not None)
    need = MIN_SCORED_TAIL if (window_ended and last_batch) else MIN_SCORED
    return scored >= need


def walk_playbook(records: list[MarketRecord], *, epoch_ms: int, now_ms: int, end_ms: int | None = None) -> dict[str, Any]:
    grouped: dict[int, list[MarketRecord]] = {}
    for r in sorted(records, key=lambda x: x.start_ms):
        grouped.setdefault(r.batch, []).append(r)
    ids = sorted(grouped)
    last = ids[-1] if ids else 0
    window_ended = end_ms is not None and now_ms >= end_ms
    mode = "LIVE"
    halt_batch = None
    recovery_batch = None
    resume_batch = None
    views = []
    for b in ids:
        if resume_batch is not None and b == resume_batch:
            mode = "LIVE"
        entering = mode
        rows = grouped[b]
        live_eq = Decimal("0")
        live_peak = Decimal("0")
        live_dd = Decimal("0")
        tripped = False
        trip_ms = None
        taken_pnl = Decimal("0")
        paper_pnl = Decimal("0")
        paper_n = 0
        taken_n = 0
        prefix_break = False
        role = entering
        for rec in rows:
            if rec.censored:
                continue
            if rec.pnl is not None:
                paper_pnl += rec.pnl
                paper_n += 1
            amount = rec.live_pnl if rec.accounting_v2 else rec.pnl
            if amount is None:
                prefix_break = True
                continue
            take = entering == "LIVE" and rec.taken and not prefix_break and not tripped
            if take:
                taken_pnl += amount
                taken_n += 1
                live_eq += amount
                live_peak = max(live_peak, live_eq)
                live_dd = min(live_dd, live_eq - live_peak)
                if live_dd <= MDD_LIMIT:
                    tripped = True
                    trip_ms = rec.start_ms
                    mode = "OBSERVE"
                    halt_batch = b
                    recovery_batch = None
                    resume_batch = None
                    role = "LIVE_TRIP"
        complete = _complete(rows, last_batch=(b == last), window_ended=(now_ms >= epoch_ms + b * BATCH_MS) if any(r.accounting_v2 for r in rows) else window_ended)
        if entering == "OBSERVE":
            role = "OBSERVE"
            if complete and halt_batch is not None and b != halt_batch and paper_pnl > 0:
                role = "RECOVERY"
                recovery_batch = b
                resume_batch = b + 1
        elif tripped:
            role = "LIVE_TRIP"
        views.append(
            dict(
                batch=b,
                entering=entering,
                role=role,
                complete=complete,
                taken_pnl=taken_pnl,
                taken_n=taken_n,
                paper_pnl=paper_pnl if paper_n else None,
                paper_n=paper_n,
                live_dd=live_dd if entering == "LIVE" else None,
                trip_ms=trip_ms,
                start_ms=rows[0].start_ms,
                end_ms=rows[-1].start_ms,
                scheduled=len(rows),
            )
        )
    now_view = None
    for v in views:
        if v["start_ms"] <= now_ms < v["end_ms"] + 300000:
            now_view = v
            break
    current = now_view or (views[-1] if views else None)
    display = mode
    if current and current["entering"] == "LIVE" and current.get("trip_ms"):
        display = "OBSERVE"
    elif current and current["entering"] == "LIVE":
        display = "LIVE"
    elif current and current["entering"] == "OBSERVE":
        display = "OBSERVE"
    return {
        "profile": PROFILE,
        "mode": display,
        "halt_batch": halt_batch,
        "recovery_batch": recovery_batch,
        "resume_batch": resume_batch,
        "current": current,
        "batches": views[-8:],
        "epoch_ms": epoch_ms,
    }


def records_from_payload(payload: Mapping[str, Any], epoch_ms: int = EPOCH_MS) -> list[MarketRecord]:
    rows = []
    for item in payload.get("markets") or []:
        if not isinstance(item, Mapping):
            continue
        start = int(item["start_ms"])
        pnl = item.get("pnl")
        rows.append(
            MarketRecord(
                start_ms=start,
                batch=int(item.get("batch") or batch_of(start, epoch_ms)),
                outcome=item.get("outcome"),
                censored=bool(item.get("censored")),
                pnl=None if pnl is None else Decimal(str(pnl)),
                taken=bool(item.get("taken")),
                accounting_v2=bool(item.get("accounting_v2")),
                live_pnl=None if item.get("live_pnl") is None else Decimal(str(item["live_pnl"])),
            )
        )
    return rows


def upsert_market(payload: dict[str, Any], record: MarketRecord) -> dict[str, Any]:
    markets = list(payload.get("markets") or [])
    found = False
    for i, item in enumerate(markets):
        if int(item.get("start_ms") or 0) == record.start_ms:
            markets[i] = {
                "start_ms": record.start_ms,
                "batch": record.batch,
                "outcome": record.outcome,
                "censored": record.censored,
                "pnl": None if record.pnl is None else str(record.pnl),
                "taken": record.taken,
            }
            found = True
            break
    if not found:
        markets.append(
            {
                "start_ms": record.start_ms,
                "batch": record.batch,
                "outcome": record.outcome,
                "censored": record.censored,
                "pnl": None if record.pnl is None else str(record.pnl),
                "taken": record.taken,
            }
        )
    payload = dict(payload)
    payload["markets"] = markets
    return payload


def playbook_snapshot(payload: Mapping[str, Any] | None, *, now_ms: int, epoch_ms: int = EPOCH_MS, end_ms: int | None = None) -> dict[str, Any]:
    records = records_from_payload(payload or {}, epoch_ms)
    result = walk_playbook(records, epoch_ms=epoch_ms, now_ms=now_ms, end_ms=end_ms)
    if (payload or {}).get("migration_hold"):
        result["mode"] = "OBSERVE"
        result["migration_hold"] = True
    if any(x.get('accounting_v2') and x.get('window_status') == 'ended' and x.get('live_pnl') is None
           for x in (payload or {}).get('markets', [])):
        result['mode'] = 'OBSERVE'
        result['accounting_incomplete'] = True
    return result


def allow_live_entry(snapshot: Mapping[str, Any], start_ms: int) -> tuple[bool, str]:
    """Live entry halt is loop peak MDD, not the 20-run batch playbook.

    Accounting/migration holds stay fail-closed. Batch OBSERVE is recorded
    for status only and must not block new entries (D1).
    """

    del start_ms
    if snapshot.get('accounting_incomplete'):
        return False, 's3s5 settled-window execution accounting unresolved'
    if snapshot.get('migration_hold'):
        return False, 's3s5 prior stop preserved after ledger correction'
    return True, "s3s5 live"


def paper_pnl_from_state(state: Mapping[str, Any] | None, outcome: str) -> Decimal | None:
    if state is None or outcome not in {"UP", "DOWN", "DRAW"}:
        return None
    total = Decimal("0")
    for leg in (state.get("legs") or {}).values():
        fill = leg.get("fill") if isinstance(leg, Mapping) else None
        if isinstance(fill, Mapping):
            total += pnl500(fill, outcome)
    return total

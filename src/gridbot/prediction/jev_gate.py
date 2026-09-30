"""TypeSafe Jev System One Gate Evaluator for CRY3 Prediction.

Reads rolling prediction cache from jev_shadow_lane without adding network latency (<1ms).
Guards against:
1. Directional conflict against strong trend momentum (e.g. betting DOWN in violent bull breakout).
2. High-risk chop / fakeouts where reversal_prob >= 0.60.
3. Fail-open tolerance on stale cache or missing daemon.
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

DEFAULT_RUNTIME_DIR = "/home/jack_shih/cry3/jev_shadow_lane/runtime"
DEFAULT_MAX_AGE_MS = 20000  # 20 seconds
DEFAULT_MIN_CONFLICT_PROB = 0.65
DEFAULT_MIN_CHOP_REVERSAL_PROB = 0.40
DEFAULT_MAX_BUY_PRICE = 0.69


@dataclass
class JevGateVerdict:
    allowed: bool
    reject_reason: str | None = None
    direction: str | None = None
    confidence: float | None = None
    p_up: float | None = None
    p_down: float | None = None
    regime: str | None = None
    reversal_prob: float | None = None
    age_ms: float | None = None
    cache_status: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "allowed": self.allowed,
            "reject_reason": self.reject_reason,
            "direction": self.direction,
            "confidence": self.confidence,
            "p_up": self.p_up,
            "p_down": self.p_down,
            "regime": self.regime,
            "reversal_prob": self.reversal_prob,
            "age_ms": self.age_ms,
            "cache_status": self.cache_status,
        }


class JevGateEvaluator:
    def __init__(
        self,
        *,
        enabled: bool = True,
        runtime_dir: str | Path = DEFAULT_RUNTIME_DIR,
        max_age_ms: int = DEFAULT_MAX_AGE_MS,
        min_conflict_prob: float = DEFAULT_MIN_CONFLICT_PROB,
        min_chop_reversal_prob: float = DEFAULT_MIN_CHOP_REVERSAL_PROB,
        max_buy_price: float = DEFAULT_MAX_BUY_PRICE,
        fail_open_on_stale: bool = True,
    ) -> None:
        self.enabled = enabled
        self.runtime_dir = Path(runtime_dir)
        self.max_age_ms = max_age_ms
        self.min_conflict_prob = min_conflict_prob
        self.min_chop_reversal_prob = min_chop_reversal_prob
        self.max_buy_price = max_buy_price
        self.fail_open_on_stale = fail_open_on_stale
        self.reject_count: int = 0
        self.allow_count: int = 0
        self._last_daemon_sync_time: float = 0.0
        self._daemon_sync_cooldown_sec: float = 10.0
        self._cached_daemon_state: bool | None = None

    def ensure_daemon_started(self) -> bool:
        """Starts cry3-jev-shadow.service via systemctl if not active."""
        try:
            res = subprocess.run(
                ["systemctl", "--user", "is-active", "cry3-jev-shadow.service"],
                capture_output=True, text=True, check=False
            )
            if res.stdout.strip() != "active":
                logger.info("[JEV_GATE] Starting cry3-jev-shadow.service daemon for active loop")
                subprocess.run(
                    ["systemctl", "--user", "start", "cry3-jev-shadow.service"],
                    capture_output=True, text=True, check=False
                )
            self._cached_daemon_state = True
            return True
        except Exception as err:
            logger.warning("[JEV_GATE] Failed to ensure daemon started: %s", err)
            return False

    def stop_daemon(self) -> bool:
        """Stops cry3-jev-shadow.service via systemctl."""
        try:
            res = subprocess.run(
                ["systemctl", "--user", "is-active", "cry3-jev-shadow.service"],
                capture_output=True, text=True, check=False
            )
            if res.stdout.strip() == "active":
                logger.info("[JEV_GATE] Stopping cry3-jev-shadow.service daemon (no active loop)")
                subprocess.run(
                    ["systemctl", "--user", "stop", "cry3-jev-shadow.service"],
                    capture_output=True, text=True, check=False
                )
            self._cached_daemon_state = False
            return True
        except Exception as err:
            logger.warning("[JEV_GATE] Failed to stop daemon: %s", err)
            return False

    def sync_daemon_state(self, has_active_loop: bool, now_sec: float | None = None) -> None:
        """Syncs cry3-jev-shadow daemon state with active loop existence (with cooldown)."""
        curr_time = time.time() if now_sec is None else now_sec
        if self._cached_daemon_state == has_active_loop and (curr_time - self._last_daemon_sync_time) < self._daemon_sync_cooldown_sec:
            return
        self._last_daemon_sync_time = curr_time
        if has_active_loop:
            self.ensure_daemon_started()
        else:
            self.stop_daemon()

    def read_cache(self, symbol: str) -> dict[str, Any] | None:
        path = self.runtime_dir / f"latest_{symbol.upper()}.json"
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except (FileNotFoundError, json.JSONDecodeError, OSError, PermissionError):
            return None

    def evaluate(
        self,
        symbol: str,
        fav_side: str,
        now_ms: int | None = None,
        raw_cache: dict[str, Any] | None = None,
        buy_price: float | None = None,
    ) -> JevGateVerdict:
        if not self.enabled:
            return JevGateVerdict(allowed=True, reject_reason=None)

        if buy_price is not None and buy_price > self.max_buy_price:
            self.reject_count += 1
            return JevGateVerdict(
                allowed=False,
                reject_reason=f"PRICE_EXCEEDS_CAP_{buy_price:.3f}_MAX_{self.max_buy_price:.2f}",
            )

        curr_now_ms = int(time.time() * 1000) if now_ms is None else now_ms
        cache = raw_cache if raw_cache is not None else self.read_cache(symbol)

        if not cache or not isinstance(cache, dict):
            if self.fail_open_on_stale:
                return JevGateVerdict(allowed=True, reject_reason="no_cache_fail_open")
            return JevGateVerdict(allowed=False, reject_reason="no_cache_fail_closed")

        pred = cache.get("prediction")
        if not isinstance(pred, dict):
            if self.fail_open_on_stale:
                return JevGateVerdict(allowed=True, reject_reason="invalid_prediction_fail_open")
            return JevGateVerdict(allowed=False, reject_reason="invalid_prediction_fail_closed")

        # Age calculation: use observed_at or requested_at or written_at
        obs_ms = pred.get("observed_at_ms")
        if obs_ms is None and pred.get("observed_at") is not None:
            obs_ms = int(pred["observed_at"] * 1000)
        if obs_ms is None and cache.get("written_at") is not None:
            obs_ms = int(cache["written_at"] * 1000)

        age_ms = (curr_now_ms - obs_ms) if obs_ms else 999999
        cache_status = "FRESH" if age_ms <= self.max_age_ms else "STALE"

        if cache_status == "STALE":
            if self.fail_open_on_stale:
                return JevGateVerdict(
                    allowed=True,
                    reject_reason="cache_stale_fail_open",
                    age_ms=age_ms,
                    cache_status=cache_status,
                )
            return JevGateVerdict(
                allowed=False,
                reject_reason="cache_stale_fail_closed",
                age_ms=age_ms,
                cache_status=cache_status,
            )

        # Parse Jev prediction values
        direction = str(pred.get("direction") or "").upper()
        p_up = float(pred.get("p_up") or 0.5)
        p_down = float(pred.get("p_down") or 0.5)
        confidence = float(pred.get("direction_confidence") or 0.0)
        regime = str(pred.get("regime") or "").upper()
        reversal_prob = float(pred.get("reversal_prob") or 0.0)

        fav_side_upper = fav_side.upper()

        # Rule 1: Directional Conflict check
        # If strategy wants to bet UP, but Jev strongly predicts DOWN
        if fav_side_upper == "UP":
            if direction == "DOWN" and (p_down >= self.min_conflict_prob or regime == "TREND_DOWN"):
                self.reject_count += 1
                return JevGateVerdict(
                    allowed=False,
                    reject_reason="JEV_DIR_DOWN_MOMENTUM",
                    direction=direction,
                    confidence=confidence,
                    p_up=p_up,
                    p_down=p_down,
                    regime=regime,
                    reversal_prob=reversal_prob,
                    age_ms=age_ms,
                    cache_status=cache_status,
                )

        # If strategy wants to bet DOWN, but Jev strongly predicts UP
        if fav_side_upper == "DOWN":
            if direction == "UP" and (p_up >= self.min_conflict_prob or regime == "TREND_UP"):
                self.reject_count += 1
                return JevGateVerdict(
                    allowed=False,
                    reject_reason="JEV_DIR_UP_MOMENTUM",
                    direction=direction,
                    confidence=confidence,
                    p_up=p_up,
                    p_down=p_down,
                    regime=regime,
                    reversal_prob=reversal_prob,
                    age_ms=age_ms,
                    cache_status=cache_status,
                )

        # Rule 2: High-risk chop / fakeout check
        if reversal_prob >= self.min_chop_reversal_prob and regime in ("CHOP", "VOLATILE_WHIPSAW", "UNCERTAIN", "RANGE"):
            self.reject_count += 1
            return JevGateVerdict(
                allowed=False,
                reject_reason="JEV_CHOP_REVERSAL_RISK",
                direction=direction,
                confidence=confidence,
                p_up=p_up,
                p_down=p_down,
                regime=regime,
                reversal_prob=reversal_prob,
                age_ms=age_ms,
                cache_status=cache_status,
            )

        # Rule 3: Allow
        self.allow_count += 1
        return JevGateVerdict(
            allowed=True,
            reject_reason=None,
            direction=direction,
            confidence=confidence,
            p_up=p_up,
            p_down=p_down,
            regime=regime,
            reversal_prob=reversal_prob,
            age_ms=age_ms,
            cache_status=cache_status,
        )

    def evaluate_late_exit(
        self,
        symbol: str,
        position_side: str,
        current_spot: float,
        reference_price: float,
        remaining_seconds: float,
        current_bid: float | None = None,
        raw_cache: dict[str, Any] | None = None,
    ) -> tuple[bool, str | None]:
        """Evaluates whether to trigger an early protective exit in final 75 seconds.

        Returns (should_exit: bool, reason: str | None).
        """
        if not self.enabled or remaining_seconds > 75.0:
            return False, None

        if current_bid is None or current_bid < 0.20:
            # Book has no liquidity or bid is too low (<0.20), exiting won't save significant principal
            return False, None

        pos_side = position_side.upper()
        spot_diff = current_spot - reference_price

        # Case 1: Spot has already crossed strike price against held position
        if pos_side == "UP" and spot_diff < 0.0:
            return True, f"SPOT_CROSSED_BELOW_STRIKE (diff={spot_diff:.2f})"
        if pos_side == "DOWN" and spot_diff > 0.0:
            return True, f"SPOT_CROSSED_ABOVE_STRIKE (diff=+{spot_diff:.2f})"

        # Case 2: Spot is dangerously close to strike (< 8 USD) and Jev indicates high reversal hazard
        cache = raw_cache if raw_cache is not None else self.read_cache(symbol)
        if cache and isinstance(cache, dict):
            pred = cache.get("prediction")
            if isinstance(pred, dict):
                reversal_prob = float(pred.get("reversal_prob") or 0.0)
                p_up = float(pred.get("p_up") or 0.5)
                p_down = float(pred.get("p_down") or 0.5)

                if abs(spot_diff) < 8.0:
                    if pos_side == "UP" and (reversal_prob >= 0.60 or p_down >= 0.65):
                        return True, f"JEV_HIGH_REVERSAL_HAZARD (rev_prob={reversal_prob:.2f}, diff={spot_diff:.2f})"
                    if pos_side == "DOWN" and (reversal_prob >= 0.60 or p_up >= 0.65):
                        return True, f"JEV_HIGH_REVERSAL_HAZARD (rev_prob={reversal_prob:.2f}, diff={spot_diff:.2f})"

        return False, None


"""Telegram control lane for the isolated Prediction runtime.

The service owns command authorization and promotion confirmation while the
injected runtime owns trading state.  No API key, secret, or dotenv setting is
read here.  The command methods are ordinary async callables, so tests can
exercise them with small fake Update/Runtime objects without a Telegram
network connection.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import secrets
import time
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Any, Callable, Mapping, Protocol, Sequence, runtime_checkable
from zoneinfo import ZoneInfo

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import CallbackQueryHandler, CommandHandler, ContextTypes

from src.gridbot.prediction.c180_batch_gate import BASE_MDD_USDT, V11_MDD_USDT, V11_LOOP_LOSS_USDT
from src.gridbot.prediction.s3s5_pair import (
    amount_lock_note,
    amount_picker_hidden,
    compact_stake_line,
    loop_mdd_limit,
    risk_status_line,
    uses_loop_risk_guards,
)

LOGGER = logging.getLogger("cry3.prediction.telegram")
C180_PROFILE = "c180_favorite_hold_v1"
REGIME_PROFILE = "regime_target6_v1"
REGIME_T61_PROFILE = "regime_target6_1_v1"
REGIME_T62_PROFILE = "regime_target6_2_v1"
REGIME_T63_PROFILE = "regime_target6_3_v1"
REGIME_T63A_PROFILE = "regime_target6_3a_v1"
REGIME_T63B_PROFILE = "regime_target6_3b_v1"
REGIME_T65_PROFILE = "regime_target6_5_v1"
REGIME_T67_PROFILE = "regime_target6_7_v1"
REGIME_T67A_PROFILE = "regime_target6_7a_v1"
REGIME_T67B_PROFILE = "regime_target6_7b_v1"
REGIME_T67C_PROFILE = "regime_target6_7c_v1"
REGIME_T67D_PROFILE = "regime_target6_7d_v1"
REGIME_T68_PROFILE = "regime_target6_8_v1"
REGIME_T68A_PROFILE = "regime_target6_8a_v1"
REGIME_T69_PROFILE = "regime_target6_9_v1"
REGIME_T69A_PROFILE = "regime_target6_9a_v1"
MULTI_MARKET_LANE_PROFILES = frozenset({REGIME_T67C_PROFILE, REGIME_T69_PROFILE, REGIME_T69A_PROFILE})
REGIME_PROFILES = frozenset((REGIME_PROFILE, REGIME_T61_PROFILE, REGIME_T62_PROFILE,
                             REGIME_T63_PROFILE, REGIME_T63A_PROFILE, REGIME_T63B_PROFILE, REGIME_T65_PROFILE, REGIME_T67_PROFILE, REGIME_T67A_PROFILE, REGIME_T67B_PROFILE, REGIME_T67C_PROFILE, REGIME_T67D_PROFILE, REGIME_T68_PROFILE, REGIME_T68A_PROFILE, REGIME_T69_PROFILE, REGIME_T69A_PROFILE))
REGIME_RISK_TEXT = "固定1 USDT｜累計PnL ≤ -6 或固定20場MDD ≥ 3.5停單｜跨Loop保存、不自動解鎖"


def _regime_risk_text(profile: str, unit: Any) -> str:
    if profile not in (REGIME_T62_PROFILE, REGIME_T63_PROFILE, REGIME_T63A_PROFILE, REGIME_T63B_PROFILE, REGIME_T65_PROFILE, REGIME_T67_PROFILE, REGIME_T67A_PROFILE, REGIME_T67B_PROFILE, REGIME_T67C_PROFILE, REGIME_T67D_PROFILE, REGIME_T68_PROFILE, REGIME_T68A_PROFILE, REGIME_T69_PROFILE, REGIME_T69A_PROFILE):
        return REGIME_RISK_TEXT
    try:
        stake = Decimal(str(unit))
        if stake not in (Decimal("1"), Decimal("2"), Decimal("3")):
            raise ValueError("unsupported T6.2 stake")
    except (ArithmeticError, TypeError, ValueError):
        return "T6.2／T6.3／T6.3a／T6.3b／T6.5 金額或風控資料待核對"
    amount = int(stake)
    loop_note = (f"本輪MDD≥{Decimal('3.5') * stake} USDT（1U等值3.5）停新進場｜"
                 "跨Loop停單不自動解鎖；整輪回撤鎖僅限該輪"
                 if profile in (REGIME_T63B_PROFILE, REGIME_T65_PROFILE, REGIME_T67_PROFILE, REGIME_T67A_PROFILE, REGIME_T67B_PROFILE, REGIME_T67C_PROFILE, REGIME_T67D_PROFILE, REGIME_T68_PROFILE, REGIME_T68A_PROFILE, REGIME_T69_PROFILE, REGIME_T69A_PROFILE) else "停單跨Loop保存、不自動解鎖")
    return (f"T6.2／T6.3／T6.3a／T6.3b／T6.5／T6.7／T6.7a／T6.7b／T6.7c／T6.7d／T6.8／T6.8a／T6.9／T6.9b 每筆{amount} USDT｜純{amount}U成交時：固定20場MDD≥{Decimal('3.5') * stake} USDT、"
            f"跨Loop累計PnL≤-{6 * amount} USDT 停新進場｜"
            "混合1/2/3U成交時按每筆實際投入折算1U等值（20場MDD≥3.5、累計PnL≤-6）；"
            + loop_note)


def _c180_risk_text(unit: Any, *, policy_version: str = "1.1") -> str:
    """Describe the active C180 20-market gate instead of the legacy loop cap."""

    try:
        stake = Decimal(str(unit))
        if stake not in (Decimal("1"), Decimal("2"), Decimal("3")):
            raise ValueError("unsupported C180 stake")
        threshold = str(V11_MDD_USDT if policy_version == "1.1" else BASE_MDD_USDT * stake)
    except (ArithmeticError, TypeError, ValueError):
        threshold = "未設定"
    if policy_version == "1.1":
        return (f"C180 V1.1｜買價 ≤0.90｜每20場 MDD 達 {threshold} USDT 停新 BUY"
                f"｜整輪 PnL ≤{V11_LOOP_LOSS_USDT} USDT 停新 BUY")
    return f"C180 V1｜每20場 MDD 嚴格超過 {threshold} USDT，停該段新 BUY"

TAIPEI = ZoneInfo("Asia/Taipei")
MAX_LOOP_MARKETS = 200
DEFAULT_LOOP_MARKETS = 10
LIVE_CALLBACK_PREFIX = "predict_shadow:"
LANE_CALLBACK_PREFIX = "predict_lane:"
ORDER_UNIT_CALLBACK_PREFIX = "predict_amount:"
HARD_STOP_CALLBACK_PREFIX = "predict_hard_stop:"
CANCEL_LOOP_CALLBACK_PREFIX = "predict_cancel:"
MONITOR_CALLBACK_PREFIX = "predict_monitor:"
SELECTABLE_LANES = (
    ('regime_target6_9a_v1', 'T6.9b T6.7c七路＋First UP≥5bp Live＋Flat Shadow（BTC／ETH／BNB；1/2/3U）'),
    ('regime_target6_9_v1', 'T6.9 T6.8a Live＋Flat Shadow（BTC／ETH／BNB；1/2/3U）'),
    ('regime_target6_7d_v1', 'T6.7d 原七路＋Flat補位 Live（1/2/3U）'),
    ('regime_target6_8a_v1', 'T6.8a First UP≥5bp＋Reference 180s Live（1/2/3U）'),
    ('regime_target6_8_v1', 'T6.8 核心＋Reference 180s Live（1/2/3U）'),
    ('regime_target6_7c_v1', 'T6.7c 核心＋增量 Live（每20 run報表；1/2/3U）'),
    ('regime_target6_7b_v1', 'T6.7b 核心＋增量 Live（送單優化；1/2/3U）'),
    ('regime_target6_7a_v1', 'T6.7a 舊核心＋C-UP／淺回撤 Live（1/2/3U）'),
    ('regime_target6_5_v1', 'Regime T6.5 A／flat／M4／M6 Shadow（1/2/3U）'),
    ('regime_target6_7_v1', 'Regime T6.7 四策略 Live 驗證（1/2/3U）'),
    ('regime_target6_3b_v1', 'Regime T6.3b B／補位Shadow＋整輪回撤（1/2/3U）'),
    ('regime_target6_3a_v1', 'Regime T6.3a 補位Shadow（1/2/3U）'),
    ('regime_target6_3_v1', 'Regime T6.3 分歧／順勢／淨跌（1/2/3U）'),
    ('regime_target6_v1', 'Regime T6 市況分流（固定1U）'),
    ('regime_target6_1_v1', 'Regime T6.1 補位（固定1U／研究版）'),
    ('regime_target6_2_v1', 'Regime T6.2 價格護欄（1/2/3U／研究版）'),
)

ETH_ONLY_LANE_PROFILES = frozenset({"fav_only_v4"})
BTC_ONLY_LANE_PROFILES = frozenset({REGIME_T61_PROFILE, REGIME_T62_PROFILE,
                                    REGIME_T63_PROFILE, REGIME_T63A_PROFILE, REGIME_T63B_PROFILE, REGIME_T65_PROFILE, REGIME_T67_PROFILE, REGIME_T67A_PROFILE, REGIME_T67B_PROFILE, REGIME_T67C_PROFILE, REGIME_T67D_PROFILE, REGIME_T68_PROFILE, REGIME_T68A_PROFILE, REGIME_T69_PROFILE, REGIME_T69A_PROFILE})


def selectable_lanes_for_market(market_symbol: str | None) -> tuple[tuple[str, str], ...]:
    """Expose only T6 family lanes; reject retired callback selections too."""
    sym = str(market_symbol or "").strip().upper()
    is_eth = sym.startswith("ETH")
    lanes: list[tuple[str, str]] = []
    for profile, label in SELECTABLE_LANES:
        if sym in {"ETHUSDT", "BNBUSDT"} and profile not in MULTI_MARKET_LANE_PROFILES:
            continue
        if profile in ETH_ONLY_LANE_PROFILES and not is_eth:
            continue
        if profile in BTC_ONLY_LANE_PROFILES and not sym.startswith("BTC") and not (profile in MULTI_MARKET_LANE_PROFILES and sym in {"ETHUSDT", "BNBUSDT"}):
            continue
        lanes.append((profile, label))
    return tuple(lanes)


WR_MONITOR_LANE_LABELS = {
    "regime_target6_9a_v1": "T6.9b T6.7c七路＋First UP≥5bp Live＋Flat Shadow／每20 run總結",
    "regime_target6_9_v1": "T6.9 T6.8a Live＋Flat Shadow／每20 run總結",
    "regime_target6_8_v1": "T6.8 核心＋Reference 180s Live／每20 run總結",
    "regime_target6_8a_v1": "T6.8a First UP≥5bp＋Reference 180s Live／每20 run總結",
    "regime_target6_7d_v1": "T6.7d 原七路＋Flat補位 Live／每20 run總結",
    "regime_target6_7c_v1": "T6.7c 核心＋增量 Live／每20 run總結",
    "regime_target6_7b_v1": "T6.7b 核心＋增量 Live／兩策略 Shadow",
    "regime_target6_7a_v1": "T6.7a 核心＋增量 Live／兩策略 Shadow",
    "regime_target6_5_v1": "Regime T6.5 篩選＋M4／M6 Shadow",
    "regime_target6_7_v1": "Regime T6.7 外部先行／基差校正／淺回撤 Live",
    "regime_target6_3b_v1": "Regime T6.3b B／補位Shadow",
    "regime_target6_3a_v1": "Regime T6.3a 補位Shadow",
    "regime_target6_3_v1": "Regime T6.3 A/B/C",
    "balanced_hold": "Balanced",
    "quality_hold": "Quality",
    "quality_hold_v2": "V2",
    "quality_hold_v3_profit1": "V3",
    "quality_hold_v3_loss_guard_v2": "V3 Loss Guard V2",
    "quality_hold_v4_rescue": "V4",
    "quality_hold_v5_pnl": "V5",
    "quality_hold_v6_a_staged": "V6A",
    "quality_hold_v6_balanced_shadow": "V6-Balanced",
    "late_maturity_v1": "Late-Maturity V1",
    "regime_value_v7": "V7",
    "s3s5_pair_v1": "R3＋FAV",
    "fav_only_v1": "FAV（獨立）",
    "fav_only_v2": "FAV only V2",
    "fav_only_v3": "FAV V3（實驗版）",
    "fav_only_v4": "FAV V4（ETH 專屬）",
    "fav_p3": "P3（FAV＋閘｜無R3）",
    "regime_target6_v1": "Regime T6 市況分流",
    "regime_target6_1_v1": "Regime T6.1 補位",
    "regime_target6_2_v1": "Regime T6.2 價格護欄",
    "c180_favorite_hold_v1": "C180 Favorite Hold",
    "regime_value_v8_calibrated": "V8-Calibrated",
    "complete_set_arb_v1": "Complete-Set V1",
    "control": "Control",
}
REGIME_STATUS_META = {
    "GREEN": ("🟢", "CALM", "V3–V5 新初始買入允許"),
    "YELLOW": ("🟡", "WATCH", "V3–V5 新初始買入允許"),
    "RED": ("🔴", "WHIPSAW", "只阻擋 V3–V5 新初始買入"),
    "WAIT_DATA": ("⚪", "WAIT DATA", "資料不足；只阻擋 V3–V5 新初始買入"),
}
READINESS_META = {
    "READY": ("✅", "READY"),
    "WATCH": ("🟡", "WATCH"),
    "AVOID": ("⛔", "AVOID"),
    "WAIT_DATA": ("⏳", "WAIT DATA"),
    "OBSERVE": ("⚪", "OBSERVE"),
}


@runtime_checkable
class PredictionRuntime(Protocol):
    """Minimal manager contract used by the Telegram lane.

    Implementations may expose synchronous methods in tests; the service
    accepts either regular values or awaitables.  Only the method names are a
    contract—runtime-specific result payloads remain mapping-friendly.
    """

    async def status(self) -> Any: ...
    async def start_loop(self, count: int) -> Any: ...
    async def one_run(self) -> Any: ...
    async def loop_pnl(self) -> Any: ...
    async def select_strategy(self, profile: str) -> Any: ...
    async def select_order_unit(self, value: Decimal | str | int) -> Any: ...
    async def stop_loop(self) -> Any: ...
    async def cancel_loop(self) -> Any: ...
    async def pause(self) -> Any: ...
    async def resume(self) -> Any: ...
    async def reset_hard_stop_once(self, reason: str = "telegram operator hard-stop reset") -> Any: ...
    async def risk(self) -> Any: ...
    async def reconcile(self) -> Any: ...
    async def set_shadow_mode(self, enabled: bool) -> Any: ...
    async def promotion_gate(self) -> Any: ...

    async def preflight(self, **kwargs: Any) -> Any: ...


@dataclass(frozen=True)
class PromotionGate:
    """Normalized result of the live-promotion gate."""

    passed: bool
    reasons: tuple[str, ...] = ()
    evidence: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class _PendingLiveConfirmation:
    token: str
    chat_id: str
    expires_at_ms: int


@dataclass(frozen=True)
class _PendingHardStopReset:
    token: str
    chat_id: str
    expires_at_ms: int


@dataclass(frozen=True)
class _PendingLoopCancel:
    token: str
    chat_id: str
    expires_at_ms: int


def _now_ms() -> int:
    return int(time.time() * 1000)


def _chat_id(update: Any) -> str | None:
    chat = getattr(update, "effective_chat", None)
    value = getattr(chat, "id", None)
    if value is None:
        query = getattr(update, "callback_query", None)
        message = getattr(query, "message", None) if query is not None else None
        value = getattr(getattr(message, "chat", None), "id", None)
    return str(value) if value is not None else None


def _is_awaitable(value: Any) -> bool:
    return inspect.isawaitable(value)


def _redact(value: Any) -> Any:
    """Prevent accidental credential echoes in manager status payloads."""

    if isinstance(value, Mapping):
        result: dict[str, Any] = {}
        for key, item in value.items():
            key_text = str(key).lower()
            if any(marker in key_text for marker in ("api_secret", "api_key", "private_key", "secret_key")):
                result[str(key)] = "[REDACTED]"
            else:
                result[str(key)] = _redact(item)
        return result
    if isinstance(value, (list, tuple)):
        return [_redact(item) for item in value]
    if hasattr(value, "value") and not isinstance(value, (str, bytes)):
        try:
            return _redact(value.value)
        except Exception:  # pragma: no cover - defensive for exotic enums
            return str(value)
    return value


_FIELD_LABELS = {
    "state": "執行狀態",
    "mode": "目前模式",
    "effective_mode": "實際模式",
    "live_armed": "Live 已確認",
    "strategy_profile": "策略設定",
    "loop_strategy_profile": "目前 Loop 策略",
    "next_strategy_profile": "下一個 Loop 策略",
    "strategy_queued": "已排程下一個 Loop",
    "live_rearm_required": "需重新確認 Live",
    "previous_loop_closed": "舊 Loop 已結束",
    "configured_live_enabled": "Live 功能設定",
    "worker_available": "Worker 可用",
    "orders_enabled": "實際下單",
    "live_capability": "Live 能力",
    "fail_closed": "安全鎖定",
    "accept_new_markets": "接受新市場",
    "allow_new_orders": "允許新訂單",
    "allow_new_buys": "允許新買入",
    "allow_reductions": "允許減少部位",
    "loop_cancel_requested": "Loop 取消中",
    "loop_cancelled": "Loop 已取消",
    "cancelled_loop_id": "已取消 Loop ID",
    "cancelled_campaigns": "已取消市場數",
    "cancelled_orders": "已取消訂單數",
    "cancelled_intents": "已取消 intent 數",
    "target_markets": "目標市場數",
    "active_campaigns": "進行中市場",
    "heartbeat": "系統心跳",
    "shadow_reasons": "Shadow 原因",
    "hard_stop_latched": "Hard Stop 鎖定",
    "rate_limit": "API 速率限制",
    "preflight": "Live 前置檢查",
    "worker_heartbeat": "Worker 心跳",
    "api": "API 連線",
    "feed": "行情連線",
    "db_write": "資料庫寫入",
    "clock": "系統時鐘",
    "last_error": "最近錯誤",
    "action_denied": "操作被拒絕",
    "reason": "原因",
    "reasons": "原因",
    "one_run": "單筆試跑",
    "one_run_target": "單筆目標數",
    "stop_requested": "已收到停止要求",
    "paused": "已暫停",
    "promotion": "升級檢查",
    "passed": "檢查通過",
    "eligible": "符合資格",
    "authorized": "已授權",
    "allow_trading": "允許交易",
    "action": "風控動作",
    "unresolved": "未解決數量",
    "matched": "已配對數量",
    "orders": "訂單數",
    "known": "資料已載入",
    "campaign_id": "市場/活動 ID",
    "status": "狀態",
    "running": "正在執行",
    "loop_limit": "市場迴圈上限",
    "completed": "已完成市場數",
    "remaining": "剩餘市場數",
    "shadow_requested": "要求 Shadow",
    "promotion_eligible": "符合 Live 升級資格",
    "requested_live": "要求 Live",
    "checked": "已完成檢查",
    "checked_at_ms": "檢查時間",
    "config_hash": "設定版本",
    "wallet_address": "錢包地址",
    "wallet_id": "錢包 ID",
    "account_type": "帳戶類型",
    "order_type": "訂單類型",
    "time_in_force": "有效期限",
    "order_unit_usdt": "單筆金額（USDT）",
    "next_order_unit_usdt": "下一個 Loop 單筆金額（USDT）",
    "order_unit_changed": "單筆金額已變更",
    "order_unit_queued": "單筆金額已排程",
    "daily_loss_limit": "每日虧損上限",
    "next_loop_loss_limit": "下一個 Loop 虧損上限",
    "daily_net_pnl": "當日淨損益",
    "loop_net_pnl": "本輪淨損益",
    "consecutive_losses": "連續虧損次數",
    "order_attempts": "下單嘗試次數",
    "buy_count": "買入次數",
    "soft_cooldown_until_ms": "冷卻截止時間",
    "markets_seen": "看見市場數",
    "markets_completed": "完成市場數",
    "last_api_ok_at_ms": "最近 API 成功時間",
    "last_db_write_at_ms": "最近資料庫寫入時間",
    "error": "錯誤",
    "started": "已啟動",
    "stopped": "已停止",
    "shadow": "Shadow 狀態",
    "live": "Live 狀態",
    "total_loop_pnl": "整體 Loop PnL",
    "current_loop_pnl": "目前 Loop PnL",
    "active_loop_id": "目前 Loop ID",
    "loop_count": "Loop 紀錄數",
    "loops": "Loop 紀錄",
    "loop_id": "Loop ID",
    "target": "目標筆數",
    "pnl": "Loop PnL",
    "created_at_ms": "建立時間",
    "updated_at_ms": "更新時間",
    "loop_loss_limit": "Loop 虧損上限",
    "loop_loss_limit_reached": "已觸發 Loop 虧損停止",
    "result": "結果",
    "progress": "執行進度",
    "protection": "保護機制",
    "funding_source": "資金來源",
    "balance_account_type": "餘額帳戶",
    "sas_verified": "SAS 已驗證",
    "available_balance_display": "可用餘額",
    "remaining_daily_limit": "今日額度",
    "wallet_balances": "錢包餘額",
    "wallet_balances_error": "錢包餘額錯誤",
}
_VALUE_LABELS = {
    "SHADOW": "Shadow（觀察模式）",
    "LIVE": "Live（實際交易）",
    "HARD_STOP": "Hard Stop（風控停止）",
    "RUNNING": "執行中",
    "CANCELLING": "取消處理中",
    "DONE": "已完成",
    "CANCELLED": "已取消",
    "IDLE": "閒置",
    "OPEN": "開放",
    "TRADING": "交易中",
    "FAILED": "失敗",
    "CONFIRMED": "已確認",
    "SETTLED": "已結算",
    "balanced_hold": "balanced_hold（平衡持有）",
    "quality_hold": "quality_hold（品質持有・Live）",
    "quality_hold_v2": "quality_hold_v2（品質持有 V2）",
    "quality_hold_v3_profit1": "quality_hold_v3_profit1（品質獲利 V3）",
    "quality_hold_v3_loss_guard_v2": "quality_hold_v3_loss_guard_v2（V3 防虧・Shadow）",
    "quality_hold_v4_rescue": "quality_hold_v4_rescue（反轉減損 V4・Live）",
    "quality_hold_v5_pnl": "quality_hold_v5_pnl（PnL 優先 V5・Live）",
    "quality_hold_v6_a_staged": "quality_hold_v6_a_staged（A級分段 1+1・Live）",
    "quality_hold_v6_balanced_shadow": "quality_hold_v6_balanced_shadow（平衡放寬・Shadow）",
    "late_maturity_v1": "late_maturity_v1（成熟窗口・Shadow）",
    "regime_value_v7": "regime_value_v7（多市況價值 V7・Live）",
    "regime_value_v8_calibrated": "regime_value_v8_calibrated（校準價值・Shadow）",
    "complete_set_arb_v1": "complete_set_arb_v1（完整配對套利・Shadow）",
    "control": "control（基準）",
    "lane_a": "lane A",
    "lane_b": "lane B",
    "BUY_INITIAL": "初始買入",
    "BUY_ADD": "追加買入",
    "HOLD": "持有",
    "SELL": "賣出",
    "REDEEM": "兌付",
    "SKIP": "跳過",
    "UNKNOWN": "未知",
    "LOSS_LIMIT": "已因 Loop 虧損上限停止",
    "live preflight failed": "Live 前置檢查未通過",
    "authoritative live preflight failed": "Live 前置檢查未通過",
    "hard stop is latched": "Hard Stop 已鎖定",
    "same-day persisted hard stop is latched": "今日 Hard Stop 已鎖定",
    "a prediction loop is already running": "已有 Loop 執行中",
    "a persisted prediction loop is already running": "已有未完成的 Loop",
}


def _human_label(key: Any) -> str:
    text = str(key)
    return _FIELD_LABELS.get(text, text.replace("_", " "))


def _human_scalar(value: Any) -> str:
    if value is None:
        return "未提供"
    if isinstance(value, bool):
        return "是" if value else "否"
    if isinstance(value, str):
        return _VALUE_LABELS.get(value, _VALUE_LABELS.get(value.upper(), value))
    return str(value)


def _format_mapping(value: Mapping[str, Any], indent: int = 0) -> list[str]:
    lines: list[str] = []
    prefix = " " * indent
    for key, item in value.items():
        label = _human_label(key)
        if isinstance(item, Mapping):
            lines.append(f"{prefix}{label}：")
            lines.extend(_format_mapping(item, indent + 2))
        elif isinstance(item, (list, tuple)):
            if not item:
                lines.append(f"{prefix}{label}：無")
            elif all(not isinstance(entry, (Mapping, list, tuple)) for entry in item):
                lines.append(f"{prefix}{label}：")
                lines.extend(f"{prefix}  • {_human_scalar(entry)}" for entry in item)
            else:
                lines.append(f"{prefix}{label}：")
                for index, entry in enumerate(item, 1):
                    if isinstance(entry, Mapping):
                        lines.append(f"{prefix}  #{index}")
                        lines.extend(_format_mapping(entry, indent + 4))
                    else:
                        lines.append(f"{prefix}  • {_human_scalar(entry)}")
        else:
            lines.append(f"{prefix}{label}：{_human_scalar(item)}")
    return lines


def _compact_reason(value: Mapping[str, Any]) -> str | None:
    """Extract one short, user-actionable reason from a runtime result."""

    candidates: list[Mapping[str, Any]] = [value]
    for key in ("preflight", "promotion"):
        nested = value.get(key)
        if isinstance(nested, Mapping):
            candidates.append(nested)
    for candidate in candidates:
        reason = candidate.get("reason")
        if reason:
            return _human_scalar(reason)
        reasons = candidate.get("reasons")
        if isinstance(reasons, str) and reasons:
            return _human_scalar(reasons)
        if isinstance(reasons, (list, tuple)):
            items = [_human_scalar(item) for item in reasons if item]
            if items:
                return "；".join(items[:2])
    return None


def _pending_pnl_lines(value: Mapping[str, Any]) -> list[str]:
    try:
        position_count = int(value.get("pending_position_count") or 0)
        settlement_count = int(value.get("pending_settlement_count") or 0)
        wait_seconds = int(value.get("pending_settlement_wait_seconds") or 0)
    except (TypeError, ValueError):
        position_count = settlement_count = wait_seconds = 0
    if position_count <= 0 and settlement_count <= 0:
        return []
    lines = [
        f"未結算部位：{position_count}（已過市場結束 {settlement_count}）",
        f"未結算保守預估：{_human_scalar(value.get('pending_estimated_pnl', '0'))} USDT",
        f"含未結算保守值：{_human_scalar(value.get('loop_pnl_with_pending', value.get('loop_net_pnl', '0')))} USDT",
    ]
    if wait_seconds > 0:
        lines.append(f"最久結算等待：{wait_seconds} 秒")
    if value.get("pending_settlement_hold"):
        lines.append("待結算保護：已暫停開啟下一個 campaign")
    return lines


def _lane_wr_monitor_lines(value: Mapping[str, Any]) -> list[str]:
    monitor = value.get("lane_wr_monitor")
    if not isinstance(monitor, Mapping):
        return []
    lanes = monitor.get("lanes")
    if not isinstance(lanes, (list, tuple)) or not lanes:
        return []
    try:
        default_window = int(monitor.get("window_runs") or 20)
    except (TypeError, ValueError):
        default_window = 20
    order_unit = monitor.get("order_unit_usdt", value.get("order_unit_usdt"))
    basis = f"（基準 {_human_scalar(order_unit)} USDT）" if order_unit not in (None, "") else ""
    lines = [f"最近 {default_window} Run 監控{basis}："]
    for item in lanes:
        if not isinstance(item, Mapping):
            continue
        profile = str(item.get("profile") or "unknown")
        label = WR_MONITOR_LANE_LABELS.get(profile, profile)
        source = str(item.get("source") or "SHADOW").upper()
        try:
            runs = int(item.get("runs") or 0)
            window = int(item.get("window_runs") or default_window)
            wins = int(item.get("wins") or 0)
            losses = int(item.get("losses") or 0)
            breakevens = int(item.get("breakevens") or 0)
            no_trades = int(item.get("no_trades") or 0)
            filled_runs = int(item.get("filled_runs") or wins + losses + breakevens)
        except (TypeError, ValueError):
            continue
        raw_rate = item.get("win_rate")
        try:
            rate = f"{Decimal(str(raw_rate)):.1f}%" if raw_rate is not None else "—"
        except (ArithmeticError, TypeError, ValueError):
            rate = "—"
        raw_fill_rate = item.get("fill_rate")
        try:
            fill_rate = (
                f"{Decimal(str(raw_fill_rate)):.1f}%" if raw_fill_rate is not None else "—"
            )
        except (ArithmeticError, TypeError, ValueError):
            fill_rate = "—"
        try:
            pnl = Decimal(str(item.get("pnl_usdt") or "0"))
            pnl_text = "0.0000" if pnl == 0 else f"{pnl:+.4f}"
        except (ArithmeticError, TypeError, ValueError):
            pnl_text = "—"
        pnl_label = "PnL" if source == "LIVE" else "Est.PnL"
        readiness = str(item.get("readiness") or "OBSERVE").upper()
        ready_icon, ready_label = READINESS_META.get(readiness, ("⚪", readiness))
        required_rate = item.get("required_win_rate")
        try:
            required_text = (
                f"BE≥{Decimal(str(required_rate)):.1f}%" if required_rate not in (None, "") else "BE—"
            )
        except (ArithmeticError, TypeError, ValueError):
            required_text = "BE—"
        details = f"{wins}W/{losses}L"
        if breakevens:
            details += f"/{breakevens}平"
        lines.append(
            f"  • {ready_icon} {label} [{source}] {ready_label}｜WR {rate}｜{required_text}｜"
            f"Fill {fill_rate} ({filled_runs}/{runs})｜"
            f"{pnl_label} {pnl_text} USDT｜{details}｜"
            f"No-fill {no_trades}｜{runs}/{window} run"
        )
    return lines


def _market_regime_lines(value: Mapping[str, Any], *, compact: bool = False) -> list[str]:
    monitor = value.get("market_regime_monitor")

    def metric_text(raw: Any) -> str:
        try:
            return f"{Decimal(str(raw)):.1f}" if raw not in (None, "") else "—"
        except (ArithmeticError, TypeError, ValueError):
            return "—"

    def age_text(raw: Any) -> str:
        try:
            return f"{max(0, int(raw)) // 1000}s前" if raw not in (None, "") else "—"
        except (TypeError, ValueError):
            return "—"

    def coverage_text(
        monitor_value: Mapping[str, Any],
        kind: str,
        fallback_valid: Any,
        fallback_expected: Any,
    ) -> str:
        direct_valid = monitor_value.get(f"valid_{kind}_slots")
        direct_expected = monitor_value.get(f"expected_{kind}_slots")
        coverage = monitor_value.get(f"{kind}_coverage")
        if isinstance(coverage, Mapping):
            direct_valid = coverage.get("valid", direct_valid)
            direct_expected = coverage.get("expected", direct_expected)
        if isinstance(monitor_value.get("valid_coverage"), Mapping):
            direct_valid = monitor_value["valid_coverage"].get(kind, direct_valid)
        if isinstance(monitor_value.get("expected_coverage"), Mapping):
            direct_expected = monitor_value["expected_coverage"].get(kind, direct_expected)
        try:
            valid = int(direct_valid if direct_valid is not None else fallback_valid)
            expected = int(direct_expected if direct_expected is not None else fallback_expected)
            return f"{valid}/{expected}"
        except (TypeError, ValueError):
            return "—"

    current = monitor.get("current_market") if isinstance(monitor, Mapping) else None
    current = current if isinstance(current, Mapping) else {}
    if current:
        current_readiness = str(current.get("readiness") or "WAIT_DATA").upper()
        current_ready = bool(current.get("ready")) or current_readiness == "READY"
        current_state = str(current.get("status") or "WAIT_DATA").upper()
        if current_ready:
            current_icon, current_label, _ = REGIME_STATUS_META.get(
                current_state, ("⚪", current_state, "持續觀察")
            )
        else:
            current_icon, current_label = "⏳", "資料收集中"
        current_path_text = metric_text(current.get("path_bps"))
        try:
            current_quotes = int(current.get("quote_count") or 0)
        except (TypeError, ValueError):
            current_quotes = 0
        current_age_ms = current.get("quote_age_ms")
        if current_age_ms in (None, "") and current.get("latest_quote_at_ms"):
            try:
                current_age_ms = max(0, int(time.time() * 1000) - int(current["latest_quote_at_ms"]))
            except (TypeError, ValueError):
                current_age_ms = None
        current_source = str(current.get("source") or "").upper()
        current_source_label = {
            "LIVE": "Live",
            "SHADOW_OBSERVER": "Observer",
        }.get(current_source, current_source or "—")
        if current_source == "SHADOW_OBSERVER":
            current_line = (
                f"目前市場：⏸ 待命｜無 Live market（Observer 監控中｜"
                f"報價 {current_quotes} 筆｜最新 {age_text(current_age_ms)}）"
            )
        else:
            current_line = (
                f"目前市場：{current_icon} {current_label}｜報價 {current_quotes} 筆｜"
                f"最新 {age_text(current_age_ms)}｜Path {current_path_text} bps｜{current_source_label}"
            )
    else:
        current_line = "目前市場：⏸ 待命｜沒有 ACTIVE Live market（目前無可交易市場）"

    if not isinstance(monitor, Mapping):
        lines = [current_line, "市場監控：⏳ 尚未取得資料"]
        return lines

    state = str(monitor.get("status") or "WAIT_DATA").upper()
    icon, label, action = REGIME_STATUS_META.get(state, ("⚪", state, "持續觀察"))

    slow_path_text = metric_text(monitor.get("median_path_bps"))
    fast_path_text = metric_text(monitor.get("fast_median_path_bps"))
    slow_coverage_text = coverage_text(
        monitor,
        "slow",
        monitor.get("markets_with_quotes", 0),
        monitor.get("window_runs", 20),
    )
    fast_coverage_text = coverage_text(monitor, "fast", 0, monitor.get("fast_window_runs", 5))
    source = str(monitor.get("source") or "—")
    freshness_text = age_text(monitor.get("latest_settled_age_ms"))
    if monitor.get("latest_settled_fresh") is False:
        freshness_text += "（stale）"
    blocks_initial = bool(
        monitor.get("gate_block_new_entries", state in {"RED", "WAIT_DATA"})
    )
    initial_gate_text = "阻擋" if blocks_initial else "允許"
    jump_stop = value.get("adaptive_jump_stop")
    jump_stop = jump_stop if isinstance(jump_stop, Mapping) else {}
    jump_active = bool(value.get("adaptive_jump_stop_active"))
    if compact:
        lines = [
            current_line,
            f"市場監控：{icon} {label}｜Path20 {slow_path_text} bps｜Path5 {fast_path_text} bps",
            f"監控資料：20槽 {slow_coverage_text}｜5槽 {fast_coverage_text}｜最新結算 {freshness_text}｜{source}",
            f"V3–V5 新初始單：{initial_gate_text}",
        ]
        if jump_active:
            lines.append(f"短窗跳停：🛑 {_human_scalar(jump_stop.get('trigger', 'ACTIVE'))}｜新單已停止")
        return lines
    lines = [
        current_line,
        f"市場監控：{icon} {label}｜{action}",
        f"Path20 {slow_path_text} bps｜Path5 {fast_path_text} bps",
        f"監控資料：20槽 {slow_coverage_text}｜5槽 {fast_coverage_text}｜最新結算 {freshness_text}｜來源 {source}",
        f"V3–V5 新初始單：{initial_gate_text}",
        "市場監控只影響 V3–V5 新初始單；既有部位仍會結算。",
    ]
    if jump_active:
        lines.extend(
            [
                "",
                f"短窗跳停：🛑 ACTIVE｜{_human_scalar(jump_stop.get('trigger', 'LOSS_CLUSTER'))}",
                "處置：停止新市場與新買單；既有部位仍可減倉、結算。需冷卻後手動 /predict_resume。",
            ]
        )
    return lines


def format_monitor_result(result: Any) -> str:
    """Format the standalone market/lane monitor without wallet noise."""

    if not isinstance(result, Mapping):
        return "【市況監控】\n\n資料讀取失敗。"
    value = _redact(result)
    lines = _market_regime_lines(value)
    lane_lines = _lane_wr_monitor_lines(value)
    if lane_lines:
        lines.append("")
        lines.extend(lane_lines)
    body = "\n".join(lines)
    if len(body) > 3900:
        body = body[:3890] + "\n…"
    return f"【市況監控】\n\n{body}"


def _monitor_alert_text(value: Mapping[str, Any], state: str) -> str:
    transition = "市況恢復" if state in {"GREEN", "YELLOW"} else "市況警示"
    return f"【{transition}】\n\n" + "\n".join(_market_regime_lines(value, compact=True))


def _jump_stop_alert_text(value: Mapping[str, Any]) -> str:
    guard = value.get("adaptive_jump_stop")
    guard = guard if isinstance(guard, Mapping) else {}
    window = guard.get("risk_window") if isinstance(guard.get("risk_window"), Mapping) else {}
    latest = window.get("latest") if isinstance(window.get("latest"), Mapping) else {}
    trigger_labels = {
        "HEAVY_WHIPSAW_LOSS": "同一 Loop 累計兩筆虧損",
        "TWO_QUICK_LOSSES": "同一 Loop 累計兩筆虧損",
        "THREE_LOSSES_IN_30M": "30 分鐘內三筆虧損",
    }
    trigger = str(guard.get("trigger") or "LOSS_CLUSTER").upper()
    return (
        "【短窗跳停已啟動】\n\n"
        f"🛑 原因：{trigger_labels.get(trigger, trigger)}\n"
        f"最新市場：{_human_scalar(guard.get('trigger_campaign_id', latest.get('campaign_id', '—')))}\n"
        f"最新 PnL：{_human_scalar(latest.get('pnl_usdt', '—'))} USDT｜Path {_human_scalar(latest.get('path_bps', '—'))} bps\n"
        f"Loop 虧損次數：{_human_scalar(window.get('losses_in_loop', 0))}｜最近 5 筆虧損：{_human_scalar(window.get('losses_in_last_five', 0))}\n"
        "已停止新市場與新買單；既有部位仍會減倉與結算。\n"
        "冷卻至少 15 分鐘，且市況非紅燈後，才可手動 /predict_resume。"
    )


def _compact_status(value: Mapping[str, Any]) -> list[str]:
    state = value.get("state", value.get("status", "idle"))
    completed = value.get("markets_completed", value.get("completed"))
    target = value.get("target_markets", value.get("target"))
    progress = f"{completed}/{target}" if completed is not None and target is not None else None
    effective_mode = value.get("effective_mode", value.get("mode"))
    requested_mode = value.get("requested_mode")
    if requested_mode in (None, ""):
        requested_mode = "LIVE" if value.get("requested_live") else effective_mode or "SHADOW"
    configured_live = value.get("configured_live_enabled")
    next_strategy = str(value.get("next_strategy_profile") or "").strip().lower()
    current_strategy = str(value.get("strategy_profile") or "").strip().lower()
    funding = (
        f"資金：{_human_scalar(value.get('funding_source', 'MPC'))}/"
        f"{_human_scalar(value.get('balance_account_type', 'CeDeFi'))}｜"
        f"SAS：{_human_scalar(value.get('sas_verified'))}"
    )
    if uses_loop_risk_guards(current_strategy):
        stake = compact_stake_line(current_strategy)
        scale = f"加倉：否｜{funding}"
    else:
        stake = (
            f"投入：設定 {_human_scalar(value.get('order_unit_usdt', '1'))} USDT｜"
            f"首筆 {_human_scalar(value.get('initial_order_usdt', value.get('order_unit_usdt', '1')))} USDT｜"
            f"市場上限 {_human_scalar(value.get('market_buy_cap_usdt', value.get('order_unit_usdt', '1')))} USDT"
        )
        scale = f"加倉：{_human_scalar(value.get('scale_in_enabled', False))}｜{funding}"
    symbol = str(value.get("market_symbol") or "").strip().upper()
    lines = [
        f"市場：{_human_scalar(symbol)}" if symbol else "",
        f"目前模式：{_human_scalar(effective_mode)}｜要求模式：{_human_scalar(requested_mode)}｜"
        f"Live 功能：{_human_scalar(configured_live)}｜Live 已確認：{_human_scalar(value.get('live_armed'))}｜"
        f"實際下單：{_human_scalar(value.get('orders_enabled'))}",
        f"執行狀態：{_human_scalar(state)}｜策略：{_human_scalar(WR_MONITOR_LANE_LABELS.get(str(value.get('strategy_profile') or '').strip().lower(), value.get('strategy_profile')))}",
        (
            f"FAV 路由：P3 lane（FAV＋閘｜無R3｜真下單={'是' if value.get('fav_p3_live_orders') else '否'}）"
            if value.get("fav_p3_lane") or str(value.get("strategy_profile") or "").strip().lower() == "fav_p3"
            else (
                f"FAV 路由：arm=live 抑止 Baseline FAV（仍可能有 R3｜profile={_human_scalar(value.get('strategy_profile'))}）"
                if str(value.get("fav_p3_arm") or "off").lower() == "live"
                else (
                    f"FAV 路由：P3 shadow（arm={_human_scalar(value.get('fav_p3_arm'))}）"
                    if str(value.get("fav_p3_arm") or "off").lower() == "shadow"
                    else f"FAV 路由：Baseline（arm={_human_scalar(value.get('fav_p3_arm') or 'off')}）"
                )
            )
        ),
        stake,
        scale,
        f"Hard Stop 鎖定：{_human_scalar(value.get('hard_stop_latched'))}",
        (
            f"Jev 守門：{'啟用' if value.get('jev_gate_enabled') else '停用'}（放行 {value.get('jev_gate_allow_count', 0)}｜攔截 {value.get('jev_gate_reject_count', 0)}）"
            if value.get("jev_gate_enabled") is not None
            else ""
        ),
    ]
    lines = [line for line in lines if line]
    if uses_loop_risk_guards(current_strategy):
        lines.append(risk_status_line())
    play = value.get("s3s5_playbook")
    if isinstance(play, Mapping) and play:
        mode = "開單中" if str(play.get("mode") or "") == "LIVE" else "停單觀測"
        lines.append(f"S3+S5 劇本：{mode}｜halt批 {_human_scalar(play.get('halt_batch'))}｜回正批 {_human_scalar(play.get('recovery_batch'))}｜恢復批 {_human_scalar(play.get('resume_batch'))}")
        cur = play.get("current") if isinstance(play.get("current"), Mapping) else {}
        if cur:
            lines.append(
                f"S3+S5 本批第{_human_scalar(cur.get('batch'))}｜回撤 {_human_scalar(cur.get('live_dd'))}｜"
                f"開單 {_human_scalar(cur.get('taken_pnl'))}｜紙上 {_human_scalar(cur.get('paper_pnl'))}"
            )
    if next_strategy and next_strategy != current_strategy:
        lines.append(f"下一個 Loop 策略：{_human_scalar(next_strategy)}")
    next_order_unit = str(value.get("next_order_unit_usdt") or "").strip()
    current_order_unit = str(value.get("order_unit_usdt") or "1").strip()
    if (
        next_order_unit
        and next_order_unit != current_order_unit
        and not amount_picker_hidden(current_strategy, next_strategy or None)
    ):
        next_loss_limit = value.get("next_loop_loss_limit")
        if next_loss_limit in (None, ""):
            try:
                next_loss_limit = str(-(Decimal(next_order_unit) * Decimal("2")))
            except Exception:
                next_loss_limit = "未設定"
        if current_strategy in REGIME_PROFILES:
            lines.append(f"下一個 Loop：{_regime_risk_text(current_strategy, next_order_unit)}")
        elif current_strategy == C180_PROFILE:
            lines.append(
                f"下一個 Loop 投入：{_human_scalar(next_order_unit)} USDT｜"
                f"{_c180_risk_text(next_order_unit)}"
            )
        else:
            lines.append(
                f"下一個 Loop 投入：{_human_scalar(next_order_unit)} USDT｜虧損上限：{_human_scalar(next_loss_limit)} USDT"
            )
    loop_id = value.get("loop_id")
    loop_active = bool(value.get("loop_active", bool(loop_id)))
    if loop_active and loop_id:
        loop_parts = [f"目前 Loop：{_human_scalar(loop_id)}"]
        if progress is not None:
            loop_parts.append(f"進度 {progress}")
        if value.get("markets_seen") is not None:
            loop_parts.append(f"已觀察 {_human_scalar(value.get('markets_seen'))}")
        lines.append("｜".join(loop_parts))
        if value.get("loop_net_pnl") is not None:
            lines.append(f"目前 Loop PnL（已確認）：{_human_scalar(value.get('loop_net_pnl'))} USDT")
        if uses_loop_risk_guards(current_strategy) and value.get("loop_peak_pnl") is not None:
            lines.append(
                f"Loop 高點 {_human_scalar(value.get('loop_peak_pnl'))}｜"
                f"回撤 {_human_scalar(value.get('loop_drawdown'))} / "
                f"{_human_scalar(value.get('loop_mdd_limit', loop_mdd_limit(value.get('order_unit_usdt', '1'))))} USDT"
            )
            cd_reason = str(value.get("loss_cooldown_reason") or "")
            remaining_s = int(value.get("loss_cooldown_remaining_s") or 0)
            losses = value.get("consecutive_filled_losses", 0)
            if remaining_s > 0 and "active" in cd_reason:
                minutes = max(1, (remaining_s + 59) // 60)
                lines.append(f"連虧冷卻中：剩餘 {minutes} 分鐘（連虧 {losses}）")
            elif cd_reason == "loss_cooldown_trial_allowed":
                lines.append(f"連虧試單放行中（連虧 {losses}）")
            elif int(losses or 0) > 0:
                lines.append(f"連虧成交：{losses} 次")
        if any(key in value for key in ("loop_win_rate", "loop_wins", "loop_losses", "loop_no_trades")):
            lines.append(f"目前 Loop WR：{_compact_wr(value, prefix='loop_')}")
    else:
        lines.append("目前 Loop：無（待命）")
        last_loop_id = value.get("last_loop_id")
        if last_loop_id:
            last_completed = value.get("last_loop_completed", 0)
            last_target = value.get("last_loop_target", 0)
            lines.append(f"上次 Loop：{_human_scalar(last_loop_id)}")
            lines.append(
                f"上次結果：{last_completed}/{last_target}｜"
                f"{_human_scalar(value.get('last_loop_state'))}"
            )
            lines.append(f"上次 Loop PnL：{_human_scalar(value.get('last_loop_net_pnl', '0'))} USDT")
            if any(
                key in value
                for key in ("last_loop_win_rate", "last_loop_wins", "last_loop_losses", "last_loop_no_trades")
            ):
                lines.append(f"上次 Loop WR：{_compact_wr(value, prefix='last_loop_')}")
    lines.extend(_market_regime_lines(value, compact=True))
    lines.extend(_pending_pnl_lines(value))
    if current_strategy in REGIME_PROFILES:
        lines.append(f"保護：{_regime_risk_text(current_strategy, current_order_unit)}")
        risk = value.get("regime_lane_risk") or {}
        if risk.get("halt_reason"):
            lines.append(f"Regime 持久停單：{risk.get('halt_reason')}")
    elif current_strategy == C180_PROFILE:
        lines.append(f"保護：{_c180_risk_text(current_order_unit, policy_version=str(value.get('c180_policy_version') or '1.1'))}")
        if value.get("loop_new_entries_stopped"):
            lines.append(f"新進場已停止：{_human_scalar(value.get('loop_terminal_reason') or '原因未提供')}")
    else:
        lines.append(f"保護：Loop 虧損上限 {_human_scalar(value.get('loop_loss_limit', '-2'))} USDT")
    if value.get("loop_loss_limit_reached") and current_strategy != C180_PROFILE:
        lines.append("保護狀態：已停止新增交易")
    balances = value.get("wallet_balances")
    if isinstance(balances, (list, tuple)):
        if balances:
            wallet_items: list[str] = []
            for item in balances:
                if not isinstance(item, Mapping):
                    continue
                account = str(item.get("account_type") or "UNKNOWN")
                amount = item.get("available_balance_display")
                state_text = "可用" if item.get("enabled") else "停用"
                wallet_items.append(f"{account} {amount if amount is not None else '未提供'} USDT（{state_text}）")
            lines.append(f"錢包：{'｜'.join(wallet_items) if wallet_items else '暫無資料'}")
        else:
            lines.append("錢包：暫無資料")
    elif value.get("wallet_balances_error"):
        lines.append(f"錢包：讀取失敗（{value['wallet_balances_error']}）")
    preflight = value.get("preflight")
    if isinstance(preflight, Mapping):
        lines.append(f"Live 檢查：{'通過' if preflight.get('passed') else '未通過'}")
        reason = _compact_reason(preflight)
        if reason:
            lines.append(f"原因：{reason}")
    return lines


def _compact_loop_result(value: Mapping[str, Any]) -> list[str]:
    if value.get("action_denied"):
        lines = ["結果：未執行"]
        reason = _compact_reason(value)
        if reason:
            lines.append(f"原因：{reason}")
    else:
        lines = ["結果：已啟動"]
    mode = value.get("mode")
    if mode is not None:
        lines.append(f"模式：{_human_scalar(mode)}")
    completed = value.get("markets_completed", value.get("completed"))
    target = value.get("target_markets", value.get("one_run_target", value.get("target")))
    if completed is not None and target is not None:
        lines.append(f"市場進度：{completed}/{target}")
    if value.get("loop_id"):
        lines.append(f"Loop ID：{_human_scalar(value.get('loop_id'))}")
    if value.get("one_run"):
        lines.append("執行方式：單筆試跑")
    if str(value.get("strategy_profile") or "").strip().lower() in REGIME_PROFILES:
        lines.append(f"保護：{_regime_risk_text(str(value.get('strategy_profile') or ''), value.get('order_unit_usdt', '1'))}")
    elif str(value.get("strategy_profile") or "").strip().lower() == C180_PROFILE:
        lines.append(f"保護：{_c180_risk_text(value.get('order_unit_usdt', '1'))}")
    else:
        loss_limit = value.get("loop_loss_limit", value.get("next_loop_loss_limit", "-2"))
        lines.append(f"保護：Loop PnL <= {_human_scalar(loss_limit)} USDT 即停止新增交易")
    return lines


def _compact_wr(
    value: Mapping[str, Any],
    *,
    prefix: str = "",
) -> str:
    wins = int(value.get(f"{prefix}wins", value.get("wins", 0)) or 0)
    losses = int(value.get(f"{prefix}losses", value.get("losses", 0)) or 0)
    breakevens = int(value.get(f"{prefix}breakevens", value.get("breakevens", 0)) or 0)
    no_trades = int(value.get(f"{prefix}no_trades", value.get("no_trades", 0)) or 0)
    raw_rate = value.get(f"{prefix}win_rate", value.get("win_rate"))
    if raw_rate is None:
        rate = "—"
    else:
        try:
            rate = f"{Decimal(str(raw_rate)):.1f}%"
        except (ArithmeticError, TypeError, ValueError):
            rate = f"{_human_scalar(raw_rate)}%"
    details = [f"{wins}W/{losses}L"]
    if breakevens:
        details.append(f"{breakevens}平")
    if no_trades:
        details.append(f"No-trade {no_trades}")
    return f"{rate}（{'；'.join(details)}）"


def _compact_pnl(value: Mapping[str, Any]) -> list[str]:
    lines = [
        f"整體 Loop PnL：{_human_scalar(value.get('total_loop_pnl', '0'))} USDT",
        f"整體 Loop WR：{_compact_wr(value, prefix='total_')}",
        f"目前 Loop PnL（已確認）：{_human_scalar(value.get('current_loop_pnl', '0'))} USDT",
        f"目前 Loop WR：{_compact_wr(value, prefix='current_')}",
        f"Loop ID：{_human_scalar(value.get('active_loop_id'))}",
        f"Loop 紀錄：{_human_scalar(value.get('loop_count', 0))}",
    ]
    lines.extend(_pending_pnl_lines(value))
    loops = value.get("loops")
    if isinstance(loops, (list, tuple)) and loops:
        lines.append("最近紀錄：")
        for item in loops[:3]:
            if not isinstance(item, Mapping):
                continue
            state = _human_scalar(item.get("state"))
            progress = f"{item.get('completed', 0)}/{item.get('target', 0)}"
            pnl = _human_scalar(item.get("pnl", "0"))
            lines.append(f"  • {state}｜{progress}｜{pnl} USDT｜WR {_compact_wr(item)}")
    return lines


def _compact_risk(value: Mapping[str, Any]) -> list[str]:
    lines = [
        f"當日 PnL：{_human_scalar(value.get('daily_net_pnl', '0'))} USDT",
        f"目前 Loop PnL（已確認）：{_human_scalar(value.get('loop_net_pnl', '0'))} USDT",
        f"連續虧損：{_human_scalar(value.get('consecutive_losses', 0))} 次",
        f"下單嘗試：{_human_scalar(value.get('order_attempts', 0))} 次",
        f"買入次數：{_human_scalar(value.get('buy_count', 0))} 次",
        f"Hard Stop：{'已鎖定' if value.get('hard_stop_latched') else '未觸發'}",
    ]
    if value.get("strategy_profile") in REGIME_PROFILES:
        lines.append(_regime_risk_text(str(value.get('strategy_profile') or ''), value.get('order_unit_usdt', '1')))
        lane = value.get("regime_lane_risk") or {}
        lines.append(f"Regime 累計淨損益：{lane.get('net_pnl_usdt', '尚未初始化')}")
        lines.append(f"Regime 停單：{lane.get('halt_reason') or '未觸發'}")
    lines.extend(_pending_pnl_lines(value))
    return lines


def format_runtime_result(title: str, result: Any) -> str:
    """Create a bounded, readable Traditional Chinese Telegram response."""

    if isinstance(result, Mapping):
        value = _redact(result)
        if title.endswith("整輪市場"):
            lines = ["目前市場："+str(value.get("market_symbol", "未知")),
                     "下一輪市場："+str(value.get("next_market_symbol", value.get("market_symbol", "未知")))]
            if value.get("action_denied"):
                lines += ["尚未套用："+str(value.get("reason", "條件未通過"))]
            elif value.get("market_queued"):
                lines += ["已排下一輪；目前 Loop 維持原幣種。", "本輪結束、持倉與訂單清空後，再點選該幣套用。"]
            else:
                lines += ["已選定；尚未建立新 Loop。"]
                if value.get("producer_switch") == "done":
                    lines += ["資料程式已切到此幣（BTC 基準保留，其他幣與觀測器已停）。"]
                elif value.get("producer_switch") == "unavailable":
                    lines += ["VM 未安裝 scripts/t6_coin.sh，資料程式未切換。"]
                ready = value.get("producer_warmup_until_ms")
                if ready:
                    left = max(0, int(ready) - int(time.time() * 1000))
                    lines += [f"資料程式暖機到下一個可交易市場，約 {-(-left // 60000)} 分鐘，期間 /predict_loop 會被擋。"]
                lines += ["確認 /predict_live on 後，用 /predict_loop 20 開始20場。"]
        elif title == "系統狀態" or title.endswith("狀態") and "風控" not in title:
            lines = _compact_status(value)
        elif "Loop PnL" in title:
            lines = _compact_pnl(value)
        elif "風控" in title:
            lines = _compact_risk(value)
        elif "Loop" in title or "單筆" in title or "市場迴圈" in title:
            lines = _compact_loop_result(value)
        else:
            # Unknown runtime responses remain human-readable but are bounded
            # to the first few top-level fields instead of dumping nested JSON.
            lines = _format_mapping(value)[:16]
        body = "\n".join(lines) or "操作完成。"
    elif isinstance(result, (list, tuple)):
        body = "\n".join(f"• {_human_scalar(item)}" for item in _redact(result))
    elif result is None:
        body = "操作完成。"
    elif isinstance(result, str):
        body = result
    else:
        body = _human_scalar(_redact(result))
    if len(body) > 3900:
        body = body[:3890] + "\n…"
    return f"【{title}】\n\n{body}"


def _mapping_hard_stop(value: Any) -> bool | None:
    """Read only explicit hard-stop signals; never infer from a generic PnL."""

    if isinstance(value, Mapping):
        for key in ("hard_stop_latched", "hard_stop", "daily_hard_stop", "same_day_hard_stop"):
            if key in value:
                return bool(value[key])
        mode = value.get("mode") or value.get("state") or value.get("risk_mode")
        if mode is not None and str(getattr(mode, "value", mode)).upper() == "HARD_STOP":
            return True
        for key in ("risk", "snapshot", "decision"):
            if key in value:
                nested = _mapping_hard_stop(value[key])
                if nested is not None:
                    return nested
        return None
    if value is None or isinstance(value, (str, bytes, bool, int, float)):
        return None
    for key in ("hard_stop_latched", "hard_stop", "daily_hard_stop"):
        if hasattr(value, key):
            return bool(getattr(value, key))
    mode = getattr(value, "mode", None)
    if mode is not None and str(getattr(mode, "value", mode)).upper() == "HARD_STOP":
        return True
    return None


def _normalize_gate(value: Any) -> PromotionGate:
    if isinstance(value, PromotionGate):
        return value
    if isinstance(value, bool):
        return PromotionGate(value)
    if isinstance(value, Mapping):
        passed = value.get("passed", value.get("eligible", value.get("ok", value.get("authorized", False))))
        reasons = value.get("reasons", value.get("reason", ()))
        if isinstance(reasons, str):
            reasons = (reasons,)
        elif not isinstance(reasons, (list, tuple)):
            reasons = (str(reasons),) if reasons else ()
        return PromotionGate(bool(passed), tuple(str(item) for item in reasons), value)
    passed = getattr(value, "passed", getattr(value, "eligible", getattr(value, "ok", False)))
    reasons = getattr(value, "reasons", ())
    if isinstance(reasons, str):
        reasons = (reasons,)
    return PromotionGate(bool(passed), tuple(str(item) for item in reasons), {})


class PredictionTelegramService:
    """Authorized Telegram command service for one injected runtime manager."""

    def __init__(
        self,
        runtime: PredictionRuntime,
        authorized_chat_ids: int | str | Sequence[int | str],
        *,
        now_ms: Callable[[], int] = _now_ms,
        token_factory: Callable[[], str] | None = None,
        confirmation_ttl_seconds: int = 60,
    ) -> None:
        if isinstance(authorized_chat_ids, (str, int)):
            authorized_chat_ids = (authorized_chat_ids,)
        self.runtime = runtime
        self.authorized_chat_ids = {str(value) for value in authorized_chat_ids if str(value)}
        self._now_ms = now_ms
        self._token_factory = token_factory or (lambda: secrets.token_urlsafe(18))
        self._confirmation_ttl_ms = max(1, int(confirmation_ttl_seconds)) * 1000
        self._pending_confirmation: _PendingLiveConfirmation | None = None
        self._pending_hard_stop_reset: _PendingHardStopReset | None = None
        self._pending_loop_cancel: _PendingLoopCancel | None = None
        self._hard_stop_date: str | None = None
        self._monitor_confirmed_state: str | None = None
        self._monitor_candidate_state: str | None = None
        self._monitor_candidate_count = 0
        self._last_jump_stop_alert_id: str | None = None
        self._lock = asyncio.Lock()

    def _market_symbol(self, status: Mapping[str, Any] | None = None) -> str:
        """Resolve market symbol for ETH-only lane filtering (status then runtime settings)."""
        if isinstance(status, Mapping):
            sym = str(status.get("market_symbol") or "").strip().upper()
            if sym:
                return sym
        runtime = getattr(self, "runtime", None)
        worker = getattr(runtime, "worker", None)
        settings = getattr(worker, "settings", None) if worker is not None else getattr(runtime, "settings", None)
        return str(getattr(settings, "market_symbol", "") or "").strip().upper()

    def authorized(self, update: Any) -> bool:
        chat = _chat_id(update)
        return chat is not None and chat in self.authorized_chat_ids

    async def _deny_if_unauthorized(self, update: Any) -> bool:
        if self.authorized(update):
            return False
        # Do not reveal configured chat IDs.  Callback queries still need an
        # answer or Telegram keeps showing a spinner to the attacker.
        query = getattr(update, "callback_query", None)
        if query is not None and hasattr(query, "answer"):
            result = query.answer("未授權的 Telegram chat。", show_alert=False)
            if _is_awaitable(result):
                await result
        await self._reply(update, "此 chat 尚未授權 Prediction 控制。")
        return True

    async def _reply(self, update: Any, text: str, *, reply_markup: Any = None, parse_mode: Any = None) -> Any:
        """Reply to commands and callbacks.

        Callback updates may carry MaybeInaccessibleMessage without reply_text.
        Returning None made cancel/confirm buttons look stuck. Prefer reply_text,
        then chat.send_message, then bot.send_message.
        """

        kwargs: dict[str, Any] = {}
        if reply_markup is not None:
            kwargs["reply_markup"] = reply_markup
        if parse_mode is not None:
            kwargs["parse_mode"] = parse_mode

        query = getattr(update, "callback_query", None)
        message = getattr(update, "effective_message", None)
        if message is None and query is not None:
            message = getattr(query, "message", None)

        reply_text = getattr(message, "reply_text", None) if message is not None else None
        if callable(reply_text):
            try:
                result = reply_text(text, **kwargs)
                return await result if _is_awaitable(result) else result
            except Exception as exc:  # noqa: BLE001 - fall through to send_message
                LOGGER.warning(
                    "prediction_telegram_reply_text_failed error_type=%s",
                    type(exc).__name__,
                )

        chat = getattr(message, "chat", None) if message is not None else None
        send_message = getattr(chat, "send_message", None) if chat is not None else None
        if callable(send_message):
            try:
                result = send_message(text, **kwargs)
                return await result if _is_awaitable(result) else result
            except Exception as exc:  # noqa: BLE001 - fall through to bot.send_message
                LOGGER.warning(
                    "prediction_telegram_chat_send_failed error_type=%s",
                    type(exc).__name__,
                )

        chat_id = _chat_id(update)
        bot = None
        for owner in (query, update, message):
            if owner is None:
                continue
            getter = getattr(owner, "get_bot", None)
            if callable(getter):
                try:
                    bot = getter()
                except Exception:  # noqa: BLE001
                    bot = None
            if bot is None:
                bot = getattr(owner, "bot", None)
            if bot is not None:
                break
        if chat_id and bot is not None and hasattr(bot, "send_message"):
            try:
                cid: Any = int(chat_id) if str(chat_id).lstrip("-").isdigit() else chat_id
                result = bot.send_message(chat_id=cid, text=text, **kwargs)
                return await result if _is_awaitable(result) else result
            except Exception as exc:  # noqa: BLE001 - last resort
                LOGGER.error(
                    "prediction_telegram_bot_send_failed error_type=%s",
                    type(exc).__name__,
                )
                return None
        LOGGER.error("prediction_telegram_reply_unavailable chat_id=%s", chat_id)
        return None

    async def handle_error(self, update: Any, context: Any) -> None:
        """Return a safe acknowledgement when a handler raises unexpectedly."""

        error = getattr(context, "error", None)
        LOGGER.error("prediction_telegram_handler_error error_type=%s", type(error).__name__ if error else "unknown")
        try:
            await self._reply(update, "Prediction 指令處理失敗，系統已維持安全狀態。")
        except Exception as exc:  # noqa: BLE001 - never hide the original handler error
            LOGGER.error("prediction_telegram_error_reply_failed error_type=%s", type(exc).__name__)

    async def _invoke(self, names: Sequence[str], *args: Any) -> Any:
        for name in names:
            method = getattr(self.runtime, name, None)
            if method is None:
                continue
            call_args = args
            if args:
                # Decide compatibility from the callable contract. Never use
                # a caught TypeError as a signal: that could hide a real
                # runtime bug and accidentally execute a second command.
                try:
                    inspect.signature(method).bind(*args)
                except (TypeError, ValueError):
                    call_args = ()
            result = method(*call_args)
            return await result if _is_awaitable(result) else result
        raise AttributeError(f"runtime does not implement any of: {', '.join(names)}")

    async def _read_risk(self) -> tuple[Any | None, str | None]:
        try:
            result = await self._invoke(("risk", "get_risk", "risk_snapshot"))
        except AttributeError:
            return None, "runtime does not expose a risk snapshot"
        except Exception as exc:  # noqa: BLE001 - control lane must fail closed
            return None, f"risk snapshot unavailable: {exc}"
        return result, None

    def _today_key(self) -> str:
        return datetime.fromtimestamp(self._now_ms() / 1000, TAIPEI).date().isoformat()

    @staticmethod
    def _hard_stop_reset_markup() -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(
            [[
                InlineKeyboardButton(
                    "⚠️ Reset Hard Stop（可重複執行）",
                    callback_data=f"{HARD_STOP_CALLBACK_PREFIX}request",
                )
            ]]
        )

    @staticmethod
    def _cancel_loop_markup() -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(
            [[
                InlineKeyboardButton(
                    "🛑 取消整個 Loop",
                    callback_data=f"{CANCEL_LOOP_CALLBACK_PREFIX}request",
                )
            ]]
        )

    def _status_markup(self, value: Mapping[str, Any]) -> InlineKeyboardMarkup | None:
        rows: list[list[InlineKeyboardButton]] = [[
            InlineKeyboardButton(
                "📊 市況監控",
                callback_data=f"{MONITOR_CALLBACK_PREFIX}show",
            )
        ]]
        if _mapping_hard_stop(value):
            rows.extend(self._hard_stop_reset_markup().inline_keyboard)
        loop_active = bool(value.get("loop_active"))
        if not loop_active and value.get("loop_id"):
            loop_active = str(value.get("loop_state") or "RUNNING").upper() == "RUNNING"
        if loop_active:
            rows.extend(self._cancel_loop_markup().inline_keyboard)
        return InlineKeyboardMarkup(rows) if rows else None

    @staticmethod
    def _monitor_markup() -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(
            [[
                InlineKeyboardButton(
                    "🔄 重新整理",
                    callback_data=f"{MONITOR_CALLBACK_PREFIX}show",
                )
            ]]
        )

    async def _hard_stop_guard(self, *, require_known: bool = True) -> str | None:
        today = self._today_key()
        # Prefer a direct manager property when available, then the public
        # risk query.  We never parse credentials or configuration files.
        direct = getattr(self.runtime, "hard_stop_latched", None)
        if callable(direct):
            direct = direct()
            if _is_awaitable(direct):
                direct = await direct
        if direct is not None:
            hard = bool(direct)
            if hard:
                self._hard_stop_date = today
                return "今日已觸發 Hard Stop（當日風控停止），Telegram 不允許覆寫。"
            return None
        payload, error = await self._read_risk()
        if error:
            return error if require_known else None
        hard = _mapping_hard_stop(payload)
        if hard:
            self._hard_stop_date = today
            return "今日已觸發 Hard Stop（當日風控停止），Telegram 不允許覆寫。"
        if hard is None and require_known:
            return "無法確認 Hard Stop 狀態，為安全起見已停止此操作。"
        return None

    async def _call_and_reply(self, update: Any, title: str, names: Sequence[str], *args: Any) -> Any:
        try:
            result = await self._invoke(names, *args)
        except Exception as exc:  # noqa: BLE001 - Telegram should receive a safe error
            return await self._reply(update, f"【{title}】\n操作失敗，系統已維持安全狀態。\n錯誤類型：{type(exc).__name__}")
        return await self._reply(update, format_runtime_result(title, result))

    async def cmd_predict_one_run(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if await self._deny_if_unauthorized(update):
            return
        blocked = await self._hard_stop_guard()
        if blocked:
            await self._reply(update, blocked)
            return
        await self._call_and_reply(update, "單筆試跑（1 個市場）", ("one_run", "run_one", "run_once", "start_loop"), 1)

    async def _cmd_predict_fixed_loop(self, update: Update, count: int) -> None:
        if await self._deny_if_unauthorized(update):
            return
        blocked = await self._hard_stop_guard()
        if blocked:
            await self._reply(update, blocked)
            return
        await self._call_and_reply(
            update,
            f"Loop {count}（最多 {count} 個市場）",
            ("start_loop", "start"),
            count,
        )

    async def cmd_predict_loop_5(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        await self._cmd_predict_fixed_loop(update, 5)

    async def cmd_predict_loop_10(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        await self._cmd_predict_fixed_loop(update, 10)

    async def cmd_predict_loop_20(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        await self._cmd_predict_fixed_loop(update, 20)

    async def cmd_predict_loop_100(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        await self._cmd_predict_fixed_loop(update, 100)

    async def cmd_predict_loop_pnl(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if await self._deny_if_unauthorized(update):
            return
        await self._call_and_reply(update, "Loop PnL 總覽", ("loop_pnl", "get_loop_pnl_summary", "pnl"))

    async def cmd_predict_market(self, update, context):
        if await self._deny_if_unauthorized(update):
            return
        args = getattr(context, 'args', None) or []
        if args:
            asset = str(args[0]).upper()
            if asset in ('BTC', 'ETH', 'BNB'):
                asset += 'USDT'
            await self._call_and_reply(update, 'T6.7c／T6.9／T6.9b 整輪市場', ('select_market',), asset)
            return
        current = await self._invoke(('status', 'predict_status'))
        await self._reply(update, '【T6.7c／T6.9／T6.9b 下一輪市場】\n目前：'+str(current.get('market_symbol', '未知'))+
            '\n下一輪：'+str(current.get('next_market_symbol', current.get('market_symbol', '未知')))+
            '\n每輪鎖定一幣；執行中只排下一輪，結束後再點選套用。換幣後重新確認 Live，再用 /predict_loop 20 啟動。',
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(a, callback_data='predict_market:'+a+'USDT') for a in ('BTC','ETH','BNB')]]))

    async def cmd_predict_lane(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Show the operator-approved strategies as one compact picker."""

        if await self._deny_if_unauthorized(update):
            return
        try:
            current = await self._invoke(("status", "predict_status"))
        except Exception:
            current = {}
        current_profile = current.get("strategy_profile") if isinstance(current, Mapping) else None
        next_profile = current.get("next_strategy_profile") if isinstance(current, Mapping) else None
        market_symbol = self._market_symbol(current if isinstance(current, Mapping) else None)
        lanes = selectable_lanes_for_market(market_symbol)
        buttons = [
            [InlineKeyboardButton(label, callback_data=f"{LANE_CALLBACK_PREFIX}{profile}")]
            for profile, label in lanes
        ]
        current_label = next(
            (label for profile, label in lanes if profile == str(current_profile).lower()),
            str(current_profile or "未設定"),
        )
        next_label = next(
            (label for profile, label in lanes if profile == str(next_profile).lower()),
            str(next_profile or current_profile or "未設定"),
        )
        await self._reply(
            update,
            "【選擇策略】\n\n"
            f"目前 Loop：{current_label}\n"
            f"下一個 Loop：{next_label}\n"
            "目前提供 T6 系列策略；選定後依原流程確認 Live 與金額。\n"
            "執行中點選會排到下一個 Loop；先停止 Loop 再點選，會在安全同步後結束舊 Loop 並立即套用。\n" +
            ("T6.2／T6.3／T6.3a／T6.3b／T6.5／T6.7／T6.7a／T6.7b／T6.7c／T6.7d／T6.8／T6.8a／T6.9／T6.9b 每筆可選 1／2／3 USDT，不加倉；切換策略不會自動啟動 Loop。"
             if str(next_profile or current_profile).lower() in (REGIME_T62_PROFILE, REGIME_T63_PROFILE, REGIME_T63A_PROFILE, REGIME_T63B_PROFILE, REGIME_T65_PROFILE, REGIME_T67_PROFILE, REGIME_T67A_PROFILE, REGIME_T67B_PROFILE, REGIME_T67C_PROFILE, REGIME_T67D_PROFILE, REGIME_T68_PROFILE, REGIME_T68A_PROFILE, REGIME_T69_PROFILE, REGIME_T69A_PROFILE) else
             "金額固定每筆 1 USDT，不加倉；切換策略不會自動啟動 Loop。"),
            reply_markup=InlineKeyboardMarkup(buttons),
        )

    async def cmd_predict_amount(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Show the 1/2/3 USDT picker and matching loop MDD caps."""

        if await self._deny_if_unauthorized(update):
            return
        try:
            current = await self._invoke(("status", "predict_status"))
        except Exception:
            current = {}
        current_amount = str(current.get("order_unit_usdt") or "1") if isinstance(current, Mapping) else "1"
        next_amount = str(current.get("next_order_unit_usdt") or current_amount) if isinstance(current, Mapping) else current_amount
        profile = str(current.get("next_strategy_profile") or current.get("strategy_profile") or "").lower() if isinstance(current, Mapping) else ""
        c180 = profile == C180_PROFILE
        t62 = profile in (REGIME_T62_PROFILE, REGIME_T63_PROFILE, REGIME_T63A_PROFILE, REGIME_T63B_PROFILE, REGIME_T65_PROFILE, REGIME_T67_PROFILE, REGIME_T67A_PROFILE, REGIME_T67B_PROFILE, REGIME_T67C_PROFILE, REGIME_T67D_PROFILE, REGIME_T68_PROFILE, REGIME_T68A_PROFILE, REGIME_T69_PROFILE, REGIME_T69A_PROFILE)
        def displayed_mdd(amount):
            if c180:
                return -(Decimal("4.49") * Decimal(str(amount)))
            if t62:
                return Decimal("3.5") * Decimal(str(amount))
            return loop_mdd_limit(amount)
        mdd_label = "持久20場MDD ≥" if t62 else "MDD"
        buttons = [[
            InlineKeyboardButton(f"1 USDT ｜ {mdd_label} {displayed_mdd('1')}", callback_data=f"{ORDER_UNIT_CALLBACK_PREFIX}1"),
            InlineKeyboardButton(f"2 USDT ｜ {mdd_label} {displayed_mdd('2')}", callback_data=f"{ORDER_UNIT_CALLBACK_PREFIX}2"),
            InlineKeyboardButton(f"3 USDT ｜ {mdd_label} {displayed_mdd('3')}", callback_data=f"{ORDER_UNIT_CALLBACK_PREFIX}3"),
        ]]
        await self._reply(
            update,
            "【選擇每腿投入】\n\n"
            f"目前設定：{_human_scalar(current_amount)} USDT｜{mdd_label} {displayed_mdd(current_amount)}\n"
            f"下一個 Loop：{_human_scalar(next_amount)} USDT｜{mdd_label} {displayed_mdd(next_amount)}\n"
            + ("C180 每 20-run 區段超過 MDD 門檻後停新進場，下一段重設。\n" if c180 else
               "T6.2／T6.3／T6.3a／T6.3b 純1/2/3U成交：固定20場MDD≥3.5/7/10.5U，跨Loop累計PnL≤-6/-12/-18U 停新進場。混合金額按每筆實際投入折算1U等值；停單跨Loop保留。\n"
               + ("T6.3b／T6.5／T6.7／T6.7a／T6.7b／T6.7c／T6.7d／T6.8／T6.8a／T6.9／T6.9b 額外整輪高點回撤：1U等值達3.5停新進場；純1/2/3U約為3.5/7/10.5U，僅鎖該輪。\n" if profile in (REGIME_T63B_PROFILE, REGIME_T65_PROFILE, REGIME_T67_PROFILE, REGIME_T67A_PROFILE, REGIME_T67B_PROFILE, REGIME_T67C_PROFILE, REGIME_T67D_PROFILE, REGIME_T68_PROFILE) else "") if t62 else
               "1/2/3 USDT 對應 Loop MDD -2, REGIME_T68A_PROFILE.5/-5.0/-7.5。\n")
            + 
            "同市場最多一筆。進行中點選會排到下一個 Loop；金額變更後需重新確認 Live。",
            reply_markup=InlineKeyboardMarkup(buttons),
        )

    async def cmd_predict_status(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if await self._deny_if_unauthorized(update):
            return
        try:
            result = await self._invoke(("status", "predict_status"))
            if isinstance(result, Mapping):
                try:
                    balances = await self._invoke(("wallet_balances", "get_wallet_balances"))
                    if isinstance(balances, Mapping):
                        result = {**result, **balances}
                except Exception:
                    result = {**result, "wallet_balances_error": "無法連線"}
        except Exception as exc:  # noqa: BLE001 - Telegram receives a safe error
            await self._reply(update, f"【系統狀態】\n讀取失敗：{type(exc).__name__}")
            return
        hard_stop = _mapping_hard_stop(result) if isinstance(result, Mapping) else None
        await self._reply(
            update,
            format_runtime_result("系統狀態", result),
            reply_markup=self._status_markup(result) if isinstance(result, Mapping) else (self._hard_stop_reset_markup() if hard_stop else None),
        )

    async def cmd_predict_monitor(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Show the standalone whipsaw and lane-readiness monitor."""

        if await self._deny_if_unauthorized(update):
            return
        try:
            result = await self._invoke(("status", "predict_status"))
        except Exception as exc:  # noqa: BLE001 - monitoring must not affect trading
            await self._reply(update, f"【市況監控】\n讀取失敗：{type(exc).__name__}")
            return
        await self._reply(
            update,
            format_monitor_result(result),
            reply_markup=self._monitor_markup(),
        )

    async def cmd_lanes(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Show live comparison between FAV_BASELINE and FAV_P3_LIVE."""
        if await self._deny_if_unauthorized(update):
            return
        repo = getattr(self.runtime, "repository", None)
        if repo is None or not hasattr(repo, "get_lane_performance_summary"):
            await self._reply(update, "【雙軌績效比較】\n資料庫未提供軌道統計")
            return
        try:
            base_summary = await repo.get_lane_performance_summary("FAV_BASELINE")
            p3_summary = await repo.get_lane_performance_summary("FAV_P3_LIVE")
            text = (
                "🏁【雙軌實盤即時績效比較】\n"
                "────────────────────\n"
                "📊 [Lane A: FAV_BASELINE]\n"
                f"• 訊號數 / 交易數: {base_summary['total_signals']} / {base_summary['accepted_trades']}\n"
                f"• 已結算: {base_summary['settled_trades']} (勝: {base_summary['wins']} / 敗: {base_summary['losses']})\n"
                f"• 勝率 (WR): {base_summary['win_rate']:.1f}%\n"
                f"• 淨損益 (PnL): {base_summary['net_pnl']:+.2f} USDT\n"
                f"• 每 100U 期望值: {base_summary['pnl_per_100']:+.2f} U\n"
                "────────────────────\n"
                "⚡ [Lane B: FAV_P3_LIVE]\n"
                f"• 訊號數 / 交易數: {p3_summary['total_signals']} / {p3_summary['accepted_trades']}\n"
                f"• 已結算: {p3_summary['settled_trades']} (勝: {p3_summary['wins']} / 敗: {p3_summary['losses']})\n"
                f"• 勝率 (WR): {p3_summary['win_rate']:.1f}%\n"
                f"• 淨損益 (PnL): {p3_summary['net_pnl']:+.2f} USDT\n"
                f"• 每 100U 期望值: {p3_summary['pnl_per_100']:+.2f} U\n"
                f"• 平均同側持續: {p3_summary['avg_same_side']:.1f}s\n"
                f"• 平均價差距離: {p3_summary['avg_distance']:.1f} bps\n"
                "────────────────────\n"
                "規則: pre_cross=0 | same_side>=30s | dist>=3bps | 1U LIMIT"
            )
            await self._reply(update, text)
        except Exception as exc:
            LOGGER.exception("cmd_lanes failed: %s", exc)
            await self._reply(update, f"【雙軌比較】讀取失敗：{exc}")

    async def check_monitor_alerts(self, bot: Any) -> dict[str, Any]:
        """Send one transition alert after the same state is seen twice."""

        result = await self._invoke(("status", "predict_status"))
        jump_stop = result.get("adaptive_jump_stop") if isinstance(result, Mapping) else None
        jump_sent = 0
        if isinstance(jump_stop, Mapping) and bool(result.get("adaptive_jump_stop_active")):
            jump_id = f"{jump_stop.get('loop_id', '')}:{jump_stop.get('at_ms', '')}"
            if jump_id != self._last_jump_stop_alert_id:
                self._last_jump_stop_alert_id = jump_id
                jump_text = _jump_stop_alert_text(result)
                for chat_id in sorted(self.authorized_chat_ids):
                    sender = getattr(bot, "send_message", None)
                    if not callable(sender):
                        break
                    try:
                        response = sender(chat_id=chat_id, text=jump_text)
                        if _is_awaitable(response):
                            await response
                        jump_sent += 1
                    except Exception as exc:  # noqa: BLE001 - one chat must not stop the monitor
                        LOGGER.warning(
                            "prediction_jump_stop_alert_failed error_type=%s",
                            type(exc).__name__,
                        )
        monitor = result.get("market_regime_monitor") if isinstance(result, Mapping) else None
        if not isinstance(monitor, Mapping):
            return {"sent": jump_sent, "jump_sent": jump_sent, "reason": "monitor unavailable"}
        state = str(monitor.get("status") or "WAIT_DATA").upper()
        if state not in REGIME_STATUS_META:
            return {"sent": 0, "reason": "invalid monitor state"}
        if state == self._monitor_candidate_state:
            self._monitor_candidate_count += 1
        else:
            self._monitor_candidate_state = state
            self._monitor_candidate_count = 1
        if self._monitor_candidate_count < 2:
            return {"sent": jump_sent, "jump_sent": jump_sent, "reason": "waiting for second confirmation", "state": state}
        previous = self._monitor_confirmed_state
        if previous == state:
            return {"sent": jump_sent, "jump_sent": jump_sent, "reason": "state unchanged", "state": state}
        self._monitor_confirmed_state = state
        # Do not announce the initial healthy/watch snapshot after every
        # service restart.  An initial RED is safety-relevant and is sent.
        if previous is None and state != "RED":
            return {"sent": jump_sent, "jump_sent": jump_sent, "reason": "initial state recorded", "state": state}
        text = _monitor_alert_text(result, state)
        sent = 0
        for chat_id in sorted(self.authorized_chat_ids):
            sender = getattr(bot, "send_message", None)
            if not callable(sender):
                break
            try:
                response = sender(chat_id=chat_id, text=text)
                if _is_awaitable(response):
                    await response
                sent += 1
            except Exception as exc:  # noqa: BLE001 - one chat must not stop the monitor
                LOGGER.warning(
                    "prediction_monitor_alert_failed error_type=%s",
                    type(exc).__name__,
                )
        return {"sent": sent + jump_sent, "jump_sent": jump_sent, "state": state, "previous_state": previous}

    async def cmd_predict_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if await self._deny_if_unauthorized(update):
            return
        args = list(getattr(context, "args", ()) or ())
        if len(args) > 1:
            await self._reply(
                update,
                f"用法：/predict_start [市場數量]\n市場數量預設 10，範圍 1～{MAX_LOOP_MARKETS}。",
            )
            return
        try:
            count = int(args[0]) if args else DEFAULT_LOOP_MARKETS
        except (TypeError, ValueError):
            await self._reply(update, f"市場數量必須是整數，範圍 1～{MAX_LOOP_MARKETS}。")
            return
        if not 1 <= count <= MAX_LOOP_MARKETS:
            await self._reply(update, f"市場數量超出範圍，最多只能執行 {MAX_LOOP_MARKETS} 個市場。")
            return
        blocked = await self._hard_stop_guard()
        if blocked:
            await self._reply(update, blocked)
            return
        await self._call_and_reply(update, f"啟動市場迴圈（目標 {count} 個市場）", ("start_loop", "start"), count)

    async def cmd_predict_stop(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if await self._deny_if_unauthorized(update):
            return
        await self._call_and_reply(update, "停止新市場", ("stop_loop", "stop"))

    async def cmd_predict_cancel_loop(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Request an expiring confirmation before cancelling the full loop."""

        if await self._deny_if_unauthorized(update):
            return
        try:
            current = await self._invoke(("status", "predict_status"))
        except Exception as exc:  # noqa: BLE001 - do not expose runtime details
            await self._reply(update, f"無法確認目前 Loop，未建立取消操作。\n錯誤類型：{type(exc).__name__}")
            return
        active = isinstance(current, Mapping) and bool(current.get("loop_active"))
        if not active and isinstance(current, Mapping) and current.get("loop_id"):
            active = str(current.get("loop_state") or "RUNNING").upper() == "RUNNING"
        if not active:
            await self._reply(update, "目前沒有可取消的執行中 Loop。")
            return
        token = str(self._token_factory())
        pending = _PendingLoopCancel(
            token,
            _chat_id(update) or "",
            self._now_ms() + self._confirmation_ttl_ms,
        )
        async with self._lock:
            self._pending_loop_cancel = pending
        keyboard = InlineKeyboardMarkup(
            [[
                InlineKeyboardButton(
                    "確認取消整個 Loop",
                    callback_data=f"{CANCEL_LOOP_CALLBACK_PREFIX}confirm:{token}",
                ),
                InlineKeyboardButton("保留 Loop", callback_data=f"{CANCEL_LOOP_CALLBACK_PREFIX}cancel"),
            ]]
        )
        await self._reply(
            update,
            "取消整個 Loop 會停止後續市場、取消未完成訂單，並永久結束這條 Loop（已完成資料與 PnL 保留）。\n"
            "若交易所仍有持倉或狀態不明，系統會拒絕標記完成並維持 Hard Stop。\n"
            "請在 60 秒內確認：",
            reply_markup=keyboard,
        )

    async def _confirm_loop_cancel(self, update: Update, token: str) -> None:
        if await self._deny_if_unauthorized(update):
            return
        async with self._lock:
            pending = self._pending_loop_cancel
            self._pending_loop_cancel = None
        if pending is None or pending.token != token or pending.chat_id != (_chat_id(update) or ""):
            await self._reply(update, "取消 Loop 確認碼無效或已使用，Loop 未變更。")
            return
        if self._now_ms() >= pending.expires_at_ms:
            await self._reply(update, "取消 Loop 確認碼已過期，Loop 未變更。")
            return
        try:
            result = await self._invoke(("cancel_loop", "cancel_prediction_loop"))
        except Exception as exc:  # noqa: BLE001 - full cancellation remains fail-closed
            await self._reply(update, f"取消整個 Loop 失敗，系統維持安全狀態。\n錯誤類型：{type(exc).__name__}")
            return
        if isinstance(result, Mapping) and bool(result.get("loop_cancelled")):
            await self._reply(
                update,
                "【整個 Loop 已取消】\n"
                f"Loop：{_human_scalar(result.get('cancelled_loop_id'))}\n"
                f"已取消市場：{_human_scalar(result.get('cancelled_campaigns', 0))}\n"
                f"已取消訂單：{_human_scalar(result.get('cancelled_orders', 0))}\n"
                "已停止新增與後續交易；歷史成交、結算與 PnL 保留。",
            )
            return
        if isinstance(result, Mapping) and bool(result.get("action_denied")):
            reason = _human_scalar(result.get("reason"))
            loop_id = _human_scalar(result.get("loop_id"))
            loop_state = _human_scalar(result.get("loop_state"))
            msg = "【取消 Loop】未執行\n原因：{}\n目前 loop={}｜state={}".format(reason, loop_id, loop_state)
            await self._reply(update, msg)
            return
        sent = await self._reply(update, format_runtime_result("取消整個 Loop 未執行", result))
        if sent is None:
            LOGGER.error("prediction_telegram_cancel_reply_silent")

    async def cmd_predict_pause(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if await self._deny_if_unauthorized(update):
            return
        await self._call_and_reply(update, "暫停新訂單", ("pause", "pause_new_orders"))

    async def cmd_predict_resume(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if await self._deny_if_unauthorized(update):
            return
        blocked = await self._hard_stop_guard()
        if blocked:
            await self._reply(update, blocked)
            return
        await self._call_and_reply(update, "恢復新訂單", ("resume",))

    async def cmd_predict_risk(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if await self._deny_if_unauthorized(update):
            return
        payload, error = await self._read_risk()
        if error:
            await self._reply(update, f"【風控狀態】\n讀取失敗，系統已維持安全狀態。\n原因：{error}")
            return
        # Latch a hard-stop date as soon as it is observed, including after a
        # process restart.  No command in this class clears that latch.
        if _mapping_hard_stop(payload):
            self._hard_stop_date = self._today_key()
        await self._reply(
            update,
            format_runtime_result("風控狀態", payload),
            reply_markup=self._hard_stop_reset_markup() if _mapping_hard_stop(payload) else None,
        )

    async def cmd_predict_hard_stop_reset(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Show a bound, expiring confirmation for a guarded hard-stop reset."""

        if await self._deny_if_unauthorized(update):
            return
        payload, error = await self._read_risk()
        if error:
            await self._reply(update, f"無法確認 Hard Stop，未建立 Reset。\n原因：{error}")
            return
        if not _mapping_hard_stop(payload):
            self._hard_stop_date = None
            await self._reply(update, "目前沒有 Hard Stop，不需要 Reset。")
            return
        token = str(self._token_factory())
        pending = _PendingHardStopReset(
            token,
            _chat_id(update) or "",
            self._now_ms() + self._confirmation_ttl_ms,
        )
        async with self._lock:
            self._pending_hard_stop_reset = pending
        keyboard = InlineKeyboardMarkup(
            [[
                InlineKeyboardButton(
                    "確認 Reset（執行）",
                    callback_data=f"{HARD_STOP_CALLBACK_PREFIX}confirm:{token}",
                ),
                InlineKeyboardButton("取消", callback_data=f"{HARD_STOP_CALLBACK_PREFIX}cancel"),
            ]]
        )
        await self._reply(
            update,
            "Hard Stop Reset 會保留歷史 PnL，但把現在設為新的風控起點。\n"
            "不設每日次數上限；每次仍須通過交易同步與零曝險檢查。重置後仍維持暫停，需另按「恢復 Loop」才會下單。\n"
            "請在 60 秒內確認：",
            reply_markup=keyboard,
        )

    async def _confirm_hard_stop_reset(self, update: Update, token: str) -> None:
        if await self._deny_if_unauthorized(update):
            return
        async with self._lock:
            pending = self._pending_hard_stop_reset
            self._pending_hard_stop_reset = None
        if pending is None or pending.token != token or pending.chat_id != (_chat_id(update) or ""):
            await self._reply(update, "Reset 確認碼無效或已使用，Hard Stop 未變更。")
            return
        if self._now_ms() >= pending.expires_at_ms:
            await self._reply(update, "Reset 確認碼已過期，Hard Stop 未變更。")
            return
        payload, error = await self._read_risk()
        if error or not _mapping_hard_stop(payload):
            if not error:
                self._hard_stop_date = None
            await self._reply(update, "確認時 Hard Stop 狀態已改變，未執行 Reset。")
            return
        try:
            result = await self._invoke(
                ("reset_hard_stop_once", "reset_hard_stop"),
                "telegram operator hard-stop reset",
            )
        except Exception as exc:  # noqa: BLE001 - reset must stay fail-closed
            await self._reply(update, f"Hard Stop Reset 失敗，系統仍保持鎖定。\n錯誤類型：{type(exc).__name__}")
            return
        if isinstance(result, Mapping) and bool(result.get("hard_stop_reset")):
            self._hard_stop_date = None
            await self._reply(
                update,
                "【Hard Stop Reset 完成】\n"
                f"Reset 前今日 PnL：{_human_scalar(result.get('prior_daily_net_pnl', '0'))} USDT\n"
                "新風控區間：0 USDT\n"
                "目前仍暫停。保留原策略可按「恢復 Loop」；若要換策略，先停止並選 Lane，再用 Loop 5/10/20 開新 Loop。",
            )
            return
        await self._reply(update, format_runtime_result("Hard Stop Reset 未執行", result))

    async def cmd_predict_reconcile(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if await self._deny_if_unauthorized(update):
            return
        await self._call_and_reply(update, "未完成訂單同步", ("reconcile", "predict_reconcile"))

    async def _promotion_gate(self) -> PromotionGate:
        try:
            result = await self._invoke(("promotion_gate", "get_promotion_gate", "check_promotion_gate"))
        except Exception as exc:  # noqa: BLE001 - no gate means no live switch
            return PromotionGate(False, (f"promotion gate unavailable: {exc}",))
        return _normalize_gate(result)

    async def _live_preflight(self) -> PromotionGate | None:
        """Re-check controller authority when the runtime exposes it.

        Older in-memory manager fakes do not have a network preflight method;
        the production runtime is the :class:`PredictionController`, whose
        ``set_shadow_mode(False)`` performs the same check again immediately
        before touching the worker.  Keeping this optional preserves that
        adapter contract while making the real Telegram path fail closed.
        """

        method = getattr(self.runtime, "preflight", None)
        if method is None:
            return None
        try:
            try:
                inspect.signature(method).bind(require_live=True)
            except (TypeError, ValueError):
                result = method()
            else:
                result = method(require_live=True)
            if _is_awaitable(result):
                result = await result
        except Exception as exc:  # noqa: BLE001
            return PromotionGate(False, (f"live preflight unavailable: {exc}",))
        return _normalize_gate(result)

    async def _arm_live(self, update: Update) -> None:
        """Arm Live after preflight. Do not require the inline confirm button.

        ``/predict_live on`` is the operator command; the controller still
        re-runs signed preflight and, when configured, skips Shadow promotion.
        """

        blocked = await self._hard_stop_guard()
        if blocked:
            await self._reply(update, blocked)
            return
        preflight = await self._live_preflight()
        if preflight is not None and not preflight.passed:
            reasons = "；".join(preflight.reasons) or "Live 前置檢查未通過"
            await self._reply(update, f"Live 前置檢查未通過\n原因：{reasons}")
            return
        try:
            result = await self._invoke(("set_shadow_mode", "set_shadow"), False)
        except Exception as exc:  # noqa: BLE001
            await self._reply(update, f"Live 切換失敗，系統已維持安全狀態。\n錯誤類型：{type(exc).__name__}")
            return
        if isinstance(result, Mapping) and bool(result.get("action_denied")):
            await self._reply(update, format_runtime_result("Live 切換未通過", result))
            return
        await self._reply(update, format_runtime_result("已切換至 Live（實際交易）", result))

    async def _request_live_confirmation(self, update: Update) -> None:
        preflight = await self._live_preflight()
        if preflight is not None and not preflight.passed:
            reasons = "；".join(preflight.reasons) or "Live 前置檢查未通過"
            await self._reply(update, f"Live 前置檢查未通過\n原因：{reasons}")
            return
        gate = await self._promotion_gate()
        if not gate.passed:
            reasons = "；".join(gate.reasons) or "Live 升級檢查未通過"
            await self._reply(update, f"Live 升級檢查未通過\n原因：{reasons}")
            return
        token = str(self._token_factory())
        pending = _PendingLiveConfirmation(token, _chat_id(update) or "", self._now_ms() + self._confirmation_ttl_ms)
        async with self._lock:
            self._pending_confirmation = pending
        keyboard = InlineKeyboardMarkup(
            [[
                InlineKeyboardButton("確認切換 Live（實際交易）", callback_data=f"{LIVE_CALLBACK_PREFIX}confirm:{token}"),
                InlineKeyboardButton("取消", callback_data=f"{LIVE_CALLBACK_PREFIX}cancel"),
            ]]
        )
        await self._reply(update, "Live 升級檢查已通過。\n請在 60 秒內按下「確認切換 Live（實際交易）」：", reply_markup=keyboard)

    async def _confirm_live(self, update: Update, token: str) -> None:
        if await self._deny_if_unauthorized(update):
            return
        async with self._lock:
            pending = self._pending_confirmation
            self._pending_confirmation = None
        if pending is None or pending.token != token or pending.chat_id != (_chat_id(update) or ""):
            await self._reply(update, "確認碼無效或已使用，未切換 Live。")
            return
        if self._now_ms() >= pending.expires_at_ms:
            await self._reply(update, "確認碼已過期，未切換 Live。")
            return
        blocked = await self._hard_stop_guard()
        if blocked:
            await self._reply(update, blocked)
            return
        preflight = await self._live_preflight()
        if preflight is not None and not preflight.passed:
            await self._reply(update, "確認時 Live 前置檢查已失效，未切換 Live。")
            return
        gate = await self._promotion_gate()
        if not gate.passed:
            await self._reply(update, "確認時 Live 升級檢查已失效，未切換 Live。")
            return
        try:
            result = await self._invoke(("set_shadow_mode", "set_shadow"), False)
        except Exception as exc:  # noqa: BLE001
            await self._reply(update, f"Live 切換失敗，系統已維持安全狀態。\n錯誤類型：{type(exc).__name__}")
            return
        await self._reply(update, format_runtime_result("已切換至 Live（實際交易）", result))

    async def cmd_predict_shadow(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if await self._deny_if_unauthorized(update):
            return
        raw_args = [str(arg) for arg in (getattr(context, "args", ()) or ())]
        args = [arg.lower() for arg in raw_args]
        mode = args[0] if args else "status"
        if mode in {"on", "shadow"}:
            await self._call_and_reply(update, "已切換至 Shadow（觀察模式）", ("set_shadow_mode", "set_shadow"), True)
            return
        if mode in {"off", "live"}:
            blocked = await self._hard_stop_guard()
            if blocked:
                await self._reply(update, blocked)
                return
            await self._request_live_confirmation(update)
            return
        if mode == "confirm" and len(raw_args) == 2:
            await self._confirm_live(update, raw_args[1])
            return
        if mode not in {"status", ""}:
            await self._reply(update, "用法：/predict_shadow on|off\non = 切換至 Shadow；off = 申請切換至 Live（需檢查與確認）。")
            return
        await self._call_and_reply(update, "Shadow/Live 模式狀態", ("status", "predict_status"))

    async def cmd_predict_live(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Explicit live/shadow switch for operators.

        The existing /predict_shadow command remains supported for backwards
        compatibility. The live-on path intentionally shares the same
        hard-stop, preflight, promotion-gate, and inline confirmation sequence.
        """

        if await self._deny_if_unauthorized(update):
            return
        raw_args = [str(arg) for arg in (getattr(context, "args", ()) or ())]
        args = [arg.lower() for arg in raw_args]
        mode = args[0] if args else "status"
        if mode in {"on", "live"}:
            await self._arm_live(update)
            return
        if mode in {"off", "shadow"}:
            await self._call_and_reply(update, "已關閉 Live，回到 Shadow（觀察模式）", ("set_shadow_mode", "set_shadow"), True)
            return
        if mode == "confirm" and len(raw_args) == 2:
            await self._confirm_live(update, raw_args[1])
            return
        if mode not in {"status", ""}:
            await self._reply(update, "用法：/predict_live on|off\non = 通過前置檢查後切換至 Live；off = 回到 Shadow。")
            return
        await self._call_and_reply(update, "Live/Shadow 模式狀態", ("status", "predict_status"))

    async def handle_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        query = getattr(update, "callback_query", None)
        if query is None:
            return
        data = str(getattr(query, "data", ""))
        if await self._deny_if_unauthorized(update):
            return
        answer = getattr(query, "answer", None)
        if answer is not None:
            try:
                result = answer()
                if _is_awaitable(result):
                    await result
            except Exception as exc:  # noqa: BLE001 - continue to handle the callback
                LOGGER.warning("prediction_telegram_callback_answer_failed error_type=%s", type(exc).__name__)
        if data.startswith("predict_market:"):
            await self._call_and_reply(update, "T6.7c／T6.9／T6.9b 整輪市場", ("select_market",), data.split(":", 1)[1])
            return
        if data == f"{MONITOR_CALLBACK_PREFIX}show":
            await self.cmd_predict_monitor(update, context)
            return
        if data == f"{LIVE_CALLBACK_PREFIX}cancel":
            async with self._lock:
                self._pending_confirmation = None
            await self._reply(update, "已取消 Live 切換。")
            return
        if data == f"{CANCEL_LOOP_CALLBACK_PREFIX}request":
            await self.cmd_predict_cancel_loop(update, context)
            return
        if data == f"{CANCEL_LOOP_CALLBACK_PREFIX}cancel":
            async with self._lock:
                self._pending_loop_cancel = None
            await self._reply(update, "已保留目前 Loop，未執行取消。")
            return
        cancel_loop_marker = f"{CANCEL_LOOP_CALLBACK_PREFIX}confirm:"
        if data.startswith(cancel_loop_marker):
            await self._confirm_loop_cancel(update, data[len(cancel_loop_marker):])
            return
        if data == f"{HARD_STOP_CALLBACK_PREFIX}request":
            await self.cmd_predict_hard_stop_reset(update, context)
            return
        if data == f"{HARD_STOP_CALLBACK_PREFIX}cancel":
            async with self._lock:
                self._pending_hard_stop_reset = None
            await self._reply(update, "已取消 Hard Stop Reset，鎖定維持不變。")
            return
        hard_stop_marker = f"{HARD_STOP_CALLBACK_PREFIX}confirm:"
        if data.startswith(hard_stop_marker):
            await self._confirm_hard_stop_reset(update, data[len(hard_stop_marker):])
            return
        lane_marker = LANE_CALLBACK_PREFIX
        if data.startswith(lane_marker):
            profile = data[len(lane_marker):].strip().lower()
            try:
                status_now = await self._invoke(("status", "predict_status"))
            except Exception:
                status_now = {}
            market_symbol = self._market_symbol(status_now if isinstance(status_now, Mapping) else None)
            allowed = {item[0] for item in selectable_lanes_for_market(market_symbol)}
            if profile not in allowed:
                await self._reply(
                    update,
                    f"策略選項無效（此市場不可選），請重新開啟選單。\nmarket={_human_scalar(market_symbol) or '未知'}｜profile={_human_scalar(profile)}",
                )
                return
            if profile == "fav_p3":
                try:
                    await self._invoke(("set_fav_p3_arm",), "live")
                    result = await self._invoke(("select_strategy", "set_strategy_profile"), "fav_p3")
                except Exception as exc:  # noqa: BLE001
                    await self._reply(update, f"【P3】\n切換失敗：{type(exc).__name__}")
                    return
                if not isinstance(result, Mapping):
                    await self._reply(update, "【P3】\n切換失敗：無回傳")
                    return
                if result.get("action_denied"):
                    await self._reply(
                        update,
                        "【P3】未套用\n"
                        f"原因：{_human_scalar(result.get('reason'))}\n"
                        f"目前策略仍是：{_human_scalar(result.get('strategy_profile'))}",
                    )
                    return
                strat = result.get("strategy_profile")
                next_s = result.get("next_strategy_profile")
                queued = bool(result.get("strategy_queued"))
                changed = bool(result.get("strategy_changed"))
                if queued:
                    await self._reply(
                        update,
                        "【P3 已排隊】\n"
                        f"目前 Loop 仍是：{_human_scalar(strat)}\n"
                        f"下一個 Loop：{_human_scalar(next_s)}（應為 fav_p3）\n"
                        "內容：只 FAV＋P3 閘｜1U｜無 R3。先停 Loop 再開，或等本輪結束。",
                    )
                    return
                if str(strat or "").strip().lower() != "fav_p3":
                    await self._reply(
                        update,
                        "【P3】未生效\n"
                        f"回傳策略={_human_scalar(strat)}｜next={_human_scalar(next_s)}\n"
                        f"changed={_human_scalar(changed)}｜reason={_human_scalar(result.get('reason'))}",
                    )
                    return
                await self._reply(
                    update,
                    "【P3 lane 已選】\n"
                    f"策略：P3（FAV＋閘｜無R3）＝{_human_scalar(strat)}\n"
                    "門檻：pre_cross=0 ∧ same_side≥30s ∧ dist≥3bps｜1U｜無 R3。\n"
                    "關掉：選其他策略。",
                )
                return
            try:
                await self._invoke(("set_fav_p3_arm",), "off")
            except Exception:
                pass
            try:
                result = await self._invoke(("select_strategy", "set_strategy_profile"), profile)
            except Exception as exc:  # noqa: BLE001 - keep Telegram response safe
                await self._reply(update, f"【選擇策略】\n切換失敗：{type(exc).__name__}")
                return
            if not isinstance(result, Mapping):
                await self._reply(update, "【選擇策略】\n切換失敗：無回傳")
                return
            if result.get("action_denied"):
                await self._reply(
                    update,
                    "【選擇策略】未套用\n"
                    f"原因：{_human_scalar(result.get('reason'))}\n"
                    f"目前策略仍是：{_human_scalar(result.get('strategy_profile'))}\n"
                    f"下一個：{_human_scalar(result.get('next_strategy_profile'))}",
                )
                return
            label = next(
                (lab for prof, lab in selectable_lanes_for_market(market_symbol) if prof == profile),
                profile,
            )
            if bool(result.get("strategy_queued")):
                rearm_note = (
                    "\n新策略套用後需重新確認 Live，舊策略的 Live 授權不會沿用。"
                    if bool(result.get("live_rearm_required"))
                    else ""
                )
                await self._reply(
                    update,
                    "【策略已排程】\n"
                    f"選項：{label}\n"
                    f"目前 Loop：{_human_scalar(result.get('strategy_profile'))}\n"
                    f"下一個 Loop：{_human_scalar(result.get('next_strategy_profile'))}\n"
                    f"目前 Loop 不會混用策略。{rearm_note}"
                    f"{amount_lock_note(result.get('next_strategy_profile'))}",
                )
                return
            if bool(result.get("previous_loop_closed")):
                rearm_note = (
                    "\nLive 已安全解除；請重新執行 /predict_live on 並確認後再開 Loop。"
                    if bool(result.get("live_rearm_required"))
                    else ""
                )
                await self._reply(
                    update,
                    "【策略切換完成】\n"
                    f"舊 Loop：{_human_scalar(result.get('previous_loop_id'))} "
                    f"({result.get('previous_loop_completed', 0)}/{result.get('previous_loop_target', 0)}) 已保留並結束\n"
                    f"新策略：{label}＝{_human_scalar(result.get('strategy_profile'))}\n"
                    f"請按 Loop 5/10/20/100 開始新的 Loop。{rearm_note}"
                    f"{amount_lock_note(result.get('strategy_profile'))}",
                )
                return
            if bool(result.get("strategy_changed")):
                rearm_note = (
                    "\nLive 已安全解除；請重新執行 /predict_live on 並確認後再開 Loop。"
                    if bool(result.get("live_rearm_required"))
                    else ""
                )
                await self._reply(
                    update,
                    "【策略切換完成】\n"
                    f"新策略：{label}＝{_human_scalar(result.get('strategy_profile'))}\n"
                    f"請按 Loop 5/10/20 開始新的 Loop。{rearm_note}"
                    f"{amount_lock_note(result.get('strategy_profile'))}",
                )
                return
            strat = str(result.get("strategy_profile") or "").strip().lower()
            next_s = str(result.get("next_strategy_profile") or "").strip().lower()
            if strat != profile and next_s != profile:
                await self._reply(
                    update,
                    "【選擇策略】未生效\n"
                    f"點選：{label}（{profile}）\n"
                    f"回傳目前={_human_scalar(strat)}｜下一個={_human_scalar(next_s)}\n"
                    f"reason={_human_scalar(result.get('reason'))}",
                )
                return
            await self._reply(
                update,
                "【選擇策略】\n"
                f"{label}｜目前={_human_scalar(strat)}｜下一個={_human_scalar(next_s)}\n"
                f"reason={_human_scalar(result.get('reason') or 'ok')}",
            )
            return

        amount_marker = ORDER_UNIT_CALLBACK_PREFIX
        if data.startswith(amount_marker):
            selected = data[len(amount_marker):].strip()
            if selected not in {"1", "2", "3"}:
                await self._reply(update, "單筆金額選項無效，請重新開啟選單。")
                return
            try:
                result = await self._invoke(("select_order_unit", "set_order_unit_usdt"), selected)
            except Exception as exc:  # noqa: BLE001 - keep Telegram response safe
                await self._reply(update, f"【選擇單筆金額】\n切換失敗：{type(exc).__name__}")
                return
            if isinstance(result, Mapping) and bool(result.get("action_denied")):
                await self._reply(update, format_runtime_result("單筆金額未變更", result))
                return
            if isinstance(result, Mapping) and bool(result.get("order_unit_queued")):
                profile = str(result.get("strategy_profile") or "").strip().lower()
                c180 = profile == C180_PROFILE
                next_protection = (
                    _c180_risk_text(result.get("next_order_unit_usdt"))
                    if c180 else
                    _regime_risk_text(profile, result.get("next_order_unit_usdt"))
                    if profile in (REGIME_T62_PROFILE, REGIME_T63_PROFILE, REGIME_T63A_PROFILE, REGIME_T63B_PROFILE, REGIME_T65_PROFILE, REGIME_T67_PROFILE, REGIME_T67A_PROFILE, REGIME_T67B_PROFILE, REGIME_T67C_PROFILE, REGIME_T67D_PROFILE, REGIME_T68_PROFILE, REGIME_T68A_PROFILE, REGIME_T69_PROFILE, REGIME_T69A_PROFILE) else
                    f"下一個 Loop 虧損上限：{_human_scalar(result.get('next_loop_loss_limit'))} USDT"
                )
                await self._reply(
                    update,
                    "【單筆金額已排程】\n"
                    f"目前 Loop 首筆：{_human_scalar(result.get('order_unit_usdt'))} USDT\n"
                    f"下一個 Loop 首筆：{_human_scalar(result.get('next_order_unit_usdt'))} USDT\n"
                    f"{next_protection}\n"
                    "目前 Loop 不會中途放大金額；新 Loop 套用後需重新確認 Live。",
                )
                return
            if isinstance(result, Mapping):
                profile = str(result.get("strategy_profile") or "").strip().lower()
                c180 = profile == C180_PROFILE
                protection = (
                    _c180_risk_text(result.get("order_unit_usdt"))
                    if c180 else
                    _regime_risk_text(profile, result.get("order_unit_usdt"))
                    if profile in (REGIME_T62_PROFILE, REGIME_T63_PROFILE, REGIME_T63A_PROFILE, REGIME_T63B_PROFILE, REGIME_T65_PROFILE, REGIME_T67_PROFILE, REGIME_T67A_PROFILE, REGIME_T67B_PROFILE, REGIME_T67C_PROFILE, REGIME_T67D_PROFILE, REGIME_T68_PROFILE, REGIME_T68A_PROFILE, REGIME_T69_PROFILE, REGIME_T69A_PROFILE) else
                    f"Loop 虧損上限：{_human_scalar(result.get('loop_loss_limit'))} USDT｜"
                    f"每日虧損上限：{_human_scalar(result.get('daily_loss_limit'))} USDT"
                )
                rearm_note = (
                    "\nLive 已安全解除；請重新執行 /predict_live on 並確認後再開 Loop。"
                    if bool(result.get("live_rearm_required"))
                    else ""
                )
                previous_note = (
                    f"\n舊 Loop：{_human_scalar(result.get('previous_loop_id'))} 已安全結束。"
                    if bool(result.get("previous_loop_closed"))
                    else ""
                )
                await self._reply(
                    update,
                    "【單筆金額切換完成】\n"
                    f"首筆實際下單：{_human_scalar(result.get('initial_order_usdt', result.get('order_unit_usdt')))} USDT\n"
                    f"單市場投入上限：{_human_scalar(result.get('market_buy_cap_usdt'))} USDT\n"
                    f"加倉：{_human_scalar(result.get('scale_in_enabled'))}\n"
                    f"{protection}"
                    f"{previous_note}{rearm_note}",
                )
                return
            await self._reply(update, format_runtime_result("單筆金額已更新", result))
            return
        marker = f"{LIVE_CALLBACK_PREFIX}confirm:"
        if data.startswith(marker):
            await self._confirm_live(update, data[len(marker):])
            return
        await self._reply(update, "找不到這個操作，請使用選單。")


    async def cmd_t67creport(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Read the independent full T6.7c observer; never invoke trading control."""
        if await self._deny_if_unauthorized(update):
            return
        args = list(getattr(context, "args", None) or [])
        if len(args)>2 or (args and args[0] not in ('20','40','100')) or (len(args)==2 and args[1].upper() not in ('BTC','ETH','BNB')):
            await self._reply(update, "用法：/t67creport [20|40|100] [BTC|ETH|BNB]；預設三幣最近20場。")
            return
        try:
            from pathlib import Path
            from operators.t67c_multimarket_observer.report import load_render
            root=Path(__file__).resolve().parents[3]
            text=await asyncio.to_thread(load_render,root,int(args[0]) if args else 20,
                args[1].upper()+'USDT' if len(args)==2 else None,self._now_ms())
            await self._reply(update,text,parse_mode=None)
        except Exception:
            LOGGER.warning('t67c_observer_report_unavailable')
            await self._reply(update,'【T6.7c三市場觀測】暫時無法讀取；不代表沒有訊號或零損益。')

    async def cmd_firstreport(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Read the independent First observer snapshot; no runtime operations."""
        if await self._deny_if_unauthorized(update):
            return
        args = list(getattr(context, "args", None) or [])
        if len(args) > 1 or (args and args[0] not in ("20", "40", "100")):
            await self._reply(update, "用法：/firstreport [20|40|100]；預設最近20場。")
            return
        window = int(args[0]) if args else 20
        try:
            from pathlib import Path
            root = Path(__file__).resolve().parents[3]
            text = await asyncio.to_thread(_format_first_observer_report, root, window, now_ms=self._now_ms())
            await self._reply(update, text, parse_mode=None)
        except Exception:
            # Do not expose files, signed URLs or credentials to chat.
            LOGGER.warning("first_observer_report_unavailable")
            await self._reply(update, "【First三市場觀測】暫時無法讀取；這不代表沒有訊號或零損益。")

    async def cmd_predict_report(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Show the selected T6.7 family; historical lanes stay out of it."""
        if await self._deny_if_unauthorized(update):
            return
        try:
            import asyncio
            import pathlib
            root = pathlib.Path(__file__).resolve().parent.parent.parent.parent
            from .live_report import format_live_report, report_pages, t67_family_report_profile
            profile = await asyncio.to_thread(t67_family_report_profile, root)
            report = await asyncio.to_thread(format_live_report, root, profile_filter=profile)
            for page in report_pages(report):
                await self._reply(update, page, parse_mode=None)
        except Exception:  # noqa: BLE001 - do not leak DB or runtime details to Telegram
            LOGGER.exception("prediction_c180_live_report_failed")
            await self._reply(update, "【Live Report】暫時無法讀取；未以其他 lane 的績效替代。")

    async def cmd_predict_shadow_report(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Keep the historical shadow report under its explicit command."""
        if await self._deny_if_unauthorized(update):
            return
        try:
            import asyncio
            import sys
            import pathlib
            root = str(pathlib.Path(__file__).resolve().parent.parent.parent.parent)
            from .live_report import T67A_PROFILE, T67B_PROFILE, T67C_PROFILE, T67D_PROFILE, T68_PROFILE, T68A_PROFILE, T69_PROFILE, T69A_PROFILE, t67_family_report_profile
            if await asyncio.to_thread(t67_family_report_profile, root) in (T67A_PROFILE, T67B_PROFILE, T67C_PROFILE, T67D_PROFILE, T68_PROFILE, T68A_PROFILE, T69_PROFILE, T69A_PROFILE):
                await self.cmd_predict_report(update, context)
                return
            if root not in sys.path:
                sys.path.insert(0, root)
            from scripts.report_shadow_batches import chunk_html_message, generate_report
            text = await generate_report()
            for chunk in chunk_html_message(text):
                await self._reply(update, chunk, parse_mode="HTML")
            if text.startswith(('<b>Paired8','<b>Last T1','<b>Vol Shadow')):
                return
            try:
                import asyncio
                supplement = await asyncio.to_thread(_format_aligned_shadow_supplement, root)
            except Exception:
                supplement = '【第四條補充】暫時無法讀取；原三條報告不受影響。'
            if supplement:
                await self._reply(update, supplement, parse_mode="HTML")
            try:
                guard_reports = await asyncio.to_thread(_format_guard_shadow_supplements, root)
            except Exception:
                guard_reports = ['【第五／六條補充】暫時無法讀取；原報告不受影響。']
            for guard_report in guard_reports:
                await self._reply(update, guard_report, parse_mode="HTML")
        except Exception as exc:  # noqa: BLE001
            await self._reply(update, f"【報告產出失敗】: {exc}")


def build_prediction_handlers(service: PredictionTelegramService) -> tuple[Any, ...]:
    """Build handlers without constructing an Application or reading config."""

    return (
        # Observer reports (/firstreport, /t67creport) are off Telegram: the
        # VM runs one coin's producers only and no research observers.
        CommandHandler("report", service.cmd_predict_report),
        CommandHandler("predict_report", service.cmd_predict_report),
        CommandHandler("shadow_report", service.cmd_predict_shadow_report),
        CommandHandler("predict_status", service.cmd_predict_status),
        CommandHandler("predict_monitor", service.cmd_predict_monitor),
        CommandHandler("predict_start", service.cmd_predict_start),
        CommandHandler("predict_one_run", service.cmd_predict_one_run),
        CommandHandler("predict_loop_5", service.cmd_predict_loop_5),
        CommandHandler("predict_loop_10", service.cmd_predict_loop_10),
        CommandHandler("predict_loop_20", service.cmd_predict_loop_20),
        CommandHandler("predict_loop_100", service.cmd_predict_loop_100),
        CommandHandler("predict_loop_pnl", service.cmd_predict_loop_pnl),
        CommandHandler("predict_market", service.cmd_predict_market),
        CommandHandler("predict_lane", service.cmd_predict_lane),
        CommandHandler("predict_amount", service.cmd_predict_amount),
        CommandHandler("predict_stop", service.cmd_predict_stop),
        CommandHandler("predict_cancel", service.cmd_predict_cancel_loop),
        CommandHandler("predict_pause", service.cmd_predict_pause),
        CommandHandler("predict_resume", service.cmd_predict_resume),
        CommandHandler("predict_risk", service.cmd_predict_risk),
        CommandHandler("predict_hard_stop_reset", service.cmd_predict_hard_stop_reset),
        CommandHandler("predict_reconcile", service.cmd_predict_reconcile),
        CommandHandler("predict_shadow", service.cmd_predict_shadow),
        CommandHandler("predict_live", service.cmd_predict_live),
        CommandHandler("lanes", service.cmd_lanes),
        CommandHandler("predict_lanes", service.cmd_lanes),
        CallbackQueryHandler(service.handle_callback, pattern=r"^predict_(shadow|lane|market|amount|hard_stop|cancel|monitor):"),
    )


def register_prediction_handlers(application: Any, service: PredictionTelegramService) -> tuple[Any, ...]:
    handlers = build_prediction_handlers(service)
    for handler in handlers:
        application.add_handler(handler)
    return handlers


def _service_from_context(context: ContextTypes.DEFAULT_TYPE) -> PredictionTelegramService:
    service = context.application.bot_data.get("prediction_telegram")
    if not isinstance(service, PredictionTelegramService):
        raise RuntimeError("prediction_telegram service is not registered")
    return service


async def cmd_predict_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _service_from_context(context).cmd_predict_status(update, context)


async def cmd_predict_monitor(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _service_from_context(context).cmd_predict_monitor(update, context)


async def cmd_predict_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _service_from_context(context).cmd_predict_start(update, context)


async def cmd_predict_one_run(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _service_from_context(context).cmd_predict_one_run(update, context)


async def cmd_predict_loop_5(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _service_from_context(context).cmd_predict_loop_5(update, context)


async def cmd_predict_loop_10(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _service_from_context(context).cmd_predict_loop_10(update, context)


async def cmd_predict_loop_20(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _service_from_context(context).cmd_predict_loop_20(update, context)


async def cmd_predict_loop_100(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _service_from_context(context).cmd_predict_loop_100(update, context)


async def cmd_predict_loop_pnl(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _service_from_context(context).cmd_predict_loop_pnl(update, context)

async def cmd_predict_stop(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _service_from_context(context).cmd_predict_stop(update, context)


async def cmd_predict_cancel_loop(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _service_from_context(context).cmd_predict_cancel_loop(update, context)


async def cmd_predict_pause(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _service_from_context(context).cmd_predict_pause(update, context)


async def cmd_predict_resume(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _service_from_context(context).cmd_predict_resume(update, context)


async def cmd_predict_risk(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _service_from_context(context).cmd_predict_risk(update, context)


async def cmd_predict_hard_stop_reset(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _service_from_context(context).cmd_predict_hard_stop_reset(update, context)


async def cmd_predict_reconcile(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _service_from_context(context).cmd_predict_reconcile(update, context)


async def cmd_predict_shadow(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _service_from_context(context).cmd_predict_shadow(update, context)


async def cmd_predict_live(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _service_from_context(context).cmd_predict_live(update, context)


__all__ = [
    "DEFAULT_LOOP_MARKETS",
    "CANCEL_LOOP_CALLBACK_PREFIX",
    "HARD_STOP_CALLBACK_PREFIX",
    "LIVE_CALLBACK_PREFIX",
    "MONITOR_CALLBACK_PREFIX",
    "ORDER_UNIT_CALLBACK_PREFIX",
    "MAX_LOOP_MARKETS",
    "PredictionRuntime",
    "PredictionTelegramService",
    "PromotionGate",
    "build_prediction_handlers",
    "register_prediction_handlers",
    "format_runtime_result",
    "format_monitor_result",
    "cmd_predict_status",
    "cmd_predict_monitor",
    "cmd_predict_start",
    "cmd_predict_one_run",
    "cmd_predict_loop_5",
    "cmd_predict_loop_10",
    "cmd_predict_loop_20",
    "cmd_predict_loop_100",
    "cmd_predict_loop_pnl",
    "cmd_predict_stop",
    "cmd_predict_cancel_loop",
    "cmd_predict_pause",
    "cmd_predict_resume",
    "cmd_predict_risk",
    "cmd_predict_hard_stop_reset",
    "cmd_predict_reconcile",
    "cmd_predict_shadow",
    "cmd_predict_live",
]


def _format_c180_live_report(root, *, now_ms=None, loop_id=None):
    """Read one durable LIVE snapshot; never infer PnL from intents or fills."""
    import json
    import sqlite3
    import time
    from collections import defaultdict
    from contextlib import closing
    from datetime import datetime, timedelta, timezone
    from decimal import Decimal, InvalidOperation
    from itertools import groupby
    from pathlib import Path

    db = Path(root) / "prediction/data/prediction.sqlite3"
    now = int(time.time() * 1000) if now_ms is None else int(now_ms)
    with closing(sqlite3.connect(db.resolve().as_uri() + "?mode=ro", uri=True, timeout=2)) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA query_only=ON")
        conn.execute("BEGIN")
        loop = conn.execute(
            "SELECT loop_id,state,target,completed,new_entries_stopped,hard_stop_latched "
            "FROM prediction_loops WHERE mode='LIVE' AND strategy_profile='c180_favorite_hold_v1' "
            "AND (? IS NULL OR loop_id=?) "
            "ORDER BY CASE WHEN state='RUNNING' THEN 0 ELSE 1 END, created_at_ms DESC LIMIT 1", (loop_id, loop_id)
        ).fetchone()
        if loop is None:
            return "【C180 Live Report】尚無 C180 Live loop。"
        loop_id = str(loop["loop_id"])
        slots = conn.execute(
            "SELECT run_ordinal,verified_at_ms,empty_attested_at_ms "
            "FROM prediction_c180_slots WHERE loop_id=? "
            "ORDER BY run_ordinal", (loop_id,)
        ).fetchall()
        claims = conn.execute(
            "SELECT s.run_ordinal,q.campaign_id,q.unit_usdt,i.order_id,i.status,i.unknown "
            "FROM prediction_c180_entry_claims q "
            "JOIN prediction_c180_slots s ON s.loop_id=q.loop_id "
            "AND s.market_start_ms=q.market_start_ms "
            "LEFT JOIN prediction_order_intents i ON i.intent_id=q.intent_id "
            "WHERE q.loop_id=?", (loop_id,)
        ).fetchall()
        fills = conn.execute(
            "SELECT DISTINCT s.run_ordinal,c.campaign_id FROM prediction_fills f "
            "JOIN prediction_campaigns c ON c.campaign_id=f.campaign_id "
            "JOIN prediction_c180_slots s ON s.loop_id=c.loop_id "
            "AND s.market_start_ms=c.start_time_ms "
            "WHERE c.loop_id=? AND f.order_side='BUY'", (loop_id,)
        ).fetchall()
        settlements = conn.execute(
            "SELECT s.run_ordinal,c.campaign_id,p.settlement_id,p.status,"
            "p.net_pnl AS official_net,o.net_pnl AS observed_net,o.known_at_ms "
            "FROM prediction_settlements p "
            "JOIN prediction_campaigns c ON c.campaign_id=p.campaign_id "
            "JOIN prediction_c180_slots s ON s.loop_id=c.loop_id "
            "AND s.market_start_ms=c.start_time_ms "
            "LEFT JOIN prediction_c180_settlement_observations o "
            "ON o.settlement_id=p.settlement_id AND o.campaign_id=c.campaign_id "
            "WHERE c.loop_id=?", (loop_id,)
        ).fetchall()
        unknown = conn.execute(
            "SELECT COUNT(DISTINCT s.run_ordinal) FROM prediction_campaigns c "
            "JOIN prediction_c180_slots s ON s.loop_id=c.loop_id "
            "AND s.market_start_ms=c.start_time_ms "
            "WHERE c.loop_id=? AND (c.pending_unknown=1 OR EXISTS "
            "(SELECT 1 FROM prediction_order_intents i WHERE i.campaign_id=c.campaign_id "
            "AND i.unknown=1))", (loop_id,)
        ).fetchone()[0]
        gate_row = conn.execute(
            "SELECT config_value_json FROM prediction_runtime_config WHERE config_key=?",
            ("c180_batch_gate_runtime_v1",),
        ).fetchone()
        conn.commit()

    target = int(loop["target"])
    completed = int(loop["completed"])
    if target < 1 or not 0 <= completed <= target:
        raise ValueError("invalid C180 loop progress")
    slot_runs = {int(row["run_ordinal"]) for row in slots}
    empty_runs = {int(row["run_ordinal"]) for row in slots
                  if row["empty_attested_at_ms"] is not None}
    claim_runs = {int(row["run_ordinal"]) for row in claims}
    fill_campaigns = defaultdict(set)
    for row in fills:
        fill_campaigns[int(row["run_ordinal"])].add(str(row["campaign_id"]))
    filled_runs = set(fill_campaigns)
    claim_by_run = {int(row["run_ordinal"]): row for row in claims}
    anomalies = set()
    if slot_runs != set(range(1, target + 1)):
        anomalies.add("排程槽位不完整")
    if any(len(campaigns) != 1 for campaigns in fill_campaigns.values()):
        anomalies.add("同一 run 有多個成交 campaign")
    if any(run not in claim_runs for run in filled_runs):
        anomalies.add("成交缺少 C180 claim")

    by_campaign = defaultdict(list)
    for row in settlements:
        by_campaign[str(row["campaign_id"])].append(row)
    confirmed = {}
    for campaign, rows in by_campaign.items():
        run = int(rows[0]["run_ordinal"])
        if campaign not in fill_campaigns.get(run, set()):
            try:
                empty_net = Decimal(str(rows[0]["official_net"])) == 0
            except (InvalidOperation, TypeError):
                empty_net = False
            if (len(rows) == 1 and rows[0]["status"] == "SETTLED"
                    and run in empty_runs and empty_net):
                continue  # Official zero-result for a verified no-BUY market.
            anomalies.add("結算沒有對應 Live BUY fill")
            continue
        if len(rows) != 1:
            anomalies.add("結算資料重複")
            continue
        row = rows[0]
        claim = claim_by_run.get(run)
        if claim is None or claim["campaign_id"] != campaign:
            anomalies.add("結算缺少對應 C180 claim")
            continue
        if row["status"] != "SETTLED":
            if row["status"] not in ("PENDING", "CLOSED_PENDING_REDEEM"):
                anomalies.add("結算狀態待核對")
            continue
        try:
            official = Decimal(str(row["official_net"]))
            observed = Decimal(str(row["observed_net"]))
        except (InvalidOperation, TypeError):
            anomalies.add("結算金額無法核對")
            continue
        if (not official.is_finite() or not observed.is_finite() or official != observed
                or row["known_at_ms"] is None or int(row["known_at_ms"]) > now):
            anomalies.add("官方與風控結算觀測不一致")
            continue
        if run in confirmed:
            anomalies.add("同一 run 有重複結算")
            confirmed.pop(run, None)
            continue
        confirmed[run] = (observed, int(row["known_at_ms"]))
    pending_runs = filled_runs - set(confirmed)

    gate = json.loads(gate_row[0]) if gate_row else {}
    gate_matches = isinstance(gate, dict) and gate.get("loop_id") == loop_id
    policy_version = str(gate.get("policy_version") or "1.0") if gate_matches else "unknown"
    unit = None
    if gate_matches:
        try:
            unit = Decimal(str(gate["unit_usdt"]))
        except (KeyError, InvalidOperation, TypeError):
            pass
    if unit not in (Decimal("1"), Decimal("2"), Decimal("3")):
        unit = None
        anomalies.add("風控單位無法核對")
    else:
        try:
            if any(Decimal(str(row["unit_usdt"])) != unit for row in claims):
                anomalies.add("區段混合投入單位")
        except (InvalidOperation, TypeError):
            anomalies.add("claim 投入單位無法核對")
    latches = gate.get("latches", {}) if gate_matches else {}
    if not isinstance(latches, dict):
        latches = {}
        anomalies.add("風控鎖狀態無法核對")

    def metrics(start, end):
        events = sorted(
            ((known_at, run, pnl) for run, (pnl, known_at) in confirmed.items()
             if start <= run <= end), key=lambda item: (item[0], item[1])
        )
        equity = peak = max_dd = Decimal("0")
        for _known_at, group in groupby(events, key=lambda item: item[0]):
            equity += sum((item[2] for item in group), Decimal("0"))
            peak = max(peak, equity)
            max_dd = max(max_dd, peak - equity)
        wins = sum(pnl > 0 for run, (pnl, _) in confirmed.items() if start <= run <= end)
        losses = sum(pnl < 0 for run, (pnl, _) in confirmed.items() if start <= run <= end)
        flats = sum(pnl == 0 for run, (pnl, _) in confirmed.items() if start <= run <= end)
        wr = f"{wins / (wins + losses):.1%}" if wins + losses else "—"
        return wins, losses, flats, wr, equity, max_dd

    taipei = timezone(timedelta(hours=8))
    clock = datetime.fromtimestamp(now / 1000, taipei).strftime("%m/%d %H:%M:%S")
    wins, losses, flats, wr, pnl, total_mdd = metrics(1, target)
    submitted = sum(row["order_id"] is not None for row in claims)
    total_pending = len(pending_runs)
    total_pnl_text = ("—（待結算）" if not confirmed and total_pending else
                      f"{pnl:+.4f} USDT")
    total_mdd_text = ("—（待結算）" if not confirmed and total_pending else
                      f"{total_mdd:.4f} USDT")
    lines = [
        "📊 C180 Live Report｜官方結算觀測",
        f"截至 {clock}（台北）｜Loop {loop_id}",
        f"狀態 {loop['state']}｜完成 {completed}/{target} run｜每場 {unit if unit is not None else '—'} USDT｜策略 V{policy_version}",
        f"總計 WR {wr}（勝{wins}/負{losses}/平{flats}；已結算{len(confirmed)}）",
        f"已知費後 PnL {total_pnl_text}｜已知 MDD {total_mdd_text}",
        f"送單 {submitted}｜成交 {len(filled_runs)}｜待結算 {total_pending}｜未知訂單 {unknown}",
        "",
        "每 20 個排程 run（含未開單場）：",
    ]
    last_observed = max((int(row["run_ordinal"]) for row in slots
                         if row["verified_at_ms"] is not None), default=0)
    shown = min(target, max(1, completed, last_observed))
    for start in range(1, shown + 1, 20):
        end = min(target, start + 19)
        index = (start - 1) // 20 + 1
        finished = max(0, min(completed, end) - start + 1)
        bw, bl, bf, bwr, bpnl, bmdd = metrics(start, end)
        batch_fills = sum(start <= run <= end for run in filled_runs)
        batch_pending = sum(start <= run <= end for run in pending_runs)
        batch_unopened = max(0, finished - sum(start <= run <= min(completed, end)
                                                for run in claim_runs))
        threshold = (f"{V11_MDD_USDT:.2f}" if policy_version == "1.1" else
                     f"{BASE_MDD_USDT * unit:.2f}" if unit is not None and policy_version == "1.0" else "—")
        halt = "｜已觸發停新 BUY" if str(index) in latches else ""
        progress = "已完成" if finished == end - start + 1 else "進行中"
        active = (f"｜第{last_observed}場執行中" if start <= last_observed <= end
                  and last_observed > completed else "")
        settled = bw + bl + bf
        bpnl_text = ("—（待結算）" if batch_pending and not settled else
                     f"{bpnl:+.4f} USDT")
        bmdd_text = ("—（待結算）" if batch_pending and not settled else
                     f"{bmdd:.4f}")
        lines.extend([
            f"{start}–{end}｜{progress} {finished}/{end - start + 1}{active}{halt}",
            f"  WR {bwr}（{bw}勝/{bl}負/{bf}平）｜PnL {bpnl_text}",
            f"  MDD {bmdd_text} / 風控線 {threshold} USDT｜成交 {batch_fills}・待結 {batch_pending}・未開單 {batch_unopened}",
        ])
    if loop["new_entries_stopped"] or loop["hard_stop_latched"]:
        lines.append("⚠️ Live 新進場已停或 Hard Stop 已鎖定。")
    if policy_version == "1.1":
        lines.append(f"V1.1 買價上限 0.90｜整輪 PnL {pnl:+.4f} / {V11_LOOP_LOSS_USDT:.2f} USDT"
                     + ("｜整輪虧損鎖已觸發" if gate.get("loop_loss_latched") else ""))
        recovery = gate.get("recovery") or {}
        phase = str(recovery.get("state") or ("待啟動" if gate.get("recovery_hold_latched") else "Live"))
        lines.append(f"恢復狀態 {phase}｜已自動恢復 {int(recovery.get('recoveries') or 0)}/2 次")
        if phase == "SHADOW" and recovery.get("start_run"):
            from src.gridbot.prediction.c180_gate_runtime import recovery_metrics, early_recovery_metrics
            from src.gridbot.prediction.c180_signal_runtime import read_c180_recovery_outcomes
            first = int(recovery["start_run"])
            last = first + 9
            anchor = int(gate["first_market_start_ms"])
            signal_db = Path(root) / "prediction/data/c180-favorite-live/signals.sqlite3"
            starts = [anchor + (r - 1) * 300_000 for r in range(first, last + 1)]
            try:
                found = read_c180_recovery_outcomes(signal_db, starts)
                paper = [found[s] for s in starts if s in found]
                m = recovery_metrics(paper)
                expected_early = max(0, min(completed - first + 1, 10))
                early_paper = [found[s] for s in starts[:expected_early] if s in found]
                early = early_recovery_metrics(early_paper, expected_count=expected_early)
                wr_paper = (f"{m['wins'] / (m['wins'] + m['losses']):.1%}"
                            if m["wins"] + m["losses"] else "—")
                lines.append(f"Shadow Run {first}–{last}｜紀錄 {m['observed']}/10｜已結模擬成交 {m['settled']}/5")
                lines.append(f"  WR {wr_paper}/60%（{m['wins']}勝/{m['losses']}負）｜費後 PnL {Decimal(m['pnl_usdt']):+.4f}/+1.00 U")
                full_result = "通過" if m["qualified"] else ("未通過" if m["complete"] else "待驗證")
                lines.append(f"  MDD {Decimal(m['mdd_usdt']):.4f}/1.00 U｜完整 {'是' if m['complete'] else '否'}｜{full_result}")
                early_result = ("窗口已結束" if completed >= last else
                                "達標，待下一場風控評估" if early["qualified"] else
                                "未達標或待結")
                lines.append(
                    f"  快速恢復（前5場起）：費後累計 PnL {Decimal(early['pnl_usdt']):+.4f} U（需 >0）"
                    f"｜完整 {'是' if early['complete'] else '否'}｜{early_result}"
                )
                unknown = sum(x.get("status") in ("UNKNOWN", "FILLED_PENDING") for x in paper)
                if unknown:
                    lines.append(f"  待補資料/官方結算 {unknown} 場；資料不全不切回 Live")
            except (OSError, sqlite3.Error, ValueError, KeyError, TypeError, ArithmeticError):
                lines.append(f"Shadow Run {first}–{last}｜資料帳暫時無法讀取，維持停新 BUY")
        elif phase == "QUALIFIED":
            lines.append(f"Shadow 已達標｜最早 Run {recovery.get('resume_at_run')} 經官方零曝險核對後恢復")
        elif phase == "PROBATION":
            trial = recovery.get("probation") or {}
            lines.append(f"Live 試行｜已結 {trial.get('settled', 0)}/5｜PnL {trial.get('pnl_usdt', '0')} U｜MDD {trial.get('mdd_usdt', '0')}/1.50 U")
        elif gate.get("recovery_hold_latched"):
            lines.append("Shadow 收集尚未開始；需從下一個完整市場建立觀測窗口。")
        lines.append("整輪虧損鎖不自動解除。")
    if anomalies:
        lines.append("⚠️ 資料待核對：" + "、".join(sorted(anomalies)) + "；PnL/WR/MDD 僅含可核對結算。")
    lines.append("每段 MDD 從 0 重算；" + ("達風控線" if policy_version == "1.1" else "嚴格超過風控線") + "停該段新 BUY。")
    lines.append("WR=勝/(勝+負)，平手不計；PnL/MDD 依已知官方費後結算，未結算不補零，亦不代表已領現金。")
    return "\n".join(lines)


def _format_aligned_shadow_supplement(root, *, now_ms=None):
    """Read-only fourth-lane snapshot; no broker, credentials, or send API."""
    import json
    import sqlite3
    import time
    from pathlib import Path
    from datetime import datetime, timezone, timedelta
    from decimal import Decimal
    from contextlib import closing
    folder = Path(root) / 'prediction/experiments/confirm3-aligned-v1'
    config_path = folder / 'config.json'
    if not config_path.is_file():
        return None
    config = json.loads(config_path.read_text())
    if config.get('lane') != 'reversion_3s_strike_aligned_v1':
        raise ValueError('Unexpected supplemental lane')
    db = folder / 'shadow.sqlite3'
    with closing(sqlite3.connect(db.resolve().as_uri() + '?mode=ro', uri=True, timeout=2)) as connection:
        connection.execute('PRAGMA query_only=ON')
        connection.execute('BEGIN')
        rows = connection.execute('SELECT decision,pnl,settled_ms FROM markets').fetchall()
        raw = connection.execute("SELECT value FROM meta WHERE key='heartbeat'").fetchone()
        heartbeat = json.loads(raw[0]) if raw else {}
    settled = [row for row in rows if row[2] is not None]
    fills = [row for row in settled if row[0] == 'filled']
    wins = sum(Decimal(row[1]) > 0 for row in fills)
    losses = sum(Decimal(row[1]) < 0 for row in fills)
    flat = len(fills) - wins - losses
    win_rate = f'{wins / len(fills):.1%}' if fills else '—（尚無已結算成交）'
    net = sum((Decimal(row[1]) for row in fills), Decimal('0'))
    pending = sum(row[0] == 'filled' and row[2] is None for row in rows)
    censored = sum(row[0].startswith('censored') or row[0] in
        ('invalid_quote','quote_mismatch','stale_quote','invalid_price','invalid_side') for row in rows)
    now = int(time.time()*1000) if now_ms is None else now_ms
    taipei = timezone(timedelta(hours=8))
    clock = lambda ms: datetime.fromtimestamp(ms/1000, taipei).strftime('%m/%d %H:%M:%S')
    lines = [
        '<b>第四條補充｜三秒版＋有利側過濾</b>',
        f'截至 {clock(now)}（台灣時間）',
        f'開始 {clock(int(config["start_ms"]))}；本輪獨立目標 {int(config["target"])} 場',
        f'已觀察 {len(rows)}/{int(config["target"])} 場；已結算 {len(settled)} 場',
        f'模擬成交 {len(fills)+pending} 筆；已結算成交 {len(fills)} 筆；待結算 {pending} 筆',
        f'W / L：{wins} / {losses}；損益平手 {flat}；WR：{win_rate}',
        f'假設成本後 PnL：{net:+.4f} USDT',
        f'有利側不符放棄 {sum(row[0] == "veto_strike" for row in rows)} 場；資料／延遲排除 {censored} 場',
        '沿用原三秒候選：UP 要求 BTC 高於本場起始價；DOWN 要求低於，否則不下。',
        '每筆模擬 2 USDT，額外 3% 成本假設；非實盤。',
        '只計加入後的新市場；不與原三條合計勝率或 PnL。',
    ]
    if not heartbeat.get('complete') and now-int(heartbeat.get('now_ms',0)) > 15000:
        lines.append('⚠️ 第四條心跳逾時；以上是最後落盤結果，非即時狀態。')
    if heartbeat.get('complete'):
        lines.append('本輪第四條已收滿並完成結算。')
    return '\n'.join(lines)


def _format_guard_shadow_supplements(root, *, now_ms=None):
    """Two isolated read-only replies. Never loads credentials or sends messages."""
    import json
    import sqlite3
    import time
    from pathlib import Path
    from decimal import Decimal
    from contextlib import closing
    from datetime import datetime, timezone, timedelta
    now = int(time.time()*1000) if now_ms is None else now_ms
    folder = Path(root) / 'prediction/experiments/confirm3-guards-v1'
    labels = [
        (5, 'reversion_3s_reconfirm_guard_v1', '第五條｜三秒成交再確認', '原反轉訊號仍成立，且最近三秒價格確實朝下單方向移動。'),
        (6, 'reversion_3s_value_margin_v1', '第六條｜價格＋有利距離', '買價 ≤ 0.50；有利距離 ≥ max(1 bp, 最近十秒價格區間)。'),
    ]
    messages = []
    for number, lane, title, rule in labels:
        try:
            cp = folder / f'lane{number}.json'
            if not cp.is_file():
                messages.append(f'<b>{title}</b>\n尚未設定，無可用報告。')
                continue
            cfg = json.loads(cp.read_text())
            if cfg.get('lane') != lane:
                raise ValueError('Lane identity mismatch')
            db = folder / f'lane{number}.sqlite3'
            with closing(sqlite3.connect(db.resolve().as_uri()+'?mode=ro', uri=True, timeout=2)) as c:
                c.execute('PRAGMA query_only=ON'); c.execute('BEGIN')
                rows = c.execute('SELECT decision,pnl,settled_ms FROM markets').fetchall()
                raw = c.execute("SELECT value FROM meta WHERE key='heartbeat'").fetchone()
                heartbeat = json.loads(raw[0]) if raw else {}
            settled = [r for r in rows if r[2] is not None]
            fills = [r for r in settled if r[0] == 'filled']
            wins = sum(Decimal(r[1]) > 0 for r in fills)
            losses = sum(Decimal(r[1]) < 0 for r in fills)
            net = sum((Decimal(r[1]) for r in fills), Decimal('0'))
            pending = sum(r[0] == 'filled' and r[2] is None for r in rows)
            vetoed = sum(r[0].startswith('veto_') for r in rows)
            censored = sum(r[0].startswith('censored') or r[0] in ('invalid_quote','quote_mismatch','stale_quote','invalid_price','invalid_side','invalid_rule') for r in rows)
            wr = f'{wins/len(fills):.1%}' if fills else '—（尚無已結算成交）'
            clock = lambda ms: datetime.fromtimestamp(ms/1000, timezone(timedelta(hours=8))).strftime('%m/%d %H:%M:%S')
            lines = [f'<b>{title}</b>', f'截至 {clock(now)}（台灣時間）',
                f'開始 {clock(int(cfg["start_ms"]))}；獨立目標 {int(cfg["target"])} 場',
                f'已觀察 {len(rows)}/{int(cfg["target"])} 場；已結算 {len(settled)} 場',
                f'模擬成交 {len(fills)+pending} 筆；已結算成交 {len(fills)} 筆；待結算 {pending} 筆',
                f'W / L：{wins} / {losses}；損益平手 {len(fills)-wins-losses}；WR：{wr}',
                f'假設成本後 PnL：{net:+.4f} USDT',
                f'策略放棄 {vetoed} 場；資料／延遲排除 {censored} 場；原三秒未成交 {sum(r[0]=="no_parent_fill" for r in rows)} 場', rule,
                '每筆模擬 2 USDT，額外 3% 成本假設；非實盤，門檻尚未驗證。',
                '只計加入後的新市場；各 lane 獨立統計，不合計。']
            names = {'veto_reconfirm':'訊號不符','veto_no_directional_move':'三秒無順向移動','veto_price':'買價過高','veto_strike':'起始價方向不符','veto_distance':'有利距離不足'}
            reasons = [f'{label} {sum(r[0]==key for r in rows)}' for key,label in names.items() if any(r[0]==key for r in rows)]
            if reasons: lines.append('放棄原因：'+'；'.join(reasons))
            if heartbeat.get('complete'): lines.append('本 lane 已收滿並完成結算。')
            elif now-int(heartbeat.get('now_ms',0)) > 15000 or now < int(heartbeat.get('now_ms',0)):
                lines.append('⚠️ 心跳逾時或異常；以上是最後落盤結果，非即時狀態。')
            elif now < int(cfg['start_ms']): lines.append('服務已啟動，等待預定市場開始。')
            messages.append('\n'.join(lines))
        except Exception:
            messages.append(f'<b>{title}</b>\n⚠️ 暫時無法讀取；其他 lane 報告不受影響。')
    return messages


def _format_first_observer_report(root, window=20, *, now_ms=None):
    """Bounded atomic JSON snapshot from independent observer, no DB writes."""
    import json
    from pathlib import Path
    from datetime import datetime
    from decimal import Decimal
    from zoneinfo import ZoneInfo
    if window not in (20, 40, 100):
        raise ValueError("first_report_window")
    path = Path(root) / "prediction/data/first-multimarket-v1/latest.json"
    with path.open("rb") as stream:
        raw = stream.read(2 * 1024 * 1024 + 1)
    if len(raw) > 2 * 1024 * 1024:
        raise ValueError("first_report_size")
    p = json.loads(raw)
    expected = "5c521aaf03e2f4ec23914a96fe8f643aaa20747be7b85f3147be02b04a234a7f"
    if p.get("mode") != "QUOTE_SIMULATION_NO_REAL_ORDERS" or p.get("policy") != expected:
        raise ValueError("first_report_provenance")
    at = int(p["at_ms"])
    current = _now_ms() if now_ms is None else now_ms
    if at > current + 1000:
        raise ValueError("first_report_future")
    stamp = datetime.fromtimestamp(at/1000, ZoneInfo("Asia/Taipei")).strftime("%m/%d %H:%M:%S")
    lines = [f"First 三市場觀測｜最近{window}場", f"更新：{stamp}（台灣）", "1U報價模擬，非真實成交；未套Live風控。", ""]
    span = p.get("rolling_ranges", {}).get(str(window), {})
    if span.get("start") is not None and span.get("end") is not None:
        fmt = lambda ms: datetime.fromtimestamp(ms/1000, ZoneInfo("Asia/Taipei")).strftime("%m/%d %H:%M")
        lines.insert(2, f"統計區間：{fmt(span['start'])}–{fmt(span['end'])}（已結束市場）")
    health = p.get("health") or {}
    if current-at > 120000 or current-int(health.get("at_ms", 0)) > 120000:
        lines.extend(["⚠ 觀測資料已過期，以下為舊快照，不能視為目前市況。", ""])
    group = p["rolling"][str(window)]
    for symbol in ("BTCUSDT", "ETHUSDT", "BNBUSDT"):
        m = group[symbol]["ALL"]
        lines.append(f"【{symbol[:-4]}】實際{m['scheduled_windows']}/{window}場｜K線{m['feature_complete']}｜雙向盤口{m['initial_books_complete']}")
        lines.append(f"訊號{m['signal']} → 趨勢{m['trend_pass']} → 初始{m['initial_quote_eligible']} → 重檢通過{m['quote_candidates']}")
        rate = "—" if m["quote_candidate_rate"] is None else f"{m['quote_candidate_rate']*100:.1f}%"
        lines.append(f"報價候選率 {rate}（非fill率）")
        if "recheck_attempted" in m:
            lines.append(f"重檢：執行{m['recheck_attempted']}｜通過{m['quote_candidates']}")
        labels = {'price_above_frozen_cap':'高於凍結限價', 'insufficient_frozen_share_depth':'限價內深度不足',
                  'recheck_not_attempted':'重檢未執行', 'recheck_data_unavailable':'重檢資料缺漏',
                  'recheck_window':'超過重檢期限', 'quote_age':'報價過期', 'price_band':'價格帶不符',
                  'book_stale':'盤口過期','book_future':'盤口時間超前','book_stale_or_future':'盤口時間不符',
                  'book_token_side':'盤口方向不符','book_market':'盤口市場不符',
                  'ask_invalid':'盤口價格或數量無效','ask_sort_or_empty':'盤口空白或排序無效','crossed_book':'盤口交叉'}
        reasons = m.get('recheck_reasons', {})
        if reasons:
            lines.append('未通過：'+'、'.join(f"{labels.get(k,'其他資料/條件')} {n}" for k,n in sorted(reasons.items())))
        if not m['quote_candidates']:
            lines.append('無重檢通過樣本；WR/PnL的「—」不是0收益。')
        for side, label in (("ALL", "合計"), ("UP", "First UP"), ("DOWN", "First DOWN")):
            row = group[symbol][side]
            wr = "—" if row["wr"] is None else f"{row['wr']*100:.1f}%"
            net = "—" if not row["settled"] else f"{Decimal(row['net_pnl']):+.4f}U"
            mdd = "—" if not row["settled"] else f"{Decimal(row['mdd']):.4f}U"
            suffix = "（待結，尚未完整）" if row["pending"] else ""
            lines.append(f"{label}：{row['wins']}勝{row['losses']}負{row['draws']}平｜待結{row['pending']}")
            lines.append(f"WR {wr}｜PnL {net}｜MDD {mdd}{suffix}")
        if m["missing_features"]:
            lines.append(f"⚠ 缺K線{m['missing_features']}場，保留分母；屬資料缺漏，不算條件不符。")
        lines.append("")
    lines.extend(["官方勝方結算；WR排除平局、PnL包含平局。", "ETH/BNB沿用BTC First條件，尚未驗證Live適用性。", "不自動選幣或開單。"])
    text = "\n".join(lines)
    if len(text) > 3900:
        raise ValueError("first_report_length")
    return text

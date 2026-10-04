"""Entry point for the isolated Binance Web3 Prediction canary.

This entry point wires the independent Prediction worker, repository, REST
client, and Telegram controller. Missing live capabilities force shadow mode,
while the background collector remains active for evidence collection.

The legacy Futures application is not imported here.  The only credentials
read by this module are the dedicated ``PREDICTION_BINANCE_API_KEY`` and
``PREDICTION_BINANCE_API_SECRET`` values after ``.env`` has been loaded.
Generic/legacy ``BINANCE_*`` values are deliberately ignored, so a testnet or
legacy key can never authorize this service.  Credentials are never logged or
copied into status payloads.
"""

from __future__ import annotations

import argparse
import asyncio
from dataclasses import dataclass
import logging
import os
from pathlib import Path
import sys
from typing import Any, Mapping, Sequence

from dotenv import load_dotenv

from src.gridbot.prediction.client import BinancePredictionClient, DEFAULT_BASE_URL
from telegram import BotCommand, BotCommandScopeAllPrivateChats
from telegram.error import Conflict

from src.gridbot.prediction.controller import PredictionController
from src.gridbot.prediction.repository import PredictionRepository
from src.gridbot.prediction.settings import PredictionSettings, RuntimeMode
from src.gridbot.prediction.telegram import (
    PredictionTelegramService,
    build_prediction_handlers,
)
from src.gridbot.prediction.worker import PredictionWorker
from src.gridbot.prediction.spot import BinanceSpotProvider


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_PREDICTION_DB = PROJECT_ROOT / "prediction" / "data" / "prediction.sqlite3"
DEFAULT_PREDICTION_LOG = PROJECT_ROOT / "prediction" / "logs" / "prediction.log"
LEGACY_DB_NAMES = frozenset({"gridbot.db", "gridbot_testnet.db", "gridbot_testnet_v2.db"})

LOGGER = logging.getLogger("cry3.prediction")


class PredictionConfigurationError(RuntimeError):
    """Raised when the isolated service cannot start safely."""


class PredictionWorkerUnavailable(RuntimeError):
    """The reviewed market worker has not been supplied to the entry point."""


def load_prediction_environment(dotenv_path: str | os.PathLike[str] | None = None) -> None:
    """Load ``.env`` without overriding an already-exported VM secret."""

    # ``override=False`` is important on a VM where systemd may provide the
    # rotated credential while the checked-out .env still contains an older
    # value.  The path is injectable for offline contract tests.
    load_dotenv(dotenv_path=dotenv_path, override=False)


def _env(environ: Mapping[str, str] | None = None) -> Mapping[str, str]:
    return os.environ if environ is None else environ


def prediction_auto_start_loop(environ: Mapping[str, str] | None = None) -> bool:
    """Keep loop startup manual unless the operator explicitly opts in."""

    value = str(_env(environ).get("PREDICTION_AUTO_START_LOOP") or "").strip().lower()
    return value in {"1", "true", "yes", "on"}


def prediction_live_arm_on_start(environ: Mapping[str, str] | None = None) -> bool:
    """Allow a one-time, explicit operator Live arm during service startup."""

    value = str(_env(environ).get("PREDICTION_LIVE_ARM_ON_START") or "").strip().lower()
    return value in {"1", "true", "yes", "on"}


def read_binance_credentials(environ: Mapping[str, str] | None = None) -> tuple[str, str]:
    """Return only the dedicated Prediction credentials, without logging them.

    This is an explicit boundary: the isolated canary never falls back to the
    legacy ``BINANCE_API_KEY``/``BINANCE_API_SECRET`` names.  Callers receive
    only a missing-name error and secret values are never echoed.
    """

    values = _env(environ)
    api_key = str(values.get("PREDICTION_BINANCE_API_KEY", "") or "").strip()
    api_secret = str(values.get("PREDICTION_BINANCE_API_SECRET", "") or "").strip()
    missing = [
        name
        for name, value in (
            ("PREDICTION_BINANCE_API_KEY", api_key),
            ("PREDICTION_BINANCE_API_SECRET", api_secret),
        )
        if not value
    ]
    if missing:
        raise PredictionConfigurationError(
            "missing required Binance credential(s): " + ", ".join(missing)
        )
    return api_key, api_secret


def resolve_prediction_db_path(
    environ: Mapping[str, str] | None = None,
    *,
    project_root: Path = PROJECT_ROOT,
    explicit: str | os.PathLike[str] | None = None,
) -> Path:
    """Resolve and validate the independent Prediction SQLite location."""

    values = _env(environ)
    raw = explicit if explicit is not None else values.get("PREDICTION_DB_PATH")
    selected = Path(raw) if raw else DEFAULT_PREDICTION_DB
    if not selected.is_absolute():
        selected = project_root / selected
    selected = selected.resolve()
    legacy_db = (project_root / "data" / "gridbot.db").resolve()
    legacy_testnet = (project_root / "testnet" / "data" / "gridbot_testnet.db").resolve()
    if selected in {legacy_db, legacy_testnet} or selected.name in LEGACY_DB_NAMES:
        raise PredictionConfigurationError(
            f"Prediction DB must be isolated from the legacy database: {selected}"
        )
    if selected == project_root.resolve() or selected.parent == Path(selected.anchor):
        raise PredictionConfigurationError("Prediction DB path is too broad")
    return selected


def _legacy_telegram_opted_in(environ: Mapping[str, str]) -> bool:
    return str(environ.get("PREDICTION_TELEGRAM_ALLOW_LEGACY") or "").strip().lower() in {"1", "true", "yes", "on"}

def prediction_chat_ids(environ: Mapping[str, str] | None = None) -> tuple[str, ...]:
    """Read only the allow-list; an empty list leaves Telegram fail-closed."""

    values = _env(environ)
    # Generic legacy IDs are accepted only with an explicit operator opt-in.
    # Without it, the Prediction control plane remains isolated.
    raw = values.get("PREDICTION_TELEGRAM_CHAT_IDS") or ""
    if not raw and _legacy_telegram_opted_in(values):
        raw = values.get("TELEGRAM_CHAT_ID") or ""
    return tuple(item.strip() for item in str(raw).split(",") if item.strip())


@dataclass(frozen=True)
class TelegramControlPlane:
    """Telegram construction result with an explicit disabled state."""

    service: PredictionTelegramService
    application: Any | None
    enabled: bool
    reason: str | None = None


def build_prediction_worker(
    settings: PredictionSettings,
    repository: PredictionRepository,
    client: BinancePredictionClient,
) -> PredictionController:
    """Create the real market worker behind a stable async controller."""

    # The worker always starts in shadow when live prerequisites are absent;
    # this is a controlled degradation, not an inert Telegram-only facade.
    spot = getattr(settings, "spot_source", None) or BinanceSpotProvider(symbol=settings.market_symbol)
    return PredictionController(PredictionWorker(settings, repository, client, spot_source=spot))


def build_prediction_telegram(
    runtime: Any,
    environ: Mapping[str, str] | None = None,
) -> TelegramControlPlane:
    """Build the Telegram control lane without starting network polling."""

    values = _env(environ)
    # Dedicated credentials remain preferred; legacy credentials require the
    # same explicit opt-in used by prediction_chat_ids().
    legacy_opted_in = _legacy_telegram_opted_in(values)
    token = str(values.get("PREDICTION_TELEGRAM_BOT_TOKEN") or "").strip()
    if not token and legacy_opted_in:
        token = str(values.get("TELEGRAM_BOT_TOKEN") or "").strip()
    chat_ids = prediction_chat_ids(values)
    service = PredictionTelegramService(runtime, chat_ids)
    if not token:
        return TelegramControlPlane(service, None, False, "Telegram bot token is not configured")
    if not chat_ids:
        return TelegramControlPlane(service, None, False, "Telegram allow-list is not configured")
    try:
        from telegram.ext import Application

        application = Application.builder().token(token).build()
        # Register directly so this helper remains the one place where the
        # application is assembled.  No polling or Telegram request happens
        # during construction.
        for handler in build_prediction_handlers(service):
            application.add_handler(handler)
        application.add_error_handler(service.handle_error)
        application.bot_data["prediction_telegram"] = service
    except Exception as exc:  # noqa: BLE001 - control plane must fail closed
        return TelegramControlPlane(service, None, False, f"Telegram initialization failed: {exc}")
    return TelegramControlPlane(service, application, True)


@dataclass
class PredictionComponents:
    settings: PredictionSettings
    repository: PredictionRepository
    client: BinancePredictionClient
    runtime: Any
    telegram: TelegramControlPlane


async def build_prediction_components(
    environ: Mapping[str, str] | None = None,
    *,
    dotenv_path: str | os.PathLike[str] | None = None,
    db_path: str | os.PathLike[str] | None = None,
) -> PredictionComponents:
    """Initialize the isolated repository/client/runtime/Telegram graph.

    Construction is side-effect-light; the background collector is started by
    ``async_main`` after the isolated graph is initialized.
    """

    if environ is None:
        load_prediction_environment(dotenv_path)
        values: Mapping[str, str] = os.environ
    else:
        # Tests and embedding callers supply an already-resolved environment;
        # avoid mutating process globals or reading a developer's .env file.
        values = environ
    settings = PredictionSettings.from_env(values)
    api_key, api_secret = read_binance_credentials(values)
    selected_db = resolve_prediction_db_path(values, explicit=db_path)
    repository = PredictionRepository(selected_db)
    try:
        await repository.initialize()
        # Persist only a secret-free config identity for promotion/deploy
        # provenance.  It is never used as live authority by itself.
        await repository.set_runtime_config("prediction_config_hash", settings.config_hash)
        client = BinancePredictionClient(
            api_key,
            api_secret,
            base_url=str(values.get("PREDICTION_BASE_URL") or DEFAULT_BASE_URL),
            recv_window=settings.recv_window,
            order_unit_usdt=settings.order_unit_usdt,
        )
        runtime = build_prediction_worker(settings, repository, client)
        telegram = build_prediction_telegram(runtime, values)
        return PredictionComponents(settings, repository, client, runtime, telegram)
    except Exception:
        await repository.close()
        raise


async def close_prediction_components(components: PredictionComponents) -> None:
    """Close only the independent Prediction repository."""

    close = getattr(components.runtime, "close", None)
    if close is not None:
        result = close()
        if asyncio.iscoroutine(result):
            await result
    await components.repository.close()


def prediction_bot_commands() -> tuple[BotCommand, ...]:
    """Return the command menu exposed by the Prediction control plane."""

    return (
        BotCommand("t67creport", "T6.7c七路三幣Shadow，參數20/40/100"),
        BotCommand("firstreport", "BTC／ETH／BNB First觀測，參數20/40/100"),
        BotCommand("report", "目前 Lane Live WR／PnL／風控"),
        BotCommand("predict_report", "目前 Lane Live WR／PnL／風控"),
        BotCommand("shadow_report", "查看原 Shadow 報告"),
        BotCommand("predict_status", "查看目前狀態與錢包"),
        BotCommand("predict_monitor", "市況警示與 Lane READY"),
        BotCommand("predict_loop_5", "執行 5 個市場"),
        BotCommand("predict_loop_10", "執行 10 個市場"),
        BotCommand("predict_loop_20", "執行 20 個市場"),
        BotCommand("predict_loop_100", "執行 100 個市場"),
        BotCommand("predict_one_run", "只執行 1 個市場"),
        BotCommand("predict_loop_pnl", "查看全部 Loop PnL"),
        BotCommand("predict_stop", "停止目前 Loop"),
        BotCommand("predict_cancel", "取消整個 Loop"),
        BotCommand("predict_resume", "安全同步後恢復 Loop"),
        BotCommand("predict_reconcile", "同步未完成訂單"),
        BotCommand("predict_risk", "查看真正風控狀態"),
        BotCommand("predict_hard_stop_reset", "Hard Stop Reset（不限次數）"),
        BotCommand("predict_market", "T6.7c 下一輪選 BTC／ETH／BNB"),
        BotCommand("predict_lane", "選擇交易策略"),
        BotCommand("predict_amount", "選擇單筆 1 / 2 USDT"),
    )


async def _configure_prediction_commands(application: Any) -> None:
    """Synchronize the Telegram command menu before polling begins."""

    commands = list(prediction_bot_commands())
    setter = getattr(application.bot, "set_my_commands", None)
    if setter is None:
        raise PredictionConfigurationError("Telegram bot command menu is unavailable")
    await setter(commands)
    await setter(commands, scope=BotCommandScopeAllPrivateChats())
    LOGGER.info("prediction_telegram_command_menu_configured command_count=%d", len(commands))


async def _poll_prediction_monitor_alerts(
    application: Any,
    *,
    interval_seconds: int = 60,
) -> None:
    """Poll advisory monitor state and notify only confirmed transitions."""

    service = application.bot_data.get("prediction_telegram")
    if not isinstance(service, PredictionTelegramService):
        LOGGER.warning("prediction_monitor_alerts_disabled service_missing=true")
        return
    while True:
        try:
            await service.check_monitor_alerts(application.bot)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - alerts must never stop trading or polling
            LOGGER.warning(
                "prediction_monitor_alert_check_failed error_type=%s",
                type(exc).__name__,
            )
        await asyncio.sleep(max(15, int(interval_seconds)))


async def _poll_telegram(application: Any) -> None:
    """Run PTB without ``run_polling`` so shutdown remains awaitable."""

    await application.initialize()
    await _configure_prediction_commands(application)
    await application.start()
    if application.updater is None:
        raise PredictionConfigurationError("Telegram updater is unavailable")
    # Long polling cannot receive callbacks while an old webhook owns this
    # bot. Keep queued operator commands when handing control to polling.
    await application.bot.delete_webhook(drop_pending_updates=False)
    webhook_repair_task: asyncio.Task[Any] | None = None
    last_webhook_repair_at = 0.0

    async def repair_webhook() -> None:
        try:
            await application.bot.delete_webhook(drop_pending_updates=False)
            LOGGER.warning("prediction_telegram_webhook_cleared_during_polling")
        except Exception as exc:  # noqa: BLE001 - polling keeps retrying
            LOGGER.error("prediction_telegram_webhook_clear_failed error_type=%s", type(exc).__name__)

    def polling_error(error: Exception) -> None:
        nonlocal webhook_repair_task, last_webhook_repair_at
        if isinstance(error, Conflict) and "webhook is active" in str(error):
            now = asyncio.get_running_loop().time()
            if (webhook_repair_task is None or webhook_repair_task.done()) and now - last_webhook_repair_at >= 10:
                last_webhook_repair_at = now
                webhook_repair_task = asyncio.create_task(repair_webhook())
            return
        LOGGER.warning("prediction_telegram_poll_error error_type=%s", type(error).__name__)

    # Telegram keeps the previous allowed_updates setting when omitted. A
    # former text-only consumer can therefore make inline controls inert even
    # while /predict_status still works.
    await application.updater.start_polling(
        error_callback=polling_error,
        allowed_updates=("message", "callback_query"),
    )
    monitor_task = asyncio.create_task(
        _poll_prediction_monitor_alerts(application),
        name="prediction-monitor-alerts",
    )
    try:
        await asyncio.Event().wait()
    finally:
        if webhook_repair_task is not None and not webhook_repair_task.done():
            webhook_repair_task.cancel()
            try:
                await webhook_repair_task
            except asyncio.CancelledError:
                pass
        monitor_task.cancel()
        try:
            await monitor_task
        except asyncio.CancelledError:
            pass
        await application.updater.stop()
        await application.stop()
        await application.shutdown()


async def async_main(
    argv: Sequence[str] | None = None,
    *,
    environ: Mapping[str, str] | None = None,
) -> int:
    parser = argparse.ArgumentParser(description="Cry3 isolated Binance Prediction canary")
    parser.add_argument("--check", action="store_true", help="initialize components and exit without polling")
    parser.add_argument("--poll-telegram", action="store_true", help="start Telegram polling after initialization")
    parser.add_argument("--dotenv", dest="dotenv_path", help="optional dotenv path")
    args = parser.parse_args(list(argv) if argv is not None else None)

    try:
        components = await build_prediction_components(environ, dotenv_path=args.dotenv_path)
    except (PredictionConfigurationError, ValueError) as exc:
        LOGGER.error("prediction_start_denied: %s", exc)
        return 2

    LOGGER.info(
        "prediction_initialized mode=%s db=%s worker_available=%s orders_enabled=%s telegram_enabled=%s",
        components.settings.mode.value,
        components.repository.db_path,
        components.runtime.worker_available,
        components.runtime.orders_enabled,
        components.telegram.enabled,
    )
    if components.telegram.reason:
        LOGGER.warning("prediction_telegram_disabled: %s", components.telegram.reason)
    if components.settings.mode is RuntimeMode.LIVE:
        LOGGER.warning("prediction_live_requested_controller_preflight_required")

    startup_loop = await components.repository.get_active_loop()
    arm_live = prediction_live_arm_on_start(environ)
    if arm_live and startup_loop:
        persisted_mode = str(startup_loop.get("mode") or "SHADOW").strip().upper()
        legacy_live = False
        detector = getattr(components.repository, "loop_has_live_execution", None)
        if callable(detector):
            legacy_live = bool(await detector(str(startup_loop.get("loop_id") or "")))
        # Migrations 001-005 defaulted every loop to SHADOW, including LIVE
        # loops.  An official order intent is the conservative proof needed
        # to recover those legacy rows as LIVE.  A real persisted SHADOW loop
        # must never be auto-promoted merely because the unit defaults LIVE.
        if persisted_mode == "SHADOW" and not legacy_live:
            arm_live = False
            LOGGER.warning(
                "prediction_live_arm_skipped_for_shadow_recovery loop_id=%s",
                startup_loop.get("loop_id"),
            )

    if arm_live:
        try:
            live_result = await components.runtime.set_shadow_mode(False)
        except Exception as exc:  # noqa: BLE001 - startup must fail closed
            LOGGER.error("prediction_live_arm_on_start_denied error_type=%s", type(exc).__name__)
            await close_prediction_components(components)
            return 2
        effective_mode = str(live_result.get("effective_mode", live_result.get("mode", ""))).upper() if isinstance(live_result, Mapping) else ""
        if effective_mode != RuntimeMode.LIVE.value.upper() or not bool(live_result.get("live_armed")):
            reason = live_result.get("reason") if isinstance(live_result, Mapping) else "invalid live result"
            LOGGER.error("prediction_live_arm_on_start_denied reason=%s", reason or "live capability not armed")
            await close_prediction_components(components)
            return 2
        LOGGER.warning("prediction_live_arm_on_start_completed")

    if args.check:
        await close_prediction_components(components)
        return 0

    try:
        # Loop admission is manual by default. Telegram exposes only the
        # reviewed Loop 5/10/20 and One Run entry points.
        if prediction_auto_start_loop(environ):
            await components.runtime.start_loop(components.settings.loop_limit)
        else:
            # Manual start remains the default, but a loop that was already
            # RUNNING before a clean service restart must resume from its
            # durable SQLite cursor. This does not create a new loop and
            # preserves the operator's target/completed progress.
            existing_loop = await components.repository.get_active_loop()
            if existing_loop:
                worker = getattr(components.runtime, "worker", None)
                recover = getattr(worker, "recover_active_loop", None)
                if callable(recover):
                    recovery = await recover()
                    LOGGER.warning(
                        "prediction_loop_recovery_completed loop_id=%s clean=%s",
                        existing_loop.get("loop_id"),
                        not bool(recovery.get("action_denied")) if isinstance(recovery, Mapping) else False,
                    )
                remaining_loop = await components.repository.get_active_loop()
                if remaining_loop and not bool(remaining_loop.get("new_entries_stopped")):
                    target = int(remaining_loop.get("target") or components.settings.loop_limit)
                    result = await components.runtime.start_loop(target)
                    LOGGER.info(
                        "prediction_loop_resumed loop_id=%s target=%d admitted=%s",
                        remaining_loop.get("loop_id"),
                        target,
                        not bool(result.get("action_denied")) if isinstance(result, Mapping) else False,
                    )
            else:
                LOGGER.info("prediction_loop_autostart_disabled")
        # Shadow collection is independent from the finite Live loop.  It
        # remains active while the operator is idle and yields only when a
        # Live campaign owns the current slot; if Live discovery has a gap,
        # the observer continues collecting so Telegram's recent-20-run
        # monitor does not lose rolling-window coverage.
        observer_starter = getattr(getattr(components.runtime, "worker", None), "start_shadow_observer", None)
        if callable(observer_starter):
            try:
                await observer_starter()
                LOGGER.info("prediction_shadow_observer_started")
            except Exception as exc:  # noqa: BLE001 - observer is advisory and cannot block control startup
                LOGGER.warning("prediction_shadow_observer_start_failed error_type=%s", type(exc).__name__)
        if args.poll_telegram:
            if not components.telegram.enabled or components.telegram.application is None:
                LOGGER.error("prediction_telegram_poll_denied: control plane is not configured")
                return 2
            await _poll_telegram(components.telegram.application)
        else:
            # A service without Telegram polling remains alive for health and
            # future worker wiring, but never spins an order loop.
            await asyncio.Event().wait()
    finally:
        await close_prediction_components(components)
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    """Synchronous console entry point used by systemd and contract tests."""

    try:
        return asyncio.run(async_main(argv))
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":  # pragma: no cover - exercised by systemd
    raise SystemExit(main())


__all__ = [
    "DEFAULT_PREDICTION_DB",
    "DEFAULT_PREDICTION_LOG",
    "PredictionComponents",
    "PredictionConfigurationError",
    "PredictionWorkerUnavailable",
    "TelegramControlPlane",
    "async_main",
    "build_prediction_components",
    "build_prediction_telegram",
    "build_prediction_worker",
    "close_prediction_components",
    "load_prediction_environment",
    "main",
    "prediction_chat_ids",
    "prediction_auto_start_loop",
    "prediction_bot_commands",
    "read_binance_credentials",
    "resolve_prediction_db_path",
]

"""Dedicated, authorized ETH Shadow reports. No generic Live/loop/amount handlers."""
import asyncio
import os

from telegram.ext import Application, CallbackQueryHandler, CommandHandler

from .eth_t67c_policy import FINGERPRINT
from .telegram import PredictionTelegramService


def report_text(engine):
    value = engine.store.report()
    codes = '\n'.join(f"{r['code']}: {r['count']}" for r in value['diagnostics']) or '無'
    return (f"ETH T6.7c Shadow｜實驗性｜PAPER_QUOTE_ONLY\n"
            f"窗口 {value['observed_windows']}/{engine.store.namespace['windows']}｜候選報價 {value['paper_quotes']}"
            f"｜官方結果 {value['official_outcomes']}\n"
            f"無實際成交／Live WR／Live PnL／自動升級。BTC 閾值尚未經 ETH 校準。\n"
            f"缺資料與時序診斷：\n{codes}")[:3900]


class EthShadowTelegram:
    def __init__(self, engine, chat_ids):
        self.engine = engine
        self.authority = PredictionTelegramService(engine, chat_ids)

    async def report(self, update, context):
        if await self.authority._deny_if_unauthorized(update):
            return
        await self.authority._reply(update, report_text(self.engine))

    async def callback(self, update, context):
        if await self.authority._deny_if_unauthorized(update):
            return
        query = update.callback_query
        await query.answer()
        if query.data != 'eth_shadow:report:'+FINGERPRINT[:16]:
            await self.authority._reply(update, 'ETH Shadow 操作已失效或不屬於本版本。')
            return
        # Repeated callbacks read the same report; they cannot mutate loop/arm/order state.
        await self.authority._reply(update, report_text(self.engine))


def build_application(engine, environ=None):
    values = os.environ if environ is None else environ
    token = str(values.get('ETH_SHADOW_TELEGRAM_BOT_TOKEN') or '').strip()
    raw_ids = str(values.get('ETH_SHADOW_TELEGRAM_CHAT_IDS') or '').strip()
    if not token or not raw_ids:
        raise ValueError('eth_shadow_telegram_configuration_missing')
    # Never poll with the BTC bot token or fall back to its configuration.
    if token in {values.get('PREDICTION_TELEGRAM_BOT_TOKEN'), values.get('TELEGRAM_BOT_TOKEN')}:
        raise ValueError('eth_shadow_telegram_btc_token_denied')
    ids = [str(int(value.strip())) for value in raw_ids.split(',')]
    service = EthShadowTelegram(engine, ids)
    app = Application.builder().token(token).build()
    app.add_handler(CommandHandler('eth_shadow_status', service.report))
    app.add_handler(CommandHandler('eth_shadow_report', service.report))
    app.add_handler(CallbackQueryHandler(service.callback, pattern=r'^eth_shadow:'))
    return app


async def collect_with_telegram(engine, budget, grace):
    from .eth_t67c_service import collect
    app = build_application(engine)
    async with app:
        await app.start()
        try:
            await app.updater.start_polling(allowed_updates=['message', 'callback_query'])
            await collect(engine, budget, grace)
        finally:
            if app.updater.running:
                await app.updater.stop()
            await app.stop()

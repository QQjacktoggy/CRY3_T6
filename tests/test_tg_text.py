import json
import unittest
from unittest.mock import AsyncMock

from src.gridbot.prediction.telegram import (
    PredictionTelegramService, REGIME_T62_PROFILE, _regime_risk_text,
    format_runtime_result,
)


class T62TelegramTextTests(unittest.IsolatedAsyncioTestCase):
    def test_two_unit_risk_text(self):
        text = _regime_risk_text(REGIME_T62_PROFILE, '2')
        for expected in ('每筆2 USDT', '累計PnL≤-12 USDT', 'MDD≥7.0 USDT', '1U等值'):
            self.assertIn(expected, text)
        self.assertNotIn('固定1 USDT', text)

    def test_runtime_status_and_risk_show_two_unit_thresholds(self):
        value = {'strategy_profile': REGIME_T62_PROFILE, 'order_unit_usdt': '2',
                 'loop_loss_limit': '-4', 'daily_loss_limit': '-4'}
        for title in ('系統狀態', '風控狀態'):
            rendered = format_runtime_result(title, value)
            self.assertIn('累計PnL≤-12 USDT', rendered)
            self.assertIn('MDD≥7.0 USDT', rendered)

    async def test_lane_and_amount_picker_show_t62_not_generic_mdd(self):
        service = PredictionTelegramService(object(), 1)
        service._deny_if_unauthorized = AsyncMock(return_value=False)
        service._invoke = AsyncMock(return_value={
            'strategy_profile': REGIME_T62_PROFILE,
            'order_unit_usdt': '2', 'market_symbol': 'BTCUSDT',
        })
        service._reply = AsyncMock()
        await service.cmd_predict_lane(None, None)
        self.assertIn('每筆可選 1／2／3 USDT', service._reply.call_args.args[1])
        await service.cmd_predict_amount(None, None)
        text = service._reply.call_args.args[1]
        self.assertIn('目前設定：2 USDT｜持久20場MDD ≥ 7.0', text)
        self.assertIn('固定20場MDD≥3.5/7/10.5U', text)
        self.assertIn('跨Loop累計PnL≤-6/-12/-18U', text)
        self.assertNotIn('-2.5/-5.0/-7.5', text)

    def test_report_selected_two_unit_does_not_relabel_old_one_unit_fill(self):
        from test_live_report import ReportTests
        fixture = ReportTests()
        fixture.setUp()
        try:
            fixture.db.execute("UPDATE prediction_loops SET strategy_profile='regime_target6_2_v1' WHERE loop_id='new'")
            fixture.gate(net_pnl_usdt='1', risk_equity_1u='1')
            fixture.fill(pnl='1')
            fixture.db.executemany('INSERT INTO prediction_runtime_config VALUES(?,?)', [
                ('prediction_selected_strategy', json.dumps({'profile':'regime_target6_2_v1'})),
                ('prediction_selected_order_unit', json.dumps({'order_unit_usdt':'2'})),
            ])
            fixture.db.commit()
            text = fixture.render()
            self.assertIn('本輪成交金額 1 USDT', text)
            self.assertIn('目前選擇 T6.2／T6.3／T6.3a／T6.3b 每筆2 USDT', text)
            self.assertIn('20場MDD相當於7/10.5U', text)
            self.assertNotIn('20場回撤達3.5 USDT', text)
        finally:
            fixture.tearDown()


if __name__ == '__main__':
    unittest.main()

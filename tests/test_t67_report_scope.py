"""Telegram must show only T6.7, even when newer old-lane records exist."""
import json
import sqlite3
from unittest.mock import AsyncMock, Mock, patch

import pytest

from test_live_report import SCHEMA
from src.gridbot.prediction.live_report import T67_PROFILE, format_live_report
from src.gridbot.prediction.telegram import PredictionTelegramService


def database(tmp_path):
    path = tmp_path/'prediction/data/prediction.sqlite3'
    path.parent.mkdir(parents=True)
    db = sqlite3.connect(path)
    db.executescript(SCHEMA)
    return db


def test_no_t67_loop_does_not_fall_back_to_old_live(tmp_path):
    with database(tmp_path) as db:
        db.execute("INSERT INTO prediction_loops VALUES('old','regime_target6_5_v1','LIVE','CANCELLED',100,52,9,1,0)")
    text = format_live_report(tmp_path, profile_filter=T67_PROFILE)
    assert '尚未開跑' in text and 'T6.7 Live Report' in text
    for unwanted in ('old', 'T6.5', 'Shadow', 'T6.6'):
        assert unwanted not in text


@pytest.mark.parametrize('old_state', ['RUNNING','CANCELLED'])
def test_report_scope_ignores_newer_legacy_loop_and_bad_settlement(tmp_path, old_state):
    with database(tmp_path) as db:
        db.execute("INSERT INTO prediction_loops VALUES('t67',?,'LIVE','DONE',100,100,1,0,0)",(T67_PROFILE,))
        db.execute("INSERT INTO prediction_loops VALUES('old','regime_target6_5_v1','LIVE',?,100,52,9,1,0)",(old_state,))
        db.execute("INSERT INTO prediction_campaigns VALUES('legacy','old',1,1)")
        db.execute("INSERT INTO prediction_fills VALUES('legacy','BUY')")
        db.execute("INSERT INTO prediction_settlements VALUES('legacy','legacy','SETTLED','-999')")
        db.execute('INSERT INTO prediction_runtime_config VALUES(?,?)',('prediction_hard_stop_latched',json.dumps({'latched':False})))
    text = format_live_report(tmp_path, profile_filter=T67_PROFILE)
    assert 'Loop t67' in text and '本輪 WR —' in text and '本輪已知淨 PnL —' in text
    assert 'HS：未鎖定' in text and 'UNKNOWN市場 0' in text
    for branch in ('外部先行','Reference 校正','淺回撤','C-UP 前趨勢鏡像'):
        assert branch+'｜成交 0｜已知WR —｜已知PnL —' in text
    for unwanted in ('old','T6.5','Shadow','T6.6','-999','累計已知淨','資料待核對'):
        assert unwanted not in text


@pytest.mark.asyncio
async def test_telegram_handler_requests_t67_only():
    service = PredictionTelegramService(object(),1)
    service._deny_if_unauthorized = AsyncMock(return_value=False)
    service._reply = AsyncMock()
    formatter = Mock(return_value='T6.7 preview')
    with patch('src.gridbot.prediction.live_report.format_live_report',formatter):
        await service.cmd_predict_report(None,None)
    assert formatter.call_args.kwargs == {'profile_filter':T67_PROFILE}
    service._reply.assert_awaited_once_with(None,'T6.7 preview',parse_mode=None)

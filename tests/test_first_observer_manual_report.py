import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock,patch
import pytest
from src.gridbot.prediction.telegram import PredictionTelegramService,_format_first_observer_report,build_prediction_handlers

AT=1791034800000
FP='5c521aaf03e2f4ec23914a96fe8f643aaa20747be7b85f3147be02b04a234a7f'

def payload():
    m=dict(scheduled_windows=3,feature_complete=3,initial_books_complete=3,signal=1,trend_pass=1,initial_quote_eligible=1,quote_candidates=1,quote_candidate_rate=1/3,
           settled=0,pending=1,wins=0,losses=0,draws=0,wr=None,net_pnl='0',mdd='0',missing_features=0)
    return dict(at_ms=AT,health=dict(at_ms=AT),policy=FP,mode='QUOTE_SIMULATION_NO_REAL_ORDERS',rolling={str(w):{s:{d:dict(m) for d in ('ALL','UP','DOWN')} for s in ('BTCUSDT','ETHUSDT','BNBUSDT')} for w in (20,40,100)})

def save(root,p=None):
    path=root/'prediction/data/first-multimarket-v1/latest.json';path.parent.mkdir(parents=True);path.write_text(json.dumps(p or payload()));return path

@pytest.mark.parametrize('window',[20,40,100])
def test_windows_and_no_assumed_fill(tmp_path,window):
    save(tmp_path);text=_format_first_observer_report(tmp_path,window,now_ms=AT)
    for x in ('BTC','ETH','BNB','First UP','First DOWN','WR —','非真實成交','尚未完整'):assert x in text
    assert f'最近{window}場' in text and len(text)<3900
    assert 'WR 0.0%' not in text

@pytest.mark.parametrize('change',[dict(mode='LIVE'),dict(policy='wrong'),dict(at_ms=AT+1001)])
def test_invalid_provenance_fails_closed(tmp_path,change):
    p=payload();p.update(change);save(tmp_path,p)
    with pytest.raises(ValueError):_format_first_observer_report(tmp_path,now_ms=AT)

def test_stale_snapshot_is_labelled(tmp_path):
    save(tmp_path);assert '資料已過期' in _format_first_observer_report(tmp_path,now_ms=AT+120001)

def test_size_and_missing_file(tmp_path):
    with pytest.raises(FileNotFoundError):_format_first_observer_report(tmp_path,now_ms=AT)
    path=save(tmp_path);path.write_bytes(b'x'*(2*1024*1024+1))
    with pytest.raises(ValueError):_format_first_observer_report(tmp_path,now_ms=AT)

@pytest.mark.asyncio
async def test_authorization_before_read():
    service=PredictionTelegramService(object(),['1']);service._deny_if_unauthorized=AsyncMock(return_value=True);service._reply=AsyncMock()
    with patch('src.gridbot.prediction.telegram._format_first_observer_report',side_effect=AssertionError('must not read')):
        await service.cmd_firstreport(None,SimpleNamespace(args=[]))
    service._reply.assert_not_called()

@pytest.mark.asyncio
@pytest.mark.parametrize('args',[['5'],['20','40']])
async def test_bad_arguments_do_not_read(args):
    service=PredictionTelegramService(object(),['1']);service._deny_if_unauthorized=AsyncMock(return_value=False);service._reply=AsyncMock()
    with patch('src.gridbot.prediction.telegram._format_first_observer_report',side_effect=AssertionError('must not read')):
        await service.cmd_firstreport(None,SimpleNamespace(args=args))
    assert '用法' in service._reply.call_args.args[1]

@pytest.mark.asyncio
async def test_command_routes_readonly_renderer():
    service=PredictionTelegramService(object(),['1'],now_ms=lambda:AT);service._deny_if_unauthorized=AsyncMock(return_value=False);service._reply=AsyncMock()
    with patch('src.gridbot.prediction.telegram._format_first_observer_report',return_value='result') as renderer:
        await service.cmd_firstreport(None,SimpleNamespace(args=['40']))
    assert renderer.call_args.args[1]==40;service._reply.assert_awaited_once_with(None,'result',parse_mode=None)

def test_handler_and_command_menu():
    from predict_main import prediction_bot_commands
    service=PredictionTelegramService(object(),['1'])
    handlers=build_prediction_handlers(service)
    for name in ('firstreport','t67creport'):
        assert not any(name in getattr(h,'commands',()) for h in handlers)
        assert not any(c.command==name for c in prediction_bot_commands())


def test_range_recheck_attempt_and_rejection_explained(tmp_path):
    p=payload();p['rolling_ranges']={'20':dict(start=AT-6000000,end=AT,count=20)}
    for group in p['rolling']['20'].values():
        group['ALL'].update(quote_candidates=0,quote_candidate_rate=0,missing_features=1,
            recheck_attempted=1,recheck_reasons={'price_above_frozen_cap':1})
    save(tmp_path,p);text=_format_first_observer_report(tmp_path,now_ms=AT)
    for label in ('統計區間','已結束市場','重檢：執行1｜通過0','高於凍結限價 1','不是0收益','不算條件不符'):
        assert label in text
    assert len(text)<3900

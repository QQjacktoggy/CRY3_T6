"""Retired menu callbacks cannot select sibling lanes or arm P3."""
from types import SimpleNamespace
from unittest.mock import AsyncMock
import pytest
from src.gridbot.prediction.telegram import PredictionTelegramService, SELECTABLE_LANES, selectable_lanes_for_market


def test_menu_keeps_all_t6_versions_only_and_prioritizes_c():
    profiles=[p for p,_ in SELECTABLE_LANES]
    assert profiles[0]=='regime_target6_7c_v1'
    assert len(profiles)==len(set(profiles))==11
    assert all(p.startswith('regime_target6') for p in profiles)
    for market in ('BTCUSDT','ETHUSDT',None):
        assert all(p.startswith('regime_target6') for p,_ in selectable_lanes_for_market(market))


@pytest.mark.asyncio
@pytest.mark.parametrize('profile',['s3s5_pair_v1','fav_only_v1','fav_only_v2','fav_only_v3','fav_only_v4','fav_p3','c180_favorite_hold_v1'])
async def test_old_non_t6_callback_has_no_selection_or_arm_side_effect(profile):
    service=PredictionTelegramService(object(),1)
    service._deny_if_unauthorized=AsyncMock(return_value=False)
    service._invoke=AsyncMock(return_value={'market_symbol':'BTCUSDT'})
    service._reply=AsyncMock()
    update=SimpleNamespace(callback_query=SimpleNamespace(data='predict_lane:'+profile,answer=AsyncMock()))
    await service.handle_callback(update,None)
    assert service._invoke.await_count==1
    assert service._invoke.await_args.args[0]==('status','predict_status')
    assert '策略選項無效' in service._reply.await_args.args[1]


@pytest.mark.asyncio
async def test_picker_text_and_buttons_only_describe_available_t6_lanes():
    service = PredictionTelegramService(object(), 1)
    service._deny_if_unauthorized = AsyncMock(return_value=False)
    service._invoke = AsyncMock(return_value={
        'strategy_profile': 'regime_target6_7c_v1',
        'market_symbol': 'BTCUSDT', 'order_unit_usdt': '1',
    })
    service._reply = AsyncMock()
    await service.cmd_predict_lane(None, None)
    text = service._reply.await_args.args[1]
    assert '目前提供 T6 系列策略' in text
    assert 'P3' not in text and 'fav_p3' not in text and 'FAV' not in text
    rows = service._reply.await_args.kwargs['reply_markup'].inline_keyboard
    assert len(rows) == 11
    assert rows[0][0].callback_data == 'predict_lane:regime_target6_7c_v1'
    assert all(row[0].callback_data.startswith('predict_lane:regime_target6') for row in rows)
    assert service._invoke.await_count == 1
    assert service._invoke.await_args.args[0] == ('status', 'predict_status')

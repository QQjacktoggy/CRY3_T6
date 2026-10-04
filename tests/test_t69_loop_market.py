"""T6.9 uses the T6.7c whole-loop asset binding with its own execution identity."""
from decimal import Decimal as D
from unittest.mock import patch

import pytest

from src.gridbot.prediction.loop_market import (PROFILE as T67C_PROFILE, PROFILES, SYMBOLS,
    data_paths, execution_fingerprint)
from src.gridbot.prediction.regime_t69_policy import PROFILE, TIER
from test_loop_market import Harness, mark, market, repo  # noqa: F401
from test_t63 import S


async def start(repo, name='loop', asset='BNBUSDT', count=20, profile=PROFILE):
    return await repo.start_loop(name, count, mode='LIVE', strategy_profile=profile,
                                 market_symbol=asset, market_unit='1')


def test_t69_is_a_loop_market_profile_with_distinct_fingerprint():
    assert PROFILES == (T67C_PROFILE, PROFILE)
    for asset in SYMBOLS:
        assert execution_fingerprint(asset, PROFILE) != execution_fingerprint(asset, T67C_PROFILE)
        assert execution_fingerprint(asset) == execution_fingerprint(asset, T67C_PROFILE)
    with pytest.raises(ValueError):
        execution_fingerprint('BTCUSDT', 'regime_target6_8a_v1')


@pytest.mark.asyncio
@pytest.mark.parametrize('asset', SYMBOLS)
async def test_t69_binds_each_asset_and_identity_is_immutable(repo, asset):
    await start(repo, asset=asset)
    binding = await repo.get_loop_market_binding('loop')
    assert binding['symbol'] == asset and binding['profile'] == PROFILE
    assert binding['execution_fingerprint'] == execution_fingerprint(asset, PROFILE)
    other = next(a for a in SYMBOLS if a != asset)
    with pytest.raises(ValueError, match='immutable'):
        await start(repo, asset=other)
    w = Harness(repo)
    w._selected_strategy_profile = PROFILE
    await w.restore_loop_market()
    assert w.settings.market_symbol == asset
    assert await w._loop_market_start_guard(20) is None
    assert w._loop_market_start_kwargs() == dict(market_symbol=asset, market_unit='1')


@pytest.mark.asyncio
async def test_t69_binding_cannot_be_reread_as_t67c(repo):
    await start(repo, asset='ETHUSDT')
    import sqlite3
    with pytest.raises(sqlite3.IntegrityError, match='immutable'):
        await repo._execute("UPDATE prediction_loop_market_bindings SET profile=?", (T67C_PROFILE,))
    await repo._execute("UPDATE prediction_loops SET strategy_profile=?", (T67C_PROFILE,))
    w = Harness(repo)
    with pytest.raises(ValueError, match='identity changed'):
        await w.restore_loop_market()


@pytest.mark.asyncio
async def test_t69_idle_market_selection(repo):
    w = Harness(repo)
    w._selected_strategy_profile = PROFILE
    for p in data_paths(repo.db_path, 'BNBUSDT'):
        mark(p, 'BNBUSDT')
    result = await w.select_market('BNBUSDT')
    assert not result.get('action_denied') and result['market_symbol'] == 'BNBUSDT'
    w._selected_strategy_profile = 'regime_target6_8a_v1'
    denied = await w.select_market('ETHUSDT')
    assert denied['action_denied'] and 'T6.9' in denied['reason']
    assert 'only T6.7c/T6.9' in await w._loop_market_start_guard(20)


@pytest.mark.asyncio
@pytest.mark.parametrize('asset,raw_asset,allowed', [
    ('BNBUSDT', 'BNBUSDT', True), ('ETHUSDT', 'ETHUSDT', True),
    ('BNBUSDT', 'BTCUSDT', False), ('ETHUSDT', 'BNBUSDT', False),
])
async def test_t69_final_atomic_buy_gate_rechecks_loop_asset(repo, asset, raw_asset, allowed):
    from src.gridbot.prediction.models import Campaign, MarketInfo
    from src.gridbot.prediction.regime_live_ledger import RegimeLiveLedger
    await repo._execute('ALTER TABLE prediction_order_intents ADD COLUMN client_order_id TEXT')
    await repo._execute('ALTER TABLE prediction_order_intents ADD COLUMN tier TEXT')
    await start(repo, asset=asset)
    m = MarketInfo('topic', 'up', 'slug', S, S+300000, up_market_id='up', down_market_id='down',
                   raw=market(raw_asset).raw)
    await repo.save_campaign(Campaign('campaign', m), loop_id='loop')
    ledger = RegimeLiveLedger(repo, profile=PROFILE)
    await ledger.seed_schedule(loop_id='loop', first_market_start_ms=S)
    await ledger.verify_market(loop_id='loop', market_start_ms=S, market_topic_id='topic',
                               market_id='up', verified_at_ms=S+120000)
    intent = dict(intent_id='intent', campaign_id='campaign', action='BUY_INITIAL', outcome='UP',
                  order_side='BUY', amount='1', limit_price='.78', created_at_ms=S+128000, ttl_ms=1000,
                  attempt=1, status='PENDING', tier=TIER, payload={})
    with patch('src.gridbot.prediction.regime_live_ledger._now_ms', return_value=S+128000):
        result = await ledger.reserve_c180_intent(loop_id='loop', market_start_ms=S, campaign_id='campaign',
            intent=intent, decision_at_ms=S+128000, wallet_reconciled_at_ms=S+128000)
    assert result.claimed is allowed, result.reason


def test_t69_menu_and_shadow_follow_bound_asset(tmp_path):
    import sqlite3
    from src.gridbot.prediction.regime_t69_shadow import observe, schema
    from src.gridbot.prediction.regime_feature_service import connect
    from src.gridbot.prediction.telegram import selectable_lanes_for_market
    for asset in ('ETHUSDT', 'BNBUSDT'):
        lanes = dict(selectable_lanes_for_market(asset))
        assert set(lanes) == {PROFILE, T67C_PROFILE}
    pred = tmp_path/'prediction.sqlite3'
    with sqlite3.connect(pred) as db:
        db.executescript("""
            CREATE TABLE prediction_loops(loop_id TEXT,strategy_profile TEXT,mode TEXT,state TEXT);
            CREATE TABLE prediction_regime_slots(loop_id TEXT,market_start_ms INTEGER,market_topic_id TEXT,market_id TEXT,verified_at_ms INTEGER);
            CREATE TABLE prediction_loop_market_bindings(loop_id TEXT,symbol TEXT);
            CREATE TABLE prediction_runtime_config(config_key TEXT,config_value_json TEXT);
        """)
        db.execute("INSERT INTO prediction_loops VALUES('loop',?,'LIVE','RUNNING')", (PROFILE,))
        db.execute("INSERT INTO prediction_regime_slots VALUES('loop',?,'t','u',?)", (S, S+1))
        db.execute("INSERT INTO prediction_loop_market_bindings VALUES('loop','ETHUSDT')")
    feature = connect(tmp_path/'features.sqlite3')
    schema(feature)
    assert observe(feature, pred, tmp_path/'signals.sqlite3', S+70000, 'BTCUSDT') == 'other_asset_loop'
    assert observe(feature, pred, tmp_path/'signals.sqlite3', S+70000, 'ETHUSDT') == 'shadow_unit_missing'

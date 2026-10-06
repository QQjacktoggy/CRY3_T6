"""Cross-asset isolation, durable boundaries and unchanged T6.7c decisions."""
import asyncio
from contextlib import closing
from dataclasses import replace
from decimal import Decimal as D
import json
import sqlite3
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import pytest
import pytest_asyncio

from src.gridbot.prediction.loop_market import (SYMBOLS, PROFILE, bind_data_db,
    data_paths, execution_fingerprint, market_matches, report_feature_path, verify_data_db)
from src.gridbot.prediction.repository import PredictionRepository
from src.gridbot.prediction.loop_market_worker import LoopMarketWorker
from src.gridbot.prediction.settings import PredictionSettings, RuntimeMode
from src.gridbot.prediction.regime_feature_service import connect
from src.gridbot.prediction.regime_worker_bridge import RegimeWorkerBridge
from src.gridbot.prediction.c180_evidence_collector import C180EvidenceCollector
from src.gridbot.prediction.c180_jev_client import questions_for_symbol, _frozen_state
from src.gridbot.prediction.c180_signal_service import C180Signal
from src.gridbot.prediction.c180_favorite import C180EntryDecision
from test_t63 import S, feature, book


def mark(path, asset):
    path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(path) as db:
        bind_data_db(db, asset)


def market(asset):
    return SimpleNamespace(start_time_ms=S, end_time_ms=S+300000,
        market_topic_id=asset+'-topic', up_market_id=asset+'-up',
        raw={'symbol':asset, 'variantData':{'priceFeedSymbol':asset}})


@pytest_asyncio.fixture
async def repo(tmp_path):
    r=PredictionRepository(tmp_path/'prediction/data/prediction.sqlite3')
    await r.initialize()
    try:
        yield r
    finally:
        await r.close()


async def start(repo, name='loop', asset='BNBUSDT', count=20):
    return await repo.start_loop(name,count,mode='LIVE',strategy_profile=PROFILE,
                                market_symbol=asset,market_unit='1')


@pytest.mark.asyncio
async def test_atomic_start_and_immutable_identity(repo):
    await start(repo)
    binding=await repo.get_loop_market_binding('loop')
    assert binding['symbol']=='BNBUSDT' and binding['target']==20
    for kwargs in ({'asset':'ETHUSDT'}, {'count':21}):
        with pytest.raises(ValueError, match='immutable'):
            await start(repo,**kwargs)
    with pytest.raises(sqlite3.IntegrityError, match='immutable'):
        await repo._execute("UPDATE prediction_loop_market_bindings SET symbol='ETHUSDT'")
    assert (await repo.get_loop_market_binding('loop'))==binding


@pytest.mark.asyncio
async def test_concurrent_start_only_one_global_loop(repo):
    other=PredictionRepository(repo.db_path)
    await other.initialize()
    try:
        results=await asyncio.gather(start(repo,'bnb'),start(other,'eth','ETHUSDT'),return_exceptions=True)
        assert sum(isinstance(r,ValueError) for r in results)==1
        assert len(await repo._fetchall("SELECT * FROM prediction_loops WHERE state='RUNNING'"))==1
        assert len(await repo._fetchall('SELECT * FROM prediction_loop_market_bindings'))==1
    finally:
        await other.close()


@pytest.mark.asyncio
async def test_failed_binding_does_not_leave_loop(repo):
    await repo._execute("CREATE TRIGGER fail_binding BEFORE INSERT ON prediction_loop_market_bindings BEGIN SELECT RAISE(ABORT,'injected'); END")
    with pytest.raises(sqlite3.IntegrityError):
        await start(repo)
    assert await repo.get_active_loop() is None


@pytest.mark.asyncio
async def test_cancelled_loop_not_reopened_and_risk_not_reset(repo):
    risk={'halt_reason':'loss_stop','loss_count':4}
    await repo.set_runtime_config('regime_target6_risk_v1',risk)
    await start(repo)
    await repo._execute("UPDATE prediction_loops SET state='CANCELLED'")
    with pytest.raises(ValueError,match='state'):
        await start(repo)
    await start(repo,'next','ETHUSDT')
    assert await repo.get_runtime_config('regime_target6_risk_v1',{})==risk


@pytest.mark.asyncio
async def test_pending_unknown_blocks_even_terminal_campaign(repo):
    # The ledger can contain an unresolved terminal campaign after a late fill.
    from src.gridbot.prediction.models import Campaign, MarketInfo, CampaignState
    c=Campaign('late',MarketInfo('topic','up','late',S,S+300000))
    c.state=CampaignState.CANCELLED
    c.pending_unknown=True
    await repo.save_campaign(c)
    assert not await repo.loop_market_local_clear()
    with pytest.raises(ValueError,match='exposure'):
        await start(repo)


class Harness(LoopMarketWorker):
    def __init__(self,repo,asset='BTCUSDT'):
        self.repository=repo
        self.settings=PredictionSettings(market_symbol=asset)
        self._selected_strategy_profile=PROFILE
        self._selected_order_unit_usdt=D(1)
        self._effective_mode=RuntimeMode.LIVE
        self._lock=asyncio.Lock()
        self._task=None
        self._c180_recovery_exposure_clear=AsyncMock(return_value=True)
        self.restore_order_unit=AsyncMock()
        self.restore_selected_strategy=AsyncMock()
    def _status(self):
        return {'market_symbol':self.settings.market_symbol}


@pytest.mark.asyncio
async def test_next_choice_does_not_change_running_loop_and_restart_uses_binding(repo):
    await start(repo)
    w=Harness(repo)
    await w.restore_loop_market()
    assert w.settings.market_symbol=='BNBUSDT'
    result=await w.select_market('ETHUSDT')
    assert result['market_queued'] and w.settings.market_symbol=='BNBUSDT'
    w._c180_recovery_exposure_clear.assert_not_called()
    assert await w._loop_market_start_guard(20) is None
    assert 'immutable' in await w._loop_market_start_guard(100)
    restarted=Harness(repo,'BTCUSDT')
    await restarted.restore_loop_market()
    assert restarted.settings.market_symbol=='BNBUSDT'
    await repo._execute("UPDATE prediction_loops SET state='DONE',completed=20")
    assert 'apply next market' in await w._loop_market_start_guard(20)


@pytest.mark.asyncio
async def test_idle_selection_requires_official_zero_and_disarms(repo):
    w=Harness(repo)
    w._c180_recovery_exposure_clear.return_value=False
    assert (await w.select_market('ETHUSDT'))['action_denied']
    assert w.settings.market_symbol=='BTCUSDT'
    for p in data_paths(repo.db_path,'ETHUSDT'):mark(p,'ETHUSDT')
    w._c180_recovery_exposure_clear.return_value=True
    result=await w.select_market('ETHUSDT')
    assert result['live_rearm_required'] and w._effective_mode is RuntimeMode.SHADOW
    assert (await repo.get_runtime_config('prediction_selected_market',{}))['symbol']=='ETHUSDT'
    assert await repo.get_active_loop() is None


@pytest.mark.asyncio
async def test_restart_does_not_infer_asset_for_unbound_running_loop(repo):
    await repo.start_loop('old',20,mode='LIVE',strategy_profile=PROFILE)
    await repo.set_runtime_config('prediction_selected_market',{'symbol':'ETHUSDT'})
    with pytest.raises(ValueError,match='no market binding'):
        await Harness(repo).restore_loop_market()


@pytest.mark.parametrize('asset',SYMBOLS)
def test_asset_identity_and_no_relabelling(tmp_path,asset):
    path=tmp_path/'features'
    mark(path,asset)
    verify_data_db(path,asset)
    other=next(a for a in SYMBOLS if a!=asset)
    with sqlite3.connect(path) as db, pytest.raises(ValueError,match='mismatch'):
        bind_data_db(db,other)
    with pytest.raises(ValueError,match='mismatch'):
        verify_data_db(path,other)
    assert market_matches(market(asset),asset)
    assert not market_matches(market(other),asset)


def test_legacy_populated_db_cannot_be_claimed_as_bnb(tmp_path):
    p=tmp_path/'old'
    with sqlite3.connect(p) as db:
        db.execute('CREATE TABLE features(start INTEGER,payload TEXT)')
        db.execute("INSERT INTO features VALUES(1,'BTC evidence')")
        db.commit()
        with pytest.raises(ValueError,match='populated'):
            bind_data_db(db,'BNBUSDT')


@pytest.mark.parametrize('asset',SYMBOLS)
@pytest.mark.parametrize('first,last,prior,up,down,branch',[
    (2,-1,2,'.3','.7','core_first_up'),
    (-2,1,-2,'.7','.3','core_first_down'),
    (2,'.2',2,'.6','.4','core_stall_down'),
    (2,-4,-2,'.3','.7','core_c_down'),
    (2,1,2,'.8','.2','core_continuation_original'),
    (-1,3,2,'.7','.3','c_mirror_up_prior'),
    (2,-1,-2,'.6','.4','shallow_retracement'),
])
def test_all_seven_t67c_branches_preserved_per_asset(tmp_path,asset,first,last,prior,up,down,branch):
    fpath,spath=data_paths(tmp_path/'prediction.sqlite3',asset)
    mark(fpath,asset);mark(spath,asset)
    f=feature(first,last,prior);f['symbol']=asset
    with closing(connect(fpath)) as db,db:
        db.execute('INSERT INTO features VALUES(?,?)',(S,json.dumps(f)))
    b=RegimeWorkerBridge(None,spath,feature_db=fpath,profile=PROFILE,symbol=asset)
    b._registered_loop_id='loop'
    m=market(asset);snap=book(up,down,124000)
    snap.update(market_topic=m.market_topic_id,market_id=m.up_market_id)
    original=None
    if branch=='core_continuation_original':
        original=C180Signal(S,m.market_topic_id,m.up_market_id,S+120000,S+120500,
            'entry_positive_cost_after_ev',C180EntryDecision('DOWN','original','DOWN',D(1),D(5),None),D('.2'),None,None,D(200))
    with patch.object(b,'_first_book',return_value=snap),patch('src.gridbot.prediction.regime_worker_bridge.read_c180_book',return_value=snap),patch('src.gridbot.prediction.regime_worker_bridge.read_c180_signal',return_value=original):
        ready=b.check_signal(market=m,unit_usdt=D(1),at_ms=S+124000,last_seen_book_at_ms=0)
    assert ready.allowed,ready.reason
    with sqlite3.connect(fpath) as db:
        decision=json.loads(db.execute('SELECT payload FROM t67c_decisions').fetchone()[0])
    assert decision['branch']==branch
    assert decision['core_guard']['features']['symbol']==asset


def test_wrong_feature_asset_rejected_at_same_timestamp(tmp_path):
    from src.gridbot.prediction.regime_t67a_bridge import freeze_core
    f=feature(2,-1,2);f['symbol']='BTCUSDT'
    b=SimpleNamespace(symbol='ETHUSDT')
    with pytest.raises(ValueError,match='asset'):
        freeze_core(b,market('ETHUSDT'),f,S+124000,D(1))


@pytest.mark.parametrize('asset',SYMBOLS)
def test_public_ws_and_original_question_follow_asset(asset):
    tape=Mock()
    collector=C180EvidenceCollector(tape=tape,feed_factory=Mock(),symbol=asset)
    collector.ingest_trade('spot',{'s':asset,'p':'100'},received_at_ms=S)
    other=next(a for a in SYMBOLS if a!=asset)
    with pytest.raises(ValueError,match='asset'):
        collector.ingest_trade('futures',{'s':other,'p':'100'},received_at_ms=S)
    assert tape.ingest.call_count==1
    q=questions_for_symbol(asset)['direction']['instructions']
    assert asset.removesuffix('USDT')+' five-minute' in q
    if asset!='BTCUSDT':assert 'BTC' not in q


@pytest.mark.asyncio
async def test_report_reads_historical_loop_binding_not_current_choice(repo,tmp_path):
    await start(repo)
    await repo.set_runtime_config('prediction_selected_market',{'symbol':'ETHUSDT'})
    assert report_feature_path(tmp_path,'loop')==data_paths(repo.db_path,'BNBUSDT')[0]


@pytest.mark.asyncio
async def test_tg_command_auth_and_selection():
    from src.gridbot.prediction.telegram import PredictionTelegramService
    svc=PredictionTelegramService(object(),1)
    svc._deny_if_unauthorized=AsyncMock(return_value=True)
    svc._call_and_reply=AsyncMock()
    await svc.cmd_predict_market(None,SimpleNamespace(args=['BNB']))
    svc._call_and_reply.assert_not_called()
    svc._deny_if_unauthorized.return_value=False
    await svc.cmd_predict_market(None,SimpleNamespace(args=['BNB']))
    assert svc._call_and_reply.await_args.args[-1]=='BNBUSDT'


@pytest.mark.asyncio
@pytest.mark.parametrize('asset,raw_asset,unit,allowed', [
    ('BNBUSDT','BNBUSDT','1',True), ('ETHUSDT','ETHUSDT','1',True),
    ('BNBUSDT','BTCUSDT','1',False), ('ETHUSDT','BNBUSDT','1',False),
    ('BNBUSDT','BNBUSDT','2',False),
])
async def test_final_atomic_buy_gate_rechecks_loop_asset_and_unit(repo,asset,raw_asset,unit,allowed):
    from src.gridbot.prediction.models import Campaign, MarketInfo
    from src.gridbot.prediction.regime_live_ledger import RegimeLiveLedger
    from src.gridbot.prediction.regime_t67c_policy import TIER
    await repo._execute('ALTER TABLE prediction_order_intents ADD COLUMN client_order_id TEXT')
    await repo._execute('ALTER TABLE prediction_order_intents ADD COLUMN tier TEXT')
    await start(repo,asset=asset)
    m=MarketInfo('topic','up','slug',S,S+300000,up_market_id='up',down_market_id='down',raw=market(raw_asset).raw)
    await repo.save_campaign(Campaign('campaign',m),loop_id='loop')
    ledger=RegimeLiveLedger(repo,profile=PROFILE)
    await ledger.seed_schedule(loop_id='loop',first_market_start_ms=S)
    await ledger.verify_market(loop_id='loop',market_start_ms=S,market_topic_id='topic',market_id='up',verified_at_ms=S+120000)
    intent=dict(intent_id='intent',campaign_id='campaign',action='BUY_INITIAL',outcome='UP',order_side='BUY',
        amount=unit,limit_price='.4',created_at_ms=S+124000,ttl_ms=1000,attempt=1,status='PENDING',tier=TIER,payload={})
    with patch('src.gridbot.prediction.regime_live_ledger._now_ms',return_value=S+124000):
        result=await ledger.reserve_c180_intent(loop_id='loop',market_start_ms=S,campaign_id='campaign',
            intent=intent,decision_at_ms=S+124000,wallet_reconciled_at_ms=S+124000)
    assert result.claimed is allowed,result.reason
    assert bool(await repo._fetchall('SELECT 1 FROM prediction_order_intents')) is allowed


@pytest.mark.asyncio
async def test_only_bound_asset_gets_paid_original(repo):
    from src.gridbot.prediction.loop_market import signal_asset_active
    await start(repo,asset='ETHUSDT')
    assert signal_asset_active(repo.db_path,'ETHUSDT')
    assert not signal_asset_active(repo.db_path,'BNBUSDT')
    assert not signal_asset_active(repo.db_path,'BTCUSDT')


@pytest.mark.asyncio
async def test_legacy_start_api_cannot_bypass_bound_identity_or_global_loop(repo):
    await start(repo)
    with pytest.raises(ValueError,match='immutable'):
        await repo.start_loop('loop',100,mode='LIVE',strategy_profile=PROFILE)
    with pytest.raises(ValueError,match='another bound'):
        await repo.start_loop('other',20,mode='LIVE',strategy_profile='regime_target6_7d_v1')
    assert len(await repo._fetchall('SELECT * FROM prediction_loops'))==1


@pytest.mark.asyncio
async def test_return_to_btc_keeps_other_legacy_t6_profiles_restartable(repo):
    await repo.set_runtime_config('prediction_selected_market',{'symbol':'BTCUSDT'})
    await repo.start_loop('old',20,mode='LIVE',strategy_profile='regime_target6_7d_v1')
    w=Harness(repo)
    w._selected_strategy_profile='regime_target6_7d_v1'
    await w.restore_loop_market()
    assert w.settings.market_symbol=='BTCUSDT'


@pytest.mark.parametrize('asset',SYMBOLS)
def test_jev_hash_and_contract_identity_are_asset_bound(asset):
    from src.gridbot.prediction.c180_jev_client import _frozen_state
    state=dict(contract=dict(start=S,end=S+300000,reference=100,topic='topic',symbol=asset),
        observed_at=S+120000,remaining_seconds=180,features={'spot':{},'futures':{}},quote=None,
        stake_usdt=1,fee_bps_cash_sensitivity=200,data_warnings=[],basis_bps=0)
    _,_,digest=_frozen_state(state,symbol=asset)
    assert len(digest)==64
    if asset!='BTCUSDT':
        with pytest.raises(ValueError,match='asset'):
            _frozen_state({**state,'contract':{**state['contract'],'symbol':'BTCUSDT'}},symbol=asset)
        _,_,btc_digest=_frozen_state(state,symbol='BTCUSDT')
        assert digest!=btc_digest


def test_tg_market_result_never_hides_pending_asset_after_large_status():
    from src.gridbot.prediction.telegram import format_runtime_result
    payload={f'old_status_{i}':i for i in range(30)}
    payload.update(market_symbol='BTCUSDT',next_market_symbol='BNBUSDT',market_queued=True)
    text=format_runtime_result('T6.7c 整輪市場',payload)
    assert 'BTCUSDT' in text and 'BNBUSDT' in text and '已排下一輪' in text
    assert 'old_status_' not in text


@pytest.mark.asyncio
@pytest.mark.parametrize('case,clear', [
    ('historical',True),('latest',False),('post_migration',False),
    ('running_parent',False),('not_done',False),('missing_parent',False),
])
async def test_legacy_closed_snapshot_boundary(repo, case, clear):
    from src.gridbot.prediction.models import Campaign, MarketInfo, CampaignState
    await repo._execute("INSERT INTO prediction_loops(loop_id,target,state,created_at_ms,updated_at_ms) VALUES('old',20,'CANCELLED',1,1)")
    if case!='latest':
        await repo._execute("INSERT INTO prediction_loops(loop_id,target,state,created_at_ms,updated_at_ms) VALUES('recent',20,'CANCELLED',1000,1000)")
    await repo._execute("UPDATE prediction_migrations SET applied_at_ms=500 WHERE filename='026_loop_market.sql'")
    c=Campaign('legacy',MarketInfo('topic','up','legacy',100,200))
    c.state=CampaignState.DONE
    await repo.save_campaign(c,loop_id='old')
    await repo.save_position_snapshot('legacy',{'up_shares':'2','down_shares':'0'})
    if case=='post_migration':
        await repo._execute("UPDATE prediction_campaigns SET end_time_ms=600")
    elif case=='running_parent':
        await repo._execute("UPDATE prediction_loops SET state='RUNNING' WHERE loop_id='old'")
    elif case=='not_done':
        await repo._execute("UPDATE prediction_campaigns SET state='CANCELLED'")
    elif case=='missing_parent':
        await repo._execute("UPDATE prediction_campaigns SET loop_id='missing'")
    assert await repo.loop_market_local_clear() is clear
    if clear:
        w=Harness(repo)
        w._c180_recovery_exposure_clear.return_value=False
        assert not await w._market_boundary_clear()
        w._c180_recovery_exposure_clear.assert_awaited_once()
        w._c180_recovery_exposure_clear.return_value=True
        assert await w._market_boundary_clear()
        # Admission never rewrites an old position or fabricates settlement.
        assert len(await repo._fetchall('SELECT * FROM prediction_position_snapshots'))==1
        assert not await repo._fetchall('SELECT * FROM prediction_settlements')


def _stub_coin_script(root, code=0):
    script = root/'scripts/t6_coin.sh'
    script.parent.mkdir(parents=True, exist_ok=True)
    script.write_text(f'echo "$@" >> "{root}/coin.log"\necho "last line"\nexit {code}\n')
    return root/'coin.log'


@pytest.mark.asyncio
async def test_telegram_market_switch_runs_producer_script_and_waits_warmup(repo, tmp_path):
    log = _stub_coin_script(tmp_path)
    for p in data_paths(repo.db_path,'ETHUSDT'):mark(p,'ETHUSDT')
    w=Harness(repo)
    result=await w.select_market('ETHUSDT')
    assert log.read_text().split() == ['use','ETH']
    assert result['producer_switch']=='done' and result['producer_warmup_until_ms']
    assert '暖機' in await w._loop_market_start_guard(20)
    await repo.set_runtime_config('prediction_producer_switch',{'symbol':'ETHUSDT','ready_at_ms':1})
    assert await w._loop_market_start_guard(20) is None
    from src.gridbot.prediction.telegram import format_runtime_result
    assert '資料程式已切到此幣' in format_runtime_result('T6.7c／T6.9／T6.9b 整輪市場', result)


@pytest.mark.asyncio
async def test_failed_producer_switch_keeps_old_market(repo, tmp_path):
    _stub_coin_script(tmp_path, code=3)
    for p in data_paths(repo.db_path,'ETHUSDT'):mark(p,'ETHUSDT')
    w=Harness(repo)
    result=await w.select_market('ETHUSDT')
    assert result['action_denied'] and 'exit 3' in result['reason'] and 'last line' in result['reason']
    assert w.settings.market_symbol=='BTCUSDT'
    assert await repo.get_runtime_config('prediction_selected_market',{})=={}


@pytest.mark.asyncio
async def test_queued_choice_never_touches_producers(repo, tmp_path):
    log = _stub_coin_script(tmp_path)
    await start(repo)
    w=Harness(repo)
    assert (await w.select_market('ETHUSDT'))['market_queued']
    assert not log.exists()


@pytest.mark.asyncio
async def test_btc_switch_has_no_warmup(repo, tmp_path):
    log = _stub_coin_script(tmp_path)
    w=Harness(repo)
    result=await w.select_market('BTCUSDT')
    assert log.read_text().split()==['use','BTC'] and result['producer_warmup_until_ms'] is None
    assert await w._loop_market_start_guard(20) is None

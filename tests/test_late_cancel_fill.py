"""Official cancellation can precede its final positive cumulative fill."""
import json
import time
from decimal import Decimal as D
from unittest.mock import AsyncMock, Mock, patch

import pytest

from src.gridbot.prediction.models import Campaign, CampaignState, MarketInfo, OrderIntent, ActionType, OrderSide, OutcomeSide
from src.gridbot.prediction.repository import PredictionRepository
from src.gridbot.prediction.regime_live_ledger import RegimeLiveLedger
from src.gridbot.prediction.worker import PredictionWorker, PredictionRateLimitDeferred
from test_loop_cancel_recovery import setup_worker, cleanup
from test_t63 import S


def snapshot(status='CANCELLED', qty='0'):
    return dict(orderId='order',marketId='official-down',marketTopicId='official-topic',
                side='BUY',outcome='Down',status=status,filledShareQty=qty,
                filledUsdtAmount='0' if qty=='0' else '.98',price='.65',
                marketProviderFee='0' if qty=='0' else '.017213',networkFee='0',
                modifyTime=S+128000,createTime=S+124000)


async def cancelled(repo, campaign):
    columns={r['name'] for r in await repo._fetchall('PRAGMA table_info(prediction_order_intents)')}
    for name in ('tier','client_order_id'):
        if name not in columns:await repo._execute('ALTER TABLE prediction_order_intents ADD COLUMN '+name+' TEXT')
    intent=OrderIntent('intent',campaign.campaign_id,ActionType.BUY_INITIAL,
                       OutcomeSide.DOWN,OrderSide.BUY,D(1),D('.65'),S+124000,2000)
    await repo.create_intent(intent)
    await repo.update_intent('intent',submission_at_ms=S+124001,order_id='order',status='SUBMITTED')
    await repo.apply_order_snapshot_atomic(campaign,intent,'order',snapshot(),outcome=OutcomeSide.DOWN)
    return intent


@pytest.mark.asyncio
@pytest.mark.parametrize('response', ['filled','partial_cancel','empty','missing','nonterminal','deferred','wrong_identity'])
async def test_market_end_rechecks_cancelled_submission(tmp_path,response):
    repo,w,c,clock,_=await setup_worker(tmp_path,shares='0',state=CampaignState.OBSERVE)
    try:
        await cancelled(repo,c);clock[0]=S+310000
        w._history_rows=AsyncMock(return_value=[snapshot('FILLED','1.51')])
        if response=='partial_cancel':w._history_rows.return_value=[snapshot('CANCELLED','1.51')]
        if response=='empty':w._history_rows.return_value=[snapshot()]
        if response=='missing':w._history_rows.return_value=[]
        if response=='nonterminal':w._history_rows.return_value=[snapshot('OPENING')]
        if response=='wrong_identity':w._history_rows.return_value=[dict(snapshot('FILLED','1.51'),marketId='unrelated')]
        if response=='deferred':w._history_rows.side_effect=PredictionRateLimitDeferred('query_order_history',{})
        result=await w._recheck_cancelled_before_settlement(c)
        assert result is (response in ('filled','partial_cancel','empty'))
        fills=await repo.get_fills(c.campaign_id)
        assert len(fills)==int(response in ('filled','partial_cancel'))
        if fills:
            assert c.buy_count==1 and c.position.down_shares==D('1.51')
            await w._recheck_cancelled_before_settlement(c)
            assert len(await repo.get_fills(c.campaign_id))==1
        w.client.place_order.assert_not_awaited()
    finally:await cleanup(repo,w)


@pytest.mark.asyncio
async def test_unsubmitted_reject_never_queries_exchange(tmp_path):
    repo,w,c,clock,_=await setup_worker(tmp_path,shares='0',state=CampaignState.OBSERVE)
    try:
        clock[0]=S+310000;w._history_rows=AsyncMock()
        assert await w._recheck_cancelled_before_settlement(c)
        w._history_rows.assert_not_awaited()
    finally:await cleanup(repo,w)


async def repair_fixture(tmp_path):
    repo=PredictionRepository(tmp_path/'db');await repo.initialize()
    await repo._execute('ALTER TABLE prediction_order_intents ADD COLUMN tier TEXT')
    await repo._execute('ALTER TABLE prediction_order_intents ADD COLUMN client_order_id TEXT')
    await repo.start_loop('loop',100,mode='LIVE',strategy_profile='regime_target6_8a_v1')
    market=MarketInfo('topic','up','market',S,S+300000,up_market_id='up',down_market_id='down',down_token_id='token')
    c=Campaign('campaign',market);await repo.save_campaign(c,loop_id='loop')
    ledger=RegimeLiveLedger(repo,profile='regime_target6_8a_v1')
    await ledger.seed_schedule(loop_id='loop',first_market_start_ms=S)
    await ledger.verify_market(loop_id='loop',market_start_ms=S,market_topic_id='topic',market_id='up',verified_at_ms=S+120000)
    await cancelled(repo,c)
    await repo._execute("INSERT INTO prediction_regime_entry_claims(loop_id,market_start_ms,campaign_id,intent_id,unit_usdt,claimed_at_ms) VALUES('loop',?,'campaign','intent','1',?)",(S,S+124000))
    await repo.set_runtime_config('regime_target6_risk_v1',{'version':1,'fingerprint':__import__('src.gridbot.prediction.regime_lane',fromlist=['FINGERPRINT']).FINGERPRINT,'first_market_start_ms':S,'unit_usdt':'1','halt_reason':None})
    await repo.finalize_settlement(c,dict(status='SETTLED',result='NO_FILL',net_pnl='0',settled_at_ms=S+300000))
    now=int(time.time()*1000)
    proof={'source':'authenticated_official_history','checked_at_ms':now,'order':snapshot('FILLED','1.51'),
           'position':dict(marketId='official-down',marketTopicId='official-topic',tokenId='token',outcomeName='Down',shares='1.51',totalCost='.9773957',realizedPnl='.5335875',positionStatus='CLAIMED',isWinner=True,canClaim=False,finalOutcome='Down',endDate=S+300000)}
    return repo,proof


@pytest.mark.asyncio
async def test_repair_is_atomic_idempotent_and_does_not_increment_run(tmp_path):
    repo,proof=await repair_fixture(tmp_path)
    try:
        before=await repo.get_loop('loop')
        result=await repo.repair_cancelled_fill(proof)
        assert result['status']=='REPAIRED'
        assert (await repo.get_loop('loop'))['completed']==before['completed']==1
        assert D((await repo.get_loop('loop'))['net_pnl'])==D('.5335875')
        assert len(await repo.get_fills('campaign'))==1
        assert (await repo.load_campaign('campaign')).state==CampaignState.DONE
        assert D((await repo.get_runtime_config('prediction_risk_state'))['daily_net_pnl'])>=0
        assert await repo._fetchone('SELECT 1 FROM prediction_regime_settlement_observations')
        assert (await repo.repair_cancelled_fill(proof))['status']=='ALREADY_REPAIRED'
        assert len(await repo.get_fills('campaign'))==1
        assert len(await repo._fetchall("SELECT 1 FROM prediction_risk_events WHERE event_type='LATE_FILL_REPAIRED'"))==1
    finally:await repo.close()


@pytest.mark.asyncio
async def test_repair_failure_rolls_back_fill_settlement_and_risk(tmp_path):
    repo,proof=await repair_fixture(tmp_path)
    try:
        repo.failure_injection='late_fill_before_commit'
        with pytest.raises(RuntimeError):await repo.repair_cancelled_fill(proof)
        assert not await repo.get_fills('campaign')
        assert (await repo.get_order('order'))['status']=='CANCELLED'
        assert D((await repo.get_settlement('campaign'))['net_pnl'])==0
        assert not await repo._fetchall('SELECT 1 FROM prediction_regime_settlement_observations')
        assert (await repo.get_loop('loop'))['completed']==1
    finally:await repo.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('mutation',['token','market','side','stale','not_settled','no_provenance','negative'])
async def test_repair_rejects_ambiguous_or_stale_proof(tmp_path,mutation):
    repo,p=await repair_fixture(tmp_path)
    try:
        if mutation=='token':p['position']['tokenId']='other'
        if mutation=='market':p['position']['marketId']='other'
        if mutation=='side':p['position']['outcomeName']='Up'
        if mutation=='stale':p['checked_at_ms']-=180000
        if mutation=='not_settled':p['position']['positionStatus']='OPEN'
        if mutation=='no_provenance':p['source']='paper'
        if mutation=='negative':p['order']['filledShareQty']='-1'
        with pytest.raises(ValueError):await repo.repair_cancelled_fill(p)
        assert not await repo.get_fills('campaign')
    finally:await repo.close()

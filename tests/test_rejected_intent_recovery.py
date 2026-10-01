"""A known rejected BUY must not strand an empty, expired Live campaign."""
from decimal import Decimal
from unittest.mock import AsyncMock, Mock

import pytest

from src.gridbot.prediction.models import CampaignState, Fill, OrderSide, OutcomeSide
from src.gridbot.prediction.regime_live_ledger import RISK_PROFILES
from src.gridbot.prediction.settings import RuntimeMode
from src.gridbot.prediction.worker import PredictionWorker
from test_loop_cancel_recovery import cleanup, setup_worker
from test_t63 import S


async def rejected_buy(repo, *, unknown=False):
    await repo._execute(
        "INSERT INTO prediction_order_intents "
        "(intent_id,campaign_id,action,outcome,order_side,amount,limit_price,"
        "created_at_ms,ttl_ms,status,unknown,payload_json) "
        "VALUES('rejected','held','BUY_INITIAL','UP','BUY','1','.75',?,2000,"
        "'REJECTED',?,?)",
        (S+129000, int(unknown), '{"not_submitted":true,"reason":"post_claim_admission_rejected"}'),
    )


def settlement_path(repo, worker):
    worker._effective_mode = RuntimeMode.LIVE
    worker._call_api = AsyncMock(return_value=[])
    worker._official_resolution = Mock(return_value=None)

    async def finalize(campaign, settlement):
        return await repo.finalize_settlement(campaign, settlement)

    worker._finalize_settlement_with_c180 = finalize
    worker.settle_campaign = PredictionWorker.settle_campaign.__get__(worker)


@pytest.mark.asyncio
@pytest.mark.parametrize('profile', RISK_PROFILES)
@pytest.mark.parametrize('prior_pending', [False, True])
async def test_empty_rejected_buy_finalizes_once_and_advances_original_loop(tmp_path, profile, prior_pending):
    repo, worker, campaign, clock, original = await setup_worker(
        tmp_path, shares='0', state=CampaignState.OBSERVE)
    settlement_path(repo, worker)
    worker._selected_strategy_profile = profile
    clock[0] = S+18000000  # Hours after the expired market, as on the VM.
    try:
        await repo._execute("UPDATE prediction_loops SET strategy_profile=?", (profile,))
        campaign.order_attempts = campaign.initial_attempts = 1
        await repo.save_campaign(campaign)
        await rejected_buy(repo)
        if prior_pending:
            await repo.finalize_settlement(campaign, {
                'status': 'CLOSED_PENDING_REDEEM', 'net_pnl': '0', 'winner': None})
        assert not await repo.load_unresolved_intents()

        result = await worker.settle_campaign(campaign)
        assert result['status'] == 'SETTLED' and result['result'] == 'NO_FILL'
        assert Decimal(result['net_pnl']) == 0
        assert campaign.state == CampaignState.DONE
        # Repeating after restart cannot increment progress or book PnL twice.
        await worker.settle_campaign(await repo.load_campaign(campaign.campaign_id))
        loop = await repo.get_loop(original['loop_id'])
        assert loop['completed'] == 1 and loop['target'] == 100
        assert loop['state'] == 'RUNNING' and Decimal(loop['net_pnl']) == 0
        assert len(await repo._fetchall('SELECT loop_id FROM prediction_loops')) == 1
        intent = await repo.get_intent('rejected')
        assert intent['status'] == 'REJECTED' and intent['submission_at_ms'] is None
        worker._call_api.assert_not_awaited()
        worker.client.place_order.assert_not_awaited()
    finally:
        await cleanup(repo, worker)


@pytest.mark.asyncio
@pytest.mark.parametrize('protection', ['unknown', 'fill', 'position'])
async def test_rejected_intent_never_turns_real_or_unknown_exposure_into_no_fill(tmp_path, protection):
    repo, worker, campaign, clock, original = await setup_worker(
        tmp_path, shares='1' if protection == 'position' else '0', state=CampaignState.OBSERVE)
    settlement_path(repo, worker)
    clock[0] = S+310000
    try:
        await rejected_buy(repo, unknown=protection == 'unknown')
        if protection == 'fill':
            await repo.record_fill(Fill('buy', 'up', OrderSide.BUY, OutcomeSide.UP,
                Decimal(1), Decimal('.5'), Decimal('.5'), event_time_ms=S+130000),
                campaign_id=campaign.campaign_id)
        unresolved = await repo.load_unresolved_intents()
        assert bool(unresolved) == (protection == 'unknown')
        result = await worker.settle_campaign(campaign)
        assert result['status'] != 'SETTLED' and result.get('result') != 'NO_FILL'
        assert campaign.state != CampaignState.DONE
        assert (await repo.get_loop(original['loop_id']))['completed'] == 0
        worker.client.place_order.assert_not_awaited()
    finally:
        await cleanup(repo, worker)

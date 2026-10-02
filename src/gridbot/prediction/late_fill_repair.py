"""Explicit, atomic recovery of an officially settled BUY missed after cancel.

The operator supplies fresh authenticated order/position evidence. This module
has no network, redemption, Live activation or order-submission capability.
"""
from __future__ import annotations

import hashlib
import json
import time
from decimal import Decimal

from .models import CampaignState, OutcomeSide
from .repository import _json_dumps


def number(value):
    value = Decimal(str(value))
    if not value.is_finite():
        raise ValueError('non-finite official amount')
    return value


async def repair_transaction(repo, evidence):
    order, position = evidence['order'], evidence['position']
    if evidence.get('source') != 'authenticated_official_history':
        raise ValueError('official provenance required')
    now = int(time.time()*1000)
    if not 0 <= now-int(evidence['checked_at_ms']) <= 120000:
        raise ValueError('official evidence is stale')
    oid = str(order['orderId'])
    if str(order['status']).upper() not in ('FILLED', 'CLOSED') or str(order['side']).upper() != 'BUY':
        raise ValueError('confirmed BUY required')
    shares, gross = number(order['filledShareQty']), number(order['filledUsdtAmount'])
    fees = sum((number(order.get(k) or 0) for k in ('marketProviderFee','networkFee')), Decimal(0))
    net = number(position['realizedPnl'])
    if shares <= 0 or gross <= 0 or fees < 0 or position.get('isWinner') is not True:
        raise ValueError('this recovery requires a positive, settled winning BUY')
    if (str(position.get('positionStatus')).upper() not in ('CLAIMED','REDEEMED','SETTLED')
            or position.get('canClaim') is not False
            or str(position.get('finalOutcome')).upper() != str(order['outcome']).upper()):
        raise ValueError('official position is not settled')
    for key in ('marketId','marketTopicId'):
        if str(order[key]) != str(position[key]):
            raise ValueError('order and position identities differ')
    if str(order['outcome']).upper() != str(position['outcomeName']).upper():
        raise ValueError('order and position outcomes differ')
    if number(position['shares']) <= 0 or number(position['shares']) > shares:
        raise ValueError('official position quantity differs')
    # Official fee-net amount is authoritative; require coherent winning payout.
    cost = number(position['totalCost'])
    if cost <= 0 or abs(number(position['shares'])-cost-net) > Decimal('.02'):
        raise ValueError('official payout is inconsistent')
    identity = {'order_id':oid,'shares':str(shares),'gross':str(gross),'fees':str(fees),
                'net_pnl':str(net),'token':str(position['tokenId'])}
    repair_id = hashlib.sha256(_json_dumps(identity).encode()).hexdigest()
    conn = repo._require_conn()
    await repo._begin(conn)
    try:
        row = await repo._tx_fetchone(conn,'SELECT * FROM prediction_orders WHERE order_id=?',(oid,))
        if row is None:
            raise ValueError('local order missing')
        local = dict(row);cid=local['campaign_id']
        old_order = json.loads(local['payload_json'])
        for key in ('orderId','marketId','marketTopicId','side','outcome'):
            if str(old_order[key]).upper() != str(order[key]).upper():
                raise ValueError('local/official order identity differs')
        if str(local['token_id']) != str(position['tokenId']):
            raise ValueError('exact local token identity differs')
        settlement = await repo._tx_fetchone(conn,'SELECT * FROM prediction_settlements WHERE campaign_id=?',(cid,))
        campaign = await repo.load_campaign(cid)
        if settlement is None or campaign is None:
            raise ValueError('closed local campaign/settlement required')
        settlement = dict(settlement);old_payload=json.loads(settlement['payload_json'])
        if (int(position['endDate']) != campaign.market.end_time_ms
                or not campaign.market.start_time_ms <= int(order['createTime']) < campaign.market.end_time_ms):
            raise ValueError('official position end time differs')
        if old_payload.get('late_fill_repair_id') == repair_id:
            if number(settlement['net_pnl']) != net or number(local['filled_shares']) != shares:
                raise ValueError('repair evidence diverged after commit')
            await conn.rollback()
            return {'status':'ALREADY_REPAIRED','campaign_id':cid,'net_pnl':str(net)}
        if (campaign.state != CampaignState.DONE or campaign.pending_unknown
                or campaign.pending_intent_id or now < campaign.market.end_time_ms
                or settlement['status'] != 'SETTLED' or number(settlement['net_pnl']) != 0
                or old_payload.get('result') != 'NO_FILL' or number(local['filled_shares']) != 0
                or str(local['status']).upper() not in ('CANCELLED','CANCELED','EXPIRED','FAILED')):
            raise ValueError('only a closed zero-fill cancellation may be corrected')
        if await repo._tx_fetchone(conn,'SELECT 1 FROM prediction_fills WHERE campaign_id=?',(cid,)):
            raise ValueError('campaign already has fills')
        intent = await repo.get_intent(local['intent_id'])
        if not intent or intent['unknown'] or intent['submission_at_ms'] is None:
            raise ValueError('submitted known intent required')
        loop_id = settlement['loop_id']
        loop = await repo.get_loop(loop_id)
        from .regime_live_ledger import RegimeLiveLedger, RISK_PROFILES
        if not loop or loop['mode'] != 'LIVE' or loop['strategy_profile'] not in RISK_PROFILES:
            raise ValueError('verified T6 LIVE loop required')
        claim = await repo._tx_fetchone(conn,'SELECT intent_id FROM prediction_regime_entry_claims WHERE campaign_id=? AND loop_id=?',(cid,loop_id))
        ledger = await repo._tx_fetchone(conn,'SELECT net_pnl FROM prediction_risk_ledger WHERE campaign_id=?',(cid,))
        if not claim or claim[0] != intent['intent_id'] or not ledger or number(ledger[0]) != 0:
            raise ValueError('original claim/zero risk ledger required')
        if await repo._tx_fetchone(conn,'SELECT 1 FROM prediction_regime_settlement_observations WHERE settlement_id=?',(settlement['settlement_id'],)):
            raise ValueError('settlement already observed')
        result = await repo.apply_order_snapshot_atomic(campaign,intent,oid,order,
                            outcome=OutcomeSide(intent['outcome']),_transaction_owned=True)
        candidate=result['campaign'];candidate.state=CampaignState.DONE
        await conn.execute("UPDATE prediction_campaigns SET state='DONE',payload_json=?,updated_at_ms=? WHERE campaign_id=?",(_json_dumps(candidate),now,cid))
        # Retain the original settlement time and loop cursor; record knowledge now.
        payload={**old_payload,'result':'OFFICIAL_SETTLED','winner':intent['outcome'],
                 'net_pnl':str(net),'gross_pnl':str(net+fees),'realized_pnl':str(net),
                 'fees':str(fees),'late_fill_repair_id':repair_id,
                 'late_fill_evidence':evidence,'prior_no_fill':settlement}
        await conn.execute('UPDATE prediction_settlements SET winner=?,net_pnl=?,realized_pnl=?,gross_pnl=?,fees=?,payload_json=? WHERE settlement_id=?',
                           (intent['outcome'],str(net),str(net),str(net+fees),str(fees),_json_dumps(payload),settlement['settlement_id']))
        await conn.execute('UPDATE prediction_risk_ledger SET net_pnl=? WHERE campaign_id=?',(str(net),cid))
        await conn.execute('INSERT INTO prediction_regime_settlement_observations(settlement_id,campaign_id,net_pnl,known_at_ms) VALUES(?,?,?,?)',
                           (settlement['settlement_id'],cid,str(net),now))
        await repo._refresh_risk_aggregate_tx(conn,now_ms=now,loop_id=loop_id)
        risk = RegimeLiveLedger(repo,profile=loop['strategy_profile'])
        await risk._risk_conn(conn,loop_id,now//300000*300000,now)
        repo._maybe_fail('late_fill_before_commit')
        await conn.execute("INSERT INTO prediction_risk_events(campaign_id,event_time_ms,event_type,severity,message,payload_json) VALUES(?,?,'LATE_FILL_REPAIRED','WARNING','Official fill corrected prior NO_FILL',?)",(cid,now,_json_dumps({'repair_id':repair_id,'order_id':oid,'net_pnl':str(net),'old_status':local['status']})))
        await conn.commit()
        return {'status':'REPAIRED','campaign_id':cid,'net_pnl':str(net),'repair_id':repair_id}
    except BaseException:
        await conn.rollback()
        raise

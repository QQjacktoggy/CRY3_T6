"""Attribute verified Live fills and official PnL, without paper returns."""
import json
import sqlite3
from contextlib import closing

from .regime_t67_policy import BRANCHES


def branch_metrics(root, campaigns, current_ids, fill_ids, events, *, fingerprint, slots):
    from .live_report import _metrics
    result = {b: dict(fills=0, pending=0, events=[]) for b in BRANCHES}
    result['unattributed'] = 0
    path = root/'prediction/data/regime-target6/features.sqlite3'
    with closing(sqlite3.connect(path.resolve().as_uri()+'?mode=ro', uri=True)) as db:
        decisions = {int(r[0]): json.loads(r[1]) for r in db.execute('SELECT start,payload FROM t67_decisions')}
    by_cid = {e['cid']: e for e in events}
    admissions = {(str(s['loop_id']), int(s['market_start_ms'])): s for s in slots}
    for cid in fill_ids & current_ids:
        campaign = campaigns[cid]
        d = decisions.get(int(campaign['start_time_ms']), {})
        branch = d.get('branch')
        start = int(campaign['start_time_ms'])
        slot = admissions.get((str(campaign['loop_id']), start), {})
        up_id = str(slot.get('market_id') or '')
        if (d.get('fingerprint') != fingerprint or not d.get('selected') or branch not in BRANCHES
                or str(d.get('market_topic')) != str(campaign.get('market_topic_id'))
                or slot.get('verified_at_ms') is None or not up_id
                or str(slot.get('market_topic_id')) != str(d.get('market_topic'))
                or str(d.get('market_id')) != up_id
                or d.get('market_start_ms') != start or d.get('market_end_ms') != start+300000
                or campaign.get('end_time_ms', start+300000) != start+300000
                or (campaign.get('market_id') and str(campaign['market_id']) != up_id)):
            result['unattributed'] += 1
            continue
        result[branch]['fills'] += 1
        if cid in by_cid:
            result[branch]['events'].append(by_cid[cid])
        else:
            result[branch]['pending'] += 1
    for branch in BRANCHES:
        result[branch].update(_metrics(result[branch].pop('events')))
    return result

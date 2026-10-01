"""Attribute verified Live fills and official PnL, without paper returns."""
import json
import sqlite3
from contextlib import closing

from .regime_t67_policy import BRANCHES


def branch_metrics(root, campaigns, current_ids, fill_ids, events, *, fingerprint):
    from .live_report import _metrics
    result = {b: dict(fills=0, pending=0, events=[]) for b in BRANCHES}
    result['unattributed'] = 0
    path = root/'prediction/data/regime-target6/features.sqlite3'
    with closing(sqlite3.connect(path.resolve().as_uri()+'?mode=ro', uri=True)) as db:
        decisions = {int(r[0]): json.loads(r[1]) for r in db.execute('SELECT start,payload FROM t67_decisions')}
    by_cid = {e['cid']: e for e in events}
    for cid in fill_ids & current_ids:
        campaign = campaigns[cid]
        d = decisions.get(int(campaign['start_time_ms']), {})
        branch = d.get('branch')
        if (d.get('fingerprint') != fingerprint or not d.get('selected') or branch not in BRANCHES
                or str(d.get('market_topic')) != str(campaign.get('market_topic_id'))
                or str(d.get('market_id')) != str(campaign.get('market_id'))):
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

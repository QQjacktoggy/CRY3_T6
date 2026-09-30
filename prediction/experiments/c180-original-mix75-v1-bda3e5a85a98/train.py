"""Train frozen C candidates only from recorded contemporaneous v2 evidence."""
import argparse
import json
import pathlib
import sqlite3
from calibration import fit, calibrated, temporal_split, probability_metrics
from features import SCHEMA, NUMERIC, META
from logic import digest, dumps, DEFINITION
from store import now


def dataset(db):
    identity = db.execute("SELECT value FROM meta WHERE key='identity'").fetchone()
    if not identity or json.loads(identity[0])['definition'] != DEFINITION:
        raise ValueError('dataset experiment definition mismatch')
    packets = {r[0]: json.loads(r[1]) for r in db.execute('SELECT topic,body FROM packets')}
    results = {r[0]: (r[1], r[2]) for r in db.execute('SELECT topic,outcome,known_at FROM resolutions')}
    rows = []
    for (body,) in db.execute('SELECT body FROM decisions'):
        d = json.loads(body); p = packets.get(d['id'])
        if not p or d.get('status') != 'complete' or d.get('missed') or d.get('meta') is None:
            continue
        r = results.get(p['market']['topic'])
        if not r or r[0] == 'TIE' or r[1] > now() or r[1] < p['market']['end']:
            continue
        if p['state']['evidence_v2']['schema'] != SCHEMA:
            raise ValueError('feature schema mismatch')
        call = d['calls']['Meta']
        if call['status'] != 'ok' or not p['cutoff'] <= call['started'] <= call['completed'] <= p['cutoff']+3000:
            continue
        if d['numeric']['market_up'] is None:
            continue
        rows.append({'start': d['start'], 'phase': d['phase'], 'cutoff': p['cutoff'],
                     'known_at': r[1], 'label': int(r[0] == 'UP'),
                     'features': {**d['numeric'], **d['meta']}})
    return rows


def train(rows):
    artifact = {'version': 1, 'schema': SCHEMA, 'created_at': now(), 'trained_through': 0,
                'dataset_hash': digest(sorted(rows, key=lambda r: (r['cutoff'], r['phase']))),
                'status': 'research_candidate_not_proven_profitable', 'phases': {}}
    for phase in ('E10', 'C180', 'C90'):
        phase_rows = [r for r in rows if r['phase'] == phase]
        train_rows, cal, test = temporal_split(phase_rows)
        pair = {}
        for lane, names in (('numeric', NUMERIC), ('jev', NUMERIC+META)):
            pair[lane] = fit(train_rows, cal, names)
            pair[lane+'_test'] = probability_metrics([(calibrated(r['features'], pair[lane]), r['label']) for r in test])
        pair['market_test'] = probability_metrics([(r['features']['market_up'], r['label']) for r in test])
        pair['split'] = {name: {'n': len(part), 'first_cutoff': part[0]['cutoff'], 'last_cutoff': part[-1]['cutoff'],
                              'last_known_at': max(r['known_at'] for r in part)}
                         for name, part in (('train', train_rows), ('calibration', cal), ('test', test))}
        pair['jev_probability_increment_pass'] = all(pair['jev_test'][k] < pair['numeric_test'][k] for k in ('brier', 'log_loss'))
        pair['purged_late_labels'] = len(phase_rows)-len(train_rows)-len(cal)-len(test)
        artifact['phases'][phase] = pair
        artifact['trained_through'] = max(artifact['trained_through'], max(r['known_at'] for r in test))
    return artifact


if __name__ == '__main__':
    parser = argparse.ArgumentParser(); parser.add_argument('--db', required=True, action='append'); parser.add_argument('--out', required=True)
    parser.add_argument('--dataset-out')
    args = parser.parse_args()
    rows = []
    for path in args.db:
        with sqlite3.connect(pathlib.Path(path).resolve().as_uri()+'?mode=ro', uri=True) as db:
            rows.extend(dataset(db))
    if args.dataset_out:
        with pathlib.Path(args.dataset_out).open('x', encoding='utf-8') as handle:
            for row in sorted(rows, key=lambda r: r['cutoff']):
                handle.write(dumps(row)+'\n')
    artifact = train(rows)
    # Never overwrite a frozen artifact as part of an ongoing cohort.
    with pathlib.Path(args.out).open('x', encoding='utf-8') as handle:
        handle.write(dumps(artifact))
    print(dumps({'rows': len(rows), 'model_hash': digest(artifact), 'status': artifact['status']}))

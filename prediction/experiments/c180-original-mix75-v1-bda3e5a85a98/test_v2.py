import asyncio
import copy
import json
import math
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from calibration import ProbabilityModel, temporal_split, probability_metrics, fit, calibrated
from engine import Engine
from features import build, META_OPTIONS, NUMERIC, META, SCHEMA, sample_seconds
from logic import *
from report import summarize, write_report
from store import Store
from train import train, dataset


def market():
    return {'start': 0, 'end': SLOT, 'reference': 100., 'topic': 'x', 'market_id': '1',
            'yes': 'UP', 'fee_bps': 200., 'identified_at': 0}


def book(at, bid=.49, ask=.51, size=20):
    return {'market_id': '1', 'received_at': at, 'received_at_ms': at, 'book_at_ms': at,
            'bids_levels': [[bid, size]], 'asks_levels': [[ask, size]]}


def tape_at(cutoff):
    tape = Tape()
    for i, at in enumerate(range(cutoff-125000, cutoff+1, 1000)):
        price = 100.+.005*math.sin(i)+.00001*i
        for source, offset in (('spot', 0.), ('futures', -.04)):
            tape.ingest({'received_at': at, 'kind': f'binance_{source}_aggTrade',
                         'body': {'s': 'BTCUSDT', 'p': price+offset, 'q': 1, 'T': at, 'a': i, 'm': False}})
    tape.ingest({'received_at': cutoff, 'kind': 'prediction_book', 'body': book(cutoff)})
    return tape


def response(questions, up=.98):
    answers = {}
    for key, spec in questions.items():
        keys = list(spec['criteria'])
        probs = {k: 1/len(keys) for k in keys}
        if key == 'direction':
            probs = {'UP': up, 'DOWN': 1-up}
        answers[key] = {'type': 'choice', 'choice': max(probs, key=probs.get),
                        'probabilities': probs, 'confidence': .12}
    return {'model': MODEL, 'answers': answers}


class FeaturesTests(unittest.TestCase):
    def test_basis_does_not_vote_twice_and_ab_isolation(self):
        p = tape_at(10000).packet(market(), 10000)
        original = model_state(p, 'Original')
        a, b = model_state(p, 'A'), model_state(p, 'B')
        self.assertIn('distance_bps', original['features']['futures']['recent']['5'])
        self.assertNotIn('evidence_v2', original)
        for state in (a, b):
            for row in state['features']['futures']['recent'].values():
                self.assertNotIn('distance_bps', row)
                self.assertNotIn('close', row)
                self.assertNotIn('crossings', row)
        self.assertNotIn('normalized_distance', a['evidence_v2']['numeric'])
        self.assertIn('normalized_distance', b['evidence_v2']['numeric'])
        self.assertLess(b['evidence_v2']['numeric']['basis_bps'], -3)
        self.assertIsNotNone(b['evidence_v2']['numeric']['basis_change_30'])

    def test_no_future_data_and_volatility_coverage(self):
        tape = tape_at(10000)
        p = tape.packet(market(), 10000)
        tape.ingest({'received_at': 11000, 'kind': 'binance_spot_aggTrade',
                     'body': {'s': 'BTCUSDT', 'p': 200, 'q': 1, 'T': 11000, 'a': 999, 'm': False}})
        later = tape.packet(market(), 10000)
        self.assertEqual(p['state']['evidence_v2'], later['state']['evidence_v2'])
        self.assertGreater(p['state']['evidence_v2']['numeric']['sigma_remaining_bps'], 0)
        self.assertIsNone(Tape().packet(market(), 10000)['state']['evidence_v2']['numeric']['normalized_distance'])

    def test_missing_official_never_uses_spot(self):
        p = tape_at(10000).packet(market(), 10000)
        evidence = p['state']['evidence_v2']
        self.assertEqual(evidence['numeric']['official_missing'], 1)
        self.assertIsNone(evidence['official_live']['price'])
        state = copy.deepcopy(p['state'])
        state['official_live'] = {'source': 'chainlink', 'verified_feed': True, 'price': 101,
                                  'source_at': 9000, 'received_at': 9000}
        self.assertEqual(build(state, tape_at(10000).trades)['numeric']['official_missing'], 0)
        state['official_live']['source_at'] = 12000
        self.assertEqual(build(state, tape_at(10000).trades)['numeric']['official_missing'], 1)

    def test_probability_not_confidence_and_validation(self):
        r = response(QUESTIONS)
        self.assertEqual(parse_response(r)['direction']['probabilities']['UP'], .98)
        for value in (True, float('nan'), 2., '0.8'):
            bad = copy.deepcopy(r); bad['answers']['direction']['probabilities']['UP'] = value
            with self.assertRaises(ValueError): parse_response(bad)
        with self.assertRaises(ValueError): parse_response(r, META_QUESTIONS)

    def test_no_fictional_mid_with_empty_or_stale_book(self):
        tape = tape_at(10000)
        tape.books['1']['asks_levels'] = []
        self.assertIsNone(tape.packet(market(), 10000)['state']['evidence_v2']['numeric']['market_up'])
        self.assertIsNone(tape.packet(market(), 20000)['state']['evidence_v2']['numeric']['market_up'])


class ModelTests(unittest.TestCase):
    def test_cold_start_identical_and_missing_market(self):
        numeric = tape_at(10000).packet(market(), 10000)['state']['evidence_v2']['numeric']
        p = ProbabilityModel().probabilities('E10', 10000, numeric, {k: 1. for k in META})
        self.assertEqual(p['numeric'], p['jev']); self.assertIn('collect_only', p['status'])
        numeric['market_up'] = None
        self.assertIsNone(ProbabilityModel().probabilities('E10', 10000, numeric)['numeric'])

    def test_split_rejects_duplicates_late_labels_and_insufficient_data(self):
        rows = [{'start': i*300000, 'cutoff': i*300000+10000, 'known_at': i*300000+301000, 'label': i%2} for i in range(200)]
        train, cal, test = temporal_split(rows)
        self.assertEqual([len(x) for x in (train, cal, test)], [120, 40, 40])
        for broken in (rows[:-1], rows+[rows[0]]):
            with self.assertRaises(ValueError): temporal_split(broken)
        rows[119]['known_at'] = rows[120]['cutoff']+1
        with self.assertRaises(ValueError): temporal_split(rows)

    def test_brier_and_confident_errors(self):
        m = probability_metrics([(.9, 0), (.8, 1)])
        self.assertAlmostEqual(m['brier'], .425)
        self.assertEqual(m['high_confidence_wrong_rate'], 1.)
        self.assertIsNone(probability_metrics([])['brier'])

    def test_automatic_purge_of_normal_late_settlement(self):
        rows = [{'start': i*SLOT, 'cutoff': i*SLOT+10000, 'known_at': i*SLOT+315000,
                 'label': i%2} for i in range(210)]
        train_rows, cal, test = temporal_split(rows)
        self.assertEqual(sum(map(len, (train_rows, cal, test))), 208)
        self.assertLess(max(r['known_at'] for r in train_rows), min(r['cutoff'] for r in cal))
        self.assertLess(max(r['known_at'] for r in cal), min(r['cutoff'] for r in test))

    def test_fit_freeze_predict_and_future_artifact_rejected(self):
        rows = [{'features': {k: (i%2 if k == 'market_up' else 0.) for k in NUMERIC+META}, 'label': i%2} for i in range(12)]
        n, j = fit(rows[:8], rows[8:], NUMERIC), fit(rows[:8], rows[8:], NUMERIC+META)
        self.assertLess(calibrated(rows[0]['features'], n), calibrated(rows[1]['features'], n))
        artifact = {'schema': SCHEMA, 'version': 1, 'created_at': 100, 'trained_through': 90,
                    'phases': {'E10': {'numeric': n, 'jev': j}}}
        model = ProbabilityModel(artifact)
        with self.assertRaises(ValueError): model.probabilities('E10', 99, rows[1]['features'])
        p = model.probabilities('E10', 101, rows[1]['features'])
        self.assertEqual(p['numeric'], p['jev']); self.assertIn('fallback', p['status'])

    def test_training_whole_pipeline_on_synthetic_time_split(self):
        # Pipeline verification only, not evidence of predictive performance.
        rows = []
        for i in range(200):
            for phase, offset in CONFIG['phases']:
                features = {k: 0. for k in NUMERIC+META}
                features['market_up'] = .55 if i%2 else .45
                rows.append({'start': i*SLOT, 'phase': phase, 'cutoff': i*SLOT+offset,
                             'known_at': i*SLOT+301000, 'label': i%2, 'features': features})
        result = train(rows)
        self.assertEqual(set(result['phases']), {'E10', 'C180', 'C90'})
        self.assertEqual(result['phases']['E10']['split']['test']['n'], 40)
        model = ProbabilityModel(result)
        pred = model.probabilities('E10', result['created_at']+1, rows[-1]['features'], {k: 0. for k in META})
        self.assertTrue(0 < pred['jev'] < 1)


class ExecutionTests(unittest.TestCase):
    def test_expensive_win_rejected_and_positive_ev(self):
        q = normalize_book(book(0, .98, .99), market(), 0)
        self.assertIsNone(entry_ev(.999, q, 200)[0])
        q = normalize_book(book(0, .39, .40), market(), 0)
        self.assertEqual(entry_ev(.7, q, 200)[0], 'UP')
        self.assertIsNone(entry_ev(.7, q, None)[0])

    def test_execution_rechecks_ev_and_original_value(self):
        with tempfile.TemporaryDirectory() as directory:
            store = Store(Path(directory)/'db', 'test', 5, 'x'); engine = Engine(store)
            packet = engine.freeze(tape_at(10000).packet(market(), 10000), 'E10')
            o = engine.order(packet, 'UP', 'entry', 'NUMERIC', 1, .7, True)
            advance(o, book(13000, .79, .8), market(), 13000); self.assertEqual(o['status'], 'open')
            advance(o, book(14000, .79, .8), market(), 14000); self.assertEqual(o['status'], 'value_gone')
            o = engine.order(packet, 'UP', 'exit', 'ORIGINAL_VALUE', 2, .7)
            o['original_value_guard'] = True
            advance(o, book(14000, .49, .5), market(), 14000); self.assertEqual(o['status'], 'value_gone')
            self.assertEqual(o['account'], 'ORIGINAL_VALUE'); store.db.close()

    def test_missing_observations_are_unknown(self):
        o = {'status': 'open', 'ready': 1000, 'expires': 13000, 'observations': []}
        advance(o, None, market(), 13001)
        self.assertEqual(o['status'], 'unknown')

    def test_partial_depth_value_exit_requires_full_snapshot(self):
        holding = {'sale_net': .8, 'sale_qty': 1, 'shares': 2}
        self.assertFalse(value_exit(.2, holding))
        holding['sale_qty'] = 2
        self.assertTrue(value_exit(.2, holding))


class IntegrationTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.store = Store(Path(self.directory.name)/'db', 'test', 5, 'x')
        self.store.set('start', 0); self.store.set('target', 1)
        self.engine = Engine(self.store)

    def tearDown(self):
        self.store.db.close(); self.directory.cleanup()

    def decide(self, cutoff, phase, fail=False):
        async def infer(session, key, state, questions):
            return {'status': 'http_error' if fail else 'ok', 'cost': .01, 'started': cutoff,
                    'completed': cutoff+100, 'response': dumps(response(questions))}
        self.store.infer = infer
        packet = self.engine.freeze(tape_at(cutoff).packet(market(), cutoff), phase)
        with patch('engine.now', return_value=cutoff+200):
            asyncio.run(self.engine.decide(packet, None, None))
        return packet

    def test_end_to_end_c180_is_independent_costs_and_report(self):
        self.decide(10000, 'E10')
        self.assertEqual(set(self.engine.orders), {'0:E10'})  # cold market EV skips both
        entry = self.engine.entry(0)
        advance(entry, book(14000), market(), 14000)
        self.store.put('orders', entry['id'], entry)
        self.decide(120000, 'C180')
        x = self.engine.orders['0:C180:C180_OBSERVE']
        advance(x, book(124000, .6, .61), market(), 124000)
        self.store.put('orders', x['id'], x)
        p = self.decide(210000, 'C90')
        self.assertAlmostEqual(p['state']['holdings']['E10']['shares'], entry['shares'])
        self.engine.resolve('x', 'UP', 301000)
        report = summarize(self.store, {'asof_ms': 302000, 'phase': 'complete'})
        self.assertAlmostEqual(report['accounts']['JEV_FEATURE_VALUE']['model_cost'], .02)
        self.assertAlmostEqual(report['accounts']['NUMERIC_VALUE']['model_cost'], .01)
        self.assertEqual(report['accounts']['E10_HOLD']['WR'], 1)
        self.assertEqual(report['accounts']['NUMERIC_ENTRY_VALUE']['statuses'], {'skip': 1})
        self.assertEqual(report['pairs']['JEV_ENTRY_VALUE minus NUMERIC_ENTRY_VALUE']['n'], 1)
        self.assertAlmostEqual(report['pairs']['JEV_ENTRY_VALUE minus NUMERIC_ENTRY_VALUE']['after_cost_delta'], -.02)
        self.assertEqual(report['probability_quality']['E10:Original']['n'], 1)
        write_report(self.store, Path(self.directory.name), {'asof_ms': 302000})
        self.assertTrue((Path(self.directory.name)/'report.json').exists())

    def test_failed_api_is_recorded_no_model_probability(self):
        self.decide(10000, 'E10', fail=True)
        d = self.store.rows('decisions')[0]
        self.assertIsNone(d['probabilities']['Original'])
        self.assertEqual(d['probabilities']['Numeric'], d['probabilities']['Jev'])
        self.assertEqual(d['origin'], 'FALLBACK')

    def test_recovery_does_not_recall_provider_and_order_unknown(self):
        packet = self.engine.freeze(tape_at(10000).packet(market(), 10000), 'E10')
        async def forbidden(*args): raise AssertionError('API recall')
        self.store.infer = forbidden
        with patch('engine.now', return_value=20000): asyncio.run(self.engine.decide(packet, None, None, recover=True))
        self.assertEqual(self.engine.entry(0)['status'], 'unknown')
        self.assertTrue(self.store.rows('decisions')[0]['missed'])

    def test_cohort_model_hash_cannot_change(self):
        self.store.set('model_hash', 'different')
        with self.assertRaises(ValueError): Engine(self.store)

    def test_budget_reservation_blocks_parallel_overrun(self):
        self.store.budget = .05
        async def fake(session, key, h, state, questions):
            await asyncio.sleep(0)
            return {'status': 'ok', 'cost': .05}
        self.store._request = fake
        async def run():
            return await asyncio.gather(*(self.store.infer(None, None, {'observed_at': 0, 'x': i}, QUESTIONS) for i in range(4)))
        results = asyncio.run(run())
        self.assertEqual(sum(r['status'] == 'budget_stop' for r in results), 3)
        request = json.loads(self.store.db.execute('SELECT state FROM calls').fetchone()[0])
        self.assertEqual(request['questions'], QUESTIONS)

    def test_training_export_rejects_posthoc_and_ties(self):
        self.decide(10000, 'E10')
        self.engine.resolve('x', 'UP', 301000)
        rows = dataset(self.store.db)
        self.assertEqual(len(rows), 1)
        d = self.store.rows('decisions')[0]
        d['calls']['Meta']['started'] = 301000
        self.store.put('decisions', d['id'], d)
        self.assertEqual(dataset(self.store.db), [])

    def test_trained_model_creates_paired_entry_orders(self):
        class FixedModel:
            hash = 'untrained'
            def probabilities(self, *args):
                return {'numeric': .8, 'jev': .85, 'status': 'test_fixture'}
        self.engine.model = FixedModel()
        self.decide(10000, 'E10')
        self.assertEqual(set(self.engine.orders), {'0:E10', '0:NUMERIC', '0:JEV'})
        for order in self.engine.orders.values():
            self.assertEqual(order['ready'], 14000)
            advance(order, book(14000), market(), 14000)
            self.store.put('orders', order['id'], order)
        self.decide(210000, 'C90')
        self.engine.resolve('x', 'UP', 301000)
        report = summarize(self.store, {'asof_ms': 302000})
        self.assertEqual(report['accounts']['JEV_ENTRY_VALUE']['known_filled'], 1)
        self.assertAlmostEqual(report['pairs']['JEV_ENTRY_VALUE minus NUMERIC_ENTRY_VALUE']['after_cost_delta'], -.02)


class RuntimeTests(unittest.TestCase):
    def test_scheduler_uses_c180_and_never_l60(self):
        from live import Runner
        self.assertEqual(CONFIG['phases'], [('E10', 10000), ('C180', 120000), ('C90', 210000)])
        with tempfile.TemporaryDirectory() as directory:
            store = Store(Path(directory)/'db', 'test', 5, 'x'); store.set('target', 1)
            runner = Runner.__new__(Runner)
            runner.store = store; runner.recovered = True; runner.next_freeze = 0
            runner.start = 0; runner.markets = {0: market()}; runner.tape = tape_at(210000)
            runner.engine = Engine(store); runner.frozen = set(); runner.tasks = set()
            runner.session = None; runner.key = None; runner.errors = {}; runner.stop = asyncio.Event()
            async def run():
                runner.freeze_due(210000)
                await asyncio.gather(*runner.tasks)
            asyncio.run(run())
            self.assertEqual({p['phase'] for p in store.rows('packets')}, {'E10', 'C180', 'C90'})
            store.db.close()

    def test_read_only_transport_denies_order_post(self):
        from live import ReadOnlyTransport
        with self.assertRaises(PermissionError):
            ReadOnlyTransport().request('POST', 'https://api.binance.com/sapi/v1/prediction/order')


if __name__ == '__main__':
    unittest.main()

"""Late Reference Live selection shares worker execution and stays immutable."""
import json
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from decimal import Decimal as D
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from src.gridbot.prediction import regime_t69_bridge as live
from src.gridbot.prediction import regime_worker_bridge as b
from src.gridbot.prediction.regime_feature_service import connect
from src.gridbot.prediction.regime_t69_policy import FINGERPRINT, LIVE_BRANCHES, SHADOW_BRANCHES
from test_t63 import S, feature, book
from test_t67 import snap, tape


MARKET = SimpleNamespace(start_time_ms=S, market_topic_id='topic', up_market_id='up')


def build(tmp_path, *, first=None, features=None, initial=True):
    path = tmp_path/'features.sqlite3'
    with closing(connect(path)) as db, db:
        db.execute('INSERT INTO features VALUES(?,?)', (S, json.dumps(features or feature(2, -1, -2))))
    pred = tmp_path/'prediction.sqlite3'
    with sqlite3.connect(pred) as db:
        db.executescript('CREATE TABLE prediction_campaigns(campaign_id TEXT, start_time_ms INTEGER,buy_count INTEGER,pending_unknown INTEGER);'
                         'CREATE TABLE prediction_order_intents(campaign_id TEXT,order_side TEXT,unknown INTEGER,status TEXT);'
                         'CREATE TABLE prediction_regime_entry_claims(loop_id TEXT,market_start_ms INTEGER);')
    obj = b.RegimeWorkerBridge(SimpleNamespace(db_path=pred), tmp_path/'signals.sqlite3', feature_db=path,
                              profile='regime_target6_9_v1')
    obj._registered_loop_id = 'testloop'
    first = first or book('.9', '.1', 124000)
    if initial:
        with patch.object(obj, '_first_book', return_value=first), patch.object(b, 'read_c180_book', return_value=first), patch.object(b, 'read_c180_signal', return_value=None):
            result = obj.check_signal(market=MARKET, unit_usdt=D(1), at_ms=S+124000, last_seen_book_at_ms=0)
        assert result.reason == 't69_wait_reference_checkpoint' or result.allowed
    return obj


def state(obj):
    with sqlite3.connect(obj.feature_db) as db:
        row = db.execute('SELECT payload FROM t69_decisions WHERE start=?', (S,)).fetchone()
        return json.loads(row[0]) if row else None


def late(obj, *, offset=180000, current=None, spots=None, seen=0, unit=D(1)):
    current = current or snap(offset, up='.4', down='.6')
    with patch.object(live, 'read_inputs', return_value=([current], tape(offset) if spots is None else spots)):
        return obj.check_signal(market=MARKET, unit_usdt=unit, at_ms=S+offset, last_seen_book_at_ms=seen)


def test_ninth_live_lane_fresh_reference_180_preserves_core_and_original_risk(tmp_path):
    obj = build(tmp_path)
    before = state(obj)
    result = late(obj)
    assert result.allowed, result.reason
    d = state(obj)
    assert len(LIVE_BRANCHES) == 9 and len(SHADOW_BRANCHES) == 5
    assert d['branch'] == 'reference_180_mid'
    assert d['core_guard'] == before['core_guard']
    assert result.signal.cutoff_ms == S+180000
    assert result.signal.completed_at_ms == S+180000
    assert result.signal.original_input_sha256 == FINGERPRINT
    assert result.signal.original_p_up == D(d['probability'])
    assert result.execution.expires_at_ms == S+182000
    assert D('.4') <= D(d['effective_cost']) < D('.65')
    assert d['reference_attempt']['status'] == 'SELECTED'
    with sqlite3.connect(obj.repository.db_path) as db:
        assert not db.execute('SELECT * FROM prediction_order_intents').fetchall()
        assert not db.execute('SELECT * FROM prediction_regime_entry_claims').fetchall()


def test_live_admission_does_not_depend_on_observer_checkpoint_record(tmp_path):
    obj = build(tmp_path)
    with sqlite3.connect(obj.feature_db) as db:
        assert not db.execute("SELECT name FROM sqlite_master WHERE name='t69_reference_checkpoints'").fetchone()
    assert late(obj).allowed


@pytest.mark.parametrize('offset,allowed', [(179999, False), (180000, True), (181500, True), (181501, False), (183500, False)])
def test_selection_and_fixed_expiry_windows(tmp_path, offset, allowed):
    obj = build(tmp_path)
    result = late(obj, offset=offset)
    assert result.allowed is allowed
    if allowed:
        assert result.execution.expires_at_ms == S+offset+2000


@pytest.mark.parametrize('first,features', [(book('.3', '.7', 124000), feature(2, -1, 5)),
                                          (book('.6', '.4', 124000), feature(2, -1, -2))])
def test_previous_core_or_addition_selection_never_promotes_to_reference(tmp_path, first, features):
    obj = build(tmp_path, first=first, features=features)
    before = state(obj)
    assert before['selected']
    result = late(obj)
    assert not result.allowed and result.reason == 't69_selected_quote_expired'
    assert state(obj) == before


@pytest.mark.parametrize('bad', ['missing_core', 'not_empty', 'unknown_core', 'missing_features',
                                'late_freeze', 'bad_base_fingerprint', 'fee', 'missing_book',
                                'wrong_topic', 'wrong_up', 'wrong_start', 'bad_model', 'bad_cost',
                                'thin_opposite', 'claim_otherloop', 'intent_rejected', 'position', 'unknown'])
def test_first_late_denial_is_sticky_never_reselects_after_later_success(tmp_path, bad):
    obj = build(tmp_path, initial=bad != 'missing_core')
    current = snap(180000, up='.4', down='.6')
    spots = tape(180000)
    if bad in ('not_empty', 'unknown_core', 'missing_features', 'late_freeze', 'bad_base_fingerprint'):
        d = state(obj)
        if bad == 'not_empty': d['core_guard']['empty'] = False
        elif bad == 'unknown_core': d['core_guard']['verified'] = False
        elif bad == 'missing_features': d['core_guard'].pop('features')
        elif bad == 'late_freeze': d['core_guard']['frozen_at_ms'] = S+126001
        else: d['core_guard']['features']['fingerprint'] = 'wrong'
        with sqlite3.connect(obj.feature_db) as db:
            db.execute('UPDATE t69_decisions SET payload=?', (json.dumps(d),))
    elif bad == 'fee': current['fee_bps'] = '300'
    elif bad == 'wrong_topic': current['market_topic'] = 'other'
    elif bad == 'wrong_up': current['market_id'] = 'other'
    elif bad == 'wrong_start': current['market_start_ms'] += 300000
    elif bad == 'bad_model': spots = []
    elif bad == 'bad_cost': current = snap(180000, up='.3', down='.7')
    elif bad == 'thin_opposite': current['quote']['DOWN']['ask_levels'] = [['.6', '.01']]
    elif bad in ('claim_otherloop', 'intent_rejected', 'position', 'unknown'):
        with sqlite3.connect(obj.repository.db_path) as db:
            if bad == 'claim_otherloop': db.execute('INSERT INTO prediction_regime_entry_claims VALUES(?,?)', ('otherloop', S))
            else:
                db.execute('INSERT INTO prediction_campaigns VALUES(?,?,?,?)', ('other', S, int(bad == 'position'), int(bad == 'unknown')))
                if bad == 'intent_rejected': db.execute('INSERT INTO prediction_order_intents VALUES(?,?,?,?)', ('other', 'BUY', 0, 'REJECTED'))
    if bad == 'missing_book':
        with patch.object(live, 'read_inputs', return_value=([], spots)):
            denied = obj.check_signal(market=MARKET, unit_usdt=D(1), at_ms=S+180000, last_seen_book_at_ms=0)
    else:
        denied = late(obj, current=current, spots=spots)
    assert not denied.allowed, denied.reason
    assert 't69_reference_denied:' in denied.reason
    before = state(obj)
    assert before['reference_attempt']['status'] == 'DENIED'
    retry = late(obj, offset=180500)
    assert not retry.allowed and retry.reason == denied.reason
    assert state(obj) == before


def test_selected_live_reference_refresh_uses_public_book_frozen_probability_and_own_claim(tmp_path):
    obj = build(tmp_path)
    first = late(obj)
    assert first.allowed
    before = state(obj)
    with sqlite3.connect(obj.repository.db_path) as db:
        db.execute('INSERT INTO prediction_regime_entry_claims VALUES(?,?)', ('testloop', S))
        db.execute('INSERT INTO prediction_campaigns VALUES(?,?,?,?)', ('own', S, 0, 0))
        db.execute('INSERT INTO prediction_order_intents VALUES(?,?,?,?)', ('own', 'BUY', 0, 'PLANNED'))
    # Refresh needs no new probability fit: a changed spot direction cannot
    # cause a side reselection. Its own common-worker claim is permitted.
    with patch.object(live, 'evaluate_checkpoint', side_effect=AssertionError('refit')):
        refreshed = late(obj, offset=180500, current=snap(180500, up='.41', down='.59'), spots=[])
    assert refreshed.allowed and refreshed.signal == first.signal
    assert refreshed.execution.expires_at_ms == S+182000
    assert state(obj) == before


def test_selected_success_is_readonly_even_under_feature_writer_lock(tmp_path):
    obj = build(tmp_path)
    assert late(obj).allowed
    with sqlite3.connect(obj.feature_db) as writer:
        writer.execute('BEGIN IMMEDIATE')
        assert late(obj, offset=180100).allowed


def test_restart_retains_exact_side_probability_signal_and_expiry(tmp_path):
    obj = build(tmp_path)
    first = late(obj)
    other = b.RegimeWorkerBridge(obj.repository, obj.signal_db, feature_db=obj.feature_db, profile=obj.profile)
    other._registered_loop_id = 'testloop'
    assert late(other, offset=180500).signal == first.signal
    assert late(other, offset=180500).execution.expires_at_ms == S+182000
    assert not late(other, offset=182000).allowed


@pytest.mark.parametrize('bad', ['cost_floor', 'cost_cap', 'up_identity', 'clock', 'depth'])
def test_actual_refresh_denial_latches_and_cannot_retry_or_flip(tmp_path, bad):
    obj = build(tmp_path)
    assert late(obj).allowed
    before = state(obj)
    current = snap(180100, up='.4', down='.6')
    if bad == 'cost_floor': current['quote']['UP']['ask_levels'] = [['.39', '100']]
    elif bad == 'cost_cap': current['quote']['UP']['ask_levels'] = [['.65', '100']]
    elif bad == 'up_identity': current['market_id'] = 'wrong'
    elif bad == 'clock': current['book_at_ms'] -= 1001
    else: current['quote']['DOWN']['ask_levels'] = [['.6']]
    assert not late(obj, offset=180100, current=current).allowed
    after = state(obj)
    assert 'reference_execution_denied' in after
    assert {key: after[key] for key in ('side', 'probability', 'signal', 'unit_usdt', 'fee_bps', 'expires_at_ms')} == {key: before[key] for key in ('side', 'probability', 'signal', 'unit_usdt', 'fee_bps', 'expires_at_ms')}
    assert not late(obj, offset=180500).allowed
    assert state(obj) == after


def test_fresh_quote_wait_is_not_a_rejection_latch(tmp_path):
    obj = build(tmp_path)
    assert late(obj).allowed
    before = state(obj)
    waiting = late(obj, seen=S+180000)
    assert waiting.reason == 'quote_not_new_after_ready'
    assert state(obj) == before
    assert late(obj, offset=180100, seen=S+180000).allowed


def test_concurrent_late_first_selection_has_one_persisted_signal(tmp_path):
    obj = build(tmp_path)
    results = []
    with patch.object(live, 'read_inputs', return_value=([snap(180000, up='.4', down='.6')], tape(180000))):
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(lambda _: obj.check_signal(market=MARKET, unit_usdt=D(1), at_ms=S+180000, last_seen_book_at_ms=0), range(2)))
    assert all(result.allowed for result in results)
    assert results[0].signal == results[1].signal
    with sqlite3.connect(obj.feature_db) as db:
        assert db.execute('SELECT COUNT(*) FROM t69_decisions').fetchone()[0] == 1


def test_missing_ledger_schema_denies_reference_before_selection(tmp_path):
    obj = build(tmp_path)
    with sqlite3.connect(obj.repository.db_path) as db:
        db.execute('DROP TABLE prediction_order_intents')
    result = late(obj)
    assert not result.allowed and 'reference_exposure_snapshot_unverified' in result.reason


def test_wrong_requested_unit_does_not_poison_other_frozen_decision(tmp_path):
    obj = build(tmp_path)
    before = state(obj)
    assert not late(obj, unit=D(2)).allowed
    assert state(obj) == before
    assert late(obj).allowed


@pytest.mark.parametrize('field,value', [('fingerprint', 'wrong'), ('loop_id', 'otherloop'),
                                        ('market_topic', 'wrong'), ('market_id', 'wrong'),
                                        ('unit_usdt', '2'), ('market_end_ms', S+600000),
                                        ('market_start_ms', S+300000), ('fee_bps', 'NaN')])
def test_mismatched_unselected_frozen_row_is_never_overwritten(tmp_path, field, value):
    obj = build(tmp_path)
    d = state(obj)
    d[field] = value
    raw = json.dumps(d, sort_keys=True)
    with sqlite3.connect(obj.feature_db) as db:
        db.execute('UPDATE t69_decisions SET payload=?', (raw,))
    assert not late(obj).allowed
    with sqlite3.connect(obj.feature_db) as db:
        assert db.execute('SELECT payload FROM t69_decisions').fetchone()[0] == raw


@pytest.mark.parametrize('value', [['not-a-decision'], 'scalar', None, True, 1])
@pytest.mark.parametrize('offset', [124500, 180000])
def test_json_scalars_are_fixed_denials_and_never_overwrite_frozen_row(tmp_path, value, offset):
    obj = build(tmp_path)
    raw = json.dumps(value)
    with sqlite3.connect(obj.feature_db) as db:
        db.execute('UPDATE t69_decisions SET payload=?', (raw,))
    if offset >= 180000:
        denied = late(obj, offset=offset)
    else:
        with patch.object(b, 'read_c180_book', return_value=book('.9', '.1', offset)):
            denied = obj.check_signal(market=MARKET, unit_usdt=D(1), at_ms=S+offset, last_seen_book_at_ms=0)
    assert not denied.allowed and 'decision_payload_invalid' in denied.reason
    with sqlite3.connect(obj.feature_db) as db:
        assert db.execute('SELECT payload FROM t69_decisions').fetchone()[0] == raw


def test_malformed_core_guard_is_fixed_denial_without_touching_frozen_row(tmp_path):
    obj = build(tmp_path)
    d = state(obj)
    d['core_guard'] = []
    raw = json.dumps(d)
    with sqlite3.connect(obj.feature_db) as db:
        db.execute('UPDATE t69_decisions SET payload=?', (raw,))
    with patch.object(b, 'read_c180_book', return_value=book('.9', '.1', 124500)):
        denied = obj.check_signal(market=MARKET, unit_usdt=D(1), at_ms=S+124500, last_seen_book_at_ms=0)
    assert not denied.allowed and 'decision_payload_invalid' in denied.reason
    with sqlite3.connect(obj.feature_db) as db:
        assert db.execute('SELECT payload FROM t69_decisions').fetchone()[0] == raw


def test_downward_reference_freezes_up_probability_and_executes_down_ev(tmp_path):
    obj = build(tmp_path)
    result = late(obj, current=snap(180000, up='.6', down='.4'), spots=tape(180000, price='99.9'))
    assert result.allowed, result.reason
    d = state(obj)
    assert result.signal.entry.side == d['side'] == 'DOWN'
    assert result.signal.original_p_up == D(d['probability']) < D('.5')
    assert (1-result.signal.original_p_up)*result.execution.expected_shares-result.execution.expected_cash_usdt >= D('.03')
    # Refresh reads no current model; the same frozen UP probability is
    # inverted exactly once by the DOWN execution scorer.
    refreshed = late(obj, offset=180100, current=snap(180100, up='.59', down='.41'), spots=[])
    assert refreshed.allowed and refreshed.signal == result.signal
    assert (1-result.signal.original_p_up)*refreshed.execution.expected_shares-refreshed.execution.expected_cash_usdt >= D('.005')


def test_fee_adjusted_upper_band_denies_refresh_even_when_ask_within_frozen_cap(tmp_path):
    obj = build(tmp_path)
    selected = late(obj, current=snap(180000, up='.63', down='.37'))
    assert selected.allowed, selected.reason
    assert selected.execution.worst_ask_limit == D('.65')
    assert D(state(obj)['effective_cost']) < D('.65')
    denied = late(obj, offset=180100, current=snap(180100, up='.65', down='.35'))
    assert not denied.allowed and 'reference_cost_band' in denied.reason
    assert state(obj)['reference_execution_denied']['reason'] == 'reference_cost_band'

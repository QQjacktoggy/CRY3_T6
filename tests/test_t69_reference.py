"""Immutable as-of checkpoints and core-protected paper backfill regressions."""
import copy
import json
import sqlite3
from decimal import Decimal as D
from unittest.mock import patch

import pytest

from src.gridbot.prediction import regime_t69_reference as reference
from src.gridbot.prediction.regime_t69_policy import FINGERPRINT, POLICY
from test_t63 import S, feature
from test_t67 import snap, spot, tape


def identity(fee=200):
    return dict(fingerprint=FINGERPRINT, loop_id='loop', market_topic='topic',
                market_id='up', market_start_ms=S, market_end_ms=S+300000,
                end_ms=S+300000, unit_usdt='1', fee_bps=str(fee) if fee is not None else None)


def empty_core():
    return dict(identity(), selected=False, core_guard=dict(
        verified=True, empty=True, candidates=[], frozen_at_ms=S+124000,
        initial_captured_at_ms=S+124000, initial_book_at_ms=S+124000,
        fee_bps='200', features=feature(2, -1, -2)))


def clear_exposure(at=180000):
    return dict(identity(), observed_at_ms=S+at, has_claim=False, has_intent=False,
                has_position=False, has_unknown=False)


def connect(tmp_path):
    return sqlite3.connect(tmp_path/'features.sqlite3')


def capture(db, at=180000, books=None, spots=None, core=None, exposure=None, who=None):
    return reference.capture_checkpoints(
        db, who or identity(), [snap(at, up='.4', down='.6')] if books is None else books,
        tape(at) if spots is None else spots, S+at, D(1),
        core_decision=empty_core() if core is None else core,
        exposure=clear_exposure(at) if exposure is None else exposure)


def checkpoint(records, offset=180000):
    return next(record for record in records if record['checkpoint_ms'] == offset)


def test_model_full_diagnostics_and_stress_cap_use_fee_adjusted_cost():
    book = snap(180000, up='.4', down='.6')
    result = reference.evaluate_checkpoint(book, tape(180000), S+180000, D(1))
    assert result['status'] == 'EVALUATED' and result['candidate']['side'] == 'UP'
    candidate = result['candidate']
    assert candidate['fingerprint'] == FINGERPRINT
    assert candidate['branch'] == 'reference_180_mid'
    assert D(candidate['effective_cost']) == D(candidate['cash'])/D(candidate['net_shares'])
    assert D('.4') < D(candidate['effective_cost']) < D('.65')
    assert D(candidate['cap']) == D(candidate['limit'])+D('.02')
    assert D(candidate['cap']) <= D('.75')
    assert result['per_side']['UP']['stress_execution']
    assert result['per_side']['DOWN']['status'] == 'REJECTED'
    assert result['per_side']['DOWN']['ev_usdt'] is not None
    assert result['latest']['received_ms'] <= S+180000
    assert result['model']['source'] == 'binance_spot'
    # Full book input copied, later mutation cannot rewrite the observation.
    assert result['snapshot'] == book
    book['quote']['UP']['ask_levels'][0][0] = '.1'
    assert result['snapshot']['quote']['UP']['ask_levels'][0][0] == '.4'
    json.dumps(result, allow_nan=False)


@pytest.mark.parametrize('price,fee,eligible', [('.4', 0, True), ('.399999', 0, False),
                                              ('.65', 0, False), ('.64', 1000, False)])
def test_cost_band_is_fee_adjusted_closed_lower_open_upper(price, fee, eligible):
    book = snap(180000, up=price, down='.8')
    book['fee_bps'] = fee
    result = reference.evaluate_checkpoint(book, tape(180000), S+180000, D(1))
    assert bool(result['candidate']) is eligible
    if not eligible:
        assert result['per_side']['UP']['reasons'] == ['reference_effective_cost_outside_band']


def test_model_requires_both_original_ev_and_shifted_ev():
    book = snap(180000, up='.46', down='.8')
    book['fee_bps'] = 0
    with patch.object(reference, 'probability', return_value=(D('.474'), spot(180000), {})):
        result = reference.evaluate_checkpoint(book, tape(180000), S+180000, D(1))
    assert D(result['per_side']['UP']['ev_usdt']) >= D('.03')
    assert result['candidate'] is None
    assert result['per_side']['UP']['reasons'] == ['reference_fee_net_ev_below_minimum']


@pytest.mark.parametrize('bad', ['stale', 'future', 'opposite_thin', 'opposite_unsorted',
                                'opposite_empty', 'opposite_malformed', 'anchor', 'generation', 'spot_future', 'fee'])
def test_noncausal_or_incomplete_inputs_do_not_manufacture_candidate(bad):
    book, spots = snap(180000, up='.4', down='.6'), tape(180000)
    if bad == 'stale':
        book['book_at_ms'] -= 1001
    elif bad == 'future':
        book['received_at_ms'] += 1
    elif bad == 'opposite_thin':
        book['quote']['DOWN']['ask_levels'] = [['.6', '.01']]
    elif bad == 'opposite_unsorted':
        book['quote']['DOWN']['ask_levels'] = [['.7', '100'], ['.6', '100']]
    elif bad == 'opposite_empty':
        book['quote']['DOWN']['ask_levels'] = []
    elif bad == 'opposite_malformed':
        book['quote']['DOWN']['ask_levels'] = [['.6']]
    elif bad == 'anchor':
        spots = [value for value in spots if not S <= value['event_ms'] <= S+1500]
    elif bad == 'generation':
        spots[-1]['generation'] = 2
    elif bad == 'spot_future':
        spots = [value for value in spots if value['event_ms'] < S+178000]
        spots.append(spot(180001, price='100.1', received=S+180000))
    else:
        book['fee_bps'] = 'NaN'
    result = reference.evaluate_checkpoint(book, spots, S+180000, D(1))
    assert result['status'] == 'INPUTS_UNAVAILABLE'
    assert result['candidate'] is None and result['reasons']


@pytest.mark.parametrize('phase', ['book', 'model', 'execution'])
def test_exception_diagnostics_never_persist_unknown_secret_or_url_text(phase):
    secret = 'https://api.invalid/private?access_token=secret-private-key'
    target = {'book':'_book_clock', 'model':'probability', 'execution':'model_candidate'}[phase]
    with patch.object(reference, target, side_effect=ValueError(secret)):
        result = reference.evaluate_checkpoint(snap(180000, up='.4', down='.6'), tape(180000), S+180000, D(1))
    serialized = json.dumps(result, allow_nan=False)
    assert 'access_token' not in serialized and 'secret-private-key' not in serialized
    assert 'https://' not in serialized
    if phase == 'execution':
        assert result['per_side']['UP']['reasons'] == ['reference_execution_invalid']
    else:
        assert result['reasons'] == ['reference_inputs_invalid']


def test_future_spot_and_outcome_do_not_change_model_or_execution():
    book, spots = snap(180000, up='.4', down='.6'), tape(180000)
    before = reference.evaluate_checkpoint(book, spots, S+180000, D(1))
    book['winner'] = 'DOWN'
    after = reference.evaluate_checkpoint(book, spots+[spot(180001, '1'), spot(181000, '999')], S+180000, D(1))
    for key in ('p_up', 'latest', 'model', 'per_side', 'candidate'):
        assert before[key] == after[key]


def test_complete_180_observation_does_not_issue_shadow_or_mutate_live_selection(tmp_path):
    core = empty_core()
    exposure = clear_exposure()
    before = copy.deepcopy((core, exposure))
    with connect(tmp_path) as db:
        records = capture(db, core=core, exposure=exposure)
        current = checkpoint(records)
        assert current['status'] == 'EVALUATED' and current['backfill_eligible']
        assert current['paper_quote'] is None and current['backfill_mode'] == 'live'
        assert {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")} == {'t69_reference_checkpoints'}
    assert (core, exposure) == before


@pytest.mark.parametrize('bad', ['selected', 'missing', 'not_verified', 'nonempty', 'contradiction',
                                'fingerprint', 'loop', 'topic', 'up', 'start', 'end', 'unit', 'fee',
                                'frozen_late', 'frozen_future', 'initial_late', 'initial_old',
                                'feature_fingerprint', 'feature_start', 'feature_cutoff', 'feature_received'])
def test_core_proof_denies_paper_without_hiding_model_candidate(tmp_path, bad):
    core = empty_core()
    if bad == 'missing': core = {}
    elif bad == 'selected': core['selected'] = True
    elif bad == 'not_verified': core['core_guard']['verified'] = False
    elif bad == 'nonempty': core['core_guard']['empty'] = False
    elif bad == 'contradiction': core['core_guard']['candidates'] = [{'branch': 'core_first_up'}]
    elif bad in ('fingerprint', 'loop', 'topic', 'up'):
        key = {'fingerprint':'fingerprint', 'loop':'loop_id', 'topic':'market_topic', 'up':'market_id'}[bad]
        core[key] = 'wrong'
    elif bad in ('start', 'end'):
        core['market_'+bad+'_ms'] += 300000
    elif bad == 'unit': core['unit_usdt'] = '2'
    elif bad == 'fee': core['fee_bps'] = '300'
    elif bad == 'frozen_late': core['core_guard']['frozen_at_ms'] = S+126001
    elif bad == 'frozen_future': core['core_guard']['frozen_at_ms'] = S+180001
    elif bad == 'initial_late': core['core_guard']['initial_captured_at_ms'] = S+124001
    elif bad == 'initial_old': core['core_guard']['initial_book_at_ms'] -= 2001
    elif bad == 'feature_fingerprint': core['core_guard']['features']['fingerprint'] = 'wrong'
    elif bad == 'feature_start': core['core_guard']['features']['market_start_ms'] += 300000
    elif bad == 'feature_cutoff': core['core_guard']['features']['cutoff_ms'] += 1
    else: core['core_guard']['features']['received_at_ms'] = S+123001
    with connect(tmp_path) as db:
        record = checkpoint(capture(db, core=core))
        assert record['evaluation']['candidate'] is not None
        assert not record['backfill_eligible'] and record['paper_quote'] is None
        assert record['backfill_reason'].startswith('reference_core_')


@pytest.mark.parametrize('bad', ['missing', 'claim', 'intent', 'position', 'unknown', 'bool_missing',
                                'stale', 'future', 'otherloop', 'fee'])
def test_readonly_exposure_must_be_causal_clear_and_identity_matched(tmp_path, bad):
    exposure = clear_exposure()
    if bad == 'missing': exposure = {}
    elif bad in ('claim', 'intent', 'position', 'unknown'): exposure['has_'+bad] = True
    elif bad == 'bool_missing': del exposure['has_intent']
    elif bad == 'stale': exposure['observed_at_ms'] -= 1001
    elif bad == 'future': exposure['observed_at_ms'] += 1
    elif bad == 'otherloop': exposure['loop_id'] = 'other'
    else: exposure['fee_bps'] = '300'
    with connect(tmp_path) as db:
        record = checkpoint(capture(db, exposure=exposure))
        assert record['evaluation']['candidate'] is not None
        assert not record['backfill_eligible'] and record['paper_quote'] is None


@pytest.mark.parametrize('offset', [60000, 120000, 240000])
def test_other_checkpoints_never_create_180_backfill_quote(tmp_path, offset):
    with connect(tmp_path) as db:
        record = checkpoint(capture(db, at=offset), offset)
        assert record['evaluation']['candidate'] is not None
        assert not record['backfill_eligible'] and record['paper_quote'] is None


def test_first_missing_checkpoint_is_immutable_later_success_does_not_replace_it(tmp_path):
    with connect(tmp_path) as db:
        first = checkpoint(capture(db, at=60000, books=[], who=identity(None)), 60000)
        assert first['status'] == 'INPUTS_UNAVAILABLE'
        frozen = db.execute('SELECT payload FROM t69_reference_checkpoints').fetchone()[0]
        assert capture(db, at=61000) == []
        assert db.execute('SELECT payload FROM t69_reference_checkpoints').fetchone()[0] == frozen
        records = capture(db, at=120000)
        assert checkpoint(records, 120000)['status'] == 'EVALUATED'
        assert db.execute('SELECT COUNT(*) FROM t69_reference_checkpoints').fetchone()[0] == 2


def test_first_invalid_snapshot_is_not_skipped_for_previous_success(tmp_path):
    good = snap(180000, up='.4', down='.6')
    bad = snap(180100, up='.4', down='.6')
    bad['market_id'] = 'wrong'
    with connect(tmp_path) as db:
        record = checkpoint(capture(db, at=180100, books=[good, bad]))
        assert record['status'] == 'INPUTS_UNAVAILABLE'
        assert record['evaluation']['candidate'] is None and record['paper_quote'] is None


def test_late_checkpoints_are_recorded_missed_without_reconstructing_history(tmp_path):
    with connect(tmp_path) as db:
        records = capture(db, at=240000, books=[snap(60000), snap(120000), snap(180000), snap(240000, up='.4', down='.6')])
        assert [record['status'] for record in records] == ['MISSED_CHECKPOINT']*3+['EVALUATED']
        for record in records[:3]:
            assert record['evaluation'] is None and record['paper_quote'] is None
        assert capture(db, at=240100) == []


def test_checkpoint_grace_boundary_and_future_book(tmp_path):
    with connect(tmp_path) as db:
        record = checkpoint(capture(db, at=181500), 180000)
        assert record['status'] == 'EVALUATED'
    with sqlite3.connect(tmp_path/'second.sqlite3') as db:
        record = checkpoint(capture(db, at=181501))
        assert record['status'] == 'MISSED_CHECKPOINT'
    with sqlite3.connect(tmp_path/'third.sqlite3') as db:
        record = checkpoint(capture(db, books=[snap(180001, up='.4', down='.6')]))
        assert record['status'] == 'INPUTS_UNAVAILABLE'


@pytest.mark.parametrize('budget', ['rows', 'bytes', 'payload', 'reserve'])
def test_budget_exhaustion_never_prunes_shared_feature_history_or_earlier_evidence(tmp_path, budget):
    with connect(tmp_path) as db:
        db.execute('CREATE TABLE features(start INTEGER PRIMARY KEY,payload TEXT)')
        db.execute('INSERT INTO features VALUES(?,?)', (S, json.dumps(feature(2, -1))))
        db.commit()
        capture(db, at=60000)
        before = db.execute('SELECT * FROM t69_reference_checkpoints').fetchall()
        features_before = db.execute('SELECT * FROM features').fetchall()
        if budget == 'rows': context = patch.dict(POLICY, checkpoint_max_rows=1)
        elif budget == 'bytes': context = patch.dict(POLICY, checkpoint_max_payload_bytes=1)
        elif budget == 'payload': context = patch.object(reference, 'bounded_payload', side_effect=ValueError('budget'))
        else: context = patch.object(reference, 'require_free_space', side_effect=OSError('reserve'))
        with context, pytest.raises((OSError, ValueError)):
            capture(db, at=120000)
        assert db.execute('SELECT * FROM t69_reference_checkpoints').fetchall() == before
        assert db.execute('SELECT * FROM features').fetchall() == features_before


def test_caller_transaction_is_never_committed_or_rolled_back(tmp_path):
    with connect(tmp_path) as db:
        db.execute('CREATE TABLE features(start INTEGER,payload TEXT)')
        db.execute('INSERT INTO features VALUES(?,?)', (S, 'pending'))
        assert db.in_transaction
        with pytest.raises(ValueError, match='own_transaction'):
            capture(db)
        assert db.in_transaction and db.execute('SELECT payload FROM features').fetchone()[0] == 'pending'

"""Selected rechecks remain read-only under writer contention and restart."""
import json
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from decimal import Decimal as D
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from src.gridbot.prediction import regime_worker_bridge as b
from src.gridbot.prediction.regime_t68a_policy import PROFILE
from src.gridbot.prediction.regime_t67_evidence import EvidenceStore, evidence_path
from test_t63 import S, feature, book
from test_t67 import snap, tape
from test_t68a import setup, state, paper_db


@pytest.mark.parametrize('first,last,prior,initial_up,new_up,branch', [
    (2,-1,5,'.30','.29','core_first_up'),
    (-2,1,-2,'.70','.71','core_first_down'),
    (2,-1,-2,'.60','.61','shallow_retracement'),
])
def test_selected_refresh_works_with_another_writer_and_never_calls_write_connector(
        tmp_path,first,last,prior,initial_up,new_up,branch):
    bridge,check=setup(tmp_path,feature(first,last,prior),book(initial_up,'.30',124000))
    one=check();assert one.allowed,one.reason
    frozen=state(bridge);assert frozen['branch']==branch
    writer=sqlite3.connect(bridge.feature_db)
    try:
        writer.execute('PRAGMA journal_mode=WAL')
        writer.execute('BEGIN IMMEDIATE')
        with patch.object(b,'connect',side_effect=AssertionError('selected refresh attempted a write connection')):
            for offset in (124200,124400,124600):
                ready=check(book(new_up,'.29',offset),seen=one.book_at_ms)
                assert ready.allowed,ready.reason
                assert ready.signal==one.signal
        # Recheck does not even update last_evaluated_ms in the selected row.
        assert state(bridge)==frozen
    finally:
        writer.rollback();writer.close()


def test_concurrent_initial_callers_keep_one_selected_signal(tmp_path):
    initial=book('.30','.70',124000)
    bridge,check=setup(tmp_path,feature(2,-1,5),initial)
    market=SimpleNamespace(start_time_ms=S,market_topic_id='topic',up_market_id='up')
    with patch.object(bridge,'_first_book',return_value=initial),patch.object(b,'read_c180_book',return_value=initial),patch.object(b,'read_c180_signal',return_value=None):
        def call(offset):
            return bridge.check_signal(market=market,unit_usdt=D(1),at_ms=S+offset,last_seen_book_at_ms=0)
        with ThreadPoolExecutor(max_workers=2) as pool:
            results=list(pool.map(call,(124000,124000)))
    assert all(r.allowed for r in results),[r.reason for r in results]
    assert results[0].signal==results[1].signal
    frozen=state(bridge)
    assert json.loads(frozen['signal'])['completed_at_ms']==frozen['selected_at_ms']
    with closing(sqlite3.connect(bridge.feature_db)) as db:
        assert db.execute('SELECT COUNT(*) FROM t68a_decisions').fetchone()[0]==1


def test_b_selection_does_not_overwrite_a_decision(tmp_path):
    bridge,check=setup(tmp_path,feature(2,-1,5),book('.30','.70',124000))
    with closing(sqlite3.connect(bridge.feature_db)) as db,db:
        db.execute('CREATE TABLE t67a_decisions(start INTEGER PRIMARY KEY,payload TEXT)')
        db.execute('INSERT INTO t67a_decisions VALUES(?,?)',(S,'historical-A'))
    assert check().allowed
    with closing(sqlite3.connect(bridge.feature_db)) as db:
        assert db.execute('SELECT payload FROM t67a_decisions').fetchone()[0]=='historical-A'
    assert bridge.profile==PROFILE


def test_real_early_empty_core_late_public_selection_and_readonly_claim_refresh(tmp_path):
    bridge, check = setup(tmp_path, feature(2,-1,-2), book('.8','.8',124000))
    first = check()
    assert not first.allowed and first.reason == 't68a_wait_reference_checkpoint'
    frozen_core = state(bridge)['core_guard']
    assert frozen_core['verified'] and frozen_core['empty']
    pred = tmp_path/'prediction.sqlite3'
    paper_db(pred)
    with sqlite3.connect(pred) as db:
        db.executescript(
            'CREATE TABLE prediction_regime_entry_claims(market_start_ms INTEGER);'
            'CREATE TABLE prediction_campaigns(campaign_id TEXT,start_time_ms INTEGER,buy_count INTEGER,pending_unknown INTEGER);'
            'CREATE TABLE prediction_order_intents(campaign_id TEXT,order_side TEXT,unknown INTEGER);')
    bridge.repository = SimpleNamespace(db_path=pred)
    with closing(EvidenceStore(evidence_path(bridge.signal_db))) as evidence:
        evidence.book(snap(180000, up='.4', down='.6'))
        with evidence.db:
            evidence.db.executemany('INSERT INTO spot VALUES(?,?,?,?,?)',
                [(s['source'],s['generation'],s['event_ms'],s['received_ms'],s['price']) for s in tape(180000)])
        # No C180 producer refresh after136s: use the real public evidence DB.
        late = check(at=S+180000)
        assert late.allowed, late.reason
        chosen = state(bridge)
        assert chosen['branch'] == 'reference_180_mid'
        assert chosen['core_guard'] == frozen_core
        assert late.signal.cutoff_ms == S+180000
        assert late.execution.expires_at_ms == S+182000
        # Our own new claim must not make the subsequent shared-worker recheck
        # deny an already selected signal or try to acquire another writer.
        with sqlite3.connect(pred) as db:
            db.execute('INSERT INTO prediction_regime_entry_claims VALUES(?)', (S,))
        evidence.book(snap(180200, up='.41', down='.59'))
        writer = sqlite3.connect(bridge.feature_db)
        try:
            writer.execute('BEGIN IMMEDIATE')
            with patch.object(b, 'connect', side_effect=AssertionError('selected refresh wrote decisions')):
                refreshed = check(at=S+180200, seen=late.book_at_ms)
            assert refreshed.allowed, refreshed.reason
            assert refreshed.signal == late.signal
            assert state(bridge) == chosen
        finally:
            writer.rollback()
            writer.close()


def test_late_public_candidate_cannot_replace_successful_early_core(tmp_path):
    bridge, check = setup(tmp_path, feature(2,-1,5), book('.3','.7',124000))
    assert check().allowed
    chosen = state(bridge)
    with closing(EvidenceStore(evidence_path(bridge.signal_db))) as evidence:
        evidence.book(snap(180000, up='.4', down='.6'))
        with evidence.db:
            evidence.db.executemany('INSERT INTO spot VALUES(?,?,?,?,?)',
                [(s['source'],s['generation'],s['event_ms'],s['received_ms'],s['price']) for s in tape(180000)])
        late = check(at=S+180000)
    assert not late.allowed and late.reason == 't68a_selected_quote_expired'
    assert state(bridge) == chosen

"""Malformed public depth cannot crash shared features or manufacture paper fills."""
import copy
import json
import sqlite3
from contextlib import closing
from decimal import Decimal as D
from unittest.mock import patch

import pytest

from src.gridbot.prediction.regime_feature_service import connect, collect_once
from src.gridbot.prediction.regime_t67d_shadow import observe, schema
from test_t63 import S
from test_t67 import snap, tape, spot
from test_t67d import paper_db


@pytest.mark.parametrize('side', ['UP', 'DOWN'])
@pytest.mark.parametrize('levels', [[], [[]], [['.3']], None, [['NaN', '10']], [['.3', 'Infinity']]])
def test_incomplete_current_book_is_skipped_and_next_valid_quote_recovers(tmp_path, side, levels):
    pred=tmp_path/'prediction.sqlite3';paper_db(pred);before=pred.read_bytes()
    bad=snap();bad['quote'][side]['ask_levels']=levels
    with closing(connect(tmp_path/'features.sqlite3')) as db:
        schema(db)
        with patch('src.gridbot.prediction.regime_t67d_shadow.read_inputs',return_value=([bad],tape())):
            assert observe(db,pred,tmp_path/'signals.sqlite3',S+60000)=='shadow_book_depth_unavailable'
        assert not db.execute('SELECT 1 FROM t67d_shadow_states').fetchone()
        assert not db.execute('SELECT 1 FROM t67d_shadow_quotes').fetchone()
        with patch('src.gridbot.prediction.regime_t67d_shadow.read_inputs',return_value=([snap(60100)],tape(60100))):
            assert observe(db,pred,tmp_path/'signals.sqlite3',S+60100)=='shadow_observed'
        quotes=[json.loads(row[0]) for row in db.execute('SELECT payload FROM t67d_shadow_quotes')]
        assert len(quotes)==1 and quotes[0]['branch']=='reference_value'
        assert quotes[0]['quoted_at_ms']==S+60100
        assert quotes[0]['fill_status']=='PAPER_QUOTE_ONLY'
    assert pred.read_bytes()==before


@pytest.mark.parametrize('levels',[[],[[]],[['.3']]])
def test_invalid_previous_book_does_not_trigger_lead_lag_or_change_evidence(tmp_path, levels):
    pred=tmp_path/'prediction.sqlite3';paper_db(pred);before=pred.read_bytes()
    prior=snap(69000);prior['quote']['UP']['ask_levels']=levels
    books=[prior,snap(70000)];original=copy.deepcopy(books)
    with closing(connect(tmp_path/'features.sqlite3')) as db:
        with patch('src.gridbot.prediction.regime_t67d_shadow.read_inputs',return_value=(books,tape(70000))):
            assert observe(db,pred,tmp_path/'signals.sqlite3',S+70000)=='shadow_observed'
        state=json.loads(db.execute('SELECT payload FROM t67d_shadow_states').fetchone()[0])
        assert not state.get('lead_lag')
        assert not db.execute('SELECT 1 FROM t67d_shadow_quotes').fetchone()
    assert books==original and pred.read_bytes()==before


def test_missing_checkpoint_depth_rejects_pending_trigger_without_paper_fill(tmp_path):
    pred=tmp_path/'prediction.sqlite3';paper_db(pred);before=pred.read_bytes()
    spots=tape(70000);spots[-2]['price']='100.07'
    with closing(connect(tmp_path/'features.sqlite3')) as db:
        with patch('src.gridbot.prediction.regime_t67d_shadow.read_inputs',return_value=([snap(69000),snap(70000)],spots)):
            assert observe(db,pred,tmp_path/'signals.sqlite3',S+70000)=='shadow_observed'
        state=json.loads(db.execute('SELECT payload FROM t67d_shadow_states').fetchone()[0])
        assert state['lead_lag']['status']=='PENDING'
        checkpoint=snap(70300);checkpoint['quote']['UP']['ask_levels']=[]
        spots += [spot(70300,'100.1'),spot(71000,'100.1')]
        with patch('src.gridbot.prediction.regime_t67d_shadow.read_inputs',return_value=([checkpoint,snap(71000)],spots)):
            assert observe(db,pred,tmp_path/'signals.sqlite3',S+71000)=='shadow_observed'
        state=json.loads(db.execute('SELECT payload FROM t67d_shadow_states').fetchone()[0])
        assert state['lead_lag']['status']=='REJECTED'
        assert not db.execute('SELECT 1 FROM t67d_shadow_quotes').fetchone()
    assert pred.read_bytes()==before


def test_empty_depth_does_not_block_next_official_feature_cutoff(tmp_path):
    pred=tmp_path/'prediction.sqlite3';paper_db(pred);before=pred.read_bytes()
    bad=snap();bad['quote']['UP']['ask_levels']=[]
    candles=[[S-900000+i*60000,'100','101','99','100','1',S-900000+i*60000+59999] for i in range(17)]
    with closing(connect(tmp_path/'features.sqlite3')) as db:
        with patch('src.gridbot.prediction.regime_t67d_shadow.read_inputs',return_value=([bad],tape())):
            assert observe(db,pred,tmp_path/'signals.sqlite3',S+60000)=='shadow_book_depth_unavailable'
        assert collect_once(db,S,clock=lambda:S+120001,fetch=lambda:candles)=='frozen'
        feature=json.loads(db.execute('SELECT payload FROM features WHERE start=?',(S,)).fetchone()[0])
        assert feature['received_at_ms']==S+120001 and feature['cutoff_ms']==S+120000
    assert pred.read_bytes()==before

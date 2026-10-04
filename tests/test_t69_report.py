"""T6.9 reports only this loop; Shadow never becomes Live performance."""
import json
import sqlite3
from unittest.mock import AsyncMock, Mock, patch

import pytest

from test_live_report import SCHEMA, START
from src.gridbot.prediction.live_report import (
    T69_PROFILE, T67_PROFILE, format_live_report, t67_family_report_profile,
)
from src.gridbot.prediction.regime_t69_policy import FINGERPRINT
from src.gridbot.prediction.regime_t69_report import (
    LIVE_LABELS, SHADOW_LABELS, checkpoint_metrics,
)
from src.gridbot.prediction.telegram import PredictionTelegramService

NOW = START+300000


def main_database(root):
    path = root/'prediction/data/prediction.sqlite3'
    path.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(path)
    db.executescript(SCHEMA)
    db.execute('CREATE TABLE prediction_orders(order_id TEXT,campaign_id TEXT,status TEXT)')
    for table, column, kind in (
        ('prediction_campaigns', 'market_topic_id', 'TEXT'),
        ('prediction_campaigns', 'market_id', 'TEXT'),
        ('prediction_campaigns', 'end_time_ms', 'INTEGER'),
        ('prediction_campaigns', 'initial_outcome', 'TEXT'),
        ('prediction_campaigns', 'payload_json', 'TEXT'),
        ('prediction_regime_slots', 'market_topic_id', 'TEXT'),
        ('prediction_regime_slots', 'market_id', 'TEXT'),
        ('prediction_fills', 'outcome', 'TEXT'),
        ('prediction_settlements', 'winner', 'TEXT'),
    ):
        db.execute(f'ALTER TABLE {table} ADD COLUMN {column} {kind}')
    db.execute("INSERT INTO prediction_loops VALUES('current',?,'LIVE','RUNNING',100,0,1,0,0)", (T69_PROFILE,))
    db.execute("INSERT INTO prediction_runtime_config VALUES('prediction_hard_stop_latched',?)", (json.dumps({'latched': False}),))
    db.commit()
    return db


def feature_database(root):
    path = root/'prediction/data/regime-target6/features.sqlite3'
    path.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(path)
    db.executescript('''
        CREATE TABLE t69_decisions(start INTEGER PRIMARY KEY,payload TEXT);
        CREATE TABLE t69_shadow_quotes(start INTEGER,branch TEXT,payload TEXT,PRIMARY KEY(start,branch));
        CREATE TABLE t69_shadow_outcomes(start INTEGER PRIMARY KEY,payload TEXT);
        CREATE TABLE t69_reference_checkpoints(start INTEGER,checkpoint_ms INTEGER,payload TEXT,PRIMARY KEY(start,checkpoint_ms));
    ''')
    return db


def admission(db, start=START):
    db.execute('INSERT OR IGNORE INTO prediction_regime_slots '
               '(loop_id,market_start_ms,run_ordinal,verified_at_ms,empty_attested_at_ms,market_topic_id,market_id) '
               'VALUES(?,?,?,?,NULL,?,?)', ('current', start, (start-START)//300000+1, start, 'topic'+str(start), 'up'+str(start)))
    db.commit()


def identity(start=START, **changes):
    result = dict(fingerprint=FINGERPRINT, loop_id='current', market_topic='topic'+str(start),
                  market_id='up'+str(start), market_start_ms=start, end_ms=start+300000)
    result.update(changes)
    return result


def live_fill(db, feature, *, branch='core_first_down', side=None, start=START, pnl='1', settled=True):
    side = side or ('UP' if branch in ('core_first_up', 'c_mirror_up_prior') else 'DOWN')
    admission(db, start)
    cid = 'c'+str(start)
    db.execute('INSERT INTO prediction_campaigns VALUES(?,?,?,?,?,?,?,?,?)',
               (cid, 'current', start, 0, 'topic'+str(start), 'up'+str(start), start+300000, side, '{}'))
    db.execute('INSERT INTO prediction_regime_entry_claims VALUES(?,?,?,?,?)', ('current', start, cid, 'i'+cid, '1'))
    db.execute('INSERT INTO prediction_order_intents VALUES(?,?,?,?,?,?)', ('i'+cid, cid, 'FILLED', 'o'+cid, 0, start+125000))
    db.execute("INSERT INTO prediction_fills VALUES(?,'BUY',?)", (cid, side))
    if settled:
        db.execute("INSERT INTO prediction_settlements VALUES(?,?,'SETTLED',?,?)", ('s'+cid, cid, pnl, side))
        db.execute('INSERT INTO prediction_regime_settlement_observations VALUES(?,?,?,?)', ('s'+cid, cid, pnl, start+300000))
    db.commit()
    decision = identity(start, selected=True, branch=branch, side=side)
    feature.execute('INSERT INTO t69_decisions VALUES(?,?)', (start, json.dumps(decision)))
    feature.commit()


def paper_quote(db, feature, *, branch='external_lead_lag', winner='UP', **changes):
    admission(db)
    quote = identity(side='UP', unit_usdt='1', cash='1', net_shares='2', fee_bps='0',
                     quoted_at_ms=START+60000, book_at_ms=START+60000, source_receive_ms=START+60000)
    quote.update(changes)
    feature.execute('INSERT INTO t69_shadow_quotes VALUES(?,?,?)', (START, branch, json.dumps(quote)))
    if winner:
        outcome = identity(complete=True, winner=winner, final_side=winner, official_status='RESOLVED', known_at_ms=NOW)
        feature.execute('INSERT OR REPLACE INTO t69_shadow_outcomes VALUES(?,?)', (START, json.dumps(outcome)))
    feature.commit()


def render(root, now=NOW):
    return format_live_report(root, now_ms=now, profile_filter=T69_PROFILE)


def test_empty_t69_never_falls_back_to_old_live(tmp_path):
    with main_database(tmp_path) as db:
        db.execute("DELETE FROM prediction_loops WHERE loop_id='current'")
        db.execute("INSERT INTO prediction_loops VALUES('old',?,'LIVE','RUNNING',100,20,99,0,0)", (T67_PROFILE,))
    text = render(tmp_path)
    assert '尚未建立 T6.9' in text
    assert 'Loop old' not in text and 'T6.6' not in text
    for title in LIVE_LABELS.values():
        assert title+'｜成交 0｜已知WR —｜已知PnL —' in text
    for title in SHADOW_LABELS.values():
        assert title+'｜報價 0｜已知paper WR —｜假設paper PnL —' in text


def test_zero_rows_show_eight_live_and_six_shadow_grouped(tmp_path):
    with main_database(tmp_path):
        pass
    text = render(tmp_path)
    assert '本輪 WR —' in text and '本輪已知淨 PnL —' in text
    assert text.count('｜成交 0｜') == 8
    assert text.count('｜報價 0｜') == 6
    for group in ('〔核心〕', '〔增量〕', '〔180s 補位〕', '〔研究〕', '〔Flat F1–F4〕'):
        assert group in text
    for retired in ('T6.6', 'M4 Shadow', 'M6 Shadow', 'A Shadow', 'flat/original Shadow', '補位Shadow'):
        assert retired not in text


def test_no_fills_still_shows_selected_amount(tmp_path):
    with main_database(tmp_path) as db:
        db.execute("INSERT INTO prediction_runtime_config VALUES('prediction_selected_order_unit',?)",
                   (json.dumps({'order_unit_usdt': '1'}),))
    text = render(tmp_path)
    assert '每筆 1（目前設定；待成交確認） USDT' in text
    assert '入場intent 0｜送單嘗試 0｜成交市場 0' in text


def test_each_live_branch_attributed_and_never_adds_shadow(tmp_path):
    with main_database(tmp_path) as db, feature_database(tmp_path) as feature:
        for i, branch in enumerate(LIVE_LABELS):
            live_fill(db, feature, branch=branch, start=START+i*300000)
        paper_quote(db, feature, winner='DOWN')
    text = render(tmp_path, now=START+8*300000)
    assert 'Live fill rate 100.0%（8/8' in text
    assert '本輪已知淨 PnL +8.0000 USDT' in text
    for title in LIVE_LABELS.values():
        assert title+'｜成交 1｜已知WR 100.0%｜已知PnL +1.0000' in text
    assert '外部先行｜報價 1｜已知paper WR 0.0%｜假設paper PnL -1.0000' in text


def test_prior_same_profile_bad_fill_does_not_poison_current(tmp_path):
    with main_database(tmp_path) as db:
        db.execute("INSERT INTO prediction_loops VALUES('prior',?,'LIVE','DONE',100,100,0,0,0)", (T69_PROFILE,))
        db.execute("INSERT INTO prediction_campaigns(campaign_id,loop_id,start_time_ms,pending_unknown) VALUES('bad','prior',1,1)")
        db.execute("INSERT INTO prediction_fills VALUES('bad','BUY','DOWN')")
        db.execute("INSERT INTO prediction_settlements VALUES('bad','bad','SETTLED','-999','DOWN')")
    text = render(tmp_path)
    assert 'Loop current' in text and 'UNKNOWN市場 0' in text
    assert '-999' not in text and '成交與lane claim不一致' not in text


@pytest.mark.parametrize('changes', [dict(market_id='wrong'), dict(loop_id='prior'), dict(fingerprint='old')])
def test_bad_decision_keeps_live_total_but_does_not_attribute(tmp_path, changes):
    with main_database(tmp_path) as db, feature_database(tmp_path) as feature:
        live_fill(db, feature)
        row = json.loads(feature.execute('SELECT payload FROM t69_decisions').fetchone()[0])
        row.update(changes)
        feature.execute('UPDATE t69_decisions SET payload=?', (json.dumps(row),))
    text = render(tmp_path)
    assert '本輪已知淨 PnL +1.0000 USDT' in text
    assert 'first DOWN｜成交 0' in text and '子策略歸因待核對 1' in text


def test_actual_buy_direction_must_match_decision(tmp_path):
    with main_database(tmp_path) as db, feature_database(tmp_path) as feature:
        live_fill(db, feature)
        db.execute("UPDATE prediction_campaigns SET initial_outcome=NULL")
        db.execute("UPDATE prediction_fills SET outcome='UP'")
    text = render(tmp_path)
    assert '本輪已知淨 PnL +1.0000 USDT' in text
    assert 'first DOWN｜成交 0' in text and '子策略歸因待核對 1' in text


@pytest.mark.parametrize('saved_up', ['up'+str(START), 'wrong', None])
def test_blank_generic_campaign_id_requires_saved_up_identity(tmp_path, saved_up):
    with main_database(tmp_path) as db, feature_database(tmp_path) as feature:
        live_fill(db, feature)
        payload = {'market': {'market_topic_id': 'topic'+str(START), 'up_market_id': saved_up,
                              'start_time_ms': START, 'end_time_ms': NOW}}
        db.execute("UPDATE prediction_campaigns SET market_id='',payload_json=?", (json.dumps(payload),))
    text = render(tmp_path)
    assert ('first DOWN｜成交 1' in text) == (saved_up == 'up'+str(START))


def test_pending_live_does_not_become_zero_or_shadow_pnl(tmp_path):
    with main_database(tmp_path) as db, feature_database(tmp_path) as feature:
        live_fill(db, feature, settled=False)
        paper_quote(db, feature)
    text = render(tmp_path)
    assert '本輪已知淨 PnL —（待核對／結算）' in text
    assert 'first DOWN｜成交 1｜已知WR —｜已知PnL — USDT｜待結算 1' in text
    assert '假設paper PnL +1.0000' in text


def test_paper_quote_only_does_not_change_live_fill_or_pnl(tmp_path):
    with main_database(tmp_path) as db, feature_database(tmp_path) as feature:
        paper_quote(db, feature)
    text = render(tmp_path)
    assert 'Live fill rate 0.0%（0/1' in text
    assert '本輪已知淨 PnL — USDT' in text and '成交市場 0' in text
    assert '假設paper PnL +1.0000 USDT' in text


def test_shadow_start_checkpoint_allows_causal_fresh_prior_book(tmp_path):
    with main_database(tmp_path) as db, feature_database(tmp_path) as feature:
        paper_quote(db, feature, book_at_ms=START+59500, source_receive_ms=START+59600)
    assert '外部先行｜報價 1｜已知paper WR 100.0%' in render(tmp_path)


@pytest.mark.parametrize('changes', [dict(market_id='wrong'), dict(quoted_at_ms=NOW+1), dict(cash='NaN'), dict(net_shares='-1')])
def test_invalid_shadow_quote_not_counted(tmp_path, changes):
    with main_database(tmp_path) as db, feature_database(tmp_path) as feature:
        paper_quote(db, feature, **changes)
    text = render(tmp_path)
    assert '外部先行｜報價 0｜已知paper WR —｜假設paper PnL —' in text
    assert '待核對 1' in text


@pytest.mark.parametrize('outcome_change', [dict(market_id='wrong'), dict(final_side='DOWN'), dict(known_at_ms=NOW+1)])
def test_unverified_or_future_shadow_outcome_stays_unknown(tmp_path, outcome_change):
    with main_database(tmp_path) as db, feature_database(tmp_path) as feature:
        paper_quote(db, feature)
        outcome = json.loads(feature.execute('SELECT payload FROM t69_shadow_outcomes').fetchone()[0])
        outcome.update(outcome_change)
        feature.execute('UPDATE t69_shadow_outcomes SET payload=?', (json.dumps(outcome),))
    text = render(tmp_path)
    assert '外部先行｜報價 1｜已知paper WR —｜假設paper PnL —｜未知 1' in text


def test_conflicting_official_winner_blocks_paper_result(tmp_path):
    with main_database(tmp_path) as db, feature_database(tmp_path) as feature:
        live_fill(db, feature, side='DOWN')
        paper_quote(db, feature, winner='UP')
    text = render(tmp_path)
    assert '本輪已知淨 PnL +1.0000 USDT' in text
    assert '外部先行｜報價 1｜已知paper WR —｜假設paper PnL —｜未知 1' in text
    assert '待核對 1' in text


def test_missing_official_shadow_result_is_unknown_and_report_read_only(tmp_path):
    with main_database(tmp_path) as db, feature_database(tmp_path) as feature:
        paper_quote(db, feature, winner=None)
        before_main, before_feature = list(db.iterdump()), list(feature.iterdump())
        text = render(tmp_path)
        assert list(db.iterdump()) == before_main and list(feature.iterdump()) == before_feature
    assert '外部先行｜報價 1｜已知paper WR —｜假設paper PnL —｜未知 1' in text


def test_selected_t69_before_first_loop_chooses_empty_t69(tmp_path):
    with main_database(tmp_path) as db:
        db.execute('DELETE FROM prediction_loops')
        db.execute("INSERT INTO prediction_runtime_config VALUES('prediction_selected_strategy',?)", (json.dumps({'profile': T69_PROFILE}),))
    assert t67_family_report_profile(tmp_path) == T69_PROFILE


@pytest.mark.asyncio
async def test_telegram_dispatch_uses_t69_family_only():
    service = PredictionTelegramService(object(), 1)
    service._deny_if_unauthorized = AsyncMock(return_value=False)
    service._reply = AsyncMock()
    formatter = Mock(return_value='T6.9 preview')
    with patch('src.gridbot.prediction.live_report.t67_family_report_profile', return_value=T69_PROFILE), \
            patch('src.gridbot.prediction.live_report.format_live_report', formatter):
        await service.cmd_predict_report(None, None)
    assert formatter.call_args.kwargs == {'profile_filter': T69_PROFILE}
    service._reply.assert_awaited_once_with(None, 'T6.9 preview', parse_mode=None)


@pytest.mark.asyncio
async def test_t69_shadow_command_does_not_run_legacy_report():
    service = PredictionTelegramService(object(), 1)
    service._deny_if_unauthorized = AsyncMock(return_value=False)
    service.cmd_predict_report = AsyncMock()
    with patch('src.gridbot.prediction.live_report.t67_family_report_profile', return_value=T69_PROFILE):
        await service.cmd_predict_shadow_report(None, None)
    service.cmd_predict_report.assert_awaited_once_with(None, None)


@pytest.mark.parametrize('column', ['market_topic_id', 'market_id'])
def test_unverified_market_identity_excluded_from_headline_fill_rate(tmp_path, column):
    with main_database(tmp_path) as db, feature_database(tmp_path) as feature:
        live_fill(db, feature)
        db.execute(f"UPDATE prediction_campaigns SET {column}='wrong'")
    text = render(tmp_path)
    assert 'Live fill rate 0.0%（0/1' in text
    assert '成交市場 0' in text
    assert '本輪 WR —' in text
    assert 'first DOWN｜成交 0' in text
    assert '待結算/核對 1' in text
    assert '成交市場登錄待核對1' in text
    assert '本輪已知淨 PnL +1.0000' not in text


def checkpoint(feature, offset=60000, *, status='EVALUATED', candidate=None, **changes):
    row = identity(checkpoint_ms=offset, market_end_ms=NOW,
                   observed_at_ms=START+offset, status=status, reasons=[], candidate=candidate)
    row.update(changes)
    feature.execute('INSERT INTO t69_reference_checkpoints VALUES(?,?,?)',
                    (START, offset, json.dumps(row)))
    feature.commit()


def coverage(root, now=NOW):
    with sqlite3.connect(root/'prediction/data/prediction.sqlite3') as db:
        db.row_factory = sqlite3.Row
        slots = [dict(row) for row in db.execute('SELECT * FROM prediction_regime_slots')]
    return checkpoint_metrics(root, now=now, loop_id='current', slots=slots,
                              fingerprint=FINGERPRINT)


def observer_table(db):
    db.execute('CREATE TABLE prediction_shadow_observer_markets('
               'market_topic_id TEXT,market_id TEXT,start_time_ms INTEGER,end_time_ms INTEGER,'
               'winner TEXT,payload_json TEXT,state TEXT)')


@pytest.mark.parametrize('side', ['UP', 'DOWN'])
def test_reference180_live_row_uses_actual_buy_and_official_net_settlement(tmp_path, side):
    with main_database(tmp_path) as db, feature_database(tmp_path) as feature:
        live_fill(db, feature, branch='reference_180_mid', side=side, pnl='2')
        paper_quote(db, feature, branch='reference_value', winner=side)
    text = render(tmp_path)
    assert '八路 Live＋六路 Shadow' in text
    assert 'Reference 180s補位｜成交 1｜已知WR 100.0%｜已知PnL +2.0000 USDT｜待結算 0' in text
    assert 'Live fill rate 100.0%（1/1' in text
    assert '本輪已知淨 PnL +2.0000 USDT' in text
    assert 'Reference 180s補位｜報價' not in text


def test_reference180_model_and_saved_paper_quote_are_never_live_fill_or_pnl(tmp_path):
    with main_database(tmp_path) as db, feature_database(tmp_path) as feature:
        admission(db)
        checkpoint(feature, 180000, candidate={'branch': 'reference_180_mid', 'side': 'UP'})
        feature.execute('INSERT INTO t69_decisions VALUES(?,?)',
                        (START, json.dumps(identity(selected=True, branch='reference_180_mid', side='UP'))))
        # Older candidate evidence cannot manufacture a BUY or become Live PnL.
        paper_quote(db, feature, branch='reference_180_mid', quoted_at_ms=START+180000,
                    book_at_ms=START+180000, source_receive_ms=START+180000)
    text = render(tmp_path)
    assert 'Reference 180s補位｜成交 0｜已知WR —｜已知PnL — USDT｜待結算 0' in text
    assert '成交市場 0' in text and '本輪已知淨 PnL — USDT' in text
    assert 'Live fill rate 0.0%（0/1' in text
    assert '180s｜評估 1/1 （候選 1／無訊號 0）' in text
    assert 'Reference 180s補位｜報價' not in text


def test_reference180_actual_pending_fill_stays_pending(tmp_path):
    with main_database(tmp_path) as db, feature_database(tmp_path) as feature:
        live_fill(db, feature, branch='reference_180_mid', settled=False)
    text = render(tmp_path)
    assert 'Reference 180s補位｜成交 1｜已知WR —｜已知PnL — USDT｜待結算 1' in text
    assert '本輪已知淨 PnL —（待核對／結算）' in text


def test_two_baseline_shadow_branches_keep_paper_performance_independent(tmp_path):
    with main_database(tmp_path) as db, feature_database(tmp_path) as feature:
        paper_quote(db, feature, branch='external_lead_lag', net_shares='2')
        paper_quote(db, feature, branch='reference_value', net_shares='3')
    text = render(tmp_path)
    for title, pnl in (('外部先行', '+1.0000'), ('Reference 校正', '+2.0000')):
        assert title+'｜報價 1｜已知paper WR 100.0%｜假設paper PnL '+pnl in text
    assert '成交市場 0' in text and '本輪已知淨 PnL — USDT' in text


def test_malformed_reference_checkpoint_does_not_hide_baseline_paper_performance(tmp_path):
    with main_database(tmp_path) as db, feature_database(tmp_path) as feature:
        paper_quote(db, feature, branch='reference_value')
        checkpoint(feature, 180000)
        feature.execute("UPDATE t69_reference_checkpoints SET payload='{' WHERE checkpoint_ms=180000")
    text = render(tmp_path)
    assert 'Reference 校正｜報價 1｜已知paper WR 100.0%｜假設paper PnL +1.0000' in text
    assert '180s｜評估 0/1' in text


def test_unverified_registration_does_not_poison_other_official_shadow_results(tmp_path):
    with main_database(tmp_path) as db, feature_database(tmp_path) as feature:
        paper_quote(db, feature)
        unverified_start = START-300000
        admission(db, unverified_start)
        db.execute('UPDATE prediction_regime_slots SET verified_at_ms=NULL WHERE market_start_ms=?',
                   (unverified_start,))
        observer_table(db)
        # This raw historical registration lacks parseable official identity.
        db.execute("INSERT INTO prediction_shadow_observer_markets VALUES('bad','',?,?, 'UP','{}','SETTLED')",
                   (unverified_start, START))
        db.execute('INSERT INTO prediction_campaigns VALUES(?,?,?,?,?,?,?,?,?)',
                   ('bad', 'current', unverified_start, 0, 'bad', '', START, 'DOWN', '{}'))
        db.execute("INSERT INTO prediction_settlements VALUES('bad','bad','SETTLED','0','DOWN')")
    text = render(tmp_path)
    assert '外部先行｜報價 1｜已知paper WR 100.0%｜假設paper PnL +1.0000' in text
    assert '外部先行｜報價待核對' not in text


def test_verified_observer_winner_conflict_still_blocks_paper_result(tmp_path):
    with main_database(tmp_path) as db, feature_database(tmp_path) as feature:
        paper_quote(db, feature)
        observer_table(db)
        db.execute("INSERT INTO prediction_shadow_observer_markets VALUES(?,?,?,?, 'DOWN','{}','SETTLED')",
                   ('topic'+str(START), 'up'+str(START), START, NOW))
    text = render(tmp_path)
    assert '外部先行｜報價 1｜已知paper WR —｜假設paper PnL —｜未知 1' in text
    assert '報價/官方勝方待核對 1' in text


def test_raw_official_detail_winner_conflict_is_not_ignored(tmp_path):
    from types import SimpleNamespace
    market = SimpleNamespace(market_topic_id='topic'+str(START), start_time_ms=START,
                             end_time_ms=NOW, up_market_id='up'+str(START))
    with main_database(tmp_path) as db, feature_database(tmp_path) as feature:
        paper_quote(db, feature)
        observer_table(db)
        db.execute("INSERT INTO prediction_shadow_observer_markets VALUES(?,'',?,?,'UP','{}','SETTLED')",
                   ('topic'+str(START), START, NOW))
    with patch('src.gridbot.prediction.models.MarketInfo.from_api', return_value=market), \
            patch('src.gridbot.prediction.worker.PredictionWorker._official_shadow_resolution', return_value='DOWN'):
        text = render(tmp_path)
    assert '外部先行｜報價待核對｜已知paper WR —｜假設paper PnL —' in text
    assert '假設paper PnL +1.0000' not in text


def test_checkpoint_candidates_no_signal_missed_and_absent_are_all_accounted(tmp_path):
    with main_database(tmp_path) as db, feature_database(tmp_path) as feature:
        admission(db)
        checkpoint(feature, 60000)
        checkpoint(feature, 120000, candidate={'branch': 'reference_180_mid', 'side': 'UP'})
        checkpoint(feature, 180000, status='MISSED_CHECKPOINT', observed_at_ms=START+182000)
    metrics, available = coverage(tmp_path)
    assert available
    assert metrics[60000]['due'] == metrics[60000]['evaluated'] == metrics[60000]['no_signal'] == 1
    assert metrics[120000]['evaluated'] == metrics[120000]['candidate'] == 1
    assert metrics[180000]['missed'] == 1 and metrics[180000]['evaluated'] == 0
    assert metrics[240000]['absent'] == 1 and metrics[240000]['evaluated'] == 0
    text = render(tmp_path)
    assert '60s｜評估 1/1 （候選 0／無訊號 1）' in text
    assert '180s｜評估 0/1' in text and '漏評 1／缺紀錄 0' in text
    assert '240s｜評估 0/1' in text and '漏評 0／缺紀錄 1' in text
    assert '成交市場 0' in text and 'Reference 180s補位｜成交 0' in text


def test_checkpoint_nested_model_candidate_and_unavailable_input_are_distinct(tmp_path):
    with main_database(tmp_path) as db, feature_database(tmp_path) as feature:
        admission(db)
        checkpoint(feature, 60000, status='INPUTS_UNAVAILABLE', reasons=['fresh_book_missing'])
        checkpoint(feature, 120000, evaluation={'candidate': {'side': 'DOWN'}})
        row = json.loads(feature.execute('SELECT payload FROM t69_reference_checkpoints WHERE checkpoint_ms=120000').fetchone()[0])
        del row['candidate']
        feature.execute('UPDATE t69_reference_checkpoints SET payload=? WHERE checkpoint_ms=120000',
                        (json.dumps(row),))
    metrics, _ = coverage(tmp_path)
    assert metrics[60000]['unavailable'] == 1 and metrics[60000]['no_signal'] == 0
    assert metrics[120000]['candidate'] == 1 and metrics[120000]['no_signal'] == 0


@pytest.mark.parametrize('changes', [dict(fingerprint='old'), dict(loop_id='other'), dict(market_id='wrong'),
                                   dict(observed_at_ms=NOW+1), dict(observed_at_ms=START+61501),
                                   dict(checkpoint_ms=120000), dict(status='BAD'), dict(candidate=[]),
                                   dict(reasons=None), dict(market_end_ms=NOW+1),
                                   dict(checkpoint_at_ms=START+60001), dict(evaluated_at_ms=START+60001)])
def test_invalid_checkpoint_cannot_be_counted_as_evaluated_or_no_signal(tmp_path, changes):
    with main_database(tmp_path) as db, feature_database(tmp_path) as feature:
        admission(db)
        checkpoint(feature, **changes)
    metric = coverage(tmp_path)[0][60000]
    assert metric['due'] == metric['unverified'] == 1
    assert metric['evaluated'] == metric['candidate'] == metric['no_signal'] == 0
    assert '60s｜評估 0/1' in render(tmp_path) and '待核對 1' in render(tmp_path)


def test_future_checkpoint_and_future_verified_slot_are_not_current_coverage(tmp_path):
    with main_database(tmp_path) as db, feature_database(tmp_path) as feature:
        admission(db)
        checkpoint(feature, 180000)
        admission(db, START+300000)
        db.execute('UPDATE prediction_regime_slots SET verified_at_ms=? WHERE market_start_ms=?',
                   (START+600000, START+300000))
    metrics, _ = coverage(tmp_path, now=START+120000)
    assert metrics[60000]['due'] == 1 and metrics[60000]['absent'] == 1
    assert metrics[180000]['due'] == metrics[180000]['evaluated'] == metrics[180000]['unverified'] == 0
    assert '180s｜尚無已到期登錄市場' in render(tmp_path, now=START+120000)


def test_missing_checkpoint_evidence_is_visible_instead_of_zero_signals(tmp_path):
    with main_database(tmp_path) as db, feature_database(tmp_path) as feature:
        admission(db)
        feature.execute('DROP TABLE t69_reference_checkpoints')
    metrics, available = coverage(tmp_path)
    assert not available and all(m['absent'] == m['due'] == 1 for m in metrics.values())
    text = render(tmp_path)
    assert '缺紀錄列入缺口，不推定無訊號' in text
    assert '240s｜評估 0/1' in text and '缺紀錄 1' in text


def test_checkpoint_reporting_is_read_only_and_exactly_repeatable(tmp_path):
    with main_database(tmp_path) as db, feature_database(tmp_path) as feature:
        admission(db)
        checkpoint(feature)
        before_main, before_feature = list(db.iterdump()), list(feature.iterdump())
        assert render(tmp_path) == render(tmp_path)
        assert list(db.iterdump()) == before_main and list(feature.iterdump()) == before_feature


def test_fixed_twenty_run_results_keep_old_risk_boundary_and_t69_loop_scope(tmp_path):
    from src.gridbot.prediction.regime_lane import FINGERPRINT as RISK_FP
    with main_database(tmp_path) as db, feature_database(tmp_path) as feature:
        db.execute("INSERT INTO prediction_runtime_config VALUES('regime_target6_risk_v1',?)",
                   (json.dumps(dict(fingerprint=RISK_FP, first_market_start_ms=START, halt_reason=None)),))
        live_fill(db, feature, start=START, pnl='2')
        live_fill(db, feature, start=START+19*300000, pnl='-1')
        live_fill(db, feature, start=START+20*300000, settled=False)
        admission(db, START+300000)
    text = render(tmp_path, now=START+21*300000)
    fixed = text.split('固定每20 run總結', 1)[1].split('Shadow（本輪報價研究）', 1)[0]
    first, second = fixed.split('第21–40 run')
    assert '本輪登錄已結3場｜成交2｜Fill 66.7%（2/3）' in first
    assert 'WR 50.0%（1勝/1負）｜已知PnL +1.0000 USDT｜MDD 1.0000' in first
    assert '共用風控已知MDD 1.0000 / 3.5U' in first
    assert '待结算' not in second and '待結算/核對1' in second
    assert '已知PnL —（待核對／結算）' in second
    assert '本輪績效僅含T6.9此loop' in fixed


@pytest.mark.parametrize('branch', ['flat_favorite', 'flat_quiet_favorite', 'flat_cheap_prior', 'flat_hold_180'])
def test_flat_shadow_rows_are_grouped_with_breakeven(tmp_path, branch):
    with main_database(tmp_path) as db, feature_database(tmp_path) as feature:
        paper_quote(db, feature, branch=branch, winner='UP')
    text = render(tmp_path)
    section = text.split('〔Flat F1–F4〕', 1)[1].split('Shadow報價不等於實際成交', 1)[0]
    title = SHADOW_LABELS[branch]
    assert f'{title}｜報價 1｜已知paper WR 100.0%｜假設paper PnL +1.0000 USDT｜未知 0' in section
    assert '兩平WR 50.0%（含費平均成本）' in section
    assert text.count('｜報價 0｜') == 5

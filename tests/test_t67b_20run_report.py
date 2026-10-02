"""Fixed risk epochs, observed Live accounting and bounded TG report pages."""
import json

import pytest

from test_t67b_report import main_database, feature_database, live_fill, admission, render
from test_live_report import START
from src.gridbot.prediction.live_report import report_pages
from src.gridbot.prediction.regime_lane import FINGERPRINT

SLOT = 300000


def gate(db, anchor=START, **changes):
    state = dict(fingerprint=FINGERPRINT, first_market_start_ms=anchor, halt_reason=None)
    state.update(changes)
    db.execute("INSERT INTO prediction_runtime_config VALUES('regime_target6_risk_v1',?)", (json.dumps(state),))
    db.commit()


def section(text):
    return text.split('固定每20 run總結', 1)[1].split('Shadow（本輪報價研究）', 1)[0]


def test_fixed_boundary_skips_and_pending_are_not_twenty_fills(tmp_path):
    with main_database(tmp_path) as db, feature_database(tmp_path) as f:
        gate(db)
        live_fill(db, f, start=START, pnl='2')
        live_fill(db, f, start=START+19*SLOT, pnl='-1')
        live_fill(db, f, start=START+20*SLOT, settled=False)
        admission(db, START+SLOT)  # Ended, registered no-fill market.
    text = section(render(tmp_path, now=START+21*SLOT))
    first, second = text.split('第21–40 run')
    assert '第1–20 run' in first and '時段已結束 20/20' in first
    assert '本輪登錄已結3場｜成交2｜Fill 66.7%（2/3）' in first
    assert 'WR 50.0%（1勝/1負）｜已知PnL +1.0000 USDT｜MDD 1.0000' in first
    assert '共用風控已知MDD 1.0000 / 3.5U' in first
    assert '進行中 1/20' in second and '待結算/核對1' in second
    assert '已知PnL —（待核對／結算）' in second
    assert '共用風控已知MDD —（待核對／結算）' in second


def test_epoch_is_cross_loop_and_shared_mdd_does_not_mix_current_pnl(tmp_path):
    with main_database(tmp_path) as db, feature_database(tmp_path) as f:
        gate(db, anchor=START-10*SLOT)
        live_fill(db, f, start=START, pnl='-2')
        # Historical T6 strategy contributes to shared risk, never current performance.
        db.execute("INSERT INTO prediction_loops VALUES('prior','regime_target6_5_v1','LIVE','DONE',100,100,0,0,0)")
        live_fill(db, f, start=START-SLOT, pnl='-3')
        cid='c'+str(START-SLOT)
        db.execute("UPDATE prediction_campaigns SET loop_id='prior' WHERE campaign_id=?", (cid,))
        db.execute("UPDATE prediction_regime_entry_claims SET loop_id='prior' WHERE campaign_id=?", (cid,))
        db.execute("DELETE FROM prediction_regime_slots WHERE market_start_ms=?", (START-SLOT,))
    text=section(render(tmp_path, now=START+SLOT))
    assert '第1–20 run' in text and '進行中 11/20' in text
    assert '已知PnL -2.0000 USDT｜MDD 2.0000' in text
    assert '共用風控已知MDD 5.0000 / 3.5U' in text
    assert '成交1' in text and '-5.0000 USDT' not in text


def test_mixed_units_normalize_shared_risk_only(tmp_path):
    with main_database(tmp_path) as db, feature_database(tmp_path) as f:
        gate(db)
        live_fill(db, f, pnl='-3')
        db.execute("UPDATE prediction_regime_entry_claims SET unit_usdt='3'")
    text=section(render(tmp_path))
    assert '已知PnL -3.0000 USDT｜MDD 3.0000' in text
    assert '共用風控已知MDD 1.0000 / 3.5U' in text


@pytest.mark.parametrize('damage', ['duplicate', 'mismatch', 'future', 'claim', 'nan'])
def test_invalid_observation_never_claims_shared_guard_passed(tmp_path, damage):
    with main_database(tmp_path) as db, feature_database(tmp_path) as f:
        gate(db)
        live_fill(db, f)
        if damage=='duplicate':
            db.execute("INSERT INTO prediction_settlements VALUES('duplicate',?,'SETTLED','1','DOWN')", ('c'+str(START),))
        elif damage=='mismatch':
            db.execute("UPDATE prediction_regime_settlement_observations SET net_pnl='-99'")
        elif damage=='future':
            db.execute('UPDATE prediction_regime_settlement_observations SET known_at_ms=?', (START+2*SLOT,))
        elif damage=='claim':
            db.execute("UPDATE prediction_regime_entry_claims SET market_start_ms=1")
        else:
            db.execute("UPDATE prediction_settlements SET net_pnl='NaN'")
    text=section(render(tmp_path))
    assert '共用風控MDD 待核對 / 3.5U；不推定通過' in text
    assert '待核對1' in text and '共用風控已知MDD 0.0000' not in text


@pytest.mark.parametrize('damage', ['missing', 'wrong_fp', 'off_grid'])
def test_missing_or_invalid_epoch_never_uses_loop_start_as_replacement(tmp_path, damage):
    with main_database(tmp_path) as db:
        admission(db)
        if damage=='wrong_fp':gate(db, fingerprint='wrong')
        elif damage=='off_grid':gate(db, anchor=START+1)
    assert '20 run區段待核對；不推定為已通過' in section(render(tmp_path))


def test_calendar_gap_is_not_silently_reported_as_registered_runs(tmp_path):
    with main_database(tmp_path) as db:
        gate(db)
        admission(db, START)
        admission(db, START+40*SLOT)
    text=section(render(tmp_path, now=START+41*SLOT))
    assert '第21–40 run' in text
    assert '本輪登錄已結0場｜成交0｜Fill —' in text
    assert '已知PnL —（無本輪登錄） USDT｜MDD —' in text
    assert '進行中 1/20' in text


def test_report_bounds_recent_blocks_and_telegram_pages(tmp_path):
    with main_database(tmp_path) as db:
        gate(db)
        admission(db, START)
        admission(db, START+120*SLOT)
    text=render(tmp_path, now=START+121*SLOT)
    segment=section(text)
    assert '共7段；顯示最近5段' in segment
    assert '第1–20 run' not in segment and '第41–60 run' in segment and '第121–140 run' in segment
    assert len(report_pages(text))>=1
    assert all(len(page.encode('utf-16-le'))//2<=3400 for page in report_pages(text))


def test_render_is_read_only_and_repeatable(tmp_path):
    with main_database(tmp_path) as db, feature_database(tmp_path) as f:
        gate(db)
        live_fill(db, f, pnl='-1')
    main=tmp_path/'prediction/data/prediction.sqlite3'
    before=main.read_bytes()
    assert render(tmp_path)==render(tmp_path)
    assert main.read_bytes()==before


def test_preclose_observation_is_excluded_from_loop_and_block_results(tmp_path):
    with main_database(tmp_path) as db, feature_database(tmp_path) as f:
        gate(db)
        live_fill(db, f, pnl='9')
        db.execute('UPDATE prediction_regime_settlement_observations SET known_at_ms=?', (START+SLOT-1,))
    text=render(tmp_path)
    assert '本輪已知淨 PnL —（待核對／結算）' in text and '本輪 WR —' in text
    assert '+9.0000' not in text and '結算觀測時間待核對1' in text
    assert '共用風控MDD 待核對' in section(text)


@pytest.mark.parametrize('valid_other_slot', [False, True])
def test_unverified_fill_is_visible_for_reconciliation_not_performance(tmp_path, valid_other_slot):
    with main_database(tmp_path) as db, feature_database(tmp_path) as f:
        gate(db)
        live_fill(db, f, pnl='9')
        db.execute('UPDATE prediction_regime_slots SET verified_at_ms=NULL')
        if valid_other_slot:admission(db, START+SLOT)
    text=render(tmp_path, now=START+2*SLOT)
    batch=section(text)
    assert '+9.0000' not in text and '成交市場登錄待核對1' in text
    assert '未驗證成交待核對1' in batch
    if valid_other_slot:
        assert '本輪登錄已結1場｜成交0｜Fill 0.0%（0/1）' in batch
    else:
        assert '尚無已驗證登錄市場' in batch


@pytest.mark.parametrize('exposure', ['unknown_intent', 'open_intent', 'unknown_order', 'open_order', 'campaign'])
def test_other_shared_lane_unresolved_order_without_fill_is_not_guard_pass(tmp_path, exposure):
    with main_database(tmp_path) as db:
        gate(db)
        admission(db)
        db.execute("INSERT INTO prediction_loops VALUES('prior','regime_target6_5_v1','LIVE','DONE',100,100,0,0,0)")
        db.execute("INSERT INTO prediction_campaigns(campaign_id,loop_id,start_time_ms,pending_unknown) VALUES('exposure','prior',?,?)", (START, int(exposure=='campaign')))
        if exposure in ('unknown_intent','open_intent'):
            db.execute("INSERT INTO prediction_order_intents VALUES('unresolved','exposure',?,NULL,?,NULL)", ('REJECTED' if exposure=='unknown_intent' else 'SUBMITTING',int(exposure=='unknown_intent')))
        elif exposure in ('unknown_order','open_order'):
            db.execute("INSERT INTO prediction_orders VALUES('open','exposure',?)", ('UNKNOWN' if exposure=='unknown_order' else 'NEW',))
    text=section(render(tmp_path))
    assert '共用風控MDD 待核對 / 3.5U；不推定通過' in text and '待核對1' in text
    assert '共用風控已知MDD 0.0000' not in text


def test_terminal_rejection_without_unknown_is_not_new_exposure(tmp_path):
    with main_database(tmp_path) as db:
        gate(db)
        admission(db)
        db.execute("INSERT INTO prediction_campaigns(campaign_id,loop_id,start_time_ms) VALUES('rejected','current',?)", (START,))
        db.execute("INSERT INTO prediction_order_intents VALUES('rejected','rejected','REJECTED',NULL,0,NULL)")
    assert '共用風控已知MDD 0.0000 / 3.5U' in section(render(tmp_path))

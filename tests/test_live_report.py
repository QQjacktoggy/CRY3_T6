import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock

from src.gridbot.prediction.live_report import format_live_report, report_pages, _metrics
from src.gridbot.prediction.regime_lane import FINGERPRINT
from decimal import Decimal as D

START=1790532000000
SCHEMA="""
CREATE TABLE prediction_loops(loop_id TEXT PRIMARY KEY,strategy_profile TEXT,mode TEXT,state TEXT,target INTEGER,completed INTEGER,created_at_ms INTEGER,new_entries_stopped INTEGER DEFAULT 0,hard_stop_latched INTEGER DEFAULT 0);
CREATE TABLE prediction_campaigns(campaign_id TEXT PRIMARY KEY,loop_id TEXT,start_time_ms INTEGER,pending_unknown INTEGER DEFAULT 0);
CREATE TABLE prediction_regime_slots(loop_id TEXT,market_start_ms INTEGER,run_ordinal INTEGER,verified_at_ms INTEGER,empty_attested_at_ms INTEGER);
CREATE TABLE prediction_regime_entry_claims(loop_id TEXT,market_start_ms INTEGER,campaign_id TEXT,intent_id TEXT,unit_usdt TEXT);
CREATE TABLE prediction_order_intents(intent_id TEXT,campaign_id TEXT,status TEXT,order_id TEXT,unknown INTEGER,submission_at_ms INTEGER);
CREATE TABLE prediction_fills(campaign_id TEXT,order_side TEXT);
CREATE TABLE prediction_settlements(settlement_id TEXT,campaign_id TEXT,status TEXT,net_pnl TEXT);
CREATE TABLE prediction_regime_settlement_observations(settlement_id TEXT,campaign_id TEXT,net_pnl TEXT,known_at_ms INTEGER);
CREATE TABLE prediction_runtime_config(config_key TEXT,config_value_json TEXT);
"""


class ReportTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.root=Path(self.tmp.name)
        (self.root/'prediction/data').mkdir(parents=True)
        self.db=sqlite3.connect(self.root/'prediction/data/prediction.sqlite3')
        self.db.executescript(SCHEMA)
        self.loop('old','c180_favorite_hold_v1','CANCELLED',1)
        self.loop('new','regime_target6_v1','RUNNING',2)
    def tearDown(self):
        self.db.close();self.tmp.cleanup()
    def loop(self,id,profile,state,created,mode='LIVE'):
        self.db.execute('INSERT INTO prediction_loops(loop_id,strategy_profile,mode,state,target,completed,created_at_ms) VALUES(?,?,?,?,100,0,?)',
                        (id,profile,mode,state,created));self.db.commit()
    def gate(self,**changes):
        data=dict(fingerprint=FINGERPRINT,first_market_start_ms=START,halt_reason=None)
        data.update(changes)
        self.db.execute('DELETE FROM prediction_runtime_config')
        self.db.execute('INSERT INTO prediction_runtime_config VALUES(?,?)',('regime_target6_risk_v1',json.dumps(data)))
        self.db.commit()
    def fill(self,id='c',loop='new',start=START,pnl='1',observed=True,status='SETTLED',unknown=0):
        self.db.execute('INSERT INTO prediction_campaigns VALUES(?,?,?,?)',(id,loop,start,unknown))
        self.db.execute('INSERT INTO prediction_regime_slots VALUES(?,?,?,?,NULL)',(loop,start,(start-START)//300000+1,start))
        self.db.execute('INSERT INTO prediction_regime_entry_claims VALUES(?,?,?,?,?)',(loop,start,id,'i'+id,'1'))
        self.db.execute('INSERT INTO prediction_order_intents VALUES(?,?,?,?,?,?)',('i'+id,id,'FILLED','o'+id,unknown,start+125000))
        self.db.execute("INSERT INTO prediction_fills VALUES(?,'BUY')",(id,))
        self.db.execute('INSERT INTO prediction_settlements VALUES(?,?,?,?)',('s'+id,id,status,pnl))
        if observed:self.db.execute('INSERT INTO prediction_regime_settlement_observations VALUES(?,?,?,?)',('s'+id,id,pnl,start+300000))
        self.db.commit()
    def render(self,**kwargs):
        return format_live_report(self.root,now_ms=START+10000000,**kwargs)
    def test_active_new_loop_without_trades_is_not_old_report(self):
        text=self.render()
        self.assertIn('Regime T6 Live Report',text)
        self.assertIn('Loop new',text)
        self.assertIn('本輪 WR —',text)
        self.assertIn('風控尚未初始化',text)
        self.assertNotIn('C180',text)
        self.assertNotIn('0.0%',text)
    def test_confirmed_win_loss_zero_no_double_fee(self):
        self.gate(net_pnl_usdt='1')
        for id,start,pnl in [('w',START,'2'),('l',START+300000,'-1'),('z',START+600000,'0')]:
            self.fill(id,start=start,pnl=pnl)
        text=self.render()
        self.assertIn('WR 50.0%（1勝/1負/1平；已結算成交 3）',text)
        self.assertIn('本輪已知淨 PnL +1.0000 USDT',text)
        self.assertIn('MDD 1.0000 USDT',text)
    def test_pending_does_not_become_zero_pnl(self):
        self.gate();self.fill(status='CLOSED_PENDING_REDEEM',observed=False)
        text=self.render()
        self.assertIn('本輪已知淨 PnL —（待核對／結算）',text)
        self.assertIn('待結算/核對 1',text)
    def test_observation_mismatch_excluded(self):
        self.gate();self.fill()
        self.db.execute("UPDATE prediction_regime_settlement_observations SET net_pnl='3'");self.db.commit()
        text=self.render()
        self.assertIn('已結算成交 0',text)
        self.assertIn('官方與風控結算觀測待核對',text)
    def test_duplicate_settlement_excluded(self):
        self.gate();self.fill()
        self.db.execute("INSERT INTO prediction_settlements VALUES('duplicate','c','SETTLED','1')");self.db.commit()
        text=self.render()
        self.assertIn('官方結算重複',text)
        self.assertIn('本輪 WR —',text)
    def test_no_fill_settlement_not_a_trade(self):
        self.gate();self.fill(pnl='0')
        self.db.execute('DELETE FROM prediction_fills');self.db.commit()
        self.assertIn('已結算成交 0',self.render())
    def test_cross_loop_accounting_and_global_batches(self):
        self.loop('prior','regime_target6_v1','DONE',0)
        self.fill('before',loop='prior',start=START,pnl='-1')
        self.fill('after',start=START+20*300000,pnl='2')
        self.gate(net_pnl_usdt='1',halt_reason='scheduled20_mdd_3.5')
        text=self.render()
        self.assertIn('本輪已知淨 PnL +2.0000',text)
        self.assertIn('累計已知淨 PnL +1.0000',text)
        self.assertIn('第21–40場',text)
        self.assertIn('持久停單：20場',text)
        self.assertIn('不自動恢復、不自動解鎖',text)
        self.assertNotIn('恢復狀態 SHADOW',text)
    def test_t61_report_shares_t6_risk_without_mixing_loop_pnl(self):
        self.db.execute("UPDATE prediction_loops SET strategy_profile='regime_target6_1_v1' WHERE loop_id='new'")
        self.db.commit()
        self.loop('prior','regime_target6_v1','DONE',0)
        self.fill('before',loop='prior',start=START,pnl='-1')
        self.fill('after',loop='new',start=START+20*300000,pnl='2')
        self.gate(net_pnl_usdt='1')
        text=self.render()
        self.assertIn('Regime T6.1 Live Report',text)
        self.assertIn('本輪已知淨 PnL +2.0000',text)
        self.assertIn('累計已知淨 PnL +1.0000',text)
        self.assertIn('T6／T6.1 共用',text)
        self.assertNotIn('風控累計與可核對結算不一致',text)
    def test_unknown_and_unsubmitted_claim_are_distinct(self):
        self.fill(unknown=1,status='PENDING',observed=False)
        self.db.execute('UPDATE prediction_order_intents SET order_id=NULL,submission_at_ms=NULL,status=?',('PENDING',));self.db.commit()
        text=self.render()
        self.assertIn('進場intent 1｜送單嘗試 0',text)
        self.assertIn('未知訂單市場 1',text)
    def test_c180_fallback_bound_to_selected_loop(self):
        self.db.execute("UPDATE prediction_loops SET state='DONE' WHERE loop_id='new'");self.db.commit()
        self.loop('c180','c180_favorite_hold_v1','RUNNING',3)
        formatter=Mock(return_value='old lane output')
        self.assertEqual(self.render(c180_formatter=formatter),'old lane output')
        self.assertEqual(formatter.call_args.kwargs['loop_id'],'c180')
    def test_finished_latest_loop_still_new_lane(self):
        self.db.execute("UPDATE prediction_loops SET state='DONE'");self.db.commit()
        self.assertIn('Loop new',self.render())
    def test_shadow_not_mislabelled_live(self):
        self.db.execute("UPDATE prediction_loops SET mode='SHADOW' WHERE loop_id='new'");self.db.commit()
        self.assertIn('目前為 SHADOW',self.render())
        self.assertNotIn('Regime T6 Live Report',self.render())
    def test_missing_claim_or_future_observation_not_counted(self):
        self.fill()
        self.db.execute('DELETE FROM prediction_regime_entry_claims');self.db.commit()
        self.assertIn('成交與lane claim不一致',self.render())
    def test_report_is_read_only(self):
        self.gate();self.fill()
        before='\n'.join(self.db.iterdump())
        self.render()
        self.assertEqual(before,'\n'.join(self.db.iterdump()))
    def test_mdd_matches_risk_order_with_same_timestamp(self):
        metric=_metrics([{'known':1,'id':'b','pnl':D('-1')},{'known':1,'id':'a','pnl':D('2')}])
        self.assertEqual(metric['mdd'],D('1'))
    def test_pages_are_safe_for_telegram(self):
        text='\n'.join(['📊 報表測試'*40]*100)
        pages=report_pages(text)
        self.assertGreater(len(pages),1)
        self.assertTrue(all(len(x.encode('utf-16-le'))//2<=3400 for x in pages))
        self.assertEqual('\n'.join(pages),text)


if __name__=='__main__':unittest.main(verbosity=2)

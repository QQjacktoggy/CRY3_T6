import asyncio,json,tempfile,unittest
from pathlib import Path
from decimal import Decimal as D
from types import SimpleNamespace
from unittest.mock import patch
from src.gridbot.prediction import regime_t63_lane as lane,regime_worker_bridge as bridge
from src.gridbot.prediction.regime_feature_service import connect
from src.gridbot.prediction.regime_lane import FINGERPRINT as RISK_FP,risk_result
from src.gridbot.prediction.regime_live_ledger import RegimeLiveLedger
from src.gridbot.prediction.repository import PredictionRepository
from src.gridbot.prediction.worker import PredictionWorker
from src.gridbot.prediction.strategy import StrategyConfig
from src.gridbot.prediction.telegram import selectable_lanes_for_market,_regime_risk_text
from src.gridbot.prediction.c180_gate_runtime import LiveSettlement
S=1790532600000
def feature(a,b,p=2):
 return dict(first_bp=str(a),last_bp=str(b),prior_bp=str(p),market_start_ms=S,cutoff_ms=S+120000,received_at_ms=S+120500,fingerprint=RISK_FP)
def book(up='.7',down='.3',t=125000):
 return dict(full_depth=True,market_start_ms=S,market_topic='topic',market_id='up',fee_bps=200,
             book_at_ms=S+t,received_at=S+t,received_at_ms=S+t,captured_at_ms=S+t,
             quote={side:{'ask_levels':[[price,'100']]} for side,price in [('UP',up),('DOWN',down)]})
def original(p='.2'):
 return dict(market_start_ms=S,cutoff_ms=S+120000,completed_at_ms=S+120500,original_p_up=p)
class Policy(unittest.TestCase):
 def test_new_branches(self):
  self.assertEqual(lane.candidates(feature(-2,4),original(),book(),D(1))[0]['branch'],'A_jev_conflict')
  self.assertEqual(lane.candidates(feature('.1',2),None,book('.6','.4'),D(1))[0]['branch'],'B_late_momentum')
  self.assertEqual(lane.candidates(feature(2,-4,-2),None,book('.3','.7'),D(1))[0]['branch'],'C_reversal_netdown')
 def test_jev_cutoff_and_prior(self):
  old=original();old['completed_at_ms']=S+123001
  self.assertEqual(lane.candidates(feature(-2,4),old,book(),D(1))[0]['action'],'net_up')
  self.assertEqual(lane.candidates(feature('.1',2,-2),None,book(),D(1)),[])
 def test_depth_amount_cap(self):
  c=lane.candidates(feature(-2,4),original(),book(),D(1))[0]
  for u in (D(1),D(2),D(3)):
   ex=lane.eligible_execution(c,book(),u);self.assertLessEqual(ex['cash'],u);self.assertGreater(ex['cash'],u-D('.01'))
  with self.assertRaises(ValueError): lane.eligible_execution(c,book(down='.41'),D(1))
  thin=book();thin['quote']['DOWN']['ask_levels']=[['.3','2']]
  with self.assertRaises(ValueError): lane.eligible_execution(c,thin,D(1))
 def test_wiring_and_risk(self):
  self.assertIn(lane.PROFILE,PredictionWorker._selectable_strategy_profiles())
  self.assertIn(lane.PROFILE,dict(selectable_lanes_for_market('BTCUSDT')))
  self.assertNotIn(lane.PROFILE,dict(selectable_lanes_for_market('ETHUSDT')))
  self.assertEqual(StrategyConfig.for_profile(lane.PROFILE).provenance_payload['regime_policy_fingerprint'],lane.FINGERPRINT)
  self.assertEqual(RegimeLiveLedger(None,profile=lane.PROFILE).tier,'REGIME_T63')
  for u in (D(1),D(2),D(3)):
   cfg=PredictionWorker._sized_strategy_config(lane.PROFILE,u);self.assertEqual(cfg.max_buy_usdt,u)
   self.assertIn(f'MDD≥{D("3.5")*u} USDT',_regime_risk_text(lane.PROFILE,u))
   self.assertIn(f'累計PnL≤-{int(6*u)} USDT',_regime_risk_text(lane.PROFILE,u))
   state=dict(fingerprint=RISK_FP,first_market_start_ms=S,halt_reason=None)
   rows=[LiveSettlement(str(n),S+n*300000,-u,S+(n+1)*300000,u) for n in range(4)]
   self.assertEqual(risk_result(state,rows,S+1500000,S+1800000)[1],'scheduled20_mdd_3.5')
   state=dict(fingerprint=RISK_FP,first_market_start_ms=S,halt_reason=None)
   rows=[LiveSettlement(str(n),S+n*300000,-u,S+(n+1)*300000,u) for n in (0,1,2,20,21,22)]
   self.assertEqual(risk_result(state,rows,S+6900000,S+7200000)[1],'cumulative_loss_6')
 def test_bridge_freeze_recheck(self):
  with tempfile.TemporaryDirectory() as tmp:
   path=Path(tmp)/'f.db';db=connect(path)
   with db: db.execute('INSERT INTO features VALUES(?,?)',(S,json.dumps(feature('.1',2))))
   db.close(); obj=bridge.RegimeWorkerBridge(None,Path(tmp)/'signals',feature_db=path,profile=lane.PROFILE)
   market=SimpleNamespace(start_time_ms=S,market_topic_id='topic',up_market_id='up')
   def check(t=125000,unit=D(1),seen=0):return obj.check_signal(market=market,unit_usdt=unit,at_ms=S+t,last_seen_book_at_ms=seen)
   with patch.object(obj,'_first_book',return_value=book('.6','.4')),patch.object(bridge,'read_c180_signal',return_value=None),patch.object(bridge,'read_c180_book',return_value=book('.6','.4')):
    ready=check();self.assertTrue(ready.allowed,ready.reason);self.assertEqual(ready.signal.entry.side,'UP')
    self.assertFalse(check(unit=D(2)).allowed)
    self.assertFalse(check(seen=S+125000).allowed)
    self.assertFalse(check(t=136000).allowed)
   with patch.object(bridge,'read_c180_book',return_value=book('.66','.34',126000)):
    self.assertFalse(check(t=126000).allowed)
   with patch.object(bridge,'read_c180_book',return_value=book('.59','.41',126000)):
    self.assertTrue(check(t=126000).allowed)
 def test_pending_depth_retries(self):
  with tempfile.TemporaryDirectory() as tmp:
   path=Path(tmp)/'f.db';db=connect(path)
   with db: db.execute('INSERT INTO features VALUES(?,?)',(S,json.dumps(feature('.1',2))))
   db.close();obj=bridge.RegimeWorkerBridge(None,'unused',feature_db=path,profile=lane.PROFILE)
   market=SimpleNamespace(start_time_ms=S,market_topic_id='topic',up_market_id='up')
   with patch.object(obj,'_first_book',return_value=book('.7','.3')),patch.object(bridge,'read_c180_signal',return_value=None),patch.object(bridge,'read_c180_book',return_value=book('.7','.3')):
    self.assertFalse(obj.check_signal(market=market,unit_usdt=D(1),at_ms=S+125000,last_seen_book_at_ms=0).allowed)
   with patch.object(bridge,'read_c180_book',return_value=book('.6','.4',128000)):
    self.assertTrue(obj.check_signal(market=market,unit_usdt=D(1),at_ms=S+128000,last_seen_book_at_ms=0).allowed)
class Ledger(unittest.IsolatedAsyncioTestCase):
 async def test_profile_sql_and_shared_state(self):
  with tempfile.TemporaryDirectory() as tmp:
   repo=PredictionRepository(Path(tmp)/'db');await repo.initialize()
   try:
    await repo.start_loop('t63-test',100,mode='LIVE',strategy_profile=lane.PROFILE)
    ledger=RegimeLiveLedger(repo,profile=lane.PROFILE)
    await ledger.seed_schedule(loop_id='t63-test',first_market_start_ms=S)
    result=await ledger.check_risk('t63-test',S,S+124000)
    self.assertTrue(result[0],result)
   finally:await repo.close()
if __name__=='__main__':unittest.main()

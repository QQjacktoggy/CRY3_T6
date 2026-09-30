"""Run T6.1 latency-fix tests against an isolated copy of the VM source."""
import compileall
import importlib.util
import json
import pathlib
import shutil
import sys
import tempfile
import unittest
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import patch

from src.gridbot.prediction import regime_lane as t6
from src.gridbot.prediction import regime_t61_lane as t61
from src.gridbot.prediction.regime_feature_service import connect
from src.gridbot.prediction.c180_signal_runtime import C180SignalStore
from src.gridbot.prediction.regime_worker_bridge import RegimeWorkerBridge
from src.gridbot.prediction.regime_live_ledger import RegimeLiveLedger
from src.gridbot.prediction.strategy import StrategyConfig
from src.gridbot.prediction.worker import PredictionWorker
from src.gridbot.prediction.telegram import selectable_lanes_for_market
from src.gridbot.prediction.repository import PredictionRepository
from src.gridbot.prediction.models import MarketInfo, Campaign

START = 1790265600000
import test_regime_lane as baseline_tests


def feature(first, last, prior, net):
    return dict(market_start_ms=START, first_bp=str(first), last_bp=str(last),
                prior_bp=str(prior), net_bp=str(net), received_at_ms=START+120200,
                cutoff_ms=START+120000, fingerprint=t6.FINGERPRINT)


def original(side):
    return dict(status='entry_positive_cost_after_ev', completed_at_ms=START+120500,
                cutoff_ms=START+120000, market_start_ms=START,
                entry={'side': side}, original_p_up='0.8' if side == 'UP' else '0.2')


def book(price, offset=124100):
    return dict(market_start_ms=START, market_topic='topic', market_id='up',
                book_at_ms=START+offset-100, received_at=START+offset,
                received_at_ms=START+offset, captured_at_ms=START+offset,
                full_depth=True, fee_bps=200,
                quote={s: {'ask_levels': [[str(price), '100']]} for s in ('UP','DOWN')})


class SelectionTests(unittest.TestCase):
    def test_fallback_states(self):
        cases = [
            (feature(2,2,0,4), None, 'continuation', 'DOWN', '0.35', '0.45'),
            (feature(-2,2,0,1), None, 'reversal', 'UP', '0.65', '0.75'),
            (feature(2,.1,-2,2), original('DOWN'), 'stall', 'DOWN', '0.25', '0.45'),
            (feature(.1,.1,0,.2), None, 'flat', 'DOWN', '0.45', '0.75'),
        ]
        for f, o, state, side, low, high in cases:
            base=t6.select_side(f,o)
            self.assertFalse(base['allowed'], state)
            got=t61.select_fallback(f,o,base)
            self.assertTrue(got['allowed'],got)
            self.assertEqual((got['state'],got['side'],got['lower'],got['upper']),
                             (state,side,low,high))
        late=feature(.1,2,0,2)
        self.assertFalse(t61.select_fallback(late,None,t6.select_side(late,None))['allowed'])

    def test_net_threshold_and_missing_feature_fail_closed(self):
        f=feature(-2,2,0,.99)
        self.assertEqual(t61.select_fallback(f,None,t6.select_side(f,None))['reason'],'net_below_1bp')
        del f['net_bp']
        with self.assertRaises(KeyError):t61.select_fallback(f,None,t6.select_side(f,None))

    def test_t6_priority_and_fixed_risk(self):
        f=feature(2,2,-2,4)
        base=t6.select_side(f,original('UP'))
        self.assertTrue(base['allowed'])
        with self.assertRaises(ValueError):t61.select_fallback(f,original('UP'),base)
        config=StrategyConfig.for_profile(t61.PROFILE)
        self.assertEqual(config.max_buy_usdt,Decimal(1))
        self.assertEqual(PredictionWorker._sized_strategy_config(t61.PROFILE,Decimal(3)).max_buy_usdt,Decimal(1))
        self.assertIn(t61.PROFILE,PredictionWorker._selectable_strategy_profiles())
        self.assertIn(t61.PROFILE,dict(selectable_lanes_for_market('BTCUSDT')))
        self.assertNotIn(t61.PROFILE,dict(selectable_lanes_for_market('ETHUSDT')))
        ledger=RegimeLiveLedger(None,profile=t61.PROFILE)
        self.assertEqual((ledger.state_key,ledger.tier,ledger.max_price),
                         (t6.STATE_KEY,t61.TIER,Decimal('.75')))
        old=RegimeLiveLedger(None)
        self.assertEqual((old.tier,old.max_price),('REGIME_T6',Decimal('.65')))

    def test_net_feature_exact(self):
        candles=[[START-900000+i*60000,'100','101','99','100',0,
                  START-900000+i*60000+59999] for i in range(17)]
        candles[15][1]='100';candles[15][4]='101'
        candles[16][1]='101';candles[16][4]='102'
        result=t6.freeze_features(START,candles,START+120200)
        self.assertEqual(Decimal(result['net_bp']),Decimal('200'))
        self.assertEqual(result['fingerprint'],t6.FINGERPRINT)



class BridgeTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.path=pathlib.Path(self.tmp.name)
        self.features=connect(self.path/'features.db')
        self.store=C180SignalStore(self.path/'signals.db')
        self.market=SimpleNamespace(start_time_ms=START,market_topic_id='topic',up_market_id='up')
        self.bridge=RegimeWorkerBridge(None,self.path/'signals.db',feature_db=self.path/'features.db',
                                       profile=t61.PROFILE)
    def tearDown(self):
        self.features.close();self.store.close();self.tmp.cleanup()
    def check(self,offset=124200):
        return self.bridge.check_signal(market=self.market,unit_usdt=Decimal(1),
            at_ms=START+offset,last_seen_book_at_ms=START+120000)
    def seed(self,f,p):
        with self.features:
            self.features.execute('INSERT INTO features VALUES(?,?)',(START,json.dumps(f)))
        self.store.persist_book(book(p))
    def test_fallback_ready_and_price_cap(self):
        self.seed(feature(2,2,0,4),'.40')
        ready=self.check()
        self.assertTrue(ready.allowed,ready.reason)
        self.assertEqual(ready.signal.entry.side,'DOWN')
        saved=json.loads(self.features.execute('SELECT payload FROM decisions').fetchone()[0])
        self.assertEqual((saved['branch'],saved['fingerprint']),('fallback',t61.FINGERPRINT))
        self.assertEqual(ready.execution.worst_ask_limit,Decimal('.40'))
    def test_old_rule_has_priority(self):
        # An original signal is not persisted here, so continuation falls back.
        # This test verifies the bridge cannot accept a wrong stake.
        self.seed(feature(2,2,0,4),'.40')
        self.assertFalse(self.bridge.check_signal(market=self.market,unit_usdt=Decimal(2),
            at_ms=START+124200,last_seen_book_at_ms=START+120000).allowed)
    def test_stale_missing_and_high_price_skip(self):
        self.seed(feature(-2,2,0,1),'.76')
        self.assertFalse(self.check().allowed)
        saved=json.loads(self.features.execute('SELECT payload FROM decisions').fetchone()[0])
        self.assertEqual(saved['reason'],'price_band')


class CrossLaneLedgerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        await baseline_tests.LedgerTests.asyncSetUp(self)
    async def asyncTearDown(self):
        await baseline_tests.LedgerTests.asyncTearDown(self)
    async def test_claim_uses_shared_epoch_and_lock(self):
        await self.ledger.check_risk('loop1',START,START+125000)
        initial=await self.repo.get_runtime_config(t6.STATE_KEY,None)
        self.assertEqual(initial['first_market_start_ms'],START)
        second=START+20*t6.SLOT_MS
        await self.repo.start_loop('loop2',20,mode='LIVE',strategy_profile=t61.PROFILE)
        market=MarketInfo('topic2','up2','test',second,second+t6.SLOT_MS,
                          up_market_id='up2',down_market_id='down2')
        await self.repo.save_campaign(Campaign('c2',market),loop_id='loop2')
        ledger=RegimeLiveLedger(self.repo,profile=t61.PROFILE)
        await ledger.seed_schedule(loop_id='loop2',first_market_start_ms=second)
        await ledger.verify_market(loop_id='loop2',market_start_ms=second,
                                   market_topic_id='topic2',market_id='up2',
                                   verified_at_ms=second+124000)
        intent={**self.intent,'intent_id':'i2','campaign_id':'c2',
                'created_at_ms':second+125000,'limit_price':'.70','tier':t61.TIER}
        with patch('src.gridbot.prediction.regime_live_ledger._now_ms',return_value=second+125000):
            claimed=await ledger.reserve_c180_intent(loop_id='loop2',market_start_ms=second,
                campaign_id='c2',intent=intent,decision_at_ms=second+125000,
                wallet_reconciled_at_ms=second+125000)
        self.assertTrue(claimed.claimed,claimed.reason)
        persisted=await self.repo.get_runtime_config(t6.STATE_KEY,None)
        self.assertEqual(persisted['first_market_start_ms'],START)
        self.assertIsNone(persisted['halt_reason'])
        rows=await self.repo._fetchall('SELECT tier,amount,limit_price FROM prediction_order_intents')
        self.assertEqual([(r['tier'],Decimal(r['amount']),Decimal(r['limit_price'])) for r in rows],
                         [(t61.TIER,Decimal('1'),Decimal('0.70'))])
        persisted['halt_reason']='manual_test_halt'
        await self.repo.set_runtime_config(t6.STATE_KEY,persisted)
        allowed,reason=await ledger.check_risk('loop2',second+t6.SLOT_MS,second+t6.SLOT_MS)
        self.assertFalse(allowed)
        self.assertEqual(reason,'manual_test_halt')


if __name__ == '__main__':
    unittest.main(verbosity=2)

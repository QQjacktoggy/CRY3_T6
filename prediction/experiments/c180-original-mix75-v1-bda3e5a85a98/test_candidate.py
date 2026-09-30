import asyncio
import copy
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from candidate_engine import Engine,ENTRY,ACCOUNT,HOLD
from candidate_report import summarize,write_report
from store import Store
from logic import advance
from test_v2 import market,book,tape_at,response

class CandidateTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.store=Store(Path(self.tmp.name)/'db','test',5,'candidate')
        self.store.set('start',0);self.store.set('target',1)
        self.engine=Engine(self.store)
        self.calls=[]

    def tearDown(self):
        self.store.db.close();self.tmp.cleanup()

    def decide(self,phase,cut,p=.8,quote=None,recover=False):
        packet=self.engine.freeze(tape_at(cut).packet(market(),cut),phase)
        if quote is not None:packet['state']['quote']=quote
        async def infer(session,key,state,questions):
            self.calls.append(copy.deepcopy(state))
            import json
            return dict(status='ok',cost=.001,started=cut,completed=cut+100,response=json.dumps(response(questions,p)))
        self.store.infer=infer
        with patch('engine.now',return_value=cut+200),patch('candidate_engine.now',return_value=cut+200):
            asyncio.run(self.engine.decide(packet,None,None,recover))
        return packet

    def fill(self):
        self.decide('C180',120000)
        e=self.engine.orders['0:'+ENTRY]
        advance(e,book(124000),market(),124000)
        self.store.put('orders',e['id'],e)
        return e

    def test_candidate_uses_existing_calls_and_preserves_baseline_holding(self):
        self.decide('E10',10000)
        e=self.engine.orders['0:E10'];advance(e,book(14000),market(),14000)
        self.store.put('orders',e['id'],e)
        self.fill();packet=self.decide('C90',210000)
        self.assertEqual(len(self.calls),12)
        self.assertEqual(packet['state']['holding']['shares'],e['shares'])
        self.assertNotIn(ENTRY,packet['state']['holdings'])

    def test_mix_value_exit_and_hold_control_are_separate(self):
        e=self.fill();self.decide('C90',210000,p=.01)
        x=self.engine.orders['0:C90:'+ACCOUNT]
        self.assertAlmostEqual(x['p_side'],.75*.5+.25*.01)
        advance(x,book(214000),market(),214000);self.store.put('orders',x['id'],x)
        self.engine.resolve('x','DOWN',301000)
        r=summarize(self.store,dict(asof_ms=302000))
        self.assertGreater(r['accounts'][ACCOUNT]['fee_PNL'],r['accounts'][HOLD]['fee_PNL'])
        self.assertEqual(e['shares'],self.engine.orders['0:'+ENTRY]['shares'])
        write_report(self.store,Path(self.tmp.name),dict(asof_ms=302000,phase='complete'))
        import json
        self.assertEqual(json.loads((Path(self.tmp.name)/'heartbeat.json').read_text())['phase'],'complete')

    def test_repeated_decision_does_not_overwrite_filled_entry_or_recall(self):
        e=self.fill();before=copy.deepcopy(e);calls=len(self.calls)
        self.decide('C180',120000)
        self.assertEqual(self.engine.orders[e['id']],before)
        self.assertEqual(len(self.calls),calls)

    def test_recovery_baseline_complete_candidate_missing_marks_unknown(self):
        packet=self.decide('C180',120000)
        d=next(x for x in self.store.rows('decisions') if x['id']==packet['id'])
        d['branches'].pop(ACCOUNT);self.store.put('decisions',d['id'],d)
        self.store.db.execute('DELETE FROM orders');self.store.db.commit();self.engine.orders={}
        async def forbidden(*args):raise AssertionError('recalled provider')
        self.store.infer=forbidden
        with patch('candidate_engine.now',return_value=120200):
            asyncio.run(self.engine.decide(packet,None,None,recover=True))
        d=next(x for x in self.store.rows('decisions') if x['id']==packet['id'])
        self.assertEqual(d['branches'][ACCOUNT]['action'],'UNKNOWN')
        self.assertFalse(self.engine.orders)

    def test_pending_candidate_order_becomes_unknown_on_process_gap(self):
        self.decide('C180',120000)
        with patch('engine.now',return_value=125000): restarted=Engine(self.store)
        self.assertEqual(restarted.orders['0:'+ENTRY]['status'],'unknown')

    def test_nonfavorite_cheap_side_skipped(self):
        from logic import normalize_book
        self.decide('C180',120000,p=.6,quote=normalize_book(book(120000,.89,.90),market(),120000))
        d=self.store.rows('decisions')[0]
        self.assertEqual(d['branches'][ACCOUNT]['reason'],'not_forecast_favorite')
        self.assertNotIn('0:'+ENTRY,self.engine.orders)

    def test_missing_fee_at_exit_is_unknown(self):
        self.fill();packet=self.engine.freeze(tape_at(210000).packet(market(),210000),'C90')
        packet['market']['fee_bps']=None
        d=dict(id=packet['id'],phase='C90',start=0,status='complete',probabilities=dict(A=.1,Market=.5),branches={})
        self.store.put('decisions',d['id'],d)
        with patch('candidate_engine.now',return_value=210200):asyncio.run(self.engine.decide(packet,None,None))
        self.assertEqual(self.store.rows('decisions')[-1]['branches'][ACCOUNT]['action'],'UNKNOWN')

    def test_missing_decision_excluded_from_wr_and_block_completion(self):
        r=summarize(self.store,dict(asof_ms=302000))
        self.assertEqual(r['accounts'][ACCOUNT]['statuses'],{'unknown':1})
        self.assertIsNone(r['accounts'][ACCOUNT]['WR'])
        self.assertFalse(r['primary_20run_blocks'][0]['complete'])

if __name__=='__main__':unittest.main()

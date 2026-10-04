import asyncio
from copy import deepcopy
from decimal import Decimal
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0,str(Path(__file__).parent))
from policy import *
from service import Reader,Observer,connect
from report import metrics,snapshot,write_reports

START=1791034500000

def raw_market(symbol='BTCUSDT'):
    return dict(symbol=symbol,startDate=START,endDate=START+SLOT,slug=symbol[:-4].lower()+'-updown-5m-'+str(START//1000),marketTopicId=123,feeRateBps=200,
        variantData=dict(type='CRYPTO_UP_DOWN',priceFeedSymbol=symbol,startPrice='100'),status='RESOLVED',
        markets=[dict(marketId=456,status='RESOLVED',outcomes=[dict(name='Up',tokenId='10',winner=True,price='1'),dict(name='Down',tokenId='11',winner=False,price='0')])])

def candles(first='2',last='-1',prior='2'):
    c=[]
    for i in range(17):
        t=START-900000+i*60000;o=Decimal(100);close=o
        if i==14:close=o*(1+dec(prior)/10000)
        if i==15:close=o*(1+dec(first)/10000)
        if i==16:close=o*(1+dec(last)/10000)
        c.append([t,str(o),'0','0',str(close),'1',t+59999])
    return c

def quote(price='.3',side='UP',offset=124200):
    return dict(side=side,book_at_ms=START+offset-50,received_at_ms=START+offset,levels=[[price,'100']])

class PolicyTests(unittest.TestCase):
    def test_all_assets_and_closed_candles(self):
        for s in SYMBOLS:
            f=features(s,START,candles(),START+120500);self.assertTrue(f['trend_pass']);self.assertEqual(f['side'],'UP')
    def test_down_and_neutral(self):
        self.assertEqual(features('BTCUSDT',START,candles('-2','1','-2'),START+121000)['side'],'DOWN')
        self.assertFalse(features('BTCUSDT',START,candles('2','-1','0'),START+121000)['trend_pass'])
    def test_zero_flat_threshold(self):
        self.assertFalse(features('BTCUSDT',START,candles('.49','-.49'),START+121000)['reversal'])
        self.assertTrue(features('BTCUSDT',START,candles('.5','-.5'),START+121000)['reversal'])
    def test_late_features_and_wrong_candle(self):
        with self.assertRaises(ValueError):features('BTCUSDT',START,candles(),START+123001)
        c=candles();c[3][0]+=1
        with self.assertRaises(ValueError):features('BTCUSDT',START,c,START+121000)
    def test_wrong_asset_market(self):
        with self.assertRaises(ValueError):metadata(raw_market('ETHUSDT'),'BTCUSDT',START)
    def test_duplicate_side_token(self):
        d=raw_market();d['markets'][0]['outcomes'][1]['tokenId']='10'
        with self.assertRaises(ValueError):metadata(d,'BTCUSDT',START)
    def test_book_fresh_identity(self):
        m=metadata(raw_market(),'BTCUSDT',START)
        raw=dict(tokenId='10',outcome='Up',timestamp=START+124000,asks=[dict(price='.3',size='10')],bids=[])
        self.assertEqual(book(raw,m,'UP',START+124500)['side'],'UP')
        for change in [dict(tokenId='11'),dict(timestamp=START+125000),dict(timestamp=START+123000)]:
            d={**raw,**change}
            with self.assertRaises(ValueError):book(d,m,'UP',START+124500)
    def test_initial_depth_price_limits(self):
        m=metadata(raw_market(),'BTCUSDT',START);f=features('BTCUSDT',START,candles(),START+121000)
        for p in ('.15','.45'):self.assertTrue(initial_quote(m,f,quote(p),START+124200))
        for p in ('.14','.46'):
            with self.assertRaises(ValueError):initial_quote(m,f,quote(p),START+124200)
        q=quote();q['levels']=[['.3','1'],['.46','100']]
        with self.assertRaises(ValueError):initial_quote(m,f,q,START+124200)
    def test_share_fee_once_and_floor(self):
        ex=walk([['.3','100']],200)
        self.assertEqual(ex['gross_shares'],'3.33');self.assertEqual(dec(ex['cash']),dec('.999'))
        self.assertEqual(dec(ex['net_shares']),dec('3.2634'))
        self.assertEqual(dec(pnl(ex,'UP','UP')),dec('2.2644'))
        self.assertEqual(dec(pnl(ex,'DOWN','UP')),dec('-.999'))
    def test_full_depth_floor_recheck(self):
        m=metadata(raw_market(),'BTCUSDT',START);initial=walk([['.3','100']],200)
        q=quote('.29',offset=128200);ex=recheck(m,initial,q,START+128200)
        self.assertEqual(ex['gross_shares'],initial['gross_shares']);self.assertLess(dec(ex['cash']),dec(initial['cash']))
        with self.assertRaises(ValueError):recheck(m,initial,quote('.31',offset=128200),START+128200)
        q['levels']=[['.29','1']]
        with self.assertRaises(ValueError):recheck(m,initial,q,START+128200)
        with self.assertRaises(ValueError):recheck(m,initial,quote('.29',offset=130000),START+130000)
    def test_resolution_identity_terminal_and_conflict(self):
        d=raw_market();m=metadata(d,'BTCUSDT',START)
        self.assertEqual(resolution(d,m,START+SLOT),'UP')
        with self.assertRaises(ValueError):resolution(d,m,START+SLOT-1)
        for change in [dict(marketTopicId=999),dict(feeRateBps=201),dict(winner='DOWN')]:
            with self.assertRaises(ValueError):resolution({**d,**change},m,START+SLOT)
        d['markets'][0]['status']='REGISTERED';d['status']='REGISTERED'
        self.assertIsNone(resolution(d,m,START+SLOT))
    def test_draw_proof(self):
        d=raw_market();m=metadata(d,'BTCUSDT',START)
        for o in d['markets'][0]['outcomes']:o.update(winner=True,price='.5')
        self.assertEqual(resolution(d,m,START+SLOT),'DRAW')
        self.assertEqual(dec(pnl(walk([['.3','100']],200),'DRAW','UP')),dec('.6327'))
        d['markets'][0]['outcomes'][1]['price']='.6'
        self.assertIsNone(resolution(d,m,START+SLOT))
    def test_pending_wr_draw_mdd(self):
        rows=[]
        for i,(win,p) in enumerate([('UP','2'),('DOWN','-1'),('DRAW','-.2')]):
            rows.append(dict(start=START+i*SLOT,known_at_ms=START+i*SLOT+SLOT,reason='sim',features=dict(reversal=True,side='UP',trend_pass=True),initial_quote={},sim_quote=dict(cash='.9',gross_shares='3'),winner=win,sim_pnl=p))
        rows.append(dict(start=START+3*SLOT,reason='pending',features=dict(reversal=True,side='UP',trend_pass=True),sim_quote=dict(cash='.9',gross_shares='3')))
        rows.append(dict(start=START+4*SLOT,reason='missing'))
        m=metrics(rows);self.assertEqual(m['wr'],.5);self.assertEqual(m['pending'],1);self.assertEqual(dec(m['net_pnl']),dec('.8'));self.assertEqual(dec(m['mdd']),dec('1.2'));self.assertEqual(m['missing_features'],1)
    def test_restart_policy_and_common_denominator_report(self):
        with tempfile.TemporaryDirectory() as td:
            with patch('service.now',return_value=START):db=connect(td)
            for s in SYMBOLS:db.execute('INSERT INTO windows VALUES(?,?,?)',(s,START,json.dumps(dict(symbol=s,start=START,end=START+SLOT,reason='missing'))))
            db.commit();write_reports(db,td,START+SLOT)
            p=json.loads((Path(td)/'latest.json').read_text())
            self.assertEqual(p['rolling']['20']['BTCUSDT']['ALL']['scheduled_windows'],1)
            self.assertIsNone(p['rolling']['20']['ETHUSDT']['ALL']['wr']);self.assertFalse(p['selector_enabled'])
            db.close();db=connect(td);db.close()
    def test_matches_live_first_features_and_walk(self):
        sys.path.insert(0,str(Path(__file__).resolve().parents[2]))
        from src.gridbot.prediction.regime_lane import freeze_features,select_side,walk as live_walk
        for a,b,prior in [('2','-1','2'),('-2','1','-2'),('.49','-.5','0'),('2','-1','-2')]:
            c=candles(a,b,prior);ours=features('BTCUSDT',START,c,START+121000)
            baseline=freeze_features(START,c,START+121000);selected=select_side(baseline,None)
            if ours['reversal']:
                self.assertEqual(ours['trend_pass'],selected['allowed']);self.assertEqual(ours['side'],selected['side'])
        for levels in [[['.3','100']],[['.3','1'],['.4','100']],[['.3','3.33'],['.4','100']]]:
            ours=walk(levels,200);expected=live_walk(levels,200)
            self.assertEqual(dec(ours['cash']),expected['cash']);self.assertEqual(dec(ours['net_shares']),expected['net_shares']);self.assertEqual(dec(ours['limit']),expected['limit'])

class GuardTests(unittest.IsolatedAsyncioTestCase):
    async def test_mutation_and_other_endpoint_denied(self):
        r=Reader(None,'key','secret')
        for path in ('/sapi/v1/w3w/wallet/prediction/order/create','/sapi/v1/w3w/wallet/prediction/order/list','https://evil.test/api/v3/time'):
            with self.assertRaises(PermissionError):await r.get(path)
        r.cooldown=10**20
        with self.assertRaises(Exception):await r.get('/api/v3/time')
    async def test_budget_before_network(self):
        r=Reader(None,'key','secret');r.calls.extend([10**20]*40)
        with self.assertRaises(Exception):await r.get('/api/v3/time')
    async def test_no_backfill_on_late_feature_reply(self):
        class Fake:
            async def get(self,*a,**kw):return candles()
        with tempfile.TemporaryDirectory() as td:
            with patch('service.now',return_value=START):db=connect(td)
            o=Observer(db,Fake(),td)
            with patch('service.now',return_value=START+124000):await o.freeze('BTCUSDT',START)
            self.assertNotIn('features',o.row('BTCUSDT',START));db.close()

if __name__=='__main__':unittest.main()

class SchedulerTests(unittest.IsolatedAsyncioTestCase):
    async def test_delayed_tick_uses_remaining_valid_window_once(self):
        from unittest.mock import AsyncMock
        with tempfile.TemporaryDirectory() as td:
            with patch('service.now',return_value=START):db=connect(td)
            o=Observer(db,None,td);o.freeze=AsyncMock();o.initial=AsyncMock();o.confirm=AsyncMock()
            o.schedule_phases(START,START+121500)
            o.schedule_phases(START,START+121600)
            await asyncio.gather(*o.tasks)
            self.assertEqual(o.freeze.await_count,3)
            o.schedule_phases(START,START+125200)
            o.schedule_phases(START,START+128800)
            await asyncio.gather(*o.tasks)
            self.assertEqual(o.initial.await_count,3);self.assertEqual(o.confirm.await_count,3)
            db.close()

    async def test_missed_dispatch_is_explicit_not_backfilled(self):
        from unittest.mock import AsyncMock
        with tempfile.TemporaryDirectory() as td:
            with patch('service.now',return_value=START):db=connect(td)
            o=Observer(db,None,td);o.freeze=AsyncMock()
            o.schedule_phases(START,START+123100)
            o.freeze.assert_not_called()
            for s in SYMBOLS:
                row=o.row(s,START)
                self.assertEqual(row['feature_capture_status'],'dispatch_window_missed')
                self.assertNotIn('features',row)
            db.close()

    async def test_late_reply_saved_for_audit_but_never_a_feature(self):
        class Fake:
            async def get(self,*a,**kw):return candles()
        with tempfile.TemporaryDirectory() as td:
            with patch('service.now',return_value=START):db=connect(td)
            o=Observer(db,Fake(),td)
            with patch('service.now',return_value=START+123001):await o.freeze('BTCUSDT',START)
            row=o.row('BTCUSDT',START)
            self.assertNotIn('features',row);self.assertEqual(row['feature_error'],'ValueError')
            o.flush()
            self.assertEqual(db.execute("SELECT COUNT(*) FROM evidence WHERE stage='features'").fetchone()[0],1)
            db.close()

class DiagnosticReportTests(unittest.TestCase):
    def test_rejection_reason_from_old_evidence_without_rewriting(self):
        from report import recheck_reason
        from copy import deepcopy
        row=dict(start=START,reason='recheck_unavailable_or_rejected',meta=metadata(raw_market(),'BTCUSDT',START),
                 features=dict(reversal=True,side='UP',trend_pass=True),initial_quote=walk([['.24','100']],200),
                 recheck_attempted=START+128100,recheck_book=quote('.26',offset=128250))
        original=deepcopy(row);self.assertEqual(recheck_reason(row),'price_above_frozen_cap')
        m=metrics([row]);self.assertEqual(m['recheck_attempted'],1);self.assertEqual(m['quote_candidates'],0)
        self.assertEqual(m['recheck_reasons'],{'price_above_frozen_cap':1});self.assertEqual(row,original)

class CapturePersistenceTests(unittest.IsolatedAsyncioTestCase):
    async def test_critical_window_has_no_sqlite_write_but_retains_exact_receipt(self):
        class Fake:
            async def get(self,*a,**kw):return candles()
        with tempfile.TemporaryDirectory() as td:
            with patch('service.now',return_value=START):db=connect(td)
            o=Observer(db,Fake(),td);writes=[]
            db.set_trace_callback(lambda sql:writes.append(sql) if sql.startswith(('INSERT','UPDATE','COMMIT','BEGIN')) else None)
            with patch('service.now',return_value=START+120500):await o.freeze('BTCUSDT',START)
            self.assertEqual(writes,[])
            self.assertEqual(o.row('BTCUSDT',START)['features']['received_at_ms'],START+120500)
            o.flush();self.assertTrue(writes)
            self.assertEqual(db.execute("SELECT received FROM evidence WHERE stage='features'").fetchone()[0],START+120500)
            db.close()

    async def test_old_raw_stale_book_reports_actual_reason_without_db_rewrite(self):
        with tempfile.TemporaryDirectory() as td:
            with patch('service.now',return_value=START):db=connect(td)
            o=Observer(db,None,td);row=dict(symbol='BTCUSDT',start=START,end=START+SLOT,reason='recheck_unavailable_or_rejected',
                meta=metadata(raw_market(),'BTCUSDT',START),features=dict(reversal=True,trend_pass=True,side='UP'),
                initial_quote=walk([['.3','100']],200),recheck_attempted=START+128100)
            o.save(row);o.evidence('BTCUSDT',START,'recheck_UP',dict(tokenId='10',outcome='Up',timestamp=START+125000,asks=[dict(price='.3',size='100')]),START+128100)
            o.flush()  # Fixture persistence must not depend on wall-clock phase.
            before=db.execute('SELECT payload FROM windows').fetchone()[0]
            p=snapshot(db,START+SLOT)
            self.assertEqual(p['rolling']['20']['BTCUSDT']['ALL']['recheck_reasons'],{'book_stale':1})
            self.assertEqual(db.execute('SELECT payload FROM windows').fetchone()[0],before);db.close()

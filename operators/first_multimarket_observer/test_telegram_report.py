import asyncio
from pathlib import Path
import sys
import tempfile
import unittest
sys.path.insert(0,str(Path(__file__).parent))
from report import metrics
from policy import SYMBOLS
from telegram_report import render,credential_config,outbox,deliver,TelegramError


def block():
    return dict(block=1,start=1791034800000,completed_windows=20,markets={s:{d:metrics([],None if d=='ALL' else d) for d in ('ALL','UP','DOWN')} for s in SYMBOLS})

class FormatTests(unittest.TestCase):
    def test_independent_simulation_and_empty_wr(self):
        text=render(block());self.assertIn('非真實成交',text);self.assertIn('第1批',text)
        self.assertIn('WR —',text);self.assertNotIn('WR 0.0%',text)
        for s in ('BTC','ETH','BNB','First UP','First DOWN','MDD'):self.assertIn(s,text)
        self.assertLess(len(text),3900)
    def test_pending_missing_and_draw(self):
        b=block();m=b['markets']['BTCUSDT']['ALL'];m.update(pending=1,missing_features=2,settled=1,draws=1,net_pnl='.2',mdd='0')
        t=render(b);self.assertIn('待結，尚未完整',t);self.assertIn('缺K線2',t);self.assertIn('1平',t);self.assertIn('+0.2000U',t)
    def test_legacy_requires_existing_opt_in(self):
        with self.assertRaises(ValueError):credential_config(dict(TELEGRAM_BOT_TOKEN='123:abc',TELEGRAM_CHAT_ID='-123'))
        token,chats=credential_config(dict(PREDICTION_TELEGRAM_ALLOW_LEGACY='true',PREDICTION_TELEGRAM_BOT_TOKEN='456:def',TELEGRAM_BOT_TOKEN='123:abc',TELEGRAM_CHAT_ID='-123'))
        self.assertEqual(token,'456:def');self.assertEqual(chats,['-123'])
    def test_dedicated_allowlist_preferred_and_deduped(self):
        token,chats=credential_config(dict(PREDICTION_TELEGRAM_BOT_TOKEN='123:abc',PREDICTION_TELEGRAM_CHAT_IDS='1,1,-2'))
        self.assertEqual(chats,['1','-2'])
        with self.assertRaises(ValueError):credential_config(dict(PREDICTION_TELEGRAM_BOT_TOKEN='123:abc',PREDICTION_TELEGRAM_CHAT_IDS='@somewhere'))

class DeliveryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.directory=tempfile.TemporaryDirectory();self.db=outbox(Path(self.directory.name)/'outbox.sqlite3')
    async def asyncTearDown(self):self.db.close();self.directory.cleanup()
    async def test_send_dedupe_and_update_same_message(self):
        class Fake:
            def __init__(self):self.calls=[]
            async def request(self,*args):self.calls.append(args);return 88
        sender=Fake()
        self.assertEqual(await deliver(self.db,sender,'1','batch','first',1000),'sent')
        self.assertEqual(await deliver(self.db,sender,'1','batch','first',2000),'unchanged')
        self.assertEqual(await deliver(self.db,sender,'1','batch','settled',3000),'edited')
        self.assertEqual([c[0] for c in sender.calls],['sendMessage','editMessageText']);self.assertEqual(sender.calls[1][-1],88)
    async def test_unknown_send_is_not_retried(self):
        class Fake:
            def __init__(self):self.calls=0
            async def request(self,*args):self.calls+=1;raise TimeoutError()
        f=Fake();self.assertEqual(await deliver(self.db,f,'1','batch','first',1000),'unknown_requires_review')
        self.assertEqual(await deliver(self.db,f,'1','batch','different',2000),'unknown_requires_review');self.assertEqual(f.calls,1)
    async def test_crash_after_send_commit_blocks_duplicate(self):
        self.db.execute("INSERT INTO messages VALUES('1','batch','SENDING',NULL,NULL,'x',NULL,NULL)");self.db.commit()
        self.assertEqual(await deliver(self.db,None,'1','batch','hello',1000),'unknown_requires_review')
    async def test_explicit_rate_rejection_retry(self):
        class Fake:
            def __init__(self):self.calls=0
            async def request(self,*args):
                self.calls+=1
                if self.calls==1:raise TelegramError(429,120)
                return 99
        f=Fake();self.assertEqual(await deliver(self.db,f,'1','batch','hello',1000),'telegram_rejected')
        self.assertEqual(await deliver(self.db,f,'1','batch','hello',2000),'deferred')
        self.assertEqual(await deliver(self.db,f,'1','batch','hello',121000),'sent')
    async def test_unknown_edit_can_retry_without_creating_new_message(self):
        class Fake:
            def __init__(self):self.calls=[]
            async def request(self,*args):
                self.calls.append(args)
                if len(self.calls)==2:raise TimeoutError()
                return 88
        f=Fake();await deliver(self.db,f,'1','batch','first',1000)
        self.assertEqual(await deliver(self.db,f,'1','batch','settled',2000),'edit_retry')
        self.assertEqual(await deliver(self.db,f,'1','batch','settled',63000),'edited')
        self.assertEqual([a[0] for a in f.calls],['sendMessage','editMessageText','editMessageText'])

if __name__=='__main__':unittest.main()

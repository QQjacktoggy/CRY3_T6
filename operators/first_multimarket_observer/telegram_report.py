"""Separate authorized TG reporter; observer DB read-only, independent outbox.

No polling/getUpdates, bot commands, trading imports or trading DB access.
Send uncertainty is persisted and never automatically retried as a new message.
"""
import argparse
import asyncio
from datetime import datetime
from decimal import Decimal
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import time
from zoneinfo import ZoneInfo
from policy import SYMBOLS,SLOT
from report import snapshot


def timestamp(ms):return datetime.fromtimestamp(ms/1000,ZoneInfo('Asia/Taipei')).strftime('%m/%d %H:%M')

def credential_config(values):
    legacy=str(values.get('PREDICTION_TELEGRAM_ALLOW_LEGACY','')).lower() in ('1','true','yes','on')
    token=str(values.get('PREDICTION_TELEGRAM_BOT_TOKEN') or (values.get('TELEGRAM_BOT_TOKEN') if legacy else '') or '').strip()
    ids=str(values.get('PREDICTION_TELEGRAM_CHAT_IDS') or (values.get('TELEGRAM_CHAT_ID') if legacy else '') or '')
    chats=list(dict.fromkeys(z.strip() for z in ids.split(',') if z.strip()))
    if not re.fullmatch(r'[0-9]+:[A-Za-z0-9_-]+',token):raise ValueError('telegram_token_unavailable')
    if not chats or any(not re.fullmatch(r'-?[0-9]+',c) for c in chats):raise ValueError('telegram_allowlist_unavailable')
    return token,chats


def render(block,bootstrap=False,*,rolling=False):
    n=block['completed_windows'];start=block['start'];end=block.get('end',start+n*SLOT)
    title=('First 三市場觀測｜最近20場（自動更新）' if rolling else
           'First 三市場觀測｜啟用快照' if bootstrap else f"First 三市場觀測｜第{block['block']}批")
    lines=[title,f'{timestamp(start)}–{timestamp(end)}（台灣）｜{n}/20場',
           '1U報價模擬，非真實成交；未套Live風控。','']
    for symbol in SYMBOLS:
        group=block['markets'][symbol];m=group['ALL']
        lines += [f"【{symbol[:-4]}】K線 {m['feature_complete']}/{m['scheduled_windows']}｜雙向盤口 {m['initial_books_complete']}/{m['scheduled_windows']}",
                  f"訊號{m['signal']} → 趨勢{m['trend_pass']} → 初始{m['initial_quote_eligible']} → 重檢{m['quote_candidates']}"]
        rate='—' if m['quote_candidate_rate'] is None else f"{m['quote_candidate_rate']*100:.1f}%"
        lines.append(f'報價候選率 {rate}（非fill率）')
        for side,label in (('ALL','合計'),('UP','First UP'),('DOWN','First DOWN')):
            row=group[side];wr='—' if row['wr'] is None else f"{row['wr']*100:.1f}%"
            net='—' if not row['settled'] else f"{Decimal(row['net_pnl']):+.4f}U"
            mdd='—' if not row['settled'] else f"{Decimal(row['mdd']):.4f}U"
            suffix='（待結，尚未完整）' if row['pending'] else ''
            lines.append(f"{label}：候選{row['quote_candidates']}｜{row['wins']}勝{row['losses']}負{row['draws']}平｜待結{row['pending']}")
            lines.append(f'WR {wr}｜PnL {net}｜MDD {mdd}{suffix}')
        missing=m['missing_features']
        if missing:lines.append(f'⚠ 缺K線{missing}場，保留分母。')
        reason_names={'not_first_reversal':'無First反轉','prior_trend_filter':'前趨勢不符','price_band':'價格帶不符',
                      'initial_book_missing':'初始盤口缺漏','initial_inputs_missing':'初始資料缺漏',
                      'feature_missing':'K線取樣失敗','missed_feature_window':'錯過取樣窗口',
                      'recheck_unavailable_or_rejected':'重檢未通過','initial_quote_eligible':'等待重檢',
                      'QUOTE_SIMULATED_NOT_FILLED':'模擬候選','insufficient_1u_depth':'深度不足'}
        reasons=m.get('reasons',{})
        if reasons:lines.append('原因：'+'、'.join(f"{reason_names.get(k,'其他資料/條件阻擋')} {v}" for k,v in sorted(reasons.items(),key=lambda kv:(-kv[1],kv[0]))[:3]))
        lines.append('')
    if rolling:lines.append('每有新場次或結算更新此訊息；每分鐘檢查。')
    lines += ['初始124–126秒凍結價/數量；128秒單次重檢。',
              '官方勝方結算；扣費一次。WR排除平局，PnL包含平局。',
              'ETH/BNB沿用BTC First條件，尚未驗證Live適用性。',
              '候選少或待結時無法判定好時機；不自動選幣。']
    text='\n'.join(lines)
    if len(text)>3900:raise ValueError('telegram_report_too_long')
    return text


def render_rolling(p):
    """Use the actual rolling membership, never the latest fixed block."""
    span=p['rolling20_range']
    group=p['rolling']['20']
    n=span['count']
    if any(group[s]['ALL']['scheduled_windows']!=n for s in SYMBOLS):
        raise ValueError('rolling_report_membership')
    return render(dict(start=span['start'],end=span['end'],completed_windows=n,
                       markets=group),rolling=True)


def report_jobs(p):
    # Reuse the already-acknowledged bootstrap message as the rolling dashboard.
    # Keeping the outbox key also preserves UNKNOWN/SENDING duplicate protection.
    jobs=[('bootstrap',render_rolling(p))]
    jobs += [('block:'+str(b['start']),render(b)) for b in p['blocks20']
             if b['completed_windows']==20]
    return jobs


class TelegramError(Exception):
    def __init__(self,code,retry_after=0):self.code=code;self.retry_after=retry_after;super().__init__('telegram_'+str(code))


class Sender:
    def __init__(self,session,token,chats):self.session=session;self.token=token;self.chats=set(chats)
    async def request(self,method,chat,text,message_id=None):
        if method not in ('sendMessage','editMessageText') or chat not in self.chats:raise PermissionError('telegram_endpoint_or_chat_denied')
        payload=dict(chat_id=chat,text=text,disable_web_page_preview=True,disable_notification=False)
        if method=='editMessageText':
            if not isinstance(message_id,int) or message_id<=0:raise ValueError('message_id')
            payload['message_id']=message_id;payload.pop('disable_notification')
        async with self.session.post('https://api.telegram.org/bot'+self.token+'/'+method,json=payload,allow_redirects=False) as response:
            chunks=[];size=0
            async for chunk in response.content.iter_chunked(16384):
                size+=len(chunk)
                if size>262144:raise ValueError('telegram_response_size')
                chunks.append(chunk)
            body=json.loads(b''.join(chunks))
            if not body.get('ok'):
                # Editing the identical text is successful idempotent recovery.
                if method=='editMessageText' and 'message is not modified' in str(body.get('description','')).lower():return message_id
                raise TelegramError(int(body.get('error_code',response.status)),int(body.get('parameters',{}).get('retry_after',0)))
            result=body['result']
            if str(result['chat']['id'])!=chat or not isinstance(result.get('message_id'),int):raise ValueError('telegram_ack_identity')
            return result['message_id']


def outbox(path):
    db=sqlite3.connect(path,timeout=1);db.row_factory=sqlite3.Row
    db.execute('CREATE TABLE IF NOT EXISTS messages(chat TEXT,report_key TEXT,status TEXT,message_id INTEGER,content_hash TEXT,attempt_hash TEXT,retry_at INTEGER,error_code TEXT,PRIMARY KEY(chat,report_key))')
    db.execute('PRAGMA synchronous=FULL');db.commit();return db


async def deliver(db,sender,chat,key,text,at):
    sha=hashlib.sha256(text.encode()).hexdigest()
    row=db.execute('SELECT * FROM messages WHERE chat=? AND report_key=?',(chat,key)).fetchone()
    if row and row['status'] in ('SENDING','UNKNOWN'):return 'unknown_requires_review'
    if row and row['content_hash']==sha and row['status']=='SENT':return 'unchanged'
    if row and row['retry_at'] and at<row['retry_at']:return 'deferred'
    if row and row['status']=='FAILED' and not row['retry_at']:return 'failed_requires_review'
    edit=bool(row and row['message_id']);message_id=row['message_id'] if row else None
    # Commit before external send. On restart SENDING must not create a duplicate.
    with db:
        db.execute('INSERT INTO messages VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(chat,report_key) DO UPDATE SET status=excluded.status,attempt_hash=excluded.attempt_hash,error_code=NULL',
                   (chat,key,'EDITING' if edit else 'SENDING',message_id,row['content_hash'] if row else None,sha,None,None))
    try:
        mid=await sender.request('editMessageText' if edit else 'sendMessage',chat,text,message_id)
    except TelegramError as exc:
        retry=at+max(60000,exc.retry_after*1000) if exc.code in (429,500,502,503,504) else None
        with db:db.execute('UPDATE messages SET status=?,retry_at=?,error_code=? WHERE chat=? AND report_key=?',('FAILED',retry,str(exc.code),chat,key))
        return 'telegram_rejected'
    except Exception:
        # New-message delivery may have succeeded without a usable ACK. Never re-send.
        with db:db.execute('UPDATE messages SET status=?,retry_at=?,error_code=? WHERE chat=? AND report_key=?',('EDITING' if edit else 'UNKNOWN',at+60000 if edit else None,'transport_or_ack_unknown',chat,key))
        return 'unknown_requires_review' if not edit else 'edit_retry'
    with db:db.execute('UPDATE messages SET status=?,message_id=?,content_hash=?,retry_at=NULL,error_code=NULL WHERE chat=? AND report_key=?',('SENT',mid,sha,chat,key))
    return 'edited' if edit else 'sent'


def load_snapshot(directory,at):
    db=sqlite3.connect((Path(directory)/'first-observer.sqlite3').resolve().as_uri()+'?mode=ro',uri=True,timeout=1)
    try:
        db.execute('PRAGMA query_only=ON');db.execute('BEGIN');p=snapshot(db,at)
        starts=[r[0] for r in db.execute('SELECT DISTINCT start FROM windows WHERE start+?<=? ORDER BY start DESC LIMIT 20',(SLOT,at))]
        p['rolling20_range']=dict(count=len(starts),start=min(starts) if starts else p['epoch'],
                                 end=max(starts)+SLOT if starts else p['epoch'])
        return p
    finally:db.close()


async def run(args):
    import aiohttp
    from dotenv import dotenv_values
    directory=Path(args.data);directory.mkdir(parents=True,exist_ok=True)
    at=time.time_ns()//1000000;p=load_snapshot(directory,at)
    health=p.get('health')
    if not health or at-health['at_ms']>120000:raise RuntimeError('observer_heartbeat_stale')
    if args.preview:
        print(render_rolling(p));return
    jobs=report_jobs(p)
    values=dotenv_values(args.credential_file,interpolate=False)
    # Explicit environment selectors override the corresponding file, matching Live convention.
    for k in ('PREDICTION_TELEGRAM_BOT_TOKEN','PREDICTION_TELEGRAM_CHAT_IDS','PREDICTION_TELEGRAM_ALLOW_LEGACY','TELEGRAM_BOT_TOKEN','TELEGRAM_CHAT_ID'):
        if k in os.environ:values[k]=os.environ[k]
    token,chats=credential_config(values);del values
    db=outbox(directory/'telegram-outbox.sqlite3');statuses=[]
    timeout=aiohttp.ClientTimeout(total=6,connect=2,sock_read=3)
    try:
        async with aiohttp.ClientSession(timeout=timeout,auto_decompress=False,headers={'Accept-Encoding':'identity'},trust_env=True) as session:
            sender=Sender(session,token,chats)
            for chat in chats:
                key,text=jobs[0]
                statuses.append(await deliver(db,sender,chat,key,text,at))
                # At most three summaries per destination per minute after downtime.
                changes=0
                for key,text in jobs[1:]:
                    result=await deliver(db,sender,chat,key,text,at);statuses.append(result)
                    if result!='unchanged':changes+=1
                    if changes>=3:break
            state=dict(at_ms=at,destinations=len(chats),statuses=statuses,
                       sent=db.execute("SELECT COUNT(*) FROM messages WHERE status='SENT'").fetchone()[0],
                       uncertain=db.execute("SELECT COUNT(*) FROM messages WHERE status IN ('SENDING','UNKNOWN')").fetchone()[0],
                       failed=db.execute("SELECT COUNT(*) FROM messages WHERE status='FAILED'").fetchone()[0])
            tmp=directory/'telegram-status.json.tmp';tmp.write_text(json.dumps(state,indent=2));tmp.replace(directory/'telegram-status.json')
            print(json.dumps(state))
    finally:db.close()


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--data',required=True);parser.add_argument('--credential-file',required=True);parser.add_argument('--preview',action='store_true')
    args=parser.parse_args()
    directory=Path(args.data)
    with open(directory/'telegram-reporter.lock','a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        try:asyncio.run(run(args))
        except Exception as exc:
            print('telegram reporter stopped: '+type(exc).__name__,flush=True);raise SystemExit(1)

if __name__=='__main__':main()

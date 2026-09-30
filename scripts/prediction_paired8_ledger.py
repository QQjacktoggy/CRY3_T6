"""Journal inputs first; the fixed T1 pair and state commit together.

This module is an offline/Shadow persistence component, not a market feed or
service launcher. It cannot submit orders. The VM worker adapter is separate.
"""
from copy import deepcopy
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import sqlite3
import zlib

from scripts.prediction_paired8_policy import (
    PAIRED8_LANES, PAIRED8_VERSION, paired8_step, validate_source_times, observe_metadata,
)


def dump(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False)


def compact_quote(q):
    fields = ('source', 'feed_ok', 'orientation_verified', 'book_at_ms', 'spot_at_ms',
              'received_at_ms', 'sampled_at_ms', 'spot', 'reference', 'spot_event_at_ms',
              'spot_received_at_ms', 'spot_trade_id', 'spot_connection_generation',
              'metadata_ready_at_ms', 'collection_started_at_ms', 'reference_pending',
              'spot_mid', 'spot_mid_received_at_ms', 'spot_mid_update_id', 'spot_connected', 'spot_raw_packets', 'spot_rejected_packets', 'spot_last_reject_reason')
    out = {k: q[k] for k in fields if k in q}
    for side in ('UP', 'DOWN'):
        out[side] = {k: q.get(side, {})[k] for k in ('bid', 'ask', 'ask_shares') if k in q.get(side, {})}
    return out


def read_report(path):
    """Read without changing journal mode or taking a writable connection."""
    store = object.__new__(Paired8Ledger)
    store.conn = sqlite3.connect(Path(path).absolute().as_uri()+'?mode=ro', uri=True, isolation_level=None)
    store.conn.row_factory = sqlite3.Row
    store.conn.execute('PRAGMA query_only=ON')
    try:
        cfg=json.loads(store.conn.execute("SELECT value FROM meta WHERE key='config'").fetchone()[0])
        store.identity=store.conn.execute("SELECT value FROM meta WHERE key='identity'").fetchone()[0]
        if cfg['version']!=PAIRED8_VERSION or tuple(cfg['lanes'])!=PAIRED8_LANES:
            raise ValueError('Unexpected research identity')
        if hashlib.sha256(dump(cfg).encode()).hexdigest()!=store.identity:
            raise ValueError('Research config hash mismatch')
        store.start_ms,store.target=cfg['start_ms'],cfg['target']
        return store.report()
    finally:
        store.conn.close()


def format_readonly_report(path):
    if not Path(path).exists():return '<b>Vol Shadow</b>\n等待固定600場初始化。'
    r=read_report(path);g=r['pilot_gate']
    lines=['<b>Vol Shadow</b>','600場 / 7組；每筆假想2U；以下扣500bps。',
           '首20場工程：'+g['status']+'；資料品質：'+g.get('quality_status','waiting')]
    for t in r['totals']:
        pnl='待結算' if t['pnl_500_settled'] is None else format(Decimal(t['pnl_500_settled']),'+.4f')
        lines.append(f"{t['lane']}：結算{t['settled']}/600 成交{t['settled_fills']} 合格{t['common_valid_settled']} PnL {pnl}")
    completed=[x for x in r['paired_batches'] if x['settled']]
    for x in completed[-3:]:lines.append(f"第{x['batch']}批：結算{x['settled']}/20，主策略相對限價差 {x['delta_500_settled']}")
    lines.append(r['financial_decision']['label'])
    return '\n'.join(lines)

class Paired8Ledger:
    def __init__(self, path, *, start_ms, target, model, create=False):
        if target != 600 or start_ms % 300000 or model.get('shadow_gate_passed') is not False:
            raise ValueError('Require aligned 600-slot Shadow-only batch')
        self.path = Path(path).absolute()
        self.model = deepcopy(model)
        self.start_ms, self.target = start_ms, target
        self.failure_injection = None
        identity = {'version': PAIRED8_VERSION, 'start_ms': start_ms, 'target': target,
                    'model': model, 'lanes': PAIRED8_LANES, 'cost_bps': [300, 500]}
        self.identity = hashlib.sha256(dump(identity).encode()).hexdigest()
        if create:
            # Exclusive creation prevents accidental opening or modifying an old study.
            with self.path.open('xb'):
                pass
        elif not self.path.is_file():
            raise FileNotFoundError(self.path)
        self.conn = sqlite3.connect(self.path.as_uri() + '?mode=rw', uri=True, isolation_level=None)
        self.conn.row_factory = sqlite3.Row
        if create:
            self.conn.executescript('''
                CREATE TABLE meta (key TEXT PRIMARY KEY,value TEXT NOT NULL);
                CREATE TABLE markets (start_ms INTEGER PRIMARY KEY,market_topic_id TEXT UNIQUE,
                    state_json TEXT,outcome TEXT,settled_at_ms INTEGER,recovery_count INTEGER NOT NULL DEFAULT 0, observation_finished INTEGER NOT NULL DEFAULT 0);
                CREATE TABLE quotes (start_ms INTEGER NOT NULL,at_ms INTEGER NOT NULL,
                    quote_json TEXT NOT NULL,processed INTEGER NOT NULL DEFAULT 0,PRIMARY KEY(start_ms,at_ms));
                CREATE TABLE fills (start_ms INTEGER NOT NULL,lane TEXT NOT NULL,
                    plan_json TEXT NOT NULL,PRIMARY KEY(start_ms,lane));
            ''')
            with self.transaction():
                self.conn.execute('INSERT INTO meta VALUES (?,?)', ('identity', self.identity))
                self.conn.execute('INSERT INTO meta VALUES (?,?)', ('config', dump(identity)))
                self.conn.executemany('INSERT INTO markets(start_ms) VALUES (?)',
                                     [(start_ms + i * 300000,) for i in range(target)])
        try:
            row = self.conn.execute("SELECT value FROM meta WHERE key='identity'").fetchone()
            if not row or row[0] != self.identity:
                raise ValueError('Refuse different experiment or legacy database')
            self.conn.execute('PRAGMA synchronous=FULL')
            self.conn.execute('PRAGMA journal_mode=WAL')
            self.conn.execute('PRAGMA wal_autocheckpoint=256')
            self.conn.execute('PRAGMA busy_timeout=5000')
        except Exception:
            self.conn.close()
            raise

    def transaction(self):
        from contextlib import contextmanager

        @contextmanager
        def tx():
            self.conn.execute('BEGIN IMMEDIATE')
            try:
                yield
                self.conn.execute('COMMIT')
            except BaseException:
                self.conn.execute('ROLLBACK')
                raise
        return tx()

    def close(self):
        self.conn.close()

    def _market(self, start_ms):
        row = self.conn.execute('SELECT * FROM markets WHERE start_ms=?', (start_ms,)).fetchone()
        if not row:
            raise ValueError('Outside frozen observation slots')
        return row

    def _assert_consistent(self, start_ms, state):
        counts = {r['lane']: 1 for r in self.conn.execute('SELECT lane FROM fills WHERE start_ms=?', (start_ms,))}
        if set(counts) - set(PAIRED8_LANES):
            raise RuntimeError('Unexpected lane')
        for lane in PAIRED8_LANES:
            ls = (state or {}).get('lanes', {}).get(lane, {})
            if counts.get(lane, 0) != int(ls.get('fills', 0)):
                raise RuntimeError('State/ledger mismatch')
            if bool(counts.get(lane)) != (ls.get('status') == 'filled'):
                raise RuntimeError('State/ledger status mismatch')
            if counts.get(lane):
                p = json.loads(self.conn.execute('SELECT plan_json FROM fills WHERE start_ms=? AND lane=?', (start_ms, lane)).fetchone()[0])
                if p != ls.get('fill'):
                    raise RuntimeError('State/ledger evidence mismatch')

    def observe(self, *, start_ms, market_topic_id, now_ms, quote):
        if not start_ms <= now_ms < start_ms + 300000:
            raise ValueError('Quote outside market')
        if not market_topic_id:
            raise ValueError('Market identity required')
        if self.target == 600 and start_ms >= self.start_ms + 20 * 300000:
            if self.pilot_gate(now_ms)['status'] != 'passed':
                raise RuntimeError('First 20-market engineering gate has not passed')
        q = compact_quote(quote)
        encoded = zlib.compress(dump(q).encode(), 6)
        if len(encoded) > 4096:
            raise ValueError('Unexpected oversized compact quote')
        resumed = False
        with self.transaction():
            row = self._market(start_ms)
            if row['outcome'] is not None:
                raise ValueError('Cannot append after settlement')
            if row['market_topic_id'] not in (None, str(market_topic_id)):
                raise ValueError('Market identity changed')
            previous = json.loads(row['state_json']) if row['state_json'] else None
            self._assert_consistent(start_ms, previous)
            # Reject fatal time/reference regressions before journaling a new
            # input, so the same invalid row cannot poison recovery forever.
            validate_source_times(previous, q, now_ms)
            observe_metadata((previous or {}).get('metadata'), q, now_ms=now_ms, start_ms=start_ms)
            old = self.conn.execute('SELECT quote_json FROM quotes WHERE start_ms=? AND at_ms=?', (start_ms, now_ms)).fetchone()
            if old:
                if old[0] != encoded:
                    raise ValueError('Conflicting same-time quote')
            elif previous and now_ms <= previous.get('last_call_ms', -1):
                raise ValueError('Time reversed')
            resumed = bool(self.conn.execute('SELECT 1 FROM quotes WHERE start_ms=? AND processed=0 LIMIT 1', (start_ms,)).fetchone())
            if not old:
                latest = self.conn.execute('SELECT MAX(at_ms) FROM quotes WHERE start_ms=?', (start_ms,)).fetchone()[0]
                if latest is not None and now_ms < latest:
                    raise ValueError('Time reversed')
                self.conn.execute('INSERT INTO quotes(start_ms,at_ms,quote_json) VALUES (?,?,?)', (start_ms, now_ms, encoded))
            self.conn.execute('UPDATE markets SET market_topic_id=? WHERE start_ms=?', (str(market_topic_id), start_ms))
        # An interrupted computation always replays its identical durable input
        # before a later quote. It cannot silently replace the parent candidate.
        return self._drain(start_ms, recovered=resumed)

    def _drain(self, start_ms, *, recovered=False):
        plans_out = []
        with self.transaction():
            row = self._market(start_ms)
            state = json.loads(row['state_json']) if row['state_json'] else None
            self._assert_consistent(start_ms, state)
            inputs = list(self.conn.execute('SELECT at_ms,quote_json FROM quotes WHERE start_ms=? AND processed=0 ORDER BY at_ms', (start_ms,)))
            if inputs and row['outcome'] is not None:
                raise RuntimeError('Unprocessed input after settlement')
            for event in inputs:
                state, plans = paired8_step(state, json.loads(zlib.decompress(event['quote_json'])), now_ms=event['at_ms'],
                    start_ms=start_ms, end_ms=start_ms+300000, model=self.model)
                for i, plan in enumerate(plans):
                    self.conn.execute('INSERT INTO fills VALUES (?,?,?)', (start_ms, plan['lane'], dump(plan)))
                    if i == 0 and self.failure_injection == 'after_first_fill':
                        raise RuntimeError('Injected failure after first fill')
                self.conn.execute('UPDATE quotes SET processed=1 WHERE start_ms=? AND at_ms=?', (start_ms, event['at_ms']))
                plans_out.extend(plans)
            if inputs:
                self.conn.execute('UPDATE markets SET state_json=?,recovery_count=recovery_count+? WHERE start_ms=?',
                    (dump(state), int(recovered), start_ms))
            self._assert_consistent(start_ms, state)
            if self.failure_injection == 'before_commit':
                raise RuntimeError('Injected failure before commit')
        return plans_out

    def finish_observation(self, *, start_ms, now_ms):
        """Call at/after market end, even when the feed never supplied a quote."""
        if now_ms < start_ms + 300000:
            raise ValueError('Observation has not finished')
        self._drain(start_ms, recovered=True)
        with self.transaction():
            row = self._market(start_ms)
            previous = json.loads(row['state_json']) if row['state_json'] else None
            self._assert_consistent(start_ms, previous)
            # Use the boundary, not the later resolution receipt time.
            state, plans = paired8_step(previous, {}, now_ms=start_ms + 300000,
                                       start_ms=start_ms, end_ms=start_ms + 300000, model=self.model)
            if plans:
                raise RuntimeError('End-of-window cannot create fill')
            state['observation_finished'] = True
            self.conn.execute('UPDATE markets SET state_json=?,observation_finished=1 WHERE start_ms=?', (dump(state), start_ms))
            self._assert_consistent(start_ms, state)

    def settle(self, *, start_ms, outcome, now_ms):
        if outcome not in ('UP', 'DOWN', 'DRAW') or now_ms < start_ms + 300000:
            raise ValueError('Require official binary outcome after market end')
        self.finish_observation(start_ms=start_ms, now_ms=now_ms)
        with self.transaction():
            row = self._market(start_ms)
            if row['outcome'] not in (None, outcome):
                raise ValueError('Immutable outcome conflict')
            self.conn.execute('UPDATE markets SET outcome=?,settled_at_ms=COALESCE(settled_at_ms,?) WHERE start_ms=?',
                              (outcome, now_ms, start_ms))

    def pilot_gate(self, now_ms):
        """Irreversible decision at the first 20 fixed market boundaries.

        Housekeeping finalizes observations before calling this method. Official
        settlement is deliberately not required for an engineering decision.
        """
        stored = self.conn.execute("SELECT value FROM meta WHERE key='pilot_gate'").fetchone()
        if stored:
            return json.loads(stored[0])
        boundary = self.start_ms + 20 * 300000
        if now_ms < boundary:
            return {'status': 'waiting', 'boundary_ms': boundary, 'reasons': []}
        with self.transaction():
            stored = self.conn.execute("SELECT value FROM meta WHERE key='pilot_gate'").fetchone()
            if stored:
                return json.loads(stored[0])
            reasons = []
            markets = list(self.conn.execute('SELECT * FROM markets ORDER BY start_ms LIMIT 20'))
            if len(markets) != 20:
                reasons.append('pilot_requires_exactly_20_fixed_slots')
            for index, market in enumerate(markets, 1):
                state = json.loads(market['state_json']) if market['state_json'] else {}
                self._assert_consistent(market['start_ms'], state)
                issues = []
                if not market['market_topic_id']:
                    issues.append('market_missing')
                if not state.get('observation_finished'):
                    issues.append('observation_not_finished')
                if not state.get('acquisition_quality', {}).get('passed'):
                    issues.append('acquisition_quality_failed')
                metadata = state.get('metadata', {})
                if not metadata.get('frozen') or not metadata.get('ready_before_window'):
                    issues.append('metadata_not_ready_by_180s')
                if market['recovery_count']:
                    issues.append('input_recovery')
                if any(lane.get('status') == 'censored' for lane in state.get('lanes', {}).values()):
                    issues.append('censored_candidate')
                if self.conn.execute('SELECT 1 FROM quotes WHERE start_ms=? AND processed=0 LIMIT 1',
                                     (market['start_ms'],)).fetchone():
                    issues.append('unprocessed_input')
                if issues:
                    reasons.append('market_%02d:' % index + ','.join(issues))
            fatal = [r for r in reasons if any(x in r for x in ('observation_not_finished','unprocessed_input','input_recovery','pilot_requires'))]
            decision = {'status': 'failed' if fatal else 'passed', 'quality_status': 'failed' if reasons else 'passed', 'checked_at_ms': now_ms,
                        'boundary_ms': boundary, 'reasons': reasons}
            self.conn.execute('INSERT INTO meta(key,value) VALUES (?,?)', ('pilot_gate', dump(decision)))
            return decision

    def report(self):
        """Always show all scheduled slots and quality cohort; open PnL stays null."""
        rows = []
        self.conn.execute('BEGIN')
        try:
            for m in self.conn.execute('SELECT * FROM markets ORDER BY start_ms'):
                state = json.loads(m['state_json']) if m['state_json'] else None
                self._assert_consistent(m['start_ms'], state)
                quality = (state or {}).get('quality', {})
                acquisition = (state or {}).get('acquisition_quality', {})
                metadata = (state or {}).get('metadata', {})
                local_censored = any(v.get('status') == 'censored' for v in (state or {}).get('lanes', {}).values())
                common_valid = (bool(acquisition.get('passed')) and bool(metadata.get('ready_before_window'))
                                and not local_censored and not m['recovery_count'])
                fills = {r['lane']: json.loads(r['plan_json']) for r in self.conn.execute('SELECT lane,plan_json FROM fills WHERE start_ms=?', (m['start_ms'],))}
                for lane in PAIRED8_LANES:
                    fill = fills.get(lane)
                    pnl = None
                    stress = None
                    if m['outcome']:
                        gross = Decimal(fill['gross']) if fill else Decimal(0)
                        payout = (gross / Decimal(fill['ask']) * (Decimal('.5') if m['outcome']=='DRAW' else Decimal(int(fill['side']==m['outcome'])))) if fill else Decimal(0)
                        pnl = payout - gross - gross * Decimal('.03')
                        stress = payout - gross - gross * Decimal('.05')
                    rows.append(dict(start_ms=m['start_ms'], batch=(m['start_ms']-self.start_ms)//6000000+1,
                        lane=lane, settled=bool(m['outcome']), common_valid=common_valid,
                        observation_closed=bool(quality.get('closed')), coverage=quality.get('coverage'),
                        max_gap_ms=quality.get('max_gap_ms'), filled=bool(fill),
                        tradable_coverage=quality.get('coverage'),
                        acquisition_coverage=acquisition.get('coverage'),
                        acquisition_max_gap_ms=acquisition.get('max_gap_ms'),
                        acquisition_passed=bool(acquisition.get('passed')),
                        metadata_ready_at_ms=metadata.get('ready_at_ms'),
                        metadata_ready_before_window=bool(metadata.get('ready_before_window')),
                        recovery_count=m['recovery_count'],
                        status=(state or {}).get('lanes', {}).get(lane, {}).get('status', 'not_observed'),
                        pnl_300=None if pnl is None else str(pnl), pnl_500=None if stress is None else str(stress)))
            gate_row = self.conn.execute("SELECT value FROM meta WHERE key='pilot_gate'").fetchone()
            gate = json.loads(gate_row[0]) if gate_row else {'status': 'waiting', 'reasons': [],
                                                         'boundary_ms': self.start_ms + 20*300000}
            self.conn.execute('COMMIT')
        except BaseException:
            self.conn.execute('ROLLBACK')
            raise
        def summarize(selected, **labels):
            closed = [r for r in selected if r['settled']]
            valid = [r for r in closed if r['common_valid']]
            def total(cohort, key):
                return str(sum((Decimal(r[key]) for r in cohort), Decimal(0))) if cohort else None
            def drawdown(cohort):
                if not cohort:
                    return None
                equity = peak = worst = Decimal(0)
                for row in cohort:
                    equity += Decimal(row['pnl_500'])
                    peak = max(peak, equity)
                    worst = max(worst, peak-equity)
                return str(worst)
            return dict(**labels, scheduled=len(selected), settled=len(closed),
                common_valid_settled=len(valid), fills=sum(r['filled'] for r in selected),
                settled_fills=sum(r['filled'] for r in closed),
                unsettled_fills=sum(r['filled'] for r in selected if not r['settled']),
                pnl_300_settled=total(closed, 'pnl_300'), pnl_500_settled=total(closed, 'pnl_500'),
                pnl_300_valid=total(valid, 'pnl_300'), pnl_500_valid=total(valid, 'pnl_500'),
                minus_best_500_settled=(str(sum((Decimal(r['pnl_500']) for r in closed),Decimal(0))-max(Decimal(0),max((Decimal(r['pnl_500']) for r in closed),default=Decimal(0)))) if closed else None),
                max_drawdown_500_settled=drawdown(closed), max_drawdown_500_valid=drawdown(valid))
        batches = []
        for batch in sorted({r['batch'] for r in rows}):
            for lane in PAIRED8_LANES:
                selected = [r for r in rows if r['batch'] == batch and r['lane'] == lane]
                batches.append(summarize(selected, batch=batch, lane=lane))
        totals = [summarize([r for r in rows if r['lane'] == lane], lane=lane) for lane in PAIRED8_LANES]
        def paired(summaries, **labels):
            control, candidate = summaries[:2]
            def delta(key):
                return None if control[key] is None or candidate[key] is None else str(Decimal(candidate[key])-Decimal(control[key]))
            return dict(**labels, settled=control['settled'],
                        delta_500_settled=delta('pnl_500_settled'), delta_500_valid=delta('pnl_500_valid'))
        paired_batches = [paired([r for r in batches if r['batch'] == batch], batch=batch)
                          for batch in sorted({r['batch'] for r in rows})]
        paired_total = paired(totals)
        decision = financial_decision(totals, batches, paired_total, target=self.target)
        return dict(version=PAIRED8_VERSION, identity=self.identity, scheduled=self.target,
                    batch_size=20, cost_bps=[300, 500], research_only=True, rows=rows, batches=batches,
                    totals=totals, paired_batches=paired_batches, paired_total=paired_total,
                    pilot_gate=gate, financial_decision=decision)


def financial_decision(totals,batches,paired_total,*,target):
    candidate=totals[1]
    if target!=600 or candidate['settled']!=600:
        return dict(status='incomplete',label='600場尚未全結算；優勢未證實。',live_eligible=False)
    import random
    rng=random.Random(20260909)
    blocks=[Decimal(r['pnl_500_settled']) for r in batches if r['lane']==PAIRED8_LANES[1]]
    means=sorted(sum((rng.choice(blocks) for _ in blocks),Decimal(0))/600 for _ in range(2000))
    last400=sum(blocks[10:],Decimal(0));reasons=[]
    checks={'fewer_than_570_quality_markets':candidate['common_valid_settled']>=570,
            'fewer_than_60_fills':candidate['settled_fills']>=60,
            'nonpositive_500bps_pnl':Decimal(candidate['pnl_500_settled'])>0,
            'nonpositive_minus_best':Decimal(candidate['minus_best_500_settled'])>0,
            'nonpositive_last400':last400>0,'nonpositive_bootstrap_lower':means[49]>0}
    reasons=[k for k,v in checks.items() if not v]
    return dict(status='unproven' if reasons else 'research_criteria_met',reasons=reasons,live_eligible=False,
        primary_lane=PAIRED8_LANES[1],other_lanes_exploratory=True,last400_pnl_500=str(last400),
        mean_500bps_per_market_ci95=[str(means[49]),str(means[1949])],
        label='本批結束；'+('優勢未證實。' if reasons else '主策略達研究門檻。')+'不自動續跑或開實盤。')

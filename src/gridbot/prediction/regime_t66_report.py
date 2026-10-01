"""Read-only T6.6 paper cohort report, kept separate from official Live PnL."""
import json
import sqlite3
import time
from contextlib import closing
from decimal import Decimal
from pathlib import Path

from .regime_t66_policy import BRANCHES, FINGERPRINT, POLICY


def pnl(quote, side, winner):
    cash, shares = Decimal(quote['cash']), Decimal(quote['net_shares'])
    if not cash.is_finite() or not shares.is_finite() or not 0 < cash <= 1 or shares <= 0:
        raise ValueError('invalid paper cash/shares')
    return (shares/2 if winner == 'DRAW' else shares if side == winner else Decimal(0))-cash


def metrics(root, now_ms=None):
    now = int(time.time()*1000) if now_ms is None else now_ms
    path = Path(root)/'prediction/data/regime-target6/features.sqlite3'
    if not path.is_file():
        return None
    with closing(sqlite3.connect(path.resolve().as_uri()+'?mode=ro', uri=True)) as db:
        db.execute('PRAGMA query_only=ON'); db.execute('BEGIN')
        if not db.execute("SELECT 1 FROM sqlite_master WHERE name='t66_observation_state'").fetchone():
            return None
        from .regime_t66_observer import state
        config = state(db)
        if not config:
            return None
        markets = {s: json.loads(raw) for s, raw in db.execute('SELECT start,payload FROM t66_observation_markets')}
        outcomes = {s: json.loads(raw) for s, raw in db.execute('SELECT start,payload FROM t66_shadow_outcomes')}
        quotes = [(s, branch, json.loads(raw)) for s, branch, raw in db.execute('SELECT start,branch,payload FROM t66_shadow_quotes')]
    official = {}
    main = Path(root)/'prediction/data/prediction.sqlite3'
    if main.is_file():
        with closing(sqlite3.connect(main.resolve().as_uri()+'?mode=ro', uri=True)) as db:
            columns = {r[1] for r in db.execute('PRAGMA table_info(prediction_settlements)')}
            if 'winner' in columns:
                for start, winner in db.execute("SELECT c.start_time_ms,s.winner FROM prediction_settlements s JOIN prediction_campaigns c ON c.campaign_id=s.campaign_id WHERE s.status='SETTLED' AND c.start_time_ms BETWEEN ? AND ?", (config['first_start_ms'], config['first_start_ms']+(config['target']-1)*300000)):
                    if winner in ('UP','DOWN','DRAW'):
                        official.setdefault(start, set()).add(winner)
    def empty():
        return dict(candidates=0, core_overlap=0, quoted=0, known=0, unknown=0, wins=0, losses=0,
                    pnl=Decimal(0), stress_pnl=Decimal(0), stress_known=0, missed=0,
                    delay300=0, delay1000=0, delay300_known=0, delay1000_known=0)
    result = {b: empty() for b in BRANCHES}
    eligible = {}
    for start, m in markets.items():
        if m['fingerprint'] != FINGERPRINT:
            raise ValueError('T6.6 report policy mismatch')
        for branch, c in m.get('candidates', {}).items():
            result[branch]['candidates'] += 1
            result[branch]['core_overlap'] += bool(c['core_overlap'])
    for start, branch, q in quotes:
        m = markets[start]; a = result[branch]
        if (q['fingerprint'] != FINGERPRINT or q['market_start_ms'] != start
                or q['market_topic'] != m['market_topic'] or q['market_id'] != m['market_id']
                or q['candidate'] != m['candidates'][branch]):
            raise ValueError('T6.6 quote identity mismatch')
        a['missed'] += q['status'] == 'UNOBSERVED_WINDOW'
        if q['status'] != 'PAPER_QUOTE_ONLY':
            continue
        at, bt = q['quoted_at_ms'], q['book_at_ms']
        if not start+128000 <= bt <= at <= start+134500 or at-bt > 1000:
            raise ValueError('T6.6 quote outside window')
        if not q['candidate']['control'] and not q['candidate']['incremental_eligible']:
            raise ValueError('core overlap promoted to incremental quote')
        a['quoted'] += 1
        for delay in ('300', '1000'):
            evidence = q['delays'].get(delay, {})
            a['delay'+delay] += evidence.get('status') == 'EXECUTABLE'
            a['delay'+delay+'_known'] += evidence.get('status') in ('EXECUTABLE', 'UNEXECUTABLE')
        o = outcomes.get(start, {})
        if not o.get('complete'):
            a['unknown'] += 1
            continue
        if (o['fingerprint'] != FINGERPRINT or o['market_start_ms'] != start or o['market_topic'] != m['market_topic']
                or o['market_id'] != m['market_id'] or o['known_at_ms'] > now or o['winner'] not in ('UP','DOWN','DRAW')):
            raise ValueError('T6.6 official identity mismatch')
        if official.get(start, {o['winner']}) != {o['winner']}:
            raise ValueError('conflicting official T6.6 winner')
        amount = pnl(q['quote'], q['candidate']['side'], o['winner'])
        a['known'] += 1; a['wins'] += amount > 0; a['losses'] += amount < 0; a['pnl'] += amount
        if q['quote'].get('stress_tick2'):
            a['stress_pnl'] += pnl(q['quote']['stress_tick2'], q['candidate']['side'], o['winner'])
            a['stress_known'] += 1
        if branch in POLICY['priority']:
            eligible.setdefault(start, []).append((at, POLICY['priority'].index(branch), amount))
    # Fixed prospective priority; the portfolio cannot count the same market twice.
    unique = [min(v) for v in eligible.values()]
    return dict(config=config, elapsed=sum(s+300000 <= now for s in markets),
                frozen=sum('market_topic' in m for m in markets.values()),
                missed_decisions=sum(m['status']=='UNOBSERVED_DECISION' for m in markets.values()),
                branches=result, portfolio_known=len(unique), portfolio_pnl=sum((r[2] for r in unique), Decimal(0)))


def format_observation_report(root, now_ms=None):
    report = metrics(root, now_ms)
    if report is None:
        return ''
    lines = ["🧪 T6.6 觀測版｜Live 核心仍為 T6.5",
             f"新市場 {report['elapsed']}/{report['config']['target']}｜凍結 {report['frozen']}｜漏觀測 {report['missed_decisions']}",
             "A／flat／B／fallback 已停止新 Shadow；M4／M6及反方向保留對照，歷史資料保留。",
             "下列為1U可執行報價，非實際成交；未知結果不計入PnL，核心重疊不計新增機會。"]
    for branch, title in BRANCHES.items():
        a = report['branches'][branch]
        lines.append(f"{title}：候選 {a['candidates']}｜核心重疊 {a['core_overlap']}｜報價 {a['quoted']}｜未知 {a['unknown']}｜漏報價 {a['missed']}")
        if a['known']:
            lines.append(f"  {a['wins']}勝/{a['losses']}負｜paper PnL {a['pnl']:+.4f}｜+0.02壓力 {a['stress_pnl']:+.4f}（{a['stress_known']}筆）")
        if a['quoted']:
            lines.append(f"  延遲300ms仍可報價 {a['delay300']}/{a['delay300_known']} 已觀測；1s {a['delay1000']}/{a['delay1000_known']} 已觀測。")
    lines.append(f"四路去重已結算市場 {report['portfolio_known']}｜假設paper PnL {report['portfolio_pnl']:+.4f}；不併入Live損益。")
    lines.append("先收500市場；各分支至少30筆且跨期/成本/完整風控驗證通過，才另行核准Live；不自動升級。")
    return '\n'.join(lines)

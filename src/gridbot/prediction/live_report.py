"""Read-only, lane-aware Telegram report. No execution or control writes."""
from __future__ import annotations

import json
import sqlite3
import time
from collections import defaultdict
from contextlib import closing
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

PROFILE = "regime_target6_v1"
T61_PROFILE = "regime_target6_1_v1"
T62_PROFILE = "regime_target6_2_v1"
T63_PROFILE = "regime_target6_3_v1"
T63A_PROFILE = "regime_target6_3a_v1"
T63B_PROFILE = "regime_target6_3b_v1"
T65_PROFILE = "regime_target6_5_v1"
T67_PROFILE = "regime_target6_7_v1"
RISK_PROFILES = (PROFILE, T61_PROFILE, T62_PROFILE, T63_PROFILE, T63A_PROFILE, T63B_PROFILE, T65_PROFILE, T67_PROFILE)
SLOT = 300000
TZ = timezone(timedelta(hours=8))
TERMINAL = {"FILLED", "CLOSED", "CANCELLED", "CANCELED", "EXPIRED", "FAILED", "REJECTED"}


def _decimal(value):
    number = Decimal(str(value))
    if not number.is_finite():
        raise ValueError("invalid report number")
    return number


def _metrics(events):
    equity = peak = mdd = Decimal(0)
    wins = losses = flats = 0
    for event in sorted(events, key=lambda x: (x["known"], x["id"])):
        pnl = event["pnl"]
        equity += pnl
        peak = max(peak, equity)
        mdd = max(mdd, peak-equity)
        wins += pnl > 0
        losses += pnl < 0
        flats += pnl == 0
    return {"pnl": equity, "mdd": mdd, "wins": wins, "losses": losses, "flats": flats,
            "wr": f"{wins/(wins+losses):.1%}" if wins+losses else "—"}


def _value(metric, events, pending, field="pnl"):
    if not events and pending:
        return "—（待核對／結算）"
    return format(metric[field], "+.4f" if field == "pnl" else ".4f")


def _shadow_metrics(root, loop_id, slots, campaigns, settlements, *, profile, key, branch, now_ms=None):
    """Count frozen fallback quotes; use only a recorded official winner.

    Empty Live campaigns often have a SETTLED row with winner=None. Those
    remain unknown here. Paper PnL assumes immediate execution at the frozen
    initial book, and is never added to Live PnL or the durable risk ledger.
    """
    from .regime_t63a_lane import FINGERPRINT as T63A_FINGERPRINT
    from .regime_t63b_lane import FINGERPRINT as T63B_FINGERPRINT
    from .regime_t65_lane import FINGERPRINT as T65_FINGERPRINT
    fingerprint = (T65_FINGERPRINT if profile == T65_PROFILE else
                   T63B_FINGERPRINT if profile == T63B_PROFILE else T63A_FINGERPRINT)
    now = int(time.time()*1000) if now_ms is None else int(now_ms)

    own = {int(c["start_time_ms"]): c["campaign_id"] for c in campaigns
           if c["loop_id"] == loop_id}
    results = defaultdict(list)
    for row in settlements:
        if row["status"] == "SETTLED":
            results[row["campaign_id"]].append(row["winner"])
    uri = (root/"prediction/data/regime-target6/features.sqlite3").resolve().as_uri()+"?mode=ro"
    count = quoted = wins = losses = flats = unobserved = pending = 0
    quoted_starts = []
    pnl = Decimal(0)
    official_winners = {}
    if profile == T65_PROFILE:
        with closing(sqlite3.connect((root/"prediction/data/prediction.sqlite3").resolve().as_uri()+"?mode=ro", uri=True)) as main:
            columns = {r[1] for r in main.execute("PRAGMA table_info(prediction_shadow_observer_markets)")}
            payload = "payload_json" if "payload_json" in columns else "NULL"
            official_winners = {r[0]: (r[1], r[2], r[3], r[4]) for r in main.execute(
                "SELECT market_topic_id,market_id,start_time_ms,winner," + payload +
                " FROM prediction_shadow_observer_markets WHERE state='SETTLED'")}
    with closing(sqlite3.connect(uri, uri=True, timeout=2)) as db:
        db.execute("PRAGMA query_only=ON")
        db.execute("BEGIN")
        has_outcomes = profile == T65_PROFILE and db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='t65_shadow_outcomes'").fetchone()
        for slot in slots:
            start = int(slot["market_start_ms"])
            row = db.execute("SELECT payload FROM decisions WHERE start=?", (start,)).fetchone()
            if row is None:
                continue
            decision = json.loads(row[0])
            if decision.get("fingerprint") != fingerprint:
                raise ValueError("shadow policy fingerprint mismatch")
            shadow = decision.get(key)
            if shadow is None:
                continue
            resolved = None
            if has_outcomes:
                outcome_row = db.execute('SELECT payload FROM t65_shadow_outcomes WHERE start=?', (start,)).fetchone()
                if outcome_row:
                    resolved = json.loads(outcome_row[0])
                    if (resolved.get('fingerprint') != fingerprint or resolved.get('market_start_ms') != start
                            or resolved.get('market_topic') != decision.get('market_topic')
                            or resolved.get('market_id') != decision.get('market_id')):
                        raise ValueError('official shadow outcome identity mismatch')
            if profile == T65_PROFILE and key in ("shadow_m4", "shadow_m6"):
                observed = db.execute("SELECT payload FROM t65_shadow_quotes WHERE start=? AND branch=?",
                                      (start, branch)).fetchone()
                if observed:
                    shadow = json.loads(observed[0])
                    if (shadow.get('fingerprint') != fingerprint or shadow.get('market_start_ms') != start
                            or shadow.get('market_topic') != decision.get('market_topic')
                            or shadow.get('market_id') != decision.get('market_id')
                            or shadow.get('unit_usdt') != decision.get('unit_usdt')):
                        raise ValueError("shadow observation identity mismatch")
            if shadow.get("branch") != branch or shadow.get("fill_status") not in (
                    "PAPER_QUOTE_ONLY", "NO_EXECUTABLE_INITIAL_QUOTE", "AWAITING_SHADOW_WINDOW",
                    "NO_EXECUTABLE_WINDOW_QUOTE", "UNOBSERVED_WINDOW"):
                raise ValueError("invalid shadow candidate")
            side = shadow["candidate"]["side"]
            if side not in ("UP", "DOWN") or start not in own:
                raise ValueError("shadow market identity unavailable")
            count += 1
            if shadow["fill_status"] != "PAPER_QUOTE_ONLY":
                if shadow.get("quote") is not None:
                    raise ValueError("invalid unquoted shadow")
                status = shadow['fill_status']
                if profile == T65_PROFILE and status == 'AWAITING_SHADOW_WINDOW' and now > start+134500:
                    status = ('NO_EXECUTABLE_WINDOW_QUOTE' if shadow.get('eligible_books', 0)
                              else 'UNOBSERVED_WINDOW')
                unobserved += status == 'UNOBSERVED_WINDOW'
                pending += status == 'AWAITING_SHADOW_WINDOW'
                continue
            if profile == T65_PROFILE and key in ("shadow_m4", "shadow_m6"):
                at, book_at = int(shadow['quoted_at_ms']), int(shadow['book_at_ms'])
                if not start+128000 <= book_at <= at <= start+134500 or at-book_at > 1000:
                    raise ValueError("invalid shadow quote window")
            quote = shadow["quote"]
            cash, shares = _decimal(quote["cash"]), _decimal(quote["net_shares"])
            if not 0 < cash <= _decimal(decision["unit_usdt"]) or shares <= 0:
                raise ValueError("invalid shadow quote")
            quoted += 1
            quoted_starts.append(start)
            official = results[own[start]]
            if profile == T65_PROFILE:
                official = list(official)
                if (resolved and resolved.get('complete') is True and resolved.get('winner') in ('UP', 'DOWN', 'DRAW')
                        and int(resolved['known_at_ms']) <= now):
                    official.append(resolved['winner'])
                observer = official_winners.get(decision['market_topic'])
                if observer:
                    observer_id = observer[0]
                    if not observer_id:
                        # The generic observer stores a topic-level ID, which
                        # can be empty for binary markets. Resolve the UP ID
                        # only from its saved official detail, never by assuming
                        # that a matching topic alone proves the identity.
                        from .models import MarketInfo
                        from .worker import PredictionWorker
                        detail = json.loads(observer[3])
                        market = MarketInfo.from_api(detail)
                        # The observer payload may be its admission snapshot;
                        # its SETTLED row holds the later official winner.
                        # Compare any winner in the detail when present, while
                        # retaining the independent outcome conflict check below.
                        detail_winner = PredictionWorker._official_shadow_resolution(detail)
                        if (market.market_topic_id != decision['market_topic']
                                or market.start_time_ms != start
                                or market.end_time_ms != start+SLOT
                                or (detail_winner is not None and detail_winner != observer[2])):
                            raise ValueError("official shadow market identity mismatch")
                        observer_id = market.up_market_id
                    if observer_id != decision['market_id'] or observer[1] != start:
                        raise ValueError("official shadow market identity mismatch")
                    official.append(observer[2])
                evidence = {w for w in official if w in ('UP', 'DOWN', 'DRAW')}
                if len(evidence) > 1:
                    raise ValueError('conflicting official shadow outcomes')
                official = list(evidence)
            if len(official) != 1 or official[0] not in ("UP", "DOWN", "DRAW"):
                continue
            net = (shares/2 if official[0] == 'DRAW' else shares if official[0] == side else Decimal(0))-cash
            wins += net > 0
            losses += net < 0
            flats += net == 0
            pnl += net
    known = wins+losses+flats
    return {"candidates": count, "quoted": quoted, "unquoted": count-quoted,
            "known": known, "unknown": quoted-known,
            "wins": wins, "losses": losses, "flats": flats, "unobserved": unobserved, "pending": pending,
            "quoted_starts": tuple(quoted_starts),
            "wr": f"{wins/(wins+losses):.1%}" if wins+losses else "—",
            "pnl": pnl}


def _t63a_shadow_metrics(root, loop_id, slots, campaigns, settlements):
    return _shadow_metrics(root, loop_id, slots, campaigns, settlements,
                           profile=T63A_PROFILE, key="shadow_fallback", branch="fallback")


def report_pages(text, limit=3400):
    """Bound replies by UTF-16 units, including Telegram's emoji accounting."""
    pages, page = [], ""
    for line in text.splitlines():
        candidate = page + ("\n" if page else "") + line
        if len(candidate.encode("utf-16-le"))//2 > limit and page:
            pages.append(page)
            page = line
        else:
            page = candidate
    if page:
        pages.append(page)
    return pages


def _format_live_report(root, *, now_ms=None, c180_formatter=None, context=None):
    now = int(time.time()*1000) if now_ms is None else int(now_ms)
    root = Path(root)
    uri = (root/"prediction/data/prediction.sqlite3").resolve().as_uri()+"?mode=ro"
    with closing(sqlite3.connect(uri, uri=True, timeout=2)) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA query_only=ON")
        conn.execute("BEGIN")
        # An active Shadow/other lane must never silently display an old Live
        # result. Without an active loop, show the most recently created loop.
        loop = conn.execute("SELECT * FROM prediction_loops ORDER BY "
                            "CASE WHEN state='RUNNING' THEN 0 ELSE 1 END,created_at_ms DESC,loop_id DESC LIMIT 1").fetchone()
        if loop is None:
            return "📊 Prediction Report｜尚無 loop。"
        loop = dict(loop)
        profile, loop_id = loop["strategy_profile"], loop["loop_id"]
        if context is not None:
            context['profile'] = profile
        clock = datetime.fromtimestamp(now/1000,TZ).strftime("%m/%d %H:%M:%S")
        if loop["mode"] != "LIVE":
            return (f"📊 Prediction Report｜目前為 {loop['mode']}\n截至 {clock}（台灣時間）\n"
                    f"Loop {loop_id}｜策略 {profile}｜{loop['state']}\n"
                    "此 loop 不是 Live；請使用 /shadow_report 查看模擬資料。")
        if profile == "c180_favorite_hold_v1":
            if c180_formatter is None:
                raise ValueError("C180 report formatter unavailable")
            conn.commit()
            return c180_formatter(root, now_ms=now, loop_id=loop_id)
        if profile not in RISK_PROFILES:
            return (f"📊 Prediction Live Report\n截至 {clock}（台灣時間）\nLoop {loop_id}\n"
                    f"策略 {profile}｜{loop['state']}｜完成 {loop['completed']}/{loop['target']} 場\n"
                    "此 lane 尚無專用 report；不以其他 lane 的績效替代。")

        rows = lambda sql, args=(): [dict(r) for r in conn.execute(sql,args)]
        slots = rows("SELECT * FROM prediction_regime_slots WHERE loop_id=? ORDER BY market_start_ms",(loop_id,))
        campaigns = rows("SELECT c.*,l.strategy_profile AS lane_profile FROM prediction_campaigns c JOIN prediction_loops l ON l.loop_id=c.loop_id "
                         "WHERE l.strategy_profile IN (?,?,?,?,?,?,?,?) AND l.mode='LIVE'",RISK_PROFILES)
        claims = rows("SELECT q.*,i.status,i.order_id,i.unknown,i.submission_at_ms FROM prediction_regime_entry_claims q "
                      "JOIN prediction_loops l ON l.loop_id=q.loop_id LEFT JOIN prediction_order_intents i ON i.intent_id=q.intent_id "
                      "WHERE l.strategy_profile IN (?,?,?,?,?,?,?,?) AND l.mode='LIVE'",RISK_PROFILES)
        fills = rows("SELECT DISTINCT f.campaign_id FROM prediction_fills f JOIN prediction_campaigns c ON c.campaign_id=f.campaign_id "
                     "JOIN prediction_loops l ON l.loop_id=c.loop_id WHERE l.strategy_profile IN (?,?,?,?,?,?,?,?) AND l.mode='LIVE' AND f.order_side='BUY'",RISK_PROFILES)
        settlements = rows("SELECT p.*,o.net_pnl AS observed_net,o.known_at_ms FROM prediction_settlements p "
                           "JOIN prediction_campaigns c ON c.campaign_id=p.campaign_id JOIN prediction_loops l ON l.loop_id=c.loop_id "
                           "LEFT JOIN prediction_regime_settlement_observations o ON o.settlement_id=p.settlement_id AND o.campaign_id=p.campaign_id "
                           "WHERE l.strategy_profile IN (?,?,?,?,?,?,?,?) AND l.mode='LIVE'",RISK_PROFILES)
        unknown_rows = rows("SELECT DISTINCT c.campaign_id FROM prediction_campaigns c JOIN prediction_loops l ON l.loop_id=c.loop_id "
                            "WHERE l.strategy_profile IN (?,?,?,?,?,?,?,?) AND l.mode='LIVE' AND (c.pending_unknown=1 OR EXISTS "
                            "(SELECT 1 FROM prediction_order_intents i WHERE i.campaign_id=c.campaign_id AND i.unknown=1))",RISK_PROFILES)
        gate_row = conn.execute("SELECT config_value_json FROM prediction_runtime_config WHERE config_key='regime_target6_risk_v1'").fetchone()
        gate = json.loads(gate_row[0]) if gate_row else None
        loop_guard_row = conn.execute(
            "SELECT config_value_json FROM prediction_runtime_config WHERE config_key=?",
            (profile.removesuffix("_v1") + "_loop_risk:" + loop_id,)).fetchone() if profile in (T63B_PROFILE, T65_PROFILE, T67_PROFILE) else None
        loop_guard = json.loads(loop_guard_row[0]) if loop_guard_row else None
        selected_rows = conn.execute(
            "SELECT config_key,config_value_json FROM prediction_runtime_config "
            "WHERE config_key IN ('prediction_selected_strategy','prediction_selected_order_unit')"
        ).fetchall()
        selected = {row[0]: json.loads(row[1]) for row in selected_rows}
        conn.commit()

    cmap = {r["campaign_id"]: r for r in campaigns}
    qmap = defaultdict(list)
    smap = defaultdict(list)
    for row in claims:
        qmap[row["campaign_id"]].append(row)
    for row in settlements:
        smap[row["campaign_id"]].append(row)
    fill_ids = {r["campaign_id"] for r in fills}
    current_ids = {cid for cid,c in cmap.items() if c["loop_id"] == loop_id}
    unknown_ids = {r["campaign_id"] for r in unknown_rows}
    events, issues = [], set()
    for cid in fill_ids:
        campaign = cmap[cid]
        q = qmap[cid]
        settled = [r for r in smap[cid] if r["status"] == "SETTLED"]
        if len(q) != 1 or q[0]["loop_id"] != campaign["loop_id"] or q[0]["market_start_ms"] != campaign["start_time_ms"]:
            issues.add("成交與lane claim不一致")
            continue
        if len(settled) > 1:
            issues.add("官方結算重複")
            continue
        if not settled:
            continue
        row = settled[0]
        try:
            pnl = _decimal(row["net_pnl"])
            unit = _decimal(q[0]["unit_usdt"])
            if (pnl != _decimal(row["observed_net"]) or row["known_at_ms"] is None
                    or int(row["known_at_ms"]) > now or unit not in (1,2,3)
                    or (campaign["lane_profile"] not in (T62_PROFILE, T63_PROFILE, T63A_PROFILE, T63B_PROFILE, T65_PROFILE, T67_PROFILE) and unit != 1)):
                raise ValueError("unconfirmed observation")
        except (ValueError, TypeError, ArithmeticError):
            issues.add("官方與風控結算觀測待核對")
            continue
        events.append({"cid":cid,"loop":campaign["loop_id"],"start":int(campaign["start_time_ms"]),
                       "pnl":pnl,"unit":unit,"known":int(row["known_at_ms"]),"id":row["settlement_id"]})
    known_ids = {e["cid"] for e in events}
    pending = fill_ids-known_ids
    current = [e for e in events if e["loop"] == loop_id]
    metric = _metrics(current)
    elapsed_slots = sum(s['verified_at_ms'] is not None and int(s['market_start_ms'])+(270000 if profile == T67_PROFILE else 136000) <= now for s in slots)
    own_claims = [q for q in claims if q["loop_id"] == loop_id]
    submitted = sum(q["submission_at_ms"] is not None or q["order_id"] is not None for q in own_claims)
    inflight = sum(q["status"] is None or str(q["status"]).upper() not in TERMINAL or q["unknown"] for q in own_claims)
    current_pending = len(pending & current_ids)
    empty = sum(s["empty_attested_at_ms"] is not None for s in slots)
    label = ("T6.7 Live Report｜三策略驗證" if profile == T67_PROFILE else "T6.5 Live Report｜A／flat／M4／M6 Shadow" if profile == T65_PROFILE else
             "T6.3b Live Report｜B／補位Shadow＋整輪回撤" if profile == T63B_PROFILE else
             "T6.3a Live Report｜T6.1補位Shadow" if profile == T63A_PROFILE else
             "T6.3 Live Report｜A分歧／B順勢／C淨跌" if profile == T63_PROFILE else "T6.2 Live Report｜價格護欄" if profile == T62_PROFILE else
             "T6.1 Live Report｜補位" if profile == T61_PROFILE else "T6 Live Report｜市況分流")
    loop_units = {str(q["unit_usdt"]) for q in own_claims}
    loop_unit_text = "/".join(sorted(loop_units)) if loop_units else "待成交確認"
    lines = [f"📊 Regime {label}",f"截至 {clock}（台灣時間）｜Loop {loop_id}",
             f"狀態 {loop['state']}｜完成 {loop['completed']}/{loop['target']} 場｜本輪成交金額 {loop_unit_text} USDT",
             f"本輪 WR {metric['wr']}（{metric['wins']}勝/{metric['losses']}負/{metric['flats']}平；已結算成交 {len(current)}）",
             f"本輪已知淨 PnL {_value(metric,current,current_pending)} USDT｜MDD {_value(metric,current,current_pending,'mdd')} USDT",
             f"進場intent {len(own_claims)}｜送單嘗試 {submitted}｜成交市場 {len(fill_ids & current_ids)}",
             f"待結算/核對 {current_pending}｜未終結intent {inflight}｜未知訂單市場 {len(unknown_ids & current_ids)}｜已確認未成交 {empty}"]
    selected_lane = selected.get('prediction_selected_strategy')
    selected_unit = selected.get('prediction_selected_order_unit')
    if profile in (T62_PROFILE, T63_PROFILE, T63A_PROFILE, T63B_PROFILE, T65_PROFILE, T67_PROFILE) and isinstance(selected_lane, dict) and isinstance(selected_unit, dict):
        if selected_lane.get('profile') in (T62_PROFILE, T63_PROFILE, T63A_PROFILE, T63B_PROFILE, T65_PROFILE, T67_PROFILE):
            unit = str(selected_unit.get('order_unit_usdt') or '')
            if unit in ('1', '2', '3'):
                unit_label = 'T6.7' if selected_lane.get('profile') == T67_PROFILE else 'T6.2／T6.3／T6.3a／T6.3b'
                lines.append(f"目前選擇 {unit_label} 每筆{unit} USDT；上方本輪成交金額依該輪實際claim，兩者可能不同。")
    if not slots:
        lines.append("等待第一個正式市場登錄；尚未開始排程區段。")
    if profile in (T65_PROFILE, T67_PROFILE):
        ended_starts = {int(s['market_start_ms']) for s in slots
                        if s['verified_at_ms'] is not None and int(s['market_start_ms'])+SLOT <= now}
        filled_starts = {int(cmap[cid]['start_time_ms']) for cid in fill_ids & current_ids}
        closed_fills = len(filled_starts & ended_starts)
        if ended_starts:
            suffix = "訊號／路由不計入成交。" if profile == T67_PROFILE else "Shadow報價不計入成交。"
            lines.append(f"Live fill rate {closed_fills/len(ended_starts):.1%}（{closed_fills}/{len(ended_starts)} 已結束登錄市場）；{suffix}")

    if profile == T67_PROFILE:
        lines.append("三策略共用同市場一次BUY：外部先行、reference校正、淺回撤；原有風控接續。")
        try:
            from .regime_t67_policy import FINGERPRINT as T67_FP, BRANCHES
            from .regime_t67_report import branch_metrics
            metrics = branch_metrics(root, cmap, current_ids, fill_ids, current, fingerprint=T67_FP, slots=slots)
            for branch in BRANCHES:
                m = metrics[branch]
                lines.append(f"{branch} Live｜成交 {m['fills']}｜已知WR {m['wr']}｜已知PnL {m['pnl']:+.4f} USDT｜待結算 {m['pending']}")
            if metrics['unattributed']:
                lines.append(f"子策略歸因待核對 {metrics['unattributed']} 筆；保留官方Live總PnL。")
        except (OSError, sqlite3.Error, ValueError, KeyError, TypeError):
            lines.append("T6.7 子策略歸因待核對；保留官方Live總PnL。")

    if profile == T62_PROFILE:
        lines.append("T6.2：flat 原模型入場上限0.60；保留T6.1補位，可選每筆1/2/3U。")
        lines.append("回測改善主要來自避開高價flat；成交率可能略降，尚未達12U／100場目標。")
    if profile == T63_PROFILE:
        lines.append("T6.3：A JEV分歧DOWN 0.20–0.40；B晚啟動順勢0.25–0.65；C淨跌DOWN 0.65–0.75。")
        lines.append("每筆1/2/3U；共用持久風控；新分支收益尚待實際驗證。")
    if profile == T63A_PROFILE:
        lines.append("T6.3a：T6主規則與A/B/C維持T6.3；T6.1補位僅記錄初始可執行盤口，不送Live BUY。")
        try:
            shadow = _t63a_shadow_metrics(root, loop_id, slots, campaigns, settlements)
            lines.append(f"補位Shadow：候選 {shadow['candidates']}｜有官方勝方 {shadow['known']}｜未知 {shadow['unknown']}。")
            if shadow['known']:
                lines.append(f"補位Shadow已知 WR {shadow['wr']}（{shadow['wins']}勝/{shadow['losses']}負）｜假設即時成交的paper PnL {shadow['pnl']:+.4f} USDT。")
            lines.append("Shadow僅為初始盤口反事實估算，沒有真實成交；未知結果不補零。")
        except (OSError, sqlite3.Error, ValueError, KeyError, TypeError):
            issues.add("補位Shadow紀錄無法核對")
    if profile in (T63B_PROFILE, T65_PROFILE):
        retired = False
        retirement_started = False
        if profile == T65_PROFILE:
            try:
                from .regime_t66_observer import state as observation_state
                with closing(sqlite3.connect((root/"prediction/data/regime-target6/features.sqlite3").resolve().as_uri()+"?mode=ro", uri=True)) as feature_db:
                    observation = observation_state(feature_db)
                starts = [int(slot['market_start_ms']) for slot in slots]
                retirement_started = bool(observation and observation['enabled'] and
                                          any(start >= observation['first_start_ms'] for start in starts))
                retired = bool(retirement_started and
                               all(start >= observation['first_start_ms'] for start in starts))
            except (OSError, sqlite3.Error, ValueError, KeyError, TypeError):
                pass
            lines.append("T6.5核心維持；A／flat／B／補位已停止新Shadow，T6.6獨立觀測見下方。" if retirement_started else "T6.5：A、flat/original、B與補位只做Shadow；保留其他T6/C Live，M4/M6於T+128–134.5秒只觀測新鮮可執行報價。")
            if retirement_started and not retired:
                lines.append("本輪跨觀測啟用邊界；以下保留啟用前的A／flat／B／補位歷史Shadow。")
        else:
            lines.append("T6.3b：T6/A/C可Live；B與T6.1補位保留紙上盤口，不送Live BUY，排除分支時整場跳過。")
        branches = [("shadow_b", "B_late_momentum", "B Shadow"),
                    ("shadow_fallback", "fallback", "補位Shadow")]
        if profile == T65_PROFILE:
            branches += [("shadow_a", "A_jev_conflict", "A Shadow"),
                         ("shadow_flat", "T6", "flat/original Shadow"),
                         ("shadow_m4", "M4_first_pullback", "M4 Shadow"),
                         ("shadow_m6", "M6_neutral_cheap", "M6 Shadow")]
        if retired:
            branches = [b for b in branches if b[0] not in ("shadow_a", "shadow_flat", "shadow_b", "shadow_fallback")]
        for key, branch, title in branches:
            try:
                shadow = _shadow_metrics(root, loop_id, slots, campaigns, settlements,
                                         profile=profile, key=key, branch=branch, now_ms=now)
                if profile == T65_PROFILE:
                    ended_quotes = sum(start+136000 <= now for start in shadow['quoted_starts'])
                    if elapsed_slots:
                        lines.append(f"{title} 報價參與率 {ended_quotes/elapsed_slots:.1%}（{ended_quotes}/{elapsed_slots} 已過窗口市場）。")
                    lines.append(f"觀測遺漏 {shadow['unobserved']}｜待觀測 {shadow['pending']}；報價不等於成交。")
                lines.append(f"{title}：候選 {shadow['candidates']}｜有可執行報價 {shadow['quoted']}｜無報價 {shadow['unquoted']}｜官方勝方已知 {shadow['known']}｜未知 {shadow['unknown']}。")
                if shadow['known']:
                    lines.append(f"{title} 已知WR {shadow['wr']}（{shadow['wins']}勝/{shadow['losses']}負）｜假設即時成交paper PnL {shadow['pnl']:+.4f} USDT。")
            except (OSError, sqlite3.Error, ValueError, KeyError, TypeError):
                issues.add(title + "紀錄無法核對")
        lines.append("Shadow是假設記錄報價即時成交；沒有真實成交，未知結果不補零。")
    if profile in (T63B_PROFILE, T65_PROFILE, T67_PROFILE):
        if isinstance(loop_guard, dict) and loop_guard.get("loop_id") == loop_id:
            lines.append(f"整輪風控1U等值：高點 {loop_guard.get('peak_1u')}｜目前 {loop_guard.get('equity_1u')}｜最大回撤 {loop_guard.get('mdd_1u')} / 3.5｜停單 {loop_guard.get('halt_reason') or '未觸發'}。")
        else:
            lines.append("整輪回撤風控尚無可核對狀態；進場仍須通過即時檢查。")
    if profile == T61_PROFILE:
        lines.append("進場順序：T6 原規則優先；原規則跳過後才評估 T6.1，並共用固定1U停單鎖。")
        lines.append("9/25–9/27 回測為同樣本挑選，非前瞻成交率或獲利保證。")
    lines += ["", "跨 loop 持久風控（Regime T6／T6.1 共用；T6.2／T6.3／T6.3a／T6.3b／T6.5／T6.7接續同一風控）："]
    if not isinstance(gate,dict):
        lines.append("風控尚未初始化；不推定為已通過或已解鎖。")
        if claims:
            issues.add("已有claim但持久風控狀態遺失")
    else:
        from .regime_lane import FINGERPRINT
        lane_metric = _metrics(events)
        normalized_events = [{**e, "pnl":e["pnl"]/e["unit"]} for e in events]
        normalized_metric = _metrics(normalized_events)
        lines.append(f"累計已知淨 PnL {_value(lane_metric,events,len(pending))} USDT｜風控1U等值 {_value(normalized_metric,normalized_events,len(pending))} / -6.0000")
        lines.append("混合金額歷史按每筆實際投入折算；純2U/3U時累計停單線相當於-12/-18U、20場MDD相當於7/10.5U。")
        lines.append(f"全lane待結算/核對 {len(pending)}｜未知訂單市場 {len(unknown_ids)}")
        if gate.get("fingerprint") != FINGERPRINT:
            issues.add("策略指紋與持久風控不一致")
        try:
            if "net_pnl_usdt" in gate and _decimal(gate["net_pnl_usdt"]) != lane_metric["pnl"]:
                issues.add("風控累計與可核對結算不一致")
            if "risk_equity_1u" in gate and _decimal(gate["risk_equity_1u"]) != normalized_metric["pnl"]:
                issues.add("風控1U等值與可核對結算不一致")
        except (ValueError,TypeError,ArithmeticError):
            issues.add("風控累計金額無效")
        reasons = {"scheduled20_mdd_3.5":"20場風控回撤達3.5（1U等值）", "cumulative_loss_6":"風控累計虧損達6（1U等值）",
                   "unknown_order_reconciliation_required":"未知訂單，需完成對帳並保留停單鎖"}
        halt = gate.get("halt_reason")
        lines.append("持久停單："+(reasons.get(halt,str(halt)) if halt else "未觸發；仍須通過即時進場檢查"))
        try:
            anchor = int(gate["first_market_start_ms"])
            if anchor <= 0 or anchor % SLOT:
                raise ValueError("invalid epoch")
            lines.append("固定20場區段（包含跳過場，跨loop沿用起點）：")
            relevant = sorted({(int(s["market_start_ms"])-anchor)//SLOT//20 for s in slots if s["verified_at_ms"] is not None})
            if any(b < 0 for b in relevant):
                raise ValueError("slot before epoch")
            if len(relevant)>5:
                lines.append(f"共 {len(relevant)} 段；顯示最近5段。")
            for block in relevant[-5:]:
                start,end=anchor+block*20*SLOT,anchor+(block+1)*20*SLOT
                batch=[e for e in events if start<=e["start"]<end]
                bm=_metrics(batch)
                normalized_batch=[{**e,"pnl":e["pnl"]/e["unit"]} for e in batch]
                normalized_bm=_metrics(normalized_batch)
                bp=sum(start<=int(cmap[cid]["start_time_ms"])<end for cid in pending)
                lines.append(f"  第{block*20+1}–{(block+1)*20}場｜WR {bm['wr']}｜PnL {_value(bm,batch,bp)} USDT")
                lines.append(f"  MDD {_value(bm,batch,bp,'mdd')} USDT｜風控1U等值 {_value(normalized_bm,normalized_batch,bp,'mdd')} / 3.5000｜已結成交 {len(batch)}｜待結/核對 {bp}")
        except (ValueError,KeyError,TypeError):
            issues.add("持久排程起點無法核對")
    if loop["new_entries_stopped"] or loop["hard_stop_latched"]:
        lines.append("⚠️ 本輪停止新進場或 Hard Stop 已鎖定。")
    lines.append("停單跨重啟、跨loop保留；不自動恢復、不自動解鎖。")
    if issues:
        lines.append("⚠️ 資料待核對："+"、".join(sorted(issues))+"；績效僅含可核對結算。")
    lines.append("WR=勝/(勝+負)，依淨損益分類，損益平手不計；未結算不補零。")
    lines.append("PnL依官方費後結算，不重複扣費；不等於已領現金，亦不含額外AI成本分攤。")
    return "\n".join(lines)


def format_live_report(root, *, now_ms=None, c180_formatter=None):
    context = {}
    report = _format_live_report(root, now_ms=now_ms, c180_formatter=c180_formatter, context=context)
    if context.get('profile') == T67_PROFILE:
        return report
    try:
        from .regime_t66_report import format_observation_report
        observation = format_observation_report(root, now_ms=now_ms)
    except (OSError, sqlite3.Error, ValueError, KeyError, TypeError, ArithmeticError):
        observation = "T6.6觀測資料無法核對；不顯示未驗證的paper損益。"
    return report + ("\n\n" + observation if observation else "")

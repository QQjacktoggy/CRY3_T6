"""Build the full dashboard page (same sections as the Custom view) from read-only data.

Run by dash_bot.py as a short-lived child process:
    python3 -I dash_page.py OUT_HTML CACHE_DIR [PREDICTION_DB]
Prints one JSON line {"caption": ..., "elapsed_ms": ...} for the Telegram message.
"""
from __future__ import annotations

import html as H
import json
import os
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import dash_coins  # noqa: E402
import dash_data  # noqa: E402

TW = timezone(timedelta(hours=8))
BRANCH_CACHE_S = 1800
COIN_CACHE_S = 900
MDD_LIMIT = "3.5"
STYLE = ("<style>.w{color:#1a7f37}.l{color:#c62828}table{border-collapse:collapse}"
         "td,th{padding:2px 8px;border-bottom:1px solid #ddd;text-align:left}</style>")
REASONS = {
    "no_candidate": "沒有符合的分支", "selected_not_filled": "選中但價格區間內沒成交",
    "shallow_prior_not_against_5bp": "淺回撤逆勢條件擋下", "first_up_prior_below_5bp": "5bp 條件擋下",
    "branch_disabled": "分支已停用", "in_progress": "尚未結算", "initial_window_missed": "資料太晚錯過",
    "no_executable_price": "沒有可成交價格", "no_decision_row": "沒有決策紀錄",
}
RULE_BLOCKS = {"shallow_prior_not_against_5bp", "first_up_prior_below_5bp", "branch_disabled"}


def zh(code):
    if not code:
        return "—"
    return "、".join(REASONS.get(c, c) for c in str(code).split(","))


def hm(ms):
    return datetime.fromtimestamp(ms / 1000, TW).strftime("%H:%M")


def md_hm(ms):
    return datetime.fromtimestamp(ms / 1000, TW).strftime("%m-%d %H:%M")


def d(v):
    return Decimal(str(v or 0))


def signed(v, places=3):
    v = d(v)
    return f"{v:+.{places}f}" if v else f"{0:.{places}f}"


def cls(v):
    v = d(v)
    return ' class="w"' if v > 0 else ' class="l"' if v < 0 else ""


def wr(w, l):
    return f"{round(100 * w / (w + l))}%" if w + l else "—"


def price(v):
    if v is None:
        return "—"
    s = f"{Decimal(str(v)):.3f}"
    return s[1:] if s.startswith("0") else s


def coin_of(loop):
    return str(loop.get("symbol") or "BTCUSDT").replace("USDT", "")


# --- sections ------------------------------------------------------------------------

def risk_flags(cfg, now_ms):
    hb = cfg.get("prediction_heartbeat") or {}
    hb_ms = max(int(hb.get(k) or 0) for k in ("last_loop_at_ms", "last_db_write_at_ms")) if isinstance(hb, dict) else 0
    hb_ok = bool(hb_ms) and now_ms - hb_ms < 120000
    risk = cfg.get("regime_target6_risk_v1") or {}
    halt = risk.get("halt_reason") if isinstance(risk, dict) else None
    hs_cfg = cfg.get("prediction_hard_stop_latched") or {}
    rs = cfg.get("prediction_risk_state") or {}
    hs = bool((isinstance(hs_cfg, dict) and hs_cfg.get("latched")) or (isinstance(rs, dict) and rs.get("hard_stop_latched")))
    lock = bool(halt and str(halt).startswith("scheduled20"))
    return {"hb_ok": hb_ok, "halt": halt, "hs": hs, "lock": lock}


def header(snap, flags):
    loop = snap.get("loop")
    now = snap["read_at_ms"]
    if not loop:
        return (f"<h2>T6 Live</h2><p><b>目前沒有 loop 在跑。</b>資料讀取 {hm(now)}（台灣時間）</p>", None)
    prof = loop.get("profile") or loop.get("strategy_profile") or ""
    fp = (loop.get("execution_fingerprint") or "")[:8]
    title = os.environ.get("DASH_TITLE") or f"T6 {coin_of(loop)} Live（{fp + '…，' if fp else ''}profile {prof}）"
    running = loop.get("state") == "RUNNING"
    w, l = loop["wins"], loop["losses"]
    stopped = "已停止新進場" if loop.get("new_entries_stopped") else "正常下單"
    status = (f"<p><b>{coin_of(loop)} {H.escape(loop['loop_id'])} {'進行中' if running else '已結束'}</b>，"
              f"{md_hm(loop['created_at_ms'])} 開始，跑 {loop.get('completed')}/{loop.get('target')} 場，"
              f"{w} 勝 {l} 負，勝率 {wr(w, l)}，淨損益 {signed(loop['filled_pnl'])}，"
              f"MDD {d(loop['max_drawdown']):.3f} / {MDD_LIMIT}。心跳{'正常' if flags['hb_ok'] else '異常'}，{stopped}，"
              f"HS {'已鎖' if flags['hs'] else '未鎖'}，halt：{H.escape(str(flags['halt'] or '無'))}，"
              f"共用鎖{'鎖住' if flags['lock'] else '未鎖'}。資料時間 {hm(now)} TW（按 /dash 唯讀查詢）。</p>")
    red = []
    if flags["hs"]:
        red.append("HS 已鎖住，不會再下單。")
    if flags["lock"]:
        red.append(f"共用停單鎖（{H.escape(str(flags['halt']))}）鎖住中，要用 TG 解除按鈕才能恢復下單。")
    elif flags["halt"]:
        red.append(f"halt：{H.escape(str(flags['halt']))}，目前停單。")
    if red:
        status += f'<p class="l"><b>{"".join(red)}</b></p>'
    return f"<h2>{H.escape(title)}</h2>{status}", loop


def end_reason(row):
    if row.get("state") == "RUNNING":
        return "進行中"
    tr = str(row.get("terminal_reason") or "")
    if row.get("completed") and row.get("target") and row["completed"] >= row["target"]:
        return "完成"
    if "mdd" in tr.lower():
        return f"MDD 停單（{d(row['max_drawdown']):.3f}），已取消"
    if row.get("state") == "CANCELLED" or "cancel" in tr.lower():
        return "已取消" + (f"（{H.escape(tr)}）" if tr and "cancel" not in tr.lower() else "")
    return H.escape(tr or str(row.get("state") or "—"))


def history_table(hist):
    out = ["<h3>最近兩輪</h3><table><tbody><tr><th>loop</th><th>時間</th><th>場數</th><th>成交</th>"
           "<th>勝/負</th><th>勝率</th><th>淨損益</th><th>結束原因</th></tr>"]
    for r in hist:
        w, l = r["wins"], r["losses"]
        running = r.get("state") == "RUNNING"
        span = md_hm(r["created_at_ms"]) + "–" + ("" if running else hm(r["updated_at_ms"]))
        out.append(f"<tr><td>{coin_of(r)} {H.escape(r['loop_id'])}</td><td>{span}</td><td>{r.get('completed')}</td>"
                   f"<td>{w + l}</td><td>{w}/{l}</td><td>{wr(w, l)}</td><td{cls(r['filled_pnl'])}>"
                   f"{signed(r['filled_pnl'])}</td><td>{end_reason(r)}</td></tr>")
    out.append("</tbody></table>")
    return "".join(out)


def fill_stats(markets):
    done = [m for m in markets if m["skip_reason"] != "in_progress"]
    by = defaultdict(lambda: {"sel": 0, "fill": 0, "w": 0, "l": 0, "net": Decimal(0), "nofill": Counter()})
    not_sel = Counter()
    blocked = 0
    for m in done:
        if m["selected"]:
            b = by[m["branch"] or "?"]
            b["sel"] += 1
            if m["fill_price"]:
                b["fill"] += 1
                if m["net_pnl"] is not None:
                    b["net"] += d(m["net_pnl"])
                    b["w"] += m["result"] == "W"
                    b["l"] += m["result"] == "L"
            else:
                b["nofill"][m["skip_reason"] or "selected_not_filled"] += 1
        else:
            for c in str(m["skip_reason"] or "?").split(","):
                not_sel[c] += 1
            if set(str(m["skip_reason"] or "").split(",")) & RULE_BLOCKS:
                blocked += 1
    n = len(done)
    s = sum(b["sel"] for b in by.values())
    f = sum(b["fill"] for b in by.values())
    tw = sum(b["w"] for b in by.values())
    tl = sum(b["l"] for b in by.values())
    pct = lambda a, b: f"{100 * a / b:.0f}%" if b else "—"  # noqa: E731
    out = [f"<h3>本輪成交統計</h3><p>已結算 {n} 場；有選中分支 {s} 場；成交 {f} 場；成交率 F/S = {pct(f, s)}"
           f"（佔全部 F/N = {pct(f, n)}）；勝率 {wr(tw, tl)}；被規則擋下 {blocked} 場。</p>",
           "<table><tbody><tr><th>交易類別</th><th>選中</th><th>成交</th><th>成交率</th><th>勝/負</th><th>勝率</th>"
           "<th>損益</th><th>沒成交原因</th></tr>"]
    tot_net = Decimal(0)
    for name, b in sorted(by.items(), key=lambda kv: -kv[1]["sel"]):
        tot_net += b["net"]
        why = "、".join(f"{zh(k)} ×{v}" for k, v in b["nofill"].most_common()) or "—"
        out.append(f"<tr><td>{H.escape(name)}</td><td>{b['sel']}</td><td>{b['fill']}</td><td>{pct(b['fill'], b['sel'])}</td>"
                   f"<td>{b['w']}/{b['l']}</td><td>{wr(b['w'], b['l'])}</td><td{cls(b['net'])}>{signed(b['net'])}</td>"
                   f"<td>{why}</td></tr>")
    out.append(f"<tr><td><b>合計</b></td><td>{s}</td><td>{f}</td><td>{pct(f, s)}</td><td>{tw}/{tl}</td><td>{wr(tw, tl)}</td>"
               f"<td{cls(tot_net)}>{signed(tot_net)}</td><td></td></tr></tbody></table>")
    out.append("<p>未選中分支原因：" + ("、".join(f"{zh(k)} ×{v}" for k, v in not_sel.most_common()) or "—") + "</p>")
    return "".join(out)


def branch_section(bs):
    if not bs:
        return ""
    if "error" in bs:
        return (f"<h3>全部 T6 子策略成交統計</h3><p class=\"l\">這次讀取失敗：{H.escape(bs['error'])}。"
                "其他區塊不受影響，下次按 /dash 會再試。</p>")
    per = lambda net, n: signed(d(net) / n) if n else "—"  # noqa: E731
    rate = lambda a, b: f"{100 * a / b:.1f}%" if b else "—"  # noqa: E731

    def summary(t, rows):
        return (f"總計（{len({r['branch'] for r in rows})} 個子策略）選中 {t['selected']} / 成交 {t['filled']} / "
                f"成交率 {rate(t['filled'], t['selected'])} / 勝 {t['wins']} 負 {t['losses']}"
                f" / 勝率 {rate(t['wins'], t['wins'] + t['losses'])} / 淨損益 {signed(t['net'], 2)}U / 平均每1U {per(t['net'], t['filled'])}")

    def table(rows):
        out = ["<table><thead><tr><th>子策略</th><th>狀態</th><th>選中</th><th>成交</th><th>成交率</th><th>勝/負</th><th>勝率</th>"
               "<th>損益 (U)</th><th>平均每1U</th></tr></thead><tbody>"]
        for r in rows:
            out.append(f"<tr><td>{H.escape(r['label'])}</td><td>{r['status']}</td><td>{r['selected']}</td><td>{r['filled']}</td>"
                       f"<td>{rate(r['filled'], r['selected'])}</td><td>{r['wins']} / {r['losses']}</td>"
                       f"<td>{rate(r['wins'], r['wins'] + r['losses'])}</td><td>{signed(r['net'], 2)}</td>"
                       f"<td>{per(r['net'], r['filled'])}</td></tr>")
        out.append("</tbody></table>")
        return "".join(out)

    coins = bs.get("by_coin") or {}
    out = ['<section class="t6-branch-fill-stats"><h3>全部 T6 子策略成交統計</h3>',
           f"<p><b>綜合（{'/'.join(coins) or 'BTC'}）</b>全歷史 LIVE loop（{bs['live_loops']} 個）· {summary(bs['total'], bs['rows'])}"
           f" · 資料 {md_hm(bs['read_at_ms'])} TW（最多每 30 分鐘重讀一次）</p>", table(bs["rows"])]
    vs = [f"{v['version']} {v['selected']} / {v['filled']} / {rate(v['filled'], v['selected'])} / {v['wins']}-{v['losses']}"
          f" / {rate(v['wins'], v['wins'] + v['losses'])} / {signed(v['net'], 2)} / {per(v['net'], v['filled'])}"
          for v in sorted(bs["versions"], key=lambda v: -v["selected"])]
    out.append("<p>分版本（選中 / 成交 / 成交率 / 勝負 / 勝率 / 損益 U / 平均每1U）：" + " · ".join(vs) + "</p>")
    if len(coins) > 1:
        for coin, c in coins.items():
            out.append(f"<h3>{H.escape(coin)} 子策略成交統計</h3><p>{H.escape(coin)} LIVE loop {c['live_loops']} 個 · "
                       f"{summary(c['total'], c['rows'])}</p>{table(c['rows'])}")
    out.append("</section>")
    return "".join(out)


COIN_COLORS = {"BTC": "#e8871e", "ETH": "#6d72f0", "BNB": "#1aa39a"}  # same colors as the 三幣 block
CUM_COLOR = "#555"


def coin_overview(totals, today):
    """各幣總覽: one row per coin that ever ran LIVE, plus 綜合."""
    if not totals:
        return ""
    td = (today or {}).get("coins") or {}
    out = ["<h3>各幣總覽（全歷史 LIVE）</h3><table><tbody><tr><th>幣</th><th>loop 數</th><th>成交</th><th>勝/負</th>"
           "<th>勝率</th><th>損益</th><th>平均每1U</th><th>今日</th></tr>"]
    allr = {"coin": "綜合", "loops": 0, "fills": 0, "wins": 0, "losses": 0, "net": Decimal(0)}
    for t in totals:
        for k in ("loops", "fills", "wins", "losses"):
            allr[k] += t[k]
        allr["net"] += d(t["net"])
    rows = totals + ([allr] if len(totals) > 1 else [])
    for t in rows:
        net = d(t["net"])
        tday = (d(td[t["coin"]]["net"]) if t["coin"] in td else None) if t["coin"] != "綜合" else \
            (d((today or {}).get("net") or 0) if today else None)
        bold = ' style="font-weight:bold"' if t["coin"] == "綜合" else ""
        out.append(f"<tr{bold}><td>{t['coin']}</td><td>{t['loops']}</td><td>{t['fills']}</td><td>{t['wins']}/{t['losses']}</td>"
                   f"<td>{wr(t['wins'], t['losses'])}</td><td{cls(net)}>{signed(net)}</td>"
                   f"<td>{signed(net / t['fills']) if t['fills'] else '—'}</td>"
                   f"<td{cls(tday) if tday is not None else ''}>{signed(tday) if tday is not None else '—'}</td></tr>")
    out.append("</tbody></table>")
    return "".join(out)


def daily_section(rows):
    """台灣時間每日總損益: bar chart (daily) + line (running total) and a table, new to old."""
    while rows and not rows[0]["fills"]:
        rows = rows[1:]  # start at the first day with a fill
    if not rows:
        return "<h3>每日總損益（台灣時間）</h3><p>最近 30 天沒有成交。</p>"
    nets = [d(r["net"]) for r in rows]
    cum, run = [], Decimal(0)
    for v in nets:
        run += v
        cum.append(run)
    coins = [c for c in COIN_COLORS if any(c in (r.get("coins") or {}) for r in rows)]
    coin_cum = {}
    for c in coins:
        run, coin_cum[c] = Decimal(0), []
        for r in rows:
            run += d(((r.get("coins") or {}).get(c) or {}).get("net") or 0)
            coin_cum[c].append(run)
    x0, x1, y0, y1 = 34, 440, 12, 132

    def scale(vals):
        lo, hi = min([Decimal(0)] + vals), max([Decimal(0)] + vals)
        hi = hi if hi != lo else lo + 1
        return lambda v: y1 - (y1 - y0) * float((v - lo) / (hi - lo)), lo, hi

    Y, lo, hi = scale(nets)      # bars: daily, left axis
    C, clo, chi = scale(cum + [v for c in coins if len(coins) > 1 for v in coin_cum[c]])  # lines, right axis
    n = len(rows)
    step = (x1 - x0) / n
    bw = max(2.0, step * 0.6)
    svg = ['<svg viewBox="0 0 480 172" width="100%" role="img" aria-label="daily net P&amp;L" fill="currentColor">']
    used = []
    for v in (Decimal(0), lo, hi):
        y = Y(v)
        if any(abs(y - u) < 10 for u in used):
            continue
        used.append(y)
        svg.append(f'<line x1="{x0}" x2="{x1}" y1="{y:.0f}" y2="{y:.0f}" stroke="currentColor" '
                   f'stroke-opacity="{0.4 if v == 0 else 0.15}"/><text x="2" y="{y + 4:.0f}" font-size="10">{v:+.1f}</text>')
    used = []
    for v in (clo, chi):
        y = C(v)
        if any(abs(y - u) < 10 for u in used):
            continue
        used.append(y)
        svg.append(f'<text x="{x1 + 4}" y="{y + 4:.0f}" font-size="10" fill="{CUM_COLOR}">{v:+.1f}</text>')
    last_label = -99
    for i, (r, v) in enumerate(zip(rows, nets)):
        cx = x0 + step * (i + 0.5)
        top, bot = sorted((Y(v), Y(Decimal(0))))
        color = "#1a7f37" if v > 0 else "#c62828"
        if v:
            svg.append(f'<rect x="{cx - bw / 2:.1f}" y="{top:.1f}" width="{bw:.1f}" height="{max(bot - top, 1):.1f}" fill="{color}"/>')
        if cx - last_label >= 40 and (n <= 10 or i % max(1, n // 7) == 0 or i == n - 1):
            last_label = cx
            svg.append(f'<text x="{cx:.0f}" y="{y1 + 14}" font-size="8" text-anchor="middle">{r["day"][5:]}</text>')
    pts = " ".join(f"{x0 + step * (i + 0.5):.0f},{C(c):.0f}" for i, c in enumerate(cum))
    svg.append(f'<polyline points="{pts}" fill="none" stroke="{CUM_COLOR}" stroke-width="2"/>')
    if len(coins) > 1:
        for c in coins:
            cp = " ".join(f"{x0 + step * (i + 0.5):.0f},{C(v):.0f}" for i, v in enumerate(coin_cum[c]))
            svg.append(f'<polyline points="{cp}" fill="none" stroke="{COIN_COLORS[c]}" stroke-width="1.5" stroke-dasharray="4 2"/>')
    svg.append(f'<text x="{(x0 + x1) / 2:.0f}" y="{y1 + 27}" font-size="9" text-anchor="middle">台灣時間（UTC+8），左軸每日 U，右軸累計 U</text></svg>')
    split = len(coins) > 1
    legend = "".join(f'、<span style="color:{COIN_COLORS[c]}">■ 虛線</span>是 {c} 累計' for c in coins) if split else ""
    by = "、".join(f"{c} {signed(coin_cum[c][-1])}U" for c in coins)
    out = ["<h3>每日總損益（台灣時間）</h3>",
           f"<p>最近 {n} 天合計 {signed(cum[-1])}U{'（' + by + '）' if split else ''}。柱狀是每日損益（綠賺紅賠，左軸），"
           f'<span style="color:{CUM_COLOR}">■ 實線</span>是綜合累計{legend}（右軸）。只算 LIVE loop 的已結算成交，含所有幣。</p>',
           "".join(svg),
           "<table><tbody><tr><th>日期</th><th>成交</th><th>勝/負</th><th>勝率</th><th>損益</th><th>累計</th>"
           + "".join(f"<th>{c}</th>" for c in coins if split) + "</tr>"]
    for r, v, c in reversed(list(zip(rows, nets, cum))):
        cells = ""
        if split:
            for k in coins:
                x = (r.get("coins") or {}).get(k)
                cells += (f"<td{cls(x['net'])}>{signed(x['net'])}（{x['fills']}）</td>" if x else "<td>—</td>")
        out.append(f"<tr><td>{r['day'][5:]}</td><td>{r['fills']}</td><td>{r['wins']}/{r['losses']}</td>"
                   f"<td>{wr(r['wins'], r['losses'])}</td><td{cls(v)}>{signed(v)}</td><td{cls(c)}>{signed(c)}</td>{cells}</tr>")
    out.append("</tbody></table>")
    if split:
        out.append("<p>各幣欄位是當天該幣的損益（括號是成交筆數）。</p>")
    today = rows[-1]
    if today.get("loops"):
        parts = "、".join(f"{H.escape(k)} {v} 筆" for k, v in sorted(today["loops"].items()))
        out.append(f"<p>{today['day'][5:]} 的成交來自：{parts}（每個 campaign 只算一次，跟「最近兩輪」同一種算法）。</p>")
    return "".join(out)


def market_table(markets, limit=None):
    rows = sorted(markets, key=lambda m: -m["start_ms"])
    cut = limit is not None and len(rows) > limit
    rows = rows[:limit] if cut else rows
    out = ["<h3>本輪逐場紀錄（新到舊）</h3><table><tbody><tr><th>時間</th><th>交易類別</th><th>方向</th><th>價格</th>"
           "<th>結果</th><th>損益</th><th>未下單原因</th></tr>"]
    for m in rows:
        if m["skip_reason"] == "in_progress":
            res = "進行中"
        elif m["result"] == "W":
            res = '<span class="w">贏</span>'
        elif m["result"] == "L":
            res = '<span class="l">輸</span>'
        else:
            res = "未成交"
        filled = bool(m["fill_price"])
        show_branch = m["selected"] or filled
        pnl = f"<td{cls(m['net_pnl'])}>{signed(m['net_pnl'])}</td>" if m["net_pnl"] is not None and filled else "<td>—</td>"
        out.append(f"<tr><td>{hm(m['start_ms'])}</td><td>{H.escape(m['branch'] or '—') if show_branch else '—'}</td>"
                   f"<td>{H.escape(m['side'] or '—') if show_branch else '—'}</td><td>{price(m['fill_price'])}</td>"
                   f"<td>{res}</td>{pnl}<td>{'—' if filled else zh(m['skip_reason'])}</td></tr>")
    out.append("</tbody></table>")
    if cut:
        out.append(f"<p>只列最近 {limit} 場。</p>")
    return "".join(out)


# --- caching and assembly -------------------------------------------------------------

def cached(path: Path, max_age_s: int, build, now_s):
    try:
        data = json.loads(path.read_text())
        if now_s - data["_at"] < max_age_s:
            return data["value"], False
    except Exception:
        data = None
    try:
        value = build()
    except Exception as exc:  # keep the stale copy rather than failing the page
        if data:
            return data["value"], True
        raise exc
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps({"_at": now_s, "value": value}, ensure_ascii=False, default=str))
    tmp.replace(path)
    return value, False


def build_page(db, cache_dir: Path, now_ms=None, coin_fetch=None):
    now_ms = now_ms or int(time.time() * 1000)
    now_s = now_ms / 1000
    snap = dash_data.snapshot(db, now_ms)
    hist = dash_data.loop_history(db, 8)
    flags = risk_flags(snap.get("config") or {}, now_ms)
    head, loop = header(snap, flags)
    try:
        bs, _ = cached(cache_dir / "branch_stats.json", BRANCH_CACHE_S, lambda: dash_data.branch_stats(db, now_ms), now_s)
    except Exception as exc:  # show the failure instead of silently dropping the section
        bs = {"error": f"{type(exc).__name__}: {exc}"[:200]}
    shade = [(f"{coin_of(r)} {signed(r['filled_pnl'], 2)}", r["created_at_ms"],
              None if r.get("state") == "RUNNING" else r["updated_at_ms"]) for r in reversed(hist)]
    try:
        (coin_html, rev8h), stale = cached(cache_dir / "coins.json", COIN_CACHE_S,
                                           lambda: dash_coins.build(shade, now_ms, coin_fetch), now_s)
    except Exception:
        coin_html, rev8h, stale = "<h3>三幣反轉比較</h3><p>幣安資料暫時取不到。</p>", {}, False
    rev = (f"<p>反轉比例：最近 8 小時 BTC {rev8h.get('BTC', '—')}%、ETH {rev8h.get('ETH', '—')}%、"
           f"BNB {rev8h.get('BNB', '—')}%（幣安）。{'（幣安這次沒讀到，顯示上次資料）' if stale else ''}</p>")
    markets = snap.get("markets") or []
    try:
        daily = dash_data.daily_pnl(db, 30, now_ms)
        daily_html = daily_section(daily)
    except Exception as exc:  # never drop a section silently
        daily, daily_html = [], f'<h3>每日總損益（台灣時間）</h3><p class="l">這次讀取失敗：{H.escape(type(exc).__name__)}。</p>'
    try:
        overview = coin_overview(dash_data.coin_totals(db), daily[-1] if daily else None)
    except Exception as exc:  # never drop a section silently
        overview = f'<h3>各幣總覽（全歷史 LIVE）</h3><p class="l">這次讀取失敗：{H.escape(type(exc).__name__)}。</p>'
    parts = [STYLE, head, rev, overview, history_table(hist), daily_html]
    if loop:
        parts.append(fill_stats(markets))
    parts.append(branch_section(bs))
    mi = None
    if loop:
        mi = len(parts)
        parts.append(market_table(markets))
        if snap.get("decisions_error"):  # say so instead of quietly showing only filled markets
            parts.append(f'<p class="l">{H.escape(coin_of(loop))} 決策紀錄這次讀不到（{H.escape(snap["decisions_error"])}），'
                         "上表只列有下單的場次。</p>")
    parts.append(coin_html)
    body = "".join(parts)
    if mi is not None and len(body.encode()) > 60000:
        parts[mi] = market_table(markets, 40)
        body = "".join(parts)
    page = ('<!doctype html><html lang="zh-Hant"><head><meta charset="utf-8">'
            '<meta name="viewport" content="width=device-width,initial-scale=1"><title>T6 儀表板</title>'
            # phone reading only: wide tables scroll sideways instead of squeezing columns
            '<style>body{font-family:sans-serif;font-size:14px;margin:8px}'
            'table{display:block;max-width:100%;overflow-x:auto}td,th{white-space:nowrap}p,h2,h3{overflow-wrap:anywhere}</style></head>'
            f"<body>{body}</body></html>")
    return page, caption(snap, flags, daily[-1] if daily else None)


def caption(snap, flags, today=None):
    loop = snap.get("loop")
    day = f"今日（台灣時間）損益 {signed(today['net'])}，{today['fills']} 筆成交\n" if today else ""
    if not loop:
        return f"目前沒有 loop 在跑。{day}資料 {hm(snap['read_at_ms'])} TW"
    w, l = loop["wins"], loop["losses"]
    warn = []
    if d(loop["max_drawdown"]) >= Decimal("3.0"):
        warn.append("MDD≥3.0")
    if flags["hs"]:
        warn.append("HS 已鎖")
    if flags["lock"] or flags["halt"]:
        warn.append(f"halt {flags['halt']}")
    if not flags["hb_ok"]:
        warn.append("心跳異常")
    return (f"{coin_of(loop)} {loop['loop_id']} {'進行中' if loop.get('state') == 'RUNNING' else '已結束'}\n"
            f"{loop.get('completed')}/{loop.get('target')} 場，{w} 勝 {l} 負（{wr(w, l)}），"
            f"淨損益 {signed(loop['filled_pnl'])}，MDD {d(loop['max_drawdown']):.3f}\n"
            + day + ("⚠ " + "、".join(warn) + "\n" if warn else "")
            + f"資料 {hm(snap['read_at_ms'])} TW，點開附檔看完整儀表板")


def main(argv):
    out, cache_dir = Path(argv[1]), Path(argv[2])
    db = argv[3] if len(argv) > 3 else dash_data.DEFAULT_DB
    cache_dir.mkdir(parents=True, exist_ok=True)
    t0 = time.time()
    page, cap = build_page(db, cache_dir)
    tmp = out.with_suffix(".tmp")
    tmp.write_text(page, encoding="utf-8")
    tmp.replace(out)
    print(json.dumps({"caption": cap, "elapsed_ms": int((time.time() - t0) * 1000)}, ensure_ascii=False))


if __name__ == "__main__":
    main(sys.argv)

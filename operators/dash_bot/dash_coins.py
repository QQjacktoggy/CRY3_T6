"""三幣反轉比較 block (ported from notes/coin_regime/coin_block.py).

Binance public 1m klines only; it never touches the trading databases. The caller
caches the result, so a /dash press re-fetches at most every CACHE_S seconds.
"""
from __future__ import annotations

import datetime as dt
import json
import math
import time
import urllib.request

N, W, B = 600, 20, 100
COL = {"BTCUSDT": "#e8871e", "ETHUSDT": "#6d72f0", "BNBUSDT": "#1aa39a"}
TW = dt.timezone(dt.timedelta(hours=8))
URL = "https://data-api.binance.vision/api/v3/klines?symbol={sym}&interval=1m&startTime={t}&limit=1000"


def bp(o, c):
    return (c / o - 1) * 1e4


def phi(x):
    return 0.5 * (1 + math.erf(x / math.sqrt(2)))


def ts(ms):
    return dt.datetime.fromtimestamp(ms / 1000, TW).strftime("%m-%d %H:%M")


def klines(sym, start, end, fetch=None):
    fetch = fetch or (lambda url: json.load(urllib.request.urlopen(url, timeout=10)))
    out, t = {}, start - 61 * 60000
    while t < end:
        rows = fetch(URL.format(sym=sym, t=t))
        if not rows:
            break
        for r in rows:
            out[int(r[0])] = (float(r[1]), float(r[4]))
        t = int(rows[-1][0]) + 60000
        time.sleep(0.1)
    return out


def state(f, l):
    a, b = abs(f) >= .5, abs(l) >= .5
    return ("continuation" if f * l > 0 else "reversal") if a and b else ("late_move" if b else "stall" if a else "flat")


def lane(f, l, prior, pup):
    st = state(f, l)
    net = ((1 + f / 1e4) * (1 + l / 1e4) - 1) * 1e4
    pd = "UP" if prior >= 1 else "DOWN" if prior <= -1 else "FLAT"
    c = []
    if st == "reversal":
        s = "UP" if f > 0 else "DOWN"
        if pd == s and (s == "DOWN" or prior >= 5):
            c.append((s, .15, .45))
    if st == "stall" and l > 0 and pd == "UP":
        c.append(("DOWN", .25, .65))
    if st == "reversal" and net >= 1 and prior >= 1:
        c.append(("UP", .65, .75))
    if f * l < 0 and abs(f) >= 1 and abs(f) >= 2 * abs(l) and net != 0:
        c.append(("UP" if net > 0 else "DOWN", .10, .75))
    for s, lo, hi in c:
        a = (pup if s == "UP" else 1 - pup) + .01
        if lo <= a <= hi:
            return s, a
    return None, None


def rows_for(k, start, end):
    rows = []
    for s in range(start, end, 300000):
        if not all(s + i * 60000 in k for i in range(-60, 5)):
            continue
        f, l = bp(*k[s]), bp(*k[s + 60000])
        prior = bp(k[s - 900000][0], k[s - 60000][1])
        sig = math.sqrt(sum(bp(*k[s + i * 60000]) ** 2 for i in range(-60, 0)) / 60) or .1
        pup = phi(bp(k[s][0], k[s + 60000][1]) / (sig * math.sqrt(3)))
        win = "UP" if k[s + 240000][1] >= k[s][0] else "DOWN"
        side, a = lane(f, l, prior, pup)
        rows.append((s, state(f, l) == "reversal", side is not None, side == win if side else False,
                     ((1 / a - 1) if side == win else -1) if side else 0))
    return rows[-N:]


def agg(rows):
    rows = [r[1:] for r in rows]
    n = len(rows) or 1
    t = sum(r[1] for r in rows)
    return 100 * sum(r[0] for r in rows) / n, t, 100 * sum(r[2] for r in rows) / max(t, 1), sum(r[3] for r in rows)


def build(loops, now_ms=None, fetch=None):
    """loops: [(label, start_ms, end_ms|None)] to shade. Returns (html, reversal_8h_pct)."""
    now = now_ms or int(time.time() * 1000)
    end = now - now % 300000 - 300000
    start = end - N * 300000
    res = {sym: rows_for(klines(sym, start, end, fetch), start, end) for sym in COL}
    x0, x1, y0, y1 = 30, 470, 10, 130
    X = lambda ms: x0 + (x1 - x0) * (ms - start) / (end - start)  # noqa: E731
    Y = lambda v: y1 - (y1 - y0) * v / 60  # noqa: E731
    svg = ['<svg viewBox="0 0 480 172" width="100%" role="img" aria-label="reversal share per 20 runs">']
    for lab, a, b in loops:
        xa, xb = max(X(a), x0), min(X(b if b is not None else now), x1)
        if xb > xa:
            svg.append(f'<rect x="{xa:.0f}" y="{y0}" width="{xb - xa:.0f}" height="{y1 - y0}" fill="currentColor" '
                       f'fill-opacity="0.08"/><text x="{(xa + xb) / 2:.0f}" y="{y0 + 10}" font-size="9" '
                       f'fill="currentColor" text-anchor="middle">{lab}</text>')
    for v in (0, 20, 40, 60):
        y = Y(v)
        svg.append(f'<line x1="{x0}" x2="{x1}" y1="{y:.0f}" y2="{y:.0f}" stroke="currentColor" stroke-opacity="0.15"/>'
                   f'<text x="2" y="{y + 4:.0f}" font-size="10" fill="currentColor">{v}%</text>')
    for sym, rows in res.items():
        if not rows:
            continue
        vals = [agg(rows[i:i + W])[0] for i in range(0, len(rows), W)]
        mids = [rows[min(i + W // 2, len(rows) - 1)][0] for i in range(0, len(rows), W)]
        svg.append('<polyline points="' + " ".join(f"{X(m):.0f},{Y(v):.0f}" for m, v in zip(mids, vals))
                   + f'" fill="none" stroke="{COL[sym]}" stroke-width="2"/>')
    tick = (start // 21600000 + 1) * 21600000 - 8 * 3600000 % 21600000
    while tick < end:
        if tick > start:
            svg.append(f'<line x1="{X(tick):.0f}" x2="{X(tick):.0f}" y1="{y1}" y2="{y1 + 4}" stroke="currentColor"/>'
                       f'<text x="{X(tick):.0f}" y="{y1 + 14}" font-size="8" fill="currentColor" '
                       f'text-anchor="middle">{ts(tick)}</text>')
        tick += 21600000
    svg.append(f'<text x="{(x0 + x1) / 2:.0f}" y="{y1 + 27}" font-size="9" fill="currentColor" '
               f'text-anchor="middle">台灣時間 (UTC+8)</text></svg>')
    leg = " ".join(f'<span style="color:{c}">■ {s[:3]}</span>' for s, c in COL.items())
    tbl = ['<table><tr><th>區間（台灣時間）</th>' + "".join(f"<th>{s[:3]} 反轉%／命中%／估算U</th>" for s in COL) + "</tr>"]
    base = res["BTCUSDT"]
    for i in range(0, len(base), B):
        cells = []
        for s in COL:
            r, _, h, p = agg(res[s][i:i + B])
            cells.append(f"<td>{r:.0f}／{h:.0f}／{p:+.1f}</td>")
        seg = base[i:i + B]
        tbl.append(f"<tr><td>{ts(seg[0][0])}–{ts(seg[-1][0] + 300000)[6:]}</td>{''.join(cells)}</tr>")
    tbl.append("</table>")
    last = {s: agg(res[s][-B:]) for s in COL}
    tot = {s: agg(res[s]) for s in COL}
    best = max(COL, key=lambda s: tot[s][3])
    rev = max(COL, key=lambda s: last[s][0])
    note = (f"<p>最近 100 run 反轉最多：{rev[:3]}（{last[rev][0]:.0f}%）。600 run 估算最好：{best[:3]}"
            f"（{tot[best][3]:+.1f}U，命中 {tot[best][2]:.0f}%）。估算U為模型進場價，非真實成交，只看排名。</p>")
    html = (f"<h3>三幣反轉比較（幣安 1 分 K，最近 {N} run，更新 {ts(now)} 台灣時間）</h3><p>每 {W} run 反轉比例 {leg}</p>"
            + "".join(svg) + "".join(tbl) + note)
    rev8h = {s[:3]: round(agg(res[s][-96:])[0]) for s in COL if res[s]}
    return html, rev8h

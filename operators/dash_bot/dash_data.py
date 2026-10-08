"""Read-only data for the /dash dashboard (stdlib only).

Every connection is opened with sqlite ``mode=ro`` plus ``PRAGMA query_only=1``,
so nothing here can write to the trading databases.

* ``snapshot``      – current LIVE loop, per-market rows, risk keys (same reads as
                      dashboard/slim/dash_slim_ro.py v2, loop-scoped through indexes).
* ``loop_history``  – the last N LIVE loops for the 最近兩輪 table.
* ``branch_stats``  – whole-history T6 sub-strategy fill stats (same rules as
                      dashboard/slim/t6_branch_fill_ro.py, keeping only small tuples
                      in memory). It scans whole tables, so the caller caches it.
"""
from __future__ import annotations

import json
import sqlite3
from collections import Counter, defaultdict
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

DEFAULT_DB = "/home/jack_shih/cry3/prediction/data/prediction.sqlite3"
MARKET_MS = 300000
KEYS = ("prediction_heartbeat", "regime_target6_risk_v1", "prediction_risk_state",
        "prediction_hard_stop_latched")


def feature_db(db: str) -> str:
    return str(Path(db).resolve().parent / "regime-target6/features.sqlite3")


def ro(path: str) -> sqlite3.Connection:
    if not Path(path).exists():  # mode=ro never creates a file, but fail clearly
        raise FileNotFoundError(path)
    con = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=2)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA query_only=1")
    return con


def _has_table(con, name):
    return con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)).fetchone() is not None


def _bp2(v):
    return None if v is None else str(Decimal(str(v)).quantize(Decimal("0.01")))


def _px(v):
    return None if v is None else str(Decimal(str(v)).normalize())


def _as_dict(v):
    return v if isinstance(v, dict) else {}


def skip_reason(d):
    if d is None:
        return "no_decision_row"
    if d.get("selected"):
        return None
    g = _as_dict(d.get("core_guard"))
    if not g.get("verified"):
        return g.get("reason") or "core_not_verified"
    rej = d.get("rejected_branches") or []
    if rej and not d.get("eligible_branches"):
        return ",".join(sorted({str(r.get("reason", "?")) if isinstance(r, dict) else str(r) for r in rej}))
    ref = _as_dict(d.get("reference_attempt"))
    if ref.get("status") == "DENIED":
        return "reference:" + str(ref.get("reason"))
    red = _as_dict(d.get("reference_execution_denied"))
    if red:
        return "reference_exec:" + str(red.get("reason"))
    if g.get("empty") and not d.get("eligible_branches"):
        return "no_candidate"
    return "no_executable_price"


def _binding(con, loop_id):
    if not _has_table(con, "prediction_loop_market_bindings"):
        return {}
    r = con.execute("SELECT symbol, profile, execution_fingerprint FROM prediction_loop_market_bindings "
                    "WHERE loop_id=?", (loop_id,)).fetchone()
    return dict(r) if r else {}


def _loop_pnl(con, loop_id):
    """Settled campaigns of one loop (index idx_prediction_settlements_loop)."""
    settles = {r["campaign_id"]: r for r in con.execute(
        "SELECT campaign_id, net_pnl, settled_at_ms, winner FROM prediction_settlements "
        "WHERE loop_id=? AND status='SETTLED' ORDER BY settled_at_ms", (loop_id,))}
    cur = peak = Decimal(0)
    mdd = Decimal(0)
    w = l = 0
    last = None
    for r in settles.values():
        p = Decimal(str(r["net_pnl"] or 0))
        if p == 0:
            continue
        w += p > 0
        l += p < 0
        cur += p
        peak = max(peak, cur)
        mdd = max(mdd, peak - cur)
        last = r["settled_at_ms"]
    return settles, {"wins": int(w), "losses": int(l), "filled_pnl": str(cur), "peak": str(peak),
                     "drawdown": str(cur - peak), "max_drawdown": str(mdd), "last_settled_ms": last}


def snapshot(db: str = DEFAULT_DB, now_ms: int | None = None) -> dict:
    con = ro(db)
    out = {"read_at_ms": now_ms if now_ms is not None else int(datetime.now(timezone.utc).timestamp() * 1000)}
    try:
        loop = con.execute("SELECT * FROM prediction_loops WHERE mode='LIVE' "
                           "ORDER BY created_at_ms DESC LIMIT 1").fetchone()
        if loop:
            loop = dict(loop)
            lid = loop["loop_id"]
            settles, pnl = _loop_pnl(con, lid)
            out["loop"] = {k: loop.get(k) for k in (
                "loop_id", "state", "target", "completed", "mode", "strategy_profile", "terminal_reason",
                "new_entries_stopped", "hard_stop_latched", "created_at_ms", "updated_at_ms")}
            out["loop"].update(pnl)
            out["loop"].update(_binding(con, lid))
            out["markets"] = _markets(con, db, loop, settles, out)
        cfg = {}
        for k in KEYS:
            r = con.execute("SELECT config_value_json FROM prediction_runtime_config WHERE config_key=?",
                            (k,)).fetchone()
            if r:
                try:
                    cfg[k] = json.loads(r[0])
                except Exception:
                    cfg[k] = r[0]
        out["config"] = cfg
    finally:
        con.close()
    return out


def _markets(con, db, loop, settles, out):
    lid = loop["loop_id"]
    camps = {}
    for c in con.execute("SELECT campaign_id, start_time_ms, state, last_error "
                         "FROM prediction_campaigns WHERE loop_id=?", (lid,)):
        fills = con.execute("SELECT outcome, shares, gross_amount FROM prediction_fills "
                            "WHERE campaign_id=? AND upper(order_side)='BUY'", (c["campaign_id"],)).fetchall()
        sh = sum(Decimal(str(f["shares"] or 0)) for f in fills)
        cost = sum(Decimal(str(f["gross_amount"] or 0)) for f in fills)
        s = settles.get(c["campaign_id"])
        camps[int(c["start_time_ms"])] = {
            "fill_side": fills[0]["outcome"] if fills else None,
            "fill_shares": str(sh) if sh else None,
            "fill_price": str((cost / sh).quantize(Decimal("0.0001"))) if sh else None,
            "winner": s["winner"] if s else None,
            "net_pnl": str(s["net_pnl"]) if s else None,
            "error": c["last_error"]}

    decisions = {}
    try:
        fcon = ro(feature_db(db))
        try:
            lo = int(loop["created_at_ms"]) - MARKET_MS
            table = "t69a_decisions" if str(loop.get("strategy_profile")).endswith("9a_v1") else "t69_decisions"
            out["decisions_table"] = table
            if not _has_table(fcon, table):
                raise LookupError(table)
            for r in fcon.execute(f"SELECT start, payload FROM {table} WHERE start>=? ORDER BY start", (lo,)):
                d = json.loads(r["payload"])
                if d.get("loop_id") not in (None, lid):
                    continue
                decisions[int(r["start"])] = d
        finally:
            fcon.close()
    except Exception as exc:  # the feature DB is optional for the dashboard
        out["decisions_error"] = type(exc).__name__

    rows = []
    for start in sorted(set(decisions) | set(camps)):
        d = decisions.get(start)
        c = camps.get(start, {})
        f = _as_dict(_as_dict((d or {}).get("core_guard")).get("features"))
        pnl = c.get("net_pnl")
        res = None
        if pnl is not None:
            p = Decimal(pnl)
            res = "W" if p > 0 else "L" if p < 0 else "0"
        reason = skip_reason(d)
        selected = bool(d and d.get("selected"))
        if pnl is None and start + MARKET_MS > out["read_at_ms"]:
            reason = "in_progress"
        elif reason is None and not c.get("fill_shares"):
            reason = "selected_not_filled"
        rows.append({
            "start_ms": start, "selected": selected,
            "branch": (d or {}).get("branch"), "side": (d or {}).get("side") or c.get("fill_side"),
            "band": [_px(d.get("lower")), _px(d.get("cap"))] if selected else None,
            "first_bp": _bp2(f.get("first_bp")), "last_bp": _bp2(f.get("last_bp")),
            "prior_bp": _bp2(f.get("prior_bp")),
            "fill_price": c.get("fill_price"), "winner": c.get("winner"), "net_pnl": pnl, "result": res,
            "skip_reason": reason,
            "rejected": [r.get("branch") for r in (d or {}).get("rejected_branches") or []
                         if isinstance(r, dict)] or None,
            "error": c.get("error")})
    return rows


def loop_history(db: str = DEFAULT_DB, n: int = 8) -> list[dict]:
    con = ro(db)
    try:
        out = []
        for loop in con.execute("SELECT * FROM prediction_loops WHERE mode='LIVE' "
                                "ORDER BY created_at_ms DESC LIMIT ?", (n,)).fetchall():
            loop = dict(loop)
            _, pnl = _loop_pnl(con, loop["loop_id"])
            row = {k: loop.get(k) for k in ("loop_id", "state", "target", "completed", "strategy_profile",
                                            "terminal_reason", "new_entries_stopped", "created_at_ms",
                                            "updated_at_ms")}
            row.update(pnl)
            row.update(_binding(con, loop["loop_id"]))
            out.append(row)
        return out
    finally:
        con.close()


# --- whole-history sub-strategy stats ---------------------------------------------------

def _utc_ms(text):
    return int(datetime.strptime(text, "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc).timestamp() * 1000)


# Revision cut-overs agreed in thread 全部 T6 子策略成交統計 (2026-10-08).
REVISIONS = {
    "shallow_retracement": _utc_ms("2026-10-07 16:06"),   # f1aed658 installed
    "c_mirror_up_prior": _utc_ms("2026-10-06 06:04"),     # cap .75 -> .70 (41ae586)
    "core_first_up": _utc_ms("2026-10-05 06:37"),         # first-15-min 5bp floor (2ceeb1f)
}
HIDDEN = {"reference_180_mid", "core_continuation_original", "flat_favorite"}
ACTIVE = {"core_first_down", "core_c_down", "core_first_up", "c_mirror_up_prior", "shallow_retracement"}
BRANCH_ORDER = ["core_first_down", "core_c_down", "core_first_up", "c_mirror_up_prior",
                "shallow_retracement", "core_stall_down"]


def branch_stats(db: str = DEFAULT_DB, now_ms: int | None = None) -> dict:
    now_ms = now_ms if now_ms is not None else int(datetime.now(timezone.utc).timestamp() * 1000)
    con = ro(db)
    try:
        loops = {r[0] for r in con.execute("SELECT loop_id FROM prediction_loops WHERE mode='LIVE'")}
        camps = defaultdict(list)
        for c in con.execute("SELECT campaign_id, loop_id, start_time_ms FROM prediction_campaigns"):
            if c[1] in loops:
                camps[(c[1], int(c[2]))].append(c[0])
        filled = set()
        for f in con.execute("SELECT campaign_id, shares FROM prediction_fills WHERE upper(order_side)='BUY'"):
            if Decimal(str(f[1] or 0)) > 0:
                filled.add(f[0])
        settle = {s[0]: Decimal(str(s[1] or 0)) for s in con.execute(
            "SELECT campaign_id, net_pnl FROM prediction_settlements WHERE status='SETTLED'")}
    finally:
        con.close()

    # (loop, start) -> (table, selected, branch); a selected row wins over unselected ones.
    groups = {}
    tables = Counter()
    for path in (db, feature_db(db)):
        try:
            dcon = ro(path)
        except Exception:
            continue
        try:
            names = [r[0] for r in dcon.execute("SELECT name FROM sqlite_master WHERE type='table' "
                                                "AND name LIKE '%decisions' AND name != 'decisions'")]
            for t in names:
                for r in dcon.execute(f"SELECT start, payload FROM {t}"):
                    try:
                        d = json.loads(r[1])
                    except Exception:
                        continue
                    lid = d.get("loop_id")
                    if lid not in loops:
                        continue
                    key = (lid, int(r[0]))
                    item = (t, bool(d.get("selected")), d.get("branch") or "(none)")
                    old = groups.get(key)
                    if old is None or (item[1] and not old[1]) or (item[1] == old[1] and t < old[0]):
                        groups[key] = item
                    tables[t] += 1
        finally:
            dcon.close()

    def zero():
        return {"selected": 0, "filled": 0, "in_progress": 0, "wins": 0, "losses": 0, "net": Decimal(0)}

    acc = defaultdict(zero)
    versions = defaultdict(zero)
    for (lid, start), (tbl, selected, branch) in groups.items():
        if not selected or branch in HIDDEN:
            continue
        label = branch
        if branch in REVISIONS:
            label = f"{branch}（{'改版後' if start >= REVISIONS[branch] else '改版前'}）"
        ids = camps.get((lid, start), [])
        is_filled = any(i in filled for i in ids)
        net = None
        for i in ids:
            if i in settle:
                net = (net or Decimal(0)) + settle[i]
        for a in (acc[(branch, label)], versions[tbl.replace("_decisions", "")]):
            a["selected"] += 1
            if is_filled:
                a["filled"] += 1
            elif not ids and start + MARKET_MS > now_ms:
                a["in_progress"] += 1
            if is_filled and net is not None:
                a["net"] += net
                a["wins"] += net > 0
                a["losses"] += net < 0

    def order(key):
        b, label = key
        return (BRANCH_ORDER.index(b) if b in BRANCH_ORDER else 99, label)

    rows = []
    for key in sorted(acc, key=order):
        a = acc[key]
        rows.append({"branch": key[0], "label": key[1],
                     "status": "使用中" if key[0] in ACTIVE else "未確認", **a, "net": str(a["net"])})
    total = zero()
    for a in acc.values():
        for k in total:
            total[k] += a[k]
    return {"read_at_ms": now_ms, "live_loops": len(loops), "rows": rows,
            "total": {**total, "net": str(total["net"])},
            "versions": [{"version": k, **v, "net": str(v["net"])} for k, v in sorted(versions.items())],
            "decision_tables": dict(tables)}

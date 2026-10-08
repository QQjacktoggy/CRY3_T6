import json
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1] / "operators" / "dash_bot"
sys.path.insert(0, str(ROOT))

import dash_bot  # noqa: E402
import dash_data  # noqa: E402
import dash_page  # noqa: E402

LOOP_START = 1791447000000 - 1791447000000 % 300000  # aligned market start


def ms(text):
    return int(datetime.strptime(text, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc).timestamp() * 1000)


def make_db(tmp_path):
    data = tmp_path / "data"
    (data / "regime-target6").mkdir(parents=True)
    db = data / "prediction.sqlite3"
    con = sqlite3.connect(db)
    con.executescript("""
    CREATE TABLE prediction_loops(loop_id TEXT PRIMARY KEY, target INT, completed INT, state TEXT, created_at_ms INT,
      updated_at_ms INT, net_pnl TEXT, hard_stop_latched INT DEFAULT 0, mode TEXT, terminal_reason TEXT,
      new_entries_stopped INT DEFAULT 0, strategy_profile TEXT);
    CREATE TABLE prediction_loop_market_bindings(loop_id TEXT PRIMARY KEY, symbol TEXT, profile TEXT,
      execution_fingerprint TEXT, unit TEXT, target INT, selected_at_ms INT);
    CREATE TABLE prediction_campaigns(campaign_id TEXT PRIMARY KEY, loop_id TEXT, start_time_ms INT, state TEXT,
      initial_outcome TEXT, last_error TEXT);
    CREATE TABLE prediction_fills(fill_id TEXT PRIMARY KEY, campaign_id TEXT, outcome TEXT, order_side TEXT,
      shares TEXT, price TEXT, gross_amount TEXT, fee TEXT);
    CREATE TABLE prediction_settlements(settlement_id TEXT PRIMARY KEY, campaign_id TEXT, loop_id TEXT,
      settled_at_ms INT, winner TEXT, status TEXT, net_pnl TEXT);
    CREATE TABLE prediction_runtime_config(config_key TEXT PRIMARY KEY, config_value_json TEXT, updated_at_ms INT);
    -- real VM table that ends in 'decisions' but has no start/payload columns
    CREATE TABLE prediction_moe_shadow_decisions(decision_id TEXT PRIMARY KEY, market_id TEXT, observed_at_ms INT);
    INSERT INTO prediction_moe_shadow_decisions VALUES('m1', 'x', 1);
    """)
    old = "loop:1"
    cur = "loop:2"
    con.execute("INSERT INTO prediction_loops VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                (old, 100, 100, "COMPLETED", LOOP_START - 10 * 3600000, LOOP_START - 3600000, "1", 0, "LIVE", "target_reached", 0,
                 "regime_target6_9a_v1"))
    con.execute("INSERT INTO prediction_loops VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                (cur, 100, 3, "RUNNING", LOOP_START, LOOP_START + 900000, "0", 0, "LIVE", None, 0, "regime_target6_9a_v1"))
    con.execute("INSERT INTO prediction_loop_market_bindings VALUES(?,?,?,?,?,?,?)",
                (cur, "BTCUSDT", "regime_target6_9a_v1", "8aeb7317abcdef", "1", 100, LOOP_START))
    rows = [  # start offset, branch selected?, fill price, pnl
        (0, "core_c_down", True, "0.70", "0.42"),
        (1, "core_first_down", True, "0.40", "-0.97"),
        (2, "shallow_retracement", True, None, None),
        (3, None, False, None, None),
    ]
    feat = sqlite3.connect(data / "regime-target6" / "features.sqlite3")
    feat.execute("CREATE TABLE t69a_decisions(start INTEGER PRIMARY KEY, payload TEXT NOT NULL)")
    for i, branch, sel, px, pnl in rows:
        start = LOOP_START + i * 300000
        cid = f"c{i}"
        payload = {"loop_id": cur, "selected": sel, "branch": branch, "side": "DOWN" if sel else None,
                   "lower": "0.1", "cap": "0.75", "core_guard": {"verified": True, "empty": not sel,
                   "features": {"first_bp": 1.234, "last_bp": -2.5, "prior_bp": 3}}}
        if i == 3:
            payload["rejected_branches"] = [{"branch": "core_first_up", "reason": "first_up_prior_below_5bp"}]
        feat.execute("INSERT INTO t69a_decisions VALUES(?,?)", (start, json.dumps(payload)))
        if sel:
            con.execute("INSERT INTO prediction_campaigns VALUES(?,?,?,?,?,?)", (cid, cur, start, "DONE", None, None))
        if px:
            con.execute("INSERT INTO prediction_fills VALUES(?,?,?,?,?,?,?,?)",
                        (f"f{i}", cid, "DOWN", "BUY", "1", px, px, "0"))
            con.execute("INSERT INTO prediction_settlements VALUES(?,?,?,?,?,?,?)",
                        (f"s{i}", cid, cur, start + 300000, "DOWN", "SETTLED", pnl))
    con.execute("INSERT INTO prediction_runtime_config VALUES(?,?,?)",
                ("prediction_heartbeat", json.dumps({"last_loop_at_ms": LOOP_START + 1200000}), 0))
    con.execute("INSERT INTO prediction_runtime_config VALUES(?,?,?)",
                ("regime_target6_risk_v1", json.dumps({"halt_reason": None}), 0))
    con.commit()
    feat.commit()
    return db


def fake_fetch(url):
    from urllib.parse import parse_qs, urlparse
    t = int(parse_qs(urlparse(url).query)["startTime"][0])
    end = LOOP_START + 1200000
    out = []
    for i in range(1000):
        s = t + i * 60000
        if s > end:
            break
        o = 100 + (s // 60000 % 7) * 0.01
        out.append([s, str(o), "0", "0", str(o + (0.02 if s // 60000 % 3 else -0.03))])
    return out


def test_gate_blocks_order_time_and_quiet_hours():
    assert dash_bot.blocked_reason(ms("2026-10-08 10:00:30")) is None
    assert "下單" in dash_bot.blocked_reason(ms("2026-10-08 10:02:05"))
    assert "下單" in dash_bot.blocked_reason(ms("2026-10-08 10:06:35"))
    assert dash_bot.blocked_reason(ms("2026-10-08 10:03:00")) is None
    assert "安靜" in dash_bot.blocked_reason(ms("2026-10-08 18:00:10"))
    assert "安靜" in dash_bot.blocked_reason(ms("2026-10-08 23:29:10"))
    assert dash_bot.blocked_reason(ms("2026-10-08 23:30:10")) is None


def test_reads_are_read_only(tmp_path):
    db = make_db(tmp_path)
    con = dash_data.ro(str(db))
    try:
        con.execute("INSERT INTO prediction_runtime_config VALUES('x','1',0)")
        raise AssertionError("write should fail")
    except sqlite3.OperationalError:
        pass
    finally:
        con.close()


def test_page_has_every_section(tmp_path):
    db = make_db(tmp_path)
    now = LOOP_START + 1200000 + 30000
    page, cap = dash_page.build_page(str(db), tmp_path, now_ms=now, coin_fetch=fake_fetch)
    for h in ("最近兩輪", "每日總損益（台灣時間）", "本輪成交統計", "全部 T6 子策略成交統計", "本輪逐場紀錄（新到舊）", "三幣反轉比較", "<svg"):
        assert h in page, h
    assert "8aeb7317…" in page and "core_c_down</td><td>使用中" in page and "讀取失敗" not in page
    assert "2 勝" not in page and "1 勝 1 負" in page
    assert "core_c_down" in page and "5bp 條件擋下" in page
    assert ".700" in page and "-0.970" in page
    assert "1/1" in cap or "1 勝 1 負" in cap
    # caches are written and reused
    assert (tmp_path / "branch_stats.json").exists() and (tmp_path / "coins.json").exists()
    page2, _ = dash_page.build_page(str(db), tmp_path, now_ms=now + 1000, coin_fetch=lambda u: 1 / 0)
    assert "三幣反轉比較（幣安" in page2


def test_branch_stats_revision_split(tmp_path):
    db = make_db(tmp_path)
    bs = dash_data.branch_stats(str(db), LOOP_START + 1200000)
    labels = {r["label"] for r in bs["rows"]}
    assert "shallow_retracement（改版後）" in labels
    assert bs["total"]["selected"] == 3 and bs["total"]["filled"] == 2


def test_handle_resends_last_page_when_blocked(tmp_path, monkeypatch):
    sent = []
    d = dash_bot.Dash("t", {1}, None, tmp_path)
    monkeypatch.setattr(dash_bot, "send_document", lambda *a: sent.append(a[3]))
    monkeypatch.setattr(dash_bot, "tg", lambda *a, **k: sent.append(a[2]["text"]))
    d.handle(1, now_ms=ms("2026-10-08 10:02:05"))
    assert "還沒有" in sent[-1]
    d.page.write_text("<p>x</p>")
    d.meta.write_text(json.dumps({"at_ms": ms("2026-10-08 10:00:30"), "caption": "cap"}))
    d.handle(1, now_ms=ms("2026-10-08 10:02:05"))
    assert sent[-1].startswith("現在是下單時間") and "10:00" not in sent[-1] and "18:00" in sent[-1]


def test_branch_section_shows_error_instead_of_vanishing(tmp_path, monkeypatch):
    db = make_db(tmp_path)
    monkeypatch.setattr(dash_data, "branch_stats", lambda *a: 1 / 0)
    page, _ = dash_page.build_page(str(db), tmp_path, now_ms=LOOP_START + 1230000, coin_fetch=fake_fetch)
    assert "全部 T6 子策略成交統計" in page and "讀取失敗" in page


def test_daily_pnl_groups_by_taiwan_day(tmp_path):
    db = make_db(tmp_path)
    con = sqlite3.connect(db)
    # 23:59 TW on the previous day vs 00:01 TW today (UTC 15:59 / 16:01)
    day0 = LOOP_START - LOOP_START % 86400000 + 16 * 3600000 - 5 * 86400000  # 00:00 TW, away from loop:2 fills
    con.execute("INSERT INTO prediction_settlements VALUES('sa','ca','loop:1',?,'UP','SETTLED','0.5')", (day0 - 60000,))
    con.execute("INSERT INTO prediction_settlements VALUES('sb','cb','loop:1',?,'UP','SETTLED','-0.2')", (day0 + 60000,))
    con.execute("INSERT INTO prediction_settlements VALUES('sc','cc','loop:x',?,'UP','SETTLED','9')", (day0 + 60000,))  # not LIVE
    con.commit()
    rows = {r["day"]: r for r in dash_data.daily_pnl(str(db), 30, day0 + 2 * 86400000)}
    before = datetime.fromtimestamp((day0 - 60000) / 1000, dash_data.TW).strftime("%Y-%m-%d")
    after = datetime.fromtimestamp((day0 + 60000) / 1000, dash_data.TW).strftime("%Y-%m-%d")
    assert before != after
    assert rows[before]["net"] == "0.5" and rows[before]["wins"] == 1
    assert rows[after]["net"] == "-0.2" and rows[after]["losses"] == 1
    html = dash_page.daily_section(list(rows.values()))
    assert "<svg" in html and "累計" in html

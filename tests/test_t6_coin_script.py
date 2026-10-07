"""scripts/t6_coin.sh against a stub systemctl: order, guards, no DB writes."""
import os
import sqlite3
import subprocess
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / "scripts/t6_coin.sh"
STUB = """#!/bin/sh
shift
echo "$*" >> "$LOG"
case "$1" in
 list-unit-files) echo "$3 enabled" ;;
 is-active) echo active ;;
 show) echo Environment= ;;
esac
"""


def _setup(tmp_path, *, running=None, selected="BTCUSDT"):
    (tmp_path / "bin").mkdir()
    stub = tmp_path / "bin/systemctl"
    stub.write_text(STUB)
    stub.chmod(0o755)
    data = tmp_path / "root/prediction/data"
    data.mkdir(parents=True)
    with sqlite3.connect(data / "prediction.sqlite3") as db:
        db.executescript("""CREATE TABLE prediction_loops(loop_id TEXT,state TEXT,created_at_ms INT);
            CREATE TABLE prediction_loop_market_bindings(loop_id TEXT,symbol TEXT);
            CREATE TABLE prediction_runtime_config(config_key TEXT,config_value_json TEXT,updated_at_ms INT);""")
        db.execute("INSERT INTO prediction_runtime_config VALUES('prediction_selected_market',?,1)",
                   ('{"symbol":"%s"}' % selected,))
        if running:
            db.execute("INSERT INTO prediction_loops VALUES('loop:1','RUNNING',1)")
            db.execute("INSERT INTO prediction_loop_market_bindings VALUES('loop:1',?)", (running,))
    env = dict(os.environ, PATH=f"{tmp_path/'bin'}:{os.environ['PATH']}", LOG=str(tmp_path / "log"),
               CRY3_ROOT=str(tmp_path / "root"), XDG_RUNTIME_DIR=str(tmp_path))
    return env


def _run(env, *args):
    return subprocess.run(["sh", str(SCRIPT), *args], env=env, capture_output=True, text=True)


def _calls(tmp_path):
    log = tmp_path / "log"
    return [line for line in (log.read_text().splitlines() if log.exists() else [])
            if line.startswith(("start", "enable", "disable", "stop"))]


def test_other_coin_stops_before_selected_coin_starts(tmp_path):
    env = _setup(tmp_path, selected="BNBUSDT")
    result = _run(env, "use", "eth")
    assert result.returncode == 0, result.stderr
    calls = _calls(tmp_path)
    stop_bnb = max(i for i, c in enumerate(calls) if c.startswith("disable") and "bnbusdt" in c)
    start_eth = calls.index("enable --now cry3-t67c-ethusdt-feature.service cry3-t67c-ethusdt-signal.service")
    observers = max(i for i, c in enumerate(calls) if "observer" in c)
    assert observers < stop_bnb < start_eth
    assert not any("ethusdt" in c for c in calls if c.startswith("disable"))
    assert "Bot 目前選的是 BNBUSDT" in result.stdout


@pytest.mark.parametrize("asset", ["BTC", "BNB"])
def test_refuses_switch_during_other_coin_loop(tmp_path, asset):
    env = _setup(tmp_path, running="ETHUSDT")
    result = _run(env, "use", asset)
    assert result.returncode == 3
    assert _calls(tmp_path) == []


def test_unreadable_db_refuses_without_changes(tmp_path):
    env = _setup(tmp_path)
    (tmp_path / "root/prediction/data/prediction.sqlite3").unlink()
    result = _run(env, "use", "ETH")
    assert result.returncode == 5 and "讀不到" in result.stderr
    assert _calls(tmp_path) == []


BTC = "cry3-regime-feature.service cry3-c180-favorite-signal.service"


def test_non_btc_coin_stops_btc_producers(tmp_path):
    env = _setup(tmp_path, selected="ETHUSDT")
    result = _run(env, "use", "ETH")
    assert result.returncode == 0, result.stderr
    calls = _calls(tmp_path)
    assert "disable --now cry3-regime-feature.service" in calls
    assert "disable --now cry3-c180-favorite-signal.service" in calls
    assert not any(c.startswith(("start", "enable")) and "regime-feature" in c for c in calls)
    assert calls[-1] == "enable --now cry3-t67c-ethusdt-feature.service cry3-t67c-ethusdt-signal.service"


def test_btc_runs_alone(tmp_path):
    env = _setup(tmp_path, selected="BTCUSDT")
    result = _run(env, "use", "BTC")
    assert result.returncode == 0, result.stderr
    calls = _calls(tmp_path)
    assert calls[-1] == "enable --now " + BTC
    assert not any(c.startswith("disable") and "regime-feature" in c for c in calls)
    for coin in ("ethusdt", "bnbusdt"):
        assert any(c.startswith("disable") and coin in c for c in calls)

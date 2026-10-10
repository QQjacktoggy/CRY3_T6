#!/usr/bin/env python3
"""Separate Telegram bot: /dash sends the T6 dashboard on demand.

Runs on its own bot token and touches nothing in the trading program:
* stdlib only; the page is built in a short-lived child process (dash_page.py)
  that opens the databases read-only and is killed after CHILD_TIMEOUT_S;
* never reads during order time (minute 1:30–2:20 of each 5-minute market) or
  during quiet hours 02:00–07:30 Taiwan time; then it resends the last page;
* answers only the chat ids in DASH_CHAT_IDS.

Environment (from the systemd EnvironmentFile, never committed):
    DASH_BOT_TOKEN   token of the NEW bot from BotFather (not the trading bot's token)
    DASH_CHAT_IDS    comma-separated chat ids allowed to use /dash
    DASH_DB          optional, prediction.sqlite3 path
    DASH_CACHE_DIR   optional, default ~/.cache/cry3-dash
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import time
import urllib.parse
import urllib.request
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
API = "https://api.telegram.org/bot{token}/{method}"
MARKET_MS = 300000
ORDER_WINDOW_MS = (90000, 140000)       # 1:30–2:20 into each market: no reads
QUIET_UTC = ((18, 0), (23, 30))         # 02:00–07:30 Taiwan time
CHILD_TIMEOUT_S = 20
MIN_GAP_S = 60                          # repeat presses inside this gap resend the last page
TW = timezone(timedelta(hours=8))


def blocked_reason(now_ms: int) -> str | None:
    t = datetime.fromtimestamp(now_ms / 1000, timezone.utc)
    m = t.hour * 60 + t.minute
    (qh, qm), (eh, em) = QUIET_UTC
    if qh * 60 + qm <= m < eh * 60 + em:
        return "安靜時段（02:00–07:30）不讀 VM"
    off = now_ms % MARKET_MS
    if ORDER_WINDOW_MS[0] <= off < ORDER_WINDOW_MS[1]:
        return "現在是下單時間（每場 1:30–2:20）不讀 VM"
    return None


def tg(token, method, params=None, timeout=60):
    data = urllib.parse.urlencode(params or {}).encode()
    with urllib.request.urlopen(API.format(token=token, method=method), data=data, timeout=timeout) as r:
        return json.load(r)


def send_document(token, chat_id, path: Path, caption: str):
    boundary = uuid.uuid4().hex
    parts = []
    for k, v in (("chat_id", str(chat_id)), ("caption", caption[:1000])):
        parts.append(f'--{boundary}\r\nContent-Disposition: form-data; name="{k}"\r\n\r\n{v}\r\n'.encode())
    parts.append(f'--{boundary}\r\nContent-Disposition: form-data; name="document"; filename="{path.name}"\r\n'
                 f"Content-Type: text/html\r\n\r\n".encode() + path.read_bytes() + b"\r\n")
    parts.append(f"--{boundary}--\r\n".encode())
    req = urllib.request.Request(API.format(token=token, method="sendDocument"), data=b"".join(parts),
                                 headers={"Content-Type": f"multipart/form-data; boundary={boundary}"})
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.load(r)


class Dash:
    def __init__(self, token, chat_ids, db, cache_dir: Path):
        self.token, self.chat_ids, self.db, self.cache = token, chat_ids, db, cache_dir
        self.page = cache_dir / "dashboard.html"
        self.meta = cache_dir / "last.json"
        cache_dir.mkdir(parents=True, exist_ok=True)

    def last(self):
        try:
            return json.loads(self.meta.read_text())
        except Exception:
            return None

    def render(self):
        cmd = [sys.executable, "-I", str(HERE / "dash_page.py"), str(self.page), str(self.cache)]
        if self.db:
            cmd.append(self.db)
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=CHILD_TIMEOUT_S)
        if p.returncode != 0:
            raise RuntimeError((p.stderr or "").strip().splitlines()[-1:] or ["render failed"])
        info = json.loads(p.stdout.strip().splitlines()[-1])
        info["at_ms"] = int(time.time() * 1000)
        self.meta.write_text(json.dumps(info, ensure_ascii=False))
        return info

    def handle(self, chat_id, now_ms=None):
        now_ms = now_ms or int(time.time() * 1000)
        last = self.last()
        why = blocked_reason(now_ms)
        if why is None and last and now_ms - last["at_ms"] < MIN_GAP_S * 1000:
            why = "剛剛讀過"
        if why is None:
            try:
                last = self.render()
                return self.send(chat_id, last["caption"])
            except Exception as exc:  # report, never crash the loop
                why = f"這次讀取失敗（{type(exc).__name__}）"
        if last and self.page.exists():
            stamp = datetime.fromtimestamp(last["at_ms"] / 1000, TW).strftime("%H:%M")
            return self.send(chat_id, f"{why}，先給你 {stamp} 的資料。\n\n{last['caption']}")
        return tg(self.token, "sendMessage", {"chat_id": chat_id, "text": f"{why}，還沒有可顯示的資料，請稍後再按 /dash。"})

    def send(self, chat_id, caption):
        return send_document(self.token, chat_id, self.page, caption)

    def run(self):
        offset = None
        started = time.time()
        while True:
            try:
                params = {"timeout": 50, "allowed_updates": json.dumps(["message"])}
                if offset is not None:
                    params["offset"] = offset
                for u in tg(self.token, "getUpdates", params, timeout=70).get("result", []):
                    offset = u["update_id"] + 1
                    msg = u.get("message") or {}
                    chat = (msg.get("chat") or {}).get("id")
                    text = (msg.get("text") or "").strip().split("@")[0]
                    if chat not in self.chat_ids or msg.get("date", 0) < started - 60:
                        continue  # strangers and backlog from before a restart are ignored
                    # single-purpose bot: any message from an allowed chat gets the dashboard
                    self.handle(chat)
            except Exception as exc:
                print(f"dash_bot: {type(exc).__name__}: {exc}", file=sys.stderr, flush=True)
                time.sleep(10)


def main():
    token = os.environ["DASH_BOT_TOKEN"]
    chat_ids = {int(x) for x in os.environ["DASH_CHAT_IDS"].split(",") if x.strip()}
    cache = Path(os.environ.get("DASH_CACHE_DIR") or Path.home() / ".cache/cry3-dash")
    Dash(token, chat_ids, os.environ.get("DASH_DB"), cache).run()


if __name__ == "__main__":
    main()

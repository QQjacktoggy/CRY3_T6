"""Read-only post-install check for PR #48.

Prints: <migration 029 applied> <lane-mask table exists> <T6.9b report shows the Lane line>
Expected after install: "1 1 1". Opens the prediction database read-only only.
"""
import sqlite3
import sys
from pathlib import Path

ROOT = Path(sys.argv[1] if len(sys.argv) > 1 else '/home/jack_shih/cry3')
sys.path.insert(0, str(ROOT))
db = sqlite3.connect((ROOT / 'prediction/data/prediction.sqlite3').as_uri() + '?mode=ro', uri=True)
applied = db.execute("SELECT count(*) FROM prediction_migrations WHERE filename='029_loop_lane_mask.sql'").fetchone()[0]
table = db.execute("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='prediction_loop_lane_masks'").fetchone()[0]
from src.gridbot.prediction.live_report import format_live_report
report = format_live_report(ROOT, profile_filter='regime_target6_9a_v1')
print(applied, table, int('本輪 Lane：' in report))

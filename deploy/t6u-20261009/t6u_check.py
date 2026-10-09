"""Read-only post-install check: migration 029 applied and the lane-mask table exists."""
import sqlite3
db = sqlite3.connect('file:/home/jack_shih/cry3/prediction/data/prediction.sqlite3?mode=ro', uri=True)
applied = db.execute("SELECT count(*) FROM prediction_migrations WHERE filename='029_loop_lane_mask.sql'").fetchone()[0]
table = db.execute("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='prediction_loop_lane_masks'").fetchone()[0]
print(applied, table)

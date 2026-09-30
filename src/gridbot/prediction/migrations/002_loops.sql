CREATE TABLE IF NOT EXISTS prediction_loops (
  loop_id TEXT PRIMARY KEY,
  target INTEGER NOT NULL,
  completed INTEGER NOT NULL DEFAULT 0,
  state TEXT NOT NULL DEFAULT 'RUNNING',
  created_at_ms INTEGER NOT NULL,
  updated_at_ms INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS prediction_risk_ledger (
  ledger_id TEXT PRIMARY KEY,
  loop_id TEXT,
  campaign_id TEXT UNIQUE NOT NULL,
  day TEXT NOT NULL,
  net_pnl TEXT NOT NULL,
  consecutive_losses INTEGER NOT NULL DEFAULT 0,
  hard_stop_latched INTEGER NOT NULL DEFAULT 0,
  created_at_ms INTEGER NOT NULL
);

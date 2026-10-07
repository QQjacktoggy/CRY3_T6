import asyncio
import copy
import json
import os
import sqlite3
import tempfile
import time
import unittest
from decimal import Decimal as D
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch, AsyncMock

from src.gridbot.prediction.regime_lane import *
from src.gridbot.prediction.regime_feature_service import connect, collect_once
from src.gridbot.prediction.regime_worker_bridge import RegimeWorkerBridge
from src.gridbot.prediction.regime_live_ledger import RegimeLiveLedger
from src.gridbot.prediction.c180_gate_runtime import LiveSettlement
from src.gridbot.prediction.c180_signal_runtime import C180SignalStore
from src.gridbot.prediction.repository import PredictionRepository, MIGRATIONS_DIR
from src.gridbot.prediction.models import MarketInfo, Campaign
from src.gridbot.prediction.strategy import StrategyConfig
from src.gridbot.prediction.worker import PredictionWorker

SCHEMA = "CREATE TABLE prediction_c180_entry_claims (\n    loop_id TEXT NOT NULL REFERENCES prediction_loops(loop_id),\n    market_start_ms INTEGER NOT NULL,\n    campaign_id TEXT NOT NULL UNIQUE REFERENCES prediction_campaigns(campaign_id),\n    intent_id TEXT NOT NULL UNIQUE REFERENCES prediction_order_intents(intent_id),\n    unit_usdt TEXT NOT NULL,\n    claimed_at_ms INTEGER NOT NULL,\n    PRIMARY KEY (loop_id, market_start_ms),\n    FOREIGN KEY (loop_id, market_start_ms)\n      REFERENCES prediction_c180_slots(loop_id, market_start_ms)\n);\nCREATE TABLE prediction_c180_settlement_observations (\n    settlement_id TEXT PRIMARY KEY REFERENCES prediction_settlements(settlement_id),\n    campaign_id TEXT NOT NULL REFERENCES prediction_campaigns(campaign_id),\n    net_pnl TEXT NOT NULL,\n    known_at_ms INTEGER NOT NULL\n);\nCREATE TABLE prediction_c180_slots (\n    loop_id TEXT NOT NULL REFERENCES prediction_loops(loop_id),\n    market_start_ms INTEGER NOT NULL,\n    run_ordinal INTEGER NOT NULL,\n    market_topic_id TEXT,\n    market_id TEXT,\n    verified_at_ms INTEGER,\n    empty_attested_at_ms INTEGER,\n    PRIMARY KEY (loop_id, market_start_ms),\n    UNIQUE (loop_id, run_ordinal)\n);\nCREATE TABLE prediction_campaigns (\n    campaign_id TEXT PRIMARY KEY,\n    loop_id TEXT,\n    market_topic_id TEXT NOT NULL,\n    market_id TEXT NOT NULL,\n    slug TEXT NOT NULL DEFAULT '',\n    start_time_ms INTEGER NOT NULL,\n    end_time_ms INTEGER NOT NULL,\n    state TEXT NOT NULL,\n    initial_outcome TEXT,\n    hedge_used INTEGER NOT NULL DEFAULT 0,\n    profit_lock_used INTEGER NOT NULL DEFAULT 0,\n    loser_unwind_count INTEGER NOT NULL DEFAULT 0,\n    loser_unwind_shares TEXT NOT NULL DEFAULT '0',\n    buy_count INTEGER NOT NULL DEFAULT 0,\n    order_attempts INTEGER NOT NULL DEFAULT 0,\n    initial_attempts INTEGER NOT NULL DEFAULT 0,\n    hedge_attempts INTEGER NOT NULL DEFAULT 0,\n    pending_intent_id TEXT,\n    pending_unknown INTEGER NOT NULL DEFAULT 0,\n    hedged_at_ms INTEGER,\n    last_error TEXT,\n    payload_json TEXT NOT NULL,\n    created_at_ms INTEGER NOT NULL,\n    updated_at_ms INTEGER NOT NULL\n);\nCREATE TABLE prediction_fills (\n    fill_id TEXT PRIMARY KEY,\n    trade_id TEXT UNIQUE,\n    order_id TEXT NOT NULL,\n    campaign_id TEXT NOT NULL REFERENCES prediction_campaigns(campaign_id) ON DELETE CASCADE,\n    token_id TEXT,\n    outcome TEXT NOT NULL,\n    order_side TEXT NOT NULL,\n    shares TEXT NOT NULL,\n    price TEXT NOT NULL,\n    gross_amount TEXT NOT NULL,\n    fee TEXT NOT NULL DEFAULT '0',\n    event_time_ms INTEGER NOT NULL,\n    payload_json TEXT NOT NULL\n);\nCREATE TABLE prediction_lane_attribution (\n    attribution_id TEXT PRIMARY KEY,\n    strategy_lane TEXT NOT NULL,\n    campaign_id TEXT NOT NULL,\n    market_id TEXT,\n    window_id TEXT,\n    intent_id TEXT,\n    order_id TEXT,\n    signal_ts INTEGER NOT NULL,\n    direction TEXT,\n    reference_price REAL,\n    spot_price REAL,\n    pre_cross_count INTEGER DEFAULT 0,\n    same_side_seconds REAL DEFAULT 0.0,\n    distance_bps REAL DEFAULT 0.0,\n    ask_at_signal REAL,\n    bid_at_signal REAL,\n    order_submit_ts INTEGER,\n    fill_ts INTEGER,\n    fill_price REAL,\n    fill_shares REAL,\n    fill_latency_ms INTEGER,\n    size_usdt REAL,\n    status TEXT NOT NULL,\n    reject_reason TEXT,\n    final_result TEXT,\n    realized_pnl REAL,\n    payload_json TEXT,\n    created_at_ms INTEGER NOT NULL,\n    updated_at_ms INTEGER NOT NULL\n);\nCREATE TABLE prediction_legacy_pending_archive (campaign_id TEXT PRIMARY KEY, settlement_json TEXT NOT NULL, archived_at_ms INTEGER NOT NULL, backup_name TEXT NOT NULL, reason TEXT NOT NULL);\nCREATE TABLE prediction_loops (\n  loop_id TEXT PRIMARY KEY,\n  target INTEGER NOT NULL,\n  completed INTEGER NOT NULL DEFAULT 0,\n  state TEXT NOT NULL DEFAULT 'RUNNING',\n  created_at_ms INTEGER NOT NULL,\n  updated_at_ms INTEGER NOT NULL\n, net_pnl TEXT NOT NULL DEFAULT '0', consecutive_losses INTEGER NOT NULL DEFAULT 0, hard_stop_latched INTEGER NOT NULL DEFAULT 0, mode TEXT NOT NULL DEFAULT 'SHADOW', batch_id TEXT, terminal_reason TEXT, new_entries_stopped INTEGER NOT NULL DEFAULT 0, strategy_profile TEXT NOT NULL DEFAULT '');\nCREATE TABLE prediction_market_state (\n    campaign_id TEXT PRIMARY KEY REFERENCES prediction_campaigns(campaign_id) ON DELETE CASCADE,\n    prior_spot TEXT,\n    last_spot TEXT,\n    last_spot_at_ms INTEGER,\n    prior_leader TEXT,\n    last_leader TEXT,\n    leader_since_ms INTEGER,\n    leader_quotes INTEGER NOT NULL DEFAULT 0,\n    cross_count INTEGER NOT NULL DEFAULT 0,\n    last_quote_at_ms INTEGER,\n    payload_json TEXT NOT NULL DEFAULT '{}',\n    updated_at_ms INTEGER NOT NULL\n);\nCREATE TABLE prediction_migrations (filename TEXT PRIMARY KEY, applied_at_ms INTEGER NOT NULL);\nCREATE TABLE prediction_moe_arm_entries (\n    config_hash TEXT NOT NULL,\n    market_id TEXT NOT NULL,\n    expert TEXT NOT NULL,\n    risk_policy TEXT NOT NULL,\n    decision_id TEXT NOT NULL REFERENCES prediction_moe_shadow_decisions(decision_id),\n    entered_at_ms INTEGER NOT NULL,\n    PRIMARY KEY(config_hash,market_id,expert,risk_policy)\n);\nCREATE TABLE prediction_moe_official_resolutions (\n    resolution_event_id TEXT PRIMARY KEY,\n    market_id TEXT NOT NULL,\n    market_topic_id TEXT NOT NULL,\n    resolution_status TEXT NOT NULL CHECK(resolution_status IN ('UP','DOWN','VOID','DISPUTED','PENDING')),\n    source_digest TEXT NOT NULL,\n    payload_json TEXT NOT NULL,\n    observed_at_ms INTEGER NOT NULL,\n    supersedes_event_id TEXT REFERENCES prediction_moe_official_resolutions(resolution_event_id),\n    UNIQUE(market_id, source_digest)\n);\nCREATE TABLE prediction_moe_positions (\n    config_hash TEXT NOT NULL,\n    market_id TEXT NOT NULL,\n    expert TEXT NOT NULL,\n    risk_policy TEXT NOT NULL,\n    side TEXT,\n    filled_shares TEXT NOT NULL,\n    gross_usdt TEXT NOT NULL,\n    fees_usdt TEXT NOT NULL,\n    status TEXT NOT NULL,\n    resolution_event_id TEXT NOT NULL REFERENCES prediction_moe_official_resolutions(resolution_event_id),\n    payload_json TEXT NOT NULL,\n    PRIMARY KEY(config_hash,market_id,expert,risk_policy,resolution_event_id)\n);\nCREATE TABLE prediction_moe_raw_events (\n    source_type TEXT NOT NULL,\n    source_id TEXT NOT NULL,\n    event_time_ms INTEGER NOT NULL,\n    received_at_ms INTEGER NOT NULL,\n    payload_hash TEXT NOT NULL,\n    payload_json TEXT NOT NULL,\n    release_fingerprint TEXT NOT NULL,\n    created_at_ms INTEGER NOT NULL,\n    PRIMARY KEY(source_type, source_id)\n);\nCREATE TABLE prediction_moe_reconciliations (\n    reconciliation_id TEXT PRIMARY KEY,\n    config_hash TEXT NOT NULL,\n    market_id TEXT NOT NULL,\n    resolution_event_id TEXT NOT NULL REFERENCES prediction_moe_official_resolutions(resolution_event_id),\n    status TEXT NOT NULL CHECK(status IN ('MATCHED','MISMATCH')),\n    details_json TEXT NOT NULL,\n    created_at_ms INTEGER NOT NULL\n);\nCREATE TABLE prediction_moe_release_binding (\n    singleton INTEGER PRIMARY KEY CHECK(singleton=1),\n    config_hash TEXT NOT NULL,\n    release_fingerprint TEXT NOT NULL CHECK(length(release_fingerprint)=64),\n    service_identity TEXT NOT NULL,\n    created_at_ms INTEGER NOT NULL\n);\nCREATE TABLE prediction_moe_router_state (\n    config_hash TEXT PRIMARY KEY,\n    state_json TEXT NOT NULL,\n    state_digest TEXT NOT NULL,\n    last_decision_id TEXT NOT NULL,\n    updated_at_ms INTEGER NOT NULL\n);\nCREATE TABLE prediction_moe_selected_entries (\n    config_hash TEXT NOT NULL,\n    market_id TEXT NOT NULL,\n    decision_id TEXT NOT NULL REFERENCES prediction_moe_shadow_decisions(decision_id),\n    expert TEXT NOT NULL,\n    risk_policy TEXT NOT NULL,\n    entered_at_ms INTEGER NOT NULL,\n    PRIMARY KEY(config_hash,market_id)\n);\nCREATE TABLE prediction_moe_shadow_decisions (\n    decision_id TEXT PRIMARY KEY,\n    market_id TEXT NOT NULL,\n    observed_at_ms INTEGER NOT NULL,\n    mode TEXT NOT NULL CHECK(mode = 'SHADOW'),\n    trade_allowed INTEGER NOT NULL CHECK(trade_allowed = 0),\n    config_hash TEXT NOT NULL,\n    candidate_regime TEXT NOT NULL,\n    active_regime TEXT NOT NULL,\n    selected_expert TEXT NOT NULL,\n    selected_side TEXT,\n    selected_risk_policy TEXT,\n    selected_amount_usdt TEXT NOT NULL,\n    reason TEXT NOT NULL,\n    safety_reasons_json TEXT NOT NULL,\n    arms_json TEXT NOT NULL,\n    payload_json TEXT NOT NULL,\n    created_at_ms INTEGER NOT NULL\n, feature_snapshot_json TEXT NOT NULL DEFAULT '{}', input_digest TEXT NOT NULL DEFAULT '', source_watermark_ms INTEGER NOT NULL DEFAULT 0, state_before_digest TEXT NOT NULL DEFAULT '', state_after_digest TEXT NOT NULL DEFAULT '', regime_scores_json TEXT NOT NULL DEFAULT '{}', expert_scores_json TEXT NOT NULL DEFAULT '{}', router_weights_json TEXT NOT NULL DEFAULT '{}');\nCREATE TABLE prediction_moe_shadow_outcomes (\n    market_id TEXT NOT NULL,\n    config_hash TEXT NOT NULL,\n    expert TEXT NOT NULL,\n    risk_policy TEXT NOT NULL,\n    official_outcome TEXT NOT NULL CHECK(official_outcome IN ('UP','DOWN')),\n    action TEXT NOT NULL,\n    entry_side TEXT,\n    entry_price TEXT,\n    amount_usdt TEXT NOT NULL,\n    simulated_pnl_usdt TEXT NOT NULL,\n    net_return REAL NOT NULL,\n    settled_at_ms INTEGER NOT NULL,\n    payload_json TEXT NOT NULL, official_source_digest TEXT NOT NULL DEFAULT '', official_market_topic_id TEXT NOT NULL DEFAULT '',\n    PRIMARY KEY (market_id, config_hash, expert, risk_policy)\n);\nCREATE TABLE prediction_moe_sim_fills (\n    fill_id TEXT PRIMARY KEY,\n    order_id TEXT NOT NULL REFERENCES prediction_moe_sim_orders(order_id),\n    event_id TEXT NOT NULL,\n    source_event_ms INTEGER NOT NULL,\n    received_at_ms INTEGER NOT NULL,\n    side TEXT NOT NULL CHECK(side IN ('UP','DOWN')),\n    shares TEXT NOT NULL,\n    price TEXT NOT NULL,\n    gross_usdt TEXT NOT NULL,\n    fee_usdt TEXT NOT NULL,\n    fee_version TEXT NOT NULL,\n    slippage_bps TEXT NOT NULL,\n    payload_json TEXT NOT NULL,\n    UNIQUE(order_id,event_id,fill_id)\n);\nCREATE TABLE prediction_moe_sim_order_events (\n    order_event_id TEXT PRIMARY KEY,\n    order_id TEXT NOT NULL REFERENCES prediction_moe_sim_orders(order_id),\n    prior_status TEXT,\n    next_status TEXT NOT NULL,\n    payload_json TEXT NOT NULL,\n    created_at_ms INTEGER NOT NULL\n);\nCREATE TABLE prediction_moe_sim_orders (\n    order_id TEXT PRIMARY KEY,\n    config_hash TEXT NOT NULL,\n    market_id TEXT NOT NULL,\n    expert TEXT NOT NULL,\n    risk_policy TEXT NOT NULL,\n    side TEXT NOT NULL CHECK(side IN ('UP','DOWN')),\n    amount_usdt TEXT NOT NULL,\n    candidate_price TEXT NOT NULL,\n    submitted_at_ms INTEGER NOT NULL,\n    latency_ms INTEGER NOT NULL,\n    ttl_ms INTEGER NOT NULL,\n    max_slippage_bps TEXT NOT NULL,\n    fee_version TEXT,\n    fee_rate TEXT,\n    status TEXT NOT NULL,\n    quarantine_reason TEXT,\n    payload_json TEXT NOT NULL, last_source_event_ms INTEGER NOT NULL DEFAULT 0, max_book_age_ms INTEGER NOT NULL DEFAULT 1000,\n    UNIQUE(config_hash,market_id,expert,risk_policy)\n);\nCREATE TABLE prediction_order_intents (\n    intent_id TEXT PRIMARY KEY,\n    campaign_id TEXT NOT NULL REFERENCES prediction_campaigns(campaign_id) ON DELETE CASCADE,\n    action TEXT NOT NULL,\n    outcome TEXT NOT NULL,\n    order_side TEXT NOT NULL,\n    amount TEXT NOT NULL,\n    limit_price TEXT NOT NULL,\n    created_at_ms INTEGER NOT NULL,\n    ttl_ms INTEGER NOT NULL,\n    attempt INTEGER NOT NULL DEFAULT 1,\n    order_id TEXT,\n    status TEXT NOT NULL DEFAULT 'PENDING',\n    unknown INTEGER NOT NULL DEFAULT 0,\n    payload_json TEXT NOT NULL, cancel_requested INTEGER NOT NULL DEFAULT 0, cancel_requested_at_ms INTEGER, submission_at_ms INTEGER, ttl_deadline_ms INTEGER, cancel_attempt_count INTEGER NOT NULL DEFAULT 0, cancel_in_flight INTEGER NOT NULL DEFAULT 0, cancel_last_attempt_at_ms INTEGER, cancel_accepted_at_ms INTEGER, cancel_last_error TEXT, client_order_id TEXT, tier TEXT,\n    UNIQUE(campaign_id, action, attempt)\n);\nCREATE TABLE prediction_orders (\n    order_id TEXT PRIMARY KEY,\n    intent_id TEXT UNIQUE,\n    campaign_id TEXT NOT NULL REFERENCES prediction_campaigns(campaign_id) ON DELETE CASCADE,\n    token_id TEXT,\n    outcome TEXT,\n    order_side TEXT,\n    status TEXT NOT NULL,\n    requested_amount TEXT,\n    limit_price TEXT,\n    filled_shares TEXT NOT NULL DEFAULT '0',\n    avg_price TEXT,\n    submitted_at_ms INTEGER,\n    updated_at_ms INTEGER NOT NULL,\n    payload_json TEXT NOT NULL\n, cumulative_gross TEXT NOT NULL DEFAULT '0', cumulative_fee TEXT NOT NULL DEFAULT '0');\nCREATE TABLE prediction_position_snapshots (\n    snapshot_id INTEGER PRIMARY KEY AUTOINCREMENT,\n    campaign_id TEXT NOT NULL REFERENCES prediction_campaigns(campaign_id) ON DELETE CASCADE,\n    captured_at_ms INTEGER NOT NULL,\n    up_shares TEXT NOT NULL DEFAULT '0',\n    down_shares TEXT NOT NULL DEFAULT '0',\n    up_cost TEXT NOT NULL DEFAULT '0',\n    down_cost TEXT NOT NULL DEFAULT '0',\n    realized_cash TEXT NOT NULL DEFAULT '0',\n    fees TEXT NOT NULL DEFAULT '0',\n    payload_json TEXT NOT NULL,\n    UNIQUE(campaign_id, captured_at_ms)\n);\nCREATE TABLE prediction_promotion_snapshots (\n    snapshot_id TEXT PRIMARY KEY,\n    strategy_profile TEXT NOT NULL,\n    mode TEXT NOT NULL CHECK(mode = 'SHADOW'),\n    evidence_identity TEXT NOT NULL,\n    strategy_config_hash TEXT NOT NULL,\n    release_fingerprint TEXT NOT NULL,\n    window_start_ms INTEGER NOT NULL,\n    window_end_ms INTEGER NOT NULL,\n    generated_at_ms INTEGER NOT NULL,\n    eligible INTEGER NOT NULL CHECK(eligible IN (0, 1)),\n    reasons_json TEXT NOT NULL,\n    evidence_json TEXT NOT NULL,\n    created_at_ms INTEGER NOT NULL\n);\nCREATE TABLE prediction_quotes (\n    quote_id INTEGER PRIMARY KEY AUTOINCREMENT,\n    campaign_id TEXT NOT NULL REFERENCES prediction_campaigns(campaign_id) ON DELETE CASCADE,\n    observed_at_ms INTEGER NOT NULL,\n    up_bid TEXT,\n    up_ask TEXT,\n    down_bid TEXT,\n    down_ask TEXT,\n    leader TEXT,\n    btc_spot TEXT,\n    reference_price TEXT,\n    feed_ok INTEGER NOT NULL DEFAULT 1,\n    flip_confirmed INTEGER NOT NULL DEFAULT 0,\n    btc_crossed_reference INTEGER NOT NULL DEFAULT 0,\n    stable_final INTEGER NOT NULL DEFAULT 0,\n    payload_json TEXT NOT NULL, leader_duration_ms INTEGER NOT NULL DEFAULT 0, reference_recross INTEGER NOT NULL DEFAULT 0, spot_observed_at_ms INTEGER,\n    UNIQUE(campaign_id, observed_at_ms)\n);\nCREATE TABLE prediction_risk_events (\n    event_id INTEGER PRIMARY KEY AUTOINCREMENT,\n    campaign_id TEXT,\n    event_time_ms INTEGER NOT NULL,\n    event_type TEXT NOT NULL,\n    severity TEXT NOT NULL,\n    message TEXT NOT NULL,\n    payload_json TEXT NOT NULL\n);\nCREATE TABLE prediction_risk_ledger (\n  ledger_id TEXT PRIMARY KEY,\n  loop_id TEXT,\n  campaign_id TEXT UNIQUE NOT NULL,\n  day TEXT NOT NULL,\n  net_pnl TEXT NOT NULL,\n  consecutive_losses INTEGER NOT NULL DEFAULT 0,\n  hard_stop_latched INTEGER NOT NULL DEFAULT 0,\n  created_at_ms INTEGER NOT NULL\n);\nCREATE TABLE prediction_runtime_config (\n    config_key TEXT PRIMARY KEY,\n    config_value_json TEXT NOT NULL,\n    updated_at_ms INTEGER NOT NULL\n);\nCREATE TABLE prediction_schema_artifacts (\n    filename TEXT PRIMARY KEY,\n    sha256 TEXT NOT NULL CHECK(length(sha256) = 64),\n    registered_at_ms INTEGER NOT NULL\n);\nCREATE TABLE prediction_settlements (\n    settlement_id TEXT PRIMARY KEY,\n    campaign_id TEXT NOT NULL REFERENCES prediction_campaigns(campaign_id) ON DELETE CASCADE,\n    loop_id TEXT,\n    settled_at_ms INTEGER NOT NULL,\n    winner TEXT,\n    status TEXT NOT NULL DEFAULT 'SETTLED',\n    gross_pnl TEXT NOT NULL DEFAULT '0',\n    realized_pnl TEXT NOT NULL DEFAULT '0',\n    net_pnl TEXT NOT NULL DEFAULT '0',\n    fees TEXT NOT NULL DEFAULT '0',\n    payload_json TEXT NOT NULL, tx_hash TEXT, batch_id TEXT,\n    UNIQUE(campaign_id, settled_at_ms)\n);\nCREATE TABLE \"prediction_shadow_campaigns\" (\n    shadow_campaign_id TEXT PRIMARY KEY,\n    campaign_id TEXT NOT NULL,\n    mode TEXT NOT NULL DEFAULT 'SHADOW' CHECK(mode = 'SHADOW'),\n    config_hash TEXT NOT NULL,\n    window_start_ms INTEGER NOT NULL,\n    window_end_ms INTEGER NOT NULL,\n    market_topic_id TEXT NOT NULL DEFAULT '',\n    market_id TEXT NOT NULL DEFAULT '',\n    slug TEXT NOT NULL DEFAULT '',\n    campaign_start_ms INTEGER NOT NULL,\n    campaign_end_ms INTEGER NOT NULL,\n    resolved_outcome TEXT NOT NULL CHECK(resolved_outcome IN ('UP','DOWN','DRAW')),\n    resolved_at_ms INTEGER NOT NULL,\n    simulated_fees TEXT NOT NULL DEFAULT '0',\n    simulated_pnl TEXT NOT NULL DEFAULT '0',\n    expected_fill_count INTEGER NOT NULL DEFAULT 0,\n    simulated_fill_count INTEGER NOT NULL DEFAULT 0,\n    created_at_ms INTEGER NOT NULL,\n    payload_json TEXT NOT NULL,\n    lane TEXT NOT NULL DEFAULT '',\n    strategy_identity_json TEXT NOT NULL DEFAULT '',\n    execution_identity_json TEXT NOT NULL DEFAULT '',\n    collection_release_fingerprint TEXT NOT NULL DEFAULT '',\n    UNIQUE(campaign_id, config_hash, window_start_ms, window_end_ms),\n    CHECK(window_start_ms <= window_end_ms),\n    CHECK(campaign_start_ms <= campaign_end_ms)\n);\nCREATE TABLE prediction_shadow_collectors (\n    window_id TEXT PRIMARY KEY REFERENCES prediction_shadow_windows(window_id) ON DELETE RESTRICT,\n    loop_id TEXT NOT NULL UNIQUE REFERENCES prediction_loops(loop_id) ON DELETE RESTRICT,\n    target INTEGER NOT NULL CHECK(target = 200),\n    collection_release_fingerprint TEXT NOT NULL,\n    created_at_ms INTEGER NOT NULL\n);\nCREATE TABLE prediction_shadow_evidence (\n    evidence_id INTEGER PRIMARY KEY AUTOINCREMENT,\n    evidence_identity TEXT NOT NULL UNIQUE,\n    mode TEXT NOT NULL DEFAULT 'SHADOW' CHECK(mode = 'SHADOW'),\n    config_hash TEXT NOT NULL,\n    window_start_ms INTEGER NOT NULL,\n    window_end_ms INTEGER NOT NULL,\n    generated_at_ms INTEGER NOT NULL,\n    repository_commit TEXT NOT NULL,\n    database_path TEXT NOT NULL,\n    unique_resolved_count INTEGER NOT NULL DEFAULT 0,\n    campaign_count INTEGER NOT NULL DEFAULT 0,\n    settled_count INTEGER NOT NULL DEFAULT 0,\n    coverage TEXT NOT NULL DEFAULT '0',\n    settlement_rate TEXT NOT NULL DEFAULT '0',\n    expected_fill_count INTEGER NOT NULL DEFAULT 0,\n    simulated_fill_count INTEGER NOT NULL DEFAULT 0,\n    fill_rate TEXT NOT NULL DEFAULT '0',\n    simulated_fees TEXT NOT NULL DEFAULT '0',\n    after_fee_pnl TEXT NOT NULL DEFAULT '0',\n    unresolved_intents INTEGER NOT NULL DEFAULT 0,\n    unresolved_orders INTEGER NOT NULL DEFAULT 0,\n    duplicate_violations INTEGER NOT NULL DEFAULT 0,\n    overbuy_violations INTEGER NOT NULL DEFAULT 0,\n    action_limit_violations INTEGER NOT NULL DEFAULT 0,\n    invariant_violations INTEGER NOT NULL DEFAULT 0,\n    payload_json TEXT NOT NULL,\n    created_at_ms INTEGER NOT NULL\n, content_hash TEXT NOT NULL DEFAULT '', strategy_profile TEXT NOT NULL DEFAULT '', execution_identity TEXT NOT NULL DEFAULT '', evidence_kind TEXT NOT NULL DEFAULT 'INTERIM', strategy_identity_json TEXT NOT NULL DEFAULT '', collection_release_fingerprint TEXT NOT NULL DEFAULT '');\nCREATE TABLE prediction_shadow_exclusions (\n    exclusion_id TEXT PRIMARY KEY,\n    shadow_campaign_id TEXT NOT NULL UNIQUE REFERENCES prediction_shadow_campaigns(shadow_campaign_id),\n    campaign_id TEXT NOT NULL,\n    mode TEXT NOT NULL DEFAULT 'SHADOW' CHECK(mode = 'SHADOW'),\n    config_hash TEXT NOT NULL,\n    window_start_ms INTEGER NOT NULL,\n    window_end_ms INTEGER NOT NULL,\n    exclusion_type TEXT NOT NULL CHECK(exclusion_type = 'DUAL_WINNER_NO_FILL'),\n    official_outcomes_json TEXT NOT NULL,\n    reason TEXT NOT NULL,\n    simulated_fees TEXT NOT NULL DEFAULT '0',\n    simulated_pnl TEXT NOT NULL DEFAULT '0',\n    excluded_at_ms INTEGER NOT NULL,\n    payload_json TEXT NOT NULL,\n    CHECK(window_start_ms <= window_end_ms)\n);\nCREATE TABLE prediction_shadow_fills (\n    shadow_fill_id TEXT PRIMARY KEY,\n    shadow_campaign_id TEXT NOT NULL REFERENCES prediction_shadow_campaigns(shadow_campaign_id),\n    campaign_id TEXT NOT NULL,\n    mode TEXT NOT NULL DEFAULT 'SHADOW' CHECK(mode = 'SHADOW'),\n    config_hash TEXT NOT NULL,\n    window_start_ms INTEGER NOT NULL,\n    window_end_ms INTEGER NOT NULL,\n    fill_identity TEXT NOT NULL,\n    outcome TEXT NOT NULL CHECK(outcome IN ('UP','DOWN')),\n    order_side TEXT NOT NULL CHECK(order_side IN ('BUY','SELL')),\n    shares TEXT NOT NULL,\n    price TEXT NOT NULL,\n    gross_amount TEXT NOT NULL,\n    simulated_fee TEXT NOT NULL DEFAULT '0',\n    event_time_ms INTEGER NOT NULL,\n    payload_json TEXT NOT NULL, action TEXT NOT NULL DEFAULT '',\n    UNIQUE(shadow_campaign_id, fill_identity),\n    UNIQUE(shadow_campaign_id, event_time_ms, outcome, order_side, shares, price)\n);\nCREATE TABLE \"prediction_shadow_observer_markets\" (\n    observer_campaign_id TEXT PRIMARY KEY,\n    market_topic_id TEXT NOT NULL UNIQUE,\n    market_id TEXT NOT NULL DEFAULT '',\n    slug TEXT NOT NULL DEFAULT '',\n    start_time_ms INTEGER NOT NULL,\n    end_time_ms INTEGER NOT NULL,\n    state TEXT NOT NULL DEFAULT 'ACTIVE',\n    winner TEXT,\n    last_quote_at_ms INTEGER NOT NULL DEFAULT 0,\n    last_seen_at_ms INTEGER NOT NULL DEFAULT 0,\n    last_error TEXT,\n    payload_json TEXT NOT NULL,\n    created_at_ms INTEGER NOT NULL,\n    updated_at_ms INTEGER NOT NULL,\n    CHECK(state IN ('ACTIVE','SETTLED','PENDING_RESOLUTION','EXPIRED')),\n    CHECK(winner IS NULL OR winner IN ('UP','DOWN','DRAW'))\n);\nCREATE TABLE prediction_shadow_observer_quotes (\n    observer_quote_id INTEGER PRIMARY KEY AUTOINCREMENT,\n    observer_campaign_id TEXT NOT NULL\n        REFERENCES prediction_shadow_observer_markets(observer_campaign_id) ON DELETE CASCADE,\n    observed_at_ms INTEGER NOT NULL,\n    up_bid TEXT,\n    up_ask TEXT,\n    down_bid TEXT,\n    down_ask TEXT,\n    leader TEXT,\n    btc_spot TEXT,\n    reference_price TEXT,\n    feed_ok INTEGER NOT NULL DEFAULT 1,\n    flip_confirmed INTEGER NOT NULL DEFAULT 0,\n    btc_crossed_reference INTEGER NOT NULL DEFAULT 0,\n    stable_final INTEGER NOT NULL DEFAULT 0,\n    leader_duration_ms INTEGER NOT NULL DEFAULT 0,\n    reference_recross INTEGER NOT NULL DEFAULT 0,\n    spot_observed_at_ms INTEGER,\n    payload_json TEXT NOT NULL,\n    UNIQUE(observer_campaign_id, observed_at_ms)\n);\nCREATE TABLE prediction_shadow_observer_state (\n    observer_campaign_id TEXT PRIMARY KEY\n        REFERENCES prediction_shadow_observer_markets(observer_campaign_id) ON DELETE CASCADE,\n    prior_spot TEXT,\n    last_spot TEXT,\n    last_spot_at_ms INTEGER,\n    prior_leader TEXT,\n    last_leader TEXT,\n    leader_since_ms INTEGER NOT NULL DEFAULT 0,\n    leader_quotes INTEGER NOT NULL DEFAULT 0,\n    cross_count INTEGER NOT NULL DEFAULT 0,\n    last_quote_at_ms INTEGER NOT NULL DEFAULT 0,\n    payload_json TEXT NOT NULL,\n    updated_at_ms INTEGER NOT NULL\n);\nCREATE TABLE \"prediction_shadow_settlements\" (\n    shadow_settlement_id TEXT PRIMARY KEY,\n    shadow_campaign_id TEXT NOT NULL REFERENCES prediction_shadow_campaigns(shadow_campaign_id),\n    campaign_id TEXT NOT NULL,\n    mode TEXT NOT NULL DEFAULT 'SHADOW' CHECK(mode = 'SHADOW'),\n    config_hash TEXT NOT NULL,\n    window_start_ms INTEGER NOT NULL,\n    window_end_ms INTEGER NOT NULL,\n    resolved_outcome TEXT NOT NULL CHECK(resolved_outcome IN ('UP','DOWN','DRAW')),\n    status TEXT NOT NULL CHECK(status = 'SETTLED'),\n    simulated_fees TEXT NOT NULL DEFAULT '0',\n    simulated_pnl TEXT NOT NULL DEFAULT '0',\n    settled_at_ms INTEGER NOT NULL,\n    payload_json TEXT NOT NULL,\n    UNIQUE(shadow_campaign_id)\n);\nCREATE TABLE prediction_shadow_windows (\n    window_id TEXT PRIMARY KEY,\n    mode TEXT NOT NULL DEFAULT 'SHADOW' CHECK(mode = 'SHADOW'),\n    config_hash TEXT NOT NULL,\n    window_start_ms INTEGER NOT NULL,\n    window_end_ms INTEGER NOT NULL,\n    database_path TEXT NOT NULL,\n    created_at_ms INTEGER NOT NULL, lane TEXT NOT NULL DEFAULT '', strategy_identity_json TEXT NOT NULL DEFAULT '', execution_identity_json TEXT NOT NULL DEFAULT '', collection_release_fingerprint TEXT NOT NULL DEFAULT '',\n    UNIQUE(mode, config_hash, window_start_ms, window_end_ms),\n    CHECK(window_start_ms <= window_end_ms)\n);\nCREATE TRIGGER prediction_moe_arm_entries_no_delete\nBEFORE DELETE ON prediction_moe_arm_entries\nBEGIN SELECT RAISE(ABORT, 'immutable moe arm entry identity'); END;\nCREATE TRIGGER prediction_moe_arm_entries_no_update\nBEFORE UPDATE ON prediction_moe_arm_entries\nBEGIN SELECT RAISE(ABORT, 'immutable moe arm entry identity'); END;\nCREATE TRIGGER prediction_moe_decisions_no_delete\nBEFORE DELETE ON prediction_moe_shadow_decisions BEGIN SELECT RAISE(ABORT, 'immutable moe decision'); END;\nCREATE TRIGGER prediction_moe_decisions_no_update\nBEFORE UPDATE ON prediction_moe_shadow_decisions BEGIN SELECT RAISE(ABORT, 'immutable moe decision'); END;\nCREATE TRIGGER prediction_moe_fills_no_delete\nBEFORE DELETE ON prediction_moe_sim_fills BEGIN SELECT RAISE(ABORT, 'immutable moe fill'); END;\nCREATE TRIGGER prediction_moe_fills_no_update\nBEFORE UPDATE ON prediction_moe_sim_fills BEGIN SELECT RAISE(ABORT, 'immutable moe fill'); END;\nCREATE TRIGGER prediction_moe_orders_no_delete\nBEFORE DELETE ON prediction_moe_sim_orders BEGIN SELECT RAISE(ABORT, 'append-only moe order identity'); END;\nCREATE TRIGGER prediction_moe_positions_no_delete\nBEFORE DELETE ON prediction_moe_positions BEGIN SELECT RAISE(ABORT, 'immutable moe position'); END;\nCREATE TRIGGER prediction_moe_positions_no_update\nBEFORE UPDATE ON prediction_moe_positions BEGIN SELECT RAISE(ABORT, 'immutable moe position'); END;\nCREATE TRIGGER prediction_moe_raw_events_no_delete\nBEFORE DELETE ON prediction_moe_raw_events\nBEGIN\n    SELECT RAISE(ABORT, 'prediction_moe_raw_events is append-only');\nEND;\nCREATE TRIGGER prediction_moe_raw_events_no_update\nBEFORE UPDATE ON prediction_moe_raw_events\nBEGIN\n    SELECT RAISE(ABORT, 'prediction_moe_raw_events is append-only');\nEND;\nCREATE TRIGGER prediction_moe_reconciliations_no_delete\nBEFORE DELETE ON prediction_moe_reconciliations BEGIN SELECT RAISE(ABORT, 'immutable moe reconciliation'); END;\nCREATE TRIGGER prediction_moe_reconciliations_no_update\nBEFORE UPDATE ON prediction_moe_reconciliations BEGIN SELECT RAISE(ABORT, 'immutable moe reconciliation'); END;\nCREATE TRIGGER prediction_moe_release_binding_no_delete\nBEFORE DELETE ON prediction_moe_release_binding BEGIN SELECT RAISE(ABORT,'immutable moe release binding'); END;\nCREATE TRIGGER prediction_moe_release_binding_no_update\nBEFORE UPDATE ON prediction_moe_release_binding BEGIN SELECT RAISE(ABORT,'immutable moe release binding'); END;\nCREATE TRIGGER prediction_moe_resolutions_no_delete\nBEFORE DELETE ON prediction_moe_official_resolutions BEGIN SELECT RAISE(ABORT, 'append-only official resolution'); END;\nCREATE TRIGGER prediction_moe_resolutions_no_update\nBEFORE UPDATE ON prediction_moe_official_resolutions BEGIN SELECT RAISE(ABORT, 'append-only official resolution'); END;\nCREATE TRIGGER prediction_moe_selected_entries_no_delete\nBEFORE DELETE ON prediction_moe_selected_entries\nBEGIN SELECT RAISE(ABORT, 'immutable moe selected entry identity'); END;\nCREATE TRIGGER prediction_moe_selected_entries_no_update\nBEFORE UPDATE ON prediction_moe_selected_entries\nBEGIN SELECT RAISE(ABORT, 'immutable moe selected entry identity'); END;\nCREATE TRIGGER prediction_moe_terminal_orders_no_update\nBEFORE UPDATE ON prediction_moe_sim_orders\nWHEN OLD.status IN ('SETTLED','QUARANTINED')\nBEGIN SELECT RAISE(ABORT, 'immutable terminal moe order'); END;\nCREATE TRIGGER prediction_promotion_snapshots_immutable_delete\nBEFORE DELETE ON prediction_promotion_snapshots\nBEGIN\n    SELECT RAISE(ABORT, 'prediction promotion snapshots are immutable');\nEND;\nCREATE TRIGGER prediction_promotion_snapshots_immutable_update\nBEFORE UPDATE ON prediction_promotion_snapshots\nBEGIN\n    SELECT RAISE(ABORT, 'prediction promotion snapshots are immutable');\nEND;\nCREATE TRIGGER prediction_shadow_campaigns_immutable_delete BEFORE DELETE ON prediction_shadow_campaigns BEGIN SELECT RAISE(ABORT, 'prediction shadow campaigns are immutable'); END;\nCREATE TRIGGER prediction_shadow_campaigns_immutable_update BEFORE UPDATE ON prediction_shadow_campaigns BEGIN SELECT RAISE(ABORT, 'prediction shadow campaigns are immutable'); END;\nCREATE TRIGGER prediction_shadow_evidence_immutable_delete\nBEFORE DELETE ON prediction_shadow_evidence\nBEGIN\n    SELECT RAISE(ABORT, 'prediction shadow evidence is immutable');\nEND;\nCREATE TRIGGER prediction_shadow_evidence_immutable_update\nBEFORE UPDATE ON prediction_shadow_evidence\nBEGIN\n    SELECT RAISE(ABORT, 'prediction shadow evidence is immutable');\nEND;\nCREATE TRIGGER prediction_shadow_exclusions_immutable_delete\nBEFORE DELETE ON prediction_shadow_exclusions\nBEGIN\n    SELECT RAISE(ABORT, 'prediction shadow exclusions are immutable');\nEND;\nCREATE TRIGGER prediction_shadow_exclusions_immutable_update\nBEFORE UPDATE ON prediction_shadow_exclusions\nBEGIN\n    SELECT RAISE(ABORT, 'prediction shadow exclusions are immutable');\nEND;\nCREATE TRIGGER prediction_shadow_fills_immutable_delete BEFORE DELETE ON prediction_shadow_fills BEGIN SELECT RAISE(ABORT, 'prediction shadow fills are immutable'); END;\nCREATE TRIGGER prediction_shadow_fills_immutable_update BEFORE UPDATE ON prediction_shadow_fills BEGIN SELECT RAISE(ABORT, 'prediction shadow fills are immutable'); END;\nCREATE TRIGGER prediction_shadow_settlements_immutable_delete BEFORE DELETE ON prediction_shadow_settlements BEGIN SELECT RAISE(ABORT, 'prediction shadow settlements are immutable'); END;\nCREATE TRIGGER prediction_shadow_settlements_immutable_update BEFORE UPDATE ON prediction_shadow_settlements BEGIN SELECT RAISE(ABORT, 'prediction shadow settlements are immutable'); END;\nCREATE VIEW prediction_shadow_campaign_rollups AS\nSELECT c.shadow_campaign_id, c.campaign_id,\n    CASE WHEN trim(c.lane) <> '' THEN lower(trim(c.lane))\n         WHEN instr(c.campaign_id, '::shadow::') > 0 THEN lower(substr(c.campaign_id, instr(c.campaign_id, '::shadow::') + length('::shadow::')))\n         ELSE '' END AS lane,\n    c.mode, c.config_hash, c.window_start_ms, c.window_end_ms,\n    c.market_topic_id, c.market_id, c.slug, c.campaign_start_ms, c.campaign_end_ms,\n    c.expected_fill_count, COALESCE(f.simulated_fill_count, 0) AS simulated_fill_count,\n    COALESCE(s.resolved_outcome, c.resolved_outcome) AS resolved_outcome,\n    COALESCE(s.settled_at_ms, c.resolved_at_ms) AS resolved_at_ms,\n    COALESCE(s.simulated_fees, c.simulated_fees) AS simulated_fees,\n    COALESCE(s.simulated_pnl, c.simulated_pnl) AS simulated_pnl,\n    c.created_at_ms, c.payload_json\nFROM prediction_shadow_campaigns c\nLEFT JOIN (SELECT shadow_campaign_id, COUNT(*) AS simulated_fill_count FROM prediction_shadow_fills WHERE mode='SHADOW' GROUP BY shadow_campaign_id) f ON f.shadow_campaign_id=c.shadow_campaign_id\nLEFT JOIN prediction_shadow_settlements s ON s.shadow_campaign_id=c.shadow_campaign_id AND s.mode='SHADOW'\nWHERE c.mode='SHADOW';"

HERE = Path(__file__).parent
START = 1790265600000


def feature(first=2, last=-2, prior=2):
    return dict(market_start_ms=START, first_bp=first, last_bp=last, prior_bp=prior,
                received_at_ms=START+120200, cutoff_ms=START+120000, fingerprint=FINGERPRINT)


def book(price="0.35", offset=124100):
    return dict(market_start_ms=START, market_topic="topic", market_id="up",
                book_at_ms=START+offset-100, received_at=START+offset,
                received_at_ms=START+offset, captured_at_ms=START+offset,
                full_depth=True, fee_bps=200,
                quote={s: {"ask_levels": [[price, "100"]]} for s in ("UP", "DOWN")})


def state():
    return dict(fingerprint=FINGERPRINT, first_market_start_ms=START, halt_reason=None)


def trade(n, pnl, known=None):
    return LiveSettlement(str(n), START+(n-1)*SLOT_MS, D(str(pnl)),
                          START+n*SLOT_MS if known is None else known, D(1))


class PolicyTests(unittest.TestCase):
    def test_regimes_and_boundaries(self):
        for a,b,s in [(0.5,0.5,"continuation"),(-0.5,0.5,"reversal"),
                      (0.49,0.5,"late_move"),(0.5,0.49,"stall"),(0,0,"flat")]:
            self.assertEqual(state_of(a,b),s)
        with self.assertRaises(ValueError): state_of("NaN",1)

    def test_reversal_and_stall(self):
        self.assertEqual(select_side(feature(),None)["side"], "UP")
        self.assertTrue(select_side(feature(2,.1,2),None)["allowed"])
        self.assertFalse(select_side(feature(2,-.1,-2),None)["allowed"])
        self.assertFalse(select_side(feature(prior=.99),None)["allowed"])

    def test_fixed_unit_and_old_profile(self):
        for n in [1,2,3]:
            new = PredictionWorker._sized_strategy_config(PROFILE,D(n))
            old = PredictionWorker._sized_strategy_config("c180_favorite_hold_v1",D(n))
            self.assertEqual(new.max_buy_usdt,D(1))
            self.assertEqual(old.max_buy_usdt,D(n))
            self.assertEqual(new.max_scale_in_attempts,0)
        self.assertIn(PROFILE,PredictionWorker._selectable_strategy_profiles())

    def test_fee_net_and_draw(self):
        x=walk([["0.25",100]],200)
        self.assertEqual(x["cash"],1)
        self.assertEqual(x["net_shares"],D("3.92"))
        self.assertEqual(x["net_shares"]*D(".5")-x["cash"],D(".96"))
        self.assertEqual(x["net_shares"]-x["cash"],D("2.92"))

    def test_invalid_depth(self):
        for levels in [[], [[0,1]], [[.25,-1]], [[.25,1]], [[.91,100]], [[.5,100],[.4,100]]]:
            with self.assertRaises(ValueError): walk(levels,200)

    def test_features_contiguous_closed_cutoff(self):
        candles=[[START-900000+i*60000,100,101,99,100,0,START-900000+i*60000+59999] for i in range(17)]
        self.assertEqual(freeze_features(START,candles,START+123000)["first_bp"],"0")
        with self.assertRaises(ValueError): freeze_features(START,candles,START+123001)
        candles[1][0]+=1
        with self.assertRaises(ValueError): freeze_features(START,candles,START+121000)

    def test_persistent_loss_and_mdd(self):
        s=state()
        allowed,reason=risk_result(s,[trade(1,2),trade(2,-1),trade(3,-1),trade(4,-1),trade(5,-.5)],
                                  START+6*SLOT_MS,START+6*SLOT_MS)
        self.assertFalse(allowed);self.assertEqual(reason,"scheduled20_mdd_3.5")
        s=json.loads(json.dumps(s))
        self.assertFalse(risk_result(s,[],START+100*SLOT_MS,START+100*SLOT_MS)[0])

    def test_risk_epoch_skips_older_losses_on_original_grid(self):
        rows=[trade(n,-1) for n in range(1,5)]
        s={**state(),"risk_epoch_start_ms":START+10*SLOT_MS}
        self.assertEqual(risk_result(s,rows,START+10*SLOT_MS,START+10*SLOT_MS),(True,"persistent_risk_pass"))
        self.assertEqual(s["risk_equity_1u"],"0");self.assertEqual(s["net_pnl_usdt"],"-4")
        self.assertEqual(risk_result({**state(),"risk_epoch_start_ms":START+10*SLOT_MS},[],START+9*SLOT_MS,START+9*SLOT_MS)[1],"market_before_risk_epoch")
        self.assertEqual(risk_result({**state(),"risk_epoch_start_ms":START+1},[],START+9*SLOT_MS,START+9*SLOT_MS)[1],"risk_epoch_invalid")
        # Losses after the epoch still latch on the same 20-run grid.
        later=[LiveSettlement(str(n),START+(9+n)*SLOT_MS,D("-1"),START+(10+n)*SLOT_MS,D(1)) for n in range(1,5)]
        s={**state(),"risk_epoch_start_ms":START+10*SLOT_MS}
        self.assertEqual(risk_result(s,later,START+15*SLOT_MS,START+15*SLOT_MS)[1],"scheduled20_mdd_3.5")

    def test_cumulative_across_blocks(self):
        rows=[trade(1,-3),trade(21,-3)]
        self.assertEqual(risk_result(state(),rows,START+22*SLOT_MS,START+22*SLOT_MS)[1],"cumulative_loss_6")

    def test_skips_count_toward_twenty(self):
        rows=[trade(20,-3),trade(21,-.5)]
        self.assertTrue(risk_result(state(),rows,START+22*SLOT_MS,START+22*SLOT_MS)[0])

    def test_unknown_latches_pending_only_waits(self):
        s=state()
        self.assertFalse(risk_result(s,[],START,START+125000,unresolved=True)[0])
        self.assertIsNone(s["halt_reason"])
        self.assertFalse(risk_result(s,[],START,START+125000,unknown=True)[0])
        self.assertFalse(risk_result(s,[],START+SLOT_MS,START+SLOT_MS+125000)[0])

    def test_bad_provenance(self):
        self.assertFalse(risk_result({**state(),"fingerprint":"wrong"},[],START,START)[0])
        for rows in [[trade(1,1),trade(1,1)],[trade(1,1,START+99999999)]]:
            self.assertFalse(risk_result(state(),rows,START+SLOT_MS,START+SLOT_MS)[0])

    @unittest.skipUnless((HERE/"parity.json").is_file(), "Historical research dataset is intentionally excluded from this source-only repo")
    def test_research_policy_parity_864_markets(self):
        admitted=0
        for row in json.loads((HERE/"parity.json").read_text()):
            if row["features"]["first_bp"] is None or row["features"]["last_bp"] is None:
                continue
            decision=select_side(row["features"],row["original"])
            expected=row["expected"]
            if expected["status"]=="FILLED":
                self.assertTrue(decision["allowed"],row["start"])
                self.assertEqual(decision["side"],expected["side"])
                x=walk(row["book"]["quote"][decision["side"]]["ask_levels"],row["book"]["fee_bps"])
                self.assertTrue(dec(decision["lower"])<=x["limit"]<=dec(decision["upper"]))
                if decision["action"]=="original":
                    self.assertGreater(dec(decision["probability"])*x["net_shares"]-x["cash"],D(".005"))
                admitted+=1
            elif expected["reason"] in {"side_scope","prior_trend_filter","state_disabled","original_not_approved"}:
                self.assertFalse(decision["allowed"],row["start"])
        self.assertEqual(admitted,122)


class BridgeTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.path=Path(self.tmp.name)
        self.db=connect(self.path/"features.db")
        with self.db:self.db.execute("INSERT INTO features VALUES(?,?)",(START,json.dumps(feature())))
        self.store=C180SignalStore(self.path/"signals.db")
        self.bridge=RegimeWorkerBridge(None,self.path/"signals.db",feature_db=self.path/"features.db")
        self.market=SimpleNamespace(start_time_ms=START,market_topic_id="topic",up_market_id="up")
    def tearDown(self):
        self.db.close();self.store.close();self.tmp.cleanup()
    def check(self,offset=124200,last=120000):
        return self.bridge.check_signal(market=self.market,unit_usdt=D(1),at_ms=START+offset,
                                       last_seen_book_at_ms=START+last)
    def test_initial_out_of_band_is_permanent_skip(self):
        self.store.persist_book(book(".55"))
        self.assertFalse(self.check().allowed)
        self.store.persist_book(book(".25",124500))
        self.assertIn("frozen_skip",self.check(124600).reason)
    def test_ready_new_quote_same_frozen_signal_and_limit(self):
        self.store.persist_book(book())
        first=self.check();self.assertTrue(first.allowed,first.reason)
        self.assertFalse(self.check(last=124000).allowed)
        self.store.persist_book(book(".25",125000))
        new=self.check(125100,last=124000)
        self.assertTrue(new.allowed,new.reason)
        self.assertEqual(first.signal,new.signal)
        self.assertEqual(new.execution.worst_ask_limit,D(".35"))
    def test_no_first_decision_after_126(self):
        self.store.persist_book(book(offset=126100))
        self.assertFalse(self.check(126200).allowed)
    def test_missing_features_does_not_make_an_order_or_risk_lock(self):
        with self.db:self.db.execute("DELETE FROM features")
        self.store.persist_book(book())
        self.assertIn("features_missing",self.check().reason)
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM decisions").fetchone()[0],0)
    def test_expired_and_future_books(self):
        self.store.persist_book(book(offset=125000))
        self.assertFalse(self.check(124500).allowed)
        self.assertFalse(self.check(136000).allowed)
    def test_fee_change_denies(self):
        self.store.persist_book(book());self.assertTrue(self.check().allowed)
        b=book(offset=125000);b["fee_bps"]=300;self.store.persist_book(b)
        self.assertEqual(self.check(125100).reason,"regime_fee_changed")


class LedgerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.repo=PredictionRepository(Path(self.tmp.name)/"test.db")
        from contextlib import closing
        with closing(sqlite3.connect(self.repo.db_path)) as db:
            db.executescript(SCHEMA)
            db.executemany("INSERT INTO prediction_migrations VALUES(?,?)",
                           [(p.name, 1) for p in MIGRATIONS_DIR.glob("*.sql")])
            db.commit()
        await self.repo.initialize()
        await self.repo._require_conn().executescript((HERE.parent/"src/gridbot/prediction/migrations/025_regime_lane.sql").read_text())
        await self.repo._require_conn().executescript((HERE.parent/"src/gridbot/prediction/migrations/026_loop_market.sql").read_text())
        self.ledger=RegimeLiveLedger(self.repo)
        self.market=MarketInfo("topic","up","test",START,START+SLOT_MS,up_market_id="up",down_market_id="down")
        await self.repo.start_loop("loop1",20,mode="LIVE",strategy_profile=PROFILE)
        await self.repo.save_campaign(Campaign("c1",self.market),loop_id="loop1")
        await self.ledger.seed_schedule(loop_id="loop1",first_market_start_ms=START)
        await self.ledger.verify_market(loop_id="loop1",market_start_ms=START,market_topic_id="topic",market_id="up",verified_at_ms=START+124000)
        self.intent=dict(intent_id="i1",campaign_id="c1",action="BUY_INITIAL",outcome="UP",order_side="BUY",
            amount="1",limit_price=".35",created_at_ms=START+125000,ttl_ms=11000,attempt=1,tier="REGIME_T6")
    async def asyncTearDown(self):
        await self.repo.close();self.tmp.cleanup()
    async def claim(self,**updates):
        with patch("src.gridbot.prediction.regime_live_ledger._now_ms",return_value=START+125000):
            return await self.ledger.reserve_c180_intent(loop_id="loop1",market_start_ms=START,campaign_id="c1",
                intent={**self.intent,**updates},decision_at_ms=START+125000,wallet_reconciled_at_ms=START+125000)
    async def test_atomic_claim_duplicate_and_restart(self):
        result=await self.claim();self.assertTrue(result.claimed,result.reason)
        self.ledger=RegimeLiveLedger(self.repo)
        result=await self.claim(intent_id="i2");self.assertFalse(result.claimed)
        rows=await self.repo._fetchall("SELECT * FROM prediction_order_intents")
        self.assertEqual(len(rows),1)
        self.assertEqual(rows[0]["tier"],"REGIME_T6")
    async def test_claim_rejects_wrong_amount_and_halt(self):
        self.assertFalse((await self.claim(amount="2")).claimed)
        self.assertFalse((await self.claim(tier="C180")).claimed)
        await self.repo.set_runtime_config(STATE_KEY,{**state(),"halt_reason":"manual_test_halt"})
        self.assertFalse((await self.claim()).claimed)
        self.assertEqual(await self.repo._fetchall("SELECT * FROM prediction_order_intents"),[])
    async def test_new_loop_preserves_anchor_and_halt(self):
        await self.repo.set_runtime_config(STATE_KEY,{**state(),"halt_reason":"cumulative_loss_6"})
        await self.repo.start_loop("loop2",20,mode="LIVE",strategy_profile=PROFILE)
        self.assertFalse((await self.ledger.check_risk("loop2",START+100*SLOT_MS,START+100*SLOT_MS))[0])
        s=await self.repo.get_runtime_config(STATE_KEY,None)
        self.assertEqual(s["first_market_start_ms"],START)
        self.assertEqual(s["halt_reason"],"cumulative_loss_6")
    async def test_operator_reset_clears_only_scheduled20_mdd_with_audit(self):
        await self.repo.set_runtime_config(STATE_KEY,{**state(),"halt_reason":"cumulative_loss_6"})
        r=await self.ledger.reset_shared_risk(now_ms=START+50*SLOT_MS+7,reason="t")
        self.assertEqual(r["reason"],"not_resettable")
        await self.repo.set_runtime_config(STATE_KEY,{**state(),"halt_reason":"scheduled20_mdd_3.5"})
        r=await self.ledger.reset_shared_risk(now_ms=START+50*SLOT_MS+7,reason="t")
        self.assertTrue(r["reset"],r);self.assertEqual(r["risk_epoch_start_ms"],START+51*SLOT_MS)
        s=await self.repo.get_runtime_config(STATE_KEY,None)
        self.assertIsNone(s["halt_reason"]);self.assertEqual(s["first_market_start_ms"],START)
        self.assertEqual(s["risk_resets"][0]["prior_halt_reason"],"scheduled20_mdd_3.5")
        self.assertEqual((await self.ledger.check_risk("loop1",START+50*SLOT_MS,START+50*SLOT_MS+8))[1],"market_before_risk_epoch")
        self.assertTrue((await self.ledger.check_risk("loop1",START+51*SLOT_MS,START+51*SLOT_MS))[0])
        self.assertEqual((await self.ledger.reset_shared_risk(now_ms=START+52*SLOT_MS,reason="t"))["reason"],"not_resettable")
    async def test_operator_reset_refuses_unknown_order(self):
        self.assertTrue((await self.claim()).claimed)
        await self.repo._execute("UPDATE prediction_order_intents SET unknown=1 WHERE intent_id='i1'")
        await self.repo.set_runtime_config(STATE_KEY,{**state(),"halt_reason":"scheduled20_mdd_3.5"})
        r=await self.ledger.reset_shared_risk(now_ms=START+50*SLOT_MS,reason="t")
        self.assertEqual(r["reason"],"unknown_order_reconciliation_required")
        self.assertEqual((await self.repo.get_runtime_config(STATE_KEY,None))["halt_reason"],"scheduled20_mdd_3.5")
    async def test_unknown_order_latch_survives_reconciliation(self):
        self.assertTrue((await self.claim()).claimed)
        await self.repo._execute("UPDATE prediction_order_intents SET unknown=1 WHERE intent_id='i1'")
        await self.ledger.check_risk("loop1",START+SLOT_MS,START+SLOT_MS)
        await self.repo._execute("UPDATE prediction_order_intents SET unknown=0 WHERE intent_id='i1'")
        s=await self.repo.get_runtime_config(STATE_KEY,None)
        self.assertEqual(s["halt_reason"],"unknown_order_reconciliation_required")
    async def test_old_lane_unchanged_and_separate(self):
        await self.repo.set_runtime_config("c180_batch_gate_runtime_v1",{"sentinel":"old"})
        self.assertTrue((await self.claim()).claimed)
        self.assertEqual(await self.repo.get_runtime_config("c180_batch_gate_runtime_v1",None),{"sentinel":"old"})
        self.assertEqual(await self.repo._fetchall("SELECT * FROM prediction_c180_entry_claims"),[])

    async def test_four_official_losses_latch_at_settlement_and_are_idempotent(self):
        for n in range(4):
            start=START+n*SLOT_MS
            cid="c1" if n==0 else "c"+str(n+1)
            market=MarketInfo("topic"+str(n),"up"+str(n),"test",start,start+SLOT_MS,
                              up_market_id="up"+str(n),down_market_id="down"+str(n))
            if n:
                await self.repo.save_campaign(Campaign(cid,market),loop_id="loop1")
                await self.ledger.verify_market(loop_id="loop1",market_start_ms=start,
                    market_topic_id=market.market_topic_id,market_id=market.up_market_id,verified_at_ms=start+124000)
            now=start+125000
            intent={**self.intent,"intent_id":"i"+str(n+1),"campaign_id":cid,"created_at_ms":now}
            with patch("src.gridbot.prediction.regime_live_ledger._now_ms",return_value=now):
                claimed=await self.ledger.reserve_c180_intent(loop_id="loop1",market_start_ms=start,
                    campaign_id=cid,intent=intent,decision_at_ms=now,wallet_reconciled_at_ms=now)
            self.assertTrue(claimed.claimed,claimed.reason)
            await self.repo._execute("UPDATE prediction_order_intents SET status='FILLED' WHERE intent_id=?",(intent["intent_id"],))
            await self.repo._execute("UPDATE prediction_campaigns SET pending_intent_id=NULL WHERE campaign_id=?",(cid,))
            await self.repo._execute("INSERT INTO prediction_fills(fill_id,order_id,campaign_id,outcome,order_side,shares,price,gross_amount,fee,event_time_ms,payload_json) VALUES(?,?,?,'UP','BUY','2.8','.35','1','.02',?,'{}')",
                                     ("f"+str(n),"o"+str(n),cid,now))
            await self.repo._execute("INSERT INTO prediction_settlements(settlement_id,campaign_id,loop_id,settled_at_ms,status,net_pnl,payload_json) VALUES(?,?,'loop1',?,'SETTLED','-1','{}')",
                                     ("s"+str(n),cid,start+SLOT_MS))
            with patch("src.gridbot.prediction.regime_live_ledger._now_ms",return_value=start+SLOT_MS):
                for _ in range(2):
                    await self.ledger.observe_settlement(loop_id="loop1",settlement_id="s"+str(n))
        s=await self.repo.get_runtime_config(STATE_KEY,None)
        self.assertEqual(s["net_pnl_usdt"],"-4")
        self.assertEqual(s["halt_reason"],"scheduled20_mdd_3.5")
        self.assertEqual(len(await self.repo._fetchall("SELECT * FROM prediction_regime_settlement_observations")),4)


if __name__ == "__main__":
    unittest.main(verbosity=2)

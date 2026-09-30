"""Report and notify the frozen Shadow 200-run experiment.

The reporter is intentionally independent from the prediction worker and repository.
It reads the immutable Shadow ledgers with the stdlib ``sqlite3`` module in read-only
mode, so it can be run on a copy of the database or from a cron job without changing
trading state.

Required production environment variables (CLI flags may override the non-secret
scope variables):

* ``PREDICTION_DB_PATH``
* ``SHADOW_FROZEN_AFTER_MS``
* ``SHADOW_LANE_SPECS_JSON`` - optional fallback scope in the form
  ``{"lane": {"config_hash": "...", "window_start_ms": 1,
  "window_end_ms": 2}}``; the database's ``prediction_shadow_lanes`` manifest
  takes priority when present.
* ``TELEGRAM_BOT_TOKEN`` and ``TELEGRAM_CHAT_ID``

``--dry-run`` deliberately skips Telegram credentials and checkpoint writes; it is
the offline inspection mode used by tests and operators before enabling delivery.
"""

from __future__ import annotations

import argparse
import contextlib
from datetime import datetime, timezone
import hashlib
import html
import json
import os
import re
import sqlite3
import sys
import tempfile
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Callable, Iterator, Mapping, Sequence
from zoneinfo import ZoneInfo


MAX_TELEGRAM_CHARS = 4096
DEFAULT_TARGET_RUNS = 200
DEFAULT_BATCH_SIZE = 20
DEFAULT_BOT_TOKEN_ENV = "TELEGRAM_BOT_TOKEN"
DEFAULT_CHAT_ID_ENV = "TELEGRAM_CHAT_ID"
PRIMARY_GATE_LANE = "quality_hold_v3_gate_v1"
BASE_V3_LANE = "quality_hold_v3_profit1"
HISTORY30_CONTROL_V1_LANE = "quality_hold_v3_history30_control_v1"
NET_EDGE_V1_LANE = "quality_hold_v3_net_edge_v1"
MOMENTUM30_V1_LANE = "quality_hold_v3_momentum30_v1"
CONFIRM30_V1_LANE = "quality_hold_v3_confirm30_v1"
VALUE9_LANES = (
    "value9_trend_control_1s",
    "value9_reversion_control_1s",
    "value9_reversion_control_3s",
    "value9_reversion_3s_p58",
    "value9_trend_1s_p68",
    "value9_hybrid_58_68",
    "value9_hybrid_60_70",
    "value9_hybrid_62_72",
    "value9_hybrid_asym_58_58_68",
)
DISPLAY_NAMES = {
    "quality_hold_v3_sniper": "V3 Sniper (優化)",
    BASE_V3_LANE: "Base V3 (基準)",
    HISTORY30_CONTROL_V1_LANE: "V3 History30 Control（資料對照）",
    MOMENTUM30_V1_LANE: "V3 Momentum30 V1（主測）",
    CONFIRM30_V1_LANE: "V3 Confirm30 V1（次要探索）",
    NET_EDGE_V1_LANE: "V3 Net Edge V1",
    PRIMARY_GATE_LANE: "V3 Gate V1 (主測)",
    "quality_hold_v3_loss_guard_v2": "V3 Loss Guard V2 (防虧)",
    "quality_hold_v4_rescue": "V4 Rescue (救援)",
    "quality_hold_v2": "V2 (經典)",
    "quality_hold_v5_pnl": "V5 PnL (動態)",
    "quality_hold_v6_a_staged": "V6 Staged (階梯)",
    "quality_hold_v6_b_45s": "V6B 45s (0.75)",
    "quality_hold_v6_c_45s": "V6C 45s (0.78)",
    "regime_value_v7": "V7 (特徵)",
    "late_maturity_v1": "Late Maturity V1 (成熟窗口)",
    "regime_value_v8_calibrated": "V8 Calibrated (保守價值)",
    "complete_set_arb_v1": "Complete Set V1 (配對套利)",
    "value9_trend_control_1s": "T-CTL 1s",
    "value9_reversion_control_1s": "R-CTL 1s",
    "value9_reversion_control_3s": "R-CTL 3s",
    "value9_reversion_3s_p58": "R3 ≥.58",
    "value9_trend_1s_p68": "T1 ≥.68",
    "value9_hybrid_58_68": "H 58/68",
    "value9_hybrid_60_70": "H 60/70",
    "value9_hybrid_62_72": "H 62/72",
    "value9_hybrid_asym_58_58_68": "H Asym 58/58/68",
}
DISPLAY_ORDER = (
    *VALUE9_LANES,
    "quality_hold_v3_sniper",
    BASE_V3_LANE,
    HISTORY30_CONTROL_V1_LANE,
    MOMENTUM30_V1_LANE,
    CONFIRM30_V1_LANE,
    NET_EDGE_V1_LANE,
    PRIMARY_GATE_LANE,
    "quality_hold_v3_loss_guard_v2",
    "quality_hold_v4_rescue",
    "quality_hold_v2",
    "quality_hold_v5_pnl",
    "quality_hold_v6_a_staged",
    "quality_hold_v6_b_45s",
    "quality_hold_v6_c_45s",
    "regime_value_v7",
    "late_maturity_v1",
    "regime_value_v8_calibrated",
    "complete_set_arb_v1",
)
TAIPEI_TZ = ZoneInfo("Asia/Taipei")


class ReporterError(RuntimeError):
    """Base class for configuration, schema, data, and delivery failures."""


class ConfigurationError(ReporterError):
    pass


class SchemaError(ReporterError):
    pass


class CheckpointError(ReporterError):
    pass


class NotificationError(ReporterError):
    pass


@dataclass(frozen=True)
class LaneSpec:
    name: str
    config_hash: str
    window_start_ms: int
    window_end_ms: int

    def as_dict(self) -> dict[str, Any]:
        return {
            "config_hash": self.config_hash,
            "window_start_ms": self.window_start_ms,
            "window_end_ms": self.window_end_ms,
        }


@dataclass(frozen=True)
class RunRecord:
    lane: str
    campaign_id: str
    shadow_campaign_id: str
    shadow_settlement_id: str
    market_key: str
    settled_at_ms: int
    resolved_outcome: str
    simulated_pnl: Decimal
    simulated_fees: Decimal
    fill_count: int

    @property
    def has_fill(self) -> bool:
        return self.fill_count > 0


@dataclass(frozen=True)
class LaneMetrics:
    lane: str
    settled_runs: int
    filled_runs: int
    fill_events: int
    wins: int
    losses: int
    draws: int
    breakevens: int
    pnl: Decimal
    fees: Decimal
    runs: tuple[RunRecord, ...] = field(repr=False)

    @property
    def fill_rate(self) -> Decimal:
        if not self.settled_runs:
            return Decimal("0")
        return Decimal(self.filled_runs) / Decimal(self.settled_runs)

    @property
    def win_rate(self) -> Decimal:
        directional_fills = self.wins + self.losses + self.breakevens
        if not directional_fills:
            return Decimal("0")
        return Decimal(self.wins) / Decimal(directional_fills)

    @property
    def directional_filled_runs(self) -> int:
        return self.wins + self.losses + self.breakevens

    @property
    def max_drawdown(self) -> Decimal:
        """Peak-to-trough drawdown over every settled market, including zeroes."""

        equity = Decimal("0")
        peak = Decimal("0")
        drawdown = Decimal("0")
        for record in self.runs:
            equity += record.simulated_pnl
            peak = max(peak, equity)
            drawdown = max(drawdown, peak - equity)
        return drawdown


@dataclass(frozen=True)
class ShadowReport:
    db_path: Path
    baseline_ms: int
    target_runs: int
    lane_specs: tuple[LaneSpec, ...]
    lanes: Mapping[str, LaneMetrics]
    issues: tuple[str, ...]
    gate_rejection_by_campaign: Mapping[str, str] = field(default_factory=dict)
    execution_censored_market_ids: tuple[str, ...] = ()

    @property
    def common_run_count(self) -> int:
        """Return the common number of settled runs across all lanes.

        A batch is publishable only after every configured lane has the same
        denominator boundary.  This keeps an A/B table from comparing a lane's
        20 runs with another lane's 19 or 40 runs.
        """

        if not self.lanes:
            return 0
        return min(metrics.settled_runs for metrics in self.lanes.values())

    def complete_batches(self, batch_size: int = DEFAULT_BATCH_SIZE) -> int:
        """Return the number of complete common batches across all lanes."""

        if batch_size <= 0:
            raise ValueError("batch_size must be positive")
        return self.common_run_count // batch_size


@dataclass(frozen=True)
class MonitorResult:
    report: ShadowReport
    sent_batches: tuple[int, ...]
    rendered_messages: tuple[str, ...]
    checkpoint_path: Path | None


def _as_int(value: Any, label: str) -> int:
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise ConfigurationError(f"{label} must be an integer") from exc


def _normalize_lane(value: Any) -> str:
    lane = str(value or "").strip().lower()
    if not lane:
        return ""
    if any(char.isspace() for char in lane):
        raise ConfigurationError(f"lane name contains whitespace: {lane!r}")
    return lane


def load_lane_specs(value: str | Mapping[str, Any]) -> tuple[LaneSpec, ...]:
    """Parse the operator-supplied immutable lane scope.

    The reporter never discovers a config hash/window from the database and then
    treats it as the baseline.  Every report therefore has an explicit scope.
    """

    if isinstance(value, str):
        try:
            raw = json.loads(value)
        except json.JSONDecodeError as exc:
            raise ConfigurationError("SHADOW_LANE_SPECS_JSON is not valid JSON") from exc
    else:
        raw = value
    if not isinstance(raw, Mapping) or not raw:
        raise ConfigurationError("lane specs must be a non-empty JSON object")

    specs: list[LaneSpec] = []
    seen: set[str] = set()
    for raw_lane, raw_spec in raw.items():
        lane = _normalize_lane(raw_lane)
        if not lane:
            raise ConfigurationError("lane spec has an empty lane name")
        if lane in seen:
            raise ConfigurationError(f"duplicate lane spec: {lane}")
        if not isinstance(raw_spec, Mapping):
            raise ConfigurationError(f"lane spec must be an object: {lane}")
        window = raw_spec.get("window")
        if not isinstance(window, Mapping):
            window = raw_spec
        config_hash = str(
            raw_spec.get("config_hash")
            or raw_spec.get("configuration_hash")
            or ""
        ).strip()
        if not config_hash:
            raise ConfigurationError(f"config_hash is missing for lane: {lane}")
        start = _as_int(
            window.get("window_start_ms", window.get("start_ms")),
            f"window_start_ms for lane {lane}",
        )
        end = _as_int(
            window.get("window_end_ms", window.get("end_ms")),
            f"window_end_ms for lane {lane}",
        )
        if start < 0 or end < start:
            raise ConfigurationError(f"invalid window for lane {lane}")
        specs.append(LaneSpec(lane, config_hash, start, end))
        seen.add(lane)
    return tuple(specs)


def _json_object(value: Any) -> Mapping[str, Any]:
    if not value:
        return {}
    try:
        decoded = json.loads(str(value))
    except (TypeError, ValueError):
        return {}
    return decoded if isinstance(decoded, Mapping) else {}


def _row_value(row: Mapping[str, Any], key: str, default: Any = None) -> Any:
    value = row.get(key, default)
    return default if value is None else value


def _decimal(value: Any, label: str) -> Decimal:
    try:
        return Decimal(str(value if value is not None else "0"))
    except (InvalidOperation, ValueError) as exc:
        raise SchemaError(f"invalid decimal in {label}") from exc


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    rows = conn.execute(f'PRAGMA table_info("{table}")').fetchall()
    return {str(row[1]) for row in rows}


def _check_schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    required = {
        "prediction_shadow_campaigns": {
            "shadow_campaign_id",
            "campaign_id",
            "mode",
            "config_hash",
            "window_start_ms",
            "window_end_ms",
            "simulated_pnl",
            "simulated_fees",
            "campaign_start_ms",
            "campaign_end_ms",
            "resolved_at_ms",
            "created_at_ms",
        },
        "prediction_shadow_settlements": {
            "shadow_settlement_id",
            "shadow_campaign_id",
            "campaign_id",
            "mode",
            "config_hash",
            "window_start_ms",
            "window_end_ms",
            "resolved_outcome",
            "status",
            "simulated_pnl",
            "simulated_fees",
            "settled_at_ms",
        },
        "prediction_shadow_fills": {
            "shadow_fill_id",
            "shadow_campaign_id",
            "campaign_id",
            "mode",
            "config_hash",
            "window_start_ms",
            "window_end_ms",
        },
    }
    result: dict[str, set[str]] = {}
    for table, columns in required.items():
        actual = _table_columns(conn, table)
        if not actual:
            raise SchemaError(f"required table is missing: {table}")
        missing = sorted(columns - actual)
        if missing:
            raise SchemaError(f"{table} is missing columns: {', '.join(missing)}")
        result[table] = actual
    return result


def _runtime_lane_manifest(conn: sqlite3.Connection) -> tuple[LaneSpec, ...] | None:
    """Read the worker's immutable lane manifest without modifying the DB."""

    tables = {
        str(row["name"])
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    }
    if "prediction_runtime_config" not in tables:
        return None
    columns = _table_columns(conn, "prediction_runtime_config")
    required = {"config_key", "config_value_json"}
    if not required.issubset(columns):
        raise SchemaError("prediction_runtime_config is missing manifest columns")
    row = conn.execute(
        "SELECT config_value_json FROM prediction_runtime_config "
        "WHERE config_key=? ORDER BY updated_at_ms DESC LIMIT 1",
        ("prediction_shadow_lanes",),
    ).fetchone()
    if row is None:
        return None
    try:
        value = json.loads(str(row["config_value_json"]))
    except (TypeError, ValueError) as exc:
        raise ConfigurationError("prediction_shadow_lanes manifest is not valid JSON") from exc
    return load_lane_specs(value)


def _normalize_spec_sequence(value: Sequence[LaneSpec] | None) -> tuple[LaneSpec, ...] | None:
    if value is None:
        return None
    specs = tuple(value)
    if not specs:
        raise ConfigurationError("at least one lane spec is required")
    seen: set[str] = set()
    for spec in specs:
        if not isinstance(spec, LaneSpec):
            raise ConfigurationError("lane_specs must contain LaneSpec values")
        if spec.name in seen:
            raise ConfigurationError(f"duplicate lane spec: {spec.name}")
        seen.add(spec.name)
    return specs


def _same_specs(left: Sequence[LaneSpec], right: Sequence[LaneSpec]) -> bool:
    return {spec.name: spec.as_dict() for spec in left} == {spec.name: spec.as_dict() for spec in right}


def _select_prefixed(alias: str, prefix: str, columns: set[str], names: Sequence[str]) -> str:
    expressions: list[str] = []
    for name in names:
        if name in columns:
            expressions.append(f'{alias}."{name}" AS "{prefix}_{name}"')
        else:
            expressions.append(f'NULL AS "{prefix}_{name}"')
    return ", ".join(expressions)


_CAMPAIGN_COLUMNS = (
    "shadow_campaign_id",
    "campaign_id",
    "mode",
    "config_hash",
    "window_start_ms",
    "window_end_ms",
    "market_topic_id",
    "market_id",
    "slug",
    "lane",
    "resolved_at_ms",
    "simulated_pnl",
    "simulated_fees",
    "campaign_start_ms",
    "campaign_end_ms",
    "created_at_ms",
    "payload_json",
)
_SETTLEMENT_COLUMNS = (
    "shadow_settlement_id",
    "shadow_campaign_id",
    "campaign_id",
    "mode",
    "config_hash",
    "window_start_ms",
    "window_end_ms",
    "resolved_outcome",
    "status",
    "simulated_pnl",
    "simulated_fees",
    "settled_at_ms",
    "payload_json",
)
_FILL_COLUMNS = (
    "shadow_fill_id",
    "shadow_campaign_id",
    "campaign_id",
    "mode",
    "config_hash",
    "window_start_ms",
    "window_end_ms",
    "fill_identity",
    "event_time_ms",
    "payload_json",
)


def _prefixed_row(row: sqlite3.Row, prefix: str, names: Sequence[str]) -> dict[str, Any]:
    return {name: row[f"{prefix}_{name}"] for name in names}


def _lane_candidates(campaign: Mapping[str, Any]) -> tuple[str, ...]:
    payload = _json_object(campaign.get("payload_json"))
    values = [campaign.get("lane"), payload.get("lane")]
    campaign_id = str(campaign.get("campaign_id") or "")
    marker = "::shadow::"
    if marker in campaign_id:
        values.append(campaign_id.rsplit(marker, 1)[1])
    result: list[str] = []
    for value in values:
        lane = _normalize_lane(value)
        if lane and lane not in result:
            result.append(lane)
    return tuple(result)


def _market_key(campaign: Mapping[str, Any]) -> str:
    for key in ("market_topic_id", "market_id", "slug", "campaign_id"):
        value = str(campaign.get(key) or "").strip()
        if value:
            return value
    return str(campaign.get("shadow_campaign_id") or "")


def _scope_mismatch(actual: Mapping[str, Any], expected: LaneSpec) -> str | None:
    if str(actual.get("mode") or "").upper() != "SHADOW":
        return "mode is not SHADOW"
    if str(actual.get("config_hash") or "") != expected.config_hash:
        return f"config_hash={actual.get('config_hash')!s} expected={expected.config_hash}"
    try:
        start = int(actual.get("window_start_ms"))
        end = int(actual.get("window_end_ms"))
    except (TypeError, ValueError):
        return "window bounds are not integers"
    if (start, end) != (expected.window_start_ms, expected.window_end_ms):
        return (
            f"window=({start},{end}) expected=({expected.window_start_ms},"
            f"{expected.window_end_ms})"
        )
    return None


def _add_issue(issues: list[str], value: str, *, limit: int = 100) -> None:
    if len(issues) < limit:
        issues.append(value)


def _load_report_rows(
    conn: sqlite3.Connection,
    columns: Mapping[str, set[str]],
    baseline_ms: int,
    issues: list[str],
) -> tuple[list[dict[str, Any]], dict[str, list[dict[str, Any]]]]:
    c_select = _select_prefixed("c", "c", columns["prediction_shadow_campaigns"], _CAMPAIGN_COLUMNS)
    s_select = _select_prefixed("s", "s", columns["prediction_shadow_settlements"], _SETTLEMENT_COLUMNS)
    sql = f"""
        SELECT {c_select}, {s_select}
        FROM prediction_shadow_campaigns AS c
        JOIN prediction_shadow_settlements AS s
          ON s.shadow_campaign_id = c.shadow_campaign_id
        WHERE UPPER(COALESCE(c.mode, '')) = 'SHADOW'
          AND UPPER(COALESCE(s.status, '')) = 'SETTLED'
          AND c.campaign_start_ms >= ?
        ORDER BY s.settled_at_ms ASC,
                 s.shadow_settlement_id ASC,
                 c.shadow_campaign_id ASC
    """
    joined: list[dict[str, Any]] = []
    for raw in conn.execute(sql, (baseline_ms,)).fetchall():
        campaign = _prefixed_row(raw, "c", _CAMPAIGN_COLUMNS)
        settlement = _prefixed_row(raw, "s", _SETTLEMENT_COLUMNS)
        lanes = _lane_candidates(campaign)
        if not lanes:
            # The worker also writes an unlabelled control counterfactual.
            # It is outside prediction_shadow_lanes and must not invalidate
            # the configured lane experiment.
            lane = ""
        elif len(lanes) > 1:
            _add_issue(
                issues,
                f"lane identity conflict: campaign={campaign.get('campaign_id')} values={','.join(lanes)}",
            )
            lane = lanes[0]
        else:
            lane = lanes[0]

        if str(settlement.get("campaign_id") or "") != str(campaign.get("campaign_id") or ""):
            _add_issue(issues, f"campaign identity mismatch: shadow={campaign.get('shadow_campaign_id')}")
        if str(settlement.get("mode") or "").upper() != "SHADOW":
            _add_issue(issues, f"settlement mode is not SHADOW: {settlement.get('shadow_settlement_id')}")
        try:
            provenance_mismatch = (
                str(settlement.get("config_hash") or "") != str(campaign.get("config_hash") or "")
                or int(settlement.get("window_start_ms")) != int(campaign.get("window_start_ms"))
                or int(settlement.get("window_end_ms")) != int(campaign.get("window_end_ms"))
            )
        except (TypeError, ValueError):
            provenance_mismatch = True
        if provenance_mismatch:
            _add_issue(issues, f"settlement provenance mismatch: shadow={campaign.get('shadow_campaign_id')}")
        try:
            pnl = _decimal(settlement.get("simulated_pnl"), "simulated_pnl")
            fees = _decimal(settlement.get("simulated_fees"), "simulated_fees")
            settled_at_ms = int(settlement.get("settled_at_ms"))
        except (TypeError, ValueError) as exc:
            raise SchemaError(f"invalid settlement row: {campaign.get('shadow_campaign_id')}") from exc
        joined.append(
            {
                "campaign": campaign,
                "settlement": settlement,
                "lane": lane,
                "market_key": _market_key(campaign),
                "pnl": pnl,
                "fees": fees,
                "settled_at_ms": settled_at_ms,
            }
        )

    # A UNIQUE(shadow_campaign_id) constraint is present in the intended schema,
    # but detecting duplicates here also protects reports made from damaged or
    # older copies of the database.
    for key_name, key_fn in (
        ("shadow campaign", lambda row: row["campaign"].get("shadow_campaign_id")),
        ("settlement", lambda row: row["settlement"].get("shadow_settlement_id")),
        ("lane/campaign", lambda row: (row["lane"], row["campaign"].get("campaign_id"))),
        ("lane/market", lambda row: (row["lane"], row["market_key"])),
    ):
        grouped: dict[Any, list[dict[str, Any]]] = {}
        for row in joined:
            key = key_fn(row)
            grouped.setdefault(key, []).append(row)
        for key, rows in grouped.items():
            if key and len(rows) > 1:
                _add_issue(issues, f"duplicate settlement ({key_name}={key!s}, count={len(rows)})")

    fill_select = _select_prefixed("f", "f", columns["prediction_shadow_fills"], _FILL_COLUMNS)
    fill_rows: dict[str, list[dict[str, Any]]] = {}
    if joined:
        fill_sql = f"""
            SELECT {fill_select}
            FROM prediction_shadow_fills AS f
            WHERE UPPER(COALESCE(f.mode, '')) = 'SHADOW'
              AND EXISTS (
                    SELECT 1
                    FROM prediction_shadow_settlements AS s
                    WHERE s.shadow_campaign_id = f.shadow_campaign_id
                      AND UPPER(COALESCE(s.status, '')) = 'SETTLED'
                      AND EXISTS (
                            SELECT 1
                            FROM prediction_shadow_campaigns AS c2
                            WHERE c2.shadow_campaign_id = s.shadow_campaign_id
                              AND c2.campaign_start_ms >= ?
                      )
              )
            ORDER BY f.shadow_campaign_id ASC, f.shadow_fill_id ASC
        """
        for raw in conn.execute(fill_sql, (baseline_ms,)).fetchall():
            fill = _prefixed_row(raw, "f", _FILL_COLUMNS)
            fill_rows.setdefault(str(fill.get("shadow_campaign_id") or ""), []).append(fill)
        campaigns_by_shadow_id = {
            str(row["campaign"].get("shadow_campaign_id") or ""): row["campaign"]
            for row in joined
        }
        for shadow_id, fills in fill_rows.items():
            ids_seen: set[str] = set()
            identities_seen: set[str] = set()
            campaign = campaigns_by_shadow_id.get(shadow_id)
            for fill in fills:
                fill_id = str(fill.get("shadow_fill_id") or "")
                fill_identity = str(fill.get("fill_identity") or "")
                if not fill_id or fill_id in ids_seen or (fill_identity and fill_identity in identities_seen):
                    _add_issue(issues, f"duplicate/invalid fill identity: shadow={shadow_id}")
                ids_seen.add(fill_id)
                if fill_identity:
                    identities_seen.add(fill_identity)
                if campaign is None:
                    _add_issue(issues, f"orphan fill: shadow={shadow_id}")
                    continue
                try:
                    fill_mismatch = (
                        str(fill.get("campaign_id") or "") != str(campaign.get("campaign_id") or "")
                        or str(fill.get("config_hash") or "") != str(campaign.get("config_hash") or "")
                        or int(fill.get("window_start_ms")) != int(campaign.get("window_start_ms"))
                        or int(fill.get("window_end_ms")) != int(campaign.get("window_end_ms"))
                    )
                except (TypeError, ValueError):
                    fill_mismatch = True
                if fill_mismatch:
                    _add_issue(issues, f"fill provenance mismatch: shadow={shadow_id}")
    return joined, fill_rows


def _summarize_records(lane: str, records: Sequence[RunRecord]) -> LaneMetrics:
    records = tuple(records)
    directional_records = tuple(
        record
        for record in records
        if record.has_fill and record.resolved_outcome != "DRAW"
    )
    wins = sum(1 for record in directional_records if record.simulated_pnl > 0)
    losses = sum(1 for record in directional_records if record.simulated_pnl < 0)
    breakevens = sum(1 for record in directional_records if record.simulated_pnl == 0)
    draws = sum(
        1
        for record in records
        if record.has_fill and record.resolved_outcome == "DRAW"
    )
    return LaneMetrics(
        lane=lane,
        settled_runs=len(records),
        filled_runs=sum(record.has_fill for record in records),
        fill_events=sum(record.fill_count for record in records),
        wins=wins,
        losses=losses,
        draws=draws,
        breakevens=breakevens,
        pnl=sum((record.simulated_pnl for record in records), Decimal("0")),
        fees=sum((record.simulated_fees for record in records), Decimal("0")),
        runs=records,
    )


def _lane_metrics(lane: str, rows: Sequence[dict[str, Any]], fill_rows: Mapping[str, Sequence[Mapping[str, Any]]]) -> LaneMetrics:
    records: list[RunRecord] = []
    for row in rows:
        campaign = row["campaign"]
        settlement = row["settlement"]
        shadow_id = str(campaign.get("shadow_campaign_id") or "")
        fills = fill_rows.get(shadow_id, ())
        records.append(
            RunRecord(
                lane=lane,
                campaign_id=str(campaign.get("campaign_id") or ""),
                shadow_campaign_id=shadow_id,
                shadow_settlement_id=str(settlement.get("shadow_settlement_id") or ""),
                market_key=str(row["market_key"]),
                settled_at_ms=int(row["settled_at_ms"]),
                resolved_outcome=str(settlement.get("resolved_outcome") or "").upper(),
                simulated_pnl=row["pnl"],
                simulated_fees=row["fees"],
                fill_count=len({str(fill.get("shadow_fill_id") or "") for fill in fills}),
            )
        )
    return _summarize_records(lane, records)


def _load_gate_rejection_reasons(
    db_path: Path,
    *,
    baseline_ms: int,
    gate_metrics: LaneMetrics | None,
) -> dict[str, str]:
    """Classify each no-fill Gate market by its strongest/latest block reason."""

    if gate_metrics is None:
        return {}
    no_fill_campaigns = {
        record.campaign_id.split("::shadow::", 1)[0]
        for record in gate_metrics.runs
        if not record.has_fill
    }
    if not no_fill_campaigns:
        return {}
    uri = "file:" + urllib.parse.quote(str(db_path), safe="/:\\") + "?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA query_only=ON")
        tables = {
            str(row["name"])
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        if "prediction_risk_events" not in tables:
            return {}
        selected: dict[str, tuple[int, int, str]] = {}
        rows = conn.execute(
            "SELECT campaign_id,event_time_ms,payload_json FROM prediction_risk_events "
            "WHERE event_type='SHADOW_LANE_DECISION' AND event_time_ms>=? "
            "ORDER BY event_time_ms ASC,event_id ASC",
            (baseline_ms,),
        )
        for row in rows:
            campaign_id = str(row["campaign_id"] or "")
            if campaign_id not in no_fill_campaigns:
                continue
            payload = _json_object(row["payload_json"])
            if _normalize_lane(payload.get("lane")) != PRIMARY_GATE_LANE:
                continue
            reason = str(payload.get("reason") or "").strip()
            allowed = payload.get("allowed")
            if allowed is False or allowed == 0:
                priority = 2
            elif reason == "base V3 entry gate not met":
                priority = 1
            else:
                continue
            candidate = (priority, int(row["event_time_ms"]), reason or "unknown")
            previous = selected.get(campaign_id)
            if previous is None or candidate[:2] >= previous[:2]:
                selected[campaign_id] = candidate
    finally:
        conn.close()
    return {
        campaign_id: selected.get(campaign_id, (0, 0, "unclassified/no gate event"))[2]
        for campaign_id in sorted(no_fill_campaigns)
    }


def _cohort_scope_matches(
    candidate: Mapping[str, Any],
    lane_specs: Sequence[LaneSpec],
) -> bool:
    """Require a shared cohort record to match this report's frozen lane scope."""

    identities = candidate.get("lane_identities")
    if not isinstance(identities, Mapping):
        return False
    expected = {spec.name: spec for spec in lane_specs}
    if set(str(lane) for lane in identities) != set(expected):
        return False
    for lane, spec in expected.items():
        actual = identities.get(lane)
        if not isinstance(actual, Mapping):
            return False
        if str(actual.get("config_hash") or "") != spec.config_hash:
            return False
        try:
            bounds = (int(actual.get("window_start_ms")), int(actual.get("window_end_ms")))
        except (TypeError, ValueError):
            return False
        if bounds != (spec.window_start_ms, spec.window_end_ms):
            return False
    return True


def _load_execution_censored_markets(
    conn: sqlite3.Connection,
    *,
    baseline_ms: int,
    lane_specs: Sequence[LaneSpec],
    settled_campaign_ids: set[str],
) -> tuple[str, ...]:
    """Find scoped shared-cohort interruptions without treating active markets as failures.

    A durable censored marker is authoritative when present.  The fallback scan
    also catches a process dying after the shared first-candidate record commit
    but before its completion record; only already-settled campaigns are
    considered, so an active market remains transient rather than censored.
    """

    tables = {
        str(row["name"])
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    }
    if "prediction_runtime_config" not in tables:
        return ()
    config_columns = _table_columns(conn, "prediction_runtime_config")
    if not {"config_key", "config_value_json"}.issubset(config_columns):
        return ()
    rows = conn.execute(
        "SELECT config_key,config_value_json FROM prediction_runtime_config "
        "WHERE config_key LIKE ?",
        ("prediction_shadow_cohort_%",),
    ).fetchall()
    configs = {str(row["config_key"]): _json_object(row["config_value_json"]) for row in rows}
    expected_lanes = {spec.name for spec in lane_specs}
    affected: set[str] = set()

    def campaign_id_for(candidate: Mapping[str, Any]) -> str:
        return str(candidate.get("campaign_id") or "").strip()

    def add_candidate(candidate: Mapping[str, Any]) -> None:
        campaign_id = campaign_id_for(candidate)
        if campaign_id in settled_campaign_ids:
            affected.add(campaign_id)

    for key, marker in configs.items():
        if not key.startswith("prediction_shadow_cohort_execution_censored:"):
            continue
        lanes = marker.get("lanes")
        if not isinstance(lanes, Sequence) or isinstance(lanes, (str, bytes)):
            continue
        if {str(lane) for lane in lanes} != expected_lanes:
            continue
        candidate_key = str(marker.get("candidate_id") or "").strip()
        candidate = configs.get(candidate_key)
        if not candidate or not _cohort_scope_matches(candidate, lane_specs):
            continue
        add_candidate(candidate)

    for key, candidate in configs.items():
        if not key.startswith("prediction_shadow_cohort_first_candidate:"):
            continue
        if not _cohort_scope_matches(candidate, lane_specs):
            continue
        campaign_id = campaign_id_for(candidate)
        if campaign_id not in settled_campaign_ids:
            continue
        digest = key.split(":", 1)[1]
        completion = configs.get(f"prediction_shadow_cohort_completion:{digest}")
        if not completion or completion.get("complete") is not True:
            affected.add(campaign_id)

    if "prediction_risk_events" in tables:
        event_columns = _table_columns(conn, "prediction_risk_events")
        required = {"event_type", "event_time_ms", "campaign_id", "payload_json"}
        if required.issubset(event_columns):
            event_rows = conn.execute(
                "SELECT campaign_id,event_time_ms,payload_json FROM prediction_risk_events "
                "WHERE event_type=? AND event_time_ms>=?",
                ("SHADOW_COHORT_EXECUTION_CENSORED", baseline_ms),
            )
            for row in event_rows:
                payload = _json_object(row["payload_json"])
                lanes = payload.get("lanes")
                if not isinstance(lanes, Sequence) or isinstance(lanes, (str, bytes)):
                    continue
                if {str(lane) for lane in lanes} != expected_lanes:
                    continue
                if payload.get("execution_censored") is not True:
                    continue
                campaign_id = str(row["campaign_id"] or "").strip()
                candidate_key = str(payload.get("candidate_id") or "").strip()
                if candidate_key:
                    candidate = configs.get(candidate_key)
                    if candidate is None or not _cohort_scope_matches(candidate, lane_specs):
                        continue
                    candidate_campaign_id = campaign_id_for(candidate)
                    if candidate_campaign_id and candidate_campaign_id != campaign_id:
                        continue
                if campaign_id in settled_campaign_ids:
                    affected.add(campaign_id)
    return tuple(sorted(affected))


def build_report(
    db_path: str | Path,
    *,
    baseline_ms: int,
    lane_specs: Sequence[LaneSpec] | None = None,
    fallback_lane_specs: str | Mapping[str, Any] | None = None,
    target_runs: int = DEFAULT_TARGET_RUNS,
) -> ShadowReport:
    """Build a read-only report with validation issues included.

    Counts are capped per lane at ``target_runs`` after sorting by authoritative
    settlement time.  Validation scans all post-baseline settled rows, so a late
    mixed-version or duplicate row cannot hide behind the 200-run display cap.
    """

    if target_runs <= 0:
        raise ConfigurationError("target_runs must be positive")
    baseline = _as_int(baseline_ms, "baseline_ms")
    if baseline < 0:
        raise ConfigurationError("baseline_ms must be non-negative")
    path = Path(db_path).expanduser().resolve()
    if not path.is_file():
        raise ConfigurationError(f"database file does not exist: {path}")

    uri = "file:" + urllib.parse.quote(str(path), safe="/:\\") + "?mode=ro"
    issues: list[str] = []
    try:
        conn = sqlite3.connect(uri, uri=True)
    except sqlite3.Error as exc:
        raise ConfigurationError(f"cannot open database read-only: {path}") from exc
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA query_only=ON")
        columns = _check_schema(conn)
        manifest_specs = _runtime_lane_manifest(conn)
        explicit_specs = _normalize_spec_sequence(lane_specs)
        fallback_specs = load_lane_specs(fallback_lane_specs) if fallback_lane_specs is not None else None
        if manifest_specs is not None:
            if explicit_specs is not None and not _same_specs(manifest_specs, explicit_specs):
                raise ConfigurationError("database lane manifest conflicts with explicit lane scope")
            if fallback_specs is not None and not _same_specs(manifest_specs, fallback_specs):
                raise ConfigurationError("database lane manifest conflicts with fallback lane scope")
            normalized_specs = manifest_specs
        else:
            normalized_specs = explicit_specs or fallback_specs
            if normalized_specs is None:
                raise ConfigurationError(
                    "lane specs are required (database manifest or SHADOW_LANE_SPECS_JSON)"
                )
        spec_by_lane = {spec.name: spec for spec in normalized_specs}
        joined, fill_rows = _load_report_rows(conn, columns, baseline, issues)
        for raw_state in conn.execute("SELECT config_value_json FROM prediction_runtime_config WHERE config_key LIKE 'prediction_shadow_reversal5:%'"):
            record=_json_object(raw_state[0])
            if int(record.get('sample_at_ms') or 0)>=baseline and record.get('coverage',{}).get('entry_window_missed'):
                _add_issue(issues,'Reversal5 missing early-entry window: '+str(record.get('campaign_id')))
            if int(record.get('sample_at_ms') or 0)>=baseline and (record.get('censored') or record.get('inflight')):
                _add_issue(issues,'Reversal5 censored/incomplete cohort: '+str(record.get('campaign_id')))

        diverse_records = {r["config_key"]: _json_object(r["config_value_json"]) for r in conn.execute(
            "SELECT config_key,config_value_json FROM prediction_runtime_config WHERE config_key LIKE 'prediction_shadow_cohort_diverse5:%'")}
        settled_virtual = {str(r["campaign"].get("campaign_id")) for r in joined}
        for key, candidate in diverse_records.items():
            if key.endswith(":complete") or candidate.get("campaign_id") not in settled_virtual:
                continue
            if int(candidate.get("decision_at_ms") or 0) < baseline:
                continue
            if diverse_records.get(key+":complete", {}).get("complete") is not True:
                _add_issue(issues, "Diverse5 incomplete first-candidate execution: " + str(candidate.get("campaign_id")))
    finally:
        conn.close()

    by_lane: dict[str, list[dict[str, Any]]] = {spec.name: [] for spec in normalized_specs}
    for row in joined:
        lane = str(row["lane"] or "")
        expected = spec_by_lane.get(lane)
        if expected is None:
            if lane:
                _add_issue(issues, f"unexpected lane: {lane}")
            continue
        campaign_mismatch = _scope_mismatch(row["campaign"], expected)
        settlement_mismatch = _scope_mismatch(row["settlement"], expected)
        if campaign_mismatch:
            _add_issue(
                issues,
                f"mixed baseline/config: lane={lane} campaign={row['campaign'].get('campaign_id')} {campaign_mismatch}",
            )
        if settlement_mismatch:
            _add_issue(
                issues,
                f"mixed settlement scope: lane={lane} settlement={row['settlement'].get('shadow_settlement_id')} {settlement_mismatch}",
            )
        by_lane[lane].append(row)

    if joined:
        for spec in normalized_specs:
            if not by_lane[spec.name]:
                _add_issue(issues, f"missing lane data: {spec.name}")

    metrics: dict[str, LaneMetrics] = {}
    ordered_by_lane: dict[str, list[dict[str, Any]]] = {}
    for spec in normalized_specs:
        ordered = sorted(
            by_lane[spec.name],
            key=lambda row: (
                int(row["campaign"].get("campaign_start_ms") or 0),
                str(row["market_key"]),
                str(row["settlement"].get("shadow_settlement_id") or ""),
                str(row["campaign"].get("shadow_campaign_id") or ""),
            ),
        )
        ordered_by_lane[spec.name] = ordered[:target_runs]
        # An invalid row is still visible in the report, but cannot be counted
        # outside the configured scope.  The caller will not publish when issues
        # is non-empty.
        metrics[spec.name] = _lane_metrics(spec.name, ordered[:target_runs], fill_rows)

    if metrics:
        common = min(item.settled_runs for item in metrics.values())
        reference_lane = PRIMARY_GATE_LANE if PRIMARY_GATE_LANE in metrics else normalized_specs[0].name
        reference_keys = [row["market_key"] for row in ordered_by_lane[reference_lane][:common]]
        for spec in normalized_specs:
            lane_keys = [row["market_key"] for row in ordered_by_lane[spec.name][:common]]
            if lane_keys != reference_keys:
                _add_issue(
                    issues,
                    f"market alignment mismatch: lane={spec.name} reference={reference_lane}",
                )

    settled_campaign_ids = {
        record.campaign_id.split("::shadow::", 1)[0]
        for item in metrics.values()
        for record in item.runs
    }
    censored_uri = "file:" + urllib.parse.quote(str(path), safe="/:\\") + "?mode=ro"
    censored_conn = sqlite3.connect(censored_uri, uri=True)
    censored_conn.row_factory = sqlite3.Row
    try:
        censored_conn.execute("PRAGMA query_only=ON")
        execution_censored_market_ids = _load_execution_censored_markets(
            censored_conn,
            baseline_ms=baseline,
            lane_specs=normalized_specs,
            settled_campaign_ids=settled_campaign_ids,
        )
    finally:
        censored_conn.close()

    gate_reasons = _load_gate_rejection_reasons(
        path,
        baseline_ms=baseline,
        gate_metrics=metrics.get(PRIMARY_GATE_LANE),
    )

    return ShadowReport(
        path,
        baseline,
        target_runs,
        normalized_specs,
        metrics,
        tuple(issues),
        gate_reasons,
        execution_censored_market_ids,
    )


def _format_decimal(value: Decimal, places: int = 6) -> str:
    quantized = value.quantize(Decimal(1).scaleb(-places))
    text = format(quantized, "f").rstrip("0").rstrip(".")
    return text if text and text != "-0" else "0"


def _format_fixed(value: Decimal, places: int) -> str:
    """Format a Decimal with a fixed number of places for human reports."""

    return format(value.quantize(Decimal(1).scaleb(-places)), "f")


def _format_signed(value: Decimal, places: int = 4) -> str:
    """Format signed PnL, retaining a visible plus sign for positive values."""

    quantized = value.quantize(Decimal(1).scaleb(-places))
    if quantized > 0:
        return f"+{format(quantized, 'f')}"
    return format(quantized, "f")


def _order_unit_usdt() -> Decimal:
    raw = os.environ.get("PREDICTION_ORDER_UNIT_USDT", "2")
    try:
        value = Decimal(raw)
    except (InvalidOperation, TypeError, ValueError):
        return Decimal("2")
    return value if value > 0 else Decimal("2")


def _display_name(lane: str) -> str:
    return DISPLAY_NAMES.get(lane, lane)


def _ranked_lanes(metrics: Mapping[str, LaneMetrics]) -> list[str]:
    """Rank lanes by realized PnL, then quality and deterministic lane order."""

    lane_order = {lane: index for index, lane in enumerate(DISPLAY_ORDER)}
    return sorted(
        metrics,
        key=lambda lane: (
            -metrics[lane].pnl,
            -metrics[lane].win_rate,
            -metrics[lane].fill_rate,
            -metrics[lane].filled_runs,
            lane_order.get(lane, len(DISPLAY_ORDER)),
            lane,
        ),
    )


def _display_ordered_lanes(metrics: Mapping[str, LaneMetrics]) -> list[str]:
    """Return lanes in the frozen display order without implying a winner."""

    lane_order = {lane: index for index, lane in enumerate(DISPLAY_ORDER)}
    return sorted(metrics, key=lambda lane: (lane_order.get(lane, len(DISPLAY_ORDER)), lane))


def _limited_metrics(report: ShadowReport, upto_runs: int) -> dict[str, LaneMetrics]:
    return {
        lane: _summarize_records(lane, metrics.runs[:upto_runs])
        for lane, metrics in report.lanes.items()
    }


def _paired_metrics(
    report: ShadowReport,
    challenger_lane: str,
    base_lane: str,
    upto_runs: int,
) -> tuple[LaneMetrics, LaneMetrics] | None:
    """Summarize two lanes only on identical market identities.

    A no-fill settlement remains in the paired stream with its authoritative
    zero PnL.  This prevents a selective lane from looking better merely
    because its skipped markets disappeared from the denominator.
    """

    challenger = report.lanes.get(challenger_lane)
    base = report.lanes.get(base_lane)
    if challenger is None or base is None:
        return None
    challenger_by_market = {
        record.market_key: record for record in challenger.runs[:upto_runs]
    }
    paired_challenger: list[RunRecord] = []
    paired_base: list[RunRecord] = []
    for base_record in base.runs[:upto_runs]:
        challenger_record = challenger_by_market.get(base_record.market_key)
        if challenger_record is None:
            continue
        paired_base.append(base_record)
        paired_challenger.append(challenger_record)
    return (
        _summarize_records(challenger_lane, paired_challenger),
        _summarize_records(base_lane, paired_base),
    )


def _format_win_rate(metrics: LaneMetrics) -> str:
    if not metrics.directional_filled_runs:
        return "—"
    return f"{_format_fixed(metrics.win_rate * 100, 1)}%"


def _append_paired_section(
    lines: list[str],
    report: ShadowReport,
    challenger_lane: str,
    base_lane: str,
    upto_runs: int,
    *,
    caveat: str = "註：Frozen sign gate 研究，非已校準 EV。",
) -> None:
    """Append one same-market frozen-sign-gate comparison, when both lanes exist."""

    paired = _paired_metrics(report, challenger_lane, base_lane, upto_runs)
    if paired is None:
        return
    challenger, paired_base = paired
    paired_count = challenger.settled_runs
    challenger_coverage = challenger.fill_rate * 100
    base_coverage = paired_base.fill_rate * 100
    base_name = "Base V3" if base_lane == BASE_V3_LANE else html.escape(_display_name(base_lane))
    lines.extend(
        [
            (
                f"📌 <b>{html.escape(_display_name(challenger_lane))} vs {base_name}"
                "（同市場配對；未交易以 0 PnL 納入）</b>"
            ),
            (
                f"配對市場: {paired_count}/{upto_runs} | "
                f"成交筆數: {challenger.fill_events} vs {paired_base.fill_events} | "
                f"Coverage: {challenger.filled_runs}/{paired_count} "
                f"({_format_fixed(challenger_coverage, 1)}%) vs "
                f"{paired_base.filled_runs}/{paired_count} "
                f"({_format_fixed(base_coverage, 1)}%)"
            ),
            (
                f"W/L/D: {challenger.wins}/{challenger.losses}/{challenger.draws} vs "
                f"{paired_base.wins}/{paired_base.losses}/{paired_base.draws} | "
                f"PnL: {_format_signed(challenger.pnl)}U vs "
                f"{_format_signed(paired_base.pnl)}U "
                f"(Δ {_format_signed(challenger.pnl - paired_base.pnl)}U)"
            ),
            (
                f"Max DD: {_format_decimal(challenger.max_drawdown, 4)}U vs "
                f"{_format_decimal(paired_base.max_drawdown, 4)}U"
            ),
            caveat,
        ]
    )


"""Read-only, clearly labelled collection report; never rank zero PnL as profit."""
def format_next5_observation_report(report,upto_runs=None):
    import sqlite3
    import json
    import html
    names={'early_value_v1':'A 開盤價值','trend_value_v1':'B 順勢價值',
           'reversion_value_v1':'C 過度反應回歸','late_oracle_watch_v1':'D 晚盤 Oracle 觀察',
           'passive_queue_watch_v1':'E 被動掛單觀察'}
    expected={spec.name:spec for spec in report.lane_specs}
    rows=[]
    with sqlite3.connect(report.db_path.resolve().as_uri()+'?mode=ro',uri=True) as c:
        c.execute('PRAGMA query_only=ON')
        for raw in c.execute("SELECT config_value_json FROM prediction_runtime_config WHERE config_key LIKE 'prediction_shadow_next5:%'"):
            r=json.loads(raw[0]); ids=r.get('lane_identities',{})
            if set(ids)!=set(expected): continue
            if any(ids[lane].get('config_hash')!=spec.config_hash or
                   ids[lane].get('window_start_ms')!=spec.window_start_ms or
                   ids[lane].get('window_end_ms')!=spec.window_end_ms for lane,spec in expected.items()): continue
            if r.get('start_ms',0)<report.baseline_ms: continue
            rows.append(r)
    rows.sort(key=lambda r:(r['start_ms'],r['campaign_id']))
    rows=rows[:report.target_runs]
    if upto_runs is not None: rows=rows[:max(0,int(upto_runs))]
    settled=min(report.common_run_count,report.target_runs)
    if upto_runs is not None: settled=min(settled,int(upto_runs))
    lines=['<b>Next5：200 場資料收集／不成交</b>',
           f'已觀察 {len(rows)}/{report.target_runs} 場；官方結算 {settled} 場。',
           'A–C：模型驗證未過；D：缺官方即時價格；E：缺成交帶／佇列證據。',
           '此批成交停用，PnL／勝率不適用；不是一般 no-fill，也不是獲利驗證。']
    for lane,name in names.items():
        states=[r['state']['lanes'][lane] for r in rows]
        observable=sum(bool(s.get('valid_samples')) for s in states)
        signals=sum(bool(s.get('signals')) for s in states)
        signal_samples=sum(s.get('signals',0) for s in states)
        lines.append(f'{name}：窗口有有效資料 {observable}/{len(states)}；訊號市場 {signals}（樣本 {signal_samples}，非獨立交易）')
    if rows: lines.append('模型：'+html.escape(rows[0]['model_id'][:12])+'；條件已凍結，收滿後離線校準。')
    if report.issues:
        lines.append('資料問題：'+html.escape('; '.join(report.issues[:5])))
    return '\n'.join(lines)


def format_confirm3_report(report,*,upto_runs=None):
    lanes=('trend_control_1s_v1','reversion_control_1s_v1','reversion_control_3s_v1')
    names=('B 順勢 1秒','C 回歸 1秒','C 回歸 3秒')
    expected={spec.name:spec for spec in report.lane_specs}
    with sqlite3.connect(report.db_path.resolve().as_uri()+'?mode=ro',uri=True) as c:
        c.execute('PRAGMA query_only=ON')
        states=[json.loads(r[0]) for r in c.execute("SELECT config_value_json FROM prediction_runtime_config WHERE config_key LIKE 'prediction_shadow_confirm3:%'")]
    states=[r for r in states if r.get('start_ms',0)>=report.baseline_ms and
        set(r.get('lane_identities',{}))==set(expected) and all(
            r['lane_identities'][lane].get('config_hash')==spec.config_hash and
            r['lane_identities'][lane].get('window_start_ms')==spec.window_start_ms and
            r['lane_identities'][lane].get('window_end_ms')==spec.window_end_ms for lane,spec in expected.items())]
    states.sort(key=lambda r:r.get('start_ms',0));states=states[:report.target_runs]
    if upto_runs is not None:states=states[:int(upto_runs)]
    lines=['<b>Confirm3：研究型模擬成交／非實盤</b>',f'已觀察 {len(states)}/200 場；三條共用市場，不是600個獨立樣本。',
           '成本為額外3%現金等值假設；實際費用／最小單未核實；舊模型閘門關閉。']
    for lane,name in zip(lanes,names):
        ls=[r.get('state',{}).get('lanes',{}).get(lane,{}) for r in states]
        signals=sum(bool(r.get('signals')) for r in ls);attempts=sum(r.get('attempts',0) for r in ls)
        fills=sum(r.get('fills',0) for r in ls)
        missing=sum(r.get('reason')=='window_data_missing' for r in ls)
        rejected=sum(r.get('status')=='rejected' for r in ls)
        pending=sum(r.get('status')=='pending' for r in ls)
        expired=sum(r.get('status')=='expired' for r in ls)
        lines.append(f'{name}：訊號{signals}／嘗試{attempts}／成交計畫{fills}；拒絕{rejected}／待成交{pending}／逾期{expired}／缺窗口{missing}')
        runs=list(report.lanes[lane].runs)[:report.target_runs]
        if upto_runs is not None:runs=runs[:int(upto_runs)]
        pnl=sum((r.simulated_pnl for r in runs),Decimal('0'))
        settled_fills=sum(r.fill_count for r in runs)
        lines.append(f'已結算{len(runs)}場／{settled_fills}筆；假設成本後 PnL {pnl:+.4f} USDT')
    censored=sum(bool(r.get('censored')) for r in states)
    if censored:lines.append(f'⚠️ {censored} 場狀態／成交落盤不完整，績效不可直接採信。')
    if report.issues:lines.append('資料問題：'+html.escape('; '.join(report.issues[:4])))
    lines.append('逾期單與資料缺失分列；全部損益是研究假設，不可晉級實盤。')
    return '\n'.join(lines)


def format_value9_report(
    report: ShadowReport,
    *,
    batch_number: int | None = None,
    upto_runs: int | None = None,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> str:
    """Compact every-20 readout for the nine-lane Value9 cohort."""
    common = min(report.common_run_count, report.target_runs)
    upto = common if upto_runs is None else min(common, report.target_runs, max(0, int(upto_runs)))
    current_batch = max(1, int(batch_number or (((upto - 1) // batch_size + 1) if upto else 1)))
    batch_start = (current_batch - 1) * batch_size
    batch_count = min(batch_size, max(0, upto - batch_start))
    cumulative = _limited_metrics(report, upto)
    batch = {
        lane: _summarize_records(lane, metrics.runs[batch_start:batch_start + batch_count])
        for lane, metrics in report.lanes.items()
    }
    ranked = _ranked_lanes(cumulative)
    lines = [
        f"<b>Value9｜第 {current_batch} 段完成｜{upto}/{report.target_runs} markets</b>",
        "格式：成交 W-L｜本20@300｜累計@300｜累計@500｜MaxDD",
    ]
    for lane in ranked:
        total = cumulative[lane]
        block = batch[lane]
        # Value9 is frozen at one 2-USDT BUY at most. Moving from the 300bps
        # ledger to the 500bps stress case therefore costs another .04U/fill.
        stress_500 = total.pnl - Decimal("0.04") * total.filled_runs
        lines.append(
            f"• {html.escape(_display_name(lane))}: {total.filled_runs} "
            f"{total.wins}-{total.losses}｜{_format_signed(block.pnl)}U｜"
            f"{_format_signed(total.pnl)}U｜{_format_signed(stress_500)}U｜"
            f"{_format_decimal(total.max_drawdown, 3)}U"
        )
    if ranked:
        leader, laggard = ranked[0], ranked[-1]
        lines.append(
            f"目前：{html.escape(_display_name(leader))} 領先；"
            f"{html.escape(_display_name(laggard))} 落後。20筆只看方向，不提早淘汰。"
        )
    if report.issues:
        lines.append("⚠️ 資料檢查：" + html.escape("; ".join(report.issues[:3])))
    lines.append("研究 Shadow；每 lane 每市場最多一筆2U，500bps為額外200bps壓力情境。")
    return "\n".join(lines)


def format_report(
    report: ShadowReport,
    *,
    batch_number: int | None = None,
    upto_runs: int | None = None,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> str:
    """Render the Chinese, Telegram-safe progress and ranking report.

    ``upto_runs`` is the cumulative boundary.  When ``batch_number`` is given
    (as it is for checkpointed notifications), it selects the batch whose
    slice ends at that boundary; otherwise the batch containing ``upto_runs``
    is selected.  The Batch section uses that slice while Total remains
    cumulative through ``upto_runs``.
    """

    if set(report.lanes) == set(VALUE9_LANES):
        return format_value9_report(
            report, batch_number=batch_number, upto_runs=upto_runs,
            batch_size=batch_size,
        )
    if set(report.lanes)==set(('early_value_v1', 'trend_value_v1', 'reversion_value_v1', 'late_oracle_watch_v1', 'passive_queue_watch_v1')):
        return format_next5_observation_report(report,upto_runs=upto_runs)
    if set(report.lanes)==set(('trend_control_1s_v1', 'reversion_control_1s_v1', 'reversion_control_3s_v1')):
        return format_confirm3_report(report,upto_runs=upto_runs)
    common = min(report.common_run_count, report.target_runs)
    if upto_runs is None:
        upto = common
    else:
        upto = min(common, report.target_runs, max(0, int(upto_runs)))
    # Keep the cumulative view as the source for the Total section and all
    # comparison/diagnostic sections below.  The Batch section is a distinct
    # view over only the current batch; previously both sections rendered this
    # same cumulative prefix.
    cumulative_metrics = _limited_metrics(report, upto)
    attribution_only = HISTORY30_CONTROL_V1_LANE in cumulative_metrics
    ranked_lanes = (
        _display_ordered_lanes(cumulative_metrics)
        if attribution_only
        else _ranked_lanes(cumulative_metrics)
    )

    baseline_utc = datetime.fromtimestamp(report.baseline_ms / 1000, tz=timezone.utc)
    baseline_tpe = baseline_utc.astimezone(TAIPEI_TZ)
    completion = Decimal(upto) / Decimal(report.target_runs) * Decimal("100")
    prefix_campaign_ids = {
        record.campaign_id.split("::shadow::", 1)[0]
        for item in report.lanes.values()
        for record in item.runs[:upto]
    }
    execution_censored_market_ids = tuple(
        campaign_id
        for campaign_id in report.execution_censored_market_ids
        if campaign_id in prefix_campaign_ids
    )
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")
    if batch_number is None:
        current_batch = ((upto - 1) // batch_size + 1) if upto else 1
    else:
        current_batch = max(1, int(batch_number))
    batch_start = (current_batch - 1) * batch_size + 1
    batch_count = min(batch_size, max(0, upto - batch_start + 1))
    batch_end = batch_start + batch_size - 1
    batch_status = "已完成" if batch_count >= batch_size else "進行中"
    batch_metrics = {
        lane: _summarize_records(
            lane,
            metrics.runs[batch_start - 1 : batch_start - 1 + batch_count],
        )
        for lane, metrics in report.lanes.items()
    }
    ranked_batch_lanes = (
        _display_ordered_lanes(batch_metrics)
        if attribution_only
        else _ranked_lanes(batch_metrics)
    )

    lines = [
        "<b>📊【Cry3 多策略 20-Run 分段與總結算報告】</b>",
        (
            f"基準起點: {baseline_tpe.strftime('%Y-%m-%d %H:%M')} "
            f"({baseline_utc.strftime('%H:%M')} UTC)"
        ),
        f"累計已結算市場: {upto} / {report.target_runs} Runs (完成度 {_format_fixed(completion, 1)}%)",
        (
            f"⚠️ 執行完整性：{len(execution_censored_market_ids)} 個受影響市場 | "
            "IDs: " + ", ".join(html.escape(value) for value in execution_censored_market_ids)
            if execution_censored_market_ids
            else "✅ 執行完整性：0（無 execution-censored 市場）"
        ),
        "---------------------------------",
        (
            f"🔹【Batch {current_batch}: Run {batch_start}~{batch_end} "
            f"({batch_count}/{batch_size}) - {batch_status}】:"
        ),
    ]
    if execution_censored_market_ids:
        lines.append(
            "註：執行中斷造成的未成交，不是策略拒絕／獲利能力，需完整性審查。"
        )
    for lane in ranked_batch_lanes:
        item = batch_metrics[lane]
        name = html.escape(_display_name(lane))
        fill_pct = _format_fixed(item.fill_rate * 100, 1)
        wr_text = _format_win_rate(item)
        pnl = _format_signed(item.pnl)
        lines.append(
            f"• {name} : {item.filled_runs}/{batch_count}筆 ({fill_pct}%) | "
            f"{wr_text} WR ({item.wins}W {item.losses}L {item.draws}D) | {pnl}U"
        )

    lines.extend(
        [
            "---------------------------------------------------------------",
            *(["⚖️ 僅作同市場歸因比較，不作勝者排名。"] if attribution_only else []),
            (
                f"🏆【Total {report.target_runs}-Run 總累計結算 "
                f"(1~{upto} Runs)】:")
        ]
    )
    order_unit = _order_unit_usdt()
    order_unit_label = _format_decimal(order_unit, 2)
    for rank, lane in enumerate(ranked_lanes, start=1):
        item = cumulative_metrics[lane]
        marker = (
            "•"
            if attribution_only
            else (("🥇", "🥈", "🥉")[rank - 1] if rank <= 3 else f"{rank}️⃣")
        )
        pnl = _format_signed(item.pnl)
        one_unit_pnl = _format_signed(item.pnl / order_unit)
        lines.extend(
            [
                f"{marker} {html.escape(_display_name(lane))}:",
                (
                    f"成交: {item.filled_runs}/{upto} ({_format_fixed(item.fill_rate * 100, 1)}%) | "
                    f"勝率: {_format_win_rate(item)} "
                    f"({item.wins}W {item.losses}L {item.draws}D)"
                ),
                (
                    f"累積損益: {pnl} USDT ({pnl} USDT / {order_unit_label}U) / "
                    f"({one_unit_pnl} USDT / 1U)"
                ),
            ]
        )
    gate = cumulative_metrics.get(PRIMARY_GATE_LANE)
    base = cumulative_metrics.get(BASE_V3_LANE)
    if gate is not None and base is not None:
        lines.append(
            "📌 <b>Gate V1 vs Base V3</b>："
            f"成交率 Δ {_format_fixed((gate.fill_rate - base.fill_rate) * 100, 1)}pp | "
            f"勝率 Δ {_format_fixed((gate.win_rate - base.win_rate) * 100, 1)}pp | "
            f"損益 Δ {_format_signed(gate.pnl - base.pnl)}U"
        )
    paired_net_edge = _paired_metrics(report, NET_EDGE_V1_LANE, BASE_V3_LANE, upto)
    if paired_net_edge is not None:
        net_edge, paired_base = paired_net_edge
        paired_count = net_edge.settled_runs
        net_coverage = net_edge.fill_rate * 100
        base_coverage = paired_base.fill_rate * 100
        lines.extend(
            [
                "📌 <b>V3 Net Edge V1 vs Base V3（同市場配對；未交易以 0 PnL 納入）</b>",
                (
                    f"配對市場: {paired_count}/{upto} | "
                    f"成交筆數: {net_edge.fill_events} vs {paired_base.fill_events} | "
                    f"Coverage: {net_edge.filled_runs}/{paired_count} "
                    f"({_format_fixed(net_coverage, 1)}%) vs "
                    f"{paired_base.filled_runs}/{paired_count} "
                    f"({_format_fixed(base_coverage, 1)}%)"
                ),
                (
                    f"W/L/D: {net_edge.wins}/{net_edge.losses}/{net_edge.draws} vs "
                    f"{paired_base.wins}/{paired_base.losses}/{paired_base.draws} | "
                    f"PnL: {_format_signed(net_edge.pnl)}U vs "
                    f"{_format_signed(paired_base.pnl)}U "
                    f"(Δ {_format_signed(net_edge.pnl - paired_base.pnl)}U)"
                ),
                (
                    f"Max DD: {_format_decimal(net_edge.max_drawdown, 4)}U vs "
                    f"{_format_decimal(paired_base.max_drawdown, 4)}U"
                ),
                "註：固定成本上限研究，非已校準 EV。",
            ]
        )
    _append_paired_section(
        lines,
        report,
        MOMENTUM30_V1_LANE,
        HISTORY30_CONTROL_V1_LANE,
        upto,
    )
    _append_paired_section(
        lines,
        report,
        HISTORY30_CONTROL_V1_LANE,
        BASE_V3_LANE,
        upto,
        caveat="註：History30 資料對照研究，非已校準 EV。",
    )
    _append_paired_section(
        lines,
        report,
        MOMENTUM30_V1_LANE,
        BASE_V3_LANE,
        upto,
    )
    _append_paired_section(
        lines,
        report,
        CONFIRM30_V1_LANE,
        BASE_V3_LANE,
        upto,
    )
    if gate is not None and report.gate_rejection_by_campaign:
        selected_campaigns = {
            record.campaign_id.split("::shadow::", 1)[0]
            for record in gate.runs[:upto]
            if not record.has_fill
        }
        reason_counts: dict[str, int] = {}
        for campaign_id in selected_campaigns:
            reason = report.gate_rejection_by_campaign.get(
                campaign_id, "unclassified/no gate event"
            )
            reason_counts[reason] = reason_counts.get(reason, 0) + 1
        top_reasons = sorted(reason_counts.items(), key=lambda item: (-item[1], item[0]))[:4]
        reason_labels = {
            "base V3 entry gate not met": "Base V3 尚未觸發進場",
            "ask_move and signed BTC margin outside gate": "Ask Move／BTC Margin 超出 Gate",
            "unclassified/no gate event": "未分類／沒有 Gate event",
        }
        lines.append(
            "🚧 <b>Gate 未成交原因</b>：" + " | ".join(
                f"{html.escape(reason_labels.get(reason, reason))}={count}"
                for reason, count in top_reasons
            )
        )
    if report.issues:
        lines.append(f"⚠️ <b>資料檢查失敗 ({len(report.issues)} 項)</b>")
        lines.extend(f"- {html.escape(issue)}" for issue in report.issues[:8])
        if len(report.issues) > 8:
            lines.append(html.escape(f"... 其餘 {len(report.issues) - 8} 項省略"))
    if upto > 0 and ranked_lanes:
        first_lane_runs = report.lanes[ranked_lanes[0]].runs
        if upto <= len(first_lane_runs):
            dt_ts = first_lane_runs[upto - 1].settled_at_ms
            report_dt = datetime.fromtimestamp(dt_ts / 1000, tz=timezone.utc)
        else:
            report_dt = datetime.fromtimestamp(report.baseline_ms / 1000, tz=timezone.utc)
    else:
        report_dt = datetime.fromtimestamp(report.baseline_ms / 1000, tz=timezone.utc)
    report_dt_tpe = report_dt.astimezone(TAIPEI_TZ)

    lines.extend(
        [
            "-------------------------------------------",
            f"🕒 結算時間: {report_dt_tpe.strftime('%Y-%m-%d %H:%M:%S')} (台北) | {report_dt.strftime('%H:%M:%S')} UTC",
        ]
    )
    return "\n".join(lines)


_TAG_RE = re.compile(r"<[^>]*>")


def chunk_html_message(text: str, *, limit: int = MAX_TELEGRAM_CHARS) -> list[str]:
    """Split on line boundaries and guarantee every chunk is <= Telegram's limit."""

    if limit <= 0:
        raise ValueError("limit must be positive")
    lines = str(text).splitlines() or [""]
    chunks: list[str] = []
    current = ""
    for raw_line in lines:
        line = raw_line
        if len(line) > limit:
            # A long arbitrary line may contain an HTML tag.  Strip formatting
            # before slicing so no chunk contains half an opening/closing tag.
            line = html.escape(html.unescape(_TAG_RE.sub("", line)))
        while len(line) > limit:
            if current:
                chunks.append(current)
                current = ""
            chunks.append(line[:limit])
            line = line[limit:]
        candidate = line if not current else f"{current}\n{line}"
        if len(candidate) <= limit:
            current = candidate
        else:
            chunks.append(current)
            current = line
    if current or not chunks:
        chunks.append(current)
    return chunks


def _state_fingerprint(
    db_path: Path,
    baseline_ms: int,
    lane_specs: Sequence[LaneSpec],
    target_runs: int,
    batch_size: int,
) -> str:
    payload = {
        "db_path": str(db_path.resolve()),
        "baseline_ms": baseline_ms,
        "target_runs": target_runs,
        "batch_size": batch_size,
        "lanes": {spec.name: spec.as_dict() for spec in lane_specs},
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _load_checkpoint(path: Path, fingerprint: str) -> dict[str, Any]:
    if not path.exists():
        return {"version": 1, "fingerprint": fingerprint, "batches": {}}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise CheckpointError(f"checkpoint is unreadable: {path}") from exc
    if not isinstance(payload, Mapping) or payload.get("version") != 1:
        raise CheckpointError("checkpoint version is unsupported")
    if payload.get("fingerprint") != fingerprint:
        raise CheckpointError("checkpoint scope does not match frozen baseline/lane configuration")
    batches = payload.get("batches")
    if not isinstance(batches, Mapping):
        raise CheckpointError("checkpoint batches are invalid")
    return dict(payload)


def _atomic_write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_name, path)
        if os.name != "nt":
            fd_dir = os.open(str(path.parent), os.O_RDONLY)
            try:
                os.fsync(fd_dir)
            finally:
                os.close(fd_dir)
    except Exception:
        with contextlib.suppress(FileNotFoundError):
            os.unlink(temp_name)
        raise


@contextlib.contextmanager
def _checkpoint_lock(path: Path) -> Iterator[None]:
    """Serialize cron invocations so two processes cannot send one batch twice."""

    lock_path = Path(f"{path}.lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+b") as handle:
        if os.name == "nt":
            import msvcrt

            handle.seek(0)
            handle.write(b"0")
            handle.flush()
            handle.seek(0)
            try:
                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            except OSError as exc:
                raise CheckpointError("another report invocation holds the checkpoint lock") from exc
            try:
                yield
            finally:
                handle.seek(0)
                with contextlib.suppress(OSError):
                    msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl

            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError as exc:
                raise CheckpointError("another report invocation holds the checkpoint lock") from exc
            try:
                yield
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def send_telegram_html(token: str, chat_id: str, message: str, *, opener: Callable[..., Any] | None = None) -> None:
    """Send one already-sized HTML message without exposing credentials on errors."""

    if not token or not chat_id:
        raise ConfigurationError("Telegram token and chat id are required")
    if len(message) > MAX_TELEGRAM_CHARS:
        raise NotificationError("Telegram message exceeds 4096 characters")
    body = urllib.parse.urlencode(
        {"chat_id": chat_id, "text": message, "parse_mode": "HTML", "disable_web_page_preview": "true"}
    ).encode()
    # Do not include the token in an exception, log line, or response message.
    request = urllib.request.Request(
        "https://api.telegram.org/bot" + urllib.parse.quote(token, safe="") + "/sendMessage",
        data=body,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )
    request_opener = opener or urllib.request.urlopen
    try:
        with request_opener(request, timeout=20) as response:
            raw = response.read()
        result = json.loads(raw.decode("utf-8"))
    except (urllib.error.URLError, urllib.error.HTTPError, OSError, ValueError) as exc:
        raise NotificationError("Telegram delivery failed") from exc
    if not isinstance(result, Mapping) or result.get("ok") is not True:
        raise NotificationError("Telegram rejected the report")


def _credentials_from_env(environ: Mapping[str, str] | None = None) -> tuple[str, str]:
    env = os.environ if environ is None else environ
    token = str(env.get(DEFAULT_BOT_TOKEN_ENV) or env.get("PREDICTION_TELEGRAM_BOT_TOKEN") or "").strip()
    chat_id = str(env.get(DEFAULT_CHAT_ID_ENV) or env.get("PREDICTION_TELEGRAM_CHAT_ID") or "").strip()
    missing = []
    if not token:
        missing.append(DEFAULT_BOT_TOKEN_ENV)
    if not chat_id:
        missing.append(DEFAULT_CHAT_ID_ENV)
    if missing:
        raise ConfigurationError("missing Telegram environment variable(s): " + ", ".join(missing))
    return token, chat_id


def run_monitor(
    db_path: str | Path,
    *,
    baseline_ms: int,
    lane_specs: Sequence[LaneSpec] | None = None,
    fallback_lane_specs: str | Mapping[str, Any] | None = None,
    checkpoint_path: str | Path,
    target_runs: int = DEFAULT_TARGET_RUNS,
    batch_size: int = DEFAULT_BATCH_SIZE,
    sender: Callable[[str], Any] | None = None,
    dry_run: bool = False,
) -> MonitorResult:
    """Build, validate, and publish each newly complete batch exactly once."""

    if batch_size <= 0 or target_runs < batch_size:
        raise ConfigurationError("target_runs must be >= positive batch_size")
    if target_runs % batch_size:
        raise ConfigurationError("target_runs must be divisible by batch_size")
    report = build_report(
        db_path,
        baseline_ms=baseline_ms,
        lane_specs=lane_specs,
        fallback_lane_specs=fallback_lane_specs,
        target_runs=target_runs,
    )
    checkpoint = Path(checkpoint_path).expanduser().resolve()
    if report.issues:
        return MonitorResult(report, (), (), checkpoint)
    if dry_run:
        return MonitorResult(report, (), (format_report(report),), None)
    if sender is None:
        token, chat_id = _credentials_from_env()

        def sender(message: str) -> None:
            send_telegram_html(token, chat_id, message)

    fingerprint = _state_fingerprint(report.db_path, report.baseline_ms, report.lane_specs, target_runs, batch_size)
    complete_batches = min(target_runs // batch_size, report.complete_batches(batch_size))
    sent_batches: list[int] = []
    rendered: list[str] = []
    with _checkpoint_lock(checkpoint):
        state = _load_checkpoint(checkpoint, fingerprint)
        batches = dict(state.get("batches", {}))
        for batch_number in range(1, complete_batches + 1):
            upto = batch_number * batch_size
            message = format_report(report, batch_number=batch_number, upto_runs=upto, batch_size=batch_size)
            chunks = chunk_html_message(message)
            chunk_hashes = [hashlib.sha256(chunk.encode("utf-8")).hexdigest() for chunk in chunks]
            key = str(batch_number)
            entry = batches.get(key)
            was_complete = bool(entry.get("complete")) if isinstance(entry, Mapping) else False
            if entry is None:
                entry = {"chunk_hashes": chunk_hashes, "sent_parts": [], "complete": False}
                batches[key] = entry
                state["batches"] = batches
                _atomic_write_json(checkpoint, state)
            elif not isinstance(entry, Mapping) or entry.get("chunk_hashes") != chunk_hashes:
                raise CheckpointError(f"checkpoint message changed for batch {batch_number}")
            sent_parts = {int(index) for index in entry.get("sent_parts", [])}
            for index, chunk in enumerate(chunks):
                if index in sent_parts:
                    continue
                sender(chunk)
                sent_parts.add(index)
                entry = dict(entry)
                entry["sent_parts"] = sorted(sent_parts)
                batches[key] = entry
                state["batches"] = batches
                _atomic_write_json(checkpoint, state)
                rendered.append(chunk)
            entry = dict(entry)
            entry["complete"] = True
            entry["sent_parts"] = sorted(sent_parts)
            batches[key] = entry
            state["batches"] = batches
            _atomic_write_json(checkpoint, state)
            if not was_complete and entry.get("complete"):
                sent_batches.append(batch_number)
    return MonitorResult(report, tuple(sent_batches), tuple(rendered), checkpoint)


def _env_or_none(name: str) -> str | None:
    value = os.environ.get(name)
    return value.strip() if value and value.strip() else None


def _load_env_file(path: str | Path) -> None:
    """Load simple KEY=VALUE entries without printing or overwriting process env."""

    env_path = Path(path).expanduser()
    if not env_path.is_file():
        return
    for raw_line in env_path.read_text(encoding="utf-8-sig").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].lstrip()
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key):
            continue
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        os.environ.setdefault(key, value)


def _default_env_file() -> Path:
    return Path(__file__).resolve().parents[1] / ".env"


def _baseline_from_env() -> str | None:
    return _env_or_none("PREDICTION_SHADOW_REPORT_BASELINE_MS") or _env_or_none("SHADOW_FROZEN_AFTER_MS")


def _baseline_from_database_manifest(db_path: str | Path) -> int | None:
    """Read the frozen lane baseline without loading any credential file."""

    path = Path(db_path).expanduser().resolve()
    if not path.is_file():
        return None
    uri = "file:" + urllib.parse.quote(str(path), safe="/:\\") + "?mode=ro"
    connection = sqlite3.connect(uri, uri=True)
    connection.row_factory = sqlite3.Row
    try:
        connection.execute("PRAGMA query_only=ON")
        specs = _runtime_lane_manifest(connection)
    finally:
        connection.close()
    if not specs:
        return None
    return min(int(spec.window_start_ms) for spec in specs)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default=None, help="SQLite path; defaults to PREDICTION_DB_PATH")
    parser.add_argument("--baseline-ms", type=int, default=None, help="frozen baseline; defaults to SHADOW_FROZEN_AFTER_MS")
    parser.add_argument("--lane-specs-json", default=None, help="lane scope JSON; defaults to SHADOW_LANE_SPECS_JSON")
    parser.add_argument("--checkpoint", default=None, help="sidecar checkpoint; defaults to SHADOW_REPORT_CHECKPOINT or <db>.shadow_report_checkpoint.json")
    parser.add_argument("--target-runs", type=int, default=DEFAULT_TARGET_RUNS)
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("--env-file", default=None, help="dotenv path; defaults to the project .env")
    parser.add_argument("--telegram", action="store_true", help="compatibility flag; delivery is the default unless --dry-run")
    parser.add_argument("--dry-run", action="store_true", help="print an offline report and do not require Telegram credentials")
    return parser


async def generate_report() -> str:
    """Compatibility entry point used by the Telegram /report command."""

    paired=_env_or_none('PREDICTION_PAIRED8_DB_PATH')
    if paired:
        from scripts.prediction_paired8_ledger import format_readonly_report
        return format_readonly_report(paired)
    # A systemd runtime already supplies its isolated DB path.  In that case,
    # never probe the project .env (which is intentionally hidden by the unit)
    # or stale legacy experiment env files.
    if not _env_or_none("PREDICTION_DB_PATH"):
        _load_env_file(_default_env_file())
        duel_env = Path("/home/jack_shih/cry3/prediction/data/v3_duel_shadow200.env")
        if duel_env.is_file():
            _load_env_file(duel_env)
        else:
            custom_env = Path("/home/jack_shih/cry3/prediction/data/v3_gate_shadow200.env")
            if custom_env.is_file():
                _load_env_file(custom_env)
    if _env_or_none("PREDICTION_ENV_FILE"):
        _load_env_file(_env_or_none("PREDICTION_ENV_FILE"))
    db_value = _env_or_none("PREDICTION_DB_PATH")
    if not db_value:
        raise ConfigurationError("database path is required (PREDICTION_DB_PATH)")
    baseline_value = _baseline_from_env()
    if baseline_value is None:
        baseline_value = _baseline_from_database_manifest(db_value)
    if baseline_value is None:
        raise ConfigurationError(
            "report baseline is required (runtime prediction_shadow_lanes manifest or SHADOW_FROZEN_AFTER_MS)"
        )
    report = build_report(
        db_value,
        baseline_ms=_as_int(baseline_value, "report baseline"),
        fallback_lane_specs=_env_or_none("SHADOW_LANE_SPECS_JSON"),
    )
    return format_report(report)


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    try:
        _load_env_file(_default_env_file())
        if args.env_file:
            _load_env_file(args.env_file)
        elif _env_or_none("PREDICTION_ENV_FILE"):
            _load_env_file(_env_or_none("PREDICTION_ENV_FILE"))
        db_value = args.db or _env_or_none("PREDICTION_DB_PATH")
        baseline_value = args.baseline_ms if args.baseline_ms is not None else _baseline_from_env()
        lane_value = args.lane_specs_json or _env_or_none("SHADOW_LANE_SPECS_JSON")
        if not db_value:
            raise ConfigurationError("database path is required (--db or PREDICTION_DB_PATH)")
        if baseline_value is None:
            baseline_value = _baseline_from_database_manifest(db_value)
        if baseline_value is None:
            raise ConfigurationError(
                "frozen baseline is required (--baseline-ms, SHADOW_FROZEN_AFTER_MS, or database lane manifest)"
            )
        baseline_ms = _as_int(baseline_value, "SHADOW_FROZEN_AFTER_MS")
        if not args.dry_run:
            _credentials_from_env()
        checkpoint = args.checkpoint or _env_or_none("SHADOW_REPORT_CHECKPOINT")
        if checkpoint is None:
            checkpoint = str(Path(db_value).expanduser().resolve()) + ".shadow_report_checkpoint.json"
        result = run_monitor(
            db_value,
            baseline_ms=baseline_ms,
            fallback_lane_specs=lane_value,
            checkpoint_path=checkpoint,
            target_runs=args.target_runs,
            batch_size=args.batch_size,
            dry_run=args.dry_run,
        )
        print(format_report(result.report))
        if result.report.issues:
            return 2
        if not args.dry_run and result.sent_batches:
            print(f"published batch(es): {','.join(str(value) for value in result.sent_batches)}")
        elif not args.dry_run:
            print("no new complete 20-run batch; checkpoint unchanged")
        return 0
    except ReporterError as exc:
        print(f"report_shadow_batches: {exc}", file=sys.stderr)
        return 2
    except (OSError, sqlite3.Error) as exc:
        print(f"report_shadow_batches: local I/O or SQLite failure: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

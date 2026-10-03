"""Reproducible SHA-256 identity for the untracked VM Prediction release."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path, PureWindowsPath
from typing import Iterable, Mapping


MANIFEST_SCHEMA = "prediction-release-v1"

# This is deliberately literal.  A target-side ``*.py``/``*.sql`` file may
# not become part of a release merely because a build happens to discover it.
# In particular, the locally dirty legacy config/core/Telegram files are not
# Prediction release inputs.
_REQUIRED_FIXED_RELEASE_PATHS = (
    'predict_main.py',
    'prediction/experiments/c180-original-mix75-v1-bda3e5a85a98/calibration.py',
    'prediction/experiments/c180-original-mix75-v1-bda3e5a85a98/candidate_engine.py',
    'prediction/experiments/c180-original-mix75-v1-bda3e5a85a98/candidate_report.py',
    'prediction/experiments/c180-original-mix75-v1-bda3e5a85a98/engine.py',
    'prediction/experiments/c180-original-mix75-v1-bda3e5a85a98/features.py',
    'prediction/experiments/c180-original-mix75-v1-bda3e5a85a98/frozen/jev/config.py',
    'prediction/experiments/c180-original-mix75-v1-bda3e5a85a98/frozen/jev/features.py',
    'prediction/experiments/c180-original-mix75-v1-bda3e5a85a98/frozen/jev/market.py',
    'prediction/experiments/c180-original-mix75-v1-bda3e5a85a98/frozen/jev/models.py',
    'prediction/experiments/c180-original-mix75-v1-bda3e5a85a98/frozen/prediction/client.py',
    'prediction/experiments/c180-original-mix75-v1-bda3e5a85a98/frozen/prediction/http_bounds.py',
    'prediction/experiments/c180-original-mix75-v1-bda3e5a85a98/frozen/prediction/loss_cooldown_guard.py',
    'prediction/experiments/c180-original-mix75-v1-bda3e5a85a98/frozen/prediction/models.py',
    'prediction/experiments/c180-original-mix75-v1-bda3e5a85a98/frozen/prediction/official_resolution.py',
    'prediction/experiments/c180-original-mix75-v1-bda3e5a85a98/frozen/prediction/rate_limit.py',
    'prediction/experiments/c180-original-mix75-v1-bda3e5a85a98/frozen/prediction/spot.py',
    'prediction/experiments/c180-original-mix75-v1-bda3e5a85a98/legacy_features.py',
    'prediction/experiments/c180-original-mix75-v1-bda3e5a85a98/live.py',
    'prediction/experiments/c180-original-mix75-v1-bda3e5a85a98/logic.py',
    'prediction/experiments/c180-original-mix75-v1-bda3e5a85a98/manifest.json',
    'prediction/experiments/c180-original-mix75-v1-bda3e5a85a98/original_logic.py',
    'prediction/experiments/c180-original-mix75-v1-bda3e5a85a98/package.py',
    'prediction/experiments/c180-original-mix75-v1-bda3e5a85a98/report.py',
    'prediction/experiments/c180-original-mix75-v1-bda3e5a85a98/store.py',
    'prediction/experiments/c180-original-mix75-v1-bda3e5a85a98/test_candidate.py',
    'prediction/experiments/c180-original-mix75-v1-bda3e5a85a98/test_v2.py',
    'prediction/experiments/c180-original-mix75-v1-bda3e5a85a98/train.py',
    'scripts/__init__.py',
    'scripts/build_t6_release.py',
    'scripts/resume_prediction_existing_loop.py',
    'scripts/prediction_paired8_ledger.py',
    'scripts/prediction_paired8_policy.py',
    'scripts/prediction_value9_parent_frozen.py',
    'scripts/report_shadow_batches.py',
    'scripts/run_t6_signal.sh',
    'src/gridbot/__init__.py',
    'src/gridbot/prediction/__init__.py',
    'src/gridbot/prediction/adaptive.py',
    'src/gridbot/prediction/c180_batch_gate.py',
    'src/gridbot/prediction/c180_evidence_collector.py',
    'src/gridbot/prediction/c180_favorite.py',
    'src/gridbot/prediction/c180_gate_runtime.py',
    'src/gridbot/prediction/c180_jev_client.py',
    'src/gridbot/prediction/c180_live_ledger.py',
    'src/gridbot/prediction/c180_signal_runtime.py',
    'src/gridbot/prediction/c180_signal_service.py',
    'src/gridbot/prediction/c180_worker_bridge.py',
    'src/gridbot/prediction/client.py',
    'src/gridbot/prediction/controller.py',
    'src/gridbot/prediction/evidence_retention.py',
    'src/gridbot/prediction/eth_t67c_policy.py',
    'src/gridbot/prediction/eth_t67c_core.py',
    'src/gridbot/prediction/eth_t67c_store.py',
    'src/gridbot/prediction/eth_t67c_data.py',
    'src/gridbot/prediction/eth_t67c_service.py',
    'src/gridbot/prediction/eth_t67c_telegram.py',
    'src/gridbot/prediction/jev_gate.py',
    'src/gridbot/prediction/http_bounds.py',
    'src/gridbot/prediction/live_report.py',
    'src/gridbot/prediction/late_fill_repair.py',
    'src/gridbot/prediction/loss_cooldown_guard.py',
    'src/gridbot/prediction/migrations/001_initial.sql',
    'src/gridbot/prediction/migrations/002_loops.sql',
    'src/gridbot/prediction/migrations/003_p0_state.sql',
    'src/gridbot/prediction/migrations/004_shadow_control.sql',
    'src/gridbot/prediction/migrations/005_live_control_promotion.sql',
    'src/gridbot/prediction/migrations/006_live_recovery_and_evidence.sql',
    'src/gridbot/prediction/migrations/007_shadow_finalization.sql',
    'src/gridbot/prediction/migrations/008_canonical_evidence.sql',
    'src/gridbot/prediction/migrations/009_exact_shadow_collection.sql',
    'src/gridbot/prediction/migrations/010_loop_strategy_binding.sql',
    'src/gridbot/prediction/migrations/011_moe_shadow_decisions.sql',
    'src/gridbot/prediction/migrations/011_shadow_observer.sql',
    'src/gridbot/prediction/migrations/012_moe_shadow_outcomes.sql',
    'src/gridbot/prediction/migrations/012_shadow_campaign_rollups.sql',
    'src/gridbot/prediction/migrations/013_moe_lineage_and_router_state.sql',
    'src/gridbot/prediction/migrations/014_moe_simulated_execution.sql',
    'src/gridbot/prediction/migrations/015_moe_official_provenance.sql',
    'src/gridbot/prediction/migrations/016_moe_append_only_evidence.sql',
    'src/gridbot/prediction/migrations/017_moe_book_age.sql',
    'src/gridbot/prediction/migrations/018_moe_router_scores.sql',
    'src/gridbot/prediction/migrations/019_moe_evidence_integrity.sql',
    'src/gridbot/prediction/migrations/020_moe_entry_identity_immutability.sql',
    'src/gridbot/prediction/migrations/021_moe_release_binding.sql',
    'src/gridbot/prediction/migrations/022_shadow_draw_resolution.sql',
    'src/gridbot/prediction/migrations/023_lane_attribution.sql',
    'src/gridbot/prediction/migrations/024_c180_live_gate.sql',
    'src/gridbot/prediction/migrations/025_regime_lane.sql',
    'src/gridbot/prediction/models.py',
    'src/gridbot/prediction/p3_lane_gate.py',
    'src/gridbot/prediction/r3_reversal_guard.py',
    'src/gridbot/prediction/rate_limit.py',
    'src/gridbot/prediction/regime_feature_service.py',
    'src/gridbot/prediction/regime_lane.py',
    'src/gridbot/prediction/regime_live_ledger.py',
    'src/gridbot/prediction/regime_t61_lane.py',
    'src/gridbot/prediction/regime_t62_lane.py',
    'src/gridbot/prediction/regime_t63_bridge.py',
    'src/gridbot/prediction/regime_t63_lane.py',
    'src/gridbot/prediction/regime_t63a_bridge.py',
    'src/gridbot/prediction/regime_t63a_lane.py',
    'src/gridbot/prediction/regime_t63b_bridge.py',
    'src/gridbot/prediction/regime_t63b_lane.py',
    'src/gridbot/prediction/regime_t63b_risk.py',
    'src/gridbot/prediction/regime_t65_lane.py',
    'src/gridbot/prediction/regime_t65_bridge.py',
    'src/gridbot/prediction/regime_t65_shadow.py',
    'src/gridbot/prediction/regime_t66_policy.py',
    'src/gridbot/prediction/regime_t66_observer.py',
    'src/gridbot/prediction/regime_t66_report.py',
    'src/gridbot/prediction/regime_t67_policy.py',
    'src/gridbot/prediction/regime_t67_lane.py',
    'src/gridbot/prediction/regime_t67_evidence.py',
    'src/gridbot/prediction/regime_t67_bridge.py',
    'src/gridbot/prediction/regime_t67_report.py',
    'src/gridbot/prediction/regime_t67a_policy.py',
    'src/gridbot/prediction/regime_t67a_bridge.py',
    'src/gridbot/prediction/regime_t67a_shadow.py',
    'src/gridbot/prediction/regime_t67a_report.py',
    'src/gridbot/prediction/regime_t67b_policy.py',
    'src/gridbot/prediction/regime_t67b_bridge.py',
    'src/gridbot/prediction/regime_t67b_shadow.py',
    'src/gridbot/prediction/regime_t67b_report.py',
    'src/gridbot/prediction/regime_t67c_policy.py',
    'src/gridbot/prediction/regime_t67c_bridge.py',
    'src/gridbot/prediction/regime_t67c_shadow.py',
    'src/gridbot/prediction/regime_t67c_report.py',
    'src/gridbot/prediction/regime_t68_policy.py',
    'src/gridbot/prediction/regime_t68a_policy.py',
    'src/gridbot/prediction/regime_t68_bridge.py',
    'src/gridbot/prediction/regime_t68a_bridge.py',
    'src/gridbot/prediction/regime_t68_shadow.py',
    'src/gridbot/prediction/regime_t68a_shadow.py',
    'src/gridbot/prediction/regime_t68_reference.py',
    'src/gridbot/prediction/regime_t68a_reference.py',
    'src/gridbot/prediction/regime_t68_report.py',
    'src/gridbot/prediction/regime_t68a_report.py',
    'src/gridbot/prediction/regime_worker_bridge.py',
    'src/gridbot/prediction/release.py',
    'src/gridbot/prediction/repository.py',
    'src/gridbot/prediction/risk.py',
    'src/gridbot/prediction/runtime.py',
    'src/gridbot/prediction/s3s5_pair.py',
    'src/gridbot/prediction/settings.py',
    'src/gridbot/prediction/spot.py',
    'src/gridbot/prediction/strategy.py',
    'src/gridbot/prediction/telegram.py',
    'src/gridbot/prediction/worker.py',
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def default_release_files(root: str | Path) -> list[Path]:
    base = Path(root).resolve()
    selected = [base / relative for relative in _REQUIRED_FIXED_RELEASE_PATHS]
    missing = [path.relative_to(base).as_posix() for path in selected if not path.is_file()]
    if missing:
        raise FileNotFoundError("reviewed Prediction release file(s) missing: " + ", ".join(missing))
    unexpected = _unexpected_target_runtime_paths(base)
    if unexpected:
        raise ValueError("unreviewed Prediction runtime file(s): " + ", ".join(unexpected))
    return sorted(selected, key=lambda item: item.relative_to(base).as_posix())


def _required_release_relative_paths(root: Path) -> set[str]:
    """Return the exact reviewed path set expected by every manifest."""

    return set(_REQUIRED_FIXED_RELEASE_PATHS)


def _unexpected_target_runtime_paths(root: Path) -> list[str]:
    """Find runtime Python/SQL files outside the reviewed literal set."""

    reviewed = set(_REQUIRED_FIXED_RELEASE_PATHS)
    candidates: set[str] = set()
    prediction_dir = root / "src" / "gridbot" / "prediction"
    if prediction_dir.is_dir():
        candidates.update(
            path.relative_to(root).as_posix()
            for path in prediction_dir.rglob("*")
            if path.is_file() and path.suffix.lower() in {".py", ".sql"}
        )
    return sorted(candidates - reviewed)


def _normalise_files(root: Path, files: Iterable[str | Path] | None) -> list[Path]:
    selected = default_release_files(root) if files is None else [
        (root / Path(item)) if not Path(item).is_absolute() else Path(item)
        for item in files
    ]
    result: list[Path] = []
    for path in selected:
        resolved = path.resolve()
        try:
            resolved.relative_to(root)
        except ValueError as exc:
            raise ValueError(f"release file is outside root: {path}") from exc
        if not resolved.is_file():
            raise FileNotFoundError(resolved)
        result.append(resolved)
    duplicate_paths = sorted(
        {
            path.relative_to(root).as_posix()
            for path in result
            if result.count(path) > 1
        }
    )
    if duplicate_paths:
        raise ValueError("release build contains duplicate path(s): " + ", ".join(duplicate_paths))
    unique = sorted(set(result), key=lambda item: item.relative_to(root).as_posix())
    reviewed = set(_REQUIRED_FIXED_RELEASE_PATHS)
    selected_relative = {item.relative_to(root).as_posix() for item in unique}
    unexpected = sorted(selected_relative - reviewed)
    if unexpected:
        raise ValueError("release build contains unreviewed path(s): " + ", ".join(unexpected))
    return unique


def build_release_manifest(root: str | Path, files: Iterable[str | Path] | None = None) -> dict[str, object]:
    base = Path(root).resolve()
    entries = [
        {"path": path.relative_to(base).as_posix(), "sha256": _sha256(path)}
        for path in _normalise_files(base, files)
    ]
    canonical = json.dumps(entries, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return {
        "schema": MANIFEST_SCHEMA,
        "files": entries,
        "release_fingerprint": hashlib.sha256(canonical.encode("utf-8")).hexdigest(),
    }


def _read_pin(pin_path: str | Path) -> str:
    path = Path(pin_path)
    text = path.read_text(encoding="utf-8").strip()
    if "=" in text:
        values = {}
        for line in text.splitlines():
            if "=" in line:
                key, value = line.split("=", 1)
                values[key.strip()] = value.strip().strip('"').strip("'")
        text = values.get("PREDICTION_EXPECTED_RELEASE_FINGERPRINT", "")
    return text.strip()


def verify_release_manifest(
    root: str | Path,
    manifest: Mapping[str, object],
    *,
    expected_fingerprint: str | None = None,
    pin_path: str | Path | None = None,
) -> tuple[str, ...]:
    base = Path(root).resolve()
    reasons: list[str] = []
    if str(manifest.get("schema") or "") != MANIFEST_SCHEMA:
        reasons.append("release manifest schema is invalid")
    raw_files = manifest.get("files")
    if not isinstance(raw_files, list):
        return ("release manifest files are invalid",)
    entries: list[dict[str, str]] = []
    seen_paths: set[str] = set()
    for raw in raw_files:
        if not isinstance(raw, Mapping):
            reasons.append("release manifest contains an invalid file entry")
            continue
        relative = str(raw.get("path") or "")
        expected = str(raw.get("sha256") or "")
        if not relative or not expected:
            reasons.append("release manifest file identity is incomplete")
            continue
        normalized_relative = relative
        if ("\\" in relative or relative.startswith("/") or PureWindowsPath(relative).drive
                or any(part in ("", ".", "..") for part in relative.split("/"))):
            reasons.append(f"release file path is not canonical relative: {relative}")
            continue
        unresolved = base / relative
        if any(path.is_symlink() for path in (unresolved, *unresolved.parents) if path != base):
            reasons.append(f"release file path contains symlink: {relative}")
            continue
        path = unresolved.resolve()
        try:
            path.relative_to(base)
        except ValueError:
            reasons.append(f"release file is outside root: {relative}")
            continue
        if normalized_relative in seen_paths:
            reasons.append(f"release manifest contains duplicate path: {normalized_relative}")
            continue
        seen_paths.add(normalized_relative)
        entries.append({"path": normalized_relative, "sha256": expected})
        if not path.is_file():
            reasons.append(f"release file is missing: {normalized_relative}")
            continue
        actual = _sha256(path)
        if actual != expected:
            reasons.append(f"release hash mismatch: {normalized_relative}")
    required = _required_release_relative_paths(base)
    for unexpected in _unexpected_target_runtime_paths(base):
        reasons.append(f"unreviewed Prediction runtime path exists: {unexpected}")
    listed = set(seen_paths)
    for missing in sorted(required - listed):
        reasons.append(f"release manifest is missing required path: {missing}")
    for unexpected in sorted(listed - required):
        reasons.append(f"release manifest contains unexpected path: {unexpected}")
    canonical = json.dumps(entries, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    computed_fingerprint = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    if str(manifest.get("release_fingerprint") or "") != computed_fingerprint:
        reasons.append("release fingerprint mismatch")
    pin = str(expected_fingerprint or "").strip()
    if pin_path is not None:
        try:
            pin = _read_pin(pin_path)
        except OSError:
            pin = ""
            reasons.append("external release fingerprint pin cannot be read")
    if not pin:
        reasons.append("external release fingerprint pin is missing")
    elif str(manifest.get("release_fingerprint") or "").strip() != pin:
        reasons.append("release fingerprint does not match external pin")
    return tuple(dict.fromkeys(reasons))


__all__ = ["MANIFEST_SCHEMA", "build_release_manifest", "default_release_files", "verify_release_manifest"]

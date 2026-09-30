"""Check migration boundaries and independently reproducible release inputs."""
from pathlib import Path
from tempfile import TemporaryDirectory

from src.gridbot.prediction.release import build_release_manifest, verify_release_manifest
from src.gridbot.prediction.strategy import StrategyConfig
from src.gridbot.prediction.worker import PredictionWorker

ROOT = Path(__file__).resolve().parents[1]


def test_release_inventory_and_external_pin_enforced():
    manifest = build_release_manifest(ROOT)
    fingerprint = manifest["release_fingerprint"]
    assert not verify_release_manifest(ROOT, manifest, expected_fingerprint=fingerprint)
    assert verify_release_manifest(ROOT, manifest, expected_fingerprint="wrong")
    with TemporaryDirectory() as directory:
        fake_root = Path(directory)
        assert verify_release_manifest(fake_root, manifest, expected_fingerprint=fingerprint)


def test_latest_t63_variants_are_selectable_and_configurable():
    for profile in ("regime_target6_3a_v1", "regime_target6_3b_v1"):
        assert profile in PredictionWorker._selectable_strategy_profiles()
        assert StrategyConfig.for_profile(profile).profile == profile


def test_frozen_jev_source_inventory():
    # Load in a subprocess so frozen top-level imports cannot contaminate tests.
    import subprocess
    import sys

    source = ROOT / "prediction/experiments/c180-original-mix75-v1-bda3e5a85a98"
    result = subprocess.run(
        [sys.executable, "-c", "from src.gridbot.prediction.c180_signal_runtime import load_frozen_source; import sys; load_frozen_source(sys.argv[1])", str(source)],
        cwd=ROOT, capture_output=True, text=True, timeout=30,
    )
    assert result.returncode == 0, result.stderr

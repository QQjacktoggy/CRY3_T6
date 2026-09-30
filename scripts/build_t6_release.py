"""Generate a manifest and external pin for this independent checkout."""
import json
from pathlib import Path
from src.gridbot.prediction.release import build_release_manifest, verify_release_manifest

if __name__ == "__main__":
    root = Path(__file__).resolve().parents[1]
    manifest = build_release_manifest(root)
    directory = root / "prediction"
    directory.mkdir(exist_ok=True)
    (directory / "release-manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    pin = directory / "release-pin.env"
    pin.write_text("PREDICTION_EXPECTED_RELEASE_FINGERPRINT=" + manifest["release_fingerprint"] + "\n")
    assert not verify_release_manifest(root, manifest, pin_path=pin)
    print(manifest["release_fingerprint"])

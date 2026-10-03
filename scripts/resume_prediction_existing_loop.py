"""One-use operator startup for an exact existing Live loop; never a new loop."""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
from pathlib import Path
import stat
import sys
import time


def validate_authorization(proof, loop, *, now_ms, profile, unit, fingerprint):
    if (not isinstance(proof, dict) or not isinstance(loop, dict)
            or not proof.get('loop_id') or loop.get('loop_id') != proof['loop_id']
            or loop.get('state') != 'RUNNING' or loop.get('mode') != 'LIVE'
            or loop.get('new_entries_stopped') or loop.get('hard_stop_latched')
            or loop.get('strategy_profile') != proof.get('profile') or profile != proof.get('profile')
            or str(unit) != str(proof.get('unit')) or fingerprint != proof.get('fingerprint')
            or int(loop.get('target', 0)) != int(proof.get('target', -1))
            or int(loop.get('completed', -1)) != int(proof.get('completed', -2))
            or not 0 <= int(loop.get('completed', -1)) < int(loop.get('target', 0))
            or not int(proof.get('issued_at_ms', 0)) <= now_ms <= int(proof.get('expires_at_ms', 0))
            or int(proof.get('expires_at_ms', 0))-int(proof.get('issued_at_ms', 0)) > 600_000):
        raise ValueError('existing-loop authorization does not match')


async def run(root: Path, authorization: Path):
    # The normal application graph and preflight own all Live authority.
    os.chdir(root)
    sys.path.insert(0, str(root))
    from predict_main import build_prediction_components, close_prediction_components, _poll_telegram
    from src.gridbot.prediction.release import verify_release_manifest

    fd = os.open(authorization, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(fd) as stream:
        info = os.fstat(stream.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
            raise ValueError('operator authorization permissions invalid')
        proof = json.load(stream)
    manifest = json.loads((root/'prediction/release-manifest.json').read_text())
    if verify_release_manifest(root, manifest, pin_path=root/'prediction/release-pin.env'):
        raise ValueError('deployed release verification failed')
    components = await build_prediction_components(dotenv_path=root/'prediction/live.env')
    try:
        worker = components.runtime.worker
        await worker.restore_order_unit()
        await worker.restore_selected_strategy()
        loop = await components.repository.get_active_loop()
        validate_authorization(proof, loop, now_ms=int(time.time()*1000),
            profile=worker._selected_strategy_profile, unit=worker._selected_order_unit_usdt,
            fingerprint=manifest['release_fingerprint'])
        if not components.settings.is_live_requested:
            raise ValueError('Live configuration required')
        # Consume before any authority transition. A restart cannot replay it.
        os.rename(authorization, authorization.with_name(authorization.name+'.consumed'))
        directory = os.open(authorization.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
        result = await components.runtime.set_shadow_mode(False)
        if result.get('action_denied') or not result.get('live_armed'):
            raise ValueError('normal Live preflight denied recovery')
        result = await components.runtime.resume_existing_loop(proof['loop_id'], int(proof['target']))
        if result.get('action_denied'):
            raise ValueError('existing-loop recovery denied')
        current = await components.repository.get_active_loop()
        if not current or current['loop_id'] != proof['loop_id']:
            raise ValueError('existing-loop recovery identity changed')
        logging.getLogger(__name__).warning('operator_existing_loop_resumed loop_id=%s target=%s',
                                          proof['loop_id'], proof['target'])
        await worker.start_shadow_observer()
        if not components.telegram.enabled or components.telegram.application is None:
            raise ValueError('normal Telegram control plane unavailable')
        await _poll_telegram(components.telegram.application)
    finally:
        await close_prediction_components(components)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--authorization', type=Path, required=True)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    try:
        asyncio.run(run(args.root.resolve(), args.authorization))
    except Exception as exc:
        # Never expose signed request URLs, environment, or request bodies.
        logging.getLogger(__name__).error('operator_existing_loop_resume_failed error_type=%s', type(exc).__name__)
        raise SystemExit(2)

"""Trusted, stdlib-only installer verification; never import candidate Python.

Ship this file beside the reviewed operator installer, outside the staged tree.
The manifest/pin remain release identity checks, not a publisher signature: the
operator must obtain the installer and release fingerprint through a trusted path.
"""
import ast
import hashlib
import json
from pathlib import Path, PurePosixPath, PureWindowsPath

RELEASE = 'src/gridbot/prediction/release.py'


def safe_path(root, relative):
    """Require a canonical relative path with no symlink component."""
    if (not isinstance(relative, str) or not relative or '\\' in relative
            or PureWindowsPath(relative).drive or relative.startswith('/')
            or any(part in ('', '.', '..') for part in relative.split('/'))
            or PurePosixPath(relative).as_posix() != relative):
        raise RuntimeError('Unsafe release path: ' + str(relative))
    base = Path(root).resolve()
    path = base
    for part in relative.split('/'):
        path = path / part
        if path.is_symlink():
            raise RuntimeError('Symlink release path: ' + relative)
    if not path.resolve().is_relative_to(base):
        raise RuntimeError('Release path escapes root: ' + relative)
    return path


def _pin(text):
    text = text.strip()
    if '=' not in text:
        return text
    return dict(line.split('=', 1) for line in text.splitlines() if '=' in line).get(
        'PREDICTION_EXPECTED_RELEASE_FINGERPRINT', '').strip().strip('"').strip("'")


def verify_release(root, manifest, *, pin_text):
    """Validate identity and inventory, returning the exact verified bytes."""
    if not isinstance(manifest, dict) or manifest.get('schema') != 'prediction-release-v1':
        raise RuntimeError('Release manifest schema is invalid')
    rows = manifest.get('files')
    if not isinstance(rows, list) or not rows:
        raise RuntimeError('Release manifest files are invalid')
    contents, entries = {}, []
    for row in rows:
        if not isinstance(row, dict) or set(row) != {'path', 'sha256'}:
            raise RuntimeError('Release manifest entry is invalid')
        relative, expected = row['path'], row['sha256']
        path = safe_path(root, relative)
        if relative in contents:
            raise RuntimeError('Duplicate release path: ' + relative)
        if not path.is_file():
            raise RuntimeError('Release file missing: ' + relative)
        content = path.read_bytes()
        if hashlib.sha256(content).hexdigest() != expected:
            raise RuntimeError('Release hash mismatch: ' + relative)
        entries.append(row)
        contents[relative] = content
    canonical = json.dumps(entries, ensure_ascii=False, sort_keys=True, separators=(',', ':'))
    fingerprint = hashlib.sha256(canonical.encode()).hexdigest()
    if fingerprint != manifest.get('release_fingerprint') or fingerprint != _pin(pin_text):
        raise RuntimeError('Release fingerprint/pin mismatch')
    # Inventory is literal data from already hash-verified bytes. Parsing it
    # never runs top-level code, imports, decorators, or executable expressions.
    if RELEASE not in contents:
        raise RuntimeError('Release inventory source missing')
    try:
        assignments = [node.value for node in ast.parse(contents[RELEASE]).body
                       if isinstance(node, ast.Assign) and any(
                           isinstance(target, ast.Name) and target.id == '_REQUIRED_FIXED_RELEASE_PATHS'
                           for target in node.targets)]
        inventory = ast.literal_eval(assignments[0]) if len(assignments) == 1 else None
        if (not isinstance(inventory, (tuple, list)) or not all(isinstance(p, str) for p in inventory)
                or len(set(inventory)) != len(inventory) or set(inventory) != set(contents)):
            raise ValueError('inventory differs')
    except (ValueError, TypeError, SyntaxError, IndexError) as exc:
        raise RuntimeError('Release inventory is invalid') from exc
    runtime = Path(root) / 'src/gridbot/prediction'
    for path in runtime.rglob('*'):
        if path.suffix.lower() in {'.py', '.sql'} and path.is_file():
            relative = path.relative_to(root).as_posix()
            safe_path(root, relative)
            if relative not in contents:
                raise RuntimeError('Unreviewed runtime path: ' + relative)
    return contents


def validate_candidate(root, stage, candidate, old, new):
    """Only the complete manifest delta may be installed; no extra copy paths."""
    if set(old) - set(new):
        raise RuntimeError('Installer does not support removing release files')
    rows = candidate.get('files')
    if not isinstance(rows, list):
        raise RuntimeError('Candidate files are invalid')
    seen = set()
    for row in rows:
        if not isinstance(row, dict) or set(row) != {'path', 'before', 'after'}:
            raise RuntimeError('Candidate entry is invalid')
        relative = row['path']
        dest = safe_path(root, relative)
        safe_path(stage, relative)
        if relative in seen or relative not in new:
            raise RuntimeError('Duplicate or unverified candidate path: ' + relative)
        seen.add(relative)
        before = hashlib.sha256(old[relative]).hexdigest() if relative in old else None
        after = hashlib.sha256(new[relative]).hexdigest()
        if row['before'] != before or row['after'] != after:
            raise RuntimeError('Candidate hash differs from verified manifest: ' + relative)
        if relative not in old and dest.exists():
            raise RuntimeError('New release path already exists: ' + relative)
    changed = {relative for relative, content in new.items() if old.get(relative) != content}
    if seen != changed:
        raise RuntimeError('Candidate files differ from verified manifest delta')

"""Compute the T6.9 STAGE overlay and its fingerprint offline, from VM hashes only.

Inputs are the VM's live ``prediction/release-manifest.json`` (paths and
sha256 only) and its ``src/gridbot/prediction/release.py`` (copied read-only).
Every manifest path whose VM hash differs from ``--ref`` must match some
earlier version of that path in ``--ref``'s history, otherwise the VM has an
unreviewed local edit and this refuses. The VM ``release.py`` stays the base.
Of the paths ``--ref`` lists and the VM does not, only new runtime modules under
``src/gridbot/prediction/`` are appended by default (the VM verifier rejects any
unlisted runtime file there). Others, such as the c180 experiment directory that
already exists on the VM outside the manifest and that the installer would
refuse to overwrite, are reported as ``not_added`` unless named with ``--add``.
``--keep-out`` drops a runtime module. ``report.json`` lists every path that
differs between the VM and ``--ref`` for per-file review before approval.

The output is the overlay, ``candidate.json`` and the expected manifest and
fingerprint. The VM session builds STAGE from the same overlay and computes the
fingerprint with STAGE's own ``release.py``; jack approves only when both agree.
"""
import argparse
import ast
import hashlib
import json
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'deploy'))
from release_verifier import RELEASE, safe_path  # noqa: E402

VERSION = '6.9.2'
RUNTIME = 'src/gridbot/prediction/'


def sha(content):
    return hashlib.sha256(content).hexdigest()


def inventory(source):
    """Parse the literal inventory without running the file; return (paths, closing line index)."""
    nodes = [node for node in ast.parse(source).body if isinstance(node, ast.Assign) and any(
        isinstance(t, ast.Name) and t.id == '_REQUIRED_FIXED_RELEASE_PATHS' for t in node.targets)]
    if len(nodes) != 1:
        raise RuntimeError('release.py must assign _REQUIRED_FIXED_RELEASE_PATHS exactly once')
    paths = ast.literal_eval(nodes[0].value)
    if not isinstance(paths, (tuple, list)) or len(set(paths)) != len(paths):
        raise RuntimeError('release.py inventory is invalid')
    return list(paths), nodes[0].end_lineno - 1


def git(repo, *args):
    return subprocess.run(['git', '-C', str(repo), *args], check=True, capture_output=True).stdout


def history_hashes(repo, ref, path):
    """sha256 of every version of ``path`` reachable from ``ref``."""
    found, blobs = set(), set()
    for commit in git(repo, 'log', '--format=%H', ref, '--', path).decode().split():
        try:
            blob = git(repo, 'rev-parse', f'{commit}:{path}').decode().strip()
        except subprocess.CalledProcessError:
            continue  # the commit deleted the path
        if blob not in blobs:
            blobs.add(blob)
            found.add(sha(git(repo, 'cat-file', 'blob', blob)))
    return found


def patch_release(source, additions):
    """Append paths before the inventory's closing parenthesis, in the VM file's own style."""
    paths, closing = inventory(source)
    if not additions:
        return source
    text = source.decode()
    newline = '\r\n' if '\r\n' in text else '\n'
    lines = text.split(newline)
    last = lines[closing - 1]
    indent = last[:len(last) - len(last.lstrip())]
    quote = '"' if last.lstrip().startswith('"') else "'"
    if not last.rstrip().endswith(','):
        raise RuntimeError('Last inventory entry must end with a comma')
    lines[closing:closing] = [f'{indent}{quote}{path}{quote},' for path in additions]
    patched = newline.join(lines).encode()
    if inventory(patched)[0] != paths + list(additions):
        raise RuntimeError('Patched inventory differs from the plan')
    return patched


def build(repo, ref, vm_manifest, vm_release, keep_out=(), add=()):
    rows = vm_manifest['files']
    vm = {row['path']: row['sha256'] for row in rows}
    if len(vm) != len(rows) or RELEASE not in vm:
        raise RuntimeError('VM manifest is invalid')
    if sha(vm_release) != vm[RELEASE]:
        raise RuntimeError('VM release.py differs from the VM manifest')
    vm_inventory, _ = inventory(vm_release)
    if set(vm_inventory) != set(vm):
        raise RuntimeError('VM release.py inventory differs from the VM manifest paths')
    main_inventory, _ = inventory(git(repo, 'show', f'{ref}:{RELEASE}'))
    fresh = [p for p in main_inventory if p not in vm]
    unknown = sorted((set(keep_out) | set(add)) - set(fresh))
    if unknown:
        raise RuntimeError('--keep-out/--add must name paths new in the ref: ' + ', '.join(unknown))
    additions = [p for p in fresh if p not in keep_out and (p.startswith(RUNTIME) or p in add)]
    not_added = [p for p in fresh if p not in additions]
    overlay, problems, vm_only = {}, [], []
    for path in vm_inventory + additions:
        safe_path(repo, path)
        if path == RELEASE:
            continue
        if path not in main_inventory:
            vm_only.append(path)  # stays as the VM has it
            continue
        content = git(repo, 'show', f'{ref}:{path}')
        if vm.get(path) == sha(content):
            continue
        if path in vm and vm[path] not in history_hashes(repo, ref, path):
            problems.append(path)
        overlay[path] = content
    if problems:
        raise RuntimeError('VM bytes are not in repository history (unreviewed VM edit): ' + ', '.join(problems))
    overlay[RELEASE] = patch_release(vm_release, additions)
    new = dict(vm)
    new.update({path: sha(content) for path, content in overlay.items()})
    entries = sorted(({'path': p, 'sha256': h} for p, h in new.items()), key=lambda r: r['path'])
    canonical = json.dumps(entries, ensure_ascii=False, sort_keys=True, separators=(',', ':'))
    manifest = {'schema': vm_manifest['schema'], 'files': entries,
                'release_fingerprint': sha(canonical.encode())}
    changed = sorted(p for p in overlay if new[p] != vm.get(p))
    candidate = {'version': VERSION, 'parent': vm_manifest['release_fingerprint'],
                 'expected_fingerprint': manifest['release_fingerprint'],
                 'files': [{'path': p, 'before': vm.get(p), 'after': new[p]} for p in changed]}
    report = {'ref': git(repo, 'rev-parse', ref).decode().strip(), 'parent': candidate['parent'],
              'fingerprint': manifest['release_fingerprint'], 'files': len(entries), 'changed': changed,
              'added': additions, 'not_added': not_added, 'vm_only': vm_only}
    return {p: overlay[p] for p in changed}, candidate, manifest, report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--vm-manifest', required=True, help="Copy of the VM's prediction/release-manifest.json")
    parser.add_argument('--vm-release', required=True, help="Copy of the VM's src/gridbot/prediction/release.py")
    parser.add_argument('--output', required=True, help='New directory for overlay/, candidate.json and the manifest')
    parser.add_argument('--ref', default='origin/main')
    parser.add_argument('--keep-out', action='append', default=[], help='New ref path not to add')
    parser.add_argument('--add', action='append', default=[], help='New ref path outside src/gridbot/prediction to add')
    args = parser.parse_args(argv)
    repo = Path(__file__).resolve().parents[1]
    overlay, candidate, manifest, report = build(
        repo, args.ref, json.loads(Path(args.vm_manifest).read_text()), Path(args.vm_release).read_bytes(),
        tuple(args.keep_out), tuple(args.add))
    out = Path(args.output)
    out.mkdir(parents=True)
    for path, content in overlay.items():
        dest = out / 'overlay' / path
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(content)
    (out / 'candidate.json').write_text(json.dumps(candidate, indent=2) + '\n')
    (out / 'release-manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
    (out / 'release-pin.env').write_text('PREDICTION_EXPECTED_RELEASE_FINGERPRINT=' + manifest['release_fingerprint'] + '\n')
    (out / 'report.json').write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    main()

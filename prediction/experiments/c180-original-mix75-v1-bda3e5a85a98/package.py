"""Freeze a standalone, reproducible paper-only experiment bundle."""
import hashlib
import json
import pathlib
import tarfile
root = pathlib.Path(__file__).parent
files = {str(p.relative_to(root)).replace('\\', '/'): hashlib.sha256(p.read_bytes()).hexdigest()
         for p in root.rglob('*.py') if '__pycache__' not in p.parts}
(root/'manifest.json').write_text(json.dumps({'version': 'c180-original-mix75-v1', 'files': files}, sort_keys=True, indent=2), encoding='utf-8')
with tarfile.open(root.parent/'c180-original-mix75-v1.tar.gz', 'w:gz') as archive:
    for name in [*files, 'manifest.json']:
        archive.add(root/name, arcname='c180-original-mix75-v1/'+name)
print('packaged', len(files), 'files')

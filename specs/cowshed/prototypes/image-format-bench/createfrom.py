#!/usr/bin/env python3
"""`diskutil image create from` as an ASIF rewrite: fragment an image (fill, four clones held, three churn rounds),
then compare cowshed's plain pread/pwrite copy with `diskutil image create from <image> <copy>` — time, extents,
allocation, and whether the copy is the same volume (fsck, case sensitivity, owner, content digests).

usage: PROTO_ROOT=<dir> createfrom.py A"""
import hashlib
import json
import os
import sys
import time

sys.argv = [sys.argv[0], sys.argv[1], 'cfrom']
HERE = os.path.dirname(os.path.abspath(__file__))
exec(open(os.path.join(HERE, 'run.py')).read().split('\ndef main():')[0])


def digest(mnt):
    h = hashlib.sha256()
    for root, dirs, files in os.walk(f'{mnt}/ds'):
        dirs.sort()
        for name in sorted(files + [d for d in dirs if os.path.islink(os.path.join(root, d))]):
            path = os.path.join(root, name)
            h.update(os.path.relpath(path, mnt).encode())
            if os.path.islink(path):
                h.update(b'L' + os.readlink(path).encode())
            elif '/target/' in path:
                with open(path, 'rb') as f:
                    for chunk in iter(lambda: f.read(1 << 20), b''):
                        h.update(chunk)
            else:
                h.update(str(os.path.getsize(path)).encode())
    return h.hexdigest()


def inspect(image, mnt):
    whole, att = attach(image, mnt)
    st = os.stat(mnt)
    r = {'attach_total_ms': att['attach_total_ms'], 'case_sensitive': os.pathconf(mnt, 11),
         'root_owned_by_user': st.st_uid == os.getuid(), 'digest': digest(mnt)}
    detach(whole)
    return r


shutil.rmtree(WORK, ignore_errors=True)
os.makedirs(WORK)
create()
fill()
for i in range(HELD):
    clonefile(IMG, f'{WORK}/held{i}.{EXT}')
for r in range(1, ROUNDS + 1):
    churn(r)
x = extents(IMG)
res = {'source_extents': int(x['extents']), 'source_alloc_gib': round(x['alloc_bytes'] / 2**30, 3)}
res['source'] = inspect(IMG, MNT)

plain = f'{WORK}/plain.{EXT}'
_, o = run([XT, 'copy', IMG, plain])
cp = kv(o)
x = extents(plain)
res['plain_copy'] = {'ms': cp['copy_ms'], 'extents': int(x['extents']), 'alloc_gib': round(x['alloc_bytes'] / 2**30, 3)}
os.unlink(plain)

conv = f'{WORK}/converted.{EXT}'
ms, _ = run(['diskutil', 'image', 'create', 'from', IMG, conv])
x = extents(conv)
res['create_from'] = {'ms': ms, 'extents': int(x['extents']), 'alloc_gib': round(x['alloc_bytes'] / 2**30, 3)}
os.makedirs(f'{WORK}/mconv', exist_ok=True)
res['create_from'].update(inspect(conv, f'{WORK}/mconv'))
fresh = f'{WORK}/fresh-clone.{EXT}'
clonefile(conv, fresh)
_, o = run([XT, 'firstwrite', fresh])
res['create_from']['firstwrite_ms'] = kv(o)['firstwrite_ms']
res['same_content'] = res['create_from']['digest'] == res['source']['digest']
print(json.dumps(res, indent=1), flush=True)
with open(f'{WORK}.json', 'w') as f:
    json.dump(res, f, indent=1)
shutil.rmtree(WORK)

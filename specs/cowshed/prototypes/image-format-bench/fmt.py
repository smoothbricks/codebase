#!/usr/bin/env python3
"""Format-specific checks per candidate: lifecycle medians, space reclaim, resize, attached locking,
clone case sensitivity. usage: PROTO_ROOT=<dir> fmt.py <A|B|C>"""
import ctypes
import fcntl
import json
import os
import shutil
import statistics
import subprocess
import sys
import time

sys.argv = [sys.argv[0], sys.argv[1], 'fmt']
exec(open(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'run.py')).read().split('\ndef main():')[0])

out = {'fmt': fmt}


def alloc(path):
    return round(os.stat(path).st_blocks * 512 / 2**20, 1)


def trial(cmd):
    p = subprocess.run(cmd, capture_output=True)
    return p.returncode, (p.stdout + p.stderr).decode(errors='replace').strip()[-300:]


shutil.rmtree(WORK, ignore_errors=True)
os.makedirs(WORK)

# 1. Lifecycle medians on empty images.
creates, attaches, detaches = [], [], []
for i in range(5):
    img = f'{WORK}/life{i}.{EXT}'
    globals()['IMG'] = img
    creates.append(create()['create_ms'])
    whole, att = attach(img, f'{WORK}/mlife')
    attaches.append(att['attach_total_ms'])
    detaches.append(detach(whole))
    t = time.monotonic()
    clonefile(img, img + '.c.' + EXT)
    os.unlink(img + '.c.' + EXT)
    os.unlink(img)
out['create_ms'] = creates
out['attach_total_ms'] = attaches
out['detach_ms'] = detaches
out['median'] = {'create_ms': statistics.median(creates),
                 'attach_total_ms': statistics.median(attaches),
                 'detach_ms': statistics.median(detaches)}
if fmt == 'B':
    lit = []
    for i in range(5):
        p = f'{WORK}/lit{i}.asif'
        ms, _ = run(['diskutil', 'image', 'create', 'blank', '--format', 'ASIF', '--size', '100g',
                     '--volumeName', 'lit', '--fs', 'APFS', p])
        lit.append(ms)
        os.unlink(p)
    out['literal_fs_APFS_create_ms'] = lit
print(json.dumps(out), flush=True)

# 2. Repeated I/O on a fresh image (medians of 5), then space reclaim after deleting data inside it.
IMG = f'{WORK}/reclaim.{EXT}'
globals()['IMG'] = IMG
create()
whole, _ = attach(IMG, MNT)
reps = []
for i in range(5):
    os.makedirs(f'{MNT}/small')
    small = kv(run([XT, 'small', f'{MNT}/small', '20000'])[1])
    shutil.rmtree(f'{MNT}/small')
    seq = kv(run([XT, 'seq', f'{MNT}/seq.bin', '1024'])[1])
    os.unlink(f'{MNT}/seq.bin')
    reps.append({**small, **seq})
out['io_median'] = {k: statistics.median(r[k] for r in reps) for k in
                    ('create_ops', 'symlink_ops', 'unlink_ops', 'seq_write_MBps', 'seq_read_MBps')}
print(json.dumps(out['io_median']), flush=True)
run([XT, 'seq', f'{MNT}/blob', '2048'])
detach(whole)
a1 = alloc(IMG)
x1 = extents(IMG)
whole, _ = attach(IMG, MNT)
os.unlink(f'{MNT}/blob')
detach(whole)
a2 = alloc(IMG)
x2 = extents(IMG)
# A further attach/detach gives a mount-time TRIM of the freed space its chance to reach the file.
whole, _ = attach(IMG, MNT)
detach(whole)
a3 = alloc(IMG)
x3 = extents(IMG)
rec = {'alloc_after_write_mib': a1, 'alloc_after_delete_mib': a2, 'alloc_after_remount_mib': a3,
       'regions_after_write': x1['regions'], 'regions_after_delete': x2['regions'],
       'regions_after_remount': x3['regions'], 'length_mib': round(x3['length'] / 2**20, 1)}
if fmt != 'C':
    conv = f'{WORK}/converted.asif'
    ms, _ = run(['diskutil', 'image', 'create', 'from', IMG, conv])
    rec['diskutil_create_from_ms'] = ms
    rec['alloc_after_create_from_mib'] = alloc(conv)
    os.unlink(conv)
if fmt == 'C':
    ms, _ = run(['hdiutil', 'compact', '-quiet', IMG])
    rec['hdiutil_compact_ms'] = ms
    rec['alloc_after_compact_mib'] = alloc(IMG)
out['reclaim'] = rec
print(json.dumps(rec), flush=True)

# 3. Resize: attached (expected refusal) and detached grow 100g -> 200g.
whole, _ = attach(IMG, MNT)
if fmt == 'C':
    out['resize_attached'] = trial(['hdiutil', 'resize', '-size', '200g', IMG])
else:
    out['resize_attached'] = trial(['diskutil', 'image', 'resize', '--size', '200g', IMG])

# 4. While attached: can another process lock or copy the backing file?
locks = {}
for name, flag in (('shlock', os.O_SHLOCK), ('exlock', os.O_EXLOCK)):
    try:
        fd = os.open(IMG, os.O_RDONLY | flag | os.O_NONBLOCK)
        os.close(fd)
        locks[name] = 'acquired'
    except OSError as e:
        locks[name] = e.strerror
out['attached_file_locks'] = locks
out['convert_while_attached'] = trial(
    ['diskutil', 'image', 'create', 'from', IMG, f'{WORK}/conv-attached.asif'] if fmt != 'C' else
    ['hdiutil', 'convert', IMG, '-format', 'UDSP', '-o', f'{WORK}/conv-attached.sparseimage'])
detach(whole)
if fmt == 'C':
    ms, _ = run(['hdiutil', 'resize', '-size', '200g', IMG])
else:
    ms, _ = run(['diskutil', 'image', 'resize', '--size', '200g', IMG])
whole, _ = attach(IMG, MNT)
cap = subprocess.run(['df', '-k', MNT], capture_output=True, text=True).stdout.split('\n')[1].split()[1]
detach(whole)
out['resize_detached_ms'] = ms
out['capacity_after_resize_gib'] = round(int(cap) / 2**20, 1)

# 5. A clone of the image mounts with the source's case sensitivity.
clone = f'{WORK}/clone.{EXT}'
clonefile(IMG, clone)
whole, _ = attach(clone, f'{WORK}/mclone')
out['clone_case_sensitive'] = os.pathconf(f'{WORK}/mclone', 11)
detach(whole)
print(json.dumps(out), flush=True)
with open(f'{WORK}.json', 'w') as f:
    json.dump(out, f, indent=1)
shutil.rmtree(WORK)

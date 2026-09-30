#!/usr/bin/env python3
"""Does a huge capacity cost anything? 100 GiB vs the store volume's full size, per format.

usage: PROTO_ROOT=<dir> cap.py <A|C>
Per capacity: create (median of 3), image allocation and host free space right after create, attach+fsck+mount
(median of 3), `df` inside the volume, fill with the data set and one churn round (no clone held): time, image
allocation and extents. Then grow the 100 GiB image to the full size: resize time, container grow, `df` after."""
import json
import os
import statistics
import subprocess
import sys
import time

sys.argv = [sys.argv[0], sys.argv[1], 'cap']
HERE = os.path.dirname(os.path.abspath(__file__))
exec(open(os.path.join(HERE, 'run.py')).read().split('\ndef main():')[0])

GIB = 2**30
store = os.statvfs(P)
FULL = (store.f_blocks * store.f_frsize) // 2**20 * 2**20  # the store volume's size, whole MiB
CAPS = {'100GiB': 100 * GIB, 'store-size': FULL}
out = {'fmt': fmt, 'store_bytes': FULL}


def free():
    s = os.statvfs(P)
    return s.f_bavail * s.f_frsize


def make(image, size):
    if fmt == 'C':
        ms, _ = run(['hdiutil', 'create', '-quiet', '-size', str(size), '-type', 'SPARSE', '-fs',
                     'Case-sensitive APFS', '-volname', 'proto' + fmt, '-nospotlight', image])
        return ms
    a, _ = run(['diskutil', 'image', 'create', 'blank', '--format', 'ASIF', '--size', str(size),
                '--volumeName', 'proto' + fmt, '--fs', 'None', image])
    b, o = run(['diskutil', 'image', 'attach', '--nobrowse', '--noMount', '--plist', image])
    dev = plistlib.loads(o)['system-entities'][0]['dev-entry']
    c, _ = run([NEWFS, '-U', str(os.getuid()), '-G', str(os.getgid()), '-e', '-v', 'proto' + fmt, '/dev/' + dev])
    d, _ = run(['diskutil', 'eject', dev])
    return round(a + b + c + d, 1)


def detach_logged(whole, log):
    """Detach, recording every refusal instead of dying on it; a huge SPARSE volume refused once here."""
    for attempt in range(5):
        t = time.monotonic()
        p = subprocess.run(['hdiutil', 'detach', whole] if fmt == 'C' else ['diskutil', 'eject', whole],
                           capture_output=True, text=True)
        ms = round((time.monotonic() - t) * 1000, 1)
        if p.returncode == 0:
            return ms
        log.append({'attempt': attempt, 'ms': ms, 'rc': p.returncode, 'err': (p.stdout + p.stderr).strip()[-200:]})
    raise RuntimeError(f'detach {whole} refused five times: {log}')


OTHER_REFUSALS = []


def detach(whole):  # churn() and the grow step detach through this too
    return detach_logged(whole, OTHER_REFUSALS)


def df(mnt):
    s = os.statvfs(mnt)
    return {'df_size_gib': round(s.f_blocks * s.f_frsize / GIB, 1), 'df_avail_gib': round(s.f_bavail * s.f_frsize / GIB, 1)}


shutil.rmtree(WORK, ignore_errors=True)
os.makedirs(WORK)
for label, size in CAPS.items():
    r = {'capacity_bytes': size}
    creates = []
    for i in range(3):
        img = f'{WORK}/{label}-{i}.{EXT}'
        before = free()
        creates.append(make(img, size))
        after = free()
        st = os.stat(img)
        if i == 2:
            r.update(file_length_gib=round(st.st_size / GIB, 1), alloc_after_create_mib=round(st.st_blocks * 512 / 2**20, 1),
                     host_free_delta_mib=round((before - after) / 2**20, 1))
        else:
            os.unlink(img)
    r['create_ms'] = creates
    img = f'{WORK}/{label}-2.{EXT}'
    atts, dets = [], []
    for i in range(3):
        whole, att = attach(img, MNT)
        atts.append(att)
        if i == 0:
            r.update(df(MNT))
        dets.append(detach_logged(whole, r.setdefault('detach_refusals', [])))
    r['attach_total_ms'] = [a['attach_total_ms'] for a in atts]
    r['fsck_ms'] = [a['fsck_ms'] for a in atts]
    r['detach_ms'] = dets
    globals()['IMG'] = img
    whole, _ = attach(img, MNT)
    ms, _ = run(['/usr/bin/ditto', DS, f'{MNT}/ds'])
    detach_logged(whole, r.setdefault('detach_refusals', []))
    x = extents(img)
    r.update(fill_ms=ms, filled_alloc_gib=round(x['alloc_bytes'] / GIB, 3), filled_extents=int(x['extents']))
    churn(1)
    x = extents(img)
    r.update(churned_alloc_gib=round(x['alloc_bytes'] / GIB, 3), churned_extents=int(x['extents']))
    out[label] = r
    print(label, json.dumps(r), flush=True)

# Grow the 100 GiB image to the store's size, the way `cowshed resize` does: image resize detached, then container grow.
img = f'{WORK}/100GiB-2.{EXT}'
if fmt == 'C':
    ms, _ = run(['hdiutil', 'resize', '-size', str(FULL), img])
else:
    ms, _ = run(['diskutil', 'image', 'resize', '--size', str(FULL), img])
g = {'resize_ms': ms}
whole, att = attach(img, MNT)
g['attach_total_ms'] = att['attach_total_ms']
g['df_before_grow'] = df(MNT)
container = next(line.split()[-1] for line in subprocess.run(
    ['diskutil', 'info', MNT], capture_output=True, text=True).stdout.splitlines() if 'APFS Container:' in line)
t = time.monotonic()
p = subprocess.run(['diskutil', 'apfs', 'resizeContainer', container, '0'], capture_output=True, text=True)
g['grow_ms'] = round((time.monotonic() - t) * 1000, 1)
g['grow_rc'] = p.returncode
g['grow_tail'] = (p.stdout + p.stderr).strip()[-200:]
g['df_after_grow'] = df(MNT)
detach(whole)
g['alloc_after_grow_gib'] = round(os.stat(img).st_blocks * 512 / GIB, 3)
out['grow_100GiB_to_store_size'] = g
print(json.dumps(g), flush=True)
out['other_detach_refusals'] = OTHER_REFUSALS
with open(f'{WORK}.json', 'w') as f:
    json.dump(out, f, indent=1)
shutil.rmtree(WORK)

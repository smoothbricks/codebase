#!/usr/bin/env python3
"""Image-format prototype: one candidate, one variant, one JSON result.

usage: PROTO_ROOT=<dir> run.py <A|B|C> <clones|noclones|inclone>
  clones    churn the source while four clones of it are held
  noclones  churn the source with no clone held
  inclone   churn a clone of the source (a shed) and leave the source untouched
  (build <dir>/xtool from xtool.c and <dir>/ds with mkds.sh first)
  A  ASIF, case-sensitive  (diskutil image create blank --fs None + newfs_apfs -e)
  B  ASIF, case-insensitive (same, newfs_apfs -i)
  C  SPARSE, case-sensitive (hdiutil create -type SPARSE -fs "Case-sensitive APFS")
"""
import ctypes
import json
import os
import plistlib
import shutil
import statistics
import subprocess
import sys
import time

P = os.environ['PROTO_ROOT']  # scratch directory on the volume that holds cowshed images
XT = f'{P}/xtool'
DS = os.environ.get('PROTO_DS', f'{P}/ds')
NEWFS = '/System/Library/Filesystems/apfs.fs/Contents/Resources/newfs_apfs'
fmt, variant = sys.argv[1], sys.argv[2]
EXT = 'sparseimage' if fmt == 'C' else 'asif'
WORK = f'{P}/{fmt}-{variant}' + os.environ.get('PROTO_TAG', '')
IMG = f'{WORK}/src.{EXT}'
MNT = f'{WORK}/mnt'
HELD = 4
ROUNDS = int(os.environ.get('PROTO_ROUNDS', '3'))
libc = ctypes.CDLL(None, use_errno=True)
result = {'fmt': fmt, 'variant': variant, 'phases': []}


def log(**kw):
    kw['load1'] = round(os.getloadavg()[0], 1)
    result['phases'].append(kw)
    print(json.dumps(kw), flush=True)


def run(cmd):
    t = time.monotonic()
    out = subprocess.run(cmd, check=True, capture_output=True)
    return round((time.monotonic() - t) * 1000, 1), out.stdout


def kv(stdout):
    return {k: float(v) for k, v in (f.split('=') for f in stdout.decode().split())}


def create():
    if fmt == 'C':
        ms, _ = run(['hdiutil', 'create', '-quiet', '-size', '100g', '-type', 'SPARSE', '-fs',
                     'Case-sensitive APFS', '-volname', 'proto' + fmt, '-nospotlight', IMG])
        return {'create_ms': ms}
    a, _ = run(['diskutil', 'image', 'create', 'blank', '--format', 'ASIF', '--size', '100g',
                '--volumeName', 'proto' + fmt, '--fs', 'None', IMG])
    b, out = run(['diskutil', 'image', 'attach', '--nobrowse', '--noMount', '--plist', IMG])
    dev = plistlib.loads(out)['system-entities'][0]['dev-entry']
    c, _ = run([NEWFS, '-U', str(os.getuid()), '-G', str(os.getgid()),
                '-e' if fmt == 'A' else '-i', '-v', 'proto' + fmt, '/dev/' + dev])
    d, _ = run(['diskutil', 'eject', dev])
    return {'create_ms': round(a + b + c + d, 1), 'blank_ms': a, 'attach_ms': b, 'newfs_ms': c,
            'eject_ms': d}


def attach(image=IMG, mnt=MNT):
    cmd = (['hdiutil', 'attach', '-nobrowse', '-owners', 'on', '-nomount', '-plist', image]
           if fmt == 'C' else
           ['diskutil', 'image', 'attach', '--nobrowse', '--noMount', '--plist', image])
    a, out = run(cmd)
    ents = plistlib.loads(out)['system-entities']
    vol = next(e['dev-entry'] for e in ents
               if e.get('content-hint') == 'Apple_APFS_Volume' or e.get('volume-kind') == 'apfs')
    vol = vol.removeprefix('/dev/')
    wholes = [e['dev-entry'].removeprefix('/dev/') for e in ents]
    wholes = [w for w in wholes if 's' not in w[4:]]
    whole = min(wholes, key=lambda w: int(w[4:]))
    f, _ = run(['/sbin/fsck_apfs', '-q', '/dev/r' + vol])
    os.makedirs(mnt, exist_ok=True)
    m, _ = run(['/sbin/mount_apfs', '-o', 'nobrowse,owners', '/dev/' + vol, mnt])
    return whole, {'attach_ms': a, 'fsck_ms': f, 'mount_ms': m, 'attach_total_ms': round(a + f + m, 1)}


def detach(whole):
    ms, _ = run(['hdiutil', 'detach', whole] if fmt == 'C' else ['diskutil', 'eject', whole])
    return ms


def extents(path=IMG):
    _, out = run([XT, 'extents', path])
    return kv(out)


def clonefile(src, dst):
    t = time.monotonic()
    if libc.clonefile(src.encode(), dst.encode(), 0) != 0:
        raise OSError(ctypes.get_errno(), 'clonefile', dst)
    return round((time.monotonic() - t) * 1000, 2)


def probe(stage):
    """clonefile cost, first write into a fresh clone, and deleting the written clone."""
    x = extents()
    clones = []
    for i in range(5):
        dst = f'{WORK}/probe{i}.{EXT}'
        clones.append(clonefile(IMG, dst))
        os.unlink(dst)
    dst = f'{WORK}/probe.{EXT}'
    clonefile(IMG, dst)
    _, out = run([XT, 'firstwrite', dst])
    fw = kv(out)
    t = time.monotonic()
    os.unlink(dst)
    rm = round((time.monotonic() - t) * 1000, 1)
    log(stage=stage, extents=int(x['extents']), regions=int(x['regions']),
        alloc_gib=round(x['alloc_bytes'] / 2**30, 3), data_gib=round(x['data_bytes'] / 2**30, 3),
        count_ms=x['count_ms'], clonefile_ms_median=statistics.median(clones),
        firstwrite_ms=fw['firstwrite_ms'], firstwrite_fsync_ms=fw['fsync_ms'],
        delete_written_clone_ms=rm)


SYMLINKS = []


SHED = f'{WORK}/shed.{EXT}'


def churn(r):
    """Fixed churn: rewrite target/ (2 GiB, new file + rename) and relink every symlink."""
    whole, att = attach(SHED if variant == 'inclone' else IMG)
    t = time.monotonic()
    tgt = f'{MNT}/ds/target'
    written = 0
    for root, _, files in os.walk(tgt):
        for name in files:
            path = os.path.join(root, name)
            size = os.path.getsize(path)
            chunk = os.urandom(1 << 20)
            fd = os.open(path + '.tmp', os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
            for _ in range(size >> 20):
                os.write(fd, chunk)
            os.close(fd)
            os.rename(path + '.tmp', path)
            written += size
    tw = time.monotonic()
    if not SYMLINKS:
        for root, dirs, files in os.walk(f'{MNT}/ds'):
            if root.endswith('/.git') or '/.git/' in root:
                continue
            for name in files + dirs:
                path = os.path.join(root, name)
                if os.path.islink(path):
                    SYMLINKS.append(os.path.relpath(path, MNT))
    for rel in SYMLINKS:
        path = os.path.join(MNT, rel)
        target = os.readlink(path)
        os.unlink(path)
        os.symlink(target, path)
    tl = time.monotonic()
    dms = detach(whole)
    log(stage=f'churn{r}', rewrite_gib=round(written / 2**30, 2), rewrite_ms=round((tw - t) * 1000),
        relinked=len(SYMLINKS), relink_ms=round((tl - tw) * 1000), detach_ms=dms, **att)


def lifecycle():
    atts, dets = [], []
    for _ in range(3):
        whole, att = attach()
        atts.append(att)
        dets.append(detach(whole))
    med = {k: statistics.median(a[k] for a in atts) for k in atts[0]}
    log(stage='lifecycle-empty', detach_ms=statistics.median(dets), **med)


def fill():
    whole, att = attach()
    ms, _ = run(['/usr/bin/ditto', DS, f'{MNT}/ds'])
    dms = detach(whole)
    log(stage='fill', fill_ms=ms, detach_ms=dms, **att)


def io():
    whole, att = attach()
    _, out = run([XT, 'seq', f'{MNT}/seq.bin', '2048'])
    seq = kv(out)
    os.unlink(f'{MNT}/seq.bin')
    os.makedirs(f'{MNT}/small')
    _, out = run([XT, 'small', f'{MNT}/small', '20000'])
    small = kv(out)
    shutil.rmtree(f'{MNT}/small')
    run(['git', '-C', f'{MNT}/ds', 'status', '--porcelain'])  # refresh the copied index once
    gs = [run(['git', '-C', f'{MNT}/ds', 'status', '--porcelain'])[0] for _ in range(5)]
    case = os.pathconf(MNT, 11)  # _PC_CASE_SENSITIVE
    dms = detach(whole)
    log(stage='io', case_sensitive=case, git_status_ms_median=statistics.median(gs),
        git_status_ms=gs, detach_ms=dms, **seq, **small, **att)


def defrag():
    dst = f'{IMG}.defrag'
    _, out = run([XT, 'copy', IMG, dst])
    cp = kv(out)
    x = extents(dst)
    # The image tools pick the format by extension (SPARSE under a foreign one attaches
    # read-only), and cowshed renames the copy over the image before attaching it.
    renamed = f'{WORK}/defragged.{EXT}'
    os.rename(dst, renamed)
    dst = renamed
    probe_dst = f'{WORK}/probe-defrag.{EXT}'
    clonefile(dst, probe_dst)
    _, out = run([XT, 'firstwrite', probe_dst])
    fw = kv(out)
    os.unlink(probe_dst)
    whole, att = attach(dst, f'{WORK}/mnt-defrag')
    ok = os.path.isdir(f'{WORK}/mnt-defrag/ds/node_modules')
    dms = detach(whole)
    os.unlink(dst)
    log(stage='defrag', copy_ms=cp['copy_ms'], copy_MBps=cp['MBps'],
        copied_gib=round(cp['copied_bytes'] / 2**30, 3), extents_after=int(x['extents']),
        firstwrite_after_ms=fw['firstwrite_ms'], verify_attach_ok=ok, detach_ms=dms, **att)


def main():
    shutil.rmtree(WORK, ignore_errors=True)
    os.makedirs(WORK)
    log(stage='create', **create())
    lifecycle()
    fill()
    probe('filled')
    if variant == 'inclone':
        log(stage='shed-clone', clonefile_ms=clonefile(IMG, SHED))
    if variant == 'clones':
        held = [clonefile(IMG, f'{WORK}/held{i}.{EXT}') for i in range(HELD)]
        log(stage='held-clones', count=HELD, clonefile_ms=held)
    for r in range(1, ROUNDS + 1):
        churn(r)
        probe(f'after-churn{r}')
        if variant == 'inclone':
            x = extents(SHED)
            log(stage=f'shed-after-churn{r}', shed_extents=int(x['extents']),
                shed_alloc_gib=round(x['alloc_bytes'] / 2**30, 3))
    defrag()
    if variant == 'noclones':
        io()
    t = time.monotonic()
    for p in [f'{WORK}/held{i}.{EXT}' for i in range(HELD)] + [SHED]:
        if os.path.exists(p):
            os.unlink(p)
    log(stage='cleanup-held', rm_ms=round((time.monotonic() - t) * 1000))
    with open(f'{WORK}.json', 'w') as f:  # copied into results/
        json.dump(result, f, indent=1)


main()

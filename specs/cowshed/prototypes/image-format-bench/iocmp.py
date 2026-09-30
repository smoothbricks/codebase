#!/usr/bin/env python3
"""I/O inside the volume, candidates interleaved so host-load drift hits all of them alike.

usage: PROTO_ROOT=<dir> iocmp.py
Creates one image per candidate (A, B, C), fills each with the data set, keeps all three mounted, then runs 7 rounds
of: git status on the data set's repository, 20,000 small files created / symlinked / unlinked, 1 GiB sequential
write (F_FULLFSYNC) and read (F_NOCACHE) — rotating the candidate order every round. Reports medians."""
import json
import os
import statistics
import subprocess
import sys
import time

sys.argv = [sys.argv[0], 'A', 'iocmp']
HERE = os.path.dirname(os.path.abspath(__file__))
SRC = open(os.path.join(HERE, 'run.py')).read().split('\ndef main():')[0]
ROOT = f"{os.environ['PROTO_ROOT']}/iocmp"
shutil_mod = __import__('shutil')
shutil_mod.rmtree(ROOT, ignore_errors=True)
os.makedirs(ROOT)

cands = {}
for f in 'ABC':
    ns = {}
    sys.argv = [sys.argv[0], f, 'iocmp']
    exec(SRC, ns)
    ns['WORK'] = f'{ROOT}/{f}'
    ns['IMG'] = f'{ROOT}/{f}/src.{ns["EXT"]}'
    ns['MNT'] = f'{ROOT}/{f}/mnt'
    os.makedirs(ns['WORK'])
    ns['create']()
    whole, _ = ns['attach'](ns['IMG'], ns['MNT'])
    t = time.monotonic()
    subprocess.run(['/usr/bin/ditto', ns['DS'], f"{ns['MNT']}/ds"], check=True)
    fill_s = round(time.monotonic() - t, 1)
    subprocess.run(['git', '-C', f"{ns['MNT']}/ds", 'status', '--porcelain'], check=True, capture_output=True)
    cands[f] = {'ns': ns, 'whole': whole, 'fill_s': fill_s, 'samples': []}
    print(f, 'filled', fill_s, flush=True)

order = list('ABC')
for rnd in range(7):
    for f in order:
        ns, mnt = cands[f]['ns'], cands[f]['ns']['MNT']
        run, kv, XT = ns['run'], ns['kv'], ns['XT']
        git_ms, _ = run(['git', '-C', f'{mnt}/ds', 'status', '--porcelain'])
        os.makedirs(f'{mnt}/small')
        small = kv(run([XT, 'small', f'{mnt}/small', '20000'])[1])
        shutil_mod.rmtree(f'{mnt}/small')
        seq = kv(run([XT, 'seq', f'{mnt}/seq.bin', '1024'])[1])
        os.unlink(f'{mnt}/seq.bin')
        cands[f]['samples'].append({'git_status_ms': git_ms, **small, **seq, 'load1': round(os.getloadavg()[0], 1)})
    order = order[1:] + order[:1]
    print('round', rnd, flush=True)

out = {}
for f, c in cands.items():
    keys = ('git_status_ms', 'create_ops', 'symlink_ops', 'unlink_ops', 'seq_write_MBps', 'seq_read_MBps')
    out[f] = {'fill_s': c['fill_s'], **{k: statistics.median(s[k] for s in c['samples']) for k in keys},
              'samples': c['samples']}
    c['ns']['detach'](c['whole'])
print(json.dumps({f: {k: v for k, v in o.items() if k != 'samples'} for f, o in out.items()}, indent=1))
with open(f'{ROOT}.json', 'w') as fh:
    json.dump(out, fh, indent=1)
shutil_mod.rmtree(ROOT)

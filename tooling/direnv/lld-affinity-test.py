#!/usr/bin/env python3
"""Link with LLD while the CPU set grows between the driver's thread count and the executor's.

LLD 21 sizes its relocation shards from a CPU-affinity snapshot the driver takes before the parallel executor exists.
The executor takes its own when it is first created, at the first parallel task. A mask that grows in between (Incus
rewrites a container's cpuset whenever any instance starts or stops) gives the executor more workers than there are
shards, and a worker indexes past them: heap corruption, then a SIGSEGV in realloc.

This runs the linker under GDB with an 8-CPU mask, grows it to 12 at the first llvm::parallel::TaskGroup::spawn, and
links 128 objects holding 4096 relative relocations each. The link must succeed and the output must keep every one of
them. A stock LLD 21 fails this; the patched one (nixpkgs-overlay/lld-affinity.patch) passes. The run proves nothing
unless the mask grew, so it fails when the first parallel task never ran.

Run on Linux with at least 12 allowed CPUs; GDB must support Python and ptrace.
"""

import argparse
import os
from pathlib import Path
import subprocess
import tempfile

INITIAL_CPUS = 8
GROWN_CPUS = 12
RELOCATIONS_PER_OBJECT = 4096
OBJECTS = 128
# Exit codes of the GDB script, beside the 1 it exits with when the linker dies.
MAIN_NOT_REACHED = 3
MASK_NEVER_GREW = 4

parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
parser.add_argument("--lld", default="ld.lld")
parser.add_argument("--llvm-mc", default="llvm-mc")
parser.add_argument("--llvm-readelf", default="llvm-readelf")
parser.add_argument("--gdb", default="gdb")
parser.add_argument("--threads", type=int)
args = parser.parse_args()
if len(os.sched_getaffinity(0)) < GROWN_CPUS:
    parser.error(f"the affinity-growth regression requires at least {GROWN_CPUS} allowed CPUs")

GDB_COMMANDS = f"""set pagination off
set confirm off
set breakpoint pending on
break main
run
python
import os
inferior = gdb.selected_inferior()
if inferior.pid == 0:
    print('MAIN_NOT_REACHED')
    gdb.execute('quit {MAIN_NOT_REACHED}')
cpus = sorted(os.sched_getaffinity(inferior.pid))
os.sched_setaffinity(inferior.pid, cpus[:{INITIAL_CPUS}])
print('AFFINITY_INITIAL', cpus[:{INITIAL_CPUS}])
grown = False
exit_code = None
def exited(event):
    global exit_code
    exit_code = getattr(event, 'exit_code', None)
gdb.events.exited.connect(exited)
end
delete breakpoints
break llvm::parallel::TaskGroup::spawn
commands
silent
python
os.sched_setaffinity(gdb.selected_inferior().pid, cpus[:{GROWN_CPUS}])
grown = True
print('AFFINITY_GROWN', cpus[:{GROWN_CPUS}])
end
disable $bpnum
continue
end
continue
python
if exit_code != 0:
    gdb.execute('thread apply all bt 5')
    gdb.execute('quit 1')
if not grown:
    print('MASK_NEVER_GREW')
    gdb.execute('quit {MASK_NEVER_GREW}')
end
quit 0
"""

with tempfile.TemporaryDirectory(prefix="lld-affinity-") as directory:
    root = Path(directory)
    source = root / "relative.s"
    obj = root / "relative.o"
    output = root / "relative.so"
    source.write_text(
        ".text\n.local target\n.type target,@function\ntarget:\nret\n.data\n"
        f".rept {RELOCATIONS_PER_OBJECT}\n.quad target\n.endr\n"
    )
    subprocess.run(
        [args.llvm_mc, "-triple=x86_64-unknown-linux-gnu", "-filetype=obj", str(source), "-o", str(obj)],
        check=True,
    )
    commands = root / "affinity.gdb"
    commands.write_text(GDB_COMMANDS)
    link = [args.lld, "-shared", "-o", str(output)]
    if args.threads is not None:
        link.append(f"--threads={args.threads}")
    link.extend([str(obj)] * OBJECTS)
    result = subprocess.run([args.gdb, "-q", "-batch", "-x", str(commands), "--args", *link])
    if result.returncode == MAIN_NOT_REACHED:
        raise SystemExit("main was never reached under gdb: the CPU set was never set, so this link proves nothing")
    if result.returncode == MASK_NEVER_GREW:
        raise SystemExit(
            "llvm::parallel::TaskGroup::spawn never ran: the CPU set never grew, so this link proves nothing"
        )
    if result.returncode != 0:
        raise SystemExit(f"the link did not survive the CPU-set growth (gdb exited {result.returncode})")
    relocs = subprocess.run(
        [args.llvm_readelf, "--relocs", "--wide", str(output)], check=True, capture_output=True, text=True
    ).stdout.count("R_X86_64_RELATIVE")
    expected = RELOCATIONS_PER_OBJECT * OBJECTS
    if relocs != expected:
        raise SystemExit(f"relative relocation count: expected {expected}, got {relocs}")
    print(f"PASS: affinity growth preserves all {relocs} relative relocations (threads={args.threads})")

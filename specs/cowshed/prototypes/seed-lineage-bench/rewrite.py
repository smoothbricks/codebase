#!/usr/bin/env python3
"""Contiguous rewrite of a detached seed, at the size lineage.py/large.py left it and padded to
about 20 and 60 GiB with incompressible filler written into a clone of it.

Two methods per size: a plain copy of the data regions (SEEK_DATA/SEEK_HOLE, pread/pwrite 8 MiB,
F_FULLFSYNC) and `diskutil image create from --format ASIF`. Each result is timed, its extents
counted, attached and fsck'd, and probed for its first-write cost.

usage: PROTO_ROOT=<dir> rewrite.py      (writes results/rewrite.json)
"""
import fcntl, os
import lineage as p
from large import latest_seed

SEEK_HOLE, SEEK_DATA, F_FULLFSYNC = 3, 4, 51
CHUNK = 8 << 20


def plain_copy(source, destination):
    started = p.time.monotonic()
    src = os.open(source, os.O_RDONLY)
    dst = os.open(destination, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
    size = os.fstat(src).st_size
    os.ftruncate(dst, size)
    offset = 0
    while offset < size:
        try:
            data = os.lseek(src, offset, SEEK_DATA)
        except OSError:
            break
        hole = os.lseek(src, data, SEEK_HOLE)
        while data < hole:
            chunk = os.pread(src, min(CHUNK, hole - data), data)
            os.pwrite(dst, chunk, data)
            data += len(chunk)
        offset = hole
    fcntl.fcntl(dst, F_FULLFSYNC)
    os.close(src)
    os.close(dst)
    return p.time.monotonic() - started


def verify(image):
    _, whole, volume = p.attach(image)
    elapsed = p.sh(["/sbin/fsck_apfs", "-q", "/dev/r" + volume])[0]
    p.sh(["diskutil", "eject", whole])
    return elapsed


def pad(source, destination, target_gib):
    p.clone(source, destination)
    _, whole, volume = p.attach(destination)
    point = f"{p.MNT}/pad"
    p.mount(volume, point)
    block = os.urandom(CHUNK)
    index = 0
    while p.allocated_gib(destination) < target_gib:
        with open(f"{point}/filler-{index}", "wb") as filler:
            for i in range(128):  # 1 GiB per file; every chunk distinct
                filler.write(i.to_bytes(8, "little") + index.to_bytes(8, "little") + block[16:])
        index += 1
    p.detach(point, whole)


def measure(image, label, out):
    row = {"label": label, "allocated_gib": p.allocated_gib(image), "extents_before": p.extents(image),
           "first_write_before": p.first_write(image)}
    for method in ("plain_copy", "image_create_from"):
        dest = f"{p.ROOT}/rewrite_{label}_{method}.asif"
        if method == "plain_copy":
            elapsed = plain_copy(image, dest)
        else:
            elapsed = p.sh(["diskutil", "image", "create", "from", "--format", "ASIF", image, dest])[0]
        row[method] = {"elapsed": elapsed, "extents_after": p.extents(dest),
                       "allocated_gib": p.allocated_gib(dest), "first_write_after": p.first_write(dest),
                       "attach_fsck": verify(dest)}
        os.unlink(dest)
        p.log(label, method, row[method])
    out["sizes"].append(row)
    p.save("rewrite.json", out)


def main():
    out = {"load_start": os.getloadavg(), "sizes": []}
    seed, _ = latest_seed()
    measure(seed, "seed", out)
    for target in (20, 60):
        padded = f"{p.ROOT}/pad_{target}.asif"
        pad(seed, padded, target)
        measure(padded, f"pad{target}", out)
        os.unlink(padded)
    p.log("rewrite done")


if __name__ == "__main__":
    main()

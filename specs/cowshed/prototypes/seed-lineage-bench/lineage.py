#!/usr/bin/env python3
"""Seed-lineage prototype: can `new` clone from a seed image that only warm builds ever write?

usage: PROTO_ROOT=<dir> PROTO_REPO=<checkout> lineage.py
  PROTO_ROOT  scratch directory on the volume that holds cowshed images (clonefile needs it)
  PROTO_REPO  the repository whose history the generations replay (default: this checkout)
  PROTO_MNT   where scratch volumes mount (default /private/tmp/seed-lineage-bench)
  GENERATIONS landed commits to replay (default 20)
  (build $PROTO_ROOT/xtool from ../image-format-bench/xtool.c first)

Every image is case-sensitive ASIF, created the way cowshed creates one (blank --fs None, attach
--noMount, newfs_apfs -e). Two tracks replay the same first-parent commits:

  S  seed_{g-1} is never written. Each generation forks it (clonefile) to warm_g, attaches it,
     fast-forwards git one landed commit, runs the incremental warm build, detaches, and
     publishes warm_g as seed_g (rename); seed_{g-1} is deleted while a shed clone of it is
     still held.
  M  one image stays mounted read-write, like a main: each generation fast-forwards the same
     commit and receives the same target/ writes in place (rsync from the warm), with one shed
     clone of it held per generation.

Per generation: fork/attach/mount/ff/build/detach/publish times, extents and allocated bytes of
both tracks, and the first write into a fresh clone of each. At the end: `new` end to end from
the detached seed and from the mounted image, and a shed made while the next warm is still
building (previous seed + fast-forward + its own rebuild) against one made from the published
seed. Writes results/lineage.json.
"""
import ctypes, json, os, plistlib, re, subprocess, time

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.environ["PROTO_ROOT"]
MNT = os.environ.get("PROTO_MNT", "/private/tmp/seed-lineage-bench")
REPO = os.environ.get("PROTO_REPO", os.path.abspath(f"{HERE}/../../../.."))
XTOOL = f"{ROOT}/xtool"
GENERATIONS = int(os.environ.get("GENERATIONS", "20"))
BUILD = ["cargo", "build", "-p", "cowshed-cli", "--bin", "cowshed"]
NEWFS = "/System/Library/Filesystems/apfs.fs/Contents/Resources/newfs_apfs"
BUILD_ENV = dict(os.environ, PATH=subprocess.check_output([REPO + "/tooling/direnv/repo-path"], text=True).strip())
libc = ctypes.CDLL(None, use_errno=True)


def log(*parts):
    print(time.strftime("%H:%M:%S"), *parts, flush=True)


def save(name, value):
    os.makedirs(f"{HERE}/results", exist_ok=True)
    with open(f"{HERE}/results/{name}", "w") as out:
        json.dump(value, out, indent=1)


def sh(args, cwd=None, env=None, check=True):
    started = time.monotonic()
    done = subprocess.run(args, cwd=cwd, env=env, capture_output=True, text=True)
    elapsed = time.monotonic() - started
    if check and done.returncode != 0:
        raise RuntimeError(f"{args} failed ({done.returncode}): {done.stderr[-2000:]}")
    return elapsed, done.stdout + done.stderr


def clone(source, destination):
    return sh(["/bin/cp", "-c", source, destination])[0]


def extents(image):
    return int(re.search(r"extents=(\d+)", sh([XTOOL, "extents", image])[1]).group(1))


def allocated_gib(image):
    return os.stat(image).st_blocks * 512 / 2**30


def create(image):
    sh(["diskutil", "image", "create", "blank", "--format", "ASIF", "--size", "400G",
        "--volumeName", "seed", "--fs", "None", image])
    out = sh(["diskutil", "image", "attach", "--nobrowse", "--noMount", "--plist", image])[1]
    whole = plistlib.loads(out[out.index("<?xml"):].encode())["system-entities"][0]["dev-entry"]
    sh([NEWFS, "-U", str(os.getuid()), "-G", str(os.getgid()), "-e", "-v", "seed", "/dev/" + whole])
    sh(["diskutil", "eject", whole])


def attach(image):
    elapsed, out = sh(["diskutil", "image", "attach", "--nobrowse", "--noMount", "--plist", image])
    entities = plistlib.loads(out[out.index("<?xml"):].encode())["system-entities"]
    whole = next(e["dev-entry"] for e in entities if not e.get("content-hint"))
    volume = next(e["dev-entry"] for e in entities if e.get("content-hint") == "Apple_APFS_Volume")
    return elapsed, whole, volume


def mount(volume, point):
    os.makedirs(point, exist_ok=True)
    return sh(["/sbin/mount_apfs", "-o", "nobrowse,owners", "/dev/" + volume, point])[0]


def detach(point, whole):
    started = time.monotonic()
    sh(["/sbin/umount", point])
    sh(["diskutil", "eject", whole])
    return time.monotonic() - started


def sync_volume(point):
    started = time.monotonic()
    assert libc.sync_volume_np(point.encode(), 0x02) == 0  # SYNC_VOLUME_WAIT
    return time.monotonic() - started


def first_write(image):
    """Clone `image`, rewrite one byte of the clone, fsync: the clone's extent-map copy."""
    probe = image + ".firstwrite"
    t_clone = clone(image, probe)
    fd = os.open(probe, os.O_WRONLY)
    started = time.monotonic()
    os.pwrite(fd, b"\0", os.fstat(fd).st_size - 1)
    os.fsync(fd)
    t_write = time.monotonic() - started
    os.close(fd)
    started = time.monotonic()
    os.unlink(probe)
    return {"clone": t_clone, "write": t_write, "delete": time.monotonic() - started}


def build(repo, **env):
    elapsed, out = sh(BUILD, cwd=repo, env=dict(BUILD_ENV, CARGO_TARGET_DIR=repo + "/target", **env))
    return elapsed, len(re.findall(r"^\s+Compiling ", out, re.M))


def ff(repo, commit):
    return sh(["git", "-C", repo, "merge", "--ff-only", "-q", commit])[0]


def main():
    results = {"load_start": os.getloadavg(), "build": " ".join(BUILD), "generations": []}
    os.makedirs(ROOT, exist_ok=True)
    commits = subprocess.check_output(["git", "-C", REPO, "rev-list", "--first-parent", "--reverse",
                                       f"-n{GENERATIONS + 1}", "HEAD"], text=True).split()
    base, commits = commits[0], commits[1:]
    results["base"] = base

    # Generation 0: a fresh clone of the repository, its node_modules and its target/.
    seed = f"{ROOT}/seed_0.asif"
    create(seed)
    _, whole, volume = attach(seed)
    point = f"{MNT}/warm"  # every warm build mounts here, so cargo's path-keyed fingerprints hold
    mount(volume, point)
    repo = point + "/repo"
    g0 = {"clone_repo": sh(["git", "clone", "-q", "--no-local", REPO, repo])[0]}
    sh(["git", "-C", repo, "checkout", "-q", "-B", "main", base])
    g0["bun_install"], out = sh(["bun", "install", "--frozen-lockfile"], cwd=repo, env=BUILD_ENV, check=False)
    g0["build"], g0["compiled"] = build(repo)
    g0["detach"] = detach(point, whole)
    g0["extents"], g0["allocated_gib"] = extents(seed), allocated_gib(seed)
    g0["first_write"] = first_write(seed)
    results["generation0"] = g0
    log("seed_0", g0)

    main_image = f"{ROOT}/main.asif"
    clone(seed, main_image)
    _, main_whole, main_volume = attach(main_image)
    main_point = f"{MNT}/main"
    mount(main_volume, main_point)
    main_repo = main_point + "/repo"
    held = []

    for g, commit in enumerate(commits, start=1):
        row = {"generation": g, "commit": commit[:10], "load": os.getloadavg()}
        warm = f"{ROOT}/warm_{g}.asif"
        row["fork"] = clone(seed, warm)
        row["attach"], whole, volume = attach(warm)
        row["mount"] = mount(volume, point)
        row["ff"] = ff(repo, commit)
        row["build"], row["compiled"] = build(repo)
        row["main_ff"] = ff(main_repo, commit)
        row["main_apply"] = sh(["rsync", "-a", "--delete", repo + "/target/", main_repo + "/target/"])[0]
        row["main_flush"] = sync_volume(main_point)
        row["detach"] = detach(point, whole)
        previous = seed
        seed = f"{ROOT}/seed_{g}.asif"
        started = time.monotonic()
        os.rename(warm, seed)
        row["publish"] = time.monotonic() - started
        shed = f"{ROOT}/shed_S{g - 1}.asif"
        clone(previous, shed)
        held.append(shed)
        started = time.monotonic()
        os.unlink(previous)
        row["delete_previous_seed"] = time.monotonic() - started
        row["seed_extents"], row["seed_allocated_gib"] = extents(seed), allocated_gib(seed)
        row["seed_first_write"] = first_write(seed)
        main_shed = f"{ROOT}/shed_M{g}.asif"
        clone(main_image, main_shed)
        held.append(main_shed)
        row["main_extents"], row["main_allocated_gib"] = extents(main_image), allocated_gib(main_image)
        row["main_first_write"] = first_write(main_image)
        results["generations"].append(row)
        log(row)
        save("lineage.json", results)

    end = {}
    for label, source, flush in (("seed", seed, None), ("main", main_image, main_point)):
        r = {"flush": sync_volume(flush) if flush else 0.0}
        shed = f"{ROOT}/new_{label}.asif"
        r["clonefile"] = clone(source, shed)
        r["attach"], w, v = attach(shed)
        r["fsck"] = sh(["/sbin/fsck_apfs", "-q", "/dev/r" + v])[0]
        r["mount"] = mount(v, f"{MNT}/new_{label}")
        r["total"] = sum(r.values())
        r["detach"] = detach(f"{MNT}/new_{label}", w)
        os.unlink(shed)
        end[label] = r
    results["new_end_to_end"] = end

    stale = {}
    older = held[-2]  # shed_S{N-1}: a clone of seed_{N-1}
    for label, source in (("from_previous_seed", older), ("from_published_seed", seed)):
        shed = f"{ROOT}/stale_{label}.asif"
        clone(source, shed)
        _, w, v = attach(shed)
        r = {"mount_with_first_write": mount(v, point)}
        r["ff"] = ff(point + "/repo", commits[-1]) if label == "from_previous_seed" else 0.0
        r["build"], r["compiled"] = build(point + "/repo")
        r["detach"] = detach(point, w)
        os.unlink(shed)
        stale[label] = r
    results["shed_during_warm"] = stale
    save("lineage.json", results)

    detach(main_point, main_whole)
    os.unlink(main_image)
    for shed in held:
        os.unlink(shed)
    log("done; last seed left at", seed)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Two more warm generations on the seed lineage.py left: a large one (every workspace and
dependency crate recompiled, as a toolchain or lockfile bump does, forced by flipping the dev
profile's debug setting) and a small one (one leaf source file touched).

usage: PROTO_ROOT=<dir> large.py      (after lineage.py; writes results/large.json)
"""
import os, re
import lineage as p


def latest_seed():
    name = max((f for f in os.listdir(p.ROOT) if re.fullmatch(r"seed_\d+\.asif", f)),
               key=lambda f: int(re.search(r"\d+", f).group()))
    return f"{p.ROOT}/{name}", int(re.search(r"\d+", name).group())


def main():
    seed, n = latest_seed()
    point = f"{p.MNT}/warm"
    rows = []
    for label, touch in (("large", None), ("small", "packages/cowshed/crates/cowshed-cli/src/main.rs")):
        n += 1
        before = p.extents(seed)
        warm = f"{p.ROOT}/warm_{n}.asif"
        p.clone(seed, warm)
        _, whole, volume = p.attach(warm)
        p.mount(volume, point)
        repo = point + "/repo"
        if touch:
            os.utime(f"{repo}/{touch}")
        elapsed, compiled = p.build(repo, CARGO_PROFILE_DEV_DEBUG="1")
        p.detach(point, whole)
        os.unlink(seed)
        seed = f"{p.ROOT}/seed_{n}.asif"
        os.rename(warm, seed)
        row = {"label": label, "compiled": compiled, "build": elapsed, "extents_before": before,
               "extents_after": p.extents(seed), "allocated_gib": p.allocated_gib(seed),
               "first_write": p.first_write(seed), "load": os.getloadavg()}
        rows.append(row)
        p.log(row)
    p.save("large.json", rows)


if __name__ == "__main__":
    main()

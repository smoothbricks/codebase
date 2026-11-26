# The cargo wrapper's DYLD_LIBRARY_PATH makes every exec under cargo probe a missing directory

Found 2026-11-26 while measuring vnode-table churn on the host. Owner: tooling (devenv).

On Darwin, rust-overlay wraps `bin/cargo` with `--prefix DYLD_LIBRARY_PATH : ${curl.out}/lib`
(`oxalica/rust-overlay lib/mk-aggregated.nix:85-102`). Every descendant of cargo, cargo-nextest or `mbx build` inherits
it: rustc, build scripts, test binaries, and the git, python, bash and coreutils they spawn. dyld then probes that
directory for every library each of those processes loads.

Measured with `fs_usage -w -f pathname` (2026-11-26):

| Process started under cargo | Path events per start | Failed lookups |
| --------------------------- | --------------------- | -------------- |
| python3                     | 4,333 (192 without)   | 4,009          |
| git                         | about 4,000–4,500     | most           |
| a coreutils binary          | about 5,700           | most           |

In one 2.5 s window, 12.7k of git's 15.2k path events were this probe, from cowshed's own test fixtures in a land gate.
The failed lookups allocate no vnodes, but they cost about 38 ms of CPU and latency per exec.

## What must hold

- A process started under cargo resolves its libraries without probing a directory nothing in it needs.
- cargo still finds a working libcurl. The unwrapped cargo links `/usr/lib/libcurl.4.dylib`, which is in the dyld shared
  cache, and `cargo --version` runs with `DYLD_LIBRARY_PATH` unset.

## Where

`tooling/direnv/devenv.nix:39-47` already customises this derivation on Darwin (`overrideAttrs`). Replacing the wrapper
there with the unwrapped cargo is one way. The change re-keys every Rust task, so it lands alone under the global
cache-key rule.

## Red

`ps eww` of a cargo child shows `DYLD_LIBRARY_PATH`; a python3 started under cargo costs about 4,300 path events instead
of about 200.

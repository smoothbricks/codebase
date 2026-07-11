# Testing & Performance

Four test tiers plus enforced performance budgets. Everything pure (unit + property) runs everywhere; the real tiers are
parametrized over the substrate (`APFS-image` on macOS, `ZFS` on Linux) and enforcement layer (Seatbelt / Landlock) and
each is explicit about where it runs.

## Unit tests (pure, all platforms)

No mounts, no root, no network — pure functions with table-driven cases:

- **Sandbox rule generation** (Seatbelt profile text and Landlock ruleset spec, from the same grant snapshot): closed
  baseline shape, grant snapshot inclusion, secret denies present regardless of grants (by omission on Landlock; on
  Seatbelt by **ordering** — see next bullet), ReadOnly drops mount writes, grant-intersects-deny refusal, path
  canonicalization/escaping (including unix-socket rule paths: the kernel matches canonical targets, so a `/tmp`-spelled
  rule silently denies — measured).
- **Profile ordering invariant (denies last)**: SBPL is last-match-wins (measured — the same rules in the opposite order
  leave a secret readable), so every generated profile MUST place all deny rules after all allows. This test asserts it
  structurally over generated grant sets; the entire secret-protection model depends on it (04_sandbox.md).
- **Path policy**: cwd validation, `..`/symlink-shape normalization, workspace-name validation.
- **Grant files**: schema round-trip, revision monotonicity, delta application, wildcard egress matching
  (`*.github.com`), port defaults.
- **Marker files**: round-trip, unknown-version rejection, role transitions (new/fork/restore).
- **CLI contract**: stdout shape per command (bare value vs JSON envelope), exit-code mapping from `CowshedError`,
  `next:` hints on the documented paths. Golden tests — changing them is a breaking change and must touch this spec.
- **Env wiring**: exec env allowlist filtering (no `*_TOKEN`/`*_SECRET`/`AWS_*` pass-through), cache exports match
  03_caches.md exactly.
- **Port-block rule generation**: the generated SBPL emits the block as **16 literal single-port `network-outbound`
  allows** (measured: SBPL rejects port ranges — `invalid port in network address` — and hosts other than
  `localhost`/`*`), leaves `network-bind`/`network-inbound` permissive on localhost, and contains no range syntax
  anywhere (a generation-text golden; kernel enforcement is proved by the escape tier).

## Property tests (proptest, pure, all platforms)

Invariants the table-driven unit cases only sample. Each is a pure function over generated inputs:

- **Path policy**: normalization is idempotent (`normalize(normalize(p)) == normalize(p)`) and containment-preserving (a
  path normalized under a root never escapes it via `..`/symlink shape).
- **Grant algebra**: delta composition is well-defined (apply then apply-inverse is a no-op), revision is strictly
  monotonic across effective mutations, a no-op mutation leaves the revision and the canonical set unchanged, and public
  vectors come out sorted + deduplicated regardless of input order.
- **Marker/schema**: round-trip (`parse(write(m)) == m`) for all roles; any unknown `version` is rejected, never
  silently coerced.
- **Egress/port normalization**: host wildcard matching (`*.github.com`) and default-port fill are order- and
  duplicate-independent; a port block round-trips through the grant file.
- **Staged-object non-enumeration**: for any generated set of in-flight staged names (adopt/new temporaries, 01/02),
  enumeration never returns one — the "derived state" promise holds under partial writes.
- **Idempotent recovery/gc**: replaying a crash-recovery or `gc` pass over any generated interrupted-state fixture
  converges to the same result as running it once (crash points from the adopt/new/rm recovery tables, 02).

## Integration tests (real substrate)

Gated by `COWSHED_INTEGRATION=1` and parametrized over the substrate the host provides. On macOS: small `.asif` images
(1 GiB caps) under `/private/tmp/cowshed-itest-<pid>`, mirroring the apfs-workspace-bench harness, cleaned up in reverse
mount order with `-force` fallback. On Linux: a scratch ZFS pool on a loopback/file vdev (`cowshed.itest.<pid>`) with
datasets destroyed and the pool exported on teardown; the Linux leg also exercises `cowshed-helper` and the
Landlock/netns exec path. A suite-level guard reaps leaked `cowshed.itest.*` volumes/pools. The same flow table runs on
both; substrate-specific assertions (fsck step on APFS, origin-snapshot GC on ZFS) are tagged.

Covered flows:

- adopt → new → exec → push → rm (the golden path), including marker/token rewrite on new;
- **attach `-nomount` → fsck device → mount** ordering on APFS (the clone is verified as a block device _before_ it is
  mounted, per 02) — asserts the sequence and that a structurally-bad clone is caught before mount, not after;
- **fork mid-write clone validity**: clone an image while a writer churns the volume, then verify the clone mounts and
  fsck-passes. (Measured baseline to hold: 10/10 clonefiles taken under a continuous file-writer plus a streaming 128
  MiB dd passed both `fsck_apfs -q` and a full `-n` check, mountable and readable, on both SPARSE and ASIF; a non-synced
  clone may miss the last writes — freshness, not consistency. This tier keeps that regression-pinned.)
- checkpoint/restore round-trip (restore undo image `pre-restore-…` present);
- ensure healing matrix: detached image, wrong-flag mount, missing/wrong-flag `cowshed.store` and `cowshed.caches`
  volumes (lazy recreate + canonical-flag remount, 01_storage.md), stub `.envrc`;
- lazy volume creation at adopt: both dedicated volumes created idempotently before the first image; **Time Machine
  default-inclusion check** (verification item, 01_storage.md): whether TM includes additional internal volumes by
  default, and that adopt's volume-level exclusion is applied when it does;
- rm-while-busy (open file handle → grace → force detach);
- gc: trash drain, checkpoint pruning, orphan mountpoint removal, compaction (SPARSE fallback);
- gateway: mirror hit/miss against a local fixture registry, token→policy mapping, 403 hint body, audit records, CONNECT
  allow/deny, `repo mirror` fetch into a read-only bare mirror;
- ASIF/SPARSE fallback selection.

## Escape tests (cowshed-escape-tests, one corpus, both OSes, release gate)

One shared adversarial corpus (04_sandbox.md), run through the real exec pipeline: **Seatbelt on macOS, Landlock +
loopback netns on Linux**, green on **both** as a release gate (a red escape test cannot be waived). Structure mirrors
jcode's `jcode-sandbox-escape-bash-*` crates: each case is a shell payload plus an assertion that the operation was
denied and the artifact untouched.

Shared categories: path escapes (traversal, symlink, hardlink), secret reads, cowshed-state tampering, cross-workspace
access, egress bypass (direct, helper-process, DNS), revocation binding, ReadOnly enforcement — plus two the port-block
model adds:

- **sibling-supervisor-socket**: workspace A attempts to `connect(2)` workspace B's supervisor unix socket and drive B's
  shells — must be denied (the baseline scopes unix-socket connect to the workspace's own supervisor socket, the nix
  daemon, and the gateway, 04_sandbox.md).
- **port-block escape**: connect to a _sibling's_ data-plane port, a sibling's service port, and a sibling's
  ephemeral-bound listener — all EPERM (isolation is **outbound-enforced**; bind stays permissive, so sibling binds are
  not prevented and need no case). Verify the errno signal while at it: denied connect = EPERM(1), allowed-but-unserved
  = ECONNREFUSED(61) — the in-band denial evidence of 04_sandbox.md.

Linux-specific cases (Landlock/netns): bind-mount a denied path into a granted root, `/proc/<pid>/root` and
`/proc/<pid>/cwd` reach-arounds, `unshare`/`setns` to leave the netns, abstract-namespace and filesystem unix sockets
reaching a non-gateway listener, `connect(2)` to a non-gateway TCP port. Policy-string goldens (unit tier) do not
substitute for this — they prove generation, not kernel enforcement. Every production-discovered escape becomes a
permanent case.

## Performance budgets (regression thresholds)

Measured by the integration suite with tinybench-style medians (≥ 10 samples); CI asserts with a 3× multiplier to absorb
runner noise, local `cowshed doctor --bench` reports raw numbers.

### APFS / Seatbelt (macOS)

| Operation                     | Budget (median, local)  | Basis                                                        |
| ----------------------------- | ----------------------- | ------------------------------------------------------------ |
| `cowshed new` cold            | ≤ 1 s                   | clonefile ~2 ms + attach ~235 ms + branch/marker work        |
| `cowshed ensure` healthy      | ≤ 25 ms                 | one statfs + one marker read, compiled binary                |
| `cowshed rm` (perceived)      | ≤ 100 ms to return      | rename + grant-file unlink; detach is background             |
| `cowshed fork` / `checkpoint` | ≤ 1 s                   | same physics as new                                          |
| `cowshed path` / `ls`         | ≤ 50 ms                 | readdir + getmntinfo only — proves the no-state-store design |
| exec sandbox overhead         | ≤ 50 ms over bare spawn | profile generation + sandbox-exec                            |

**Basis, and what the numbers are (and are not).** Figures come from the apfs-workspace-bench study
(`specs/cowshed/prototypes/apfs-workspace-bench/`, harness, REPORT.md, and results committed alongside): clonefile of a
populated 100k-file image ~2 ms; `hdiutil attach` **median ~235 ms**; clone-backed images beat shadow mounts 2.34× on
synchronous write throughput, which is why clones are the substrate. These are **primitive-operation evidence from a
single 20-sample run**, not end-to-end cowshed SLOs — budgets are stated as **medians** precisely because 20 samples
cannot fix a stable tail. Note the p99 correction: the ~590 ms figure sometimes quoted is the p99 of **shadow
create+attach**, not clone attach — clone _attach-only_ p99 in that run was ~273 ms and clonefile+attach ~271 ms. Do not
cite ~590 ms as a clone-attach percentile.

The format experiment (`results/2026-07-11-substrate-experiments.json`, single-run medians, 4 GiB / 2000 files) adds:
**ASIF vs SPARSE — create 479 vs 1017 ms (2.1×), direct read 3.2 vs 17.9 ms (5.6×), direct write 15.7 vs 39.6 ms (2.5×),
metadata ~102 vs ~234 ms (2.3×), clonefile equal (~1.8–2.0 ms), attach ~416 vs ~342 ms (ASIF ~75 ms slower)** — the
basis for the ASIF default (01_storage.md); budgets absorb the ~75 ms attach delta within the 1 s median. **Attach floor
verdict: not flag-reducible** — `-noverify` saves ~15 ms (noise), `-noautofsck` nothing, `diskutil image attach` exposes
no equivalent knobs; the ~235–400 ms is inherent to DiskImages + APFS mount + DiskArbitration. Halving it would need a
different attach path (DiskImages2 / diskarbitrationd private API) — a research note, and budgets must not assume it.
The bench harness is the reference methodology for any future substrate change (it validated ASIF before it became the
default; it establishes the ZFS baseline below).

### ZFS / Landlock (Linux) — separate baseline

ZFS has no `attach` and no `fsck` step, so the APFS numbers do not transfer; the same _user-visible_ operations get
their own measured baseline and regression limits on a ZFS-capable Linux host (the cowshed CI runner, 10_ci.md),
established before the Linux leg gates.

| Operation                | Budget (median, local)  | Basis                                                                |
| ------------------------ | ----------------------- | -------------------------------------------------------------------- |
| `cowshed new` cold       | ≤ 250 ms (to establish) | `zfs snapshot` + `zfs clone` + mount, tens of ms; no attach, no fsck |
| `cowshed ensure` healthy | ≤ 25 ms                 | same pure fast path as macOS                                         |
| `cowshed rm` (perceived) | ≤ 100 ms to return      | logical retire; `zfs destroy` clone+origin is background             |
| `cowshed path` / `ls`    | ≤ 50 ms                 | `zfs list` + mount table                                             |
| exec sandbox overhead    | ≤ 50 ms over bare spawn | Landlock ruleset apply + netns join                                  |

The "to establish" figures are placeholders until first measured on the runner; they are recorded here so the Linux
baseline is an explicit deliverable, not an inherited APFS number.

## CI

- Unit tier: every PR, all platforms.
- Integration + escape tiers: macOS runners **and** a Linux+ZFS runner on PRs touching `packages/cowshed`, and nightly.
  The Linux leg is the natural dogfood target — a cowshed CI runner (10_ci.md) running cowshed's own suite.
- Escape suite green on both OSes is a release gate; a red escape test cannot be waived.
- `cargo clippy --workspace -D warnings` and `cargo fmt --check` gate merges (repo rule: fix everything you see — no
  pre-existing-failure waivers).

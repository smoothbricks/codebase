# `cowshed new` stalls 25–60s in a silent mount hang (DiskArbitration contention)

## Symptom

| Observation | Detail |
|---|---|
| `cowshed new` wall time | 35–102s on a 322GB image (minigraf), vs ~seconds expected |
| Which step hangs | The **first** staging `diskutil mount` blocks 24–62s (caught 3x by PID age; the volume sits unmounted meanwhile) |
| Which step does not | The **second** (canonical) mount is always ~1s |
| Hang shape | Duration varies 24–62s, then **succeeds** — a contention/timeout signature, not a deadlock |
| Blast radius | System-wide DiskArbitration stalls accompany the hang (whole-machine symptom, not just cowshed) |
| Clean repro, no observer load | routine `cowshed new mgperf-selfwatch` took **74.67s wall with zero
concurrent storage ops from the investigator** (no rehearsals, no parallel probes) — the hang
persists outside experiment churn, so the trigger cannot be the observer's own storm alone |

`diskutil` sampled during a hang: 100% in `SKDisk mountWithOptions → SKHelperClient
mountDisk:options:blocking → semaphore_wait` on on-demand storagekitd, while the gateway
daemon sat at 0% CPU (62k samples kevent-parked). Time Machine idle, Spotlight disabled on
workspace volumes, no snapshots, no third-party dissenters.

## What was ruled out (with numbers)

Every other leg of `new` measured sub-second in isolation:

| Step | Measured |
|---|---|
| clonefile (CoW clone) | 0.03s |
| `hdiutil attach` | 0.26s |
| `fsck_apfs -q` | 0.03s |
| `/bin/sync` | ≤0.15s |
| `diskutil mount` / rename / detach (solo rehearsals) | ≤0.7s |
| `-mountOptions owners` on vs off | 0.43s vs 0.35s |
| `rm`, `git`, CLI overhead | instant / seconds / 0.5s |

Notably, solo rehearsal mounts of the **identical bytes** complete in ~0.35s, and a clean
`new` with no observer load still hung 74.67s. The hang tracks the operation's own
detach→attach sequence, not the substance of any step (see theory).

## Root-cause analysis (current theory: DA churn, not duplicates alone)

Earlier suspicion centered on co-mounted duplicate volume UUIDs. That is real but no longer
the whole story:

- **Duplicate volume UUIDs confirmed.** The live main volume and its clone report the same
  UUID (`916B874F-…`); renames change labels only. Observed DA device confusion: detaching
  `disk11` ejected `disk10`, twice. There is **no public verb that regenerates an APFS
  volume UUID in place** (`diskutil apfs changeVolumeRole` measured as a NO-OP on the UUID;
  the man page shows no such verb). Volume UUIDs are therefore untouchable — a fix must not
  go near them.
- **The XPC layer is exonerated.** A DA-bypass mount (`/sbin/mount_apfs` direct on a scratch
  clone) hung 47.9s with a userspace poll-spin shape (4.4s user CPU). The stall lives
  at/below the kernel/DA mount path; no userspace workaround avoids it.
- **The trigger suspect is now the churn every `new` performs.** Each create/fork does
  staging-attach → staging-mount → init → **staging-detach → canonical-attach → remount** —
  a back-to-back detach/attach of same-UUID bytes through DA on every single invocation.
  Rehearsals of identical bytes are fast solo (0.35s). Early hangs correlated with churn
  state that included a mount storm from my own investigation (mea culpa — the observer
  contributed load to the observed system) — but a later clean run (`new mgperf-selfwatch`,
  74.67s, zero concurrent ops from my side) reproduces the hang with no observer load at
  all. The remaining suspect is the operation's *own* internal detach→attach churn, possibly
  against ambient system DA state, rather than any external storm.

In short: the hang is DA/kernel contention correlated with detach→attach churn of
duplicate-UUID bytes, resolving on a tens-of-seconds timeout, invisible to every userspace
instrument except elapsed time.

## A considered-and-rejected mitigation

A user-space deadline (~45s) with bounded retry around the blocking mount was drafted and
then **deliberately cut**: a timeout cannot release kernel/DA contention — it fires after
the mount would have succeeded anyway, or kills real progress mid-flight — and a retry
re-enters the exact contention it just left. It would convert a slow success into a loud
failure plus a second slow attempt. No timeout/retry is included in this branch.

## Mitigations in this branch

1. **Per-leg timing spans** around every external step of prepare/commit
   (`sync`, `clonefile`, `attach`, `fsck`, `mount`, `rename`, `creds`, `marker`, `init`,
   `detach`, `publish`, `remount`), emitted as `cowshed: apfs <leg>/<step> start|done`
   lines on the existing stderr diagnostic path. The next investigation reads timestamps
   instead of needing dtrace.
2. **Detach-settle verification**: after every whole-device detach, the backend polls the
   attachment inventory until the device departs (bounded, logged) before any later attach
   re-enters DA. Soft by design — at the bound the operation proceeds exactly as before,
   but loudly — so we *measure* whether lingering departures correlate with hangs before
   hardening anything. No blind sleeps: a departed device costs one inventory read.
3. **Single-mount restructure** (proposal, not code): eliminate the detach→attach churn by
   mounting once. Crash-window analysis and the resume-vs-rollback comparison are written
   up separately; landing is sequenced *after* the settle data confirms the trigger.

## Ask for Apple (Feedback)

1. Is there a supported way to regenerate an APFS volume UUID in place (e.g. for
   cloned/duplicated volumes that must coexist), or a supported mount option to make DA
   treat same-UUID volumes as distinct?
2. What telemetry (beyond `semaphore_wait` on on-demand storagekitd) diagnoses a
   `mountDisk:options:blocking` stall of 24–62s that always self-resolves — and is there a
   supported way to quiesce DA between a detach and an immediate re-attach of the same
   bytes (the back-to-back sequence appears to be the trigger)?
3. Environment: macOS 26.6.1 (Tahoe), Apple Silicon, 322GB sparse image, APFS,
   `diskutil mount -mountOptions owners -mountPoint …`.

## Repro

`cowshed new <name>` from a large APFS-backed workspace under concurrent attach/detach
load; observe the first staging mount's PID age vs the volume's unmounted state. No
deterministic repro — contention-dependent.

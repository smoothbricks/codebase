# Orphan recovery — blocked on workspace publication

The operator’s **land-only directive** stops new roots and recovery preparation. This tranche has **no frozen recovery
units, commits, behavioral passes, landings or removals**. Existing queued units take precedence; original donors stay
custody.

## What the orphan counts mean

The split identifies **823 distinct orphan SHAs across 76 sheds**, not 823 landing units. The **367 rewrite-duplicate
appearances and 49 no-op transplants must never be cherry-picked**. Only **193 appearances** are mechanically
self-contained (`y`); the remaining `n` rows name prerequisites or conflicts. Repeated SHAs across sheds are one content
change, and even `y` proves neither semantic independence nor a passing gate. The directly recoverable set is therefore
much smaller; most content requires a separate dependency/conflict unit and remains blocked in this pass.

## Blocker and candidates — not frozen units

Fresh `cowshed fork` destinations did not publish: disk-image attaches remained running for minutes, source-build
sealing hit a DiskArbitration detach timeout or a busy build volume, and a named resume refused an active lifecycle
lease. The resolution and cause-chain attempts ended with statuses 143/137 after 1532.21s / 1518.26s, respectively,
without usable roots. Public-export, gateway-fixture and sandbox attempts were deliberately stopped by the coordinator
to release wedged store-lock holders; storage services were restarted. These events authorize no further preparation or
removal. Full receipts remain in local evidence.

Observed basis: `c0e3d162fb20d5835ae90bbd29844d24e2a26b01`.

| Pinned source SHA                          | Independent candidate                                            |
| ------------------------------------------ | ---------------------------------------------------------------- |
| `b93b15da023e4616ee64dd31a0df541ace80a4e1` | LMAO cause-chain prerequisite; first if recovery is reauthorized |
| `029fa4aec8f079fef9c719a6ac1a81aca0be6b97` | Bounded Cargo resolution                                         |
| `d965dac0b596ae7de06c7e7fa13a0d656e837bcb` | Inspector export conditions                                      |
| `69170df3cd9bb83bf9b437e24690a1c6b577fd03` | Gateway fixture grants; separate unit                            |
| `a0f1244ce7152398345bb289f8e710380cba180a` | Checkout-local sandbox build state                               |

`d05856c33ccf527b6bfb7cb2c72ee577b4f50157` has the same measured patch and postimages as `029fa4a`: import once only.
Grant-isolation source `2270c6e119e3d1c720dee4edf77726c8059c7873` is not admitted: its return value and obsolete fixture
CLI calls require excluded dependent commits. Source-shed identities and intended roots are retained only in the local
custody evidence.

## How to use the evidence without loading it

Set **`EVIDENCE_ROOT` to the local evidence directory**. Do not load the 1.6 MB
`$EVIDENCE_ROOT/shed-orphan-split-20261014.md` whole. Grep a shed name to jump to its row/heading, then read only the
needed selector:

- `:3-15` — Result and count meanings.
- `:72-168` — Per-shed split.
- `:194-1383` — ORPHAN TABLE; narrow further to the shed’s rows.
- `:1384-1439` — NO-OP table, never a pick list.
- `:1440-5367` — Per-shed ordered cherry-pick lists; read one shed section.
- `:5368-5677` — Deduplication evidence.

Prefer these smaller files beneath `$EVIDENCE_ROOT`:

- `shed-orphan-*-20261014.json` (15 KB): structured recovery dispositions; query with `jq '.counts'` or `jq '.sheds'`
  rather than reading whole.
- `orphan-rest-held-20261014.json` (55 KB): held remainder; query `jq '.sheds'`.
- `sb-orphan-recovery-*-20261014.md` (13 KB): all 27 source dispositions.
- `sb-orphan-dependency-plans-*-20261014.md` (337 KB): exact prerequisite/conflict quotations; grep the requested shed
  and read that section, not the whole file.

The split’s §§Method and meaning of labels, Self-contained y/n, ORPHAN TABLE and ordered lists govern classification.
Local reports are evidence, not acceptance.

## Resume only after explicit operator authorization

1. Assign one owner, read the source’s governing sections and local custody record, observe current main, and establish
   that any old named operation is terminal.
2. Make one authorized `cowshed --project <repo> --json fork main <unit> --clean` attempt. Require published path/marker
   and current-main basis; preserve a refusal or unfinished intent. Never loop, rewrite its journal or force a mount.
3. Recheck containment; import only admitted real orphan content with `git -C <root> cherry-pick --no-commit <SHA>`. Do
   not silently include `n` repairs. Resolve reviewed conflicts via icm and retain receipts. LMAO conflicts are only
   specs 01c/01f/01h; preserve newer result ownership. Sandbox needs the existing fixture-owned `profile_directory`
   field. Commit each coherent unit, no trailers.
4. Run owning focused Nx acceptance with `NO_COLOR=1 CARGO_BUILD_JOBS=4` and
   `--parallel=2 --outputStyle=static-failures-only`; LMAO uses `bun nx run-many -t lint test build -p lmao`. Freeze
   root/base/HEAD plus proof; hand to the gate fleet and sole lander, prerequisite first. Never self-land.

## Protected custody and governing contracts

Do not mutate, detach or retire original donors, ignored evidence, unfinished images/intents, queued land units,
`landing-queue-sheds.txt` entries or live-owner exclusions. Contained commits, checkpoints and reports never authorize
dirty-state loss. Follow `specs/cowshed/02_workspaces.md` §§The safe-to-clone-main invariant and `cowshed rm`,
`packages/cowshed/docs/cli.md` §`cowshed fork`, and `AGENTS.md` Portability rule and the Acceptance line of Commands.
Keep private paths and session identities in local evidence, not public code, documentation or history.

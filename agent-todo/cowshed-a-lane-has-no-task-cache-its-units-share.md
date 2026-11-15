# cowshed: the units of a lane recompute what the lane already proved

A workspace's Nx cache lives on its own build volume (`.nx/cache` → `.cowshed/build/nx/cache`), cloned from the lane
when the unit is forked. The shared task cache the gateway serves is read-only for a workspace
(`NX_SELF_HOSTED_REMOTE_CACHE_SERVER` with `NX_SELF_HOSTED_REMOTE_CACHE_READ_ONLY=true` in `.cowshed/env`), and it is
written by the land of main. So a result computed after a unit's fork is seen by no other workspace until main lands.

## What that costs

A lane that is not yet on main (a batch of units stacked for one land) moves by folds. After each fold every unit of the
lane is rebased onto it and re-runs its checks. The tasks whose inputs the fold changed, and which the unit itself did
not touch, have the same hash in every unit, and each unit computes them again: nothing has published them.

Measured on 2026-11-15, one repository, a lane with four to five live units: the host load averaged 28 to 43 for ten
minutes five times in one afternoon (1.6 to 2.4 times its 18 cores), each time with three to five units running their
checks at once after a fold or a rebase. The same build of the runtime and the same typechecks ran once per unit.

## What is wanted

- A lane has a task cache its units read and the lane writes: after a fold, one trusted run in the lane's own workspace
  publishes the tasks of the folded tree, and the units' checks restore them.
- Or a unit's own results are published under its task hashes into a lane-scoped store, since a hash names the inputs
  exactly; what must be established first is why workspaces were made read-only (an untrusted workspace poisoning a
  shared cache) and whether a lane-scoped store bounds that.
- A unit's first run after a rebase reports how many tasks it restored and how many it computed, so the cost is visible
  per unit.

## Done when

- Two units of one lane, rebased onto the same fold, run the same check, and the second restores every task the first
  computed that neither unit's own commits touch.
- The afternoon above is re-measured: units running checks after a fold, host load, tasks restored.

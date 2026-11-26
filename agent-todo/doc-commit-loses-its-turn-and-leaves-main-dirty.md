# doc-commit loses its turn and leaves main dirty

Found 2026-11-26 on a consumer repository's main at host load 46-66 with 0% idle. Owner: cowshed.

## What happened

- **The turn bound covers direnv.** `cowshed doc-commit` runs `[doc] check` and the commit's pre-commit hook under
  direnv. Under load the direnv entry alone outlasted the 120 s bounded-turn deadline: one `direnv exec . true` in that
  main took more than 200 s, with a direnv setup script in state U. Four doc-commits in a row lost the turn with
  "exceeded bounded-turn deadline (120 s); direnv was sent SIGTERM".
- **A lost turn doesn't put the tree back.** The failure line says "main's tree is back as it was", but three runs left
  their applied patch in main's working tree (one queue file, three spec files), and one left a stale `.git/index.lock`
  with no holder. Every later doc-commit was refused with "main's tree holds uncommitted edits", the author's own retry
  included, until the tree was cleaned by hand.

## What must hold

- Whatever stops a doc-commit, main's tree and index end exactly as they were, with no leftover lock: the failure line's
  promise holds.
- The bound covers the work the turn protects (apply, check, commit), not an environment load that depends on host load.
  The environment loads before the turn is taken, or the check runs with the environment main's checkout has already
  resolved.

## Red

A doc-commit whose check outlives the bound leaves `git status` on main empty and no `.git/index.lock`.

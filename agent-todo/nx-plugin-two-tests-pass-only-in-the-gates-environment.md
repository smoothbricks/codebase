# Two nx-plugin test groups pass only in the land gate's environment

Found 2026-11-27 while diagnosing a red `nx-plugin:test-shard3` gate. Run outside the gate, through a plain
`cowshed exec <ws> -- bun test <file>` in a consumer-of-nothing shed, the same 13 tests fail on the parent commit and on
the tip alike, so no change caused them; the gate's environment hides them.

## 1. The owning reader refuses Bun through cowshed's manager-bin link (12 tests, and production)

`src/graph-input-contract.test.ts`, every test in "owning-reader external input byproducts" (12): `finish().status` is
`unproved` where `complete` is expected. Its detail is `Bun runtime is not the PATH-selected executable`.

`GraphInputReadCollector` (`src/graph-input-contract.ts`, ≈:244-249) resolves `bun` on `PATH` (`executable('bun')`,
≈:378-400) and refuses unless `realpath` of that candidate equals `realpath(process.execPath)`. In a cowshed exec,
`PATH` leads with `<ws>/.cowshed/tools/manager-bin`, whose `bun` links to a cowshed shell-host executable
(`.cowshed/shell-host/<key>`), a Mach-O that runs the store Bun. `process.execPath` is the store Bun
(`/nix/store/…-bun-1.4.5-canary…/bin/bun`). The realpaths differ though the same Bun runs, so the reader refuses.

That is not only a test defect: every owning read made by a Bun that a cowshed shell selects, which is a developer's
shell and every plain `cowshed exec`, publishes an `unproved` witness, so the graph-input proof never holds there. Only
the gate's environment, whose `PATH` names the store Bun first, sees `complete`.

What must hold: the reader's "the PATH-selected Bun is the running Bun" check accepts cowshed's manager-bin link to the
running Bun and still refuses a different Bun. The test then runs under the PATH the caller gives it, or sets its own,
and names which.

## 2. repo-path's expected home is the caller's HOME, not the passwd home (1 test)

`src/managed-files/repo-path.test.ts:161` "reaches the installed cowshed through the real uid's passwd home when the
caller passes only PATH" computes its expectation as
`join(userInfo().homedir, 'Library/Application Support/dev.cowshed/bin')` (:170). Under cowshed exec
`userInfo().homedir` is the shed's `.cowshed/home`, while the script under test, as the test's own name says, reaches
the real uid's passwd home (`/Users/<user>`), so the expectation names the wrong directory. The expected home must come
from the same source the script reads (the passwd entry for the real uid), not from the runner's environment.

## Red

Both files, run as `cowshed exec <ws> --class interactive -- sh -c 'cd packages/nx-plugin && bun test <file>'`: 12 of 18
fail in `graph-input-contract.test.ts`, 1 of 9 in `managed-files/repo-path.test.ts`. They pass once each test controls
the inputs it asserts on.

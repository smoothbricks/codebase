# smoo monorepo update puts a raised ttsc pin back to the lockfile's version

`packages/cli/src/monorepo/tool-validation.ts` (`pinnedFromLockfile`, l.82; the check at l.581-583 and its message at
l.607-612; "updated package.json workspace tool dependencies" at l.192 and l.224).

## What is measured

On 2026-11-17 a consumer workspace moved ttsc from 0.36.3 to 0.37.1. Its root `package.json` was edited to
`"ttsc": "0.37.1"` with the matching `patchedDependencies` key, and `smoo monorepo update` was run to bring the managed
`patches/ttsc@0.37.1.patch`.

The update printed "updated package.json workspace tool dependencies" and wrote the pin back to `0.36.3`: the policy
takes the tool's pin from `bun.lock`, which still held the old version because nothing had been installed yet. The
install the update then ran gave the workspace an unpatched ttsc 0.36.3 under a `ttsc@0.37.1` patch key.

The two steps depend on each other. Installing 0.37.1 first needs the managed patch for 0.37.1, and only the update
writes that patch. What worked: update, set the pin to 0.37.1 again, `bun install`, update again (which then changed
nothing).

## What to change

A pin a person raised in `package.json` is the statement of intent; the lockfile follows it. When the managed patch for
the version `package.json` names exists, the update keeps that pin, writes the patch and installs. It reads a pin back
from the lockfile only where `package.json` names none.

## Done when

- A workspace whose `package.json` names the managed patch's ttsc version and whose lockfile still holds the previous
  one comes out of one `smoo monorepo update` on the new version, patched, with the pin unchanged. A test holds it at
  the policy's boundary.
- A pin that names a version no managed patch exists for is refused by name; it is not rewritten.

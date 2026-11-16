# smoo monorepo validate rewrites a package.json while it runs

`packages/cli`, `smoo monorepo validate` (its package-hygiene step, the one that packs).

## What is measured

On 2026-11-16 `smoo monorepo validate` was run in a consumer workspace's main checkout and was ended by a 180 s deadline
while it was still running. Afterwards the checkout was not clean:

- one tracked `packages/<name>/package.json` was modified: the same data, re-serialised (38 lines differ: escaped
  characters written out, the `types` member of each export moved ahead of its siblings);
- an untracked `.bun-pack.<hash>.tmp` of 860 KB lay at the workspace root.

Both were written in the second the command died. A watcher of build inputs in that checkout saw the manifest change and
started a native build. Restoring the file from HEAD and removing the temporary file put the checkout back.

So the command edits a tracked file in place for the length of a step and puts it back at the step's end. Which call
does the rewrite was not traced; it is the pack that the hygiene step runs, or something the step does around it.

A command named validate is run to learn about a tree, often under a bound, beside editors, watchers and agents that
read the same files. For as long as it runs, each of them sees a manifest nobody wrote.

## What to change

Validate changes no tracked file, at any moment of its run. Where a step needs a rewritten manifest, it works on a copy
outside the checkout, or on a pack's output, never on the file in place.

## Done when

- `smoo monorepo validate` ended by a signal at any point leaves `git status` clean and no temporary file in the
  workspace. A test ends it during the hygiene step and asserts both.
- A file watcher over the workspace's `package.json` files sees no event during a complete validate.

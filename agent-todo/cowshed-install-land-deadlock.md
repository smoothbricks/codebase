# An install and a land deadlock on the turns

Seen 2026-11-11, 17:27 to 17:56, minigraf repository, land `sb-pin`.

## What happened

1. 17:26:53: `cowshed land sb-pin` took the land turn `minigraf` and started its gate.
2. 17:27:00: the after-land job of SmoothBricks land `4642fd1e9d` ran `tooling/land-install.sh`, which ran
   `cowshed gateway await-turns -- …setup`. It waits for every land turn held when it asked: `sb-pin`'s.
3. 17:53: `sb-pin`'s gate passed (1617 s). Its next step is the fast-forward under the ref turn `minigraf.ref`. The
   gateway answered: "land waits for the ref turn minigraf.ref: parked behind an install that waits for the turns or
   fences them".
4. Neither moved for 29 minutes at 0% CPU. `cowshed gateway status` showed "land turn minigraf: sb-pin holds it". Ending
   the `await-turns` process let the land fast-forward at once.

## Why

The land takes two locks in order, the land turn and then the ref turn (02_workspaces.md, "A declared land", step 5:
"Lock order is always the land turn, then the ref turn"). An install queued between the two is ordered after the land
turn and before the ref turn the same land asks for later. The install waits for the land; the land's ref turn waits for
the install.

## What the fix has to hold

- A land that holds its land turn takes its ref turn ahead of any install that asked after that land turn was taken: the
  install's order among the lands is its order against land turns, and a ref turn asked by a land already ahead of it is
  part of that land.
- Red first: a gateway test with a land holding its turn, an `await-turns` asked next, then that land asking its ref
  turn. Today both park; it must grant the ref turn.
- `cowshed gateway status` names an install that waits and what it waits for, as it names a land turn's holder.

Also seen: the install was for a head whose `packages/cowshed` tree equals the installed one
(`git diff --stat f6a1ef893c 4642fd1e9d -- packages/cowshed` is empty). An install that would change nothing should not
ask for the turns at all.

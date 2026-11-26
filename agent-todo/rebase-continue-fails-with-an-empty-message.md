# rebase --continue fails with an empty message

Found 2026-11-26 while a workspace's rebase resolved conflicts. Owner: cowshed.

`cowshed rebase --continue` answered:

```
{"code":"conflict","message":"continue the replay failed: "}
```

The cause was a resolved path whose worktree copy had a later edit than its staged copy. A `git add` of that path made
the next `--continue` succeed, but nothing in the answer said so: git's own reason was dropped from the message.

## What must hold

- A refused `--continue` names its cause and the paths involved, as git reported them, so the caller can act without
  reading git's state by hand.
- A resolved path whose worktree differs from its staged copy is reported by name, with what to do (stage it, or discard
  the later edit).

## Red

A replay stopped on a conflict, the conflict resolved and staged, then the path edited again without staging:
`--continue` refuses and its message names the path and the unstaged edit.

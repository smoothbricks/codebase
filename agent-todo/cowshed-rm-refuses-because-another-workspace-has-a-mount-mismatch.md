# cowshed rm refuses because another workspace has a mount identity mismatch

Found 2026-11-26 while retiring an empty workspace. Owner: cowshed.

`cowshed rm nx-plugin-mut` (also with `--abandon`) refused. The workspace held no work. The refusal came from two
unrelated workspaces of another project, which every listing reports as:

```
cowshed: warning: <project>/t5-pre-move is not listed: it is neither attached nor detached: workspace mount identity mismatch at …
cowshed: warning: <project>/t5-resolution-red is not listed: it is neither attached nor detached: workspace mount identity mismatch at …
```

`cowshed doctor` walks every workspace's marker but does not repair or retire these two, so the state persists and keeps
blocking an unrelated removal.

## What must hold

- Retiring one workspace depends only on that workspace's own state. Another workspace's mount mismatch is reported,
  never a reason to refuse.
- A workspace whose mount identity does not match its record has a way out: `cowshed doctor` (or `rm --abandon` on it)
  repairs it, or retires it after bundling what it holds, and says which.

## Red

With one workspace in a mount identity mismatch, `cowshed rm` of a different, empty workspace succeeds, and `doctor`
names the mismatched one with the action that resolves it.

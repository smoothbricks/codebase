# `cowshed doc-commit` hides a hook's refusal and leaves a new file staged in main

Met 2026-11-25 committing a new `agent-todo/*.md` through `cowshed doc-commit --patch … -F …`. The commit-msg hook
refused the message ("Invalid conventional commit scope: agent-todo"). Two defects:

1. **The refusal hid its cause.** The first attempts printed only
   `cowshed: the edit did not commit, and main's tree still differs from HEAD at: agent-todo/<new file>` and
   `next: put those paths back by hand: git -C <main> restore --source=HEAD --staged --worktree -- <path>`. The hook's
   own reason appeared only on a later attempt, after the leftover path had been cleared by hand. A refusal must print
   the hook's reason (its stderr's last lines, or the whole of it when short) the first time, beside cowshed's own line.
2. **A failed doc-commit left main as it was not.** The edit added a new file. The rollback restored paths "to HEAD",
   which a path HEAD does not hold cannot be: the new file stayed staged (`AM`) in main's index and tree, and the next
   doc-commit refused it as "someone else's edit". Its `next:` line names a restore that fails for that path. A failed
   doc-commit must leave main's index and tree exactly as they were before it, a new file included (removed from the
   index and the tree), and never tell the caller to restore by hand what it applied itself.

Red first for each: a doc-commit whose commit-msg hook refuses prints the hook's reason on its first refusal; and a
refused doc-commit that adds a new file leaves `git status --porcelain` of main as it was before the call.

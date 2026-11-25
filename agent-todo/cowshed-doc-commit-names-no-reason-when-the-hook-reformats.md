# cowshed doc-commit names no reason when the hook reformats

`cowshed doc-commit --patch` with a Markdown file whose paragraphs ran past 120 columns exited 4. It said only "the edit
did not commit, and main's tree still differs from HEAD at: <path>" and left the file in main's tree as `AM`. The
`[doc] check` (tooling/checks/doc-lane.ts) passed when run by hand. The pre-commit hook had rewrapped the file and the
commit stopped, but nothing in the output said so (2026-11-25 22:25 +04, three attempts).

What must hold: a refused doc commit names the step that refused it (check, hook, or commit) with that step's own
output, and puts main's tree back itself, including a new file it added. Rewrapped Markdown is either committed as the
hook formatted it, or refused with the formatted diff, so the caller can resubmit it unchanged.

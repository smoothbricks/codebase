# A fork's first Git command rewrites the index and discards the kept developer-link snapshot

Measured on 2026-11-27 with the dtrace pid provider on a fresh consumer fork's workspace supervisor, after the kept
readings stopped watching `.git` lock churn (`perf(cowshed)` 7e91bd78e, 9418e4b1d, c968830d8):

| Exec in the fork                              | Supervisor lookups |
| --------------------------------------------- | ------------------ |
| repeat `true`, kept readings hit              | 11,189             |
| right after the fork's first `git status` job | 39,495             |
| right after any later `git status` job        | 11,194             |

The first `git status` in a fresh fork rewrites `.git/index`: the image clone gives every tracked file a stat Git did
not record (its cached ctime, inode or device no longer match), so Git refreshes the index and writes it. `.git/index`
is a present input of the developer-link capture's listing (`kept_listing_inputs`), so that write moves it, and the next
exec re-lists every package tree once (11_shell.md §Kept readings). Every later Git command leaves the index as it is,
and the snapshot is kept.

## What to do

- Find which stat fields an APFS image clone changes for a tracked file that Git's index records (`ce_stat_data`: ctime,
  mtime, dev, ino, uid, gid, size), on the clone and on the build volume's mount.
- Have the fork hand over an index whose cached stat data matches the clone. For example, refresh it once inside the
  fork's own provisioning, before its supervisor admits a job and captures its first snapshot, so no job pays for it.
  The refresh is Git's own (`git update-index --refresh`, or the equivalent the fork already runs), never a hand rewrite
  of the index.
- Red: in a fresh fork, the first `git status` job leaves `.git/index` unchanged, and the next exec's developer-link
  snapshot is kept: no `capture_reading`, a supervisor lookup count like the repeat-hit row.

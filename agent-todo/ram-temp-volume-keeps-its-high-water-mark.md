# The RAM temp volume keeps its high-water mark for as long as leases overlap

Measured 2026-11-25 12:47 (host swap 18 of 19 GiB):

- diskimages-helper pid 35900, attached at 09:38 (`.smoo-ram` mtime), serves cowshed's 8 GiB `ram://16777219` volume at
  the per-user `ram3`.
- `vmmap --summary 35900`: physical footprint 7.8 GB, MALLOC_LARGE 8.0 GB virtual, of which 7.7 GB is swapped or
  compressed.
- The volume has 47 MB in use (`df`): eight live leases (`<pid>-<6>`) and two archives.

A DiskImages `ram://` disk never returns to the host a page that was freed inside it. Its helper's footprint is the
highest the volume ever reached since attach, and the kernel compresses and swaps those pages while nothing uses them.
nx-plugin README "RAM temp" already states the reclaim rule: "detached when the last live lease ends, since its pages
return only on detach". That rule never fires while tasks run back to back: on a busy host some lease is always live, so
the disk has stayed attached for three hours, holding about 8 GB of dead pages.

Owed, by the owner of the RAM temp volume (nx-plugin bounded-exec's lease and the gateway's `ram-lease`, 05_gateway.md
"RAM disks"):

- Reclaim without waiting for a moment with no live lease. For example, roll to a fresh volume: once the volume's
  attached high-water exceeds what its live leases hold by a bound, new leases go to a new disk (`ram3-new` already
  names a second mountpoint in the test layout), and the old disk is detached when its last lease ends. Or bound the
  size of the disk to what the concurrent leases need.
- The archives (the two newest failing runs) must survive a roll. Today they live on the same volume so that a lease can
  be archived with one `rename(2)`.
- Red: with one long-lived lease held throughout, write and delete N GB through other leases. The helper's physical
  footprint, from `proc_pid_rusage` or `vmmap --summary`, returns below a bound within one idle interval. Today it stays
  at N GB until the long lease ends.

Evidence commands: `vmmap --summary <helper pid>`, `hdiutil info` (`image-path : ram://…`), and `df` and `ls -la` on the
volume.

Measured again 2026-11-27 21:40, from swap alert 20261127-213327 (host swap 8.76 of 9.2 GiB):

- diskimages-helper pid 67840, attached ~18.6 h, serves `ram://16777219` at `ram3`. `vmmap --summary 67840` gives a
  physical footprint of 8.0 GB, about 7.2 GB of it compressed.
- `df` shows 29 MB in use. The only lease is `64670-SWRyyY` (07:45, 3.4 MB), and pid 64670 no longer exists.
- So the volume isn't held by overlapping live leases but by a dead owner's lease. A lease whose pid has exited isn't
  reaped, so "detached when the last live lease ends" never fires even on an idle host.
- Also owed: a lease whose owner pid is gone counts as ended, and it's archived or removed by the same path as a normal
  end. Red: take a lease in a child, kill the child with SIGKILL, and the volume detaches within one idle interval.

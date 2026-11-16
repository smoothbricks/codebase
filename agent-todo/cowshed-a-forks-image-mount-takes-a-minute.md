# A fork's image mount takes a minute and stalls another volume's flush

Measured on 2026-11-16 in one provision of a workspace by an executor client (spans of the cowshed controller).

## What was measured

A provision by clone took 82.7 s:

| step                              | wall   |
| --------------------------------- | ------ |
| `cowshed.fork.build-volume`       | 1.9 s  |
| `cowshed.fork.build-volume-ready` | 72.7 s |
| `cowshed.apfs.canonical/init`     | 7.1 s  |
| `cowshed.fork.links`              | 6.3 s  |

Inside `build-volume-ready`, two `cowshed.apfs-command.mount verified APFS volume` commands ran concurrently from the
same instant and took 46.6 s and 72.3 s. Each sits inside a `cowshed.disk-lease.namespace phase` of the same length; the
only `disk-lease.namespace wait` is 88 ms, so the time is inside the mount command, not in a lease or a queue. The two
attaches before them took 180 ms and 277 ms, and both fsck spans are under 50 ms.

During the first mount, an `F_FULLFSYNC` by another process on another volume of the disk-image layer (a store on a
volume that is itself a cowshed image) began 4 ms after the clone and returned 6 ms after that mount ended: 48.8 s for a
flush whose median over 1,067 others was 13.6 ms. No kernel stack was sampled, so the mechanism is an inference from the
two coincidences. Flushes during the remainder of the second mount ran at 15 to 36 ms.

The host's vnode table was full at the time (8,388,609 of 8,388,608).

## What to find

- Why `mount` of a just-cloned APFS image takes 47 to 72 s when its attach and fsck take under 300 ms: sample the mount
  command and `diskarbitrationd`/`apfsd` during one, per the profiling rule, before any change.
- Whether the two concurrent mounts wait on each other (the second took 26 s longer than the first).
- Whether a full vnode table is the cost (a mount that must recycle vnodes to proceed).

## Done when

A provision by clone on a host in the same state mounts each image in under two seconds, or the report names the kernel
cost that cannot be avoided and the provision stops paying it twice (one image, or mounts in sequence if they contend).

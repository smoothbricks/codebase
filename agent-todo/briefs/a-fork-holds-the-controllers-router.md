# A fork holds the controller's router, and its build volume waits on a seed

## Observed

One integration case of a consumer drives a small fleet through a cowshed controller of its own (a scratch project, a
handful of worker workspaces forked in turn). Alone on a host with idle cores it takes 470 to 511 s. A process listing
every 20 s over the whole case shows CPU work for the first 100 s and then five minutes in which no process of the tree
is on a CPU: the runtime, the Nx daemon, the test and the controller all wait. A 5 s `sample` of the runtime in that
window holds only `__psynch_cvwait`, `kevent` and `__ulock_wait`.

What they wait on is said by the controller's own client:

```
its job's status could not be read (SILENT: controller call: cowshed controller on a socketpair, router busy:
coordinator.fork <workspace> for 90298 ms showed no progress for 15.0 s (idle bound 15s); waited 183.5 s in all,
last progress: heartbeat 40; cowshed controller stack capture failed: TIMEOUT: sample silent peer …: no answer within
its ffi bound of 10s
```

and by the fork it was busy with:

```
cowshed: <sibling>'s seed stays 69831 ms behind its build volume, so <workspace> misses what <sibling> built since
cowshed: image clone before … role=source / role=destination
cowshed: fork build-volume done elapsed=45.3488705s status=ok
```

Every other `fork build-volume` step of the same run takes 0.1 ms to 2.7 s.

## Two defects

1. **A fork occupies the router.** While `coordinator.fork` runs, a status read of another job gets no answer; the
   caller's idle bound (15 s) fires, it reports the controller silent, and the reader's whole wait was 183 s. A status
   read touches nothing the fork writes. Read `specs/cowshed/` on the controller's router and on what a fork holds
   before changing it: state which calls a fork must exclude and let the rest through.
2. **One fork's build volume took 45 s** where its siblings take milliseconds, with the note that the sibling's seed is
   70 s behind its build volume. Find from the phase timings (`fork build-volume`, `image clone`, the disk lease's
   storage wait of 10.0 s that follows) what the 45 s are: a wait for the seed, a full image clone where a clone of the
   seed would do, or the disk lease. Profile it; do not guess.

Also: the stack capture of the silent controller timed out at its own 10 s bound, so the report that should have named
the frame named nothing.

## Done when

- A status read answers while a fork runs (a test with a fork held open at its build-volume phase).
- The fork of a workspace whose sibling built since the seed takes the time of its siblings' forks, or the spec says why
  it cannot and the caller is told the wait by name.
- The consumer's case runs in the time its CPU work needs (about 100 s on this host).

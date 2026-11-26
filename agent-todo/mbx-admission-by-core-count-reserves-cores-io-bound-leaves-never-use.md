# mbx admission by core count reserves cores that I/O-bound leaves never use

Measured on 2026-11-26 at 16:33 on the host, from the shared pool cowshed's jobs use
(`/private/var/folders/cowshed/shared/build-cache/mbx/scheduler`, mbx 1.30.0, mr-boxington b307e62e2377).

## What was measured

- All 18 cores were held by 18 one-core leases: 7 and 6 by two low-priority reservation servers (an `nx run-many` of
  typecheck, lint and test, and one project's test target), 3 by a normal-priority gate, 2 by rustc compiles.
- 29 requests waited, almost all low-priority one-core reservations. Low reservations admitted at about 16:30 had waited
  1,915 to 1,926 s; normal ones waited 3 to 7 s. The pool admitted 82 requests in the 20 minutes before.
- The host was 43.7% idle (`top`), and the pool's own pressure sample said `pressured: false`.
- The seven grants of the first server were seven `bun test` leaves at 0 to 11% CPU each; the vnode table was full
  (8,391,264 of 8,388,608), so the leaves waited on the filesystem while each held a core.
- No lost wakeup and no leaked hold: each admission's `free` equals 18 minus the leases, the threads parked in
  `Pool::reserve_observed` → `Wait::pause` are low requests waiting their turn, and `inflight/` files are not counted.

## Where

- `crates/mbx/src/cli/reservations.rs:381-446`: each `reserve` request waits on a thread of its own in
  `Pool::reserve_observed` and is granted whole cores, held until the client releases it.
- The `ordered` rule in the scheduler lets normal and interactive requests take a freed core ahead of every low one.

## What to find

- Whether admission can count what a holder uses rather than what it was granted (the pool already samples pressure), so
  a pool whose holders are idle on I/O admits more while the host has CPU to spare, and stops when it does not.
- Whether low requests need an aging bound against normal ones, so a low gate does not wait half an hour behind a stream
  of normal compiles.

## Done when

On a host in the same state (every core leased, the host mostly idle, leaves I/O-bound), waiting requests are admitted
until measured CPU is the limit, and a low request's wait is bounded.

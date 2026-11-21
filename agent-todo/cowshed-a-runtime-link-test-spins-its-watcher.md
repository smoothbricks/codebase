# A runtime-link test spins a watcher thread on lstat while its retargets wait on the file system

`packages/cowshed/crates/cowshed-core/src/runtime/supervisor.rs`, the test
`runtime::supervisor::workspace_toolchain_tests::retargeting_a_runtime_link_never_leaves_its_path_absent`.

## What is measured

The test retargets one symlink 2,000 times with `point_runtime_link_blocking` (each a temporary link and a rename) while
a watcher thread calls `symlink_metadata` on the link in an unbounded loop until a flag flips. The loop has no wait in
it.

On 2026-11-21, smoothbricks land gate of `retired-mbx-clean`, task `cowshed:cargo-test-cowshed-core-shard7` was ended as
wedged after 506 s (idle bound 180 s). The bounded-exec hang snapshot (terminal output `4682406682344980302`) shows the
test process (pid 57510) at 83 % CPU with 2:11 of CPU time, running this test, while 17 other tests of the shard sat
idle. The host was in a /private/tmp metadata stall at the same minutes: other shards' processes were in state U on
renames, and single renames took up to 0.8 s.

So the retargets crawl on the stalled file system while the watcher burns a core doing lstat on the same directory,
which adds to the metadata load the renames wait on. The test proves its property only by sampling as fast as it can.

## What to change

The watcher observes the link without a busy loop: for example it samples only around each retarget (the retargeting
thread and the watcher hand over each round), or each lstat is paced by the retargets themselves, so the test does
bounded work per round. The property stays the same: no observer ever finds the link's path absent.

## Done when

The test does at most a constant number of lstats per retarget, it passes on a quiet host, and under a stalled file
system it uses no more CPU than its own retargets.

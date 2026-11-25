# macOS spawns through posix_spawn with POSIX_SPAWN_CLOEXEC_DEFAULT, and creating a descriptor takes no fork lock

Parked 2026-11-25 for Danny's ruling on three spec decisions below. spawn-off-runtime (option B) already keeps every
fork-lock wait off the supervisor's and gateway's runtime threads. What this unit would add: an accept or a pipe no
longer waits behind a long exec in flight, and the supervisor listeners' accept→`FD_CLOEXEC` window
(supervisor_socket.rs:1437/1443, supervisor_manager.rs:941) closes for every descriptor.

## Why

Darwin has neither `pipe2` nor `SOCK_CLOEXEC` (01c_macos.md:1076-1089). So every descriptor is created and marked
close-on-exec under the exclusive fork lock (`fork_lock::create`), and every spawn takes the lock shared
(11_shell.md:792-821, cowshed-os fork_lock.rs:1-63). `POSIX_SPAWN_CLOEXEC_DEFAULT` closes every descriptor in the child
except those the file actions name, atomically. A spawn would then inherit exactly what it names, whatever another
thread creates meanwhile.

## What exists (quiz on spawn-off-runtime at 8c680f2a06)

- **std.** It uses `posix_spawnp` on Apple only with no `pre_exec`, no setsid and no relative program with `current_dir`
  (library/std/src/sys/process/unix/unix.rs:446-477, 657-676, 765-774). It never sets `CLOEXEC_DEFAULT`, and
  `CommandExt` has no hook for spawn flags. So the macOS spawn moves into cowshed-os on `libc` 0.2.189, which exports
  `posix_spawn*`, `POSIX_SPAWN_CLOEXEC_DEFAULT` (0x4000), `POSIX_SPAWN_START_SUSPENDED` (0x0080), SETPGROUP, SETSIGDEF
  and SETSIGMASK.
- **Missing declarations.** Two small Darwin declarations are missing: `POSIX_SPAWN_SETSID` (0x0400) and
  `posix_spawn_file_actions_addinherit_np`. `nix::spawn` lacks most of these.
- **The macOS `pre_exec` sites map cleanly:**
  - exec/macos.rs:64's per-fd `FD_CLOEXEC` sweep is replaced by `CLOEXEC_DEFAULT`;
  - setsid (child_fence.rs:147, compaction.rs:126, supervisor_manager.rs:137 with addinherit of the report fd) maps to
    `SETSID`;
  - shell_job.rs:1319 (setpgid plus dup2 to 3) maps to `SETPGROUP` plus `adddup2`/`addinherit_np`;
  - supervisor.rs:3051 maps to `SETPGROUP`;
  - held_lock.rs:317 maps to `addinherit_np` per fd;
  - gateway_host.rs:105 maps to `SETSIGMASK` with an empty set;
  - the leader gate (supervisor.rs:3078, process_monitor.rs:315) maps to `START_SUSPENDED`: the parent watches and
    places the pid, then SIGCONT, or SIGKILL and reap on error.
- **Linux is unchanged.** It keeps pidfd/clone3, pipe2/SOCK_CLOEXEC, `pre_exec` and the lock, which a fork+pre_exec
  spawn still needs for `Fenced::drop`.

## Decisions for Danny

1. **Land-turn bind.** 05_gateway.md:564-570 says "A command binds itself: … between its fork and its exec, the child
   writes the bind step". posix_spawn runs no child code, so the bind moves to the parent: spawn `START_SUSPENDED`,
   write bind, read bound, then SIGCONT. That opens a new window: an asker killed after bound and before SIGCONT leaves
   the turn held by a child stopped forever. Options:
   - (a) the child gets its own process group, so the asker's death orphans the stopped group and the kernel's
     SIGHUP+SIGCONT resumes or ends it;
   - (b) the gateway resumes or ends a bound pid whose asker died;
   - (c) this path keeps fork+pre_exec. (a) and (b) change 05:564-570, 05:606 and 02_workspaces.md:1297/1403.
2. **Detached disk tools** (cowshed-apfs lib.rs:1862). They set `SIG_IGN` for HUP/INT/TERM in `pre_exec`. posix_spawn
   can block these signals (`SETSIGMASK`) but not ignore them. The blocked mask survives exec and is what stops diskutil
   (lib.rs:1680, 8525), so `SETSIGMASK` alone may be enough. A red would prove it.
3. **Scope of the lock on macOS.** With every macOS spawn a posix_spawn with `CLOEXEC_DEFAULT`, no user-space fork→exec
   window remains. Only XNU's in-kernel transient copies do, which 01c_macos.md:1055 already handles by waiter grants.
   `fork_lock::create` and `Fenced::drop`'s exclusive close both become no-ops on macOS. This changes
   11_shell.md:792-821 and 01c_macos.md:1055-1089, and scopes about eight lock-contention tests to Linux
   (fork_lock.rs:514/586/612, build_volume/mod.rs:2988, cowshed-apfs lib.rs:8956, shell_pool.rs:2777, gateway.rs:515).

Recommendation: 1(a), 2 proven by a red, 3 yes.

## Reds, once ruled

- `fork_lock::tests::descriptor_creation_does_not_wait_on_a_spawn_in_flight` and
  `gateway_control_accept_does_not_wait_on_a_spawn_in_flight` (gateway.rs) cover an accept or pipe creation on another
  thread returning while a spawn is parked in flight.
- `fork_lock::tests::spawned_child_does_not_inherit_non_cloexec_descriptor` covers a descriptor left without
  `FD_CLOEXEC` that is absent from the spawned child (the child sees `EBADF` from `F_GETFD`).

The full quiz is unitA-t1.md under the host's spawn-off-runtime handoff directory.

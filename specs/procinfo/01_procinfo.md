# procinfo

## Scope

procinfo reads host process facts natively: the kernel's own record of one process, of one process group, or of the
host's run queue. Nothing in it spawns a process (no `ps`, no `lsof`), walks the whole process table or waits on another
process's task, so a process stalled in the kernel cannot stall the reader. Darwin reads libproc (`proc_pidinfo`,
`proc_pidfdinfo`, `proc_listpgrppids`, `proc_listchildpids`, `proc_pid_rusage`), `sysctl(KERN_PROCARGS2)` and
`sandbox_check`; Linux reads procfs. The one Linux read the kernel serves under another process's own state is
`cmdline`, copied out of that process's memory under its memory-map lock, which a thread of it stalled in a page fault
(or a writer queued behind one) holds: procinfo reads it on a reader thread of its own under a 250 ms bound, and past
the bound answers the command name the kernel keeps in the task (`comm`), marked `name_only`, and replaces the reader,
so no later read queues behind the stuck one.

It is a Rust crate, `procinfo` (`packages/procinfo/crates/procinfo`), and an npm package, `@smoothbricks/procinfo`,
whose addon crate `procinfo-napi` binds every fact of the crate. cowshed's Rust crates link the `procinfo` crate;
cowshed never builds `procinfo-napi`. nx-plugin's bounded executor depends on the npm package.

Waiting for a process to exit (cowshed-os `ExitWatch`: a kqueue `NOTE_EXIT` on Darwin, a pidfd on Linux), cgroup v2
readings and signals, and every job-control decision stay in cowshed: they are the job runner's, not facts.

## Facts

Every fact takes a pid, a process group id or a descriptor of this process, and reads that one record.

| Fact                | Answers                                                                                                                  | Darwin                                                                                                   | Linux                                                                                                                              |
| ------------------- | ------------------------------------------------------------------------------------------------------------------------ | -------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------- |
| `awake_time_ms`     | Milliseconds from a process-local origin on a clock that stops while the host sleeps                                     | `Instant`: `CLOCK_UPTIME_RAW`                                                                            | `Instant`: `CLOCK_MONOTONIC`                                                                                                       |
| `lineage`           | `{ pid, ppid, pgid, zombie }`, or none when no process holds the pid                                                     | `proc_pidinfo(PROC_PIDTBSDINFO)`, which answers for an exited, unreaped process too                      | `/proc/<pid>/stat` state, parent and group                                                                                         |
| `running`           | Whether the process has not exited; a zombie has; a process this one may not inspect still runs                          | `proc_pidinfo` status                                                                                    | `/proc/<pid>/stat` state: no stat, `Z` or `X` have exited                                                                          |
| `run_state`         | `running`, `sleeping`, `uninterruptible`, `stopped`, `zombie`, `gone`, or `unread` with why                              | status, then the threads: any running thread runs, else any in an uninterruptible wait                   | the stat state letter                                                                                                              |
| `process_arguments` | The executable the kernel records and the byte-exact argument vector                                                     | `KERN_PROCARGS2`: the path exec was given                                                                | `/proc/<pid>/exe` (resolved) and `cmdline` under a 250 ms bound, past it `comm` alone (`name_only`); an exited process has neither |
| `child_pids`        | The pids whose parent is the process, exited and unreaped ones included; none for a pid no process holds                 | `proc_listchildpids`, re-read with a doubled buffer while full                                           | each thread's `/proc/<pid>/task/<tid>/children`; a kernel without that list is an error                                            |
| `started`           | When the process started: fixed for its life, so with its pid it names one process; none once no process holds the pid   | `pbi_start_tvsec` and `pbi_start_tvusec` of `PROC_PIDTBSDINFO`                                           | `starttime` ticks of `/proc/<pid>/stat` after `btime` of `/proc/stat`, read once                                                   |
| `group_pids`        | The pids holding the group id, exited ones included (Darwin only)                                                        | `proc_listpgrppids`, re-read with a doubled buffer while full                                            | not offered                                                                                                                        |
| `group_members`     | The group's running processes, each with its command line and run state (Darwin only)                                    | the group's pids, their arguments (else executable) and state                                            | not offered                                                                                                                        |
| `group_cpu`         | CPU the group has used with its reaped children's, comparable only to another reading of the same group (Darwin only)    | `proc_pid_rusage`, Mach ticks                                                                            | not offered                                                                                                                        |
| `resident_bytes`    | Bytes resident now, or none once no such process runs; an exited process holds none                                      | `pti_resident_size` of `PROC_PIDTASKINFO`                                                                | `statm` resident pages times the page size                                                                                         |
| `cpu_time`          | CPU time the process's threads used, user and system; none once it has exited (a zombie included) or no process holds it | `pti_total_user` and `pti_total_system` of `PROC_PIDTASKINFO`, Mach ticks scaled by `mach_timebase_info` | `utime` and `stime` of `/proc/<pid>/stat`, clock ticks of `_SC_CLK_TCK`                                                            |
| `listening_ports`   | Local ports of the process's TCP sockets in `LISTEN`, IPv4 and IPv6, each once                                           | each socket descriptor's `socket_fdinfo`                                                                 | socket inodes of `/proc/<pid>/fd` against the `LISTEN` rows of its namespace's `net/tcp` and `tcp6`                                |
| `descriptor_name`   | What `lsof -F di` names one descriptor of this process by                                                                | the kernel address (`d`) of a pipe or socket, the inode (`i`) of anything else                           | the inode `fstat(2)` reports, for every kind                                                                                       |
| `descriptor_names`  | Another process's open descriptors of the kinds asked, each named as that process would name it, or `gone`/`withheld`    | descriptor list, then one `proc_pidfdinfo` per descriptor asked                                          | each `/proc/<pid>/fd` entry's inode                                                                                                |
| `sandboxed`         | Whether the process may run confined; `false` proves a live, unconfined process                                          | `sandbox_check` with no operation                                                                        | `NoNewPrivs` and the seccomp mode of `/proc/<pid>/status`                                                                          |
| `HostLoad::read`    | The 1, 5 and 15 minute run-queue averages, or none                                                                       | `getloadavg(3)`                                                                                          | `getloadavg(3)`                                                                                                                    |

Darwin lists a process group's members to its reader (`proc_listpgrppids`). Linux lists them to no one, so reading a
group there takes a census of `/proc`, which procinfo never does: it offers `group_pids`, `group_members` and
`group_cpu` on Darwin only. On Linux a job's processes are its cgroup's; cowshed-os keeps the Linux group readings
beside the cgroup readings of its job runner, and reads each pid they list through two Linux-only exports of the crate:
`stat_lineage(pid)`, a pid's lineage with the fields of the stat line it was read from, and `running_members(pids)`, the
running processes among a list of pids.

## JavaScript

`@smoothbricks/procinfo` has one entry, `"."`. It exports `awakeTimeMs`, `processLineages`, `running`,
`processRunState`, `processArguments`, `childPids`, `cpuTimeNs`, `startedUs`, `residentBytes`, `listeningPorts`,
`ownDescriptorName`, `descriptorNames`, `sandboxed` and `hostLoad`, one per fact, and `processGroups`:
`{ pids, members, cpu }` on Darwin and `null` on Linux, so a caller cannot ask for a group fact where the kernel lists
no group. Its types are `ProcessLineage`, `RunState`, `ProcessArguments`, `GroupMember`, `ProcessGroups`,
`DescriptorName`, `DescriptorNames` and `HostLoad`.

- `processLineages(pids)` reads each listed pid for itself, in the order asked, and leaves out a pid no process holds,
  as `ps -p` leaves it out.
- `processArguments` answers `nameOnly: true`, with the command name as its one argument, where the argument vector
  could not be read within its bound.
- Byte strings (arguments) are `Uint8Array`. A count that can pass 2^53 (an inode, a kernel address, a group's CPU,
  resident bytes, a process's CPU time in nanoseconds) is a `bigint`; a start in microseconds since the epoch is a
  `number`, which passes 2^53 only after the year 2255.
- A number that is no 32-bit integer is a `RangeError`. An id that names no process or descriptor throws the addon's
  error with `code: "usage"`, and a record that cannot be read one with `code: "environment-missing"`, each with its
  message, naming the id and the system error, and `hint`.
- The entry loads the addon at its first call, never at import. It loads `procinfo.<family>.node`: in a checkout (one
  holding `crates/procinfo-napi/src/lib.rs`) from `.cache/native-dev/<family>/`, which `procinfo:napi-native` writes; in
  an installed package from `dist/native/<family>/`; under `NAPI_DEBUG_ADDON=1` from `.cache/native-debug/`, which
  `procinfo:napi-debug` writes. napi-rs's `NAPI_RS_NATIVE_LIBRARY_PATH` comes first. A tree never falls back to another.
  The families are `darwin-arm64`, `darwin-x64`, `linux-arm64-gnu` and `linux-x64-gnu`, as package.json `napi.targets`
  names their triples.

## Package

The addons are built by the targets nx-plugin infers from `napi.binaryName: "procinfo"`: `napi-<arch>-<family>` per
triple (Linux by cargo-zigbuild against glibc 2.17, macOS by `@napi-rs/cli`), `napi-native` for this host and
`napi-debug` for the tests. `build` is the TypeScript entry alone (`^build`, `tsc-js`), so a package that depends on
procinfo, nx-plugin included, takes an ordinary package edge and never builds an addon through it.

`release-check` judges the packed `dist/native` as cowshed's judges its trees (../cowshed/06_cli.md, "Launcher"): this
host's family's addon must be there, and every family's that is there must be a non-empty regular file that opens for
reading. It declares no dependencies and is cached on a runtime digest of `dist/native` with each file's mode and bytes,
so a replay stands for exactly the bytes that get packed.

# @smoothbricks/procinfo

Host process facts, read natively on macOS and Linux. Each answer is the kernel's own record of one process, one process
group or the host's run queue. Nothing here spawns `ps` or `lsof`, walks the whole process table, or waits on another
process, so a process stuck in the kernel cannot stall the code that asks about it.

## Why it exists

Tools that supervise other processes keep needing the same small set of facts: is this pid still alive, who is its
parent, what is it doing, how much CPU has its group used, which ports does it listen on, who holds this file open. The
usual way to get them is to run `ps` or `lsof` and parse the output. That is slow, it costs a process per question, and
it blocks behind any process the kernel is stuck on. procinfo asks the kernel directly, one record at a time.

## Who uses it

- `@smoothbricks/nx-plugin`'s bounded executor. It reads `processLineages` to tell a task that is stuck from one that is
  queued for a CPU grant (its waiters are the task's own descendants or group members), and `awakeTimeMs` for its idle
  clock, so a laptop that sleeps never counts the sleep as the task going silent. Node's `performance.now()` on macOS
  counts sleep; this clock does not.
- cowshed, through the Rust crate: its supervisor, job runner and deadlines read run states, arguments, resident memory
  and listening ports of the processes it runs, and on macOS their process group's members and CPU.
- Watchdogs and holder checks that ask which process tree is burning CPU, or which process holds a volume or a file
  open.

## What each function answers

| Function                      | Answers                                                                                                           | macOS                                               | Linux                                      |
| ----------------------------- | ----------------------------------------------------------------------------------------------------------------- | --------------------------------------------------- | ------------------------------------------ |
| `awakeTimeMs()`               | Milliseconds from a process-local origin, not counting host sleep                                                 | `CLOCK_UPTIME_RAW`                                  | `CLOCK_MONOTONIC`                          |
| `processLineages(pids)`       | `{ pid, ppid, pgid, zombie }` per live pid, in order; pids no process holds are left out                          | `proc_pidinfo`                                      | `/proc/<pid>/stat`                         |
| `running(pid)`                | Whether the process has not exited (a zombie has)                                                                 | `proc_pidinfo`                                      | `/proc/<pid>/stat`                         |
| `processRunState(pid)`        | `running`, `sleeping`, `uninterruptible`, `stopped`, `zombie`, `gone`, or `unread` with a reason                  | status and thread states                            | stat state letter                          |
| `processArguments(pid)`       | The executable and the byte-exact argument vector                                                                 | `KERN_PROCARGS2`                                    | `/proc/<pid>/exe`, `cmdline`               |
| `processGroups.pids(pgid)`    | Pids in the process group, exited ones included                                                                   | `proc_listpgrppids`                                 | `processGroups` is `null`                  |
| `processGroups.members(pgid)` | Running members with command line and run state                                                                   | as above                                            | `processGroups` is `null`                  |
| `processGroups.cpu(pgid)`     | CPU used by the group and its reaped children, as a `bigint` comparable only to another reading of the same group | Mach ticks (`proc_pid_rusage`)                      | `processGroups` is `null`                  |
| `residentBytes(pid)`          | Resident bytes as a `bigint`, or `null` once the process is gone                                                  | `PROC_PIDTASKINFO`                                  | `statm`                                    |
| `listeningPorts(pid)`         | TCP ports in `LISTEN`, IPv4 and IPv6, each once                                                                   | `socket_fdinfo` per descriptor                      | `/proc/<pid>/fd` against `net/tcp`, `tcp6` |
| `ownDescriptorName(fd)`       | How `lsof -F di` names one of this process's descriptors                                                          | kernel address of a pipe or socket, inode of a file | inode of every kind                        |
| `descriptorNames(pid, kinds)` | Another process's descriptors named the same way, or `gone` / `withheld`                                          | `proc_pidfdinfo`                                    | `/proc/<pid>/fd`                           |
| `processCwd(pid)`             | The process's current working directory as `Uint8Array`, or `null` once the process is gone                       | `PROC_PIDVNODEPATHINFO`                             | `readlink /proc/<pid>/cwd`                 |
| `openPaths(pid)`              | Another process's open file-system paths (`{ fd, path, eventOnly }`), or `gone` / `withheld`                      | `PROC_PIDFDVNODEPATHINFO`                           | `readlink /proc/<pid>/fd/<n>`              |
| `sandboxed(pid)`              | Whether the process may run confined; `false` proves a live, unconfined process                                   | `sandbox_check`                                     | `NoNewPrivs` and seccomp mode              |
| `hostLoad()`                  | The 1, 5 and 15 minute load averages, or `null`                                                                   | `getloadavg`                                        | `getloadavg`                               |

A number that is not a 32-bit integer throws a `RangeError`. An id that names no process throws an error with
`code: "usage"`; a record the kernel would not give throws one with `code: "environment-missing"`. Both carry a `hint`.
The addon loads on the first call, not on import.

## What it is not

It does not wait for processes, signal them, or manage them. Exit watches, cgroup v2 readings, signalling a cgroup and
every decision about jobs stay in cowshed, which owns the job runner. procinfo only reads.

Linux lists no process group without a census of `/proc`, so procinfo does not offer group facts there: `processGroups`
is `null`. On Linux, use the job's cgroup, which is cowshed's.

The full contract is [specs/procinfo/01_procinfo.md](../../specs/procinfo/01_procinfo.md).

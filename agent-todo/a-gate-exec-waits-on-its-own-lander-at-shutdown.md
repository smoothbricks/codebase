# A gate exec waits on its own lander at shutdown

Found 2026-11-26 on a consumer repository's land, ticket #105. Owner: cowshed.

The gate's `cowshed exec <ws> --class gate --host -- …` had no child left and its supervisor had no child, yet the exec
never exited, and the land turn stayed held for 70+ minutes with no gate progress. `sample` of the exec: the main thread
is in tokio's `BlockingPool::shutdown`, waiting for one blocking task. That task is
`cowshed_cli::lander::Lander::watch`, parked in `cowshed_os::process::ExitWatch::wait` (kevent) on a lander process's
exit. The lander is the `cowshed land` process that holds the turn and is itself waiting for this exec to finish, so
each waits on the other.

## What must hold

- An exec's shutdown never waits for a watcher of a process that outlives it: the lander watch is cancelled or detached
  when the exec's command has ended.
- A land's gate exec ends once its command has ended.

## Red

An exec run by a `cowshed land` (the lander alive and waiting on it) exits within a second of its command's exit.

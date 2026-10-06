#!/bin/sh
# Runs one Nx `runtime` input:
#
#   sh node_modules/@smoothbricks/nx-plugin/runtime-input.sh <command> [argument...]
#
# The input's value is the command's stdout. A command that fails stops Nx with
# an error; its failure never becomes a key.
#
# WHY the 0xFF byte. Nx 23.2.1 (`packages/nx/src/native/tasks/hashers/
# hash_runtime.rs`) runs a runtime input under `sh -c`, hashes its trimmed
# stdout followed by its trimmed stderr, and never looks at the exit status. A
# failed input is therefore a valid key: its error text, the same string for
# every history, toolchain or file it failed to read, so a result cached under
# one is replayed for all of them. Nx refuses exactly one outcome: a stream
# that is not UTF-8. `std::str::from_utf8` fails, the hash fails, and `nx run`
# exits 1 with `invalid utf-8 sequence of 1 bytes from index N`. So a failed
# command ends this script's stdout with 0xFF, a byte no UTF-8 text contains.
# runtime-input.test.ts plants a failure through real Nx and goes red the day
# Nx hashes these bytes instead of refusing them; if Nx starts honouring the
# exit status, the byte is redundant and can go.
#
# Nx's message names neither the input nor the cause. The cause is printed on
# stderr below, which Nx captures and drops. To read it, run the target's
# runtime inputs (`nx show target <project>:<target> --inputs`) by hand; a
# daemonless client also prints every input's status, stdout and stderr under
# NX_NATIVE_LOGGING=nx::native::tasks::hashers::hash_runtime=trace.
#
# The command's stderr is not part of the value; it surfaces only on failure.
# Diagnostics name temp paths, process ids and the boundary the input ran in
# (an Nx daemon inside a sandbox runs inputs with the sandbox's view of the
# client's environment), none of which is the input.
set -u
if [ "$#" -eq 0 ]; then
  echo 'usage: runtime-input.sh <command> [argument...]' >&2
  printf '\377'
  exit 64
fi
exec 3>&1
if diagnostic=$("$@" 2>&1 >&3 3>&-); then
  exit 0
else
  status=$?
fi
[ -z "$diagnostic" ] || printf '%s\n' "$diagnostic" >&2
printf 'runtime input exited %s: %s\n' "$status" "$*" >&2
printf '\377'
exit "$status"

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
# Nx drops runtime stderr, so it is not a reliable diagnostic channel. Failures
# retain private argv, byte-exact stderr, status and a readable report below
# $PWD/.runtime-input-failures/, outside Nx and temporary state. Read them with
# `cat .runtime-input-failures/*/report` after a decoder error. Stderr and a
# controlling /dev/tty also name the failure where those channels are visible.
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
if diagnostic=$(mktemp "${TMPDIR:-/tmp}/nx-runtime-input.XXXXXXXXXX"); then
  :
else
  printf 'runtime input could not capture stderr: %s\n' "$*" >&2
  printf '\377'
  exit 73
fi
if "$@" 2>"$diagnostic"; then
  rm -f "$diagnostic"
  exit 0
else
  status=$?
fi
cat "$diagnostic" >&2
if record=$(umask 077; mkdir -p "$PWD/.runtime-input-failures" &&
  mktemp -d "$PWD/.runtime-input-failures/failure.XXXXXXXXXX"); then
  if printf '%s\000' "$@" >"$record/argv" &&
    printf '%s\n' "$status" >"$record/exit-status" &&
    mv "$diagnostic" "$record/stderr" && {
      printf 'runtime input exited %s: %s\n' "$status" "$*"
      printf 'working directory: %s\n' "$PWD"
      index=0
      for argument
      do
        printf 'argv[%s]: <%s>\n' "$index" "$argument"
        index=$((index + 1))
      done
      printf 'original stderr:\n'
      cat "$record/stderr"
    } >"$record/report"; then
    printf '\nruntime input exited %s: %s; evidence: %s/report\n' "$status" "$*" "$record" >&2
    # A detached daemon may have no terminal. Preserve that error too, rather
    # than sending it into Nx's discarded stderr or swallowing it.
    (exec 4>/dev/tty && cat "$record/report" >&4) 2>"$record/terminal-error"
  else
    printf '\nruntime input exited %s: %s; evidence incomplete: %s; stderr capture: %s\n' \
      "$status" "$*" "$record" "$diagnostic" >&2
  fi
else
  printf '\nruntime input exited %s: %s; could not retain evidence; stderr capture: %s\n' \
    "$status" "$*" "$diagnostic" >&2
fi
printf '\377'
exit "$status"

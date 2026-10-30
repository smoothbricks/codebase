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
# retain private argv, byte-exact stdout and stderr, status and a readable
# report below $PWD/.runtime-input-failures/, outside Nx and temporary state.
# Read them with `cat .runtime-input-failures/*/report` after a decoder error.
# Stderr and a controlling /dev/tty also name the failure where those channels
# are visible.
#
# A failed command's own stdout never reaches the hasher either: it is
# captured from the start, retained in the failure record, and the only byte
# this script emits on failure is the 0xFF poison above. Nothing from a
# failure path flows into the hash as input bytes. Successful stdout is
# replayed from the capture.
#
# The command's stderr is not part of the value; it surfaces only on failure.
# Diagnostics name temp paths, process ids and the boundary the input ran in
# (an Nx daemon inside a sandbox runs inputs with the sandbox's view of the
# client's environment), none of which is the input.
# A successful input costs the command alone: both of its streams are captured
# by command substitution, a forked subshell rather than a program run, so the
# success path executes nothing but the command. Under Nx a runtime input runs
# on every hash of every task that names it, and each program the wrapper ran
# cost more than the input it wrapped. The command's stdout goes to the outer
# capture as it is written; its stderr, then its exit status, are captured
# within, and follow it with their length, so the value is cut from the end and
# no delimiter has to be absent from the output. Lengths and patterns count
# bytes (LC_ALL=C), set only once the command has run.
#
# The one residual difference: a shell variable cannot hold a NUL byte, so a
# NUL in either stream is dropped. Two outputs that differ only in NUL bytes
# share a key; a command that might print a NUL hex-encodes its output.
set -u
if [ "$#" -eq 0 ]; then
  echo 'usage: runtime-input.sh <command> [argument...]' >&2
  printf '\377'
  exit 64
fi
both=$(
  exec 3>&1
  ran=$("$@" 2>&1 1>&3 3>&-; printf ':%s' "$?")
  LC_ALL=C
  printf '%s;%s' "$ran" "${#ran}"
)
LC_ALL=C
case $both in
*';'*) ;;
*)
  printf 'runtime input could not capture streams: %s\n' "$*" >&2
  printf '\377'
  exit 73
  ;;
esac
length=${both##*;}
both=${both%;*}
tail=
while [ "${#tail}" -lt "$length" ]; do
  tail="$tail?"
done
stdout=${both%$tail}
ran=${both#"$stdout"}
status=${ran##*:}
stderr=${ran%:*}
if [ "$status" -eq 0 ]; then
  printf '%s' "$stdout"
  exit 0
fi
printf '%s' "$stderr" >&2
if record=$(umask 077; mkdir -p "$PWD/.runtime-input-failures" &&
  mktemp -d "$PWD/.runtime-input-failures/failure.XXXXXXXXXX"); then
  if printf '%s\000' "$@" >"$record/argv" &&
    printf '%s\n' "$status" >"$record/exit-status" &&
    printf '%s' "$stderr" >"$record/stderr" &&
    printf '%s' "$stdout" >"$record/stdout" && {
      printf 'runtime input exited %s: %s\n' "$status" "$*"
      printf 'working directory: %s\n' "$PWD"
      index=0
      for argument
      do
        printf 'argv[%s]: <%s>\n' "$index" "$argument"
        index=$((index + 1))
      done
      printf 'original stderr:\n'
      printf '%s' "$stderr"
      printf 'original stdout (never hashed):\n'
      printf '%s' "$stdout"
    } >"$record/report"; then
    printf '\nruntime input exited %s: %s; evidence: %s/report\n' "$status" "$*" "$record" >&2
    # Nx discards this stderr, so a controlling terminal is the one live display
    # of the report. Nx's daemon has none (ENXIO) and cowshed's sandbox denies
    # it (EPERM). Having no terminal is no failure of the input: the shell's
    # refusal of the open joins the notice above on stderr, and the record
    # gains nothing. `command` keeps that refusal from exiting the shell. A
    # write that fails on a terminal that did open is retained in the record.
    if command exec 4>/dev/tty; then
      if ! failure=$(cat "$record/report" 2>&1 >&4); then
        printf '%s\n' "$failure" >"$record/terminal-error"
      fi
      exec 4>&-
    fi
  else
    printf '\nruntime input exited %s: %s; evidence incomplete: %s\n' \
      "$status" "$*" "$record" >&2
  fi
else
  printf '\nruntime input exited %s: %s; could not retain evidence\n' \
    "$status" "$*" >&2
fi
printf '\377'
exit "$status"

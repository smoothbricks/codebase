import { mkdir, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import type { SubtreeRefusal } from '../executors/bounded-exec/owned-subtree.js';

/**
 * How a stand-in answers: the admission `subtree-root` writes, whether `retire-descendants` works, and the cowshed
 * every other verb runs on (a RAM volume's disk commands, say), when the caller has one.
 */
export interface StandInMode {
  readonly admission: 'admitted' | SubtreeRefusal;
  readonly retire: 'retire' | 'fail';
  readonly others?: string;
}

/**
 * A stand-in for the installed `cowshed`'s two subtree verbs, speaking their exact wire shapes
 * (specs/cowshed/07_api.md, `JobSubtreeAdmission`, `JobSubtreeRetirement`) and exit codes: a typed
 * refusal exits 0 with its JSON, a transport failure exits nonzero. `subtree-root --handle-fd N --
 * argv` writes the admission to descriptor N, closes it, and execs argv in the same process, as the
 * real verb does. The handle's `index` is the root's pid.
 *
 * Its tree is the one thing it cannot have: a supervisor observes forks, a script cannot. It stands
 * the root's process group in for that tree, which holds for a bounded-exec command (the executor
 * makes the root lead a group of its own) and its background children, and never for a process the
 * test starts elsewhere — which is exactly the line between owned and unowned the tests draw. A
 * retirement signals those members with TERM, waits up to the grace for their exits, KILLs what is
 * left and waits again, so a success answer means they are gone, as the real one does. A handle
 * retires once; a replay is `spentHandle`.
 */
export async function standInCowshed(directory: string, mode: StandInMode): Promise<string> {
  const state = join(directory, 'stand-in-state');
  await mkdir(state, { recursive: true });
  const path = join(directory, 'cowshed');
  await writeFile(
    path,
    `#!/bin/sh
set -eu
admission='${mode.admission}'
retire='${mode.retire}'
state='${state}'
others='${mode.others ?? ''}'
gone() { ! kill -0 "$1" 2>/dev/null || case $(ps -o stat= -p "$1" 2>/dev/null) in Z*) true ;; *) false ;; esac; }
case "$1" in
subtree-root)
  [ "$2" = --handle-fd ] && [ "$4" = -- ] || { echo 'usage: cowshed subtree-root --handle-fd N -- argv' >&2; exit 64; }
  fd=$3
  shift 4
  if [ "$admission" = admitted ]; then
    line="{\\"kind\\":\\"admitted\\",\\"handle\\":{\\"jobId\\":1,\\"index\\":$$,\\"incarnation\\":\\"0123456789abcdef0123456789abcdef\\"}}"
  else
    line="{\\"kind\\":\\"refused\\",\\"reason\\":\\"$admission\\"}"
  fi
  eval "printf '%s\\n' \\"\\$line\\" >&$fd; exec $fd>&-"
  exec "$@"
  ;;
retire-descendants)
  [ "$2" = --grace-ms ] || { echo 'usage: cowshed retire-descendants --grace-ms N' >&2; exit 64; }
  rounds=$(( $3 / 50 + 1 ))
  handle=$(cat)
  if [ "$retire" = fail ]; then
    echo 'stand-in: the supervisor socket is unreachable' >&2
    exit 1
  fi
  root=$(printf '%s' "$handle" | sed -n 's/.*"index":\\([0-9][0-9]*\\).*/\\1/p')
  if [ -e "$state/spent-$root" ]; then
    printf '{"kind":"refused","reason":"spentHandle"}\\n'
    exit 0
  fi
  : > "$state/spent-$root"
  members=$(ps -A -o pid= -o pgid= | awk -v g="$root" '$2 == g { print $1 }')
  images=''
  signalled=''
  for pid in $members; do
    gone "$pid" && continue
    image=$(ps -o comm= -p "$pid" 2>/dev/null || true)
    kill -TERM "$pid" 2>/dev/null || continue
    signalled="$signalled $pid"
    images="$images$pid=$image
"
  done
  killed=' '
  for round in TERM KILL; do
    i=0
    for pid in $signalled; do
      while ! gone "$pid" && [ $i -lt $rounds ]; do sleep 0.05; i=$((i + 1)); done
    done
    left=''
    for pid in $signalled; do gone "$pid" || left="$left $pid"; done
    [ -z "$left" ] && break
    [ "$round" = KILL ] && { echo "stand-in: still running after KILL:$left" >&2; exit 1; }
    for pid in $left; do
      kill -KILL "$pid" 2>/dev/null || true
      killed="$killed$pid "
    done
  done
  rows=''
  for pid in $signalled; do
    image=$(printf '%s' "$images" | sed -n "s/^$pid=//p")
    case "$killed" in *" $pid "*) signal=9 ;; *) signal=15 ;; esac
    rows="$rows\${rows:+,}{\\"pid\\":$pid,\\"image\\":\\"$image\\",\\"signal\\":$signal}"
  done
  printf '{"kind":"retired","retirement":{"containment":{"kind":"observed","lost":[]},"outlived":[%s]}}\\n' "$rows"
  ;;
*)
  [ -n "$others" ] && exec "$others" "$@"
  echo "stand-in cowshed: unknown verb $1" >&2
  exit 64
  ;;
esac
`,
    { mode: 0o755 },
  );
  return path;
}

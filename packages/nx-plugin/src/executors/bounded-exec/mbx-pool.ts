import { shellTokens } from '../../runtime-input-policy.js';
import { invocationSpan, type RunnerKind } from './runner-sizing.js';
import { shellQuote } from './verdict.js';

/**
 * mbx's machine-wide pool as a leaf runner meets it (specs/cowshed/05_gateway.md, "Host scheduling", "Parallelism inside
 * a job"). With the mbx capability the canonical environment names the one pool (`MBX_SCHEDULER=1` beside the shared
 * `MBX_CACHE_DIR`) and the job's priority in it; that pool is the host's CPU and memory ledger, so no Nx run or leaf
 * serves a pool of its own beside it.
 */
export interface MbxPool {
  readonly priority: 'normal' | 'low';
}

/**
 * The pool the environment names, or `null` where the project has no mbx and cowshed's own pools apply unchanged, or
 * where this run is already inside an mbx reservation: `mbx reserve` hands its command `MBX_SCHED_DISABLE=1`, and nested
 * non-Cargo work shares the enclosing grant instead of reserving twice (specs/cowshed/05_gateway.md, "Parallelism
 * inside a job"). A second reservation would wait in the pool while its parent holds a grant, so under a full pool the
 * nested leaf queued behind the very work it belongs to.
 */
export function mbxPool(environment: Readonly<NodeJS.ProcessEnv>): MbxPool | null {
  if (environment.MBX_SCHEDULER !== '1' || !environment.MBX_CACHE_DIR || environment.MBX_SCHED_DISABLE === '1') {
    return null;
  }
  return { priority: environment.MBX_SCHEDULER_PRIORITY === 'normal' ? 'normal' : 'low' };
}

/**
 * How a leaf enters the pool once. A Cargo leaf that builds runs through mbx, which admits its compilers and supported
 * test work itself, so wrapping it would charge it twice. Any other leaf holds `cpus` of the pool for its whole run
 * through `mbx reserve`, `cpus` being the job's own count. That includes a nextest run of test binaries already built
 * (the inferred extracted-archive run, or one from `--archive-file`): it compiles nothing, and beneath a reservation
 * mbx schedules none of its test processes, so they run inside the one grant. Admitted one by one under
 * `MBX_SCHEDULER_TESTS=1`, each queued for a ticket of its own: in a downstream gate on 2026-10-30, codec tests of at
 * most 0.25 s took 73.5 s apiece, and their 121 tests run in 1.18 s unqueued (specs/cowshed/05_gateway.md,
 * "Parallelism inside a job").
 */
export type MbxLeaf =
  | { readonly kind: 'cargo' }
  | { readonly kind: 'reserve'; readonly cpus: number; readonly priority: 'normal' | 'low' };

const CARGO_LEAVES: ReadonlySet<RunnerKind> = new Set<RunnerKind>(['cargo', 'cargo-mutants', 'nextest']);

/** A nextest invocation that runs test binaries already built, from extracted archive metadata or an archive file. */
/** A nextest invocation that runs test binaries already built, from extracted archive metadata or an archive file. */
const PREBUILT_NEXTEST = /(?<![\w-])--(?:binaries-metadata|archive-file)(?=[=\s]|$)/;

/** Whether `command` runs nextest over test binaries already built, which compiles nothing. */
export function runsPrebuiltNextest(command: string): boolean {
  return PREBUILT_NEXTEST.test(command);
}

export function mbxLeaf(pool: MbxPool, kind: RunnerKind, want: number, command: string): MbxLeaf {
  const builds = CARGO_LEAVES.has(kind) && !(kind === 'nextest' && runsPrebuiltNextest(command));
  return builds ? { kind: 'cargo' } : { kind: 'reserve', cpus: Math.max(1, want), priority: pool.priority };
}

/** Nx's own default count of tasks at once, when nothing names one. */
const NX_DEFAULT_PARALLEL = 3;

/**
 * The cores one task of an Nx run counts as its own: the host's cores divided among the tasks Nx runs at once, at
 * least one. Nx reads that count from `NX_PARALLEL`, a number or a share of the cores (`100%`), else runs three.
 *
 * A prebuilt nextest leaf reserves its runner's count for its whole run, and nextest asks for every core by default,
 * so each cargo-test leaf of a gate reserved the whole pool and they ran one at a time behind each other: on
 * 2026-10-30 a gate's 18-core host ran 4.2 cores busy, its leaves each holding `mbx reserve --cpus 18`. Sized to its
 * share, a leaf leaves the rest of the pool to the tasks Nx starts beside it.
 */
export function nxTaskCores(cores: number, environment: Readonly<NodeJS.ProcessEnv>): number {
  const named = environment.NX_PARALLEL?.trim() ?? '';
  const share = /^(\d+)%$/.exec(named);
  const count = share?.[1] !== undefined ? (cores * Number(share[1])) / 100 : /^\d+$/.test(named) ? Number(named) : 0;
  const parallel = count >= 1 ? Math.floor(count) : NX_DEFAULT_PARALLEL;
  return Math.max(1, Math.floor(cores / parallel));
}

/**
 * Only a known scheduled execution may stop its idle clock at zero holds. Plain Cargo tests can run doctests;
 * archive-file targets, direct binaries and other runners are not proven scheduled at spawn (11_shell.md,
 * "Cargo test-runner boundaries"). The inferred extracted native nextest command is a supported Cargo invocation.
 */
export function mbxIdleGated(leaf: MbxLeaf | null, command: string, environment: Readonly<NodeJS.ProcessEnv>): boolean {
  if (leaf === null) return false;
  if (leaf.kind === 'reserve') return true;
  if (environment.MBX_SCHEDULER_TESTS !== '1') return false;
  if (environment.MBX_SCHEDULER === '0' || environment.MBX_SCHED_DISABLE === '1' || environment.MBX_DISABLE === '1')
    return false;
  // invocationSpan shares the lifecycle mapper's conservative shell-syntax boundary.
  if (/[<>\n\r`\\]/.test(command)) return false;
  if (/[^\S \t]/.test(command)) return false;
  const invocation = /cargo(?:\s+(?:--frozen|--locked|--offline))*\s+(nextest\s+run|test)(?=\s|$)/;
  const found = invocation.exec(command);
  if (found === null) return false;
  const prefix = command.slice(0, found.index).trim();
  if (prefix !== '' && !/^extracted="\$\([^"\n]+\)"\s*&&$/.test(prefix)) return false;
  let remaining = command.slice(found.index);
  const unquoted = remaining.replace(/"[^"]*"|'[^']*'/g, '');
  if (/[$*?{}~[\]]/.test(unquoted)) return false;
  // Only the inferred command's path templates are known expansions; arbitrary words could supply mbx overrides.
  const paths = remaining.replace(/\$PWD(?=\/)/g, '');
  if ((prefix === '' ? paths : paths.replace(/\$extracted(?=\/)/g, '')).includes('$')) return false;
  for (;;) {
    const leg = invocation.exec(remaining);
    if (leg === null || leg.index !== 0) return false;
    const { argsEnd } = invocationSpan(remaining, invocation);
    const args: string[] = [];
    for (const token of shellTokens(remaining.slice(0, argsEnd), [])) {
      if (token.kind !== 'word') return false;
      if (token.text === '--') break;
      if (
        token.text.startsWith('$') &&
        !(
          token.text.startsWith('$extracted/') &&
          ['--binaries-metadata', '--cargo-metadata', '--target-dir-remap'].includes(args.at(-1) ?? '')
        )
      )
        return false;
      args.push(token.text);
    }
    if (
      args.some((arg) =>
        /^(?:--(?:config|directory|archive-file)(?:=|$)|--(?:doc|no-run|list|help)(?:=|$)|-C|-h$|\+)/.test(arg),
      )
    ) {
      return false;
    }
    if (
      leg[1] === 'test' &&
      !args.some((arg) => /^--(?:lib|tests|bins|examples|benches|all-targets|test|bin|example|bench)(?:=|$)/.test(arg))
    ) {
      return false;
    }
    const suffix = remaining.slice(argsEnd).trim();
    if (suffix === '') return true;
    const separator = /^(?:&&|\|\||;)\s*/.exec(suffix);
    if (separator === null) return false;
    remaining = suffix.slice(separator[0].length);
  }
}

/**
 * mbx writes one byte per admission (g) and permit drop (r). Cargo descendants inherit this descriptor through
 * MBX_GRANT_FD; reserve names it with --grant-fd and closes it on exec of the external command.
 */
export const MBX_GRANT_FD = 5;

/**
 * `command` as the shell runs it under its leaf's admission. `mbx` is the canonical environment's, whose realpath
 * cowshed's admission proved to be the host's pinned mbx; it writes the grant on this descriptor itself.
 */
export function withMbxLeaf(command: string, leaf: MbxLeaf): string {
  if (leaf.kind === 'cargo') {
    return command;
  }
  return `mbx reserve --grant-fd ${MBX_GRANT_FD} --cpus ${leaf.cpus} --priority ${leaf.priority} -- /bin/sh -c ${shellQuote(command)}`;
}

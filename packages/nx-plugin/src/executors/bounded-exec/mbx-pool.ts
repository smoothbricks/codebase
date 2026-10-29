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
 * How a leaf enters the pool once: a Cargo leaf runs through mbx, which admits its compiler and supported test work
 * itself, so wrapping it would charge it twice; any other leaf holds `cpus` of the pool for its whole run through
 * `mbx reserve`, `cpus` being the job's own count.
 */
export type MbxLeaf =
  | { readonly kind: 'cargo' }
  | { readonly kind: 'reserve'; readonly cpus: number; readonly priority: 'normal' | 'low' };

const CARGO_LEAVES: ReadonlySet<RunnerKind> = new Set<RunnerKind>(['cargo', 'cargo-mutants', 'nextest']);

export function mbxLeaf(pool: MbxPool, kind: RunnerKind, want: number): MbxLeaf {
  return CARGO_LEAVES.has(kind)
    ? { kind: 'cargo' }
    : { kind: 'reserve', cpus: Math.max(1, want), priority: pool.priority };
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
  const invocation = /cargo(?:\s+(?:--frozen|--locked|--offline))*\s+(nextest\s+run|test)(?=\s|$)/;
  const found = invocation.exec(command);
  if (found === null) return false;
  const prefix = command.slice(0, found.index).trim();
  if (prefix !== '' && !/^extracted="\$\([^\n]+\)"\s*&&$/.test(prefix)) return false;
  const { argsEnd } = invocationSpan(command, invocation);
  if (command.slice(argsEnd).trim() !== '') return false;
  const args = command.slice(found.index, argsEnd).split(/\s--(?:\s|$)/, 1)[0] ?? '';
  if (
    /(?:^|\s)(?:--(?:config|directory|archive-file)(?:=|\s)|--(?:doc|no-run|list|help)(?:\s|$)|-C|-h(?:\s|$)|\+)/.test(
      args,
    )
  ) {
    return false;
  }
  return (
    found[1] !== 'test' ||
    /(?:^|\s)--(?:lib|tests|bins|examples|benches|all-targets|test|bin|example|bench)(?=\s|=|$)/.test(args)
  );
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

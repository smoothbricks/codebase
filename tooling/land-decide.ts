#!/usr/bin/env bun
// The land's branch decisions as pure functions of plain values. land-host.sh asks them through this CLI, one
// process per decision, so the script keeps its sequence and every branch is testable as a plain call.

export type WorkspaceArgument =
  | { readonly kind: 'usage' }
  | { readonly kind: 'main' }
  | { readonly kind: 'workspace'; readonly name: string };

/** The first argument names the workspace. `main` is refused: a land moves main, it never lands itself. */
export function workspaceArgument(argv: readonly string[]): WorkspaceArgument {
  const [name] = argv;
  if (name === undefined) return { kind: 'usage' };
  if (name === 'main') return { kind: 'main' };
  return { kind: 'workspace', name };
}

const APPROVED_STATUS = 'Found RC allowed 0';

/**
 * direnv trusts an .envrc by content. A byte-identical one is approved here. A differing one runs only when direnv
 * already reports it approved, matched as a whole line like `grep -x`, so a gate never runs an unapproved environment.
 */
export type EnvrcVerdict = 'allow' | 'approved' | 'refuse';

export function envrcVerdict(sameBytes: boolean, direnvStatus: string): EnvrcVerdict {
  if (sameBytes) return 'allow';
  return direnvStatus.split('\n').includes(APPROVED_STATUS) ? 'approved' : 'refuse';
}

/**
 * The fence reason of a cowshed `--json` error envelope, read from `error.fence.reason`. Anything else is undefined:
 * a reason that merely appears inside some message string is not a fence refusal.
 */
export function fenceReason(outcome: string): string | undefined {
  let envelope: unknown;
  try {
    envelope = JSON.parse(outcome);
  } catch {
    return undefined;
  }
  if (typeof envelope !== 'object' || envelope === null || !('error' in envelope)) return undefined;
  const error: unknown = envelope.error;
  if (typeof error !== 'object' || error === null || !('fence' in error)) return undefined;
  const fence: unknown = error.fence;
  if (typeof fence !== 'object' || fence === null || !('reason' in fence)) return undefined;
  return typeof fence.reason === 'string' ? fence.reason : undefined;
}

/** Main moved past the head the rebase was told: rebase again onto its new head. Any other refusal stops the land. */
export function afterRebase(outcome: string): 'retry' | 'stop' {
  return fenceReason(outcome) === 'ontoMoved' ? 'retry' : 'stop';
}

/** The target moved while the gate ran: rebase and gate the new tree. Never a retry of a red gate. */
export function afterLand(outcome: string): 'gateAgain' | 'stop' {
  return fenceReason(outcome) === 'targetMoved' ? 'gateAgain' : 'stop';
}

function fail(message: string): never {
  process.stderr.write(message);
  process.exit(64);
}

if (import.meta.main) {
  const WORKSPACE_USAGE = 'usage: land-host.sh <workspace> [cowshed land args...]\n';
  const USAGE =
    'usage: land-decide.ts workspace ARG... | envrc same|differs STATUS | after-rebase OUTCOME | after-land OUTCOME\n';
  const [verb, ...args] = process.argv.slice(2);
  const [first, second] = args;
  if (verb === 'workspace') {
    const argument = workspaceArgument(args);
    if (argument.kind === 'usage') fail(WORKSPACE_USAGE);
    if (argument.kind === 'main') fail('land-host: main is not a workspace to land\n');
    process.stdout.write(argument.name + '\n');
  } else if (
    verb === 'envrc' &&
    args.length === 2 &&
    (first === 'same' || first === 'differs') &&
    second !== undefined
  ) {
    process.stdout.write(envrcVerdict(first === 'same', second) + '\n');
  } else if ((verb === 'after-rebase' || verb === 'after-land') && args.length === 1 && first !== undefined) {
    process.stdout.write((verb === 'after-rebase' ? afterRebase(first) : afterLand(first)) + '\n');
  } else {
    fail(USAGE);
  }
}

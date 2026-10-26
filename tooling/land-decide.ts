#!/usr/bin/env bun
// The gate's branch decisions as pure functions of plain values. land-host.sh asks them through this CLI, one
// process per decision, so the script keeps its sequence and every branch is testable as a plain call.

export type WorkspaceArgument =
  | { readonly kind: 'usage' }
  | { readonly kind: 'main' }
  | { readonly kind: 'workspace'; readonly name: string };

/**
 * The arguments after land-host.sh's `--gate` name the workspace, and nothing follows it. `main` is refused: a land
 * moves main, it never lands itself.
 */
export function workspaceArgument(argv: readonly string[]): WorkspaceArgument {
  const [name] = argv;
  if (name === undefined || argv.length !== 1) return { kind: 'usage' };
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

function fail(message: string): never {
  process.stderr.write(message);
  process.exit(64);
}

if (import.meta.main) {
  const WORKSPACE_USAGE = 'usage: land-host.sh --gate <workspace>\n';
  const USAGE = 'usage: land-decide.ts workspace ARG... | envrc same|differs STATUS\n';
  const [verb, ...args] = process.argv.slice(2);
  const [first, second] = args;
  if (verb === 'workspace') {
    const argument = workspaceArgument(args);
    if (argument.kind === 'usage') fail(WORKSPACE_USAGE);
    if (argument.kind === 'main') fail('land-host: main is not a workspace to land\n');
    process.stdout.write(`${argument.name}\n`);
  } else if (
    verb === 'envrc' &&
    args.length === 2 &&
    (first === 'same' || first === 'differs') &&
    second !== undefined
  ) {
    process.stdout.write(`${envrcVerdict(first === 'same', second)}\n`);
  } else {
    fail(USAGE);
  }
}

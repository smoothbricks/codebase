#!/usr/bin/env bun
// The post-land install's decisions as pure functions of plain values. land-install.sh asks them through this CLI,
// one process per decision, so the script keeps its sequence and every branch is testable as a plain call.
//
// The gateway daemon and the cowshed an operator types are one installed artifact, and only 'cowshed setup' replaces
// it (specs/cowshed/05_gateway.md, "The installed cowshed"): a land that changed cowshed leaves the host on the build
// before it. The script builds the land's release CLI, then does what setup can do with nobody present, and says in
// one land-status line on stdout what it did and the exact command for what it did not.

import { join } from 'node:path';

/** A release CLI this host can install, and where its build writes it. */
export interface CliTarget {
  /** The Nx target whose production configuration builds it. */
  readonly target: string;
  /** The dist/bin directory that build writes: the platform part of a binary's place in a distribution. */
  readonly platform: string;
}

/** The release CLI of a macOS host, by architecture. No other host installs one: setup installs a LaunchAgent. */
export function cliTarget(system: string, machine: string): CliTarget | undefined {
  if (system !== 'Darwin') return undefined;
  switch (machine) {
    case 'arm64':
      return { target: 'cli-arm64-macos', platform: 'darwin-arm64' };
    case 'x86_64':
      return { target: 'cli-x64-macos', platform: 'darwin-x64' };
    default:
      return undefined;
  }
}

/** Whether a land changed cowshed or something its release CLI is built from. 'unknown' builds: a stale install is the defect. */
export type Wanted = 'yes' | 'no' | 'unknown';

const COWSHED_PROJECT = 'cowshed';
// Nx attributes a changed root Cargo.lock to the root project alone (checked with: nx show projects --affected
// --files=Cargo.lock), although the cowshed CLI build reads the lock. Every other input is mapped by Nx itself:
// the project's own files, its dependencies' (nx-cache-server is installed with the CLI) and the root Cargo.toml.
const ROOT_CARGO_LOCK = 'Cargo.lock';

/**
 * affectedProjects is the JSON array that 'nx show projects --affected --base=BEFORE --head=HEAD --json' prints, and
 * changedFiles the output of 'git diff --name-only BEFORE HEAD'. Nx's affected set holds a project when it or any
 * project it depends on changed, which is the install's trigger; Nx cannot say what it could not read, so an
 * unreadable set is 'unknown', never 'no'.
 */
export function cowshedChanged(affectedProjects: string, changedFiles: string): Wanted {
  if (changedFiles.split('\n').includes(ROOT_CARGO_LOCK)) return 'yes';
  const affected = stringList(affectedProjects);
  if (affected === undefined) return 'unknown';
  return affected.includes(COWSHED_PROJECT) ? 'yes' : 'no';
}

/** A full commit id: the only name a stage directory or a land-status line carries for a land. */
export function isCommit(text: string): boolean {
  return /^[0-9a-f]{40}$/.test(text);
}

/** Where the build staged for a commit lives, or undefined when the name is not a commit. */
export function stageDirectory(main: string, commit: string): string | undefined {
  return isCommit(commit) ? join(main, '.cache', 'cowshed-release', commit) : undefined;
}

/**
 * The stage directories to remove, given the names in the stage root newest first: every commit-named one past the
 * newest 'keep'. Anything else in the root is not a stage and is never named.
 */
export function staleStages(names: readonly string[], keep: number): readonly string[] {
  return names.filter(isCommit).slice(keep);
}

/**
 * The command line that runs a staged build's setup, for a person to run: in main's devenv, because setup installs the
 * PATH of the shell it runs in and its build-state discovery runs cargo, quoted for a shell.
 */
export function setupCommand(main: string, binary: string): string {
  return `direnv exec ${shellWord(main)} ${shellWord(binary)} setup`;
}

function shellWord(word: string): string {
  return /^[A-Za-z0-9_./:=@%+-]+$/.test(word) ? word : `'${word.replaceAll("'", "'\\''")}'`;
}

/** What 'cowshed doctor' says setup would need an administrator for. */
export type PromptVerdict =
  | { readonly kind: 'none' }
  | { readonly kind: 'needed'; readonly reasons: readonly string[] }
  | { readonly kind: 'unknown'; readonly why: string };

// The findings doctor derives from the same host-storage plan setup executes (cowshed-cli runtime.rs,
// host_storage_findings): each is an error exactly when setup has storage, mount-service or mount-root work, the work
// that raises the one authorization dialog. Doctor does not print the plan's own answer (requires_authorization), so
// this reads the findings and errs toward attended: host-mount-root also fires when only the host configuration file
// is missing, which needs no authorization. A future finding with another code but the same remedy is caught by its
// hint: not knowing is not permission.
const HOST_STORAGE_CODES: ReadonlySet<string> = new Set([
  'host-storage',
  'host-volume-absent',
  'mount',
  'marker',
  'host-mount-root',
  'host-filevault',
  'host-fstab',
  'mount-stubs',
  'host-mount-service',
]);
const SETUP_HINT = 'cowshed setup';

/**
 * doctorJson is the stdout of 'cowshed --json doctor' from the build to install. Doctor exits 5 for an unhealthy host
 * and still prints its report, so the status is not asked: a report that cannot be read is 'unknown'.
 */
export function setupPrompt(doctorJson: string): PromptVerdict {
  const envelope = successResult(doctorJson, 'doctor');
  if (envelope.kind === 'refused') return { kind: 'unknown', why: envelope.why };
  const findings = envelope.result.findings;
  if (!Array.isArray(findings)) return { kind: 'unknown', why: 'cowshed doctor reported no findings list' };
  const reasons: string[] = [];
  for (const finding of findings) {
    if (
      !isRecord(finding) ||
      typeof finding.code !== 'string' ||
      typeof finding.severity !== 'string' ||
      typeof finding.message !== 'string'
    ) {
      return { kind: 'unknown', why: 'cowshed doctor reported a finding without a code, a severity and a message' };
    }
    if (finding.severity === 'error' && (HOST_STORAGE_CODES.has(finding.code) || finding.hint === SETUP_HINT)) {
      reasons.push(`${finding.code}: ${finding.message}`);
    }
  }
  return reasons.length === 0 ? { kind: 'none' } : { kind: 'needed', reasons };
}

/** The line the prompt verb prints: the verdict word, then its detail. */
export function promptLine(verdict: PromptVerdict): string {
  switch (verdict.kind) {
    case 'none':
      return 'none';
    case 'needed':
      return `needed ${verdict.reasons.join('; ')}`;
    case 'unknown':
      return `unknown ${verdict.why}`;
  }
}

/** Whether an install may restart the gateway now. */
export type Readiness =
  | { readonly kind: 'ready' }
  | { readonly kind: 'busy'; readonly blockers: readonly string[] }
  | { readonly kind: 'unknown'; readonly why: string };

/**
 * statusJson is the stdout of 'cowshed --json gateway status'. An install refuses, and restarts nothing, while a land
 * holds its repository's turn (05_gateway.md, "An install refuses while a land runs"; cowshed-cli agent_cycle.rs
 * restart_blockers): the holder of each repository's turn is the first
 * ticket of its queue, a queued land behind it is not a blocker. This is that decision over the status the CLI prints,
 * so a refusal is named before the install is tried; the install still checks under its own slot. No daemon answering
 * means nothing to restart and nothing to cut off. A held install slot defers too: an install takes the slot first and
 * waits for it with no deadline, daemon or none.
 */
export function installReadiness(statusJson: string): Readiness {
  const envelope = successResult(statusJson, 'gateway status');
  if (envelope.kind === 'refused') return { kind: 'unknown', why: envelope.why };
  const status = envelope.result;
  if (typeof status.running !== 'boolean')
    return { kind: 'unknown', why: 'cowshed gateway status named no running state' };
  if (status.pendingInstall !== undefined && typeof status.pendingInstall !== 'boolean')
    return { kind: 'unknown', why: 'cowshed gateway status named a pending install that is not a boolean' };
  const blockers: string[] = [];
  if (status.pendingInstall === true) blockers.push('another install holds the install slot');
  if (status.running) {
    if (typeof status.landTurnsError === 'string') blockers.push(`land turns unreadable: ${status.landTurnsError}`);
    const lands = landTurnHolders(status.landTurns);
    if (lands === undefined) {
      return {
        kind: 'unknown',
        why: 'cowshed gateway status reported land turns in a shape this reader does not know',
      };
    }
    blockers.push(...lands);
  }
  return blockers.length === 0 ? { kind: 'ready' } : { kind: 'busy', blockers };
}

/** The line the readiness verb prints: the verdict word, then its detail. */
export function readinessLine(verdict: Readiness): string {
  switch (verdict.kind) {
    case 'ready':
      return 'ready';
    case 'busy':
      return `busy ${verdict.blockers.join('; ')}`;
    case 'unknown':
      return `unknown ${verdict.why}`;
  }
}

/** One line per repository whose land turn is held: the first live ticket holds it. Undefined when malformed. */
function landTurnHolders(queues: unknown): readonly string[] | undefined {
  if (queues === undefined) return [];
  if (!Array.isArray(queues)) return undefined;
  const holders: string[] = [];
  for (const queue of queues) {
    if (!isRecord(queue) || typeof queue.repo !== 'string' || !Array.isArray(queue.tickets)) return undefined;
    const [holder] = queue.tickets;
    if (holder === undefined) continue;
    if (
      !isRecord(holder) ||
      typeof holder.workspace !== 'string' ||
      typeof holder.number !== 'number' ||
      typeof holder.pid !== 'number'
    ) {
      return undefined;
    }
    holders.push(
      'the land of ' +
        holder.workspace +
        ' holds the ' +
        queue.repo +
        ' land turn (ticket ' +
        holder.number +
        ', pid ' +
        holder.pid +
        ')',
    );
  }
  return holders;
}

/** What one run of setup came to. */
export type SetupOutcome =
  | { readonly kind: 'installed' }
  | { readonly kind: 'refused'; readonly why: string }
  | { readonly kind: 'failed'; readonly why: string };

/**
 * status and stdout are those of 'cowshed --json setup'. A failure says its typed error, never a scan of its text. The
 * typed code 'conflict' is a refusal, and it covers two things: work the install refuses under, which a fresh gateway
 * status names and which ends, and a build that lacks the installed one, which waiting does not change.
 */
export function setupOutcome(status: number, stdout: string): SetupOutcome {
  if (status === 0) return { kind: 'installed' };
  const parsed = parseJson(stdout);
  const error = parsed.parsed && isRecord(parsed.value) ? errorFields(parsed.value.error) : undefined;
  return {
    kind: error?.code === 'conflict' ? 'refused' : 'failed',
    why: `setup exited ${status}${error === undefined ? ' without an error envelope' : `: ${error.code}: ${error.message}`}`,
  };
}

/** One land-status outcome of an install. A command is the exact one to run, as the record states it. */
export type Outcome =
  | { readonly kind: 'installed' }
  | { readonly kind: 'skipped' | 'failed'; readonly why: string }
  | { readonly kind: 'attended' | 'unverified' | 'deferred'; readonly why: string; readonly command: string };

/**
 * The line an install prints for a land, which the declared after-land command's log keeps
 * (lands/<head>.after<n>.log): a UTC timestamp, the landed commit, then 'install' and the outcome. attended and
 * unverified name the command to run by hand, deferred the one to rerun once the named work has ended:
 * 'deferred: BLOCKER; BLOCKER; rerun COMMAND', split at its last '; rerun '.
 */
export function landStatusLine(timestamp: string, commit: string, outcome: Outcome): string {
  return `${timestamp} ${commit} install ${describe(outcome)}`;
}

function describe(outcome: Outcome): string {
  switch (outcome.kind) {
    case 'installed':
      return 'installed';
    case 'skipped':
      return `skipped: ${oneLine(outcome.why)}`;
    case 'failed':
      return `failed: ${oneLine(outcome.why)}`;
    case 'attended':
      return `attended: ${oneLine(outcome.why)}; run ${outcome.command}`;
    case 'unverified':
      return `unverified: ${oneLine(outcome.why)}; run ${outcome.command}`;
    case 'deferred':
      return `deferred: ${oneLine(outcome.why)}; rerun ${outcome.command}`;
  }
}

function oneLine(text: string): string {
  return text.replace(/\s+/g, ' ').trim();
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

type Parsed = { readonly parsed: true; readonly value: unknown } | { readonly parsed: false };

function parseJson(text: string): Parsed {
  try {
    return { parsed: true, value: JSON.parse(text) };
  } catch {
    return { parsed: false };
  }
}

function stringList(text: string): readonly string[] | undefined {
  const parsed = parseJson(text);
  if (!parsed.parsed || !Array.isArray(parsed.value)) return undefined;
  const items: string[] = [];
  for (const item of parsed.value) {
    if (typeof item !== 'string') return undefined;
    items.push(item);
  }
  return items;
}

/** The code and message of a cowshed --json error, or undefined when the value is not one. */
function errorFields(error: unknown): { readonly code: string; readonly message: string } | undefined {
  if (!isRecord(error) || typeof error.code !== 'string' || typeof error.message !== 'string') return undefined;
  return { code: error.code, message: error.message };
}

type Envelope =
  | { readonly kind: 'result'; readonly result: Record<string, unknown> }
  | { readonly kind: 'refused'; readonly why: string };

/** The result of a cowshed --json success envelope, or why the output is none. */
function successResult(text: string, verb: string): Envelope {
  const parsed = parseJson(text);
  if (!parsed.parsed || !isRecord(parsed.value)) {
    return { kind: 'refused', why: `cowshed ${verb} printed no JSON envelope` };
  }
  if (parsed.value.ok !== true) {
    const error = errorFields(parsed.value.error);
    const detail = error === undefined ? '' : `: ${error.code}: ${error.message}`;
    return { kind: 'refused', why: `cowshed ${verb} answered an error${detail}` };
  }
  return isRecord(parsed.value.result)
    ? { kind: 'result', result: parsed.value.result }
    : { kind: 'refused', why: `cowshed ${verb} answered no result` };
}

function fail(message: string): never {
  process.stderr.write(message);
  process.exit(64);
}

if (import.meta.main) {
  const USAGE =
    'usage: land-install.ts target SYSTEM MACHINE | wanted AFFECTED_JSON CHANGED_FILES | commit COMMIT... | ' +
    'stage-dir MAIN COMMIT | stale KEEP <NAMES | command MAIN BINARY | prompt DOCTOR_JSON | readiness STATUS_JSON | ' +
    'setup STATUS STDOUT | record TIMESTAMP COMMIT KIND [WHY [COMMAND]]\n';
  const [verb, ...args] = process.argv.slice(2);
  const [first, second, third, fourth, fifth] = args;
  if (verb === 'target' && args.length === 2 && first !== undefined && second !== undefined) {
    const cli = cliTarget(first, second);
    process.stdout.write(`${cli === undefined ? 'unsupported' : `${cli.target} ${cli.platform}`}\n`);
  } else if (verb === 'wanted' && args.length === 2 && first !== undefined && second !== undefined) {
    process.stdout.write(`${cowshedChanged(first, second)}\n`);
  } else if (verb === 'commit' && args.length > 0) {
    const bad = args.find((arg) => !isCommit(arg));
    if (bad !== undefined) fail(`land-install: ${JSON.stringify(bad)} is not a full commit id\n`);
  } else if (verb === 'stage-dir' && args.length === 2 && first !== undefined && second !== undefined) {
    const directory = stageDirectory(first, second);
    if (directory === undefined) fail(`land-install: ${JSON.stringify(second)} is not a full commit id\n`);
    process.stdout.write(`${directory}\n`);
  } else if (verb === 'stale' && args.length === 1 && first !== undefined && /^[0-9]+$/.test(first)) {
    const names = (await Bun.stdin.text()).split('\n').filter((name) => name !== '');
    for (const stale of staleStages(names, Number(first))) process.stdout.write(`${stale}\n`);
  } else if (verb === 'command' && args.length === 2 && first !== undefined && second !== undefined) {
    process.stdout.write(`${setupCommand(first, second)}\n`);
  } else if (verb === 'prompt' && args.length === 1 && first !== undefined) {
    process.stdout.write(`${promptLine(setupPrompt(first))}\n`);
  } else if (verb === 'readiness' && args.length === 1 && first !== undefined) {
    process.stdout.write(`${readinessLine(installReadiness(first))}\n`);
  } else if (
    verb === 'setup' &&
    args.length === 2 &&
    first !== undefined &&
    second !== undefined &&
    /^[0-9]+$/.test(first)
  ) {
    const outcome = setupOutcome(Number(first), second);
    process.stdout.write(`${outcome.kind === 'installed' ? 'installed' : `${outcome.kind} ${outcome.why}`}\n`);
  } else if (
    verb === 'record' &&
    args.length >= 3 &&
    first !== undefined &&
    second !== undefined &&
    third !== undefined
  ) {
    const why = fourth ?? '';
    const outcome = recordOutcome(third, why, fifth);
    if (outcome === undefined) fail(`land-install: cannot record ${JSON.stringify(third)} with these arguments\n`);
    process.stdout.write(`${landStatusLine(first, second, outcome)}\n`);
  } else {
    fail(USAGE);
  }
}

function recordOutcome(kind: string, why: string, command: string | undefined): Outcome | undefined {
  switch (kind) {
    case 'installed':
      return { kind };
    case 'skipped':
    case 'failed':
      return { kind, why };
    case 'attended':
    case 'unverified':
    case 'deferred':
      return command === undefined ? undefined : { kind, why, command };
    default:
      return undefined;
  }
}

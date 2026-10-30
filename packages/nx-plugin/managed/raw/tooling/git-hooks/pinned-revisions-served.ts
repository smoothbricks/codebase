/**
 * Every git revision Cargo.lock pins, judged against the remote it names, which is where CI fetches it.
 *
 * A workspace pins an external crate by git revision and builds it from the local Cargo git database, so a pin lands
 * with no publish and no waiting. CI meets the pin only once the tree is pushed: its runner has nothing but the remote,
 * and cargo (`net.git-fetch-with-cli`) asks the remote for the revision. When the remote does not serve it, every build
 * that reaches the crate fails ("upload-pack: not our ref"). So a land gate reports such a pin (`--report`), and the
 * pre-push hook holds the push until every remote serves its pinned revision (`--wait <seconds> <commit>...`), which
 * the pinned repository's own push normally does within minutes. Past the bound the hook refuses and names each pin,
 * so no tree CI cannot fetch is published.
 *
 * "Served" is what cargo does: fetch the revision by id, and when the remote refuses that, fetch its branches and tags
 * and look for the revision among them. A remote the fetch cannot reach proves nothing either way and is said as that.
 * Like the other managed git-hook helpers it imports only the runtime.
 */
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

/** One pinned revision of one remote, with the packages Cargo.lock builds from it. */
export interface Pin {
  readonly url: string;
  readonly rev: string;
  readonly packages: readonly string[];
}

export type PinsRead =
  | { readonly kind: 'pins'; readonly pins: readonly Pin[] }
  | { readonly kind: 'malformed'; readonly detail: string };

export type PinAnswer =
  | { readonly kind: 'served' }
  | { readonly kind: 'unserved' }
  | { readonly kind: 'unreachable'; readonly detail: string };

export type AskRemote = (url: string, rev: string) => Promise<PinAnswer>;

/** The hold's one use of time, so a test can move it by hand. */
export interface Clock {
  /** Milliseconds on a scale that only moves forward. */
  now(): number;
  sleep(ms: number): Promise<void>;
}

export const systemClock: Clock = { now: () => performance.now(), sleep: (ms) => Bun.sleep(ms) };

export type HoldOutcome =
  /** `rounds` counts the asks: one means the remotes served every pin at once. */
  | { readonly kind: 'served'; readonly waitedMs: number; readonly rounds: number }
  | {
      readonly kind: 'refused';
      readonly waitedMs: number;
      readonly outstanding: readonly (readonly [Pin, PinAnswer])[];
    };

/** How often a held push asks the remotes again. */
export const POLL_MS = 30_000;

const REVISION = /^[0-9a-f]{40}$/;

function malformed(detail: string): PinsRead {
  return { kind: 'malformed', detail };
}

/** The git sources of a Cargo.lock, `source = "git+<url>[?<reference>]#<revision>"`, one pin per remote and revision. */
export function readPins(lockText: string): PinsRead {
  let parsed: unknown;
  try {
    parsed = Bun.TOML.parse(lockText);
  } catch (error) {
    return malformed(`Cargo.lock is not TOML: ${error instanceof Error ? error.message : String(error)}`);
  }
  if (typeof parsed !== 'object' || parsed === null) return malformed('Cargo.lock is not a table');
  if (!('package' in parsed)) return { kind: 'pins', pins: [] };
  const entries = parsed.package;
  if (!Array.isArray(entries)) return malformed('Cargo.lock `package` is not an array of tables');
  const pins = new Map<string, { readonly url: string; readonly rev: string; readonly packages: string[] }>();
  for (const entry of entries) {
    if (typeof entry !== 'object' || entry === null || !('name' in entry) || typeof entry.name !== 'string')
      return malformed(`a Cargo.lock package has no name: ${JSON.stringify(entry)}`);
    if (!('source' in entry)) continue;
    const source = entry.source;
    if (typeof source !== 'string') return malformed(`${entry.name}'s source is not a string`);
    if (!source.startsWith('git+')) continue;
    const hash = source.lastIndexOf('#');
    const rev = hash === -1 ? '' : source.slice(hash + 1);
    if (!REVISION.test(rev)) return malformed(`${entry.name}'s git source names no full revision: ${source}`);
    const located = source.slice('git+'.length, hash);
    const query = located.indexOf('?');
    const url = query === -1 ? located : located.slice(0, query);
    const key = `${url}#${rev}`;
    const pin = pins.get(key) ?? { url, rev, packages: [] };
    pin.packages.push(entry.name);
    pins.set(key, pin);
  }
  return { kind: 'pins', pins: [...pins.values()] };
}

/** The pins of several Cargo.lock files, each remote and revision once. */
export function unitePins(reads: readonly (readonly Pin[])[]): Pin[] {
  const united = new Map<string, { readonly url: string; readonly rev: string; readonly packages: Set<string> }>();
  for (const pins of reads)
    for (const pin of pins) {
      const key = `${pin.url}#${pin.rev}`;
      const entry = united.get(key) ?? { url: pin.url, rev: pin.rev, packages: new Set<string>() };
      for (const name of pin.packages) entry.packages.add(name);
      united.set(key, entry);
    }
  return [...united.values()].map(({ url, rev, packages }) => ({ url, rev, packages: [...packages] }));
}

export function describePin(pin: Pin, answer: PinAnswer): string {
  const built = `Cargo.lock builds ${pin.packages.join(', ')} from it`;
  switch (answer.kind) {
    case 'served':
      return `${pin.url} serves ${pin.rev} (${built})`;
    case 'unserved':
      return `${pin.url} does not serve ${pin.rev} (${built})`;
    case 'unreachable':
      return `could not ask ${pin.url} for ${pin.rev} (${built}): ${answer.detail}`;
  }
}

function formatSeconds(ms: number): string {
  return `${Math.round(ms / 1000)} s`;
}

/**
 * Asks the remotes until each serves its pin, or until `boundMs` has passed since the first ask. An unreachable remote
 * is asked again like an unserving one: neither proves the push safe.
 */
export async function holdUntilServed(
  pins: readonly Pin[],
  ask: AskRemote,
  clock: Clock,
  boundMs: number,
  pollMs: number,
  say: (line: string) => void,
): Promise<HoldOutcome> {
  const started = clock.now();
  let outstanding = pins;
  for (let rounds = 1; ; rounds += 1) {
    const answers = await Promise.all(
      outstanding.map(async (pin): Promise<readonly [Pin, PinAnswer]> => [pin, await ask(pin.url, pin.rev)]),
    );
    const waiting = answers.filter(([, answer]) => answer.kind !== 'served');
    const waitedMs = clock.now() - started;
    if (waiting.length === 0) return { kind: 'served', waitedMs, rounds };
    if (waitedMs >= boundMs) return { kind: 'refused', waitedMs, outstanding: waiting };
    for (const [pin, answer] of waiting)
      say(`waiting (${formatSeconds(waitedMs)} of ${formatSeconds(boundMs)}): ${describePin(pin, answer)}`);
    outstanding = waiting.map(([pin]) => pin);
    await clock.sleep(Math.min(pollMs, boundMs - waitedMs));
  }
}

interface GitRun {
  readonly exitCode: number;
  readonly stdout: string;
  readonly stderr: string;
}

async function git(args: readonly string[], cwd: string): Promise<GitRun> {
  const child = Bun.spawn(['git', ...args], {
    cwd,
    stdin: 'ignore',
    stdout: 'pipe',
    stderr: 'pipe',
    // A hook and a gate run unattended: a remote that wants credentials it has no helper for refuses instead of asking.
    // A partial repository never goes back to its remote for an object it lacks: absence is the answer.
    env: { ...process.env, GIT_TERMINAL_PROMPT: '0', GIT_NO_LAZY_FETCH: '1' },
  });
  const [stdout, stderr, exitCode] = await Promise.all([
    new Response(child.stdout).text(),
    new Response(child.stderr).text(),
    child.exited,
  ]);
  return { exitCode, stdout, stderr: stderr.trim() };
}

async function gitOrThrow(args: readonly string[], cwd: string): Promise<void> {
  const run = await git(args, cwd);
  if (run.exitCode !== 0) throw new Error(`git ${args.join(' ')} exited ${run.exitCode} in ${cwd}: ${run.stderr}`);
}

/**
 * Asks each remote through a scratch repository of its own under `scratchRoot`, kept for the asker's life so that a
 * repeated ask fetches only what changed. Commits are all it fetches (`--filter=tree:0`).
 */
export function remoteAsker(scratchRoot: string): AskRemote {
  const repositories = new Map<string, Promise<string>>();
  const repository = (url: string): Promise<string> => {
    const known = repositories.get(url);
    if (known !== undefined) return known;
    const directory = join(scratchRoot, String(repositories.size));
    const created = (async () => {
      await gitOrThrow(['init', '--quiet', '--bare', directory], scratchRoot);
      await gitOrThrow(['remote', 'add', 'pin', url], directory);
      return directory;
    })();
    repositories.set(url, created);
    return created;
  };
  const fetch = ['fetch', '--quiet', '--no-tags', '--no-write-fetch-head', '--filter=tree:0'];
  return async (url, rev) => {
    const directory = await repository(url);
    const direct = await git([...fetch, '--depth=1', 'pin', rev], directory);
    if (direct.exitCode === 0) return { kind: 'served' };
    const listed = await git(
      [...fetch, 'pin', '+refs/heads/*:refs/pinned/heads/*', '+refs/tags/*:refs/pinned/tags/*'],
      directory,
    );
    if (listed.exitCode !== 0)
      return { kind: 'unreachable', detail: listed.stderr || direct.stderr || `git fetch exited ${listed.exitCode}` };
    const found = await git(['cat-file', '-e', `${rev}^{commit}`], directory);
    return found.exitCode === 0 ? { kind: 'served' } : { kind: 'unserved' };
  };
}

/** Cargo.lock as `commit` holds it, or `undefined` when that tree has none. */
async function lockAt(commit: string, root: string): Promise<string | undefined> {
  const shown = await git(['cat-file', '-p', `${commit}:Cargo.lock`], root);
  if (shown.exitCode === 0) return shown.stdout;
  const has = await git(['cat-file', '-e', `${commit}^{tree}`], root);
  if (has.exitCode === 0) return undefined;
  throw new Error(`cannot read ${commit}:Cargo.lock: ${shown.stderr}`);
}

function pinsOf(text: string, where: string): readonly Pin[] {
  const read = readPins(text);
  if (read.kind === 'malformed') throw new Error(`${where}: ${read.detail}`);
  return read.pins;
}

const USAGE =
  'usage: pinned-revisions-served.ts --report\n       pinned-revisions-served.ts --wait <seconds> <commit>...';

async function report(root: string, ask: AskRemote): Promise<number> {
  const file = Bun.file(join(root, 'Cargo.lock'));
  if (!(await file.exists())) return 0;
  const pins = pinsOf(await file.text(), join(root, 'Cargo.lock'));
  const answers = await Promise.all(pins.map(async (pin) => [pin, await ask(pin.url, pin.rev)] as const));
  const waiting = answers.filter(([, answer]) => answer.kind !== 'served');
  if (waiting.length === 0) {
    const remotes = new Set(pins.map((pin) => pin.url)).size;
    console.log(
      `PINNED-REVISIONS: every git revision Cargo.lock pins is served (${pins.length} pin(s), ${remotes} remote(s))`,
    );
    return 0;
  }
  console.log(
    `PINNED-REVISIONS note: ${waiting.length} pinned revision(s) not served by their remote, reported, not refused: the land builds them from the local Cargo git database, and the push (tooling/git-hooks/pre-push.sh) waits until the remote serves them, since CI fetches them there:\n` +
      waiting.map(([pin, answer]) => `  ${describePin(pin, answer)}`).join('\n'),
  );
  return 0;
}

async function hold(root: string, boundSeconds: number, commits: readonly string[], ask: AskRemote): Promise<number> {
  const locks = await Promise.all(commits.map(async (commit) => [commit, await lockAt(commit, root)] as const));
  const pins = unitePins(
    locks.flatMap(([commit, text]) => (text === undefined ? [] : [pinsOf(text, `${commit}:Cargo.lock`)])),
  );
  if (pins.length === 0) return 0;
  const say = (line: string) => console.error(`pre-push: ${line}`);
  const outcome = await holdUntilServed(pins, ask, systemClock, boundSeconds * 1000, POLL_MS, say);
  if (outcome.kind === 'served') {
    if (outcome.rounds > 1) say(`every pinned revision is served after ${formatSeconds(outcome.waitedMs)}`);
    return 0;
  }
  for (const [pin, answer] of outcome.outstanding) say(describePin(pin, answer));
  say(
    `refused: CI fetches every git revision Cargo.lock pins from the remote it names, and ${outcome.outstanding.length} ` +
      `of them are not served after ${formatSeconds(outcome.waitedMs)}. Push each one to its remote (the pinned ` +
      "repository's own land normally does), then push again.",
  );
  return 1;
}

if (import.meta.main) {
  const [mode, ...rest] = process.argv.slice(2);
  const root = process.cwd();
  const scratch = mkdtempSync(join(tmpdir(), 'pinned-revisions-'));
  try {
    const ask = remoteAsker(scratch);
    if (mode === '--report' && rest.length === 0) process.exitCode = await report(root, ask);
    else if (mode === '--wait' && rest.length > 1 && /^[1-9]\d*$/.test(rest[0] ?? ''))
      process.exitCode = await hold(root, Number(rest[0]), rest.slice(1), ask);
    else {
      console.error(USAGE);
      process.exitCode = 2;
    }
  } finally {
    rmSync(scratch, { recursive: true, force: true });
  }
}

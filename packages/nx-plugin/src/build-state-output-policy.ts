import { type Dirent, lstatSync, readdirSync, statSync } from 'node:fs';
import { posix } from 'node:path';
import { Minimatch } from 'minimatch';
import type { ProjectGraph, ProjectGraphProjectNode } from 'nx/src/config/project-graph';
import type { TargetConfiguration } from 'nx/src/devkit-exports.js';
import type { expandOutputs } from 'nx/src/native';
import { getOutputsForTargetAndConfiguration, interpolate } from 'nx/src/tasks-runner/utils';

/** The read-only path records returned by `cowshed build-state --json`. */
export interface BuildStatePath {
  readonly checkout: string;
  readonly volume: string;
}

/**
 * What an output names, as its declaration states it: a declaration ending in `/` names a directory, any other a file.
 * Nx records no kind and its interpolation drops a trailing `/`, so the kind is read from the declaration as written.
 */
export type OutputKind = 'file' | 'directory';

/**
 * One output a task declares (a target under one configuration), as Nx interpolates it. `undefined` is the target's own
 * options, a task only when it has no `defaultConfiguration`: Nx resolves a run naming none to that.
 */
export interface DeclaredOutput {
  readonly project: string;
  readonly target: string;
  readonly configuration: string | undefined;
  readonly output: string;
  readonly kind: OutputKind;
}

export interface BuildStateOutputFinding extends DeclaredOutput {
  readonly buildStatePath: string;
}

/** `project:target[:configuration]`, as Nx names the task. */
function taskName({
  project,
  target,
  configuration,
}: Pick<DeclaredOutput, 'project' | 'target' | 'configuration'>): string {
  return `${project}:${target}${configuration ? `:${configuration}` : ''}`;
}

export class BuildStateOutputError extends Error {
  constructor(readonly findings: readonly BuildStateOutputFinding[]) {
    super(
      `Nx outputs must not cover cowshed build-state paths:\n${findings
        .map(
          (finding) =>
            `  ${taskName(finding)}: ${JSON.stringify(finding.output)} covers ${JSON.stringify(finding.buildStatePath)}`,
        )
        .join('\n')}`,
    );
    this.name = 'BuildStateOutputError';
  }
}

/** A target whose outputs Nx refuses to interpolate, named: Nx's own error names only the output. */
export class OutputDeclarationError extends Error {
  constructor(
    readonly project: string,
    readonly target: string,
    readonly configuration: string | undefined,
    cause: unknown,
  ) {
    super(
      `${taskName({ project, target, configuration })}: ${cause instanceof Error ? cause.message : String(cause)}`,
      { cause },
    );
    this.name = 'OutputDeclarationError';
  }
}

/**
 * Two tasks, each a target under one configuration, that declare one output path or pattern, or one inside the other.
 * Each side names its project, target, configuration and output.
 */
export interface OutputOverlap {
  readonly left: DeclaredOutput;
  readonly right: DeclaredOutput;
}

export class OutputOverlapError extends Error {
  constructor(readonly overlaps: readonly OutputOverlap[]) {
    super(
      `Each Nx output path has one producing task; these tasks declare one path or pattern, or one inside another:\n${overlaps
        .map(
          ({ left, right }) =>
            `  ${taskName(left)}: ${JSON.stringify(left.output)} overlaps ${taskName(right)}: ${JSON.stringify(right.output)}`,
        )
        .join('\n')}`,
    );
    this.name = 'OutputOverlapError';
  }
}

/** What a path is on disk: absent, or the kind a restore of it would write. */
export type OutputOnDisk = OutputKind | 'absent';

/** A literal output whose declared kind the built tree contradicts. */
export interface OutputKindFinding extends DeclaredOutput {
  readonly onDisk: OutputKind;
}

export class OutputKindError extends Error {
  constructor(readonly findings: readonly OutputKindFinding[]) {
    super(
      `Each Nx output declaration states its kind: a directory output ends in \`/\`, a file output does not. These contradict the built tree:\n${findings
        .map(
          (finding) =>
            `  ${taskName(finding)}: ${JSON.stringify(finding.output)} is declared a ${finding.kind} but is a ${finding.onDisk} on disk`,
        )
        .join('\n')}`,
    );
    this.name = 'OutputKindError';
  }
}

/** Task outputs naming the workspace root itself or a path outside it: no file of the checkout is theirs to name. */
export class OutputEscapeError extends Error {
  constructor(readonly findings: readonly DeclaredOutput[]) {
    super(
      `Each Nx output names a path inside its workspace, below the root; these do not:\n${findings
        .map((finding) => `  ${taskName(finding)}: ${JSON.stringify(finding.output)}`)
        .join('\n')}`,
    );
    this.name = 'OutputEscapeError';
  }
}

/** A path segment holding glob syntax, read conservatively: any character that may start a pattern. */
const GLOB_PART = /[*?[\]{}()!]/;

function contains(parent: string, child: string): boolean {
  return parent === child || child.startsWith(parent.endsWith('/') ? parent : `${parent}/`);
}

/** The non-glob directory prefix, conservatively including every possible glob match. */
function staticPrefix(output: string): string {
  const parts = output.replaceAll('\\', '/').split('/');
  const firstGlob = parts.findIndex((part) => GLOB_PART.test(part));
  return firstGlob < 0 ? parts.join('/') : parts.slice(0, firstGlob).join('/') || '.';
}

function absoluteRoot(workspaceRoot: string): string {
  const root = workspaceRoot.replaceAll('\\', '/');
  if (!posix.isAbsolute(root)) throw new Error('Nx output validation requires an absolute workspace root');
  return root;
}

/**
 * What one target declares under one configuration. Nx's task helper rejects root globs and absolute declarations
 * before expanding them, and its interpolation throws on a misplaced `{workspaceRoot}` without naming the target; the
 * declarations are interpolated here, so those outputs are judged too and a refusal names its target.
 */
function targetOutputs(
  project: string,
  target: string,
  options: TargetConfiguration,
  configuration: string | undefined,
  node: ProjectGraphProjectNode,
  root: string,
): Pick<DeclaredOutput, 'output' | 'kind'>[] {
  try {
    return options.outputs
      ? options.outputs
          .map((output) => ({
            output: interpolate(output, {
              workspaceRoot: root,
              projectRoot: node.data.root,
              projectName: node.name,
              project: { ...node.data, name: node.name },
              options: { ...options.options, ...(configuration ? options.configurations?.[configuration] : {}) },
            }),
            kind: declaredKind(output),
          }))
          .filter(({ output }) => output && !/{(projectRoot|workspaceRoot|(options.*))}/.test(output))
      : // A target declaring no outputs takes Nx's fallback: its options' outputPath, or for build and prepare the
        // legacy dist, build and public directories. Each names a directory, and no declaration states otherwise.
        getOutputsForTargetAndConfiguration({ project, target, configuration }, {}, node).map((output) => ({
          output,
          kind: 'directory' as const,
        }));
  } catch (error) {
    // Nx tests a template against a module-level global regex before it throws, leaving that regex's lastIndex past
    // the first placeholder; the next interpolation in this process would then skip its placeholders and drop its
    // output. One interpolation rewinds it: a test that fails resets lastIndex.
    interpolate('', {});
    throw new OutputDeclarationError(project, target, configuration, error);
  }
}

function declaredKind(declaration: string): OutputKind {
  return declaration.endsWith('/') ? 'directory' : 'file';
}

/**
 * The configurations a target's tasks run under: each named one, and the run naming none. Nx resolves that run to the
 * target's `defaultConfiguration` when it has one, so the target's own options are a task only without one.
 */
export function taskConfigurations({
  defaultConfiguration,
  configurations,
}: TargetConfiguration): (string | undefined)[] {
  const named = Object.keys(configurations ?? {});
  return defaultConfiguration ? [...new Set([defaultConfiguration, ...named])] : [undefined, ...named];
}

/**
 * Every output of every task in the resolved graph (project overrides included), as Nx interpolates it: one task per
 * configuration a target runs under. A target declaring no outputs yields Nx's fallback outputs. Pass the absolute
 * workspace root. This is the one reading of what a task writes: every output policy takes its declarations from here.
 */
export function declaredOutputs(graph: ProjectGraph, root: string): DeclaredOutput[] {
  const declared: DeclaredOutput[] = [];
  for (const [project, node] of Object.entries(graph.nodes)) {
    for (const [target, options] of Object.entries(node.data.targets ?? {})) {
      for (const configuration of taskConfigurations(options)) {
        for (const output of targetOutputs(project, target, options, configuration, node, root)) {
          declared.push({ project, target, configuration, ...output });
        }
      }
    }
  }
  return declared;
}

/**
 * Enforce spec 16 at the consuming repository's Nx lint boundary, using its already resolved
 * graph (including project overrides). Never construct a graph during cowshed job admission.
 * An Nx restore may remove an output before restoring it, so ancestors and overlapping glob
 * prefixes are unsafe too: they can replace the fixed links even without naming a tool's files.
 */
export function refuseOutputsUnderBuildState(
  graph: ProjectGraph,
  buildStatePaths: readonly BuildStatePath[],
  workspaceRoot: string,
): void {
  const root = absoluteRoot(workspaceRoot);
  const paths = buildStatePaths.map(({ checkout }) => ({ checkout, absolute: posix.resolve(root, checkout) }));
  const findings: BuildStateOutputFinding[] = [];
  for (const declared of declaredOutputs(graph, root)) {
    const prefix = posix.resolve(root, staticPrefix(declared.output));
    for (const path of paths) {
      if (contains(prefix, path.absolute) || contains(path.absolute, prefix)) {
        findings.push({ ...declared, buildStatePath: path.checkout });
      }
    }
  }
  if (findings.length) throw new BuildStateOutputError(findings);
}

/** One path a target's output names, absolute, and workspace-relative for matching. */
interface PathClaim {
  readonly form: 'path';
  readonly declared: DeclaredOutput;
  readonly absolute: string;
  readonly relative: string;
}

/** One pattern a target's output names: the directory it cannot write outside of, and the pattern compiled once. */
interface GlobClaim {
  readonly form: 'glob';
  readonly declared: DeclaredOutput;
  readonly prefix: string;
  readonly matcher: Minimatch;
}

type OutputClaim = PathClaim | GlobClaim;

/**
 * Each task's declared outputs once per distinct path and declared kind; a negation claims nothing. A task is a target
 * under one configuration, and the configuration is part of the identity: two configurations declaring one path are
 * two claims, and each is judged. So is the kind: a path a task declares as both a file and a directory is two claims.
 */
function outputClaims(declared: readonly DeclaredOutput[], root: string): OutputClaim[] {
  const seen = new Set<string>();
  const claims: OutputClaim[] = [];
  for (const output of declared) {
    if (output.output.startsWith('!')) continue;
    const absolute = posix.resolve(root, output.output);
    const key = JSON.stringify([output.project, output.target, output.configuration, absolute, output.kind]);
    if (seen.has(key)) continue;
    seen.add(key);
    const relative = posix.relative(root, absolute);
    claims.push(
      GLOB_PART.test(relative)
        ? {
            form: 'glob',
            declared: output,
            prefix: posix.resolve(root, staticPrefix(relative)),
            // Nx's own matcher, as Nx pins it. Dotfiles match: a restore writes them like any other file.
            matcher: new Minimatch(relative, { dot: true }),
          }
        : { form: 'path', declared: output, absolute, relative },
    );
  }
  return claims;
}

/**
 * A pattern conflicts with a path at or above its static prefix, a path it matches, or a directory it can reach inside.
 * Reaching is minimatch's partial match: it expands braces before it splits segments, so an alternative that crosses a
 * slash is walked whole, and a directory whose every segment matches the pattern's leading segments is one the pattern
 * can write under. A file the pattern does not match below its prefix is not written by it, whatever the pattern writes
 * beside it; a file at or above the prefix collides by kind, since the pattern needs a directory there to write into.
 */
function patternMeetsPath(pattern: GlobClaim, path: PathClaim): boolean {
  return (
    contains(path.absolute, pattern.prefix) ||
    pattern.matcher.match(path.relative) ||
    (path.declared.kind === 'directory' && pattern.matcher.match(path.relative, true))
  );
}

/**
 * Two paths conflict at, under or above each other. Two patterns conflict only as one pattern named twice, which
 * matches the same files: Nx restores a pattern file by file, so two different patterns collide only on a file both
 * match, and that cannot be named without the tree.
 */
function conflicts(left: OutputClaim, right: OutputClaim): boolean {
  if (left.form === 'path') {
    return right.form === 'path'
      ? contains(left.absolute, right.absolute) || contains(right.absolute, left.absolute)
      : patternMeetsPath(right, left);
  }
  return right.form === 'path' ? patternMeetsPath(left, right) : left.matcher.pattern === right.matcher.pattern;
}

/**
 * One producing task per output path: refuse any task whose output is at, under or above another task's output. A task
 * is a target under one configuration (`project:target[:configuration]`, as Nx names it): two targets conflict, and so
 * do two configurations of one target. `outputs` belongs to the target, so only an options placeholder
 * (`{options.out}`) that the configurations set apart gives each its own. A task's own outputs never conflict with
 * each other. Nx restores a directory output as one tree, so a second task writing inside it makes the first task's
 * tree differ from its cache artifact after every build: each run restores one producer over the other, and neither is
 * ever a cache hit.
 */
export function refuseOverlappingOutputs(graph: ProjectGraph, workspaceRoot: string): void {
  const root = absoluteRoot(workspaceRoot);
  const claims = outputClaims(declaredOutputs(graph, root), root);
  const overlaps: OutputOverlap[] = [];
  for (const [index, left] of claims.entries()) {
    for (const right of claims.slice(index + 1)) {
      const sameTask =
        left.declared.project === right.declared.project &&
        left.declared.target === right.declared.target &&
        left.declared.configuration === right.declared.configuration;
      if (!sameTask && conflicts(left, right)) overlaps.push({ left: left.declared, right: right.declared });
    }
  }
  if (overlaps.length) throw new OutputOverlapError(overlaps);
}

/** The kind of `path` on disk, following links as a restore's reader does; a path under a file is absent. */
export function outputOnDisk(path: string): OutputOnDisk {
  try {
    const stats = statSync(path, { throwIfNoEntry: false });
    return stats === undefined ? 'absent' : stats.isDirectory() ? 'directory' : 'file';
  } catch (error) {
    if (error instanceof Error && 'code' in error && error.code === 'ENOTDIR') return 'absent';
    throw error;
  }
}

/**
 * Hold every literal output's declared kind to the built tree under `workspaceRoot`: a directory output must be a
 * directory and a file output a file. Run where the outputs exist after a build; an output absent on disk is not judged,
 * and a pattern names its files by matching, so it states no kind to hold. The overlap policy reads the declared kind
 * alone; this keeps that declaration honest.
 */
export function refuseMisdeclaredOutputKinds(graph: ProjectGraph, workspaceRoot: string): void {
  const root = absoluteRoot(workspaceRoot);
  const findings: OutputKindFinding[] = [];
  for (const claim of outputClaims(declaredOutputs(graph, root), root)) {
    if (claim.form !== 'path') continue;
    const onDisk = outputOnDisk(claim.absolute);
    if (onDisk !== 'absent' && onDisk !== claim.declared.kind) findings.push({ ...claim.declared, onDisk });
  }
  if (findings.length) throw new OutputKindError(findings);
}

/** What a path itself is, read as written: a symbolic link is `other`, never what it points at. */
type EntryKind = OutputKind | 'other' | 'absent';

/** A path that is gone, or that runs through a file: what an output not written yet reads as. */
function isAbsent(error: unknown): boolean {
  return error instanceof Error && 'code' in error && (error.code === 'ENOENT' || error.code === 'ENOTDIR');
}

/** The failure a read under `existingOutputFiles` raises: the operation and the workspace-relative path it read. */
function readError(operation: string, relative: string, cause: unknown): Error {
  return new Error(
    `existingOutputFiles: ${operation} ${JSON.stringify(relative)} failed: ${cause instanceof Error ? cause.message : String(cause)}`,
    { cause },
  );
}

/** What the workspace-relative path itself is under `root`, without following a link at it. */
function entryKind(root: string, relative: string): EntryKind {
  try {
    const stats = lstatSync(posix.join(root, relative), { throwIfNoEntry: false });
    return stats === undefined ? 'absent' : stats.isDirectory() ? 'directory' : stats.isFile() ? 'file' : 'other';
  } catch (error) {
    if (isAbsent(error)) return 'absent';
    throw readError('lstat', relative, error);
  }
}

/** The entries of the directory at the workspace-relative path, each read as written; none once it is gone. */
function directoryEntries(root: string, relative: string): Dirent[] {
  try {
    return readdirSync(posix.join(root, relative), { withFileTypes: true });
  } catch (error) {
    if (isAbsent(error)) return [];
    throw readError('readdir', relative, error);
  }
}

/**
 * An output as stock Nx's expansion reads it: relative to `root`, a negation keeping its `!`. `undefined` when the
 * output names `root` itself or a path outside it.
 */
function outputEntry(output: string, root: string): string | undefined {
  const negated = output.startsWith('!');
  const relative = posix.relative(root, posix.resolve(root, negated ? output.slice(1) : output));
  if (relative === '' || relative === '..' || relative.startsWith('../')) return undefined;
  return negated ? `!${relative}` : relative;
}

/**
 * Whether some ancestor of `relative` below `root` is not a directory as written, a symbolic link above all: a path
 * reached only through one is never listed under that spelling.
 */
function throughNonDirectory(root: string, relative: string): boolean {
  const parts = relative.split('/');
  let ancestor = '';
  for (const [index, part] of parts.entries()) {
    if (index === parts.length - 1) break;
    ancestor = ancestor === '' ? part : `${ancestor}/${part}`;
    if (entryKind(root, ancestor) !== 'directory') return true;
  }
  return false;
}

/** Add every regular file beneath the directory `relative` to `files`, never descending a symbolic link. */
function collectDirectory(root: string, relative: string, files: Set<string>): void {
  for (const entry of directoryEntries(root, relative)) {
    const child = `${relative}/${entry.name}`;
    if (entry.isFile()) files.add(child);
    else if (entry.isDirectory()) collectDirectory(root, child, files);
  }
}

/**
 * Add what one path stock Nx's expansion returned contributes to `files`: a regular file itself, every regular file
 * beneath a directory, and nothing for a symbolic link, any other kind, an absent path, or a path reached through a
 * link above it.
 */
function collectExpanded(root: string, relative: string, files: Set<string>): void {
  if (throughNonDirectory(root, relative)) return;
  const kind = entryKind(root, relative);
  if (kind === 'file') files.add(relative);
  else if (kind === 'directory') collectDirectory(root, relative, files);
}

/**
 * Every regular file the resolved graph's outputs name under `workspaceRoot`, workspace-relative, sorted and once each.
 * Every task counts, whether or not a run selected it: each target under each configuration it runs under, Nx's
 * fallback outputs of a target declaring none included (`declaredOutputs`).
 *
 * Each task's outputs are expanded on their own with stock Nx's `expandOutputs` (`expand`, from the caller's workspace Nx), the native expansion Nx's cache uses,
 * so a task's negation drops only its own matches, never a path another task claims. An expanded regular file is
 * listed; an expanded directory contributes every regular file beneath it, read without descending a symbolic link at,
 * above or below it; a symbolic link or other entry is skipped, and an absent path contributes nothing. Any other
 * read failure throws, naming the operation and the workspace-relative path, so an unreadable directory beneath a
 * literal directory output is refused rather than passed over. Stock expansion silently drops entries it cannot read
 * while matching a glob; that is Nx's behaviour, and it is accepted here. An undeclared generated build input is
 * outside this contract: only a file some task's declared output names is listed. An output naming the workspace root
 * or a path outside it throws an `OutputEscapeError` naming every such task, before any disk access.
 */
export function existingOutputFiles(
  graph: ProjectGraph,
  workspaceRoot: string,
  expand: typeof expandOutputs,
): string[] {
  const root = absoluteRoot(workspaceRoot);
  const tasks = new Map<string, string[]>();
  const escapes: DeclaredOutput[] = [];
  for (const declared of declaredOutputs(graph, root)) {
    const entry = outputEntry(declared.output, root);
    if (entry === undefined) {
      escapes.push(declared);
      continue;
    }
    const task = JSON.stringify([declared.project, declared.target, declared.configuration]);
    const entries = tasks.get(task);
    if (entries) entries.push(entry);
    else tasks.set(task, [entry]);
  }
  if (escapes.length) throw new OutputEscapeError(escapes);
  const files = new Set<string>();
  const collected = new Set<string>();
  for (const entries of tasks.values()) {
    for (const expanded of expand(root, entries)) {
      if (collected.has(expanded)) continue;
      collected.add(expanded);
      collectExpanded(root, expanded, files);
    }
  }
  return [...files].sort();
}

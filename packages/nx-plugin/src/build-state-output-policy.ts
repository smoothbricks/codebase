import { statSync } from 'node:fs';
import { posix } from 'node:path';
import { Minimatch } from 'minimatch';
import type { ProjectGraph, ProjectGraphProjectNode } from 'nx/src/config/project-graph';
import type { TargetConfiguration } from 'nx/src/devkit-exports.js';
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

/** One output a target declares under one configuration (`undefined`: its default options), as Nx interpolates it. */
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

/** Two targets that declare one output path, or one inside the other. */
export interface OutputOverlap {
  readonly left: DeclaredOutput;
  readonly right: DeclaredOutput;
}

export class OutputOverlapError extends Error {
  constructor(readonly overlaps: readonly OutputOverlap[]) {
    super(
      `Each Nx output path has one producing target; these targets declare one path, or one inside another:\n${overlaps
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

/** Every output of every target in the resolved graph (project overrides included), for each configuration. */
function declaredOutputs(graph: ProjectGraph, root: string): DeclaredOutput[] {
  const declared: DeclaredOutput[] = [];
  for (const [project, node] of Object.entries(graph.nodes)) {
    for (const [target, options] of Object.entries(node.data.targets ?? {})) {
      for (const configuration of [undefined, ...Object.keys(options.configurations ?? {})]) {
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
 * Each target's declared outputs once per distinct path and declared kind across its configurations; a negation claims
 * nothing. The kind is part of the identity, so a path a target declares as both a file and a directory is two claims,
 * and each is judged.
 */
function outputClaims(declared: readonly DeclaredOutput[], root: string): OutputClaim[] {
  const seen = new Set<string>();
  const claims: OutputClaim[] = [];
  for (const output of declared) {
    if (output.output.startsWith('!')) continue;
    const absolute = posix.resolve(root, output.output);
    const key = JSON.stringify([output.project, output.target, absolute, output.kind]);
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
 * Two paths conflict at, under or above each other. Two patterns are not judged: Nx restores a pattern file by file,
 * so only a file both match collides, and a pattern pair cannot name one without the tree.
 */
function conflicts(left: OutputClaim, right: OutputClaim): boolean {
  if (left.form === 'path') {
    return right.form === 'path'
      ? contains(left.absolute, right.absolute) || contains(right.absolute, left.absolute)
      : patternMeetsPath(right, left);
  }
  return right.form === 'path' && patternMeetsPath(left, right);
}

/**
 * One producing target per output path: refuse any target whose output is at, under or above another target's output,
 * across every configuration of the resolved graph. Nx restores a directory output as one tree, so a second target
 * writing inside it makes the first target's tree differ from its cache artifact after every build: each run restores
 * one producer over the other, and neither is ever a cache hit.
 */
export function refuseOverlappingOutputs(graph: ProjectGraph, workspaceRoot: string): void {
  const root = absoluteRoot(workspaceRoot);
  const claims = outputClaims(declaredOutputs(graph, root), root);
  const overlaps: OutputOverlap[] = [];
  for (const [index, left] of claims.entries()) {
    for (const right of claims.slice(index + 1)) {
      const sameTarget =
        left.declared.project === right.declared.project && left.declared.target === right.declared.target;
      if (!sameTarget && conflicts(left, right)) overlaps.push({ left: left.declared, right: right.declared });
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

import { existsSync } from 'node:fs';
import { isAbsolute, join, posix } from 'node:path';
import type { ProjectConfiguration, ProjectGraphProjectNode, TargetConfiguration } from 'nx/src/devkit-exports.js';
import { readJsonFile } from 'nx/src/devkit-exports.js';
import { matchGlobPaths } from 'nx/src/native/index.js';
import { mergeTargetConfigurations } from 'nx/src/project-graph/utils/project-configuration/target-merging.js';
import { getOutputsForTargetAndConfiguration } from 'nx/src/tasks-runner/utils.js';
import { splitTargetFromConfigurations } from 'nx/src/utils/split-target.js';
import { type CargoProducerOutputInput, cargoProducerOutputInputs } from './cargo-workspace.js';

interface Producer {
  readonly id: string;
  readonly outputs: readonly string[];
}

interface GeneratedLeafInputs {
  readonly dependsOn: string[];
  readonly inputs: (string | CargoProducerOutputInput)[];
}

export interface CargoTestGeneratedInputs {
  forLeaf(inputs: readonly (string | object)[], projectRoot: string): GeneratedLeafInputs;
}

const workspacePrefix = '{workspaceRoot}/';
const projectPrefix = '{projectRoot}/';

/** The closure emits exact paths or crate trees. Keep its exclusions when matching producers. */
function closurePatterns(inputs: readonly (string | object)[], projectRoot: string): string[] {
  return inputs.flatMap((input) => {
    if (typeof input !== 'string') return [];
    const negated = input.startsWith('!');
    const path = negated ? input.slice(1) : input;
    const prefix = negated ? '!' : '';
    if (path.startsWith(workspacePrefix)) return [prefix + path.slice(workspacePrefix.length)];
    if (path.startsWith(projectPrefix)) return [prefix + posix.join(projectRoot, path.slice(projectPrefix.length))];
    return [];
  });
}

/** A producer declares the file itself, or a directory with a trailing slash. No generated file must exist yet. */
function outputCandidates(output: string, patterns: string[]): string[] {
  if (!output.endsWith('/')) return [output];
  const candidates = [`${output}__generated__`];
  // A directory may contain an entire crate tree, or a single separately named input outside that tree.
  for (const pattern of patterns) {
    if (!pattern.startsWith(output)) continue;
    candidates.push(pattern.endsWith('/**/*') ? posix.join(pattern.slice(0, -5), '__generated__') : pattern);
  }
  return candidates;
}

function scopedInputs(
  producers: readonly Producer[],
  extraReads: ReadonlyMap<string, readonly string[]>,
): CargoTestGeneratedInputs {
  if (producers.length === 0) return { forLeaf: () => ({ dependsOn: [], inputs: [] }) };
  return {
    forLeaf(inputs, projectRoot) {
      const patterns = closurePatterns(inputs, projectRoot);
      const readers = [...extraReads.keys()];
      const extra = new Set<string>();
      for (const [index, matches] of matchGlobPaths(patterns, readers).entries()) {
        const reader = readers[index];
        if (matches && reader !== undefined) for (const file of extraReads.get(reader) ?? []) extra.add(file);
      }
      for (const file of extra) patterns.push(file);
      const candidates: string[] = [];
      const ends: number[] = [];
      for (const { outputs } of producers) {
        for (const output of outputs) candidates.push(...outputCandidates(output, patterns));
        ends.push(candidates.length);
      }
      const matches = matchGlobPaths(patterns, candidates);
      let from = 0;
      const dependsOn: string[] = [];
      for (const [index, { id }] of producers.entries()) {
        const to = ends[index] ?? from;
        if (matches.slice(from, to).some(Boolean)) dependsOn.push(id);
        from = to;
      }
      const fileInputs = [...extra].map((file) => `${workspacePrefix}${file}`);
      return { dependsOn, inputs: [...fileInputs, ...cargoProducerOutputInputs(fileInputs, projectRoot)] };
    },
  };
}

/** Read only the named authority and the project declarations of the concrete producers it names. */
export function readCargoTestGeneratedInputs(
  workspaceRoot: string,
  authority: string | undefined,
  projectRoots: ReadonlyMap<string, string>,
): CargoTestGeneratedInputs {
  if (authority === undefined) return scopedInputs([], new Map());
  const projects = Object.fromEntries(
    [...projectRoots].map(([name, root]): [string, ProjectConfiguration] => [name, { name, root }]),
  );
  const loaded = new Set<string>();
  /** Nx splits qualified target strings (including targets whose names contain colons). */
  function namedTarget(id: string): { node: ProjectGraphProjectNode; name: string; target: TargetConfiguration } {
    const [projectName] = splitTargetFromConfigurations(id, projects, { silent: true });
    const project = projects[projectName];
    if (project === undefined || !id.startsWith(`${projectName}:`)) {
      throw new Error(`cargoTestGeneratedBy ${authority}: ${id} must name a declared project and target`);
    }
    if (!loaded.has(projectName)) {
      const packageFile = join(workspaceRoot, project.root, 'package.json');
      const projectFile = join(workspaceRoot, project.root, 'project.json');
      const pkg = existsSync(packageFile)
        ? readJsonFile<{ nx?: Partial<ProjectConfiguration> }>(packageFile)
        : undefined;
      const explicit = existsSync(projectFile) ? readJsonFile<Partial<ProjectConfiguration>>(projectFile) : undefined;
      const targets = { ...pkg?.nx?.targets };
      for (const [name, target] of Object.entries(explicit?.targets ?? {})) {
        const base = targets[name];
        targets[name] = base === undefined ? target : mergeTargetConfigurations(target, base);
      }
      project.targets = targets;
      loaded.add(projectName);
    }
    const [, name, configuration] = splitTargetFromConfigurations(id, projects, { silent: true });
    const target = name === undefined ? undefined : project.targets?.[name];
    if (name === undefined || target === undefined || configuration !== undefined) {
      throw new Error(`cargoTestGeneratedBy ${authority}: ${id} must name a declared target without a configuration`);
    }
    return { node: { type: 'lib', name: projectName, data: project }, name, target };
  }

  const aggregate = namedTarget(authority).target;
  if (aggregate.executor !== 'nx:noop' || (aggregate.dependsOn?.length ?? 0) === 0) {
    throw new Error(`cargoTestGeneratedBy ${authority}: expected a noop target listing its generated-source producers`);
  }
  const producers: Producer[] = [];
  for (const id of new Set(aggregate.dependsOn)) {
    if (typeof id !== 'string' || id === authority) {
      throw new Error(`cargoTestGeneratedBy ${authority}: expected concrete producer targets`);
    }
    const { node, name, target } = namedTarget(id);
    if (target.executor === 'nx:noop' || !target.outputs?.length) {
      throw new Error(`cargoTestGeneratedBy ${authority}: ${id} must declare its generated outputs`);
    }
    const outputs = getOutputsForTargetAndConfiguration({ project: node.name, target: name }, {}, node);
    if (
      outputs.length !== target.outputs.length ||
      outputs.some((output) => isAbsolute(output) || /[!*?{}\[\]]/.test(output))
    ) {
      throw new Error(
        `cargoTestGeneratedBy ${authority}: ${id} outputs must resolve to workspace-relative files or directories`,
      );
    }
    producers.push({
      id,
      // Nx normalizes away a directory's trailing slash. Its declaration still defines the output kind.
      outputs: outputs.map((output, index) =>
        target.outputs?.[index]?.endsWith('/') ? `${output.replace(/\/$/, '')}/` : output,
      ),
    });
  }
  return scopedInputs(producers, declaredBuildScriptReads(authority, aggregate.metadata?.['cargoTestReads']));
}

/** Out-of-crate reads via a build script rather than a direct Rust include. */
function declaredBuildScriptReads(authority: string, value: unknown): ReadonlyMap<string, readonly string[]> {
  const reads = new Map<string, readonly string[]>();
  if (value === undefined) return reads;
  const refusal = () =>
    new Error(`cargoTestGeneratedBy ${authority}: cargoTestReads must map build.rs paths to workspace-relative files`);
  const workspaceFile = (path: unknown): path is string =>
    typeof path === 'string' &&
    path.length > 0 &&
    !isAbsolute(path) &&
    !path.startsWith('../') &&
    posix.normalize(path) === path &&
    !/[!*?{}\[\]]/.test(path);
  if (typeof value !== 'object' || value === null || Array.isArray(value)) throw refusal();
  for (const [reader, files] of Object.entries(value)) {
    if (!reader.endsWith('/build.rs') || !workspaceFile(reader) || !Array.isArray(files) || !files.every(workspaceFile))
      throw refusal();
    reads.set(reader, files);
  }
  return reads;
}

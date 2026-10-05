import { posix } from 'node:path';
import type { ProjectGraph } from 'nx/src/config/project-graph';
import { getOutputsForTargetAndConfiguration, interpolate } from 'nx/src/tasks-runner/utils';

/** The read-only path records returned by `cowshed build-state --json`. */
export interface BuildStatePath {
  readonly checkout: string;
  readonly volume: string;
}

export interface BuildStateOutputFinding {
  readonly project: string;
  readonly target: string;
  readonly configuration: string | undefined;
  readonly output: string;
  readonly buildStatePath: string;
}

export class BuildStateOutputError extends Error {
  constructor(readonly findings: readonly BuildStateOutputFinding[]) {
    super(
      `Nx outputs must not cover cowshed build-state paths:\n${findings
        .map(
          ({ project, target, configuration, output, buildStatePath }) =>
            `  ${project}:${target}${configuration ? `:${configuration}` : ''}: ${JSON.stringify(output)} covers ${JSON.stringify(buildStatePath)}`,
        )
        .join('\n')}`,
    );
    this.name = 'BuildStateOutputError';
  }
}

function contains(parent: string, child: string): boolean {
  return parent === child || child.startsWith(parent.endsWith('/') ? parent : `${parent}/`);
}

/** The non-glob directory prefix, conservatively including every possible glob match. */
function staticPrefix(output: string): string {
  const parts = output.replaceAll('\\', '/').split('/');
  const firstGlob = parts.findIndex((part) => /[*?[\]{}()!]/.test(part));
  return firstGlob < 0 ? parts.join('/') : parts.slice(0, firstGlob).join('/') || '.';
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
  const root = workspaceRoot.replaceAll('\\', '/');
  if (!posix.isAbsolute(root)) throw new Error('build-state output validation requires an absolute workspace root');
  const paths = buildStatePaths.map(({ checkout }) => ({ checkout, absolute: posix.resolve(root, checkout) }));
  const findings: BuildStateOutputFinding[] = [];
  for (const [project, node] of Object.entries(graph.nodes)) {
    for (const [target, options] of Object.entries(node.data.targets ?? {})) {
      const configurations: (string | undefined)[] = [undefined, ...Object.keys(options.configurations ?? {})];
      for (const configuration of configurations) {
        // Nx's task helper rejects root globs and absolute declarations before expanding
        // them. This lint must name the overlapping build path even for those declarations.
        const outputs = options.outputs
          ? options.outputs
              .map((output) =>
                interpolate(output, {
                  workspaceRoot: root,
                  projectRoot: node.data.root,
                  projectName: node.name,
                  project: { ...node.data, name: node.name },
                  options: { ...options.options, ...(configuration ? options.configurations?.[configuration] : {}) },
                }),
              )
              .filter((output) => output && !/{(projectRoot|workspaceRoot|(options.*))}/.test(output))
          : getOutputsForTargetAndConfiguration({ project, target, configuration }, {}, node);
        for (const output of outputs) {
          const prefix = posix.resolve(root, staticPrefix(output));
          for (const path of paths) {
            if (contains(prefix, path.absolute) || contains(path.absolute, prefix)) {
              findings.push({ project, target, configuration, output, buildStatePath: path.checkout });
            }
          }
        }
      }
    }
  }
  if (findings.length) throw new BuildStateOutputError(findings);
}

import type { ProjectConfiguration, ProjectGraph, TargetConfiguration } from 'nx/src/devkit-exports.js';
import { ProjectGraphBuilder } from 'nx/src/project-graph/project-graph-builder.js';
import { applyImplicitDependencies } from 'nx/src/project-graph/utils/implicit-project-dependencies.js';
import { createTaskGraph } from 'nx/src/tasks-runner/create-task-graph.js';
import { createTaskId } from 'nx/src/tasks-runner/utils.js';
import { declaredOutputs, taskConfigurations } from './build-state-output-policy.js';
import { isBuildOutputTargetName, isTestRunnerTargetName } from './workspace-config-policy.js';

/** The semantic kind inference already knows, retained through Nx target merging. */
export type TaskKind = 'build' | 'test';

export function inferredTaskKind(name: string): TaskKind | undefined {
  if (isTestRunnerTargetName(name)) return 'test';
  return name === 'build' || isBuildOutputTargetName(name) ? 'build' : undefined;
}

function isTestWork(name: string, target: TargetConfiguration): boolean {
  return inferredTaskKind(name) === 'test' || target.metadata?.smooTaskKind === 'test';
}

/** Metadata hooks precede Nx's implicit-edge pass. Reuse that pass without changing the hook's shared graph. */
function withImplicitDependencies(graph: ProjectGraph): ProjectGraph {
  const implicit = Object.entries(graph.nodes).filter(([, node]) => node.data.implicitDependencies?.length);
  if (implicit.length === 0) return graph;
  const dependencies = { ...graph.dependencies };
  const projects: Record<string, ProjectConfiguration> = {};
  for (const [source, node] of implicit) {
    dependencies[source] = [...(dependencies[source] ?? [])];
    projects[source] = node.data;
  }
  const builder = new ProjectGraphBuilder({ ...graph, externalNodes: graph.externalNodes ?? {}, dependencies }, {}, []);
  applyImplicitDependencies(projects, builder);
  return builder.getUpdatedProjectGraph();
}

export interface BuildTestDependency {
  /** The build whose Nx-expanded task closure contains the offending edge. */
  readonly build: string;
  readonly source: string;
  readonly test: string;
}

export class BuildTestDependencyError extends Error {
  constructor(readonly findings: readonly BuildTestDependency[]) {
    super(
      'Build outputs must not depend on test work:\n' +
        findings.map(({ build, source, test }) => `  ${build}: ${source} -> ${test}`).join('\n'),
    );
    this.name = 'BuildTestDependencyError';
  }
}

/**
 * Nx owns dependency normalization, wildcard/caret expansion, configurations, cycles and reachability. Expanding one
 * build root returns only its reachable tasks; inspecting those edges needs no second graph traversal or name list.
 */
export function buildTestDependencies(inputGraph: ProjectGraph, workspaceRoot: string): BuildTestDependency[] {
  const graph = withImplicitDependencies(inputGraph);
  const producers = new Set<TargetConfiguration>();
  for (const output of declaredOutputs(graph, workspaceRoot)) {
    const target = graph.nodes[output.project]?.data.targets?.[output.target];
    if (!target) throw new Error(`Nx output declaration has no owning target: ${output.project}:${output.target}`);
    producers.add(target);
  }
  const findings: BuildTestDependency[] = [];
  for (const [project, node] of Object.entries(graph.nodes)) {
    for (const [name, target] of Object.entries(node.data.targets ?? {})) {
      if (isTestWork(name, target)) continue;
      if (inferredTaskKind(name) !== 'build' && target.metadata?.smooTaskKind !== 'build' && !producers.has(target))
        continue;
      for (const configuration of taskConfigurations(target)) {
        const tasks = createTaskGraph(graph, {}, [project], [name], configuration, {});
        const build = tasks.tasks[createTaskId(project, name, configuration)];
        if (!build) throw new Error(`Nx omitted the requested build task ${project}:${name}`);
        for (const edges of [tasks.dependencies, tasks.continuousDependencies ?? {}]) {
          for (const [source, dependencies] of Object.entries(edges)) {
            const from = tasks.tasks[source];
            if (!from) throw new Error(`Nx task edge has no source: ${source}`);
            const fromTarget = graph.nodes[from.target.project]?.data.targets?.[from.target.target];
            if (!fromTarget || isTestWork(from.target.target, fromTarget)) continue;
            for (const test of dependencies) {
              const to = tasks.tasks[test];
              if (!to) throw new Error(`Nx task edge has no dependency: ${test}`);
              const toTarget = graph.nodes[to.target.project]?.data.targets?.[to.target.target];
              if (toTarget && isTestWork(to.target.target, toTarget)) findings.push({ build: build.id, source, test });
            }
          }
        }
      }
    }
  }
  return findings;
}

export function refuseBuildTestDependencies(graph: ProjectGraph, workspaceRoot: string): void {
  const findings = buildTestDependencies(graph, workspaceRoot);
  if (findings.length > 0) throw new BuildTestDependencyError(findings);
}

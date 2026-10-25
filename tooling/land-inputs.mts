// LAND-INPUTS: the workspace files Nx hashes for a task a red land gate failed and for every task it waits for.
//
//   node tooling/land-inputs.mts TARGET TASK...
//       For each named task of `nx run-many -t TARGET` over every project, the files of its hash plan and of the hash
//       plans of every task it waits for, as one JSON object on stdout: {"<taskId>": ["<path>", ...]}.
//
// tooling/land-attribution.ts names the commits of the land that change these files. Only the failed tasks are asked
// for, so a red costs one project graph and one hash plan per failed task. It runs under node, the runtime `bun nx`
// runs Nx's CLI in (its shebang), never bun: Nx reads an external package through the runtime's own module
// resolution, and bun resolves some to another version node does (measured 2026-10-25 in a sibling workspace's lint:
// `npm:@hughescr/stryker-bun-runner@1.4.0` under bun, the unversioned node under node, so every key hashing that
// package differed from the gate's), so a plan read under bun is not the one the gate ran. It imports only Nx's plain
// JS modules: node ESM cannot name an import of Nx's napi binding.
import { createProjectGraphAsync } from 'nx/src/devkit-exports.js';
import { HashPlanInspector } from 'nx/src/hasher/hash-plan-inspector.js';
import { createTaskGraph } from 'nx/src/tasks-runner/create-task-graph.js';

const [target, ...named] = process.argv.slice(2);
if (target === undefined || target === '' || named.length === 0) {
  console.error('usage: node tooling/land-inputs.mts TARGET TASK...');
  process.exit(64);
}
const graph = await createProjectGraphAsync();
// The projects `nx run-many -t TARGET` runs: those that have the target (createTaskGraph refuses any other).
const projects = Object.values(graph.nodes)
  .filter((node) => node.data.targets?.[target] !== undefined)
  .map((node) => node.name);
const taskGraph = createTaskGraph(graph, {}, projects, [target], undefined, {});
const inspector = new HashPlanInspector(graph);
await inspector.init();
const inputs: Record<string, string[]> = {};
for (const id of named) {
  const task = taskGraph.tasks[id];
  if (task === undefined) {
    console.error(`land-inputs: ${id} is no task of nx run-many -t ${target}`);
    process.exit(1);
  }
  const files = new Set<string>();
  for (const plan of Object.values(inspector.inspectTaskInputs(task.target))) {
    for (const file of [...plan.files, ...plan.depOutputs]) files.add(file);
  }
  inputs[id] = [...files].sort();
}
process.stdout.write(`${JSON.stringify(inputs)}\n`);

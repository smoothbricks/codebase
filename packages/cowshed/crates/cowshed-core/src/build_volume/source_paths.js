// Land (16_build_volumes.md, Land 4 and 5.3): every regular file a task output of the landing checkout's resolved Nx
// graph names, so the land carries generated inputs with the tracked sources. Every target and configuration counts,
// checked or not; @smoothbricks/nx-plugin's output policy (`existingOutputFiles`) reads the declarations and stock Nx
// expands them. No task runner, executor or execution hook runs, and nothing is recorded.
// Usage: node - <absolute Nx root>, with this program on stdin. Prints one JSON array of paths relative to that root,
// or `null` when the root's package graph does not resolve @smoothbricks/nx-plugin at all, which leaves its outputs
// without a policy. A graph or read failure, a plugin that is present but throws or misses a module of its own
// included, is stderr and a nonzero exit, never an empty answer.
const answer = process.stdout.write.bind(process.stdout);
process.stdout.write = process.stderr.write.bind(process.stderr);
const path = require('node:path');
const { createRequire } = require('node:module');
const { pathToFileURL } = require('node:url');
const root = process.argv[2];
if (!root || !path.isAbsolute(root)) {
  throw new Error(`declared output enumeration requires an absolute Nx root, got ${JSON.stringify(root)}`);
}
process.env.NX_WORKSPACE_ROOT_PATH = root;
process.env.NX_WORKSPACE_DATA_DIRECTORY = path.join(root, '.nx', 'workspace-data');
process.env.NX_CACHE_DIRECTORY = path.join(root, '.nx', 'cache');
const requireNx = createRequire(path.join(root, 'package.json'));
const { createProjectGraphAsync } = requireNx('nx/src/project-graph/project-graph');

async function main() {
  // The landing tree is the one its gate checked: the land moved only the target since. So the checkout's own daemon,
  // when one runs, holds this tree's graph, and it is left running: quiesce stops it through the job ledger (Land 4). A
  // job finding none builds the graph in its own process (NX_DAEMON_START=never), writing only the build volume its
  // grant names.
  const graph = await createProjectGraphAsync({ exitOnError: false, resetDaemonClient: false });
  // Absent is the package itself not resolving from the root, nothing else: once its manifest resolves, every failure
  // of the plugin's own is the enumeration's failure.
  try {
    requireNx.resolve('@smoothbricks/nx-plugin/package.json');
  } catch (error) {
    if (error.code === 'MODULE_NOT_FOUND' && String(error.message).includes("'@smoothbricks/nx-plugin/package.json'")) {
      return null;
    }
    throw error;
  }
  const policy = requireNx.resolve('@smoothbricks/nx-plugin/build-state-output-policy');
  const { existingOutputFiles } = await import(pathToFileURL(policy).href);
  return existingOutputFiles(graph, root, requireNx('nx/src/native').expandOutputs);
}
main().then(
  (paths) => answer(`${JSON.stringify(paths)}\n`),
  (error) => {
    console.error(error);
    process.exitCode = 1;
  },
);

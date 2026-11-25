# nx-plugin: a deleted untracked file stays in the indexed-file table, and a lint check reads it

Seen 2026-11-25 in a consumer repository's cowshed workspace (nx 24.0.6, nx-plugin at smoothbricks b0103e84d4). An
untracked test file was created, used for one `bun test` run, and deleted; it was never staged. Afterwards every run of
the consumer's policy lint failed with `ENOENT` on that path, because the project graph still listed it in the owning
project's lint target `metadata.eslintFileSet.files`, and a check there opens every file that field lists.

- `file-map.json` no longer held the file; `project-graph.json` did.
- It survived `nx daemon --stop`, deleting `.nx/workspace-data/project-graph.json`, and a rebuild of the graph: the
  rebuilt graph listed the file again.
- The listing comes from `eslintWorkspace` in `src/index.ts` (~line 523) through `globIndexedFiles`
  (`src/workspace-index.ts:20-24`), whose own doc names the hazard (`workspace-index.ts:26-30`): "A table only learns of
  a write when a watcher event reaches it, so one the watcher missed leaves a deleted file listed and an added one
  unlisted until that process ends." Here it persisted across processes, so the stale row lives in what the index
  persists, not only in one process's memory.

Effect: a graph field derived from the index names a file that does not exist, and any consumer that opens what the
graph lists fails on a tree that is otherwise clean. A land gate's fresh tree does not have it; a developer workspace
does.

Owed: a graph derived from the indexed table never lists a path the filesystem no longer holds, after a missed watcher
event and across daemon restarts. Red: in a fixture workspace, create an untracked `*.test.ts`, build the graph, delete
the file with no watcher running, start a new process, and assert the inferred `eslintFileSet` no longer lists it.

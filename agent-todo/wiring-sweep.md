# Wiring sweep: wire or delete every unreached item, then tag the project

Owner of the root lint's scope (`@smoothbricks/codebase:wiring-lint`, `tooling/wiring.ts --tree`; AGENTS.md rule 7). The
root lint judges every item of each Nx project tagged `wiring:enforced` and nothing else, so it landed enforcing the 15
projects that were already clean. This brief is the work list for the rest; there is no ledger or exemption file in
code. The land's diff check (`@smoothbricks/codebase:wiring`, run by `tooling/land-host.sh` before the gate) already
refuses any new unreached item in every project, tagged or not.

## Per project

1. Add `"wiring:enforced"` to the project's Nx tags (`nx.tags` in its package.json, or `tags` in its project.json).
2. Run `nx run @smoothbricks/codebase:wiring-lint`. Each refusal names the item and its file:line; that output is the
   project's current list.
3. Wire each item into the production path that needs it, or delete it with its tests. A test-only export moves into the
   test or the test goes through the production path. An Nx target no gate runs declares `metadata.ownedBy` or a
   closed-enum `metadata.ungated` kind.
4. Land the tag together with the fixes, and delete the project's row below.

## Counts when the lint landed (338 unreached items in 13 projects)

Measured with every project tagged, over the tree at `ae2863182f` plus the lint itself. The tool's output is the current
list. The `.` row is the root project (`@smoothbricks/codebase`), which owns every file outside a nested project.

| project root                              | unreached | export | pub | script | target |
| ----------------------------------------- | --------- | ------ | --- | ------ | ------ |
| `packages/columine`                       | 121       | 1      | 120 |        |        |
| `packages/cowshed`                        | 80        | 1      | 70  | 1      | 8      |
| `packages/lmao`                           | 72        | 11     | 61  |        |        |
| `packages/bitmosaic`                      | 15        |        | 14  |        | 1      |
| `packages/nx-plugin`                      | 13        | 6      |     | 7      |        |
| `packages/cli`                            | 11        | 11     |     |        |        |
| `packages/statebus-core/scripts/consumer` | 8         | 3      |     | 5      |        |
| `packages/lmao-expo-benchmark`            | 7         | 5      |     |        | 2      |
| `tooling`                                 | 6         | 1      |     | 4      | 1      |
| `targets/lmao-docs`                       | 2         | 1      |     |        | 1      |
| `packages/cargo-retain`                   | 1         |        |     | 1      |        |
| `packages/statebus-core`                  | 1         |        |     |        | 1      |
| `.`                                       | 1         |        |     |        | 1      |
| total                                     | 338       | 40     | 265 | 18     | 15     |

/// <reference types="bun" />
// Planted fixtures for tooling/wiring.ts.
//
// A judge that stopped resolving looks exactly like a fully wired change, so every block plants an added item no
// production code reaches and must see it refused, beside the same item wired the way production wires it, which
// must pass. Same-name decoys prove a caller is resolved, not matched by name.
//
// (a script both wiring targets run before the judge, not a *.test.ts: a judge that stopped resolving must fail the
// same run whose verdict it would otherwise give.)

import {
  type Added,
  addedItems,
  enforcedTree,
  judgeAdded,
  WIRING_TAG,
  type WiringChange,
  type WiringTree,
} from './wiring.ts';

let failures = 0;

function fail(what: string, expected: unknown, actual: unknown): void {
  failures++;
  console.error(`FAIL ${what}\n  expected: ${JSON.stringify(expected)}\n  actual:   ${JSON.stringify(actual)}`);
}

const TEST = /(^|\/)(tests?|__tests__)\/|\.(test|spec|fixtures)\./;

function treeOf(texts: ReadonlyMap<string, string>): WiringTree {
  return {
    files: [...texts.keys()],
    textOf: (path) => texts.get(path),
    filesHolding: (needle) => [...texts].filter(([p, t]) => !TEST.test(p) && t.includes(needle)).map(([p]) => p),
  };
}

/**
 * Judge `added` (every file of `after` the change adds, unless `before` holds it) and expect exactly the findings
 * named `kind name@file:line`.
 */
function expectFindings(
  what: string,
  after: Readonly<Record<string, string>>,
  expected: readonly string[],
  before: Readonly<Record<string, string>> = {},
): void {
  const texts = new Map(Object.entries(after));
  const changes: WiringChange[] = Object.entries(after)
    .filter(([path, text]) => before[path] !== text)
    .map(([path, text]) => ({ path, before: before[path], after: text }));
  const actual = judgeAdded(addedItems(changes), treeOf(texts)).map(
    (f) => `${f.added.kind} ${f.added.name}@${f.added.file}:${f.added.line}`,
  );
  if (JSON.stringify(actual) !== JSON.stringify(expected)) fail(what, expected, actual);
}

function expectAdded(what: string, change: WiringChange, expected: readonly Pick<Added, 'kind' | 'name'>[]): void {
  const actual = addedItems([change]).map((a) => ({ kind: a.kind, name: a.name }));
  if (JSON.stringify(actual) !== JSON.stringify(expected)) fail(what, expected, actual);
}

const PKG = (name: string, extra = ''): string => `{ "name": "${name}", "private": true${extra} }\n`;

// --- TS exports -----------------------------------------------------------------------------------------------------

expectFindings(
  'an export nothing imports, one only a test imports, and one a same-name decoy shadows are refused',
  {
    'packages/w/package.json': PKG('@w/w'),
    'packages/w/src/lone.ts': 'export function lone(): number {\n  return 1;\n}\n',
    'packages/w/src/tested.ts': 'export const tested = 2;\n',
    'packages/w/src/tested.test.ts': "import { tested } from './tested.ts';\nconsole.log(tested);\n",
    'packages/w/src/shadowed.ts': 'export const shared = 3;\n',
    'packages/w/src/decoy.ts': 'export const shared = 4;\n',
    'packages/w/src/main.ts': "import { shared } from './decoy.ts';\nconsole.log(shared);\n",
  },
  [
    'export lone@packages/w/src/lone.ts:1',
    'export tested@packages/w/src/tested.ts:1',
    'export shared@packages/w/src/shadowed.ts:1',
  ],
);

expectFindings(
  'exports wired by relative import, package barrel, aliased re-export, namespace member, local use and a host field pass',
  {
    'packages/w/package.json': PKG(
      '@w/w',
      ', "exports": { ".": "./dist/index.js", "./sub/*": "./dist/sub/*.js" }, "host": { "extensions": ["./src/plugin.ts"] }',
    ),
    'packages/w/src/index.ts': "export * from './barrel-a.js';\nexport { inner as outer } from './aliased.js';\n",
    'packages/w/src/barrel-a.ts': 'export const viaBarrel = 1;\n',
    'packages/w/src/aliased.ts': 'export const inner = 2;\n',
    'packages/w/src/sub/deep.ts': 'export const viaPattern = 3;\n',
    'packages/w/src/rel.ts': 'export const viaRelative = 4;\n',
    'packages/w/src/ns.ts': 'export const viaNamespace = 5;\nexport const viaSpread = 6;\n',
    'packages/w/src/local.ts': 'export const helper = 7;\nexport const consumer = helper + 1;\n',
    'packages/w/src/plugin.ts': 'export default function plugin(): void {}\n',
    'packages/w/src/run.ts':
      "import { viaRelative } from './rel.ts';\nimport * as ns from './ns.ts';\nconsole.log(viaRelative, ns.viaNamespace, { ...ns });\nexport { consumer } from './local.ts';\n",
    'packages/v/package.json': PKG('@w/v'),
    'packages/v/src/use.ts':
      "import { viaBarrel, outer } from '@w/w';\nimport { viaPattern } from '@w/w/sub/deep';\nimport './side.ts';\nconsole.log(viaBarrel, outer, viaPattern);\n",
    'packages/v/src/side.ts': 'console.log(1);\n',
    'packages/v/src/main.ts': "import { consumer } from '../../w/src/run.ts';\nconsole.log(consumer);\n",
  },
  [],
);

expectFindings(
  'a package whose tsconfig emits src/ into dist/ts/ resolves through that emit, for its entries and its importers',
  {
    'packages/p/package.json': '{ "name": "@w/p", "main": "./dist/ts/index.js" }\n',
    'packages/p/tsconfig.lib.json':
      '{\n  // tsconfig files may carry comments\n  "compilerOptions": { "rootDir": "src", "outDir": "dist/ts" }\n}\n',
    'packages/p/src/index.ts': 'export const published = 1;\n',
    'packages/p/src/orphan.ts': 'export const orphan = 2;\n',
    // A declared emit is the whole layout: src/ts/ is not where dist/ts/ comes from, so nothing names this module.
    'packages/p/src/ts/index.ts': 'export const decoy = 4;\n',
    'packages/e/package.json': PKG('@w/e', ', "exports": { "./extra": "./dist/ts/extra.js" }'),
    // The emit is set only by a base outside the package, each directory resolved against the file that names it.
    'packages/e/tsconfig.json': '{ "extends": "../../configs/emit.json" }\n',
    'configs/emit.json':
      '{ "compilerOptions": { "rootDir": "../packages/e/src", "outDir": "../packages/e/dist/ts" } }\n',
    'packages/e/src/extra.ts': 'export const viaEmit = 3;\n',
    'packages/u/package.json': PKG('@w/u'),
    'packages/u/src/main.ts': "import { viaEmit } from '@w/e/extra';\nconsole.log(viaEmit);\n",
  },
  ['export orphan@packages/p/src/orphan.ts:1', 'export decoy@packages/p/src/ts/index.ts:1'],
);

expectFindings(
  "a published package's entry modules are roots; a private package's are not",
  {
    'packages/p/package.json': '{ "name": "@w/p", "exports": { ".": "./src/index.ts" } }\n',
    'packages/p/src/index.ts': 'export const publicApi = 1;\n',
    'packages/q/package.json': '{ "name": "@w/q", "private": true, "exports": { ".": "./src/index.ts" } }\n',
    'packages/q/src/index.ts': 'export const privateApi = 1;\n',
  },
  ['export privateApi@packages/q/src/index.ts:1'],
);

expectAdded(
  'only names the change adds are judged; type exports are not values',
  {
    path: 'packages/w/src/m.ts',
    before: 'export const kept = 1;\n',
    after:
      'export const kept = 1;\nexport const fresh = 2;\nexport type Shape = { a: 1 };\nexport interface Face {}\nexport function make() {}\n',
  },
  [
    { kind: 'export', name: 'fresh' },
    { kind: 'export', name: 'make' },
  ],
);

// --- Rust pub items -------------------------------------------------------------------------------------------------

const CRATE = (name: string, deps = ''): string => `[package]\nname = "${name}"\n\n[dependencies]\n${deps}`;

expectFindings(
  'a pub fn called only from tests/ or #[cfg(test)], or by name from a crate that does not depend on it, is refused',
  {
    'crates/core/Cargo.toml': CRATE('core'),
    'crates/core/src/lib.rs':
      'pub fn only_tests() {}\n\npub fn only_cfg_test() {}\n\npub fn decoyed() {}\n\n#[cfg(test)]\nmod tests {\n    #[test]\n    fn t() {\n        super::only_cfg_test();\n    }\n}\n',
    'crates/core/tests/it.rs': 'fn main() { core::only_tests(); }\n',
    'crates/other/Cargo.toml': CRATE('other'),
    'crates/other/src/lib.rs': 'pub fn run() { decoyed(); }\nfn decoyed() {}\n',
  },
  [
    'pub only_tests@crates/core/src/lib.rs:1',
    'pub only_cfg_test@crates/core/src/lib.rs:3',
    'pub decoyed@crates/core/src/lib.rs:5',
  ],
  { 'crates/other/src/lib.rs': 'pub fn run() { decoyed(); }\nfn decoyed() {}\n' },
);

expectFindings(
  'a pub item a dependent crate, its own crate (a format capture included) or JavaScript (#[napi]) calls passes',
  {
    'crates/core/Cargo.toml': CRATE('core-lib'),
    'crates/core/src/lib.rs':
      'pub fn shared_helper() {}\n\npub const LABEL: &str = "x";\n\npub fn describe() -> String {\n    format!("{LABEL}")\n}\n\n#[napi]\npub fn native_open() {}\n\n#[napi(js_name = "closeIt")]\npub fn native_close() {}\n',
    'crates/app/Cargo.toml': CRATE('app', 'core_lib = { workspace = true }\n'),
    'crates/app/src/main.rs': 'fn main() { core_lib::shared_helper(); core_lib::describe(); }\n',
    'packages/js/package.json': PKG('@w/js'),
    'packages/js/src/bind.ts':
      'declare const n: { nativeOpen(): void; closeIt(): void };\nn.nativeOpen();\nn.closeIt();\n',
  },
  [],
  { 'crates/app/src/main.rs': 'fn main() { core_lib::shared_helper(); core_lib::describe(); }\n' },
);

// --- Wire ops -------------------------------------------------------------------------------------------------------

expectFindings(
  'a wire op only dispatch sites name (a match arm, a case, a handler key) has no sender',
  {
    'crates/srv/Cargo.toml': CRATE('srv'),
    'crates/srv/src/ops.rs': 'pub const TASK_ADMIT_OP: &str = "task-admit";\n',
    'crates/srv/src/control.rs':
      'fn dispatch(op: &str) {\n    match op {\n        TASK_ADMIT_OP => {}\n        _ => {}\n    }\n}\n',
    'packages/w/package.json': PKG('@w/w'),
    'packages/w/src/ops.ts': "export const PING_OP = 'ping';\n",
    'packages/w/src/server.ts':
      "import { PING_OP } from './ops.ts';\nexport function on(op: string): void {\n  switch (op) {\n    case PING_OP:\n      break;\n  }\n}\nconst handlers = { [PING_OP]: () => 1 };\nconsole.log(handlers, on);\n",
  },
  ['op TASK_ADMIT_OP@crates/srv/src/ops.rs:1', 'op PING_OP@packages/w/src/ops.ts:1'],
);

expectFindings(
  'a wire op a client sends, by constant or by its string value, passes',
  {
    'crates/srv/Cargo.toml': CRATE('srv'),
    'crates/srv/src/ops.rs': 'pub const TASK_ADMIT_OP: &str = "task-admit";\n',
    'crates/srv/src/control.rs':
      'fn dispatch(op: &str) {\n    match op {\n        TASK_ADMIT_OP => {}\n        _ => {}\n    }\n}\n',
    'crates/srv/src/client.rs': 'pub fn admit(c: &C) { c.send(TASK_ADMIT_OP, ()); }\n',
    'packages/w/package.json': PKG('@w/w'),
    'packages/w/src/ops.ts': "export const PING_OP = 'ping';\n",
    'packages/w/src/client.ts': "export function ping(send: (op: string) => void): void {\n  send('ping');\n}\n",
    'packages/w/src/main.ts': "import { ping } from './client.ts';\nping(console.log);\n",
  },
  [],
  {
    'crates/srv/src/control.rs':
      'fn dispatch(op: &str) {\n    match op {\n        TASK_ADMIT_OP => {}\n        _ => {}\n    }\n}\n',
    'crates/srv/src/client.rs': 'pub fn admit(c: &C) { c.send(TASK_ADMIT_OP, ()); }\n',
    'packages/w/src/client.ts': "export function ping(send: (op: string) => void): void {\n  send('ping');\n}\n",
  },
);

// --- CLI verbs ------------------------------------------------------------------------------------------------------

const CLI =
  '#[derive(Subcommand)]\nenum Command {\n    Status,\n    #[command(name = "stage-artifact")]\n    Stage { path: String },\n    MirrorRepo(Args),\n}\n';
// The CLI's own source names its verbs (a completion list); that is the registration, not an invocation.
const CLI_MAIN = 'const VERBS: [&str; 3] = ["status", "stage-artifact", "mirror-repo"];\nfn main() {}\n';

expectFindings(
  'a clap verb nothing outside its CLI invokes is refused; one a script runs passes',
  {
    'crates/cli/Cargo.toml': CRATE('cli'),
    'crates/cli/src/args.rs': CLI,
    'crates/cli/src/main.rs': CLI_MAIN,
    'tooling/land.sh': 'cli stage-artifact ./x\n',
  },
  ['verb mirror-repo@crates/cli/src/args.rs:6'],
  {
    'crates/cli/src/args.rs': '#[derive(Subcommand)]\nenum Command {\n    Status,\n}\n',
    'crates/cli/src/main.rs': CLI_MAIN,
    'tooling/land.sh': 'cli stage-artifact ./x\n',
    'crates/cli/Cargo.toml': CRATE('cli'),
  },
);

// --- Scripts --------------------------------------------------------------------------------------------------------

expectFindings(
  'a new script only a test runs is refused; one a target command runs passes',
  {
    'package.json':
      '{ "name": "@w/root", "private": true, "nx": { "targets": { "gate-x": { "metadata": { "ownedBy": "r:lint" }, "options": { "command": "tooling/gates/run-x.sh" } } } } }\n',
    'tooling/gates/run-x.sh': '#!/bin/sh\ntrue\n',
    'tooling/helper-writer.ts': 'console.log(1);\n',
    'tooling/helper.test.ts': "Bun.spawn(['bun', 'helper-writer.ts']);\n",
  },
  ['script helper-writer.ts@tooling/helper-writer.ts:1'],
);

expectFindings(
  "a script a published package's bin exposes through its emit passes; a private package's bin is no caller",
  {
    'packages/tool/package.json': '{ "name": "@w/tool", "bin": { "tool": "./dist/ts/bin/tool.js" } }\n',
    'packages/tool/tsconfig.lib.json': '{ "compilerOptions": { "rootDir": "src", "outDir": "dist/ts" } }\n',
    'packages/tool/src/bin/tool.ts': 'console.log(1);\n',
    'packages/own/package.json': '{ "name": "@w/own", "private": true, "bin": { "own": "./dist/ts/bin/own.js" } }\n',
    'packages/own/tsconfig.lib.json': '{ "compilerOptions": { "rootDir": "src", "outDir": "dist/ts" } }\n',
    'packages/own/src/bin/own.ts': 'console.log(2);\n',
  },
  ['script own.ts@packages/own/src/bin/own.ts:1'],
);
// --- Nx targets -----------------------------------------------------------------------------------------------------

const TARGETS = (targets: string): string =>
  `{\n  "name": "@w/t",\n  "private": true,\n  "nx": {\n    "name": "t",\n    "targets": {\n${targets}\n    }\n  }\n}\n`;

expectFindings(
  'a target nothing runs, a prose ungated reason, and a ci kind whose workflow does not run it are refused',
  {
    'packages/t/package.json': TARGETS(
      [
        '      "orphan": { "command": "true" },',
        '      "prose": { "command": "true", "metadata": { "ungated": "runs by hand" } },',
        '      "ci-wrong": { "command": "true", "metadata": { "ungated": { "kind": "ci", "workflow": ".github/workflows/ci.yml" } } }',
      ].join('\n'),
    ),
    '.github/workflows/ci.yml': 'jobs:\n  a:\n    steps:\n      - run: nx run t:other\n',
  },
  [
    'target orphan@packages/t/package.json:7',
    'target prose@packages/t/package.json:8',
    'target ci-wrong@packages/t/package.json:9',
  ],
);

expectFindings(
  'a target a gate depends on, an owner, an enum kind, a ci workflow that runs it, a watcher, and a typed ref pass',
  {
    'packages/t/package.json': TARGETS(
      [
        '      "lint": { "dependsOn": ["dep-a"] },',
        '      "dep-a": { "command": "true" },',
        '      "owned": { "command": "true", "metadata": { "ownedBy": "t:lint" } },',
        '      "bench-x": { "command": "true", "metadata": { "ungated": { "kind": "bench" } } },',
        '      "ci-run": { "command": "true", "metadata": { "ungated": { "kind": "ci", "workflow": ".github/workflows/ci.yml" } } },',
        '      "watch": { "continuous": true, "command": "true" },',
        '      "typed": { "command": "true" }',
      ].join('\n'),
    ),
    '.github/workflows/ci.yml': 'jobs:\n  a:\n    steps:\n      - run: nx run t:ci-run\n',
    'packages/w/package.json': PKG('@w/w'),
    'packages/w/src/policy.ts': "export const TYPED = { project: 't', target: 'typed' };\nconsole.log(TYPED);\n",
  },
  [],
  { 'packages/t/package.json': TARGETS('      "lint": { "dependsOn": ["dep-a"] }') },
);

// --- Review regressions ----------------------------------------------------------------------------------------------

expectFindings(
  'restricted pub, a file-form #[cfg(test)] module, a wrapped match arm and an unrelated ^target do not pass',
  {
    'crates/core/Cargo.toml': CRATE('core'),
    'crates/core/src/lib.rs':
      '#[cfg(test)]\nmod tests;\n\npub(crate) fn hidden() {}\n\npub fn only_mod_tests() {}\n\npub const PING_OP: &str = "ping";\n\nfn on(op: Option<&str>) {\n    match op {\n        Some(PING_OP) => {}\n        _ => {}\n    }\n}\n\n/// docs\n/// more docs\n#[napi]\npub fn native_open() {}\n',
    'crates/core/src/tests.rs': 'fn t() { super::only_mod_tests(); super::hidden(); }\n',
    'packages/js/package.json': PKG('@w/js'),
    'packages/js/src/bind.ts': 'declare const n: { nativeOpen(): void };\nn.nativeOpen();\n',
    'packages/o/package.json':
      '{ "name": "@w/o", "private": true, "nx": { "name": "o", "targets": { "lint": { "dependsOn": ["^orphan"] } } } }\n',
    'packages/t/package.json': TARGETS('      "orphan": { "command": "true" }'),
  },
  [
    'pub hidden@crates/core/src/lib.rs:4',
    'pub only_mod_tests@crates/core/src/lib.rs:6',
    'op PING_OP@crates/core/src/lib.rs:8',
    'target orphan@packages/t/package.json:7',
  ],
  {
    'packages/o/package.json':
      '{ "name": "@w/o", "private": true, "nx": { "name": "o", "targets": { "lint": { "dependsOn": ["^orphan"] } } } }\n',
  },
);

expectFindings(
  'an export imported only under its alias, and a namespace re-exported by name, pass',
  {
    'packages/w/package.json': PKG('@w/w'),
    'packages/w/src/a.ts': 'export const foo = 1;\nexport { foo as bar };\nexport const deep = 2;\n',
    'packages/w/src/barrel.ts': "import * as ns from './deep.ts';\nexport { ns };\n",
    'packages/w/src/deep.ts': 'export const inner = 3;\n',
    'packages/w/src/main.ts':
      "import { bar } from './a.ts';\nimport { ns } from './barrel.ts';\nconsole.log(bar, ns.inner);\n",
  },
  ['export deep@packages/w/src/a.ts:3'],
);

expectFindings(
  'an item under #[cfg(all(not(test), …))] is production code',
  {
    'crates/core/Cargo.toml': CRATE('core'),
    'crates/core/src/lib.rs': '#[cfg(all(not(test), feature = "x"))]\npub fn prod_only() {}\n',
  },
  ['pub prod_only@crates/core/src/lib.rs:2'],
);

expectFindings(
  'a package whose only emit leaves it builds none of its entries, so its dist/ does not map back to src/',
  {
    'packages/x/package.json': '{ "name": "@w/x", "main": "./dist/index.js" }\n',
    'packages/x/tsconfig.json': '{ "compilerOptions": { "rootDir": "src", "outDir": "../../.cache/proof" } }\n',
    'packages/x/src/index.ts': 'export const unbuilt = 1;\n',
  },
  ['export unbuilt@packages/x/src/index.ts:1'],
);

expectFindings(
  'a module file wins over a same-stem directory index, as Node and TypeScript resolve it',
  {
    'packages/w/package.json': PKG('@w/w'),
    'packages/w/src/util.ts': 'export const fileWins = 1;\n',
    'packages/w/src/util/index.ts': 'export const indexLoses = 2;\n',
    'packages/w/src/main.ts': "import { fileWins, indexLoses } from './util';\nconsole.log(fileWins, indexLoses);\n",
  },
  ['export indexLoses@packages/w/src/util/index.ts:1'],
);

expectFindings(
  'a targetDefaults dependsOn scoped to other projects does not run the same-named target elsewhere',
  {
    'nx.json': '{ "targetDefaults": { "gate": { "dependsOn": [{ "target": "proof", "projects": ["other"] }] } } }\n',
    'packages/t/package.json': TARGETS('      "proof": { "command": "true" }'),
    'packages/other/package.json':
      '{ "name": "@w/other", "private": true, "nx": { "name": "other", "targets": { "proof": { "command": "true" } } } }\n',
  },
  ['target proof@packages/t/package.json:7'],
);

expectAdded(
  'widening a Rust item from pub(crate) to pub adds it',
  { path: 'crates/core/src/lib.rs', before: 'pub(crate) fn widened() {}\n', after: 'pub fn widened() {}\n' },
  [{ kind: 'pub', name: 'widened' }],
);

// --- The root lint's scope ------------------------------------------------------------------------------------------

{
  const texts = new Map(
    Object.entries({
      'package.json': '{ "name": "@w/root", "private": true }\n',
      'packages/on/package.json': `{ "name": "@w/on", "private": true, "nx": { "tags": ["${WIRING_TAG}"] } }\n`,
      'packages/on/src/dead.ts': 'export const deadInEnforced = 1;\n',
      'packages/on/src/inner/project.json': '{ "name": "inner", "tags": [] }\n',
      'packages/on/src/inner/dead.ts': 'export const deadInUntaggedChild = 1;\n',
      'packages/off/package.json': '{ "name": "@w/off", "private": true }\n',
      'packages/off/src/dead.ts': 'export const deadInUntagged = 1;\n',
    }),
  );
  const tree = treeOf(texts);
  const { projects, changes } = enforcedTree(tree);
  const actual = judgeAdded(addedItems(changes), tree).map((f) => `${f.added.name}@${f.added.file}`);
  const expected = ['deadInEnforced@packages/on/src/dead.ts'];
  if (JSON.stringify(projects) !== JSON.stringify(['@w/on']) || JSON.stringify(actual) !== JSON.stringify(expected))
    fail(
      'the root lint judges every item of a tagged project and nothing of an untagged one',
      { expected },
      {
        projects,
        actual,
      },
    );
}

if (failures > 0) {
  console.error(
    `\n${failures} wiring fixture(s) failed: the judge no longer refuses what it must, or refuses wired code.`,
  );
  process.exit(1);
}
console.log('Wiring fixtures passed: every planted unwired item is refused and every wired control passes.');

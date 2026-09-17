import assert from 'node:assert/strict';
import { readFileSync, writeFileSync, existsSync, readdirSync } from 'node:fs';
import { createRequire } from 'node:module';
import { join } from 'node:path';
import { execFileSync } from 'node:child_process';

const root = process.cwd();
const require = createRequire(join(root, 'package.json'));
const ts = require('typescript');
const expected = '115142bc063ee989df55844d70117d3176bfc3fe';
assert.equal(execFileSync('git', ['rev-parse', 'HEAD'], { encoding: 'utf8' }).trim(), expected);
const pkg = 'packages/patchnote/';
const changed = new Set();
function read(path) { return readFileSync(path, 'utf8'); }
function write(path, text) { writeFileSync(path, text); changed.add(path); }
function replace(path, old, next, count = 1) {
  const source = read(path);
  assert.equal(source.split(old).length - 1, count, `${path}: unexpected replacement context ${old}`);
  write(path, source.replaceAll(old, next));
}
function addTypia(path) {
  const source = read(path);
  if (!source.includes("import typia from 'typia';")) write(path, "import typia from 'typia';\n" + source);
}
function walk(dir) {
  return readdirSync(dir, { withFileTypes: true }).flatMap(entry => {
    const path = join(dir, entry.name);
    return entry.isDirectory() ? walk(path) : [path];
  });
}

// Config lists are replaced as complete values by mergeConfig, not merged by index.
// Preserving their element contracts makes the resolved config provable without a cast.
replace(pkg+'src/types.ts', 'export type DeepPartial<T> = T extends object', 'export type DeepPartial<T> = T extends readonly unknown[] ? T : T extends object');
// A process terminated by a signal has no numeric exit code in Execa's public contract.
replace(pkg+'src/types.ts', '  /** Exit code */\n  exitCode: number;', '  /** Exit code; undefined when no exit code was produced (for example, a signal). */\n  exitCode: number | undefined;');
replace(pkg+'src/types.ts', "export type SupportedProvider = 'zai' | 'gemini';", "export const SUPPORTED_PROVIDERS = ['zai', 'gemini'] as const;\nexport type SupportedProvider = (typeof SUPPORTED_PROVIDERS)[number];");
replace(pkg+'src/ai/ai-client.ts', "import type { SendPromptOptions, SupportedProvider } from '../types.js';", "import { SUPPORTED_PROVIDERS, type SendPromptOptions, type SupportedProvider } from '../types.js';");
replace(pkg+'src/ai/ai-client.ts', '  for (const [name, cfg] of Object.entries(PROVIDER_CONFIGS)) {', '  for (const name of SUPPORTED_PROVIDERS) {\n    const cfg = PROVIDER_CONFIGS[name];');
replace(pkg+'src/ai/ai-client.ts', 'provider: name as SupportedProvider', 'provider: name');
replace(pkg+'src/updaters/bun.ts', "currentSection = sectionMatch[1] as 'dependencies' | 'devDependencies';", "currentSection = sectionMatch[1] === 'devDependencies' ? 'devDependencies' : 'dependencies';");

// Validate gh's JSON at its boundary; keep the existing operation-specific failures.
const github = pkg+'src/auth/github-client.ts';
replace(github, `      const parsed = JSON.parse(stdout);

      // Validate that gh CLI returned an array
      if (!Array.isArray(parsed)) {
        throw new Error(\`Expected array from gh pr list, got \${typeof parsed}\`);
      }

      // Return all PRs - let the caller filter by branch prefix
      return parsed as GitHubPR[];`, `      const parsed = typia.json.validateParse<GitHubPR[]>(stdout);
      if (!parsed.success) throw new Error('Expected array of valid PRs from gh pr list');
      // Return all PRs - let the caller filter by branch prefix.
      return parsed.data;`);
replace(github, `      const parsed = JSON.parse(stdout);

      // Validate that gh CLI returned an object with mergeable field
      if (typeof parsed !== 'object' || parsed === null || !('mergeable' in parsed)) {
        throw new Error('Expected object with mergeable field from gh pr view');
      }

      const data = parsed as { mergeable: 'MERGEABLE' | 'CONFLICTING' | 'UNKNOWN' };
      return data.mergeable === 'CONFLICTING';`, `      const parsed = typia.json.validateParse<{ mergeable: 'MERGEABLE' | 'CONFLICTING' | 'UNKNOWN' }>(stdout);
      if (!parsed.success) throw new Error('Expected object with mergeable field from gh pr view');
      return parsed.data.mergeable === 'CONFLICTING';`);
replace(github, `      const parsed = JSON.parse(stdout);
      if (!Array.isArray(parsed)) {
        throw new Error(\`Expected array from gh pr list, got \${typeof parsed}\`);
      }
      return parsed.length > 0 ? (parsed[0] as GitHubPR) : null;`, `      const parsed = typia.json.validateParse<GitHubPR[]>(stdout);
      if (!parsed.success) throw new Error('Expected array of valid PRs from gh pr list');
      return parsed.data[0] ?? null;`);
addTypia(github);
for (const path of [github, pkg+'src/commands/validate-setup.ts']) {
  replace(path, "const stderr = (error as { stderr?: string }).stderr || '';", "const stderr = typia.is<Pick<Awaited<ReturnType<CommandExecutor>>, 'stderr'>>(error) ? error.stderr : '';");
  addTypia(path);
}

// Validate JSON and module inputs once; preserve the established config fallback.
const config = pkg+'src/config.ts';
replace(config, '    let userConfig: DeepPartial<PatchnoteConfig>;', '    let userConfig: PatchnoteConfigInput;');
replace(config, '      userConfig = module.default || module;', '      userConfig = typia.assert<PatchnoteConfigInput>(module.default ?? module);');
replace(config, '      userConfig = JSON.parse(fileContent) as DeepPartial<PatchnoteConfig>;', '      userConfig = typia.json.assertParse<PatchnoteConfigInput>(fileContent);');
replace(config, `    // Validate basic structure
    if (typeof userConfig !== 'object' || userConfig === null || Array.isArray(userConfig)) {
      console.warn(
        \`Invalid config file at \${configPath}: expected object, got \${Array.isArray(userConfig) ? 'array' : typeof userConfig}\`,
      );
      return { ...defaultConfig };
    }

`, '');
replace(config, 'const { extends: presetRefs, ...localConfig } = userConfig as PatchnoteConfigInput;', 'const { extends: presetRefs, ...localConfig } = userConfig;');
replace(config, '  } as PatchnoteConfig;', '  };');
{
  const source = read(config);
  const tree = ts.createSourceFile(config, source, ts.ScriptTarget.Latest, true);
  let declaration;
  function visit(node) {
    if (ts.isVariableDeclaration(node) && node.name.getText(tree) === 'defaultConfig') declaration = node;
    ts.forEachChild(node, visit);
  }
  visit(tree);
  assert.ok(declaration?.type && declaration.initializer);
  const start = declaration.name.end;
  const end = declaration.type.end;
  write(config, source.slice(0, start)+source.slice(end, declaration.initializer.end)+' satisfies PatchnoteConfig'+source.slice(declaration.initializer.end));
}
addTypia(config);

// Rewrite only the actual assertion syntax at the diagnosed boundaries.
// Internal literals/const assertions are not data validation and remain untouched.
const boundaries = new Set([
  'src/ai/ai-client.ts', 'src/changelog/fetcher.ts', 'src/deprecated/checker.ts',
  'src/deprecated/replacements.ts', 'src/expo/sdk-checker.ts',
  'src/expo/versions-fetcher.ts', 'src/provenance/checker.ts',
]);
let executorAssertions = 0;
let boundaryAssertions = 0;
for (const path of walk(pkg+'src').filter(path => path.endsWith('.ts'))) {
  const source = read(path);
  const tree = ts.createSourceFile(path, source, ts.ScriptTarget.Latest, true);
  const edits = [];
  let validates = false;
  function visit(node) {
    if (ts.isAsExpression(node)) {
      const type = node.type.getText(tree);
      if (type === 'CommandExecutor') {
        let expression = node.expression;
        while (ts.isAsExpression(expression) || ts.isParenthesizedExpression(expression)) expression = expression.expression;
        assert.equal(expression.getText(tree), 'execa', `${path}: unsupported executor assertion`);
        edits.push([node.getStart(tree), node.end, 'execa']);
        executorAssertions++;
        return;
      }
      if (boundaries.has(path.slice(pkg.length)) && type !== 'const') {
        const expression = node.expression;
        let replacement;
        if (ts.isCallExpression(expression) && expression.expression.getText(tree) === 'JSON.parse') {
          assert.equal(expression.arguments.length, 1);
          replacement = `typia.json.assertParse<${type}>(${expression.arguments[0].getText(tree)})`;
        } else {
          replacement = `typia.assert<${type}>(${expression.getText(tree)})`;
        }
        edits.push([node.getStart(tree), node.end, replacement]);
        validates = true;
        boundaryAssertions++;
        return;
      }
    }
    ts.forEachChild(node, visit);
  }
  visit(tree);
  if (edits.length) {
    let next = source;
    for (const [start,end,text] of edits.sort((a,b) => b[0]-a[0])) next = next.slice(0,start)+text+next.slice(end);
    write(path, next);
    if (validates) addTypia(path);
  }
}
assert.ok(executorAssertions >= 12 && boundaryAssertions >= 8, 'Expected the diagnosed assertion sites');

// Resolve the accompanying actionable Biome diagnostics, without unsafe autofix.
replace(pkg+'src/cli.ts', 'Number.parseInt(options.maxAge)', 'Number.parseInt(options.maxAge, 10)');
replace(pkg+'src/commands/rebase-open-prs.ts', '    for (let i = 0; i < levels.length; i++) {\n      for (const pr of levels[i]!) {', '    for (const [i, level] of levels.entries()) {\n      for (const pr of level) {');
replace(pkg+'test/commands/rebase-open-prs.test.ts', 'CommandExecutor, GitHubPR, IGitHubClient, RebaseResult', 'CommandExecutor, GitHubPR, IGitHubClient');
replace(pkg+'test/git/modification-functions.test.ts', 'const mock = async (cmd: string | URL, args?: readonly string[], opts?: Record<string, unknown>) => {', 'const mock = async (cmd: string | URL, args?: readonly string[]) => {');

// The existing repository transformer serves both execution and production builds.
const manifest = JSON.parse(read(pkg+'package.json'));
manifest.dependencies.typia = require('typia/package.json').version;
manifest.devDependencies['@typia/unplugin'] = require('@typia/unplugin/package.json').version;
manifest.devDependencies['@smoothbricks/validation'] = 'workspace:*';
manifest.nx.targets.build = { ...manifest.nx.targets.build, dependsOn: ['^build'] };
write(pkg+'package.json', JSON.stringify(manifest,null,2)+'\n');
assert.ok(!existsSync(pkg+'bunfig.toml'), 'Do not overwrite an existing runtime preload');
write(pkg+'bunfig.toml', 'preload = ["@smoothbricks/validation/bun/preload"]\n');
replace(pkg+'build.ts', "import { $, build } from 'bun';", "import UnpluginTypia from '@typia/unplugin/bun';\nimport { $, build } from 'bun';");
replace(pkg+'build.ts', "  sourcemap: 'none',", "  sourcemap: 'none',\n  plugins: [UnpluginTypia({ log: false })],", 2);
const compiler = JSON.parse(read(pkg+'tsconfig.lib.json'));
compiler.compilerOptions ??= {};
assert.ok(!compiler.compilerOptions.plugins, 'Existing compiler plugins need review');
compiler.compilerOptions.plugins = [{ transform: 'typia/lib/transform' }];
write(pkg+'tsconfig.lib.json', JSON.stringify(compiler,null,2)+'\n');

// Boundary regressions execute the same public functions as the application.
const testPath = pkg+'test/lint-boundaries.test.ts';
assert.ok(!existsSync(testPath));
write(testPath, `import { afterEach, describe, expect, test } from 'bun:test';
import { mkdtemp, mkdir, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { execa } from 'execa';
import { GitHubCLIClient } from '../src/auth/github-client.js';
import { defaultConfig, loadConfig, mergeConfig } from '../src/config.js';
import type { CommandExecutor } from '../src/types.js';
import { createMockExeca } from './helpers/mock-execa.js';

const roots: string[] = [];
afterEach(async () => {
  await Promise.all(roots.splice(0).map(root => rm(root, { recursive: true, force: true })));
});
describe('validated command and configuration boundaries', () => {
  test('the real text executor satisfies the injectable contract without assertions', async () => {
    const execute: CommandExecutor = execa;
    const result = await execute(process.execPath, ['-e', 'process.stdout.write("text"); process.stderr.write("diagnostic")']);
    expect(result.stdout).toBe('text');
    expect(result.stderr).toBe('diagnostic');
    expect(result.exitCode).toBe(0);
  });
  test('rejects malformed members, not only a non-array PR response', async () => {
    for (const value of [[null], [{}], [{ number: '123', title: 'bad', headRefName: 'branch', createdAt: '2026-01-01', url: 'url' }]]) {
      const client = new GitHubCLIClient(createMockExeca({
        'gh pr list --json number,title,headRefName,baseRefName,createdAt,url --state open': JSON.stringify(value),
      }));
      await expect(client.listUpdatePRs('/repo')).rejects.toThrow('Expected array');
    }
  });
  test('refuses undeclared mergeability values', async () => {
    const client = new GitHubCLIClient(createMockExeca({
      'gh pr view 123 --json mergeable': JSON.stringify({ mergeable: 'invented' }),
    }));
    await expect(client.checkPRConflicts('/repo',123)).rejects.toThrow('mergeable');
  });
  test('enhancing an operational rejection cannot crash on null or a primitive', async () => {
    for (const failure of [null, undefined, 'offline']) {
      const executor: CommandExecutor = async () => { throw failure; };
      await expect(new GitHubCLIClient(executor).listUpdatePRs('/repo')).rejects.toThrow('Failed to list PRs');
    }
  });
  test('invalid nested config falls back instead of leaking an invalid runtime type', async () => {
    const root = await mkdtemp(join(tmpdir(), 'patchnote-config-boundary-'));
    roots.push(root);
    await mkdir(join(root,'tooling'));
    await writeFile(join(root,'tooling/patchnote.json'), JSON.stringify({ prStrategy: { maxStackDepth: 'many' } }));
    expect((await loadConfig(root,'tooling/patchnote.json')).prStrategy.maxStackDepth).toBe(defaultConfig.prStrategy.maxStackDepth);
  });
  test('configuration merges replace complete list elements and retain required defaults', () => {
    const config = mergeConfig({ expo: { projects: [{ packageJsonPath: 'apps/mobile/package.json' }] }, packageRules: [{ match: 'react' }] });
    expect(config.expo?.projects).toEqual([{ packageJsonPath: 'apps/mobile/package.json' }]);
    expect(config.expo?.enabled).toBe(defaultConfig.expo.enabled);
    expect(config.packageRules).toEqual([{ match: 'react' }]);
  });
});
`);
console.log(JSON.stringify({ expected, executorAssertions, boundaryAssertions, paths: [...changed] }, null, 2));
writeFileSync(join(process.env.RUNNER_TEMP,'patchnote-changed.json'),JSON.stringify([...changed]));

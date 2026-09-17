import assert from 'node:assert/strict';
import { readFileSync, writeFileSync, existsSync } from 'node:fs';
import { createRequire } from 'node:module';
import { dirname, join, relative } from 'node:path';
import { pathToFileURL } from 'node:url';

let repair = readFileSync(process.env.REPAIR_SOURCE, 'utf8');
const lines = repair.split('\n');
const providerLines = lines.filter(line => line.startsWith("replace(pkg+'src/types.ts', \"export type SupportedProvider"));
assert.equal(providerLines.length, 1);
repair = lines.filter(line => !providerLines.includes(line)).join('\n');
repair = repair.replace("require('@typia/unplugin/package.json').version", "JSON.parse(read('package.json')).devDependencies['@typia/unplugin']");
const executorGuard = "assert.equal(expression.getText(tree), 'execa', `${path}: unsupported executor assertion`);";
assert.equal(repair.split(executorGuard).length, 2);
repair = repair.replace(executorGuard, "assert.ok(['execa', 'execaOriginal'].includes(expression.getText(tree)), `${path}: unsupported executor assertion`);");
repair = repair.replace("edits.push([node.getStart(tree), node.end, 'execa']);", "edits.push([node.getStart(tree), node.end, 'executeCommand']);");
repair = repair.replace('for (const [i, level] of levels.entries())', 'for (const level of levels)');
const temporary = join(process.env.RUNNER_TEMP,'patchnote-repair.mjs');
writeFileSync(temporary,repair);
await import(pathToFileURL(temporary).href);

const path = 'packages/patchnote/src/config.ts';
let source = readFileSync(path,'utf8');
assert.equal(source.split('export const defaultConfig =').length, 2);
source = source.replace('export const defaultConfig =', 'const baseConfig =');
source = source.replace('} satisfies PatchnoteConfig;', '} satisfies PatchnoteConfig;\nexport const defaultConfig: PatchnoteConfig = baseConfig;');
for (const name of ['expo','syncpack','nix','provenanceCheck','deprecationCheck','git','semanticCommits','filters','lockFileMaintenance']) {
  source = source.replaceAll(`...defaultConfig.${name}`, `...baseConfig.${name}`);
}
writeFileSync(path,source);
writeFileSync('packages/patchnote/bunfig.toml', 'preload = ["@smoothbricks/validation/bun/preload"]\n\n[test]\npreload = ["@smoothbricks/validation/bun/preload"]\n');

const adapter = 'packages/patchnote/src/executor.ts';
assert.ok(!existsSync(adapter));
writeFileSync(adapter, `import { execa } from 'execa';
import typia from 'typia';
import type { CommandExecutor, ExecutorResult } from './types.js';

/**
 * Adapt Execa's overloaded API to the text-only command port. Missing captured
 * streams (inherit/ignore/buffer:false) are empty text; binary/object output is
 * refused rather than asserted to be a string. Options and rejections pass through.
 */
export const executeCommand: CommandExecutor = async (file, args, options) => {
  const result = typia.assert<Partial<ExecutorResult>>(await execa(file, args, options));
  return { stdout: result.stdout ?? '', stderr: result.stderr ?? '', exitCode: result.exitCode };
};
`);
const listed = join(process.env.RUNNER_TEMP,'patchnote-changed.json');
const paths = JSON.parse(readFileSync(listed,'utf8'));
const ts = createRequire(join(process.cwd(),'package.json'))('typescript');
for (const file of paths.filter(file => file.startsWith('packages/patchnote/src/') && file.endsWith('.ts'))) {
  let text = readFileSync(file,'utf8');
  if (!text.includes('executeCommand')) continue;
  const ast = ts.createSourceFile(file,text,ts.ScriptTarget.Latest,true);
  const edits = [];
  for (const statement of ast.statements) {
    if (!ts.isImportDeclaration(statement) || statement.moduleSpecifier.text !== 'execa') continue;
    const bindings = statement.importClause?.namedBindings;
    if (!bindings || !ts.isNamedImports(bindings)) continue;
    const keep = bindings.elements.filter(element => {
      if ((element.propertyName ?? element.name).text !== 'execa') return true;
      let used = false;
      function visit(node) {
        if (node === statement) return;
        if (ts.isIdentifier(node) && node.text === element.name.text) used = true;
        ts.forEachChild(node,visit);
      }
      visit(ast);
      return used;
    });
    if (keep.length === bindings.elements.length) continue;
    const replacement = keep.length ? `import { ${keep.map(element=>element.getText(ast)).join(', ')} } from 'execa';` : '';
    edits.push([statement.getStart(ast),statement.end,replacement]);
  }
  for (const [start,end,replacement] of edits.sort((a,b)=>b[0]-a[0])) text = text.slice(0,start)+replacement+text.slice(end);
  if (file.endsWith('/commands/onboard.ts')) {
    const obsolete = "    const { execa } = await import('execa');\n";
    assert.equal(text.split(obsolete).length, 2);
    text = text.replace(obsolete, '');
  }
  const module = relative(dirname(file),adapter).replace(/\.ts$/,'.js');
  text = `import { executeCommand } from '${module.startsWith('.') ? module : './'+module}';\n` + text;
  text = text.replace('Default executor - execa cast to CommandExecutor type\n * The cast is safe because execa\'s Result extends our ExecutorResult interface', 'Default text executor; callers can inject the same port in tests.');
  writeFileSync(file,text);
}
paths.push(adapter);
writeFileSync(listed,JSON.stringify(paths));

// Construct internal collections with annotations, not runtime validators.
const changelog = 'packages/patchnote/src/changelog/fetcher.ts';
let text = readFileSync(changelog,'utf8');
assert.equal(text.split('typia.assert<PackageUpdate[]>([])').length, 5);
text = text.replace('const sections = {', "const sections: Record<PackageUpdate['updateType'], PackageUpdate[]> = {")
  .replaceAll('typia.assert<PackageUpdate[]>([])','[]');
// A repository descriptor is inspected only after selecting the requested version.
// An unrelated version's descriptor must not invalidate an otherwise usable response.
text = text.replace('typia.assert<{ versions?: Record<string, { repository?: { url?: string } }> }>(rawData)',
  'typia.assert<{ versions?: Record<string, { repository?: unknown } | null>; repository?: unknown }>(rawData)')
  .replace('typia.assert<{ repository?: { url?: string } }>(data).repository','data.repository')
  .replace('if (repository?.url)', 'if (typia.is<{ url: string }>(repository) && repository.url)')
  .replaceAll('body?: string','body?: string | null');
writeFileSync(changelog,text);
for (const file of paths.filter(file => file.endsWith('.ts') && file !== adapter)) {
  const text = readFileSync(file,'utf8');
  writeFileSync(file,text.replace(/^((?:import [^\n]+;\n)+)(\/\*\*[\s\S]*?\*\/)\n+/, '$2\n\n$1'));
}
const test = 'packages/patchnote/test/lint-boundaries.test.ts';
writeFileSync(test,readFileSync(test,'utf8')
  .replace('defaultConfig.expo.enabled','defaultConfig.expo?.enabled')
  .replace("import { execa } from 'execa';", "import { executeCommand } from '../src/executor.js';")
  .replace('const execute: CommandExecutor = execa;', 'const execute: CommandExecutor = executeCommand;') + `

describe('text executor output modes', () => {
  test('represents uncaptured streams as empty text without changing execution', async () => {
    const result = await executeCommand(process.execPath, ['-e', 'process.stdout.write("ignored")'], { stdio: 'ignore' });
    expect(result).toEqual({ stdout: '', stderr: '', exitCode: 0 });
  });
  test('retains a nonzero exit code when the caller requests non-throwing execution', async () => {
    const result = await executeCommand(process.execPath, ['-e', 'process.exit(7)'], { reject: false });
    expect(result.exitCode).toBe(7);
  });
  test('refuses a binary result instead of passing bytes off as text', async () => {
    await expect(executeCommand(process.execPath, ['-e', 'process.stdout.write("bytes")'], { encoding: 'buffer' })).rejects.toThrow();
  });
});
`);

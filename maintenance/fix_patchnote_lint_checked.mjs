import assert from 'node:assert/strict';
import { readFileSync, writeFileSync, existsSync } from 'node:fs';
import { createRequire } from 'node:module';
import { dirname, join, relative } from 'node:path';
import { pathToFileURL } from 'node:url';

// The original source already declares the provider tuple; use it as the authority.
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

// Keep the public optional config shape. Only the internal default values need
// their known required members when merging a supplied partial section.
const path = 'packages/patchnote/src/config.ts';
let source = readFileSync(path,'utf8');
assert.equal(source.split('export const defaultConfig =').length, 2);
source = source.replace('export const defaultConfig =', 'const baseConfig =');
source = source.replace('} satisfies PatchnoteConfig;', '} satisfies PatchnoteConfig;\nexport const defaultConfig: PatchnoteConfig = baseConfig;');
for (const name of ['expo','syncpack','nix','provenanceCheck','deprecationCheck','git','semanticCommits','filters','lockFileMaintenance']) {
  source = source.replaceAll(`...defaultConfig.${name}`, `...baseConfig.${name}`);
}
writeFileSync(path,source);

// Selecting the command-call overload once avoids casting Execa's overloaded
// binding/template interface at every dependency-injection boundary.
const adapter = 'packages/patchnote/src/executor.ts';
assert.ok(!existsSync(adapter));
writeFileSync(adapter, `import { execa } from 'execa';
import type { CommandExecutor } from './types.js';

/** Select Execa's command-call overload; preserve its result, options and rejection. */
export const executeCommand: CommandExecutor = (file, args, options) => execa(file, args, options);
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
  const module = relative(dirname(file),adapter).replace(/\.ts$/,'.js');
  text = `import { executeCommand } from '${module.startsWith('.') ? module : './'+module}';\n` + text;
  writeFileSync(file,text);
}
paths.push(adapter);
writeFileSync(listed,JSON.stringify(paths));
const test = 'packages/patchnote/test/lint-boundaries.test.ts';
writeFileSync(test,readFileSync(test,'utf8')
  .replace('defaultConfig.expo.enabled','defaultConfig.expo?.enabled')
  .replace("import { execa } from 'execa';", "import { executeCommand } from '../src/executor.js';")
  .replace('const execute: CommandExecutor = execa;', 'const execute: CommandExecutor = executeCommand;'));

import { posix as path } from 'node:path';
import {
  getProjects,
  joinPathFragments,
  normalizePath,
  type ProjectConfiguration,
  readJson,
  type Tree,
  updateJson,
} from 'nx/src/devkit-exports.js';

interface BunTestTracingGeneratorSchema {
  project: string;
  opContextModule: string;
  opContextExport?: string;
  tracerModule?: string;
}

type JsonObject = Record<string, unknown>;

interface PackageJson extends JsonObject {
  name?: string;
  scripts?: Record<string, string>;
  nx?: {
    targets?: Record<string, JsonObject>;
  };
  dependencies?: Record<string, string>;
  devDependencies?: Record<string, string>;
}

interface TsConfigJson extends JsonObject {
  extends?: string;
  files?: string[];
  include?: string[];
  references?: Array<{ path: string }>;
  compilerOptions?: Record<string, unknown>;
}

const DEFAULT_TEST_INCLUDES = [
  'src/**/*.test.ts',
  'src/**/*.spec.ts',
  'src/**/__tests__/**/*.ts',
  'src/**/__tests__/**/*.tsx',
  'src/test-suite-tracer.ts',
];

const COPIED_COMPILER_OPTIONS = ['baseUrl', 'module', 'moduleResolution', 'jsx', 'lib'];

export default async function generator(tree: Tree, schema: BunTestTracingGeneratorSchema): Promise<void> {
  const options = normalizeOptions(schema);
  const resolved = resolveProject(tree, options.project);
  const packageJsonPath = joinPathFragments(resolved.root, 'package.json');
  const tsconfigPath = joinPathFragments(resolved.root, 'tsconfig.json');
  const libTsconfigPath = joinPathFragments(resolved.root, 'tsconfig.lib.json');

  if (!tree.exists(packageJsonPath)) {
    throw new Error(`Project ${resolved.name} is missing ${packageJsonPath}`);
  }

  if (!tree.exists(tsconfigPath)) {
    throw new Error(`Project ${resolved.name} is missing ${tsconfigPath}`);
  }

  if (!tree.exists(libTsconfigPath)) {
    throw new Error(`Project ${resolved.name} is missing ${libTsconfigPath}`);
  }

  updatePackageJson(tree, packageJsonPath);
  writeBunfig(tree, resolved.root);
  tree.write(joinPathFragments(resolved.root, 'src/test-suite-tracer.ts'), renderSuiteTracer(options));
  updateTsconfigTest(tree, resolved.root, packageJsonPath, tsconfigPath, libTsconfigPath);
}

function normalizeOptions(schema: BunTestTracingGeneratorSchema): Required<BunTestTracingGeneratorSchema> {
  return {
    project: schema.project,
    opContextModule: schema.opContextModule,
    opContextExport: schema.opContextExport ?? 'opContext',
    tracerModule: schema.tracerModule ?? '@smoothbricks/lmao/testing/bun',
  };
}

function resolveProject(tree: Tree, projectInput: string): { name: string; root: string } {
  const normalizedInput = normalizeLookupValue(projectInput);

  for (const [name, config] of getProjects(tree)) {
    if (normalizeLookupValue(name) === normalizedInput || normalizeLookupValue(config.root) === normalizedInput) {
      return { name, root: config.root };
    }

    const packageJsonPath = joinPathFragments(config.root, 'package.json');
    if (!tree.exists(packageJsonPath)) {
      continue;
    }

    const packageJson = readJson<PackageJson>(tree, packageJsonPath);
    if (packageJson.name && normalizeLookupValue(packageJson.name) === normalizedInput) {
      return { name, root: config.root };
    }
  }

  throw new Error(`Could not resolve project ${projectInput}`);
}

function normalizeLookupValue(value: string): string {
  return value.replace(/^\.\//, '').replace(/\\/g, '/');
}

function updatePackageJson(tree: Tree, packageJsonPath: string): void {
  updateJson<PackageJson>(tree, packageJsonPath, (packageJson: PackageJson) => {
    packageJson.scripts ??= {};
    packageJson.scripts.test ??= 'bun test';

    packageJson.nx ??= {};
    packageJson.nx.targets ??= {};
    packageJson.nx.targets.lint ??= {};

    const hasLmaoDependency = Boolean(
      packageJson.dependencies?.['@smoothbricks/lmao'] || packageJson.devDependencies?.['@smoothbricks/lmao'],
    );

    if (!hasLmaoDependency) {
      packageJson.devDependencies ??= {};
      packageJson.devDependencies['@smoothbricks/lmao'] = 'workspace:*';
    }

    return packageJson;
  });
}

function writeBunfig(tree: Tree, projectRoot: string): void {
  const bunfigPath = joinPathFragments(projectRoot, 'bunfig.toml');
  const preloadEntries = '"@smoothbricks/lmao/bun/preload", "@smoothbricks/lmao/bun/trace-preload"';

  if (!tree.exists(bunfigPath)) {
    tree.write(bunfigPath, `[test]\npreload = [${preloadEntries}]\n`);
    return;
  }

  const current = tree.read(bunfigPath, 'utf-8') ?? '';

  // Already has the package specifier preloads
  if (current.includes('@smoothbricks/lmao/bun/preload')) {
    return;
  }

  // Replace old patterns with the package specifier preloads
  if (current.includes('preload = [')) {
    tree.write(bunfigPath, current.replace(/preload\s*=\s*\[.*?\]/s, `preload = [${preloadEntries}]`));
    return;
  }

  if (current.includes('[test]')) {
    tree.write(bunfigPath, current.replace('[test]', `[test]\npreload = [${preloadEntries}]`));
    return;
  }

  const suffix = current.endsWith('\n') ? '' : '\n';
  tree.write(bunfigPath, `${current}${suffix}[test]\npreload = [${preloadEntries}]\n`);
}

function renderSuiteTracer(options: Required<BunTestTracingGeneratorSchema>): string {
  return [
    `import { ${options.opContextExport} } from '${options.opContextModule}';`,
    `import { defineTestTracer } from '${options.tracerModule}';`,
    '',
    `export const { useTestSpan, opContext, extraTestColumns } = defineTestTracer(${options.opContextExport});`,
    '',
  ].join('\n');
}

function updateTsconfigTest(
  tree: Tree,
  projectRoot: string,
  packageJsonPath: string,
  tsconfigPath: string,
  libTsconfigPath: string,
): void {
  const tsconfigTestPath = joinPathFragments(projectRoot, 'tsconfig.test.json');
  const packageJson = readJson<PackageJson>(tree, packageJsonPath);
  const projectTsconfig = readJson<TsConfigJson>(tree, tsconfigPath);
  const libTsconfig = readJson<TsConfigJson>(tree, libTsconfigPath);
  const extendsPath = chooseTsconfigTestExtends(projectTsconfig, libTsconfig);
  const copiedCompilerOptions = getCopiedCompilerOptions(libTsconfig);
  const referencePaths = collectReferencePaths(tree, projectRoot, packageJson);

  if (!tree.exists(tsconfigTestPath)) {
    const compilerOptions: Record<string, unknown> = {
      ...copiedCompilerOptions,
      types: mergeStringArray([], ['bun']),
      composite: false,
      declaration: false,
      declarationMap: false,
      emitDeclarationOnly: false,
      noEmit: true,
    };

    const tsconfigTest: TsConfigJson = {
      extends: extendsPath,
      compilerOptions,
      include: [...DEFAULT_TEST_INCLUDES],
      references: referencePaths.map((refPath) => ({ path: refPath })),
    };

    tree.write(tsconfigTestPath, `${JSON.stringify(tsconfigTest, null, 2)}\n`);
    return;
  }

  updateJson<TsConfigJson>(tree, tsconfigTestPath, (tsconfigTest: TsConfigJson) => {
    tsconfigTest.extends ??= extendsPath;
    tsconfigTest.compilerOptions = {
      ...copiedCompilerOptions,
      ...tsconfigTest.compilerOptions,
      types: mergeStringArray(readStringArray(tsconfigTest.compilerOptions?.types), ['bun']),
      composite: false,
      declaration: false,
      declarationMap: false,
      emitDeclarationOnly: false,
      noEmit: true,
    };
    delete tsconfigTest.compilerOptions.outDir;
    delete tsconfigTest.compilerOptions.tsBuildInfoFile;
    tsconfigTest.include = mergeStringArray(tsconfigTest.include, DEFAULT_TEST_INCLUDES);
    tsconfigTest.references = mergeReferences(tsconfigTest.references, referencePaths);
    return tsconfigTest;
  });
}

function chooseTsconfigTestExtends(projectTsconfig: TsConfigJson, libTsconfig: TsConfigJson): string {
  return isWrapperTsconfig(projectTsconfig) ? (libTsconfig.extends ?? '../../tsconfig.base.json') : './tsconfig.json';
}

function isWrapperTsconfig(tsconfig: TsConfigJson): boolean {
  const includeLength = Array.isArray(tsconfig.include) ? tsconfig.include.length : 0;
  const filesLength = Array.isArray(tsconfig.files) ? tsconfig.files.length : 0;
  const compilerOptionKeys = Object.keys(tsconfig.compilerOptions ?? {});
  return includeLength === 0 && filesLength === 0 && compilerOptionKeys.length === 0;
}

function getCopiedCompilerOptions(libTsconfig: TsConfigJson): Record<string, unknown> {
  const result: Record<string, unknown> = {};
  const compilerOptions = libTsconfig.compilerOptions ?? {};

  for (const key of COPIED_COMPILER_OPTIONS) {
    if (key in compilerOptions) {
      result[key] = compilerOptions[key];
    }
  }

  return result;
}

function collectReferencePaths(tree: Tree, projectRoot: string, packageJson: PackageJson): string[] {
  const referencePaths = ['./tsconfig.lib.json'];
  const workspacePackages = buildWorkspacePackageMap(tree);
  const dependencyNames = new Set<string>([
    ...Object.keys(packageJson.dependencies ?? {}),
    ...Object.keys(packageJson.devDependencies ?? {}),
  ]);

  dependencyNames.add('@smoothbricks/lmao');

  for (const dependencyName of dependencyNames) {
    const dependencyProject = workspacePackages.get(dependencyName);
    if (!dependencyProject) {
      continue;
    }

    const dependencyTsconfig = joinPathFragments(dependencyProject.root, 'tsconfig.lib.json');
    if (!tree.exists(dependencyTsconfig)) {
      continue;
    }

    const relativePath = normalizePath(path.relative(projectRoot, dependencyTsconfig));
    if (!referencePaths.includes(relativePath)) {
      referencePaths.push(relativePath);
    }
  }

  return referencePaths;
}

function buildWorkspacePackageMap(tree: Tree): Map<string, ProjectConfiguration> {
  const packageMap = new Map<string, ProjectConfiguration>();

  for (const [, config] of getProjects(tree)) {
    const packageJsonPath = joinPathFragments(config.root, 'package.json');
    if (!tree.exists(packageJsonPath)) {
      continue;
    }

    const packageJson = readJson<PackageJson>(tree, packageJsonPath);
    if (packageJson.name) {
      packageMap.set(packageJson.name, config);
    }
  }

  return packageMap;
}

function mergeStringArray(a: string[] | undefined, b: string[]): string[] {
  const aArray = a ?? [];
  const set = new Set([...aArray, ...b]);
  return Array.from(set);
}

function readStringArray(value: unknown): string[] {
  if (Array.isArray(value)) {
    return value.filter((item): item is string => typeof item === 'string');
  }
  return [];
}

function mergeReferences(existing: Array<{ path: string }> | undefined, newPaths: string[]): Array<{ path: string }> {
  const existingPaths = existing ?? [];
  const existingSet = new Set(existingPaths.map((ref) => ref.path));

  const merged = [...existingPaths];
  for (const path of newPaths) {
    if (!existingSet.has(path)) {
      merged.push({ path });
    }
  }

  return merged;
}

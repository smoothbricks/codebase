// Scaffold TypeScript libraries and Rust workspaces through one convention-owned generator.
// CLI wiring: packages/cli/src/generate/index.ts (variant registry)
import { readJson, type Tree, writeJson } from 'nx/src/devkit-exports.js';

import type { CreatePackageGeneratorSchema } from './schema.js';

export default async function generator(tree: Tree, schema: CreatePackageGeneratorSchema): Promise<void> {
  const rootPkg = readJson<{ name?: string; license?: string; repository?: unknown }>(tree, 'package.json');
  const scope = extractScope(rootPkg.name);
  const packageName = scope ? `${scope}/${schema.name}` : schema.name;
  const projectRoot = `packages/${schema.name}`;

  if (tree.exists(`${projectRoot}/package.json`)) {
    throw new Error(`Package already exists at ${projectRoot}`);
  }
  if (schema.variant === 'ts-lib' && schema.wasm) {
    throw new Error('wasm output metadata is only available for rust-crate');
  }

  if (schema.variant === 'rust-crate') {
    writeRustFiles(tree, schema, packageName, projectRoot, rootPkg);
  } else {
    writeTypeScriptFiles(tree, schema, packageName, projectRoot, rootPkg);
  }
}

function extractScope(name: string | undefined): string | null {
  if (!name) return null;
  const match = /^(@[^/]+)\//.exec(name);
  return match?.[1] ?? null;
}

function writeTypeScriptFiles(
  tree: Tree,
  schema: CreatePackageGeneratorSchema,
  packageName: string,
  projectRoot: string,
  rootPkg: { name?: string; repository?: unknown },
): void {
  const packageJson = buildTypeScriptPackageJson(schema, packageName, projectRoot, rootPkg);
  writeJson(tree, `${projectRoot}/package.json`, packageJson);

  writeJson(tree, `${projectRoot}/tsconfig.json`, {
    extends: '../../tsconfig.base.json',
    files: [],
    include: [],
    references: [{ path: './tsconfig.lib.json' }],
  });

  writeJson(tree, `${projectRoot}/tsconfig.lib.json`, {
    extends: '../../tsconfig.base.json',
    compilerOptions: {
      rootDir: 'src',
      outDir: 'dist',
      tsBuildInfoFile: 'dist/tsconfig.lib.tsbuildinfo',
      emitDeclarationOnly: false,
      forceConsistentCasingInFileNames: true,
      types: [],
    },
    include: ['src/**/*.ts'],
    exclude: ['src/**/*.test.ts', 'src/**/__tests__/**'],
  });

  writeJson(tree, `${projectRoot}/tsconfig.test.json`, {
    extends: '../../tsconfig.base.json',
    compilerOptions: {
      types: ['bun'],
      composite: false,
      declaration: false,
      declarationMap: false,
      emitDeclarationOnly: false,
      noEmit: true,
    },
    include: [
      'src/**/*.test.ts',
      'src/**/*.spec.ts',
      'src/**/__tests__/**/*.ts',
      'src/**/__tests__/**/*.tsx',
      'src/test-suite-tracer.ts',
    ],
    references: [{ path: './tsconfig.lib.json' }],
  });

  tree.write(
    `${projectRoot}/bunfig.toml`,
    '[test]\ntimeout = 30000\npreload = ["@smoothbricks/validation/bun/preload"]\n',
  );

  tree.write(`${projectRoot}/src/index.ts`, '');
}

function buildTypeScriptPackageJson(
  schema: CreatePackageGeneratorSchema,
  packageName: string,
  projectRoot: string,
  rootPkg: { name?: string; repository?: unknown },
): Record<string, unknown> {
  const packageJson: Record<string, unknown> = {
    name: packageName,
    version: '0.0.0',
    type: 'module',
    sideEffects: false,
    main: './dist/index.js',
    module: './dist/index.js',
    types: './dist/index.d.ts',
    exports: {
      './package.json': './package.json',
      '.': {
        types: './dist/index.d.ts',
        development: './src/index.ts',
        import: './dist/index.js',
        default: './dist/index.js',
      },
    },
    files: ['dist', 'src', '!**/*.tsbuildinfo'],
    scripts: {
      test: `nx run ${schema.name}:test --outputStyle=stream`,
    },
    dependencies: {
      tslib: '^2.8.1',
    },
    devDependencies: {
      '@smoothbricks/validation': 'workspace:*',
    },
    nx: {
      name: schema.name,
      targets: {
        lint: {},
        test: {
          executor: '@smoothbricks/nx-plugin:bounded-exec',
          options: {
            command: 'bun test',
            cwd: '{projectRoot}',
            timeoutMs: 120000,
            killAfterMs: 10000,
          },
        },
      },
    },
  };

  if (schema.public) {
    packageJson.license = 'MIT';
    packageJson.publishConfig = { access: 'public' };
    const rootRepository = rootPkg.repository;
    if (isRecord(rootRepository)) {
      packageJson.repository = {
        type: rootRepository.type ?? 'git',
        url: rootRepository.url,
        directory: projectRoot,
      };
    }
    const nx = packageJson.nx;
    if (!isRecord(nx)) {
      throw new Error('generated package.json nx configuration must be an object');
    }
    nx.tags = ['npm:public'];
  } else {
    packageJson.private = true;
  }

  return packageJson;
}

function writeRustFiles(
  tree: Tree,
  schema: CreatePackageGeneratorSchema,
  packageName: string,
  projectRoot: string,
  rootPkg: { license?: string; repository?: unknown },
): void {
  if (schema.public) {
    throw new Error('rust-crate does not use npm publication metadata; publish Rust crates through Cargo policy');
  }

  const crateName = schema.name;
  const crateRoot = `${projectRoot}/crates/${crateName}`;
  writeJson(tree, `${projectRoot}/package.json`, {
    name: packageName,
    version: '0.0.0',
    private: true,
    scripts: {
      test: `nx run ${schema.name}:test --outputStyle=stream`,
    },
    nx: {
      name: schema.name,
    },
  });

  const repository = repositoryUrl(rootPkg.repository);
  const workspacePackage = [
    '[workspace]',
    'resolver = "3"',
    `members = ["crates/${crateName}"]`,
    '',
    '[workspace.package]',
    'version = "0.0.0"',
    'edition = "2024"',
    `license = ${JSON.stringify(rootPkg.license ?? 'UNLICENSED')}`,
    'publish = false',
    ...(repository === null ? [] : [`repository = ${JSON.stringify(repository)}`]),
    '',
  ];
  tree.write(`${projectRoot}/Cargo.toml`, workspacePackage.join('\n'));

  const crateManifest = [
    '[package]',
    `name = ${JSON.stringify(crateName)}`,
    'version.workspace = true',
    'edition.workspace = true',
    'license.workspace = true',
    'publish.workspace = true',
    ...(schema.wasm
      ? [
          '',
          '[lib]',
          'crate-type = ["cdylib", "rlib"]',
          '',
          '[package.metadata.smoothbricks.wasm-bindgen]',
          'targets = ["web", "nodejs"]',
          'out-dir = "dist-wasm"',
        ]
      : []),
    '',
  ];
  tree.write(`${crateRoot}/Cargo.toml`, crateManifest.join('\n'));
  tree.write(`${crateRoot}/src/lib.rs`, '#![forbid(unsafe_code)]\n');
}

function repositoryUrl(repository: unknown): string | null {
  if (typeof repository === 'string') {
    return repository;
  }
  if (!isRecord(repository)) {
    return null;
  }
  return typeof repository.url === 'string' ? repository.url : null;
}
function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

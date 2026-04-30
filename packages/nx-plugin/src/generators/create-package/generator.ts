import { readJson, type Tree, writeJson } from 'nx/src/devkit-exports.js';

import type { CreatePackageGeneratorSchema } from './schema.js';

export default async function generator(tree: Tree, schema: CreatePackageGeneratorSchema): Promise<void> {
  const rootPkg = readJson<{ name?: string; repository?: unknown }>(tree, 'package.json');
  const scope = extractScope(rootPkg.name);
  const packageName = scope ? `${scope}/${schema.name}` : schema.name;
  const projectRoot = `packages/${schema.name}`;

  if (tree.exists(`${projectRoot}/package.json`)) {
    throw new Error(`Package already exists at ${projectRoot}`);
  }

  writeCommonFiles(tree, schema, packageName, projectRoot, rootPkg);

  if (schema.variant === 'ts-zig') {
    writeZigFiles(tree, schema, projectRoot);
  }
}

function extractScope(name: string | undefined): string | null {
  if (!name) return null;
  const match = /^(@[^/]+)\//.exec(name);
  return match?.[1] ?? null;
}

function writeCommonFiles(
  tree: Tree,
  schema: CreatePackageGeneratorSchema,
  packageName: string,
  projectRoot: string,
  rootPkg: { name?: string; repository?: unknown },
): void {
  const packageJson = buildPackageJson(schema, packageName, projectRoot, rootPkg);
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

function buildPackageJson(
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
      test: `nx run ${schema.name}:test --tui=false --outputStyle=stream`,
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
    if (rootRepository && typeof rootRepository === 'object') {
      packageJson.repository = {
        type: (rootRepository as Record<string, unknown>).type ?? 'git',
        url: (rootRepository as Record<string, unknown>).url,
        directory: projectRoot,
      };
    }
    (packageJson.nx as Record<string, unknown>).tags = ['npm:public'];
  } else {
    packageJson.private = true;
  }

  return packageJson;
}

function writeZigFiles(tree: Tree, schema: CreatePackageGeneratorSchema, projectRoot: string): void {
  // Modify package.json for ts-zig variant
  const packageJson = readJson<Record<string, unknown>>(tree, `${projectRoot}/package.json`);

  // Add wasm export and remove development condition
  const exports = packageJson.exports as Record<string, unknown>;
  exports['./wasm'] = `./dist/${schema.name}.wasm`;
  const dotExport = exports['.'] as Record<string, unknown>;
  delete dotExport.development;

  // Add build scripts
  const scripts = packageJson.scripts as Record<string, string>;
  scripts['build:zig'] = `nx run ${schema.name}:zig-wasm`;
  scripts['build:ts'] = `nx run ${schema.name}:tsc-js`;
  scripts.build = 'bun run build:ts && bun run build:zig';

  // Change files for zig packages (no src published)
  packageJson.files = ['dist'];

  writeJson(tree, `${projectRoot}/package.json`, packageJson);

  // Modify tsconfig.lib.json for ts-zig
  const tsconfigLib = readJson<Record<string, unknown>>(tree, `${projectRoot}/tsconfig.lib.json`);
  const libCompilerOptions = tsconfigLib.compilerOptions as Record<string, unknown>;
  libCompilerOptions.lib = ['es2022', 'webworker'];
  libCompilerOptions.declaration = true;
  libCompilerOptions.sourceMap = true;
  libCompilerOptions.skipLibCheck = true;
  writeJson(tree, `${projectRoot}/tsconfig.lib.json`, tsconfigLib);

  // Modify tsconfig.test.json for ts-zig
  const tsconfigTest = readJson<Record<string, unknown>>(tree, `${projectRoot}/tsconfig.test.json`);
  const testCompilerOptions = tsconfigTest.compilerOptions as Record<string, unknown>;
  testCompilerOptions.lib = ['es2022', 'webworker'];
  writeJson(tree, `${projectRoot}/tsconfig.test.json`, tsconfigTest);

  // Write build.zig
  tree.write(
    `${projectRoot}/build.zig`,
    `const std = @import("std");

pub fn build(b: *std.Build) void {
    const optimize = b.standardOptimizeOption(.{});

    const wasm_step = b.step("wasm", "Build WASM artifact");

    const wasm_target = b.resolveTargetQuery(.{
        .cpu_arch = .wasm32,
        .os_tag = .freestanding,
    });

    const lib = b.addExecutable(.{
        .name = "${schema.name}",
        .root_source_file = b.path("src/${schema.name}.zig"),
        .target = wasm_target,
        .optimize = optimize,
    });
    lib.entry = .disabled;
    lib.rdynamic = true;

    const install = b.addInstallArtifact(lib, .{
        .dest_dir = .{ .override = .{ .custom = "../dist" } },
    });
    wasm_step.dependOn(&install.step);
}
`,
  );

  // Write build.zig.zon
  tree.write(
    `${projectRoot}/build.zig.zon`,
    `.{
    .name = .${schema.name},
    .version = "0.0.0",
    .minimum_zig_version = "0.14.0",
    .dependencies = .{},
    .paths = .{
        "build.zig",
        "build.zig.zon",
        "src",
    },
}
`,
  );

  // Write src/<name>.zig
  tree.write(
    `${projectRoot}/src/${schema.name}.zig`,
    `export fn add(a: i32, b: i32) i32 {
    return a + b;
}
`,
  );
}

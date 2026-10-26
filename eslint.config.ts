import tseslint from 'typescript-eslint';
import { deadlinePlugin } from './tooling/checks/no-unbounded-wait.ts';

// Type-checked rules only — biome handles everything else (formatting, imports, style).
// This config catches unsafe type assertions that biome cannot detect,
// and refuses an unbounded wait on another process (tooling/checks/no-unbounded-wait.ts): both need the types.
export default tseslint.config(
  {
    ignores: [
      '**/dist/**',
      '**/dist-*/**',
      '**/node_modules/**',
      '**/*.disabled.ts',
      '**/*.js',
      '**/*.cjs',
      '**/*.mjs',
    ],
  },
  {
    files: ['packages/*/src/**/*.ts', 'packages/*/src/**/*.tsx'],
    plugins: {
      '@typescript-eslint': tseslint.plugin,
    },
    languageOptions: {
      parser: tseslint.parser,
      parserOptions: {
        projectService: true,
        tsconfigRootDir: import.meta.dirname,
      },
    },
    rules: {
      // Catches `as any`, `as unknown as T`, and other unsafe type assertions
      '@typescript-eslint/no-unsafe-type-assertion': 'error',
      // Enforces consistent assertion style and bans object literal assertions
      '@typescript-eslint/consistent-type-assertions': [
        'error',
        {
          assertionStyle: 'as',
          objectLiteralTypeAssertions: 'never',
        },
      ],
    },
  },
  // Every wait on another process is bounded (specs/cowshed/05_gateway.md, "Deadlines", "Enforcement"): a kept one says
  // what ends it in its `// eslint-disable-next-line deadline/no-unbounded-wait -- WHY unbounded: …` directive.
  {
    files: ['packages/*/src/**/*.ts', 'packages/*/src/**/*.tsx'],
    ignores: [
      // The primitive's own module is the bound.
      'packages/lmao/src/lib/errors/Deadline.ts',
      // WHY unbounded: test code and the fixtures only tests load wait under the test runner's no-progress bound
      // (bounded-exec's idleTimeoutMs, specs/cowshed/08_testing.md), which ends a test that stops finishing.
      'packages/*/src/**/*.test.ts',
      'packages/*/src/**/*.test.tsx',
      'packages/*/src/**/__tests__/**',
      'packages/*/src/**/testing/**',
      'packages/nx-plugin/src/testing.ts',
      'packages/nx-plugin/src/executors/bounded-exec/ram-temp-fixtures.ts',
    ],
    plugins: { deadline: deadlinePlugin },
    rules: { 'deadline/no-unbounded-wait': 'error', 'deadline/unbounded-wait-why': 'error' },
  },
  {
    files: ['packages/*/src/**/*.test.ts', 'packages/*/src/**/*.test.tsx', 'packages/*/src/**/__tests__/**/*.ts'],
    languageOptions: {
      parserOptions: {
        projectService: false,
        project: ['./packages/*/tsconfig.test.json'],
        tsconfigRootDir: import.meta.dirname,
      },
    },
  },
);

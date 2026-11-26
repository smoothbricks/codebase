import type { Linter } from 'eslint';
import tseslint from 'typescript-eslint';
import { deadlinePlugin } from './tooling/checks/no-unbounded-wait.ts';

// Type-checked rules only — biome handles everything else (formatting, imports, style).
// This config catches unsafe type assertions that biome cannot detect (rule 9, AGENTS.md "Types"), in the packages'
// sources and in the repository's own tooling. `bun lint --type-aware` runs it and builds its own types. The rules it
// cannot run, custom rules that walk a TypeScript program, are in eslint.typed.config.ts, which ESLint runs.

/**
 * The sources the deadline rules read (eslint.typed.config.ts): every package source but the bound's own module and
 * test code.
 */
export const UNBOUNDED_WAIT_SOURCES = {
  files: ['packages/*/src/**/*.ts', 'packages/*/src/**/*.tsx'],
  ignores: [
    // The primitive's own module is the bound.
    'packages/lmao/src/lib/errors/Deadline.ts',
    // WHY unbounded: test code and the fixtures only tests load wait under the bound the test runner puts on a test
    // that stops finishing (specs/cowshed/08_testing.md).
    'packages/*/src/**/*.test.ts',
    'packages/*/src/**/*.test.tsx',
    'packages/*/src/**/__tests__/**',
    'packages/*/src/**/testing/**',
    'packages/nx-plugin/src/testing.ts',
    'packages/nx-plugin/src/executors/bounded-exec/ram-temp-fixtures.ts',
  ],
};
const typeAssertionRules: Linter.RulesRecord = {
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
};

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
    rules: typeAssertionRules,
  },
  // tooling/ (land, shell entry, checks) is typed by tooling/tsconfig.test.json, the one program that holds all of its
  // modules. The `lint` target of the tooling project lints that program's tracked files, so a module the program does
  // not include is linted by nobody: keep its include as wide as the directory. The program excludes the git-ignored
  // local draft `npm-prune-versions.ts`, so `typecheck-tests` types what the lint target lints and every checkout of one
  // tree gets the same answer.
  {
    files: ['tooling/**/*.ts', 'tooling/**/*.mts'],
    plugins: {
      '@typescript-eslint': tseslint.plugin,
    },
    languageOptions: {
      parser: tseslint.parser,
      parserOptions: {
        projectService: false,
        project: ['./tooling/tsconfig.test.json'],
        tsconfigRootDir: import.meta.dirname,
      },
    },
    rules: typeAssertionRules,
  },
  // The deadline rules run in ESLint (eslint.typed.config.ts), and these sources carry their
  // `eslint-disable-next-line deadline/no-unbounded-wait` directives. bun lint refuses a directive naming a rule of an
  // unregistered plugin, so the plugin is registered here with no rule on. A directive whose rule this pass does not run
  // suppresses nothing in it and would be reported unused, and that report is per file, never per rule, so it is off
  // for these sources in both passes: `deadline/unbounded-wait-why` is what judges those directives.
  {
    ...UNBOUNDED_WAIT_SOURCES,
    plugins: { deadline: deadlinePlugin },
    linterOptions: { reportUnusedDisableDirectives: 'off' },
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

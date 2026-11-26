import tseslint from 'typescript-eslint';
import { UNBOUNDED_WAIT_SOURCES } from './eslint.config.ts';
import { deadlinePlugin } from './tooling/checks/no-unbounded-wait.ts';

// The config ESLint runs for the type-directed custom rules, and nothing else. `bun lint` runs eslint.config.ts, but
// hands a rule written in JavaScript no TypeScript program (`parserServices.program` is null), so a rule that walks one
// runs here. Neither linter's config lookup reaches a file of this name; each project's inferred `lint` passes it to
// ESLint with `-c` (packages/nx-plugin/README.md, "Lint commands are inferred per project").
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
  // Every wait on another process is bounded (specs/cowshed/05_gateway.md, "Deadlines", "Enforcement"): a kept one says
  // what ends it in its `// eslint-disable-next-line deadline/no-unbounded-wait -- WHY unbounded: …` directive.
  // `deadline/unbounded-wait-why` reads only comments, but it judges the directives of the rule beside it, so the two
  // run in the same pass.
  {
    ...UNBOUNDED_WAIT_SOURCES,
    // The directives in these sources also name the rules bun lint runs: the plugin is registered so their names
    // resolve. This pass runs none of them, so it reports no unused directive (eslint.config.ts says why).
    plugins: { deadline: deadlinePlugin, '@typescript-eslint': tseslint.plugin },
    languageOptions: {
      parser: tseslint.parser,
      parserOptions: { projectService: true, tsconfigRootDir: import.meta.dirname },
    },
    linterOptions: { reportUnusedDisableDirectives: 'off' },
    rules: { 'deadline/no-unbounded-wait': 'error', 'deadline/unbounded-wait-why': 'error' },
  },
);

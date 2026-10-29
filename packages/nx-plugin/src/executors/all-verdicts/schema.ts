/** The executor the inferred `lint` target runs its tool commands with. */
export const ALL_VERDICTS_EXECUTOR = '@smoothbricks/nx-plugin:all-verdicts';

/**
 * Where an inferred ESLint command names its files. The graph holds no list of them: a list is the files of the moment
 * the graph was computed, and a run that outlives that moment fails on a file since deleted (ESLint refuses a pattern
 * no file matches) and skips one since added. The executor replaces each occurrence with the files ESLint is handed
 * when the command runs.
 */
export const ESLINT_SOURCES_TOKEN = '{eslintSources}';

/** Options of `@smoothbricks/nx-plugin:all-verdicts`. */
export interface AllVerdictsOptions {
  /** Shell commands, all started at once; none ends another. An empty list is refused: it could never fail. */
  readonly commands: readonly string[];
  /** Where every command runs, relative to the workspace root. */
  readonly cwd: string;
  /**
   * The project, by workspace-relative root, whose ESLint sources replace {@link ESLINT_SOURCES_TOKEN} in a command when
   * it runs: its `src` files and those its `tsconfig.test.json` declares, read from a fresh workspace file index. A
   * command that names the token with no project is refused. A project with none passes without running ESLint.
   */
  readonly eslintProjectRoot?: string;
}

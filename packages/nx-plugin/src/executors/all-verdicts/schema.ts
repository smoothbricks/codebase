/** The executor the inferred `lint` target runs its tool commands with. */
export const ALL_VERDICTS_EXECUTOR = '@smoothbricks/nx-plugin:all-verdicts';

/** Options of `@smoothbricks/nx-plugin:all-verdicts`. */
export interface AllVerdictsOptions {
  /** Shell commands, all started at once; none ends another. An empty list is refused: it could never fail. */
  readonly commands: readonly string[];
  /** Where every command runs, relative to the workspace root. */
  readonly cwd: string;
}

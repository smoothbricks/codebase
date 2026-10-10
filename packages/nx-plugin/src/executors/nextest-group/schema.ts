/** Options of `@smoothbricks/nx-plugin:nextest-group`: one serial nextest group, bounded once its tests are counted. */
export interface NextestGroupOptions {
  /** The group's run, built from the same archive and reuse-build flags as `listCommand`. */
  command: string;
  /** The same selection listed as JSON. Its selected, non-ignored tests size the bound. */
  listCommand: string;
  cwd?: string;
  killAfterMs?: number;
}

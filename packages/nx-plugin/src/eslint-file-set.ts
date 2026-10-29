import typia from 'typia';

/**
 * What source-aware ESLint inference retains for cache inspectors: the command, which names no file, and the sources
 * the graph that wrote it resolved. The run reads the sources it lints when it starts.
 */
export interface EslintFileSet {
  readonly command: string;
  readonly files: readonly string[];
}

export const isEslintFileSet = typia.createEquals<EslintFileSet>();

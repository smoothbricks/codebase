import typia from 'typia';

/** The exact command and sources source-aware ESLint inference retains for cache inspectors. */
export interface EslintFileSet {
  readonly command: string;
  readonly files: readonly string[];
}

export const isEslintFileSet = typia.createEquals<EslintFileSet>();

import tseslint from 'typescript-eslint';

// Type-checked rules only — biome handles everything else (formatting, imports, style).
// This config exists solely to catch unsafe type assertions that biome cannot detect.
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
);

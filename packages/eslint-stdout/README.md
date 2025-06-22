# eslint-stdout

A custom ESLint formatter that outputs fixed source code to stdout while displaying errors on stderr. This formatter is
designed to work with tools like `git-format-staged` for pre-commit hooks.

## How It Works

1. **Fixed output to stdout**: The formatter outputs the fixed source code (when available) to stdout
2. **Errors to stderr**: Any linting errors are formatted using `eslint-friendly-formatter` and sent to stderr
3. **Multiple files**: When processing multiple files, outputs are separated by null characters (`\0`)
4. **Ignored files**: No output is produced for files that ESLint is configured to ignore

This separation allows tools like `git-format-staged` to capture the fixed code while still displaying linting errors to
the user.

## Installation

```bash
npm install --save-dev eslint-stdout
# or
yarn add --dev eslint-stdout
# or
bun add -d eslint-stdout
```

## Usage

### Quick Start with git-format-staged

This package includes a convenient `eslint-stdout` command that can be used directly with
[git-format-staged](https://github.com/hallettj/git-format-staged):

```bash
#!/bin/sh
git-format-staged --verbose -f \
  "eslint-stdout '{}'" \
  '*.js' '*.ts' '*.jsx' '*.tsx'
```

The `eslint-stdout` command automatically:

- Changes to the file's directory for local config support
- Runs ESLint with `--fix-dry-run` to generate fixes
- Outputs the fixed code to stdout
- Displays any errors to stderr

### Using as an ESLint Formatter

You can also use it directly as an ESLint formatter:

```bash
eslint --format eslint-stdout --fix-dry-run your-file.js
```

## Example

```bash
# Run ESLint with fixes and capture both outputs
eslint --format eslint-stdout --fix-dry-run src/**/*.js > fixed-output.js 2> errors.log
```

## License

MIT

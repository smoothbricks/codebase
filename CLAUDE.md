# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Important Notes

**ALWAYS use Bun, never npm/npx:** This project uses Bun exclusively. Use `bun` instead of `npm`, and `bun run` instead
of `npm run`. Dev tools like `nx`, `biome`, etc. are available directly on PATH (via node_modules/.bin) so use them
directly without `bunx`.

## Common Development Commands

### Build and Development

- **Build a project**: `nx build <project-name>`
- **Type check**: `nx typecheck <project-name>`
- **Generate a new library**: `nx g @nx/js:lib packages/<name> --publishable --importPath=@my-org/<name>`
- **Sync TypeScript references**: `nx sync`
- **Check TypeScript references**: `nx sync:check`
- **Visualize project graph**: `nx graph`

### Testing

- **Run tests**: `bun test`
- **Run specific test file**: `bun test <file-path>`

### Linting and Formatting

- Code is automatically formatted on commit via Git hooks
- **Format code**: `bun run format`
- **Lint code**: `bun run lint`
- **Fix linting issues**: `bun run lint:fix`

### Package Management

- **Install dependencies**: `bun install`
- **Add dependency**: `bun add <package>`
- **Add dev dependency**: `bun add -d <package>`

## Architecture Overview

This is an Nx-based monorepo using Bun as the package manager, with devenv/direnv for environment management.

### Development Environment

- **Devenv/Direnv** automatically sets up the environment when entering the directory:
  - Installs Node.js (v22 for AWS Lambda compatibility) and Bun via Nix
  - Runs `bun install --no-summary`
  - Adds `node_modules/.bin` to PATH
  - Applies workspace Git configuration
- All dev tools (nx, biome, etc.) are available directly on PATH
- **Important**: If Claude Code is started from within a direnv session, environment changes won't reload automatically.
  To pick up environment changes after `devenv update`:
  - **Option 1**: Exit and re-enter the directory in your terminal, then restart Claude Code
  - **Option 2**: Use the `direnv-run` helper script:
    ```bash
    tooling/direnv-run <your-command>
    ```
- **Devenv command** is available anywhere in the monorepo:
  - `devenv shell` - Enter development shell
  - `devenv update` - Update devenv.lock from devenv.yaml inputs
  - `devenv up` - Start processes in the foreground
  - `devenv processes` - Start or stop processes
  - `devenv tasks` - Run tasks
  - `devenv test` - Run tests
  - `devenv search <package>` - Search for packages in nixpkgs
  - `devenv info` - Print information about the environment
  - `devenv gc` - Delete previous shell generations
  - Configuration files are in `tooling/direnv/`

### Project Structure

```
/
├── modules/        # Shared utilities and components
├── packages/       # Workspace packages (configured in package.json)
└── tooling/        # Development tools and configurations
```

### Code Quality

- **Git hooks** automatically format staged files on commit using:
  - Biome for JS/TS/JSX/TSX/JSON/HTML/CSS/GraphQL
  - ESLint for .astro files
  - Prettier for Markdown
  - Alejandra for Nix files
- **TypeScript** with strict mode and composite projects
- **Code style**: 2 spaces, single quotes, 120 character line width
- **Nx uses inferred tasks** - don't add build/typecheck scripts to package.json (Nx infers these from tsconfig), but DO
  add test scripts

### Testing

Tests use Bun's built-in test runner:

```typescript
import { describe, expect, it } from 'bun:test';
```

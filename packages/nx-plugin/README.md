# Nx Plugin

Local Nx generators for workspace-standard package setup.

## Bun Test Tracing Generator

Configure a package for the Bun test tracing + `dist-test` TypeScript pattern used in this repo.

```bash
nx generate ./packages/nx-plugin:bun-test-tracing \
  --project @smoothbricks/my-package \
  --opContextModule @smoothbricks/lmao \
  --opContextExport lmaoOpContext \
  --spanContextModule @smoothbricks/lmao \
  --spanContextExport LmaoSpanContext
```

What it wires:

- `bunfig.toml` preload for `test-trace-setup.ts`
- `test-trace-setup.ts`
- `src/test-suite-tracer.ts`
- `tsconfig.test.json` with `dist-test` output
- package `tsconfig.json` reference to `./tsconfig.test.json`
- package `package.json` test/lint/devDependency wiring needed for the standard pattern

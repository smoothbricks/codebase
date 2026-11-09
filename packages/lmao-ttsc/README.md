# @smoothbricks/lmao-ttsc

The [LMAO](../lmao) compile-time transform: a native ttsc/TypeScript-Go plugin that lowers span, tag, log and result
calls to monomorphic direct writes and registers literal log templates and span names in LMAO's global vocabulary. The
contract for emitted code is [spec 01o](../../specs/lmao/01o_typescript_transformer.md).

## Install

Add `@smoothbricks/lmao-ttsc` as a direct dependency; ttsc discovers its plugin from the package's `ttsc.plugin` field.
For `Bun.build`, pass the adapter:

```typescript
import ttsc from '@smoothbricks/lmao-ttsc/bun';

await Bun.build({ entrypoints: ['src/index.ts'], outdir: 'dist', plugins: [ttsc()] });
```

For Bun's runtime loader (`bun run`, `bun test`), preload `@smoothbricks/lmao-ttsc/bun-register` in `bunfig.toml`.

## Trace statements compile away in production

A `ctx.log.trace(...)` statement, arguments included, is removed from every compile whose `NODE_ENV` is not `test`, and
kept in test compiles, so trace-claim tests can count the entries production never pays for. `bun test` and Vitest set
`NODE_ENV=test` themselves. A trace call used as a value is the compile error `LMAO_TRACE_NOT_STATEMENT` in every mode.
See [spec 01o §6.3](../../specs/lmao/01o_typescript_transformer.md#smoo/lmao!n/transformer-trace-elision).

## Caveats

- **Build caches must key on `NODE_ENV`.** An Nx or other task cache over output this plugin transformed must include
  `NODE_ENV` in its inputs. This package declares `"smoo": { "compileEnv": ["NODE_ENV"] }`, from which
  `@smoothbricks/nx-plugin` adds the input to the `tsc-js` and scriptc targets it infers for every direct dependent; a
  hand-declared target that compiles through this plugin, and a test target, list `{ "env": "NODE_ENV" }` themselves.

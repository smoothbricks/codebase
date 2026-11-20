# Bend emitter

`bend-emit` checks a Bend 2 book and emits its first-order kernel and law drivers as Rust, Goose, and TypeScript. It
lowers the checked terms, not the source syntax. Proof terms are erased; emitted drivers exercise the claims over
enumerated and seeded inputs. A law binding supplies the production comparison separately.

## Command

```sh
bend-emit <model.bend> <outdir> --fns a,b --laws x,y
```

- `--fns a,b` and `--laws x,y` select definitions and laws. The emitter includes definitions they reach.
- `--cap N` bounds enumerated cases per law (default 400). `--len N` bounds enumerated list length (default 3).
  `--no-corpus x,y` leaves those laws' enumerated cases to their binding.
- `--props N` sets seeded draws per law (default 200). `--prop-len N` sets their list length bound (default 12).
- `--rust-module path` or `--ts-module path` writes a single binding module to that path instead of standalone drivers.
  `--model-name name` supplies the model's name in its provenance header (the default is the input basename).

The command always takes both positionals; module mode does not use the output directory. Without module mode, it writes
`<stem>.rs`, `<stem>.goose`, `<stem>.ts` and `run_<stem>.ts`. It refuses outside the first-order fragment over Nat,
Bool, lists and non-generic user ADTs, including closures, templates and unsupported term/type forms. A checker or
emitter refusal exits nonzero without writing an output.

## Package API and local link

```ts
import { checkBendFile } from '@smoothbricks/bend-emit';

const book = await checkBendFile('model.bend');
const output = book.emit({
  target: { kind: 'all', stem: 'model' },
  functions: ['a', 'b'],
  laws: ['x'],
});
```

`checkBendFile` loads and checks once; `emit` returns the sources and counts without writing files. Multiple emissions
from one checked book are independent. The package runs on Bun. To test a local package in another checkout:

```sh
(cd <provider>/packages/bend-emit && bun link)
(cd <consumer> && bun link @smoothbricks/bend-emit)
(cd <consumer> && bun run bend-emit <model.bend> <outdir> --fns a,b --laws x,y)
```

The linked command needs no build or checker environment variable. The regular package build emits the public TypeScript
declarations for publication.

## Checker custody

The package carries the checker `bend2/bend.ts`, its `bend2/base.bend` standard library, and `LICENSE` from
[bendlang/bend at `38103b3d46425026eff2d16bef93684967bee992`](https://github.com/bendlang/bend/tree/38103b3d46425026eff2d16bef93684967bee992),
under `vendor/bend2/`. The checker and standard library stay side by side as upstream expects. Neither linking nor
running the command downloads a checker. A checker update replaces these files from one named revision, then rechecks
the bound laws and compares their emissions. The vendored checker marks the local TypeScript compatibility edits; they
preserve the pinned checker's behavior.

The gateway's production `Turns` binding follows
[Cowshed gateway §Land turns](../../specs/cowshed/05_gateway.md#land-turns). From the repository root run
`bun run --filter @smoothbricks/cowshed emit-land-turn-law` to regenerate its kernel; add `--check` to compare without
rewriting it.

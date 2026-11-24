# Bend emitter

`bend-emit` checks a Bend 2 book and emits its first-order kernel and law drivers as Rust, Goose, Go, and TypeScript. It
lowers the checked terms, not the source syntax. Proof terms are erased; emitted drivers exercise the claims over
enumerated and seeded inputs. A law binding supplies the production comparison separately.

## Command

```sh
bend-emit <model.bend> <outdir> --fns a,b --laws x,y [--manifest path]
```

- `--fns a,b` and `--laws x,y` select definitions and laws. The emitter includes definitions they reach.
- `--cap N` bounds enumerated cases per law (default 400). `--len N` bounds enumerated list length (default 3).
  `--adt-depth N` bounds enumerated nesting depth for recursive user ADTs (default 2). `--no-corpus x,y` leaves those
  laws' enumerated cases to their binding.
- `--props N` sets seeded draws per law (default 200). `--prop-len N` sets their list length bound (default 12).
- `--rust-module path`, `--ts-module path` or `--go-module path` writes one binding module instead of standalone
  drivers. `--model-name name` supplies the model's name in its provenance header and manifest (the default is the input
  basename).
- `--manifest path` writes a deterministic JSON provenance manifest containing `modelName`, `bendRevision`,
  `hashAlgorithm` (`"blake3"`), and per-law
  `{ name, proven, digest, params, hypsCount, corpusCount, propsCount, reachedFns }`. Each law `digest` is the 256-bit
  BLAKE3 hex digest over the de-Bruijn-normalized checked core type of the law, so alpha-equivalent laws with renamed
  binders produce identical digests.

The command always takes both positionals; module mode does not use the output directory. Without module mode, it writes
`<stem>.rs`, `<stem>.goose`, `<stem>.go`, `<stem>.ts` and `run_<stem>.ts`.

## Supported fragment and lowerings

`bend-emit` accepts the first-order fragment over `Nat`, `Bool`, `List<T>`, and monomorphized first-order user and
`Base` ADTs (including `Maybe<T>`, `Result<E, A>`, `Either<A, B>`, non-dependent `Pair`/`Sigma`, and custom polymorphic
ADTs), plus call-site specialization of polymorphic first-order functions. It refuses closures, templates, dependent
non-trivial `Sigma`, and unsupported term/type forms; a checker or emitter refusal exits nonzero without writing an
output.

- **Equality and predicate laws**: laws may state hypotheses and claims as either equality (`{a == b : T}`) or
  `IsTrue(p)` (where `IsTrue` maps `True{} -> Unit` and `False{} -> Empty`, or is an abstract `IsTrue` law). `IsTrue(p)`
  lowers identically to `{p == True{} : Bool}`. Nullary laws (zero `for` binders) emit valid unit-tuple corpora (`()` in
  Rust, `[]` in TS).
- **Verified bounded arithmetic & bitwise lowerings**:
  - `Nat.add`, `Nat.sub` (saturating), `Nat.mul` (overflow-checked `< 2^48`), `Nat.div` (`div(a, 0) = 0`), `Nat.mod`
    (`mod(a, 0) = a`), and `Nat.is_{eq,ne,le,lt,ge,gt}` are verified against their Bend `Base` definitions on boundary
    and seeded pairs before lowering.
  - Structural user-defined Nat relations (`==`, `!=`, `<=`, `<`, `>=`, `>`) and structural `sub`/`mul` are verified
    against the recursive IR evaluator before lowering.
  - User-defined Nat arithmetic/bitwise defs named `sub`, `mul`, `div`, `mod`, `bit_and`, `bit_or`, `bit_xor`, `shl`
    (overflow-checked `< 2^48`), `shr`, `popcount`, and `ctz` (matching either `ctz(0) = 0` or `ctz(0) = 64`) are
    lowered to native 48-bit-exact operations only after verifying exact agreement with their Bend recursive definitions
    on boundary and seeded inputs. TypeScript bitwise helpers use `BigInt` internally so values above `2^31` remain
    exact.
- **Vacuity-rate tracking & definedness gate**:
  - Standalone drivers and `run_<stem>.ts` print `tested`/`vacuous` counts and a `definedness` summary per law, failing
    closed (exit 1) if any law with `total > 0` has `tested == 0` (100% vacuous), unless `--allow-vacuous` is passed.
  - Binding modules emit `LawStats` and a companion `check_<law>_definedness()` (`Check_<law>_definedness()` in Go)
    without changing existing `law_*`, `corpus_*`, or `Drive_*` signatures.
- **Recursive user ADTs**: self-recursive and mutually-recursive user ADTs (such as radix trees and ancestor paths) are
  automatically detected, wrapped in `Rc<T>` in Rust to prevent `E0072` infinite-size errors while keeping clones O(1),
  and enumerated and property-generated with strict depth bounds (`--adt-depth`, default 2; seeded draws capped at
  depth 3) across all four targets.
- **Iterative tail-call lowering**: single-function self-tail-recursive definitions lower to iterative loops in Rust
  (`loop`), TypeScript (`tail: for (;;)`), and Go (`tail: for`), avoiding call-stack overflow on deep inputs.

## Package API and local link

```ts
import { checkBendFile } from '@smoothbricks/bend-emit';

const book = await checkBendFile('model.bend');
const output = book.emit({
  target: { kind: 'all', stem: 'model' },
  functions: ['a', 'b'],
  laws: ['x'],
});
// output.manifest holds the deterministic provenance manifest and BLAKE3 law digests.
```

`checkBendFile` loads and checks once; `emit` returns the sources, counts, and `manifest` without writing files.
Multiple emissions from one checked book are independent. The package runs on Bun. To test a local package in another
checkout:

```sh
(cd <provider>/packages/bend-emit && bun link)
(cd <consumer> && bun link @smoothbricks/bend-emit)
(cd <consumer> && bun run bend-emit <model.bend> <outdir> --fns a,b --laws x,y)
```

The linked command needs no build or checker environment variable. The regular package build emits the public TypeScript
declarations for publication.

## Go binding

`--go-module model.go` emits `package bendmodel`. Select `{ kind: 'go-module', modelName, flags }` through the library
for the same source. Nat is `uint64` with the same checked addition bound as the other targets. Bool is `bool`. A list
is `*List[T]` (`nil` is empty; fields `Head` and `Tail`). A monomorphic datatype `Packet` is the sealed interface
`Type_Packet`; `Ctor_Packed{F0: n, F1: live, F2: items}` constructs its `Packed` variant. Monomorphized generic ADTs
disambiguate constructors by their instantiated type name (for example `Type_Maybe_Nat` and `Ctor_Maybe_Nat_Some`).

`Fn_number(x)` runs the model. `Law_all_packets_are_zero(x)` returns `(ok, lhs)`; a false hypothesis returns
`(true, "vacuous")`. `Drive_all_packets_are_zero(visit)` calls `visit(kind, index, ok, lhs, x)` on the shared corpus
(`kind == "law"`) and then on the shared seeded draws (`"prop"`). `Check_all_packets_are_zero_definedness()` returns a
`LawStats` summary (`Total`, `Tested`, `Vacuous`, `Failed`, `IsVacuous()`, `Passed()`). Indices in each start at zero. A
binding uses these typed inputs to run production in the same process. No Go command is invoked during emission.

To exercise the package's conformance fixture from this directory:

```sh
out=$(mktemp -d)
laws=packets_agree,guarded_lists_agree,all_packets_are_zero
bun bin/bend-emit.ts test/conformance.bend "$out" --laws "$laws" --cap 40 --len 2 --props 16
bun "$out/run_conformance.ts" --build
```

The last command needs `rustc`, `go`, Bun, and the Goose language compiler (`GOOSE` can name it). It builds each native
driver once, then compares row identities, verdicts and canonical results. Run without `--build` to reuse the binaries.
The first two laws must pass on all four targets. The open law must fail with status **1**, not a tool failure **2**;
the four targets must agree on `Packed(0,false,[])` as the first counterexample, with lhs `1`.

To build and then execute the callable-package witness separately:

```sh
bun bin/bend-emit.ts test/conformance.bend . --go-module "$out/model.go" --laws "$laws" --cap 40 --len 2 --props 16
cp test/testdata/binding_test.go "$out/"
go test -c -o "$out/binding.test" "$out/model.go" "$out/binding_test.go"
"$out/binding.test" -test.v
```

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

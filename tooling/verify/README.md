# Standalone Verification Toolchain (`tooling/verify`)

This standalone Nix flake provides formal verification and undefined-behavior checking tools without touching
`tooling/direnv/devenv.nix` or invalidating any Nx build/test cache key across the monorepo.

## Included Tools

- **Nightly Rust + Miri** (`cargo-miri`, `rust-src`, `llvm-tools-preview`, `rustc-dev`), pinned to the exact nightly
  matching Kani's compiler ABI
- **`miri` helper** — sets `MIRIFLAGS="${MIRIFLAGS:--Zmiri-disable-isolation}"` and runs `cargo miri "$@"`
- **Kani** (`kani`, `cargo-kani`, prebuilt `0.68.0` release for macOS and Linux)
- **Alive2** (`alive-tv`) — LLVM IR translation validation
- **CBMC** (`cbmc`) — bounded model checker
- **Z3** (`z3`) — SMT solver
- **Lean 4** (`lean`) — interactive theorem prover

## Usage

Enter the verification shell:

```sh
nix develop ./tooling/verify
```

Or run a single command without entering an interactive shell:

```sh
# Run Miri on bitmosaic unit tests
nix develop ./tooling/verify --command miri test -p bitmosaic --lib

# Verify Kani harnesses (when invoked inside a crate with #[cfg(kani)] proofs)
nix develop ./tooling/verify --command cargo kani -p bitmosaic

# Verify LLVM IR equivalence with Alive2
nix develop ./tooling/verify --command alive-tv before.ll after.ll
```

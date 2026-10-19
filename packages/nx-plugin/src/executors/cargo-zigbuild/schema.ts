export interface CargoZigbuildOptions {
  /** Rust target triple, with cargo-zigbuild's glibc suffix where it pins one: `x86_64-unknown-linux-gnu.2.17`. */
  target: string;
  /** The crate's Cargo.toml, relative to the workspace root. */
  manifestPath: string;
  /** Cargo package to build. */
  package: string;
  /** Build this binary and copy its executable; absent, build the package's cdylib. */
  bin?: string;
  /** Destination file for the artifact, relative to the workspace root. */
  output: string;
  /** Compile cargo's `release` profile instead of `dev`. */
  release?: boolean;
}

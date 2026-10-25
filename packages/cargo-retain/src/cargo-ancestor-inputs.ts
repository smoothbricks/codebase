/** Files in every ancestor of a package directory that Cargo reads while building it. */
export const CARGO_ANCESTOR_INPUTS = ['Cargo.toml', '.cargo/config', '.cargo/config.toml'] as const;

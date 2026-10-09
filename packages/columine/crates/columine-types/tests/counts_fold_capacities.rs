//! Generates `src/counts-fold-capacities.ts` from [`COUNTS_FOLD_CAPACITIES`].
//!
//! The VM's counts decoder reads the Rust value; a compiler that emits counts
//! instructions reads the TypeScript one. The TypeScript is rendered from the
//! Rust value and committed, so importing it needs no wasm, and a capacity
//! change that is not regenerated fails here. `UPDATE_GENERATED=1` rewrites the
//! file, as the arrow plane generator does in the other direction.

use columine_types::vo::{COUNTS_FOLD_CAPACITIES, CountBounds};

fn generated_path() -> String {
    let manifest = std::env::var("CARGO_MANIFEST_DIR")
        .expect("cargo sets CARGO_MANIFEST_DIR for the tests it runs");
    format!("{manifest}/../../src/counts-fold-capacities.ts")
}

fn bounds_type(bounds: CountBounds) -> String {
    format!("CountBounds<{}, {}>", bounds.min, bounds.max)
}

fn bounds_value(bounds: CountBounds) -> String {
    format!("{{ min: {}, max: {} }}", bounds.min, bounds.max)
}

fn render() -> String {
    let c = COUNTS_FOLD_CAPACITIES;
    format!(
        "// Generated from COUNTS_FOLD_CAPACITIES in crates/columine-types/src/vo.rs by
// crates/columine-types/tests/counts_fold_capacities.rs; rerun that test with
// UPDATE_GENERATED=1 instead of editing.

/** Inclusive bounds on how many of one thing a counts instruction may name. */
export interface CountBounds<Min extends number, Max extends number> {{
  readonly min: Min;
  readonly max: Max;
}}

/**
 * What one counts instruction may hold. The VM refuses an instruction outside
 * these bounds; a compiler reads the same value to refuse a counts role before
 * it emits one. Zero histograms is below `histograms.min`; zero masks is valid.
 */
export interface CountsFoldCapacities {{
  readonly histograms: {};
  readonly bins: {};
  readonly masks: {};
}}

export const COUNTS_FOLD_CAPACITIES: CountsFoldCapacities = {{
  histograms: {},
  bins: {},
  masks: {},
}};
",
        bounds_type(c.histograms),
        bounds_type(c.bins),
        bounds_type(c.masks),
        bounds_value(c.histograms),
        bounds_value(c.bins),
        bounds_value(c.masks),
    )
}

#[test]
fn typescript_capacities_are_generated_from_the_native_definition() {
    let path = generated_path();
    let rendered = render();
    let committed = std::fs::read_to_string(&path).unwrap_or_default();
    if std::env::var("UPDATE_GENERATED").as_deref() == Ok("1") {
        if committed != rendered {
            std::fs::write(&path, &rendered).expect("write the generated capacities");
        }
        return;
    }
    assert_eq!(
        committed, rendered,
        "{path} is not generated from COUNTS_FOLD_CAPACITIES; rerun with UPDATE_GENERATED=1"
    );
}

// Generated from COUNTS_FOLD_CAPACITIES in crates/columine-types/src/vo.rs by
// crates/columine-types/tests/counts_fold_capacities.rs; rerun that test with
// UPDATE_GENERATED=1 instead of editing.

/** Inclusive bounds on how many of one thing a counts instruction may name. */
export interface CountBounds<Min extends number, Max extends number> {
  readonly min: Min;
  readonly max: Max;
}

/**
 * What one counts instruction may hold. The VM refuses an instruction outside
 * these bounds; a compiler reads the same value to refuse a counts role before
 * it emits one. Zero histograms is below `histograms.min`; zero masks is valid.
 */
export interface CountsFoldCapacities {
  readonly histograms: CountBounds<1, 8>;
  readonly bins: CountBounds<1, 64>;
  readonly masks: CountBounds<0, 4>;
}

export const COUNTS_FOLD_CAPACITIES: CountsFoldCapacities = {
  histograms: { min: 1, max: 8 },
  bins: { min: 1, max: 64 },
  masks: { min: 0, max: 4 },
};

//! Counts fold against an independent model: a histogram is a plain array of
//! counts, decay divides every count by two, and the frame is decoded back
//! into that model after every sample.
//!
//! The module is schema-blind, so these frames are opaque byte layouts built
//! here for geometry alone: leading bytes, a constant region (count, three
//! region words, a mask), three `[u32 count][u16 × 16]` regions, and a
//! metadata tail the fold never interprets but must keep and guard. Schema
//! certification of a real program's layout is the program admitter's proof,
//! not this module's, and is not exercised here.

use columine_types::operand::encode_index;
use columine_vm::counts_fold::{CountsFoldError, CountsFoldOp, CountsFoldTarget};
use proptest::prelude::*;

const BINS: usize = 16;
const AXES: usize = 3;
/// Opaque leading bytes every frame must repeat; its length is the origin.
const PREFIX: &[u8] = b"leading-bytes-16";
const ORIGIN: u32 = PREFIX.len() as u32;
// Origin-relative constant positions.
const COUNT_OFF: u32 = 0;
const REF_OFF: [u32; AXES] = [4, 8, 12];
const MASK_OFF: u32 = 16;
/// Region `h` starts here (origin-relative): `[u32 count][u16 × BINS]`.
const REGION: [u32; AXES] = [20, 20 + 36, 20 + 72];
const ELEMS_AT: u8 = 4;
const VALUE_LEN: usize = 20 + 108;
const KEY_COL: u32 = 0;
const BUCKET_COLS: [u32; AXES] = [1, 2, 3];
const MASK_COL: u32 = 4;
const INIT_OFFSETS_COL: u32 = 5;
const INIT_VALUES_COL: u32 = 6;

fn at(off: u32) -> usize {
    PREFIX.len() + off as usize
}

fn bin_at(axis: usize, bin: usize) -> usize {
    at(REGION[axis]) + ELEMS_AT as usize + 2 * bin
}

/// A zero-count frame whose metadata tail is `meta` (empty: none recorded).
fn zero_frame(meta: &[u8]) -> Vec<u8> {
    let mut frame = PREFIX.to_vec();
    frame.resize(PREFIX.len() + VALUE_LEN, 0);
    for axis in 0..AXES {
        frame[at(REF_OFF[axis])..at(REF_OFF[axis]) + 4]
            .copy_from_slice(&REGION[axis].to_le_bytes());
        frame[at(REGION[axis])..at(REGION[axis]) + 4].copy_from_slice(&(BINS as u32).to_le_bytes());
    }
    frame.extend_from_slice(meta);
    frame
}

const META: &[u8] = b"\x05cargo";

fn push_index(code: &mut Vec<u8>, value: u32) {
    code.extend_from_slice(encode_index(value).as_bytes());
}

/// Operands with explicit layout knobs, so refusals can be planted one at a time.
struct Program {
    prefix: Vec<u8>,
    count_off: u32,
    hists: Vec<(u32, u8, u8, u32)>,
    masks: Vec<(u32, u32)>,
}

impl Program {
    fn standard() -> Self {
        Self {
            prefix: PREFIX.to_vec(),
            count_off: COUNT_OFF,
            hists: (0..AXES)
                .map(|axis| (REF_OFF[axis], ELEMS_AT, BINS as u8, BUCKET_COLS[axis]))
                .collect(),
            masks: vec![(MASK_OFF, MASK_COL)],
        }
    }

    fn encode(&self) -> Vec<u8> {
        let mut code = Vec::new();
        push_index(&mut code, 7); // slot
        push_index(&mut code, KEY_COL);
        code.push(2); // field
        push_index(&mut code, INIT_OFFSETS_COL);
        push_index(&mut code, INIT_VALUES_COL);
        push_index(&mut code, self.count_off);
        code.push(self.hists.len() as u8);
        for &(ref_off, elems_at, bins, col) in &self.hists {
            push_index(&mut code, ref_off);
            code.push(elems_at);
            code.push(bins);
            push_index(&mut code, col);
        }
        code.push(self.masks.len() as u8);
        for &(off, col) in &self.masks {
            push_index(&mut code, off);
            push_index(&mut code, col);
        }
        push_index(&mut code, self.prefix.len() as u32);
        code.extend_from_slice(&self.prefix);
        code
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Model {
    n: u32,
    hists: [[u32; BINS]; AXES],
    mask: u8,
}

impl Model {
    const ZERO: Self = Self {
        n: 0,
        hists: [[0; BINS]; AXES],
        mask: 0,
    };

    fn fold(&mut self, sample: &Sample) -> Result<(), CountsFoldError> {
        self.n = self
            .n
            .checked_add(1)
            .ok_or(CountsFoldError::CountOverflow)?;
        for (hist, &bucket) in self.hists.iter_mut().zip(&sample.buckets) {
            if hist[bucket as usize] == 65_535 {
                for count in hist.iter_mut() {
                    *count /= 2;
                }
            }
            hist[bucket as usize] += 1;
        }
        self.mask |= sample.mask as u8;
        Ok(())
    }

    fn decode(frame: &[u8]) -> Self {
        let mut hists = [[0; BINS]; AXES];
        for (axis, hist) in hists.iter_mut().enumerate() {
            for (bin, count) in hist.iter_mut().enumerate() {
                let off = bin_at(axis, bin);
                *count = u32::from(u16::from_le_bytes([frame[off], frame[off + 1]]));
            }
        }
        Self {
            n: u32::from_le_bytes(frame[at(COUNT_OFF)..at(COUNT_OFF) + 4].try_into().unwrap()),
            hists,
            mask: frame[at(MASK_OFF)],
        }
    }

    /// The frame bytes that hold this model, with metadata tail `meta`.
    fn encode(&self, meta: &[u8]) -> Vec<u8> {
        let mut frame = zero_frame(meta);
        frame[at(COUNT_OFF)..at(COUNT_OFF) + 4].copy_from_slice(&self.n.to_le_bytes());
        for (axis, hist) in self.hists.iter().enumerate() {
            for (bin, &count) in hist.iter().enumerate() {
                let off = bin_at(axis, bin);
                frame[off..off + 2].copy_from_slice(&(count as u16).to_le_bytes());
            }
        }
        frame[at(MASK_OFF)] = self.mask;
        frame
    }
}

#[derive(Clone, Debug)]
struct Sample {
    buckets: [u32; AXES],
    mask: u32,
    init: Vec<u8>,
}

fn sample(buckets: [u32; AXES], mask: u32) -> Sample {
    Sample {
        buckets,
        mask,
        init: zero_frame(META),
    }
}

/// One batch as the columns the instruction reads (column 0: keys).
fn columns(samples: &[Sample]) -> Vec<Vec<u8>> {
    let mut cols = vec![Vec::new(); 7];
    cols[INIT_OFFSETS_COL as usize].extend_from_slice(&0u32.to_le_bytes());
    for sample in samples {
        cols[KEY_COL as usize].extend_from_slice(&1u32.to_le_bytes());
        for axis in 0..AXES {
            cols[BUCKET_COLS[axis] as usize].extend_from_slice(&sample.buckets[axis].to_le_bytes());
        }
        cols[MASK_COL as usize].extend_from_slice(&sample.mask.to_le_bytes());
        cols[INIT_VALUES_COL as usize].extend_from_slice(&sample.init);
        let end = cols[INIT_VALUES_COL as usize].len() as u32;
        cols[INIT_OFFSETS_COL as usize].extend_from_slice(&end.to_le_bytes());
    }
    cols
}

/// Fold every element of the batch into one row, in batch order, the way the
/// dispatch does: prepare borrows; an absent row becomes a copy of the init
/// frame; apply writes only what prepare accepted.
fn fold_batch(
    op: &CountsFoldOp<'_>,
    row: &mut Option<Vec<u8>>,
    samples: &[Sample],
) -> Result<(), CountsFoldError> {
    let cols = columns(samples);
    let cols: Vec<&[u8]> = cols.iter().map(Vec::as_slice).collect();
    for element in 0..samples.len() as u32 {
        let sample = op.read_sample(&cols, element)?;
        let plan = op.prepare(row.as_deref(), &sample)?;
        match plan.target() {
            CountsFoldTarget::Insert => {
                assert!(row.is_none());
                let mut frame = sample.init().to_vec();
                op.apply(&plan, &mut frame);
                *row = Some(frame);
            }
            CountsFoldTarget::Stored => op.apply(&plan, row.as_mut().unwrap()),
        }
    }
    Ok(())
}

fn prepared(
    op: &CountsFoldOp<'_>,
    stored: Option<&[u8]>,
    one: Sample,
) -> Result<(), CountsFoldError> {
    let cols = columns(&[one]);
    let cols: Vec<&[u8]> = cols.iter().map(Vec::as_slice).collect();
    op.prepare(stored, &op.read_sample(&cols, 0)?).map(|_| ())
}

fn sample_strategy() -> impl Strategy<Value = Sample> {
    // Few buckets, so bins reach the decay threshold from near-full histories.
    ([0u32..3, 0u32..BINS as u32, 0u32..2], 0u32..256)
        .prop_map(|(buckets, mask)| sample(buckets, mask))
}

fn history_strategy() -> impl Strategy<Value = Model> {
    (
        0u32..u32::MAX - 512,
        proptest::array::uniform3(proptest::array::uniform16(prop_oneof![
            0u32..4,
            65_530u32..=65_535
        ])),
        any::<u8>(),
    )
        .prop_map(|(n, hists, mask)| Model { n, hists, mask })
}

fn standard() -> Vec<u8> {
    Program::standard().encode()
}

proptest! {
    #[test]
    fn every_fold_matches_the_model(history in history_strategy(), samples in proptest::collection::vec(sample_strategy(), 1..200)) {
        let code = standard();
        let op = CountsFoldOp::decode(&code, 0).unwrap();
        let mut row = Some(history.encode(META));
        let mut model = history;
        for one in &samples {
            fold_batch(&op, &mut row, std::slice::from_ref(one)).unwrap();
            model.fold(one).unwrap();
            let frame = row.as_deref().unwrap();
            prop_assert_eq!(Model::decode(frame), model.clone());
            // Byte for byte: the counters the model holds, every other byte the row's own.
            let expected = model.encode(META);
            prop_assert_eq!(frame, expected.as_slice());
        }
    }

    #[test]
    fn a_replay_folds_identical_bytes(samples in proptest::collection::vec(sample_strategy(), 1..300)) {
        let code = standard();
        let op = CountsFoldOp::decode(&code, 0).unwrap();
        let mut first = None;
        let mut replay = None;
        fold_batch(&op, &mut first, &samples).unwrap();
        fold_batch(&op, &mut replay, &samples).unwrap();
        prop_assert_eq!(first, replay);
    }

    #[test]
    fn below_decay_the_sample_order_does_not_matter(
        samples in proptest::collection::vec(sample_strategy(), 1..100),
        rotate in 0usize..100,
    ) {
        let code = standard();
        let op = CountsFoldOp::decode(&code, 0).unwrap();
        let mut reordered = samples.clone();
        reordered.reverse();
        let len = reordered.len();
        reordered.rotate_left(rotate % len);
        let mut forward = None;
        let mut other = None;
        fold_batch(&op, &mut forward, &samples).unwrap();
        fold_batch(&op, &mut other, &reordered).unwrap();
        prop_assert_eq!(Model::decode(forward.as_deref().unwrap()).n, samples.len() as u32);
        prop_assert_eq!(forward, other);
    }
}

#[test]
fn an_absent_row_starts_from_the_elements_init_frame() {
    let code = standard();
    let op = CountsFoldOp::decode(&code, 0).unwrap();
    let mut row = None;
    fold_batch(
        &op,
        &mut row,
        &[
            sample([1, 2, 3], 0b100),
            sample([1, 0, 3], 0b001),
            sample([1, 2, 15], 0),
        ],
    )
    .unwrap();
    let frame = row.unwrap();
    let folded = Model::decode(&frame);
    assert_eq!(folded.n, 3);
    assert_eq!(folded.hists[0][..2], [0, 3]);
    assert_eq!(folded.hists[1][..3], [1, 0, 2]);
    assert_eq!((folded.hists[2][3], folded.hists[2][15]), (2, 1));
    assert_eq!(folded.mask, 0b101);
    assert_eq!(frame, folded.encode(META));
}

#[test]
fn a_full_bin_halves_its_whole_histogram_before_counting() {
    let code = standard();
    let op = CountsFoldOp::decode(&code, 0).unwrap();
    let mut history = Model {
        n: 70_000,
        ..Model::ZERO
    };
    history.hists[0][..3].copy_from_slice(&[65_535, 5_543, 7]);
    // Axis 1 holds a full bin the sample does not land in: it must not decay.
    history.hists[1][..2].copy_from_slice(&[65_535, 9]);
    history.hists[2][0] = 65_534;
    let mut row = Some(history.encode(META));
    fold_batch(&op, &mut row, &[sample([0, 1, 0], 0)]).unwrap();
    let folded = Model::decode(row.as_deref().unwrap());
    assert_eq!(folded.n, 70_001);
    assert_eq!(folded.hists[0][..3], [32_768, 2_771, 3]);
    assert_eq!(folded.hists[1][..2], [65_535, 10]);
    assert_eq!(folded.hists[2][0], 65_535);
    // The next sample on axis 2's now-full bin halves it then.
    fold_batch(&op, &mut row, &[sample([1, 1, 0], 0)]).unwrap();
    assert_eq!(Model::decode(row.as_deref().unwrap()).hists[2][0], 32_768);
}

#[test]
fn a_full_count_refuses_the_sample_before_any_write() {
    let code = standard();
    let op = CountsFoldOp::decode(&code, 0).unwrap();
    let mut history = Model {
        n: u32::MAX,
        mask: 1,
        ..Model::ZERO
    };
    history.hists[0][4] = 65_535;
    let stored = history.encode(META);
    // `prepare` only borrows the frame: the refusal cannot have written.
    assert_eq!(
        prepared(&op, Some(&stored), sample([4, 0, 0], 0b10)),
        Err(CountsFoldError::CountOverflow)
    );
    assert_eq!(stored, history.encode(META));
    // One below the ceiling still folds (the STRICT-BY control).
    let mut row = Some(
        Model {
            n: u32::MAX - 1,
            ..history
        }
        .encode(META),
    );
    fold_batch(&op, &mut row, &[sample([4, 0, 0], 0b10)]).unwrap();
    assert_eq!(Model::decode(row.as_deref().unwrap()).n, u32::MAX);
}

#[test]
fn a_row_keeps_its_metadata_and_refuses_an_element_that_disagrees() {
    let code = standard();
    let op = CountsFoldOp::decode(&code, 0).unwrap();
    let stored = Model {
        n: 5,
        ..Model::ZERO
    }
    .encode(META);
    let conflict = Err(CountsFoldError::MetadataConflict);
    let with_init = |meta: &[u8]| Sample {
        init: zero_frame(meta),
        ..sample([0, 0, 0], 0)
    };
    assert_eq!(prepared(&op, Some(&stored), with_init(META)), Ok(()));
    assert_eq!(
        prepared(&op, Some(&stored), with_init(b"\x05cargX")),
        conflict,
        "other metadata"
    );
    assert_eq!(
        prepared(&op, Some(&stored), with_init(b"")),
        conflict,
        "metadata the row has, absent"
    );
    let bare = Model {
        n: 5,
        ..Model::ZERO
    }
    .encode(b"");
    assert_eq!(
        prepared(&op, Some(&bare), with_init(META)),
        conflict,
        "metadata the row lacks, present"
    );
    assert_eq!(
        prepared(&op, Some(&bare), with_init(b"")),
        Ok(()),
        "absence kept is agreement"
    );
}

#[test]
fn an_init_frame_the_fold_cannot_start_from_is_refused() {
    let code = standard();
    let op = CountsFoldOp::decode(&code, 0).unwrap();
    let invalid = Err(CountsFoldError::InvalidCellValue);
    let init = |edit: &dyn Fn(&mut Vec<u8>)| {
        let mut frame = zero_frame(META);
        edit(&mut frame);
        Sample {
            init: frame,
            ..sample([0, 0, 0], 0)
        }
    };
    for stored in [None, Some(zero_frame(META))] {
        let stored = stored.as_deref();
        assert_eq!(prepared(&op, stored, init(&|_| {})), Ok(()));
        assert_eq!(
            prepared(&op, stored, init(&|f| f[3] ^= 1)),
            invalid,
            "other leading bytes"
        );
        assert_eq!(
            prepared(&op, stored, init(&|f| f[at(COUNT_OFF)] = 1)),
            invalid,
            "a nonzero count"
        );
        assert_eq!(
            prepared(&op, stored, init(&|f| f[at(MASK_OFF)] = 1)),
            invalid,
            "a nonzero mask"
        );
        assert_eq!(
            prepared(&op, stored, init(&|f| f[bin_at(1, 7)] = 1)),
            invalid,
            "a nonzero bin"
        );
        assert_eq!(
            prepared(&op, stored, init(&|f| f[at(REGION[2])] = 15)),
            invalid,
            "15 elements"
        );
        assert_eq!(
            prepared(&op, stored, init(&|f| f.truncate(bin_at(2, BINS) - 1))),
            invalid,
            "short"
        );
    }
}

#[test]
fn a_stored_frame_whose_geometry_differs_is_refused() {
    let code = standard();
    let op = CountsFoldOp::decode(&code, 0).unwrap();
    let refused = |edit: &dyn Fn(&mut Vec<u8>)| {
        let mut stored = zero_frame(META);
        edit(&mut stored);
        prepared(&op, Some(&stored), sample([0, 0, 0], 0)).err()
    };
    let mismatch = Some(CountsFoldError::FrameMismatch);
    assert_eq!(refused(&|_| {}), None);
    assert_eq!(
        refused(&|f| f[PREFIX.len() - 1] ^= 1),
        mismatch,
        "other leading bytes"
    );
    assert_eq!(
        refused(&|f| f.truncate(at(MASK_OFF))),
        mismatch,
        "short of a constant range"
    );
    assert_eq!(
        refused(&|f| f.truncate(bin_at(2, BINS) - 1)),
        mismatch,
        "a region past the end"
    );
    let word = |axis: usize, value: u32| {
        move |f: &mut Vec<u8>| {
            f[at(REF_OFF[axis])..at(REF_OFF[axis]) + 4].copy_from_slice(&value.to_le_bytes())
        }
    };
    assert_eq!(
        refused(&word(0, 0)),
        mismatch,
        "a region over the constant ranges"
    );
    assert_eq!(
        refused(&word(1, REGION[0])),
        mismatch,
        "two histograms in one region"
    );
    assert_eq!(
        refused(&word(2, u32::MAX)),
        mismatch,
        "a region word past the address space"
    );
    assert_eq!(
        refused(&|f| f[at(REGION[1])] = 15),
        mismatch,
        "an element count other than bin_count"
    );
}

#[test]
fn samples_outside_the_layout_are_refused() {
    let code = standard();
    let op = CountsFoldOp::decode(&code, 0).unwrap();
    let invalid = Err(CountsFoldError::InvalidCellValue);
    assert_eq!(prepared(&op, None, sample([0, BINS as u32, 0], 0)), invalid);
    assert_eq!(prepared(&op, None, sample([0, 0, 0], 256)), invalid);
    let cols = columns(&[sample([0, 0, 0], 0)]);
    let full: Vec<&[u8]> = cols.iter().map(Vec::as_slice).collect();
    assert!(op.read_sample(&full, 0).is_ok());
    assert_eq!(
        op.read_sample(&full, 1).err(),
        Some(CountsFoldError::ColumnUnderrun)
    );
    let short: Vec<&[u8]> = cols.iter().map(|col| &col[..col.len().min(2)]).collect();
    assert_eq!(
        op.read_sample(&short, 0).err(),
        Some(CountsFoldError::ColumnUnderrun)
    );
    let mut backwards = cols.clone();
    backwards[INIT_OFFSETS_COL as usize] = [8u32, 4].iter().flat_map(|v| v.to_le_bytes()).collect();
    let backwards: Vec<&[u8]> = backwards.iter().map(Vec::as_slice).collect();
    assert_eq!(
        op.read_sample(&backwards, 0).err(),
        Some(CountsFoldError::InvalidCellValue)
    );
    let mut past = cols.clone();
    let beyond = past[INIT_VALUES_COL as usize].len() as u32 + 1;
    past[INIT_OFFSETS_COL as usize] = [0u32, beyond]
        .iter()
        .flat_map(|v| v.to_le_bytes())
        .collect();
    let past: Vec<&[u8]> = past.iter().map(Vec::as_slice).collect();
    assert_eq!(
        op.read_sample(&past, 0).err(),
        Some(CountsFoldError::ColumnUnderrun)
    );
}

#[test]
fn decode_reports_the_instruction_it_read() {
    let mut code = standard();
    let end = code.len();
    code.push(0x00); // the next instruction
    let op = CountsFoldOp::decode(&code, 0).unwrap();
    assert_eq!(
        (op.slot, op.key_col, op.field_idx, op.end),
        (7, KEY_COL, 2, end)
    );
    assert_eq!(
        (op.init_offsets_col, op.init_values_col),
        (INIT_OFFSETS_COL, INIT_VALUES_COL)
    );
    assert_eq!(op.prefix(), PREFIX);
    let mut seen = Vec::new();
    op.for_each_column(&mut |col| seen.push(col));
    assert_eq!(
        seen,
        [
            KEY_COL,
            INIT_OFFSETS_COL,
            INIT_VALUES_COL,
            1,
            2,
            3,
            MASK_COL
        ]
    );
    assert_eq!(op.histograms()[2].ref_off, ORIGIN + REF_OFF[2]);
    assert_eq!(op.masks()[0].off, ORIGIN + MASK_OFF);
}

#[test]
fn every_truncation_is_refused() {
    let code = standard();
    for len in 0..code.len() {
        assert_eq!(
            CountsFoldOp::decode(&code[..len], 0).err(),
            Some(CountsFoldError::InvalidProgram),
            "{len}"
        );
    }
}

#[test]
fn malformed_operands_are_refused() {
    let refused = |edit: &dyn Fn(&mut Program)| {
        let mut program = Program::standard();
        edit(&mut program);
        CountsFoldOp::decode(&program.encode(), 0).err()
    };
    let invalid = Some(CountsFoldError::InvalidProgram);
    assert_eq!(refused(&|_| {}), None);
    assert_eq!(
        refused(&|p| p.prefix.clear()),
        invalid,
        "no leading bytes to compare"
    );
    assert_eq!(
        refused(&|p| p.hists[1].0 = REF_OFF[0] + 2),
        invalid,
        "overlapping region words"
    );
    assert_eq!(
        refused(&|p| p.masks[0].0 = COUNT_OFF + 3),
        invalid,
        "a mask inside the count"
    );
    assert_eq!(
        refused(&|p| p.count_off = REF_OFF[2] + 1),
        invalid,
        "a count over a region word"
    );
    assert_eq!(
        refused(&|p| p.hists[0].1 = 3),
        invalid,
        "elements inside the count word"
    );
    assert_eq!(refused(&|p| p.hists.clear()), invalid, "no histogram");
    assert_eq!(
        refused(&|p| p.hists = vec![(REF_OFF[0], ELEMS_AT, BINS as u8, 1); 9]),
        invalid,
        "too many histograms"
    );
    assert_eq!(
        refused(&|p| p.hists[0].2 = 0),
        invalid,
        "an empty histogram"
    );
    assert_eq!(refused(&|p| p.hists[0].2 = 65), invalid, "too many bins");
    assert_eq!(
        refused(&|p| p.masks = vec![(MASK_OFF, MASK_COL); 5]),
        invalid,
        "too many masks"
    );
    // A mask-free layout is a layout like any other.
    assert_eq!(refused(&|p| p.masks.clear()), None);
}

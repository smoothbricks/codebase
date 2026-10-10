//! The BITMAP slot: a native bitmosaic image patched where it lies, read
//! through a zero-copy view.
//!
//! # Slot layout
//!
//! `[image: bitmap_payload_capacity bytes]` — the self-delimited native u32
//! image directly, with no length word and no fixed headroom. The empty set
//! is the four-byte empty image; a non-empty set occupies the first
//! `serialized_len` bytes as the forest root of the set, and every payload
//! byte past the image is zero. Together those make the slot image a pure
//! function of the set: byte-equal, whatever the history, to a fresh
//! `Bitmosaic::write_forest_into_slice` of the same members (the forest is
//! the mutable root — `bitmosaic::patch` keeps the root arm, and a forest
//! patch touches only the chunks a batch names).
//!
//! Slot data offsets stay 8-aligned from the state layout, and every reader
//! uses byte access, so no alignment padding lives inside the image.
//!
//! # Mutation
//!
//! Every write is one `bitmosaic::patch_witnessed` over the image: the batch
//! is sorted and deduplicated into VM-owned buffers, the patch re-ladders
//! only the touched chunks and reports per value which adds were new and
//! which removes were held — the bit the edit overwrote, so the undo
//! records and TTL entries cost no second probe — and its refusals leave
//! the slot untouched: a `Capacity` refusal becomes
//! `ErrorCode::CapacityExceeded`, which the slot-growth retry answers by
//! doubling the slot and replaying. Nothing on the steady path allocates:
//! the `PatchScratch`, the batch buffers and the outcome planes keep their
//! high-water mark inside [`BitmapEnv`].
//!
//! # Reads
//!
//! Membership, cardinality, rank/select, iteration and intersection open an
//! `BitmosaicView` over the slot bytes (or over caller-supplied bytes for the
//! decision-side `*_serialized` queries) and answer from the string; no read
use crate::bitmap_ttl::{TtlAlgebraDemand, TtlAlgebraScratch, merge_expiries};
use crate::hooks::{ByteCapture, TtlMutation, VmHooks};
use crate::meta::SlotMetaView;
use crate::state_init::bind_slot;
use crate::ttl_plan::TtlBatchPlan;
use bitmosaic::{
    AndNotRange, AndRange, BatchOutcome, Bitmosaic, BitmosaicView, IMAGE_ID_LEN, KeyWidth, OrRange,
    PatchError, PatchReport, PatchScratch, PreparedPatch, Range, XorRange, image_header,
    patched_len_bound, prepare_patch_in_witnessed,
};
use columine_types::{
    BITMAP_BYTES_PER_CAPACITY, ChangeFlag, EMPTY_KEY, ErrorCode, SlotMetaOffset, TOMBSTONE,
};

/// Canonical payload-capacity formula used by allocation, grow-copy, and
/// readers. Keeping one formula prevents those paths from disagreeing: four
/// bytes per element of slot capacity plus the image identifier, so even a
/// zero-capacity slot fits the four-byte empty image.
pub const fn bitmap_payload_capacity(slot_capacity: u32) -> u32 {
    slot_capacity * BITMAP_BYTES_PER_CAPACITY + IMAGE_ID_LEN as u32
}
/// Bitmap storage view carrying offsets into the state buffer rather than
/// references into it.
#[derive(Clone, Copy, Debug)]
pub struct BitmapStorage {
    /// Offset of the native image (== the slot data offset: the image sits
    /// directly, with no length word).
    pub data_offset: u32,
    pub payload_capacity: u32,
}

impl BitmapStorage {
    pub fn payload_offset(&self) -> u32 {
        self.data_offset
    }

    /// The image's self-delimited length: the header is parsed over exactly
    /// the capacity-sized region, so a width mismatch or an extent the slot
    /// does not hold is `InvalidState`, never a slice.
    pub fn serialized_len(&self, state: &[u8]) -> Result<u32, ErrorCode> {
        let start = self.data_offset as usize;
        let capacity = self.payload_capacity as usize;
        let region = state
            .get(start..start.checked_add(capacity).ok_or(ErrorCode::InvalidState)?)
            .ok_or(ErrorCode::InvalidState)?;
        let header = image_header(region).ok_or(ErrorCode::InvalidState)?;
        if header.width != KeyWidth::U32 {
            return Err(ErrorCode::InvalidState);
        }
        u32::try_from(header.encoded_len).map_err(|_| ErrorCode::InvalidState)
    }

    /// The native image bytes, or `Err` when no width-checked image of a
    /// fitting extent starts at the slot offset. The empty set is the
    /// four-byte empty image, not an absent slice.
    pub fn serialized_data<'a>(&self, state: &'a [u8]) -> Result<&'a [u8], ErrorCode> {
        let len = self.serialized_len(state)? as usize;
        let start = self.data_offset as usize;
        state.get(start..start + len).ok_or(ErrorCode::InvalidState)
    }

    /// The slot's set as a zero-copy view. `Ok(None)` is the empty set,
    /// admitted from the empty header in constant work without walking a
    /// body; `Err` an image the payload does not hold.
    pub fn view<'a>(&self, state: &'a [u8]) -> Result<Option<BitmosaicView<'a>>, ErrorCode> {
        let data = self.serialized_data(state)?;
        if data.len() == IMAGE_ID_LEN {
            return Ok(None);
        }
        BitmosaicView::open(data)
            .map(Some)
            .ok_or(ErrorCode::InvalidState)
    }
}

/// Build a bitmap storage view from slot metadata.
pub fn get_bitmap_storage(meta: &SlotMetaView) -> BitmapStorage {
    BitmapStorage {
        data_offset: meta.offset,
        payload_capacity: bitmap_payload_capacity(meta.capacity),
    }
}

/// Observable bitmap operation state: the patch scratch and the batch
/// buffers every write reuses, the decision-side algebra result, and the
/// last diagnostic.
pub struct BitmapEnv {
    /// `g_bitmap_last_error` — diagnostic code readable after a failure.
    pub last_error: u32,
    scratch: PatchScratch,
    eviction_witness: (),
    capture_ranges: Vec<(u32, u32)>,
    /// Staged adds as `elem << 32 | column index`, so one sort orders them by
    /// value and, within a value, by first occurrence.
    staged_adds: Vec<u64>,
    adds: Vec<u32>,
    removes: Vec<u32>,
    /// The witness planes: bit `i` of `adds_new` for `adds[i]`, bit `i` of
    /// `removes_held` for `removes[i]`.
    adds_new: Vec<u64>,
    removes_held: Vec<u64>,
    algebra_result: Vec<u8>,
    ttl_mutations: Vec<TtlMutation>,
    ttl_algebra_scratch: TtlAlgebraScratch,
    /// Capacities when a batch's reservation was sealed: no operation of
    /// the batch may grow past them.
    #[cfg(debug_assertions)]
    sealed: Option<[usize; 8]>,
    /// Whether a batch reserved this storage and sealed it. An operation
    /// inside such a batch reserves nothing of its own: the batch's bound
    /// covers every set its operations reach from the batch's starting
    /// images, while an operation's own bound, taken from an image the batch
    /// already changed, can exceed it.
    batch_sealed: bool,
}

/// The largest bitmap patch one batch or call can make.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct BitmapPatchDemand {
    /// Most values one operation adds or removes, before deduplication.
    pub(crate) values: usize,
    /// The largest image one patch can leave, bounded by its slot.
    pub(crate) image_bytes: usize,
    /// Most members one patch touches.
    pub(crate) touches: usize,
    /// The largest TTL algebra merge.
    pub(crate) ttl: TtlAlgebraDemand,
}

impl BitmapPatchDemand {
    pub(crate) fn widen(&mut self, other: Self) {
        self.values = self.values.max(other.values);
        self.image_bytes = self.image_bytes.max(other.image_bytes);
        self.touches = self.touches.max(other.touches);
        self.ttl.widen(other.ttl);
    }
}

/// The demand of one patch adding at most `adds` and removing at most
/// `removes` members of `meta`'s bitmap: its image never outgrows
/// `patched_len_bound`, nor the slot's payload.
pub(crate) fn patch_demand(
    state: &[u8],
    meta: &SlotMetaView,
    adds: u64,
    removes: u64,
) -> Result<BitmapPatchDemand, ErrorCode> {
    storage_demand(state, get_bitmap_storage(meta), adds, removes)
}

/// [`patch_demand`] for a bitmap known by its storage.
fn storage_demand(
    state: &[u8],
    storage: BitmapStorage,
    adds: u64,
    removes: u64,
) -> Result<BitmapPatchDemand, ErrorCode> {
    let image = storage.serialized_data(state)?;
    let touches = adds.saturating_add(removes);
    let bound = patched_len_bound(image, adds, touches)
        .ok_or(ErrorCode::InvalidState)?
        .min(u64::from(storage.payload_capacity));
    let narrow = |value: u64| usize::try_from(value).map_err(|_| ErrorCode::OutOfMemory);
    Ok(BitmapPatchDemand {
        values: narrow(touches)?,
        image_bytes: narrow(bound)?,
        touches: narrow(touches)?,
        ttl: TtlAlgebraDemand::default(),
    })
}

/// The number of values a column names, for demand arithmetic.
fn count(values: &[u32]) -> u64 {
    u64::try_from(values.len()).unwrap_or(u64::MAX)
}

/// The live expiry rows of `meta`'s slot; none without TTL.
fn expiry_rows(state: &[u8], meta: &SlotMetaView) -> usize {
    if meta.has_ttl() {
        usize::try_from(meta.eviction_index_size(state)).unwrap_or(usize::MAX)
    } else {
        0
    }
}

/// Grow `buf` to hold `total` elements, refusing with `OutOfMemory`.
fn hold<T>(buf: &mut Vec<T>, total: usize) -> Result<(), ErrorCode> {
    buf.try_reserve(total.saturating_sub(buf.len()))
        .map_err(|_| ErrorCode::OutOfMemory)
}

impl Default for BitmapEnv {
    fn default() -> Self {
        BitmapEnv {
            last_error: 0,
            scratch: PatchScratch::new(),
            eviction_witness: (),
            capture_ranges: Vec::new(),
            staged_adds: Vec::new(),
            adds: Vec::new(),
            removes: Vec::new(),
            adds_new: Vec::new(),
            removes_held: Vec::new(),
            algebra_result: Vec::new(),
            ttl_mutations: Vec::new(),
            ttl_algebra_scratch: TtlAlgebraScratch::default(),
            #[cfg(debug_assertions)]
            sealed: None,
            batch_sealed: false,
        }
    }
}

impl core::fmt::Debug for BitmapEnv {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("BitmapEnv")
            .field("last_error", &self.last_error)
            .field("algebra_result_len", &self.algebra_result.len())
            .finish_non_exhaustive()
    }
}

impl BitmapEnv {
    /// `vm_rbmp_algebra_result_{ptr,len}` equivalent — the bindings stage
    /// exposes the buffer; core code returns the slice.
    pub fn algebra_result(&self) -> &[u8] {
        &self.algebra_result
    }

    /// Back every buffer the patches `demand` bounds can fill: the value and
    /// witness buffers, the capture ranges (one run per old payload word and
    /// eight per touched chunk, then the tail, size, flag and two TTL
    /// ranges), the kernel's patch scratch and the TTL algebra merge.
    /// Capacity already held is kept.
    pub(crate) fn reserve_for(&mut self, demand: BitmapPatchDemand) -> Result<(), ErrorCode> {
        let planes = demand.values.div_ceil(64);
        hold(&mut self.adds, demand.values)?;
        hold(&mut self.removes, demand.values)?;
        hold(&mut self.staged_adds, demand.values)?;
        hold(&mut self.ttl_mutations, demand.values)?;
        hold(&mut self.adds_new, planes)?;
        hold(&mut self.removes_held, planes)?;
        let ranges = (demand.image_bytes / 8)
            .saturating_add(demand.touches.saturating_mul(8))
            .saturating_add(6);
        hold(&mut self.capture_ranges, ranges)?;
        self.scratch
            .try_reserve_for(demand.image_bytes, demand.touches)
            .map_err(|_| ErrorCode::OutOfMemory)?;
        self.ttl_algebra_scratch.reserve_for(demand.ttl)
    }

    /// From here until [`Self::unseal`], no operation may grow this storage;
    /// debug builds check it when the batch ends.
    /// Reserve one operation's own demand, unless a batch already reserved
    /// and sealed this storage.
    fn reserve_op(&mut self, demand: BitmapPatchDemand) -> Result<(), ErrorCode> {
        if self.batch_sealed {
            return Ok(());
        }
        self.reserve_for(demand)
    }

    pub(crate) fn seal(&mut self) {
        self.batch_sealed = true;
        self.ttl_algebra_scratch.seal();
        #[cfg(debug_assertions)]
        {
            self.sealed = Some(self.capacities());
        }
    }

    pub(crate) fn unseal(&mut self) {
        self.batch_sealed = false;
        self.ttl_algebra_scratch.unseal();
        #[cfg(debug_assertions)]
        {
            if let Some(sealed) = self.sealed.take() {
                debug_assert_eq!(
                    self.capacities(),
                    sealed,
                    "a bitmap operation grew past its batch's reservation"
                );
            }
        }
    }

    #[cfg(debug_assertions)]
    fn capacities(&self) -> [usize; 8] {
        [
            self.adds.capacity(),
            self.removes.capacity(),
            self.staged_adds.capacity(),
            self.ttl_mutations.capacity(),
            self.adds_new.capacity(),
            self.removes_held.capacity(),
            self.capture_ranges.capacity(),
            self.scratch.capacity_bytes(),
        ]
    }
}

/// Whether a column is already the batch a patch takes: strictly ascending
/// and free of the sentinel keys. Such a column is patched as it is, with
/// no copy, sort or dedup; the sentinels are the two largest `u32`s, so an
/// ascending column can only carry them at its end and the last value
/// decides.
fn is_batch(column: &[u32]) -> bool {
    column.last().is_none_or(|&last| last < TOMBSTONE) && column.is_sorted_by(|a, b| a < b)
}

fn bitmap_patch_error(last_error: &mut u32, error: PatchError) -> ErrorCode {
    match error {
        PatchError::Capacity { .. } => {
            *last_error = 60;
            ErrorCode::CapacityExceeded
        }
        PatchError::Malformed => {
            *last_error = 102;
            ErrorCode::InvalidState
        }
        PatchError::Unsorted => {
            columine_types::die!("bitmap batches are sorted and deduplicated before patching")
        }
    }
}

fn bitmap_payload_range(
    storage: BitmapStorage,
    state_len: usize,
) -> Result<core::ops::Range<usize>, ErrorCode> {
    let start = storage.payload_offset() as usize;
    let end = start
        .checked_add(storage.payload_capacity as usize)
        .filter(|&end| end <= state_len)
        .ok_or(ErrorCode::InvalidState)?;
    Ok(start..end)
}

/// Retain the real kernel's paid mutation plan for VM-owned eviction. The
/// authority borrows the same state and scratch until applied, so it cannot
/// be replayed against another image or after another patch reuses scratch.
pub fn prepare_bitmap_patch<'state, 'work>(
    env: &'work mut BitmapEnv,
    state: &'state mut [u8],
    storage: BitmapStorage,
    adds: &'work [u32],
    removes: &'work [u32],
) -> Result<PreparedPatch<'state, 'work, ()>, ErrorCode> {
    let range = bitmap_payload_range(storage, state.len()).inspect_err(|_| {
        env.last_error = 102;
    })?;
    env.reserve_op(storage_demand(state, storage, count(adds), count(removes))?)?;
    prepare_patch_in_witnessed(
        state,
        range,
        adds,
        removes,
        &mut env.scratch,
        &mut env.eviction_witness,
    )
    .map_err(|error| bitmap_patch_error(&mut env.last_error, error))
}

/// Patch the native image once, using its existing bitmosaic membership
/// witness. A refusal changes no state; success also clears the old framing
/// tail, without copying the image or its allocated capacity.
pub fn bitmap_patch(
    env: &mut BitmapEnv,
    state: &mut [u8],
    storage: BitmapStorage,
    adds: &[u32],
    removes: &[u32],
) -> Result<PatchReport, ErrorCode> {
    let range = bitmap_payload_range(storage, state.len()).inspect_err(|_| {
        env.last_error = 102;
    })?;
    env.reserve_op(storage_demand(state, storage, count(adds), count(removes))?)?;
    env.adds_new.resize(adds.len().div_ceil(64), 0);
    env.removes_held.resize(removes.len().div_ceil(64), 0);
    let mut outcome = BatchOutcome::new(&mut env.adds_new, &mut env.removes_held);
    let prepared =
        prepare_patch_in_witnessed(state, range, adds, removes, &mut env.scratch, &mut outcome)
            .map_err(|error| bitmap_patch_error(&mut env.last_error, error))?;
    Ok(prepared.apply_with_cleared_tail().0)
}

pub(crate) fn push_payload_capture_range(
    ranges: &mut Vec<(u32, u32)>,
    payload: u32,
    relative: usize,
    len: usize,
) -> Result<(), ErrorCode> {
    if len == 0 {
        return Ok(());
    }
    let relative = u32::try_from(relative).map_err(|_| ErrorCode::InvalidState)?;
    let len = u32::try_from(len).map_err(|_| ErrorCode::InvalidState)?;
    let offset = payload
        .checked_add(relative)
        .ok_or(ErrorCode::InvalidState)?;
    let end = offset.checked_add(len).ok_or(ErrorCode::InvalidState)?;
    if let Some((previous, previous_len)) = ranges.last_mut() {
        let previous_end = *previous + *previous_len;
        if offset <= previous_end {
            *previous_len = previous_end.max(end) - *previous;
            return Ok(());
        }
    }
    ranges.try_reserve(1).map_err(|_| ErrorCode::OutOfMemory)?;
    ranges.push((offset, len));
    Ok(())
}

enum BitmapTtlCapture {
    Unchanged,
    Membership,
    Prepared(TtlBatchPlan),
}

/// Admit every payload/header/index range before the prepared patch writes
/// its first byte. The shared owner reserves StateBytes and the outstanding
/// flag boundary; TTL work stays inside this one physical capture.
#[allow(clippy::too_many_arguments)]
fn capture_bitmap_patch(
    env: &mut BitmapEnv,
    hooks: &mut impl VmHooks,
    state: &mut [u8],
    meta: &SlotMetaView,
    storage: BitmapStorage,
    adds: &[u32],
    removes: &[u32],
    ttl_capture: BitmapTtlCapture,
) -> Result<(PatchReport, ByteCapture, Option<TtlBatchPlan>), ErrorCode> {
    let range = bitmap_payload_range(storage, state.len()).inspect_err(|_| {
        env.last_error = 102;
    })?;
    env.adds_new.resize(adds.len().div_ceil(64), 0);
    env.removes_held.resize(removes.len().div_ceil(64), 0);
    let mut outcome = BatchOutcome::new(&mut env.adds_new, &mut env.removes_held);
    let prepared =
        prepare_patch_in_witnessed(state, range, adds, removes, &mut env.scratch, &mut outcome)
            .map_err(|error| bitmap_patch_error(&mut env.last_error, error))?;
    env.capture_ranges.clear();
    // Merge the single cleared-tail interval into the kernel's already sorted
    // ranges in one pass. No re-sort, duplicate capture, or capacity-sized copy.
    let mut tail = prepared.cleared_tail_range();
    for &(offset, len) in prepared.mutation_ranges() {
        if let Some((tail_offset, tail_len)) = tail
            && tail_offset <= offset
        {
            push_payload_capture_range(
                &mut env.capture_ranges,
                storage.payload_offset(),
                tail_offset,
                tail_len,
            )?;
            tail = None;
        }
        push_payload_capture_range(
            &mut env.capture_ranges,
            storage.payload_offset(),
            offset,
            len,
        )?;
    }
    if let Some((offset, len)) = tail {
        push_payload_capture_range(
            &mut env.capture_ranges,
            storage.payload_offset(),
            offset,
            len,
        )?;
    }
    env.capture_ranges
        .push((meta.meta_base + SlotMetaOffset::SIZE, 4));
    // The shared owner filters this byte when an enclosing boundary owns it;
    // a direct container caller without that boundary still journals flags.
    env.capture_ranges
        .push((meta.meta_base + SlotMetaOffset::CHANGE_FLAGS, 1));
    let ttl_plan = match ttl_capture {
        BitmapTtlCapture::Unchanged => None,
        BitmapTtlCapture::Prepared(plan) => Some(plan),
        BitmapTtlCapture::Membership => {
            // Membership operations consume the paid kernel's final witness.
            // Algebra already joined its expiry changes and admitted its plan.
            for (i, &key) in removes.iter().enumerate() {
                if prepared.witness().remove_was_held(i) {
                    env.ttl_mutations.push(TtlMutation::Remove { key });
                }
            }
            Some(hooks.plan_ttl_batch(prepared.state(), meta, env.ttl_mutations.iter().copied())?)
        }
    };
    if let Some(plan) = &ttl_plan {
        env.capture_ranges.extend_from_slice(plan.ranges());
    }
    let capture = hooks.begin_capture(prepared.state(), || &env.capture_ranges)?;
    let (report, _) = prepared.apply_with_cleared_tail();
    Ok((report, capture, ttl_plan))
}

/// Consume the already-admitted TTL plan once; neither payload nor index can
/// discover an operational refusal after the first primary byte is written.
fn finish_bitmap_mutations(
    env: &mut BitmapEnv,
    hooks: &mut impl VmHooks,
    delta_mode: bool,
    state: &mut [u8],
    meta: &SlotMetaView,
    capture: ByteCapture,
    ttl_plan: Option<TtlBatchPlan>,
) {
    if let Some(plan) = ttl_plan {
        hooks.consume_ttl_batch_plan(state, meta, plan);
    }
    env.ttl_mutations.clear();
    hooks.finish_capture(delta_mode, state, capture);
}

/// Select the element at `rank` in ascending order.
pub fn bitmap_select(state: &[u8], storage: BitmapStorage, rank: u32) -> Option<u32> {
    let data = storage.serialized_data(state).ok()?;
    let view = BitmosaicView::open(data)?;
    view.select(u64::from(rank))
}

/// Add a batch of elements. Elements already present refresh their TTL;
/// a value repeated within the batch is inserted once, with its first
/// timestamp. When the batch would exceed `meta.capacity` elements, the
/// earliest of the batch that fit are committed and the call returns
/// `CapacityExceeded`; a payload-capacity refusal commits nothing.
#[allow(clippy::too_many_arguments)]
pub fn batch_bitmap_add(
    env: &mut BitmapEnv,
    hooks: &mut impl VmHooks,
    delta_mode: bool,
    state: &mut [u8],
    meta: &SlotMetaView,
    elem_col: &[u32],
    ts_col: Option<&[f64]>,
) -> ErrorCode {
    env.last_error = 0;
    env.ttl_mutations.clear();
    env.staged_adds.clear();
    let storage = get_bitmap_storage(meta);
    // Every buffer this add can fill is backed before it writes.
    if let Err(code) =
        storage_demand(state, storage, count(elem_col), 0).and_then(|demand| env.reserve_op(demand))
    {
        return code;
    }
    let has_ttl = meta.has_ttl();
    let timestamp_at = |i: usize| -> f64 {
        if has_ttl {
            ts_col.unwrap_or_else(|| columine_types::die!("TTL slot requires a timestamp column"))
                [i]
        } else {
            0.0
        }
    };

    let cardinality = meta.size(state);
    let room = meta.capacity.saturating_sub(cardinality) as usize;

    // A column that is already the batch, and cannot reach the element
    // cap, is patched as it is: its index in the column is its index in
    // the batch.
    if elem_col.len() <= room && is_batch(elem_col) {
        if elem_col.is_empty() {
            return ErrorCode::Ok;
        }
        if has_ttl {
            for (i, &key) in elem_col.iter().enumerate() {
                env.ttl_mutations.push(TtlMutation::Upsert {
                    key,
                    timestamp: timestamp_at(i),
                    mapped_value: key,
                });
            }
        }
        let ttl_capture = if has_ttl {
            BitmapTtlCapture::Membership
        } else {
            BitmapTtlCapture::Unchanged
        };
        let (report, capture, ttl_plan) = match capture_bitmap_patch(
            env,
            hooks,
            state,
            meta,
            storage,
            elem_col,
            &[],
            ttl_capture,
        ) {
            Ok(patched) => patched,
            Err(code) => return code,
        };
        meta.set_size(state, cardinality + report.added);
        if report.added > 0 {
            meta.set_change_flag(state, ChangeFlag::INSERTED);
        }
        finish_bitmap_mutations(env, hooks, delta_mode, state, meta, capture, ttl_plan);
        return ErrorCode::Ok;
    }

    // Value order with first occurrence first, so the dedup keeps the
    // earliest column index of a repeated value.
    for (i, &elem) in elem_col.iter().enumerate() {
        if elem == EMPTY_KEY || elem == TOMBSTONE {
            continue;
        }
        env.staged_adds.push((u64::from(elem) << 32) | i as u64);
    }
    env.staged_adds.sort_unstable();
    env.staged_adds
        .dedup_by_key(|staged| (*staged >> 32) as u32);

    let overflow = if env.staged_adds.len() > room {
        // The batch may exceed the element cap: only the values not already
        // held count against it, so this path probes for them and keeps
        // the `room` earliest new values by column index. The common path
        // below never probes — the patch reports what was new.
        let view = match storage.view(state) {
            Ok(view) => view,
            Err(code) => {
                env.last_error = 102;
                return code;
            }
        };
        let mut kept = 0usize;
        env.staged_adds.retain(|staged| {
            let elem = (*staged >> 32) as u32;
            let present = view.is_some_and(|v| v.contains(elem));
            present || {
                kept += 1;
                true
            }
        });
        let overflow = kept > room;
        if overflow {
            let mut admitted = 0usize;
            env.staged_adds
                .sort_unstable_by_key(|staged| *staged as u32);
            env.staged_adds.retain(|staged| {
                let present = view.is_some_and(|v| v.contains((*staged >> 32) as u32));
                present || {
                    admitted += 1;
                    admitted <= room
                }
            });
            env.staged_adds.sort_unstable();
        }
        overflow
    } else {
        false
    };

    if env.staged_adds.is_empty() {
        return if overflow {
            ErrorCode::CapacityExceeded
        } else {
            ErrorCode::Ok
        };
    }
    let mut adds = core::mem::take(&mut env.adds);
    adds.clear();
    adds.extend(env.staged_adds.iter().map(|staged| (*staged >> 32) as u32));
    if has_ttl {
        for staged in &env.staged_adds {
            let key = (*staged >> 32) as u32;
            env.ttl_mutations.push(TtlMutation::Upsert {
                key,
                timestamp: timestamp_at(*staged as u32 as usize),
                mapped_value: key,
            });
        }
    }
    let ttl_capture = if has_ttl {
        BitmapTtlCapture::Membership
    } else {
        BitmapTtlCapture::Unchanged
    };
    let patched = capture_bitmap_patch(env, hooks, state, meta, storage, &adds, &[], ttl_capture);
    env.adds = adds;
    let (report, capture, ttl_plan) = match patched {
        Ok(patched) => patched,
        Err(code) => return code,
    };
    meta.set_size(state, cardinality + report.added);
    if report.added > 0 {
        meta.set_change_flag(state, ChangeFlag::INSERTED);
    }
    finish_bitmap_mutations(env, hooks, delta_mode, state, meta, capture, ttl_plan);
    if overflow {
        ErrorCode::CapacityExceeded
    } else {
        ErrorCode::Ok
    }
}

/// Remove a batch of elements; failures leave slot bytes unchanged.
pub fn batch_bitmap_remove(
    env: &mut BitmapEnv,
    hooks: &mut impl VmHooks,
    delta_mode: bool,
    state: &mut [u8],
    meta: &SlotMetaView,
    elem_col: &[u32],
) -> ErrorCode {
    env.last_error = 0;
    env.ttl_mutations.clear();
    let storage = get_bitmap_storage(meta);
    let cardinality = meta.size(state);
    if cardinality == 0 || elem_col.is_empty() {
        return ErrorCode::Ok;
    }
    // Every buffer this removal can fill is backed before it writes.
    if let Err(code) =
        storage_demand(state, storage, 0, count(elem_col)).and_then(|demand| env.reserve_op(demand))
    {
        return code;
    }
    // A column that is already the batch is patched as it is; any other is
    // staged, sorted and deduplicated into the retained buffer.
    let mut removes = core::mem::take(&mut env.removes);
    let batch: &[u32] = if is_batch(elem_col) {
        elem_col
    } else {
        removes.clear();
        removes.extend(
            elem_col
                .iter()
                .copied()
                .filter(|&elem| elem != EMPTY_KEY && elem != TOMBSTONE),
        );
        removes.sort_unstable();
        removes.dedup();
        &removes
    };

    if batch.is_empty() {
        env.removes = removes;
        return ErrorCode::Ok;
    }
    let ttl_capture = if meta.has_ttl() {
        BitmapTtlCapture::Membership
    } else {
        BitmapTtlCapture::Unchanged
    };
    let patched = capture_bitmap_patch(env, hooks, state, meta, storage, &[], batch, ttl_capture);
    let (report, capture, ttl_plan) = match patched {
        Ok(patched) => patched,
        Err(code) => {
            env.removes = removes;
            return code;
        }
    };
    env.removes = removes;
    meta.set_size(state, cardinality - report.removed);
    if report.removed > 0 {
        meta.set_change_flag(state, ChangeFlag::REMOVED);
    }
    finish_bitmap_mutations(env, hooks, delta_mode, state, meta, capture, ttl_plan);
    ErrorCode::Ok
}

/// Set-algebra operation applied to a target bitmap.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BitmapAlgebraOp {
    And,
    Or,
    AndNot,
    Xor,
}

/// The right-hand operand of [`batch_bitmap_algebra`]. Naming the operand's
/// home rather than handing over bytes lets the operation read it in place:
/// a slot operand lives in the same `state` the target is written to, and the
/// scratch operand lives in the `env` whose buffers the store path reuses.
#[derive(Clone, Copy, Debug)]
pub enum BitmapSource<'a> {
    /// Another authoritative BITMAP slot of the same state buffer.
    Slot(u32),
    /// `env.algebra_result` — the image the previous decision-side algebra
    /// left behind.
    Scratch,
    /// A serialized image supplied by the caller.
    Bytes(&'a [u8]),
}

fn verified_bitmap_view(data: &[u8]) -> Result<Option<BitmosaicView<'_>>, ErrorCode> {
    if data.is_empty() {
        return Ok(None);
    }
    let view = BitmosaicView::open_verified(data).ok_or(ErrorCode::InvalidState)?;
    Ok((!view.is_empty()).then_some(view))
}

/// Apply membership algebra and its optional exact-bit expiry join together.
/// Slot operands are bound by index and all views remain borrowed; the pure
/// timestamp join feeds the one existing atomic TTL planner before either
/// payload or index writes. Timestamp-only changes are not membership no-ops.
pub fn batch_bitmap_algebra(
    env: &mut BitmapEnv,
    hooks: &mut impl VmHooks,
    delta_mode: bool,
    op: BitmapAlgebraOp,
    state: &mut [u8],
    target_meta: &SlotMetaView,
    source: BitmapSource<'_>,
) -> ErrorCode {
    env.ttl_mutations.clear();
    let target_storage = get_bitmap_storage(target_meta);
    let original_size = target_meta.size(state);

    // Every buffer the algebra can fill is backed before it writes: it adds
    // at most the source's members, removes at most the target's, and merges
    // at most both sides' live expiry rows. An unreadable operand is refused
    // below, where it is read.
    let source_slot = match source {
        BitmapSource::Slot(slot) => bind_slot(state, slot).ok(),
        BitmapSource::Scratch | BitmapSource::Bytes(_) => None,
    };
    let source_members = match source {
        BitmapSource::Slot(_) => source_slot
            .as_ref()
            .map_or(0, |meta| u64::from(meta.size(state))),
        BitmapSource::Scratch => {
            BitmosaicView::open(&env.algebra_result).map_or(0, |view| view.len())
        }
        BitmapSource::Bytes(data) => BitmosaicView::open(data).map_or(0, |view| view.len()),
    };
    let adds_source = matches!(op, BitmapAlgebraOp::Or | BitmapAlgebraOp::Xor);
    let removes_target = !matches!(op, BitmapAlgebraOp::Or);
    let algebra_demand = storage_demand(
        state,
        target_storage,
        if adds_source { source_members } else { 0 },
        if removes_target {
            u64::from(original_size)
        } else {
            0
        },
    )
    .map(|mut demand| {
        demand.ttl = TtlAlgebraDemand {
            target_rows: expiry_rows(state, target_meta),
            source_rows: source_slot
                .as_ref()
                .map_or(0, |meta| expiry_rows(state, meta)),
        };
        demand
    });
    if let Err(code) = algebra_demand.and_then(|demand| env.reserve_op(demand)) {
        return code;
    }

    let target_data = match target_storage.serialized_data(state) {
        Ok(data) => data,
        Err(code) => {
            env.last_error = 102;
            return code;
        }
    };
    let target_view = match verified_bitmap_view(target_data) {
        Ok(view) => view,
        Err(code) => {
            env.last_error = 102;
            return code;
        }
    };
    let (source_meta, source_data) = match source {
        BitmapSource::Slot(slot) => {
            let meta = match bind_slot(state, slot) {
                Ok(meta) => meta,
                Err(code) => {
                    env.last_error = 80;
                    return code;
                }
            };
            if meta.slot_type() != columine_types::SlotType::Bitmap {
                return ErrorCode::InvalidSlot;
            }
            if meta.has_ttl() && !target_meta.has_ttl() {
                return ErrorCode::InvalidProgram;
            }
            let data = if meta.offset == target_meta.offset && meta.capacity == target_meta.capacity
            {
                target_data
            } else {
                match get_bitmap_storage(&meta).serialized_data(state) {
                    Ok(data) => data,
                    Err(code) => {
                        env.last_error = 80;
                        return code;
                    }
                }
            };
            (Some(meta), data)
        }
        BitmapSource::Scratch => (None, &env.algebra_result[..]),
        BitmapSource::Bytes(data) => (None, data),
    };
    let source_view = if source_meta.as_ref().is_some_and(|meta| {
        meta.meta_base == target_meta.meta_base
            && meta.offset == target_meta.offset
            && meta.capacity == target_meta.capacity
    }) {
        target_view
    } else {
        match verified_bitmap_view(source_data) {
            Ok(view) => view,
            Err(code) => {
                env.last_error = 80;
                return code;
            }
        }
    };
    let expiry_changed = match merge_expiries(
        state,
        target_meta,
        source_meta.as_ref(),
        target_view,
        source_view,
        op,
        &mut env.ttl_algebra_scratch,
    ) {
        Ok(changed) => changed,
        Err(code) => return code,
    };

    let mut adds = core::mem::take(&mut env.adds);
    let mut removes = core::mem::take(&mut env.removes);
    adds.clear();
    removes.clear();
    match (target_view, source_view) {
        (None, None) => {}
        (None, Some(source)) => match op {
            BitmapAlgebraOp::And | BitmapAlgebraOp::AndNot => {}
            BitmapAlgebraOp::Or | BitmapAlgebraOp::Xor => adds.extend(source.range()),
        },
        (Some(target), None) => match op {
            BitmapAlgebraOp::And => removes.extend(target.range()),
            BitmapAlgebraOp::Or | BitmapAlgebraOp::AndNot | BitmapAlgebraOp::Xor => {}
        },
        (Some(target), Some(source)) => match op {
            BitmapAlgebraOp::And => {
                removes.extend(AndNotRange::new(target.range(), source.range()));
            }
            BitmapAlgebraOp::Or => {
                adds.extend(AndNotRange::new(source.range(), target.range()));
            }
            BitmapAlgebraOp::AndNot => {
                removes.extend(AndRange::leapfrog(target.range(), source.range()));
            }
            BitmapAlgebraOp::Xor => {
                adds.extend(AndNotRange::new(source.range(), target.range()));
                removes.extend(AndRange::leapfrog(target.range(), source.range()));
            }
        },
    }

    let membership_changed = !adds.is_empty() || !removes.is_empty();
    if !membership_changed && !expiry_changed {
        env.adds = adds;
        env.removes = removes;
        return ErrorCode::Ok;
    }
    let old_members = target_view.map_or(0, |view| view.len());
    let admitted_size = (|| {
        let added = u64::try_from(adds.len()).map_err(|_| ErrorCode::InvalidState)?;
        let removed = u64::try_from(removes.len()).map_err(|_| ErrorCode::InvalidState)?;
        let result_members = old_members
            .checked_add(added)
            .and_then(|len| len.checked_sub(removed))
            .ok_or(ErrorCode::InvalidState)?;
        if result_members > u64::from(target_meta.capacity) {
            return Err(ErrorCode::CapacityExceeded);
        }
        u32::try_from(result_members).map_err(|_| ErrorCode::CapacityExceeded)
    })();
    let new_card = match admitted_size {
        Ok(size) => size,
        Err(code) => {
            env.adds = adds;
            env.removes = removes;
            return code;
        }
    };
    let ttl_capture = if expiry_changed {
        match hooks.plan_ttl_batch(state, target_meta, env.ttl_algebra_scratch.mutations()) {
            Ok(plan) => BitmapTtlCapture::Prepared(plan),
            Err(code) => {
                env.adds = adds;
                env.removes = removes;
                return code;
            }
        }
    } else {
        BitmapTtlCapture::Unchanged
    };
    let patched = capture_bitmap_patch(
        env,
        hooks,
        state,
        target_meta,
        target_storage,
        &adds,
        &removes,
        ttl_capture,
    );
    env.adds = adds;
    env.removes = removes;
    let (report, capture, ttl_plan) = match patched {
        Ok(patched) => patched,
        Err(code) => return code,
    };
    // The verified set-difference batches prove this count before mutation;
    // the retained kernel report is a consistency check, not new admission.
    debug_assert_eq!(report.len, u64::from(new_card));
    target_meta.set_size(state, new_card);
    if new_card != original_size {
        target_meta.set_change_flag(state, ChangeFlag::SIZE_CHANGED);
    }
    if expiry_changed {
        target_meta.set_change_flag(state, ChangeFlag::UPDATED);
    }
    finish_bitmap_mutations(
        env,
        hooks,
        delta_mode,
        state,
        target_meta,
        capture,
        ttl_plan,
    );
    ErrorCode::Ok
}

/// Replace the slot's set with `input`, a native u32 image that crossed a
/// trust boundary: it is verified in full, its width checked, its
/// cardinality checked against the slot's element capacity, and a sealed
/// Elias-Fano root is re-encoded as the forest the slot keeps. The canonical
/// empty image is the empty set; zero-length bytes are no native encoding
/// and are refused.
pub fn bitmap_import(
    env: &mut BitmapEnv,
    state: &mut [u8],
    meta: &SlotMetaView,
    input: &[u8],
) -> ErrorCode {
    let storage = get_bitmap_storage(meta);
    let payload = storage.payload_offset() as usize;
    let capacity = storage.payload_capacity as usize;
    let Some(header) = image_header(input) else {
        env.last_error = 102;
        return ErrorCode::InvalidState;
    };
    if header.width != KeyWidth::U32 {
        env.last_error = 102;
        return ErrorCode::InvalidState;
    }
    // The header delimits the image: trailing bytes in the caller's buffer
    // are not part of it and must not reach the slot, whose tail is zero by
    // contract.
    let image = &input[..header.encoded_len];
    let Some(view) = BitmosaicView::open_verified(image) else {
        env.last_error = 102;
        return ErrorCode::InvalidState;
    };
    let Ok(card) = u32::try_from(view.len()) else {
        return ErrorCode::CapacityExceeded;
    };
    if card > meta.capacity {
        return ErrorCode::CapacityExceeded;
    }
    let slot = &mut state[payload..payload + capacity];
    let written = if view.is_elias_fano() {
        match Bitmosaic::from_sorted(view.range()).write_forest_into_slice(slot) {
            Ok(written) => written,
            Err(_) => {
                env.last_error = 60;
                return ErrorCode::CapacityExceeded;
            }
        }
    } else {
        if image.len() > capacity {
            env.last_error = 60;
            return ErrorCode::CapacityExceeded;
        }
        slot[..image.len()].copy_from_slice(image);
        image.len()
    };
    slot[written..].fill(0);
    meta.set_size(state, card);
    ErrorCode::Ok
}

// =============================================================================
// Serialized-level queries and set algebra (decision-function side)
// =============================================================================

/// Test whether a serialized bitmap contains `value`. Invalid or empty
/// payloads return false.
pub fn contains_serialized(data: &[u8], value: u32) -> bool {
    BitmosaicView::open(data).is_some_and(|view| view.contains(value))
}

/// Return serialized bitmap cardinality, saturating at `u32::MAX`.
pub fn cardinality_serialized(data: &[u8]) -> u32 {
    BitmosaicView::open(data).map_or(0, |view| u32::try_from(view.len()).unwrap_or(u32::MAX))
}

/// Verify a serialized native image in full and return its cardinality. A
/// `None` result distinguishes malformed bytes from an empty bitmap: only
/// the canonical empty image verifies to zero, zero-length bytes verify to
/// nothing.
pub fn cardinality_validated(data: &[u8]) -> Option<u32> {
    let view = BitmosaicView::open_verified(data)?;
    u32::try_from(view.len()).ok()
}
/// Extract ascending values into `out`, capped at its length; return the count.
pub fn extract_serialized(data: &[u8], out: &mut [u32]) -> u32 {
    let Some(view) = BitmosaicView::open(data) else {
        return 0;
    };
    let mut count = 0usize;
    for (slot, value) in out.iter_mut().zip(view.range()) {
        *slot = value;
        count += 1;
    }
    count as u32
}

/// Test whether two serialized bitmaps intersect: the leapfrog merge stops
/// at the first common member.
pub fn intersects_serialized(left: &[u8], right: &[u8]) -> bool {
    match (BitmosaicView::open(left), BitmosaicView::open(right)) {
        (Some(l), Some(r)) => !AndRange::leapfrog(l.range(), r.range()).empty(),
        _ => false,
    }
}

/// Count the intersection of two serialized bitmaps, saturating at `u32::MAX`.
pub fn intersect_count_serialized(left: &[u8], right: &[u8]) -> u32 {
    match (BitmosaicView::open(left), BitmosaicView::open(right)) {
        (Some(l), Some(r)) => u32::try_from(l.and_len(&r)).unwrap_or(u32::MAX),
        _ => 0,
    }
}

/// Apply set algebra and store the result in `env.algebra_result`, the
/// VM-owned buffer exported by the wasm layer. Zero bytes are the empty
/// scratch result. Either side accepts the decision-side empty literal
/// (zero bytes) or any valid native image, whose empty form reads as the
/// empty set; a nonempty side that opens as nothing is malformed.
pub fn set_algebra(
    env: &mut BitmapEnv,
    op: BitmapAlgebraOp,
    left: &[u8],
    right: &[u8],
) -> ErrorCode {
    env.algebra_result.clear();

    let left_view = if left.is_empty() {
        None
    } else {
        match BitmosaicView::open(left) {
            Some(view) => (!view.is_empty()).then_some(view),
            None => {
                env.last_error = 71;
                return ErrorCode::InvalidState;
            }
        }
    };
    let right_view = if right.is_empty() {
        None
    } else {
        match BitmosaicView::open(right) {
            Some(view) => (!view.is_empty()).then_some(view),
            None => {
                env.last_error = 72;
                return ErrorCode::InvalidState;
            }
        }
    };

    // Empty-set identities — copy the survivor's image directly. A decision-
    // side operand is often a whole slot region, image followed by unused
    // capacity, so the copy is bounded by the image the view parsed: the
    // scratch result is a native image, not a padded region.
    match (left_view, right_view) {
        (None, None) => ErrorCode::Ok,
        (None, Some(r)) => match op {
            BitmapAlgebraOp::And | BitmapAlgebraOp::AndNot => ErrorCode::Ok,
            BitmapAlgebraOp::Or | BitmapAlgebraOp::Xor => {
                env.algebra_result
                    .extend_from_slice(&right[..r.serialized_len()]);
                ErrorCode::Ok
            }
        },
        (Some(l), None) => match op {
            BitmapAlgebraOp::And => ErrorCode::Ok,
            BitmapAlgebraOp::Or | BitmapAlgebraOp::AndNot | BitmapAlgebraOp::Xor => {
                env.algebra_result
                    .extend_from_slice(&left[..l.serialized_len()]);
                ErrorCode::Ok
            }
        },
        (Some(l), Some(r)) => {
            let result = match op {
                BitmapAlgebraOp::And => {
                    Bitmosaic::from_sorted(AndRange::leapfrog(l.range(), r.range()))
                }
                BitmapAlgebraOp::Or => Bitmosaic::from_sorted(OrRange::new(l.range(), r.range())),
                BitmapAlgebraOp::AndNot => {
                    Bitmosaic::from_sorted(AndNotRange::new(l.range(), r.range()))
                }
                BitmapAlgebraOp::Xor => Bitmosaic::from_sorted(XorRange::new(l.range(), r.range())),
            };
            if result.is_empty() {
                return ErrorCode::Ok;
            }
            env.algebra_result.resize(result.forest_len(), 0);
            if result
                .write_forest_into_slice(&mut env.algebra_result)
                .is_err()
            {
                columine_types::die!("forest_len sized the buffer the forest writer refused");
            }
            ErrorCode::Ok
        }
    }
}

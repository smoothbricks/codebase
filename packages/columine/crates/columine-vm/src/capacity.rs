//! Batch capacity admission: before a batch's first write, decide whether it
//! can run out of slot capacity, so a host without a journal never replays a
//! batch over its own partial mutations. A batch the admission cannot clear
//! answers `NEEDS_GROWTH` with the state exactly as it was.
//!
//! Three tiers, each paid only when the one before cannot clear a slot:
//!
//! 1. A bound from the program and the batch's row count, in O(instructions):
//!    every keyed insert can add at most one new key per execution, a list
//!    append one entry per execution, and a bitmap's image is bounded by
//!    [`bitmosaic::patched_len_bound`] over the members it can add or touch.
//!    No row is read.
//! 2. For the slots tier 1 cannot clear, the same walk reading the rows,
//!    in O(rows): a keyed insert counts only rows whose key the slot does not
//!    already hold, so a batch of updates costs nothing; struct-map array
//!    arenas and list appends count their exact executions, and nested
//!    containers their exact arena demand ([`crate::nested::batch_demand`]).
//! 3. For anything still open (bitmap algebra past its bound, counts folds,
//!    scatters routed by stored rows), the batch runs once on a retained copy
//!    of the state with the journal off. The VM is deterministic, so that run
//!    answers exactly; there is no second model of any opcode's capacity rule.
//!
//! Every bound is an upper bound on what execution can admit: removals and
//! updates are never credited, and a key absent before the batch is counted
//! once per row that names it.

use crate::bitmap_ops::{BitmapEnv, get_bitmap_storage};
use crate::element_body::{OperandSink, envelope_body, walk_operands};
use crate::hash_table::max_load_for_cap;
use crate::hashmap_ops::bind_slot_map;
use crate::hashset_ops::bind_slot_set;
use crate::meta::SlotMetaView;
use crate::scratch::ScratchDemand;
use crate::state_init;
use crate::struct_map::{ArenaRegion, StructMap2Slot, StructMapSlot};
use bitmosaic::{BitmosaicView, patched_len_bound};
use columine_types::operand::Operands;
use columine_types::types::{
    EMPTY_KEY, Opcode, SlotType, StructFieldType, TOMBSTONE, arena_elem_size, state_num_slots,
};

/// Container nesting a reduce section may reach; the input planner and the
/// source walker keep the same bound.
const MAX_DEPTH: usize = 64;

/// How each batch was admitted, counted for the life of the VM.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CapacityCounts {
    /// Cleared by the row-count bound.
    pub bound: u64,
    /// Cleared by reading the rows.
    pub probed: u64,
    /// Decided by a dry run that fit.
    pub dry_run_fit: u64,
    /// Refused with `NEEDS_GROWTH` before any write.
    pub refused: u64,
}

/// What admission decided without running the batch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Admission {
    /// No slot can run out of capacity.
    Fits,
    /// Only a dry run can decide.
    DryRun,
    /// Admission's own working memory could not be backed; the batch is
    /// refused before any write.
    OutOfMemory,
}

/// Retained admission storage: no allocation after its high-water mark.
#[derive(Debug, Default)]
pub(crate) struct CapacityScratch {
    demand: Vec<Demand>,
    touched: Vec<u32>,
    operands: Vec<u32>,
    /// Tier 2's `(outer, inner)` keys of one nested slot.
    pairs: Vec<(u32, u32)>,
    /// The tier-3 state copy.
    pub(crate) image: Vec<u8>,
    pub(crate) counts: CapacityCounts,
    /// The working memory the last batch admitted by tiers 1 and 2 needs;
    /// a dry run grows scratch itself.
    pub(crate) scratch: ScratchDemand,
    /// Set when admission could not back its own storage.
    out_of_memory: bool,
}

/// One slot's demand. `units` are new keys, members or entries; `touches`
/// bitmap members a batch can add or remove; `bytes` arena bytes.
#[derive(Clone, Copy, Debug, Default)]
struct Demand {
    units: u64,
    touches: u64,
    bytes: u64,
    /// The part of `units`/`touches` bitmap algebra contributes: it reads
    /// slot cardinalities, not rows, so tier 2 carries it over.
    algebra_units: u64,
    algebra_touches: u64,
    touched: bool,
    /// A nested slot's verdict, which only tier 2 reaches.
    nested: Option<bool>,
    /// Tier 1 has no bound; tier 2 reads the rows.
    rows: bool,
    /// Neither bound applies; only a dry run decides.
    opaque: bool,
}

/// Collects one instruction's operands in wire order; raw spans are skipped.
struct Collect<'a>(&'a mut Vec<u32>);

impl OperandSink for Collect<'_> {
    fn index(&mut self, value: u32) {
        self.0.push(value);
    }

    fn byte(&mut self, value: u8) {
        self.0.push(u32::from(value));
    }

    fn raw(&mut self, _: &[u8]) {}
}

/// The rows one instruction runs over.
#[derive(Clone, Copy)]
struct Domain<'p> {
    /// `FOR_EACH`'s type column and match ids, or the whole batch.
    for_each: Option<(u32, &'p [u8])>,
    /// Offsets columns of the enclosing `FLAT_MAP`s, outermost first.
    flat: [u32; MAX_DEPTH],
    depth: usize,
}

impl Domain<'_> {
    const TOP: Domain<'static> = Domain {
        for_each: None,
        flat: [0; MAX_DEPTH],
        depth: 0,
    };

    fn flat_maps(&self) -> &[u32] {
        &self.flat[..self.depth]
    }
}

/// A capacity effect of one instruction, as admission reads it.
#[derive(Clone, Copy)]
enum Effect {
    /// One new key per execution whose key cell the slot lacks.
    Key { slot: u32, col: u32 },
    /// One new pair per execution.
    Pair { slot: u32, col1: u32, col2: u32 },
    /// One bitmap member per execution leaves (no growth for a hash set).
    Remove { slot: u32, col: u32 },
    /// One list entry per execution.
    Entry { slot: u32 },
    /// One struct-map row per execution plus its array bytes.
    Arena { slot: u32, col: u32 },
    /// Slot-to-slot bitmap algebra: `source` is `None` for the scratch image.
    Algebra {
        target: u32,
        source: Option<u32>,
        adds: bool,
        removes: bool,
    },
    /// Keyed by a route column: route `route` of the instruction names
    /// `slot`, and its key is `col` (a kind-1 route's value column).
    Routed {
        slot: u32,
        route_col: u32,
        route: u32,
        key_col: u32,
        col: u32,
    },
    /// One nested container mutation per execution: `inner_col` is unused
    /// for aggregates.
    Nested {
        slot: u32,
        outer_col: u32,
        inner_col: u32,
        inner: SlotType,
    },
    /// Only a dry run decides this slot.
    Opaque { slot: u32 },
}

impl Effect {
    fn slot(self) -> u32 {
        match self {
            Effect::Key { slot, .. }
            | Effect::Pair { slot, .. }
            | Effect::Remove { slot, .. }
            | Effect::Entry { slot }
            | Effect::Arena { slot, .. }
            | Effect::Routed { slot, .. }
            | Effect::Nested { slot, .. }
            | Effect::Opaque { slot } => slot,
            Effect::Algebra { target, .. } => target,
        }
    }
}

fn cell(cols: &[&[u8]], col: u32, row: u32) -> Option<u32> {
    let at = usize::try_from(row).ok()?.checked_mul(4)?;
    let bytes = cols
        .get(usize::try_from(col).ok()?)?
        .get(at..at.checked_add(4)?)?;
    Some(u32::from_le_bytes(bytes.try_into().ok()?))
}

fn cells(cols: &[&[u8]], col: u32) -> u64 {
    usize::try_from(col)
        .ok()
        .and_then(|col| cols.get(col))
        .and_then(|col| u64::try_from(col.len() / 4).ok())
        .unwrap_or(0)
}

fn is_sentinel(key: u32) -> bool {
    key == EMPTY_KEY || key == TOMBSTONE
}

/// Decode the capacity effects of one instruction into `out`.
fn effects(op: Opcode, o: &[u32], out: &mut impl FnMut(Effect)) -> Option<()> {
    let at = |i: usize| o.get(i).copied();
    match op {
        Opcode::BatchMapUpsertLatest
        | Opcode::BatchMapUpsertLatestTtl
        | Opcode::BatchMapUpsertFirst
        | Opcode::BatchMapUpsertLast
        | Opcode::BatchMapUpsertLastTtl
        | Opcode::BatchMapUpsertMax
        | Opcode::BatchMapUpsertMin
        | Opcode::BatchMapUpsertLatestIf
        | Opcode::BatchMapUpsertFirstIf
        | Opcode::BatchMapUpsertLastIf
        | Opcode::BatchMapUpsertMaxIf
        | Opcode::BatchMapUpsertMinIf
        | Opcode::BatchSetInsert
        | Opcode::BatchSetInsertTtl
        | Opcode::BatchSetInsertIf
        | Opcode::BatchBitmapAdd => out(Effect::Key {
            slot: at(0)?,
            col: at(1)?,
        }),
        Opcode::BatchSetRemove | Opcode::BatchBitmapRemove => out(Effect::Remove {
            slot: at(0)?,
            col: at(1)?,
        }),
        Opcode::BatchBitmapAnd
        | Opcode::BatchBitmapOr
        | Opcode::BatchBitmapAndNot
        | Opcode::BatchBitmapXor => out(Effect::Algebra {
            target: at(0)?,
            source: Some(at(1)?),
            adds: matches!(op, Opcode::BatchBitmapOr | Opcode::BatchBitmapXor),
            removes: !matches!(op, Opcode::BatchBitmapOr),
        }),
        Opcode::BatchBitmapAndScratch
        | Opcode::BatchBitmapOrScratch
        | Opcode::BatchBitmapAndNotScratch
        | Opcode::BatchBitmapXorScratch => out(Effect::Algebra {
            target: at(0)?,
            source: None,
            adds: matches!(
                op,
                Opcode::BatchBitmapOrScratch | Opcode::BatchBitmapXorScratch
            ),
            removes: !matches!(op, Opcode::BatchBitmapOrScratch),
        }),
        Opcode::BatchStructMapUpsertLast
        | Opcode::BatchStructMapUpsertFirst
        | Opcode::BatchStructMapUpsertMax => {
            // slot, key, num_vals, (col, field) × num_vals, num_arrays, ...
            let (slot, col) = (at(0)?, at(1)?);
            let num_vals = usize::try_from(at(2)?).ok()?;
            let num_arrays = at(3 + 2 * num_vals)?;
            if num_arrays == 0 {
                out(Effect::Key { slot, col });
            } else {
                out(Effect::Arena { slot, col });
            }
        }
        // Init frames charged for absent rows and clear fields alike.
        Opcode::BatchStructMapCountsFold => out(Effect::Opaque { slot: at(0)? }),
        Opcode::BatchStructMap2UpsertLast | Opcode::BatchStructMap2UpsertMaxI64x2 => {
            out(Effect::Pair {
                slot: at(0)?,
                col1: at(1)?,
                col2: at(2)?,
            });
        }
        // probe_slot, key, miss_mode, out_slot, num_fields, out_key.
        Opcode::BatchStructMapProbe => out(Effect::Key {
            slot: at(3)?,
            col: at(5)?,
        }),
        // Routes and keys come from the probed row: state, not columns.
        Opcode::BatchStructMapProbeScatter => {
            let routes = usize::try_from(at(2)?).ok()?;
            for route in 0..routes {
                out(Effect::Opaque {
                    slot: at(4 + 2 * route)?,
                });
            }
        }
        // route_col, op_col, key_col, num_routes, (kind, dest, field, v_col) ×.
        Opcode::BatchStructMapScatter => {
            routed(o, 3, at(0)?, at(2)?, out)?;
        }
        // route_col, op_col, key_col, guard_slot, num_guards, (col, width) ×,
        // num_routes, routes. Every assert plans its guard row.
        Opcode::BatchStructMapScatterGuarded => {
            let guards = usize::try_from(at(4)?).ok()?;
            out(Effect::Key {
                slot: at(3)?,
                col: at(2)?,
            });
            routed(o, 5 + 2 * guards, at(0)?, at(2)?, out)?;
        }
        Opcode::ListAppend | Opcode::ListAppendStruct => out(Effect::Entry { slot: at(0)? }),
        Opcode::NestedSetInsert | Opcode::NestedMapUpsertLast | Opcode::NestedAggUpdate => {
            out(Effect::Nested {
                slot: at(0)?,
                outer_col: at(1)?,
                inner_col: at(2)?,
                inner: match op {
                    Opcode::NestedSetInsert => SlotType::HashSet,
                    Opcode::NestedMapUpsertLast => SlotType::HashMap,
                    _ => SlotType::Aggregate,
                },
            });
        }
        _ => {}
    }
    Some(())
}

/// The routes of a scatter table starting at operand `at`.
fn routed(
    o: &[u32],
    at: usize,
    route_col: u32,
    key_col: u32,
    out: &mut impl FnMut(Effect),
) -> Option<()> {
    let routes = usize::try_from(*o.get(at)?).ok()?;
    for route in 0..routes {
        let base = at + 1 + 4 * route;
        let kind = *o.get(base)?;
        let slot = *o.get(base + 1)?;
        let v_col = *o.get(base + 3)?;
        out(Effect::Routed {
            slot,
            route_col,
            route: u32::try_from(route).ok()?,
            key_col,
            col: if kind == 0 { key_col } else { v_col },
        });
    }
    Some(())
}

/// Visit every instruction of `code` with the domain it runs over.
fn walk<'p>(
    code: &'p [u8],
    domain: Domain<'p>,
    operands: &mut Vec<u32>,
    visit: &mut impl FnMut(Opcode, &[u32], &Domain<'p>) -> Option<()>,
) -> Option<()> {
    let mut pc = 0;
    while pc < code.len() {
        let op = Opcode::from_u8(code[pc])?;
        if op == Opcode::Halt {
            return Some(());
        }
        match op {
            Opcode::ForEach | Opcode::FlatMap => {
                let mut inner = domain;
                let mut header = Operands::at(code, pc + 1);
                if op == Opcode::ForEach {
                    let type_col = header.index()?;
                    let count = usize::try_from(header.index()?).ok()?;
                    let ids = header.bytes(count.checked_mul(4)?)?;
                    inner.for_each = Some((type_col, ids));
                } else {
                    if inner.depth == MAX_DEPTH {
                        return None;
                    }
                    inner.flat[inner.depth] = header.index()?;
                    inner.depth += 1;
                }
                let (body_at, body) = envelope_body(op, code, pc + 1)?;
                walk(body, inner, operands, visit)?;
                pc = body_at.checked_add(body.len())?;
            }
            _ => {
                let mut r = Operands::at(code, pc + 1);
                operands.clear();
                walk_operands(op, &mut r, &mut Collect(operands))?;
                visit(op, operands, &domain)?;
                pc = r.pos();
            }
        }
    }
    Some(())
}

/// Call `f` for every element `domain` runs an instruction over, as
/// execution enumerates them.
fn for_rows(
    domain: &Domain<'_>,
    cols: &[&[u8]],
    batch_len: u32,
    f: &mut impl FnMut(u32) -> Option<()>,
) -> Option<()> {
    for row in 0..batch_len {
        if let Some((type_col, ids)) = domain.for_each {
            let value = cell(cols, type_col, row)?;
            if !ids
                .as_chunks::<4>()
                .0
                .iter()
                .any(|id| u32::from_le_bytes(*id) == value)
            {
                continue;
            }
        }
        descend(domain.flat_maps(), cols, row, f)?;
    }
    Some(())
}

fn descend(
    flat: &[u32],
    cols: &[&[u8]],
    element: u32,
    f: &mut impl FnMut(u32) -> Option<()>,
) -> Option<()> {
    let Some((&offsets, inner)) = flat.split_first() else {
        return f(element);
    };
    let start = cell(cols, offsets, element)?;
    let end = cell(cols, offsets, element.checked_add(1)?)?;
    for child in start..end {
        descend(inner, cols, child, f)?;
    }
    Some(())
}

impl CapacityScratch {
    /// Tiers 1 and 2. A program or state admission cannot read is left to a
    /// dry run, which reports whatever execution would.
    pub(crate) fn admit(
        &mut self,
        state: &[u8],
        code: &[u8],
        cols: &[&[u8]],
        batch_len: u32,
        env: &BitmapEnv,
    ) -> Admission {
        self.out_of_memory = false;
        match self.admit_inner(state, code, cols, batch_len, env) {
            Some(admission) => admission,
            None if self.out_of_memory => Admission::OutOfMemory,
            None => Admission::DryRun,
        }
    }

    /// Back `additional` more elements of `buf`; a failure is remembered so
    /// the batch is refused as out of memory, not left to a dry run.
    fn room<T>(out_of_memory: &mut bool, buf: &mut Vec<T>, additional: usize) -> Option<()> {
        if buf.try_reserve(additional).is_err() {
            *out_of_memory = true;
            return None;
        }
        Some(())
    }

    fn admit_inner(
        &mut self,
        state: &[u8],
        code: &[u8],
        cols: &[&[u8]],
        batch_len: u32,
        env: &BitmapEnv,
    ) -> Option<Admission> {
        let slots = state_num_slots(state);
        for &slot in &self.touched {
            self.demand[usize::try_from(slot).ok()?] = Demand::default();
        }
        self.touched.clear();
        let slot_count = usize::try_from(slots).ok()?;
        if self.demand.len() < slot_count {
            let have = self.demand.len();
            Self::room(&mut self.out_of_memory, &mut self.demand, slot_count - have)?;
            self.demand.resize(slot_count, Demand::default());
        }
        // Every slot is touched at most once, and one instruction has fewer
        // operands than the program has bytes.
        Self::room(&mut self.out_of_memory, &mut self.touched, slot_count)?;
        let operands_len = self.operands.len();
        Self::room(
            &mut self.out_of_memory,
            &mut self.operands,
            code.len().saturating_sub(operands_len),
        )?;

        // Tier 1: the row-count bound, in program order.
        let mut operands = core::mem::take(&mut self.operands);
        let tier1 = Tier1 {
            state,
            cols,
            batch_len,
            slots,
            scratch_card: scratch_members(env),
        };
        let demand = &mut self.demand;
        let touched = &mut self.touched;
        let walked = walk(code, Domain::TOP, &mut operands, &mut |op, o, domain| {
            let mut ok = Some(());
            effects(op, o, &mut |effect| {
                if ok.is_some() {
                    ok = tier1.account(effect, domain, demand, touched);
                }
            });
            ok
        });
        self.operands = operands;
        walked?;

        // Working memory: a TTL slot's planner storage follows its rows. A
        // TTL slot whose rows have no row-free bound is left to the dry run,
        // which grows that storage before the batch's first write.
        self.scratch = ScratchDemand::default();
        for index in 0..self.touched.len() {
            let slot = self.touched[index];
            let d = self.demand[usize::try_from(slot).ok()?];
            let meta = slot_meta(state, slot)?;
            if meta.has_ttl() {
                if d.rows || d.opaque {
                    return Some(Admission::DryRun);
                }
                self.scratch.ttl.widen(crate::ttl_plan::plan_demand(
                    state,
                    &meta,
                    d.units.max(d.touches),
                ));
            }
        }

        let mut cleared = true;
        for index in 0..self.touched.len() {
            let slot = self.touched[index];
            let d = self.demand[usize::try_from(slot).ok()?];
            if d.opaque {
                return Some(Admission::DryRun);
            }
            if d.rows || !fits(state, slot, &d)? {
                cleared = false;
            }
        }
        if cleared {
            self.counts.bound += 1;
            return Some(Admission::Fits);
        }

        // Tier 2: read the rows for every slot tier 1 left open.
        for index in 0..self.touched.len() {
            let slot = self.touched[index];
            let d = self.demand[usize::try_from(slot).ok()?];
            if !d.rows && fits(state, slot, &d)? {
                continue;
            }
            let probed = self.probe(state, code, cols, batch_len, slot, d)?;
            if probed.opaque || !fits(state, slot, &probed)? {
                return Some(Admission::DryRun);
            }
        }
        self.counts.probed += 1;
        Some(Admission::Fits)
    }

    /// Recount `target`'s demand from the rows. Algebra bounds carry over
    /// from tier 1: they read slot cardinalities, not rows.
    fn probe(
        &mut self,
        state: &[u8],
        code: &[u8],
        cols: &[&[u8]],
        batch_len: u32,
        target: u32,
        tier1: Demand,
    ) -> Option<Demand> {
        let meta = slot_meta(state, target)?;
        let lookup = Lookup::bind(state, target, &meta)?;
        let mut d = Demand {
            touched: true,
            ..Demand::default()
        };
        let mut operands = core::mem::take(&mut self.operands);
        let mut pairs = core::mem::take(&mut self.pairs);
        pairs.clear();
        let mut out_of_memory = false;
        let mut nested_type = None;
        let walked = walk(code, Domain::TOP, &mut operands, &mut |op, o, domain| {
            let mut ok = Some(());
            effects(op, o, &mut |effect| {
                if ok.is_none() {
                    return;
                }
                ok = match effect {
                    Effect::Key { slot, col } if slot == target => {
                        for_rows(domain, cols, batch_len, &mut |row| {
                            let key = cell(cols, col, row)?;
                            if !is_sentinel(key) && !lookup.contains(state, key) {
                                d.units += 1;
                                d.touches += 1;
                            }
                            Some(())
                        })
                    }
                    Effect::Pair { slot, col1, col2 } if slot == target => {
                        for_rows(domain, cols, batch_len, &mut |row| {
                            let (k1, k2) = (cell(cols, col1, row)?, cell(cols, col2, row)?);
                            if !lookup.contains_pair(state, k1, k2) {
                                d.units += 1;
                            }
                            Some(())
                        })
                    }
                    // Present members only: the same deliberate touch margin
                    // as tier 1.
                    Effect::Remove { slot, col } if slot == target => {
                        for_rows(domain, cols, batch_len, &mut |row| {
                            if lookup.contains(state, cell(cols, col, row)?) {
                                d.touches += 1;
                            }
                            Some(())
                        })
                    }
                    Effect::Entry { slot } if slot == target => {
                        for_rows(domain, cols, batch_len, &mut |_| {
                            d.units += 1;
                            Some(())
                        })
                    }
                    Effect::Arena { slot, col } if slot == target => {
                        arena_demand(state, &lookup, o, domain, cols, batch_len, col, &mut d)
                    }
                    Effect::Routed {
                        slot,
                        route_col,
                        route,
                        key_col,
                        col,
                    } if slot == target => for_rows(domain, cols, batch_len, &mut |row| {
                        // Execution skips sentinel keys and SKIP or
                        // out-of-range routes before choosing a slot.
                        if is_sentinel(cell(cols, key_col, row)?)
                            || cell(cols, route_col, row)? != route
                        {
                            return Some(());
                        }
                        let key = cell(cols, col, row)?;
                        if !is_sentinel(key) && !lookup.contains(state, key) {
                            d.units += 1;
                            d.touches += 1;
                        }
                        Some(())
                    }),
                    Effect::Nested {
                        slot,
                        outer_col,
                        inner_col,
                        inner,
                    } if slot == target => {
                        if nested_type.is_some_and(|ty| ty != inner) {
                            d.opaque = true;
                        }
                        nested_type = Some(inner);
                        for_rows(domain, cols, batch_len, &mut |row| {
                            let outer = cell(cols, outer_col, row)?;
                            let key = if inner == SlotType::Aggregate {
                                0
                            } else {
                                cell(cols, inner_col, row)?
                            };
                            Self::room(&mut out_of_memory, &mut pairs, 1)?;
                            pairs.push((outer, key));
                            Some(())
                        })
                    }
                    Effect::Opaque { slot } if slot == target => {
                        d.opaque = true;
                        Some(())
                    }
                    _ => Some(()),
                };
            });
            ok
        });
        self.operands = operands;
        let nested = match (walked, nested_type) {
            (Some(()), Some(inner)) if !d.opaque => {
                crate::nested::batch_demand(state, &meta, inner, &mut pairs)
            }
            _ => None,
        };
        self.pairs = pairs;
        if out_of_memory {
            self.out_of_memory = true;
            return None;
        }
        walked?;
        if nested_type.is_some() {
            match nested {
                Some(nested) => {
                    self.scratch
                        .nested
                        .get_or_insert_default()
                        .widen(nested.rehash);
                    d.units = nested.outer_keys;
                    d.bytes = nested.arena_bytes;
                    d.nested = Some(nested.fits);
                }
                None => d.opaque = true,
            }
        }
        d.units = d.units.saturating_add(tier1.algebra_units);
        d.touches = d.touches.saturating_add(tier1.algebra_touches);
        Some(d)
    }
}

fn mark(touched: &mut Vec<u32>, d: &mut Demand, slot: u32) {
    if !d.touched {
        d.touched = true;
        touched.push(slot);
    }
}

/// Members of the decision-side scratch image a scratch algebra reads.
fn scratch_members(env: &BitmapEnv) -> Option<u64> {
    let image = env.algebra_result();
    if image.is_empty() {
        return Some(0);
    }
    BitmosaicView::open(image).map(|view| view.len())
}

/// `slot`'s metadata as execution reads it. The state's geometry was
/// checked when it was bound; `state_init::bind_slot` would re-check it, and
/// for a nested slot that walks the whole outer table on every batch.
fn slot_meta(state: &[u8], slot: u32) -> Option<SlotMetaView> {
    (slot < state_num_slots(state)).then(|| SlotMetaView::read(state, slot))
}

/// Whether `d` fits `slot` as the slot is now. A TTL slot's eviction index
/// holds one entry per live key, so its new keys must fit there too.
fn fits(state: &[u8], slot: u32, d: &Demand) -> Option<bool> {
    let meta = slot_meta(state, slot)?;
    if meta.has_ttl()
        && u64::from(meta.eviction_index_size(state)) + d.units
            > u64::from(meta.eviction_index_capacity(state))
    {
        return Some(false);
    }
    Some(match meta.slot_type() {
        SlotType::HashMap => {
            let table = bind_slot_map(&meta);
            u64::from(table.size(state)) + d.units <= u64::from(table.max_load())
        }
        SlotType::HashSet => {
            let table = bind_slot_set(&meta);
            u64::from(table.size(state)) + d.units <= u64::from(table.max_load())
        }
        SlotType::StructMap => {
            let map = state_init::bind_struct_map(state, slot).ok()?;
            let rows =
                u64::from(map.size(state)) + d.units <= u64::from(max_load_for_cap(map.capacity));
            let arena = if d.bytes == 0 {
                true
            } else {
                let arena = ArenaRegion::bind(state, map.arena_header_off).ok()?;
                u64::from(arena.used) + d.bytes <= u64::from(arena.capacity)
            };
            rows && arena
        }
        SlotType::StructMap2 => {
            let map = StructMap2Slot::bind(state, slot);
            u64::from(map.size(state)) + d.units <= u64::from(max_load_for_cap(map.capacity))
        }
        SlotType::Bitmap => {
            let storage = get_bitmap_storage(&meta);
            let image = storage.serialized_data(state).ok()?;
            u64::from(meta.size(state)) + d.units <= u64::from(meta.capacity)
                && patched_len_bound(image, d.units, d.touches)?
                    <= u64::from(storage.payload_capacity)
        }
        SlotType::Nested => d.nested == Some(true),
        SlotType::OrderedList => u64::from(meta.size(state)) + d.units <= u64::from(meta.capacity),
        // A capacity effect on any other slot kind refuses at execution as
        // a program error, never as growth.
        _ => true,
    })
}

/// Read-only membership of the slot tier 2 counts against.
enum Lookup<'s> {
    Table(crate::hash_table::FlatTable),
    Rows(StructMapSlot),
    Pairs(StructMap2Slot),
    Bitmap(Option<BitmosaicView<'s>>),
    /// Membership is irrelevant (lists count executions).
    None,
}

impl<'s> Lookup<'s> {
    fn bind(state: &'s [u8], slot: u32, meta: &SlotMetaView) -> Option<Self> {
        Some(match meta.slot_type() {
            SlotType::HashMap => Lookup::Table(bind_slot_map(meta)),
            SlotType::HashSet => Lookup::Table(bind_slot_set(meta)),
            SlotType::StructMap => Lookup::Rows(state_init::bind_struct_map(state, slot).ok()?),
            SlotType::StructMap2 => Lookup::Pairs(StructMap2Slot::bind(state, slot)),
            SlotType::Bitmap => Lookup::Bitmap(get_bitmap_storage(meta).view(state).ok()?),
            _ => Lookup::None,
        })
    }

    fn contains(&self, state: &[u8], key: u32) -> bool {
        match self {
            Lookup::Table(table) => table.contains(state, key),
            Lookup::Rows(map) => map.find(state, key).is_some(),
            Lookup::Bitmap(view) => view.as_ref().is_some_and(|view| view.contains(key)),
            Lookup::Pairs(_) | Lookup::None => false,
        }
    }

    fn contains_pair(&self, state: &[u8], key1: u32, key2: u32) -> bool {
        match self {
            Lookup::Pairs(map) => map.find(state, key1, key2).is_some(),
            _ => false,
        }
    }
}

/// Rows and exact array bytes of one struct-map upsert. Bytes are charged on
/// every execution with a non-sentinel key, update or insert, unaligned.
#[allow(clippy::too_many_arguments)]
fn arena_demand(
    state: &[u8],
    lookup: &Lookup<'_>,
    o: &[u32],
    domain: &Domain<'_>,
    cols: &[&[u8]],
    batch_len: u32,
    key_col: u32,
    d: &mut Demand,
) -> Option<()> {
    let Lookup::Rows(map) = lookup else {
        return None;
    };
    let num_vals = usize::try_from(*o.get(2)?).ok()?;
    let arrays_at = 3 + 2 * num_vals;
    let num_arrays = usize::try_from(*o.get(arrays_at)?).ok()?;
    for_rows(domain, cols, batch_len, &mut |row| {
        let key = cell(cols, key_col, row)?;
        if is_sentinel(key) {
            return Some(());
        }
        if map.find(state, key).is_none() {
            d.units += 1;
        }
        for array in 0..num_arrays {
            let base = arrays_at + 1 + 3 * array;
            let (offsets, field) = (*o.get(base)?, *o.get(base + 2)?);
            let kind =
                StructFieldType::from_u8(map.field_type_byte(state, u8::try_from(field).ok()?))?;
            let start = cell(cols, offsets, row)?;
            let end = cell(cols, offsets, row.checked_add(1)?)?;
            let len = end.checked_sub(start)?;
            d.bytes = d
                .bytes
                .saturating_add(u64::from(len) * u64::from(arena_elem_size(kind)));
        }
        Some(())
    })
}

/// The inputs of the row-count bound.
struct Tier1<'a> {
    state: &'a [u8],
    cols: &'a [&'a [u8]],
    batch_len: u32,
    slots: u32,
    scratch_card: Option<u64>,
}

impl Tier1<'_> {
    /// Executions an instruction keyed by `col` can run: every batch row, or
    /// under `FLAT_MAP` the cells of its key column (distinct keys are bounded
    /// by the cells naming them; executions are not). `None` when no
    /// row-free bound exists.
    fn executions(&self, domain: &Domain<'_>, col: Option<u32>) -> Option<u64> {
        if domain.depth == 0 {
            Some(u64::from(self.batch_len))
        } else {
            col.map(|col| cells(self.cols, col))
        }
    }

    /// A bitmap slot's member bound so far in program order; `None` when the
    /// slot is no bitmap or an earlier instruction's adds have no row-free
    /// bound (algebra over it then needs a dry run).
    fn card(&self, demand: &[Demand], slot: u32) -> Option<u64> {
        let meta = state_init::bind_slot(self.state, slot).ok()?;
        let d = &demand[usize::try_from(slot).ok()?];
        (meta.slot_type() == SlotType::Bitmap && !d.rows && !d.opaque)
            .then(|| u64::from(meta.size(self.state)) + d.units)
    }

    fn account(
        &self,
        effect: Effect,
        domain: &Domain<'_>,
        demand: &mut [Demand],
        touched: &mut Vec<u32>,
    ) -> Option<()> {
        let slot = effect.slot();
        if slot >= self.slots {
            return None;
        }
        if let Effect::Algebra {
            target,
            source,
            adds,
            removes,
        } = effect
        {
            let source_card = match source {
                Some(source) => self.card(demand, source),
                None => self.scratch_card,
            };
            let target_card = self.card(demand, target);
            let d = &mut demand[usize::try_from(target).ok()?];
            mark(touched, d, target);
            let (Some(source_card), Some(target_card)) = (source_card, target_card) else {
                // Algebra over a slot that is not a bitmap refuses at
                // execution; the dry run reports it.
                d.opaque = true;
                return Some(());
            };
            let add = if adds { source_card } else { 0 };
            let touch = add.saturating_add(if removes { target_card } else { 0 });
            d.algebra_units = d.algebra_units.saturating_add(add);
            d.algebra_touches = d.algebra_touches.saturating_add(touch);
            d.units = d.units.saturating_add(add);
            d.touches = d.touches.saturating_add(touch);
            return Some(());
        }
        let bound = match effect {
            Effect::Key { col, .. } | Effect::Routed { col, .. } => {
                self.executions(domain, Some(col))
            }
            Effect::Pair { col1, .. } => self.executions(domain, Some(col1)),
            _ => self.executions(domain, None),
        };
        let d = &mut demand[usize::try_from(slot).ok()?];
        mark(touched, d, slot);
        match (effect, bound) {
            (Effect::Opaque { .. }, _) => d.opaque = true,
            // Rehash allocations depend on each container's capacity.
            (Effect::Nested { .. }, _) => d.rows = true,
            // A removal re-encodes its chunk: charged per `patched_len_bound`'s
            // touched-chunk term. Deliberate margin: no test can isolate it,
            // because the bound's two bytes per member already cover every
            // shape tried, but the bound's soundness argument is per term.
            (Effect::Remove { .. }, Some(n)) => d.touches = d.touches.saturating_add(n),
            (Effect::Arena { col, .. }, _) => {
                // Rows are bounded like any key; the array bytes need rows.
                d.rows = true;
                if let Some(n) = self.executions(domain, Some(col)) {
                    d.units = d.units.saturating_add(n);
                }
            }
            (_, Some(n)) => {
                d.units = d.units.saturating_add(n);
                d.touches = d.touches.saturating_add(n);
            }
            (_, None) => d.rows = true,
        }
        Some(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vm::u32s_as_bytes;
    use columine_types::PROGRAM_MAGIC;
    use columine_types::types::ProgramHeader;

    fn bitmap_pair(reduce: &[u8]) -> (Vec<u8>, Vec<u8>) {
        let mut init = Vec::new();
        for slot in 0..2u8 {
            init.extend([Opcode::SlotDef as u8, slot, SlotType::Bitmap as u8, 64, 0]);
        }
        init.push(Opcode::Halt as u8);
        let mut prog = vec![0u8; 32];
        prog.extend(PROGRAM_MAGIC.to_le_bytes());
        prog.extend([2, 0, 0, 0]);
        prog.extend(2u32.to_le_bytes());
        prog.extend(1u32.to_le_bytes());
        prog.extend((init.len() as u32).to_le_bytes());
        prog.extend((reduce.len() as u32).to_le_bytes());
        prog.extend(init);
        prog.extend_from_slice(reduce);
        let size =
            state_init::calculate_state_size(&prog, state_init::DEFAULT_ACCEPTED_PROGRAM_MAGICS)
                .expect("valid program admission");
        let mut state = vec![0u8; size as usize];
        state_init::init_state(
            &mut state,
            &prog,
            state_init::DEFAULT_ACCEPTED_PROGRAM_MAGICS,
        )
        .expect("init");
        (prog, state)
    }

    /// Algebra reads its source's member bound as of its position in the
    /// program: the source's members plus every add an earlier instruction
    /// may make, never the pre-batch cardinality alone.
    #[test]
    fn algebra_source_bound_includes_earlier_adds_in_the_batch() {
        let reduce = [
            Opcode::BatchBitmapAdd as u8,
            1,
            0,
            Opcode::BatchBitmapOr as u8,
            0,
            1,
            Opcode::BatchBitmapXor as u8,
            1,
            0,
        ];
        let (prog, state) = bitmap_pair(&reduce);
        let code = ProgramHeader::sections(&prog).unwrap().reduce_code;
        let values = [5u32, 6, 7];
        let cols = [u32s_as_bytes(&values)];
        let mut scratch = CapacityScratch::default();
        assert_eq!(
            Admission::Fits,
            scratch.admit(&state, code, &cols, 3, &BitmapEnv::default())
        );
        // Slot 1: three adds. Slot 0: OR adds slot 1's bound (3). Slot 1:
        // XOR adds slot 0's bound (3) and touches both bounds.
        assert_eq!(3, scratch.demand[0].units);
        assert_eq!(3 + 3, scratch.demand[1].units);
        assert_eq!(3 + 3 + 3, scratch.demand[1].touches);
    }
}

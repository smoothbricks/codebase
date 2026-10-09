//! The single TTL batch planner: capacity, binary timestamp placement and every
//! physical destination are computed before primary mutation, then the same
//! retained commands are consumed once. A per-key mutation is a one-element
//! batch.
//!
//! Placement is defined by applying the mutations in order to the live index:
//! each removes every entry with its key, and an upsert then inserts at the
//! binary lower bound of `ts < timestamp` over the current sequence. The
//! result is reproduced without that per-mutation scan:
//!
//! - When every original timestamp is non-NaN and non-decreasing and no
//!   upsert carries NaN, every intermediate sequence stays sorted, so a lower
//!   bound is the count of smaller timestamps. The final order is then the
//!   survivors merged with the effective upserts ordered by timestamp, later
//!   upserts before equal earlier ones, every upsert before equal survivors:
//!   O(N log M + M log M).
//! - Otherwise (NaN or disordered timestamps make the bisection path matter)
//!   the same sequence is replayed exactly on an implicit treap: each probe is
//!   one selection, so O((N + M) log² (N + M)) instead of O(N·M).
//!
//! Commands are then derived from the final order alone: survivors move as
//! coalesced runs (left-moving runs ascending, right-moving runs descending,
//! so no run reads bytes another already overwrote) and upserts are written
//! last.

use crate::bytes;
use crate::hooks::TtlMutation;
use crate::meta::SlotMetaView;
use crate::state_init::EVICTION_ENTRY_SIZE;
use columine_types::types::ErrorCode;
use core::cmp::Ordering;

const NIL: u32 = u32::MAX;
/// A `links` value: the original's key is not mutated, so it survives.
const SURVIVOR: u32 = u32::MAX - 1;

/// Retained planner storage: no allocation after its high-water mark.
#[derive(Debug, Default)]
pub(crate) struct TtlBatchScratch {
    generation: u64,
    mutations: Vec<TtlMutation>,
    /// Mutation indices sorted by `(key, index)`.
    by_key: Vec<u32>,
    /// One record per distinct mutated key, ascending by key.
    keys: Vec<KeyState>,
    /// Mutation index → its `keys` record.
    key_of: Vec<u32>,
    /// Per original entry: [`SURVIVOR`], or the next original carrying the
    /// same mutated key ([`NIL`] ends the list).
    links: Vec<u32>,
    /// Effective upserts (mutation indices) for the merge plan.
    inserts: Vec<u32>,
    nodes: Vec<Node>,
    stack: Vec<u32>,
    rng: u32,
    order: Vec<TtlSource>,
    commands: Vec<TtlCommand>,
    right_runs: Vec<TtlCommand>,
    writes: Vec<TtlCommand>,
}

impl TtlBatchScratch {
    /// Invalidate any retained plan.
    pub(crate) fn reset(&mut self) {
        self.order.clear();
        self.commands.clear();
        self.generation = self.generation.wrapping_add(1);
    }
}

#[derive(Clone, Copy, Debug)]
struct KeyState {
    key: u32,
    originals: u32,
    head: u32,
    /// Mutation index of the live upsert of this key, or [`NIL`].
    live: u32,
    seen: bool,
}

/// One entry of the planned order: an original index position, or the
/// upsert of one mutation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TtlSource {
    Original(u32),
    Inserted(u32),
}

#[derive(Clone, Copy, Debug)]
struct Node {
    left: u32,
    right: u32,
    parent: u32,
    size: u32,
    priority: u32,
    source: TtlSource,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum TtlCommand {
    Move {
        source: u32,
        destination: u32,
        len: u32,
    },
    Write {
        position: u32,
        timestamp: f64,
        key: u32,
        value: u32,
    },
}

/// Consuming authority for one preflight TTL batch's retained commands.
#[derive(Debug)]
pub struct TtlBatchPlan {
    generation: u64,
    base: u32,
    initial_size: u32,
    final_size: u32,
    ranges: [(u32, u32); 2],
}

impl TtlBatchPlan {
    pub fn ranges(&self) -> &[(u32, u32); 2] {
        &self.ranges
    }
}

fn entry_ts(state: &[u8], base: u32, i: u32) -> f64 {
    bytes::read_f64(state, base + i * EVICTION_ENTRY_SIZE)
}

fn entry_key(state: &[u8], base: u32, i: u32) -> u32 {
    bytes::read_u32(state, base + i * EVICTION_ENTRY_SIZE + 8)
}

fn mutation_key(mutation: &TtlMutation) -> u32 {
    match *mutation {
        TtlMutation::Upsert { key, .. } | TtlMutation::Remove { key } => key,
    }
}

/// The timestamp of an upsert; a remove is never placed.
fn upsert_ts(mutation: &TtlMutation) -> f64 {
    match *mutation {
        TtlMutation::Upsert { timestamp, .. } => timestamp,
        TtlMutation::Remove { .. } => unreachable!("only upserts are placed"),
    }
}

pub(crate) fn prepare_ttl_batch(
    state: &[u8],
    meta: &SlotMetaView,
    mutations: impl IntoIterator<Item = TtlMutation>,
    scratch: &mut TtlBatchScratch,
) -> Result<TtlBatchPlan, ErrorCode> {
    let validated = crate::hooks::ttl_capture_ranges(state, meta, 0)?;
    let base = meta.eviction_index_offset(state);
    let initial_size = meta.eviction_index_size(state);
    let capacity = meta.eviction_index_capacity(state);
    scratch.reset();
    plan_order(state, base, initial_size, capacity, mutations, scratch)?;
    let (first, last) = emit_commands(scratch);
    let final_size = u32::try_from(scratch.order.len()).map_err(|_| ErrorCode::InvalidState)?;
    let touched = if first == u32::MAX {
        (0, 0)
    } else {
        let offset = base
            .checked_add(
                first
                    .checked_mul(EVICTION_ENTRY_SIZE)
                    .ok_or(ErrorCode::InvalidState)?,
            )
            .ok_or(ErrorCode::InvalidState)?;
        let len = (last - first)
            .checked_mul(EVICTION_ENTRY_SIZE)
            .ok_or(ErrorCode::InvalidState)?;
        if offset
            .checked_add(len)
            .is_none_or(|end| end as usize > state.len())
        {
            return Err(ErrorCode::InvalidState);
        }
        (offset, len)
    };
    Ok(TtlBatchPlan {
        generation: scratch.generation,
        base,
        initial_size,
        final_size,
        ranges: [
            (
                validated[0].0,
                if final_size != initial_size { 4 } else { 0 },
            ),
            touched,
        ],
    })
}

pub(crate) fn consume_ttl_batch(
    state: &mut [u8],
    meta: &SlotMetaView,
    plan: TtlBatchPlan,
    scratch: &mut TtlBatchScratch,
) {
    assert_eq!(
        plan.generation, scratch.generation,
        "TTL batch lost its retained commands"
    );
    assert_eq!(
        meta.eviction_index_size(state),
        plan.initial_size,
        "TTL batch layout changed"
    );
    apply_commands(state, plan.base, &scratch.commands);
    if plan.final_size != plan.initial_size {
        meta.set_eviction_index_size(state, plan.final_size);
    }
    scratch.order.clear();
    scratch.commands.clear();
}

fn apply_commands(state: &mut [u8], base: u32, commands: &[TtlCommand]) {
    for command in commands {
        match *command {
            TtlCommand::Move {
                source,
                destination,
                len,
            } => bytes::copy_within(
                state,
                base + source * EVICTION_ENTRY_SIZE,
                base + destination * EVICTION_ENTRY_SIZE,
                len * EVICTION_ENTRY_SIZE,
            ),
            TtlCommand::Write {
                position,
                timestamp,
                key,
                value,
            } => {
                let at = base + position * EVICTION_ENTRY_SIZE;
                bytes::write_f64(state, at, timestamp);
                bytes::write_u32(state, at + 8, key);
                bytes::write_u32(state, at + 12, value);
            }
        }
    }
}

/// Fill `scratch.order` with the final live index, or refuse with the first
/// capacity failure the in-order application would hit.
fn plan_order(
    state: &[u8],
    base: u32,
    size: u32,
    capacity: u32,
    mutations: impl IntoIterator<Item = TtlMutation>,
    s: &mut TtlBatchScratch,
) -> Result<(), ErrorCode> {
    s.mutations.clear();
    s.mutations.extend(mutations);
    let count = u32::try_from(s.mutations.len()).map_err(|_| ErrorCode::InvalidState)?;

    // Distinct mutated keys, each with the originals that carry it.
    s.by_key.clear();
    s.by_key.extend(0..count);
    let mutations = &s.mutations;
    s.by_key
        .sort_unstable_by_key(|&j| (mutation_key(&mutations[j as usize]), j));
    s.keys.clear();
    s.key_of.clear();
    s.key_of.resize(count as usize, 0);
    for &j in &s.by_key {
        let key = mutation_key(&s.mutations[j as usize]);
        if s.keys.last().is_none_or(|last| last.key != key) {
            s.keys.push(KeyState {
                key,
                originals: 0,
                head: NIL,
                live: NIL,
                seen: false,
            });
        }
        s.key_of[j as usize] = (s.keys.len() - 1) as u32;
    }
    let mut sorted = s
        .mutations
        .iter()
        .all(|m| !matches!(m, TtlMutation::Upsert { timestamp, .. } if timestamp.is_nan()));
    let mut previous = f64::NEG_INFINITY;
    s.links.clear();
    s.links.resize(size as usize, SURVIVOR);
    for i in 0..size {
        let ts = entry_ts(state, base, i);
        sorted &= ts >= previous;
        previous = ts;
        let key = entry_key(state, base, i);
        if let Ok(u) = s.keys.binary_search_by_key(&key, |k| k.key) {
            s.links[i as usize] = s.keys[u].head;
            s.keys[u].head = i;
            s.keys[u].originals += 1;
        }
    }

    // In-order sizes: the first capacity failure is the batch's refusal.
    let mut live_size = size;
    for j in 0..count {
        let k = &mut s.keys[s.key_of[j as usize] as usize];
        if !k.seen {
            k.seen = true;
            live_size -= k.originals;
        }
        if k.live != NIL {
            live_size -= 1;
            k.live = NIL;
        }
        if matches!(s.mutations[j as usize], TtlMutation::Upsert { .. }) {
            if live_size >= capacity {
                return Err(ErrorCode::CapacityExceeded);
            }
            live_size += 1;
            k.live = j;
        }
    }

    s.order.clear();
    if sorted {
        merge_order(state, base, size, s);
    } else {
        replay_order(state, base, size, s);
    }
    debug_assert_eq!(s.order.len(), live_size as usize);
    Ok(())
}

/// Sorted, NaN-free plan: survivors merged with the effective upserts.
fn merge_order(state: &[u8], base: u32, size: u32, s: &mut TtlBatchScratch) {
    s.inserts.clear();
    s.inserts
        .extend(s.keys.iter().filter(|k| k.live != NIL).map(|k| k.live));
    let mutations = &s.mutations;
    s.inserts.sort_unstable_by(|&a, &b| {
        let (ta, tb) = (
            upsert_ts(&mutations[a as usize]),
            upsert_ts(&mutations[b as usize]),
        );
        if ta < tb {
            Ordering::Less
        } else if tb < ta {
            Ordering::Greater
        } else {
            b.cmp(&a)
        }
    });
    let mut next = 0usize;
    for i in 0..size {
        if s.links[i as usize] != SURVIVOR {
            continue;
        }
        let ts = entry_ts(state, base, i);
        while let Some(&j) = s.inserts.get(next) {
            if ts < upsert_ts(&s.mutations[j as usize]) {
                break;
            }
            s.order.push(TtlSource::Inserted(j));
            next += 1;
        }
        s.order.push(TtlSource::Original(i));
    }
    s.order
        .extend(s.inserts[next..].iter().map(|&j| TtlSource::Inserted(j)));
}

/// Exact replay of the in-order application on an implicit treap.
fn replay_order(state: &[u8], base: u32, size: u32, s: &mut TtlBatchScratch) {
    let count = s.mutations.len() as u32;
    for k in &mut s.keys {
        k.seen = false;
        k.live = NIL;
    }
    if s.rng == 0 {
        s.rng = 0x9e37_79b9;
    }
    let mut rope = Rope {
        nodes: &mut s.nodes,
        rng: &mut s.rng,
        root: NIL,
    };
    rope.nodes.clear();
    for i in 0..size {
        rope.push_node(TtlSource::Original(i));
    }
    for _ in 0..count {
        rope.push_node(TtlSource::Original(0));
    }
    rope.build(size, &mut s.stack);

    for j in 0..count {
        let k = s.key_of[j as usize] as usize;
        if !s.keys[k].seen {
            s.keys[k].seen = true;
            let mut original = s.keys[k].head;
            while original != NIL {
                rope.erase(original);
                original = s.links[original as usize];
            }
        }
        if s.keys[k].live != NIL {
            rope.erase(size + s.keys[k].live);
            s.keys[k].live = NIL;
        }
        if let TtlMutation::Upsert { timestamp, .. } = s.mutations[j as usize] {
            let (mut left, mut right) = (0u32, rope.len());
            while left < right {
                let middle = left + (right - left) / 2;
                let probe = match rope.nodes[rope.select(middle) as usize].source {
                    TtlSource::Original(i) => entry_ts(state, base, i),
                    TtlSource::Inserted(m) => upsert_ts(&s.mutations[m as usize]),
                };
                if probe < timestamp {
                    left = middle + 1;
                } else {
                    right = middle;
                }
            }
            rope.insert(left, size + j, TtlSource::Inserted(j));
            s.keys[k].live = j;
        }
    }
    rope.in_order(&mut s.stack, &mut s.order);
}

/// Derive physical commands from `s.order`; returns the touched entry range
/// `[first, last)` (`first == u32::MAX` when nothing changes).
fn emit_commands(s: &mut TtlBatchScratch) -> (u32, u32) {
    s.commands.clear();
    s.right_runs.clear();
    s.writes.clear();
    let mut first = u32::MAX;
    let mut last = 0u32;
    let mut open: Option<(u32, u32, u32)> = None;
    // Left-moving runs keep their ascending order; right-moving runs are
    // reversed after the scan.
    let flush = |run: Option<(u32, u32, u32)>, s: &mut TtlBatchScratch| {
        if let Some((source, destination, len)) = run {
            let command = TtlCommand::Move {
                source,
                destination,
                len,
            };
            if source > destination {
                s.commands.push(command);
            } else {
                s.right_runs.push(command);
            }
        }
    };
    for position in 0..s.order.len() as u32 {
        match s.order[position as usize] {
            TtlSource::Original(source) if source == position => {
                flush(open.take(), s);
            }
            TtlSource::Original(source) => {
                first = first.min(position);
                last = last.max(position + 1);
                match open {
                    Some((start, destination, len)) if start + len == source => {
                        open = Some((start, destination, len + 1));
                    }
                    run => {
                        flush(run, s);
                        open = Some((source, position, 1));
                    }
                }
            }
            TtlSource::Inserted(j) => {
                flush(open.take(), s);
                first = first.min(position);
                last = last.max(position + 1);
                if let TtlMutation::Upsert {
                    key,
                    timestamp,
                    mapped_value,
                } = s.mutations[j as usize]
                {
                    s.writes.push(TtlCommand::Write {
                        position,
                        timestamp,
                        key,
                        value: mapped_value,
                    });
                }
            }
        }
    }
    flush(open.take(), s);
    s.commands.extend(s.right_runs.drain(..).rev());
    s.commands.append(&mut s.writes);
    (first, last)
}

/// Implicit treap over node indices; in-order position is sequence position.
struct Rope<'a> {
    nodes: &'a mut Vec<Node>,
    rng: &'a mut u32,
    root: u32,
}

impl Rope<'_> {
    fn next_priority(&mut self) -> u32 {
        let mut x = *self.rng;
        x ^= x << 13;
        x ^= x >> 17;
        x ^= x << 5;
        *self.rng = x;
        x
    }

    fn push_node(&mut self, source: TtlSource) {
        let priority = self.next_priority();
        self.nodes.push(Node {
            left: NIL,
            right: NIL,
            parent: NIL,
            size: 1,
            priority,
            source,
        });
    }

    fn size(&self, n: u32) -> u32 {
        if n == NIL {
            0
        } else {
            self.nodes[n as usize].size
        }
    }

    fn len(&self) -> u32 {
        self.size(self.root)
    }

    fn update(&mut self, n: u32) {
        let (left, right) = (self.nodes[n as usize].left, self.nodes[n as usize].right);
        self.nodes[n as usize].size = 1 + self.size(left) + self.size(right);
        for child in [left, right] {
            if child != NIL {
                self.nodes[child as usize].parent = n;
            }
        }
    }

    /// Cartesian build over nodes `0..count` in sequence order.
    fn build(&mut self, count: u32, stack: &mut Vec<u32>) {
        stack.clear();
        for n in 0..count {
            let mut last = NIL;
            while let Some(&top) = stack.last()
                && self.nodes[top as usize].priority < self.nodes[n as usize].priority
            {
                stack.pop();
                self.update(top);
                last = top;
            }
            self.nodes[n as usize].left = last;
            if let Some(&top) = stack.last() {
                self.nodes[top as usize].right = n;
            }
            stack.push(n);
        }
        while let Some(top) = stack.pop() {
            self.update(top);
        }
        self.root = if count == 0 { NIL } else { self.top_of(0) };
        if self.root != NIL {
            self.nodes[self.root as usize].parent = NIL;
        }
    }

    fn top_of(&self, mut n: u32) -> u32 {
        while self.nodes[n as usize].parent != NIL {
            n = self.nodes[n as usize].parent;
        }
        n
    }

    /// `(first k, rest)`.
    fn split(&mut self, t: u32, k: u32) -> (u32, u32) {
        if t == NIL {
            return (NIL, NIL);
        }
        let left = self.nodes[t as usize].left;
        let left_size = self.size(left);
        if k <= left_size {
            let (a, b) = self.split(left, k);
            self.nodes[t as usize].left = b;
            self.update(t);
            if a != NIL {
                self.nodes[a as usize].parent = NIL;
            }
            (a, t)
        } else {
            let right = self.nodes[t as usize].right;
            let (a, b) = self.split(right, k - left_size - 1);
            self.nodes[t as usize].right = a;
            self.update(t);
            if b != NIL {
                self.nodes[b as usize].parent = NIL;
            }
            (t, b)
        }
    }

    fn merge(&mut self, a: u32, b: u32) -> u32 {
        if a == NIL {
            return b;
        }
        if b == NIL {
            return a;
        }
        if self.nodes[a as usize].priority > self.nodes[b as usize].priority {
            let right = self.nodes[a as usize].right;
            self.nodes[a as usize].right = self.merge(right, b);
            self.update(a);
            a
        } else {
            let left = self.nodes[b as usize].left;
            self.nodes[b as usize].left = self.merge(a, left);
            self.update(b);
            b
        }
    }

    fn set_root(&mut self, root: u32) {
        self.root = root;
        if root != NIL {
            self.nodes[root as usize].parent = NIL;
        }
    }

    fn select(&self, mut k: u32) -> u32 {
        let mut n = self.root;
        loop {
            let left = self.nodes[n as usize].left;
            let left_size = self.size(left);
            match k.cmp(&left_size) {
                Ordering::Less => n = left,
                Ordering::Equal => return n,
                Ordering::Greater => {
                    k -= left_size + 1;
                    n = self.nodes[n as usize].right;
                }
            }
        }
    }

    fn rank(&self, mut n: u32) -> u32 {
        let mut rank = self.size(self.nodes[n as usize].left);
        while self.nodes[n as usize].parent != NIL {
            let parent = self.nodes[n as usize].parent;
            if self.nodes[parent as usize].right == n {
                rank += self.size(self.nodes[parent as usize].left) + 1;
            }
            n = parent;
        }
        rank
    }

    fn erase(&mut self, n: u32) {
        let rank = self.rank(n);
        let (a, b) = self.split(self.root, rank);
        let (_, c) = self.split(b, 1);
        let root = self.merge(a, c);
        self.set_root(root);
    }

    fn insert(&mut self, position: u32, n: u32, source: TtlSource) {
        let priority = self.next_priority();
        self.nodes[n as usize] = Node {
            left: NIL,
            right: NIL,
            parent: NIL,
            size: 1,
            priority,
            source,
        };
        let (a, b) = self.split(self.root, position);
        let front = self.merge(a, n);
        let root = self.merge(front, b);
        self.set_root(root);
    }

    fn in_order(&self, stack: &mut Vec<u32>, out: &mut Vec<TtlSource>) {
        stack.clear();
        let mut n = self.root;
        while n != NIL || !stack.is_empty() {
            while n != NIL {
                stack.push(n);
                n = self.nodes[n as usize].left;
            }
            let Some(top) = stack.pop() else { break };
            out.push(self.nodes[top as usize].source);
            n = self.nodes[top as usize].right;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    /// The previous planner's placement, kept as the differential oracle: each
    /// mutation scans the whole live sequence to remove its key, then an
    /// upsert is inserted at its binary lower bound.
    fn oracle(
        state: &[u8],
        size: u32,
        capacity: u32,
        mutations: &[TtlMutation],
    ) -> Result<Vec<TtlSource>, ErrorCode> {
        let mut order: Vec<TtlSource> = (0..size).map(TtlSource::Original).collect();
        let key = |source: TtlSource| match source {
            TtlSource::Original(i) => entry_key(state, 0, i),
            TtlSource::Inserted(j) => mutation_key(&mutations[j as usize]),
        };
        let ts = |source: TtlSource| match source {
            TtlSource::Original(i) => entry_ts(state, 0, i),
            TtlSource::Inserted(j) => upsert_ts(&mutations[j as usize]),
        };
        for (j, mutation) in mutations.iter().enumerate() {
            let k = mutation_key(mutation);
            order.retain(|&source| key(source) != k);
            if let TtlMutation::Upsert { timestamp, .. } = *mutation {
                if order.len() as u32 >= capacity {
                    return Err(ErrorCode::CapacityExceeded);
                }
                let (mut left, mut right) = (0usize, order.len());
                while left < right {
                    let middle = left + (right - left) / 2;
                    if ts(order[middle]) < timestamp {
                        left = middle + 1;
                    } else {
                        right = middle;
                    }
                }
                order.insert(left, TtlSource::Inserted(j as u32));
            }
        }
        Ok(order)
    }

    fn image(entries: &[(f64, u32)], capacity: u32) -> Vec<u8> {
        let mut state = vec![0xa5u8; (capacity * EVICTION_ENTRY_SIZE) as usize];
        for (i, &(ts, key)) in entries.iter().enumerate() {
            let at = i as u32 * EVICTION_ENTRY_SIZE;
            bytes::write_f64(&mut state, at, ts);
            bytes::write_u32(&mut state, at + 8, key);
            bytes::write_u32(&mut state, at + 12, 1000 + i as u32);
        }
        state
    }

    fn check(entries: &[(f64, u32)], capacity: u32, mutations: &[TtlMutation]) {
        let size = entries.len() as u32;
        let state = image(entries, capacity);
        let expected = oracle(&state, size, capacity, mutations);
        let mut s = TtlBatchScratch::default();
        let planned = plan_order(&state, 0, size, capacity, mutations.iter().copied(), &mut s);
        match expected {
            Err(code) => assert_eq!(planned, Err(code)),
            Ok(order) => {
                assert_eq!(planned, Ok(()));
                assert_eq!(s.order, order, "placement differs from the oracle");
                // Applying the emitted commands yields exactly the planned
                // order, and every changed byte lies in the touched range.
                let (first, last) = emit_commands(&mut s);
                let mut after = state.clone();
                apply_commands(&mut after, 0, &s.commands);
                for (position, source) in order.iter().enumerate() {
                    let at = position * EVICTION_ENTRY_SIZE as usize;
                    let want: [u8; 16] = match *source {
                        TtlSource::Original(i) => {
                            let from = (i * EVICTION_ENTRY_SIZE) as usize;
                            state[from..from + 16].try_into().unwrap()
                        }
                        TtlSource::Inserted(j) => {
                            let TtlMutation::Upsert {
                                key,
                                timestamp,
                                mapped_value,
                            } = mutations[j as usize]
                            else {
                                unreachable!()
                            };
                            let mut entry = [0u8; 16];
                            entry[..8].copy_from_slice(&timestamp.to_le_bytes());
                            entry[8..12].copy_from_slice(&key.to_le_bytes());
                            entry[12..].copy_from_slice(&mapped_value.to_le_bytes());
                            entry
                        }
                    };
                    assert_eq!(after[at..at + 16], want, "entry {position}");
                }
                for (at, (before, now)) in state.iter().zip(&after).enumerate() {
                    if before != now {
                        let entry = (at / EVICTION_ENTRY_SIZE as usize) as u32;
                        assert!(first <= entry && entry < last, "byte {at} outside range");
                    }
                }
            }
        }
    }

    fn timestamp() -> impl Strategy<Value = f64> {
        prop_oneof![
            4 => (0u8..6).prop_map(f64::from),
            1 => Just(f64::NAN),
            1 => Just(-0.0),
            1 => Just(f64::INFINITY),
        ]
    }

    fn mutation() -> impl Strategy<Value = TtlMutation> {
        prop_oneof![
            3 => (0u32..14, timestamp(), any::<u32>()).prop_map(|(key, timestamp, mapped_value)| {
                TtlMutation::Upsert { key, timestamp, mapped_value }
            }),
            1 => (0u32..14).prop_map(|key| TtlMutation::Remove { key }),
        ]
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(2048))]

        #[test]
        fn placement_matches_the_in_order_oracle(
            mut entries in prop::collection::vec((timestamp(), 0u32..14), 0..40),
            sort in any::<bool>(),
            finite in any::<bool>(),
            extra in 0u32..8,
            mut mutations in prop::collection::vec(mutation(), 0..24),
        ) {
            if finite {
                for entry in &mut entries {
                    if entry.0.is_nan() { entry.0 = 2.0; }
                }
                for m in &mut mutations {
                    if let TtlMutation::Upsert { timestamp, .. } = m && timestamp.is_nan() {
                        *timestamp = 3.0;
                    }
                }
            }
            if sort {
                entries.sort_by(|a, b| a.0.total_cmp(&b.0));
            }
            check(&entries, entries.len() as u32 + extra, &mutations);
        }
    }

    #[test]
    fn sorted_full_change_matches_the_oracle_at_scale() {
        let entries: Vec<(f64, u32)> = (0..2048).map(|i| (f64::from(i / 3), i as u32)).collect();
        let mutations: Vec<TtlMutation> = (0..2048)
            .map(|i| TtlMutation::Upsert {
                key: i,
                timestamp: f64::from((i * 7919) % 500),
                mapped_value: i,
            })
            .collect();
        check(&entries, 2048, &mutations);
    }

    #[test]
    fn nan_bearing_full_change_matches_the_oracle_at_scale() {
        let entries: Vec<(f64, u32)> = (0..1024)
            .map(|i| {
                (
                    if i % 97 == 0 {
                        f64::NAN
                    } else {
                        f64::from(i % 50)
                    },
                    i as u32,
                )
            })
            .collect();
        let mutations: Vec<TtlMutation> = (0..1100)
            .map(|i| TtlMutation::Upsert {
                key: (i * 31) % 1100,
                timestamp: if i % 89 == 0 {
                    f64::NAN
                } else {
                    f64::from((i * 13) % 61)
                },
                mapped_value: i,
            })
            .collect();
        check(&entries, 1100, &mutations);
    }
}

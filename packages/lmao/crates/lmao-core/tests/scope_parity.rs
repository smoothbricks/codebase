//! Differential tests for span scope attributes (`specs/lmao/01i_span_scope_attributes.md`).
//!
//! ## The oracle
//!
//! Not a transcription of the Rust code under test. Two independent reference models
//! are derived from the TypeScript implementation's SOURCE and the Rust behaviour is
//! compared against them over generated sequences:
//!
//! - [`ts_set_scope`] is `packages/lmao/src/lib/codegen/spanLoggerGenerator.ts`'s
//!   generated `_setScope` — spread the current object, `null` deletes, `undefined`
//!   is ignored, freeze the result.
//! - [`ts_arrow_fill`] is the per-row loop in
//!   `packages/lmao/src/lib/convertToArrow.ts` (the `lmaoType === 'number'` branch):
//!   a row whose validity bit is set keeps its direct write, a row without one takes
//!   the scope value, and a row with neither stays null.
//!
//! Agreement between two implementations written against one spec is a far stronger
//! signal than either agreeing with itself, and it is what the parity requirement
//! asks for.
//!
//! ## Deliberate divergence, recorded rather than hidden
//!
//! TypeScript ALSO eagerly prefills scope into an overflow buffer's future rows at
//! overflow-creation time (`spanContext.ts:1113` calling the generated
//! `_prefillScopedAttributesOn`). That snapshots the scope as it stood when the
//! overflow was created, so a later `setScope` cannot reach those rows — which
//! contradicts `01i`'s own "Scope Value Changes" section, where ALL rows take the
//! LATEST scope value. Rust fills only at conversion and propagates a `set_scope`
//! down the whole overflow chain, so it matches the spec uniformly. The primary
//! buffer behaves identically in both; the divergence is confined to rows of an
//! overflow buffer whose span changed its scope after overflowing, and there Rust is
//! the correct one. [`overflow_chain_shares_one_scope`] pins the Rust invariant.

use lmao_core::clock::{Clock, TraceAnchor};
use lmao_core::{
    EntryType, F64Column, ScopeEntry, ScopeValue, SharedStr, SpanBuffer, SpanIdentity, SpanScope,
    TraceContext, TraceId,
};
use lmao_macros::define_log_schema;
use proptest::prelude::*;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

const FIELDS: [&str; 4] = ["alpha", "beta", "gamma", "delta"];

struct TickClock(AtomicU64);
impl Clock for TickClock {
    fn wall_nanos(&self) -> i64 {
        1_700_000_000_000_000_000
    }
    fn monotonic_nanos(&self) -> u64 {
        self.0.fetch_add(1, Ordering::Relaxed)
    }
}

fn trace() -> TraceContext {
    TraceContext::new(
        TraceId::new("scope-parity").unwrap(),
        7,
        Arc::new(TickClock(AtomicU64::new(0))),
    )
}

// ---------------------------------------------------------------------------
// Reference model 1: the TypeScript `_setScope`
// ---------------------------------------------------------------------------

/// `spanLoggerGenerator.ts: generateSetScopeMethod` — transcribed, not adapted:
///
/// ```js
/// const next = { ...current };
/// for (const key of Object.keys(attributes)) {
///   const value = attributes[key];
///   if (value === null) delete next[key];
///   else if (value !== undefined) next[key] = value;
/// }
/// this._buffer._scopeValues = Object.freeze(next);
/// ```
///
/// A `BTreeMap` stands in for the JS object: `SpanScope` is name-sorted, so comparing
/// sorted key/value pairs compares exactly the observable content and nothing
/// incidental. Applying the update entries in order reproduces the
/// last-occurrence-wins behaviour a JS object literal gets from deduplicating keys.
fn ts_set_scope(
    current: &BTreeMap<&'static str, ScopeValue>,
    update: &[ScopeEntry],
) -> BTreeMap<&'static str, ScopeValue> {
    let mut next = current.clone();
    for (key, value) in update {
        match value {
            None => {
                next.remove(key);
            }
            Some(value) => {
                next.insert(key, value.clone());
            }
        }
    }
    next
}

fn observed(scope: Option<&SpanScope>) -> BTreeMap<&'static str, ScopeValue> {
    scope
        .map(|scope| {
            scope
                .iter()
                .map(|(name, value)| (name, value.clone()))
                .collect()
        })
        .unwrap_or_default()
}

// ---------------------------------------------------------------------------
// Reference model 2: the TypeScript Arrow scope fill
// ---------------------------------------------------------------------------

/// `convertToArrow.ts`, `lmaoType === 'number'` branch:
///
/// ```js
/// const isValid = (srcNulls[i >>> 3] & (1 << (i & 7))) !== 0;
/// if (isValid) { /* direct write - mark as valid */ }
/// else if (scopeValue !== undefined) { allValues[i] = scopeValue; /* mark valid */ }
/// else { nullCount++; }
/// ```
fn ts_arrow_fill(direct: &[Option<f64>], rows: usize, scope: Option<f64>) -> Vec<Option<f64>> {
    let mut out = direct.to_vec();
    // The JS loop indexes `allValues`; iterating the same prefix is the same walk.
    for cell in out.iter_mut().take(rows) {
        if cell.is_some() {
            continue;
        }
        if let Some(scope) = scope {
            *cell = Some(scope);
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Generators
// ---------------------------------------------------------------------------

fn any_value() -> impl Strategy<Value = ScopeValue> {
    prop_oneof![
        (-1000i32..1000).prop_map(|n| ScopeValue::Number(f64::from(n))),
        (0u64..1000).prop_map(ScopeValue::Uint64),
        any::<bool>().prop_map(ScopeValue::Boolean),
        (0usize..FIELDS.len()).prop_map(|i| ScopeValue::Text(SharedStr::Static(FIELDS[i]))),
    ]
}

fn any_entry() -> impl Strategy<Value = ScopeEntry> {
    (
        (0usize..FIELDS.len()).prop_map(|i| FIELDS[i]),
        prop::option::of(any_value()),
    )
}

fn any_update() -> impl Strategy<Value = Vec<ScopeEntry>> {
    prop::collection::vec(any_entry(), 0..5)
}

// ---------------------------------------------------------------------------
// Merge parity + the immutability properties
// ---------------------------------------------------------------------------

proptest! {
    /// Rust's merge agrees with the TypeScript `_setScope` for every sequence of
    /// updates: same fields present, same values, same clears.
    #[test]
    fn merge_matches_the_typescript_model(updates in prop::collection::vec(any_update(), 0..8)) {
        let mut rust: Option<SpanScope> = None;
        let mut model: BTreeMap<&'static str, ScopeValue> = BTreeMap::new();
        for update in &updates {
            rust = SpanScope::merge(rust.as_ref(), update);
            model = ts_set_scope(&model, update);
            prop_assert_eq!(observed(rust.as_ref()), model.clone());
        }
    }

    /// `SpanBuffer::set_scope` agrees with the same model — the buffer's
    /// `None`-is-empty normalization must not be observable.
    #[test]
    fn buffer_set_scope_matches_the_typescript_model(
        updates in prop::collection::vec(any_update(), 0..8),
    ) {
        let clock = TickClock(AtomicU64::new(0));
        let anchor = TraceAnchor::capture(&clock);
        let identity = Arc::new(SpanIdentity {
            thread_id: 1,
            span_id: 1,
            trace_id: TraceId::new("t").unwrap(),
            parent: None,
        });
        let mut buf = SpanBuffer::start_dynamic(identity, 64, "s".into(), &anchor, &clock);

        let mut model: BTreeMap<&'static str, ScopeValue> = BTreeMap::new();
        for update in &updates {
            buf.set_scope(update);
            model = ts_set_scope(&model, update);
            prop_assert_eq!(observed(buf.scope()), model.clone());
        }
    }

    /// `01i`'s immutability guarantee: the value handed to a child is never mutated,
    /// so a snapshot taken before ANY further updates still reads the same.
    #[test]
    fn merge_never_mutates_the_receiver(
        first in any_update(),
        rest in prop::collection::vec(any_update(), 1..6),
    ) {
        let snapshot = SpanScope::merge(None, &first);
        let expected = observed(snapshot.as_ref());

        let mut later = snapshot.clone();
        for update in &rest {
            later = SpanScope::merge(later.as_ref(), update);
            prop_assert_eq!(
                observed(snapshot.as_ref()),
                expected.clone(),
                "the snapshot changed under a later merge",
            );
        }
    }

    /// Clearing removes EXACTLY the named field and leaves every other value
    /// untouched — the `null`-clears semantics, stated as a property rather than
    /// checked on one example.
    #[test]
    fn clearing_removes_exactly_one_field(
        setup in prop::collection::vec(any_entry(), 1..6),
        victim_index in 0usize..FIELDS.len(),
    ) {
        // Only sets, so the starting scope is known to hold what it was given.
        let sets: Vec<ScopeEntry> = setup
            .into_iter()
            .map(|(name, value)| (name, value.or(Some(ScopeValue::Uint64(1)))))
            .collect();
        let before = SpanScope::merge(None, &sets).expect("only sets, so non-empty");
        let victim = FIELDS[victim_index];
        let victim_was_present = before.get(victim).is_some();

        let after = SpanScope::merge(Some(&before), &[(victim, None)]);
        let after_len = after.as_ref().map_or(0, SpanScope::field_count);

        prop_assert_eq!(
            after.as_ref().and_then(|scope| scope.get(victim)),
            None,
            "the cleared field is gone",
        );
        prop_assert_eq!(
            after_len,
            before.field_count() - usize::from(victim_was_present),
            "exactly one field was removed",
        );
        for (name, value) in before.iter() {
            if name != victim {
                prop_assert_eq!(
                    after.as_ref().and_then(|scope| scope.get(name)),
                    Some(value),
                    "{} was collateral damage",
                    name,
                );
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Child snapshot semantics
// ---------------------------------------------------------------------------

proptest! {
    /// THE async-safety property of `01i`: a child's scope equals its parent's scope
    /// AT THE MOMENT OF CREATION, whatever the parent does afterwards.
    #[test]
    fn child_scope_is_a_creation_time_snapshot(
        before in prop::collection::vec(any_update(), 0..4),
        after in prop::collection::vec(any_update(), 1..4),
    ) {
        let t = trace();
        let mut expected = BTreeMap::new();
        for update in &before {
            expected = ts_set_scope(&expected, update);
        }

        let (_, root) = t.span("parent", None, 8, |ctx| {
            for update in &before {
                ctx.set_scope(update);
            }
            // The child snapshots here, by reference.
            ctx.child("kid", 8, |_| Ok::<_, ()>(()))?;
            // Everything from here on must be invisible to that child.
            for update in &after {
                ctx.set_scope(update);
            }
            Ok::<_, ()>(())
        });

        let kid = &root.children()[0];
        prop_assert_eq!(
            observed(kid.scope()),
            expected.clone(),
            "the child saw a parent mutation that happened after it was created",
        );

        // And the parent did move on, so the assertion above is not vacuous.
        let mut full = expected;
        for update in &after {
            full = ts_set_scope(&full, update);
        }
        prop_assert_eq!(observed(root.scope()), full);
    }

    /// A child created AFTER an update sees it; scope inheritance is not a one-shot
    /// copy taken at span start.
    #[test]
    fn child_created_after_an_update_inherits_it(update in any_update()) {
        let t = trace();
        let expected = ts_set_scope(&BTreeMap::new(), &update);

        let (_, root) = t.span("parent", None, 8, |ctx| {
            ctx.child("before", 8, |_| Ok::<_, ()>(()))?;
            ctx.set_scope(&update);
            ctx.child("after", 8, |_| Ok::<_, ()>(()))?;
            Ok::<_, ()>(())
        });

        prop_assert!(observed(root.children()[0].scope()).is_empty());
        prop_assert_eq!(observed(root.children()[1].scope()), expected);
    }

    /// Inheritance is transitive: a grandchild carries what the root set.
    #[test]
    fn scope_reaches_grandchildren(update in any_update()) {
        let t = trace();
        let expected = ts_set_scope(&BTreeMap::new(), &update);

        let (_, root) = t.span("root", None, 8, |ctx| {
            ctx.set_scope(&update);
            ctx.child("mid", 8, |mid| mid.child("leaf", 8, |_| Ok::<_, ()>(())))
        });

        let leaf = &root.children()[0].children()[0];
        prop_assert_eq!(observed(leaf.scope()), expected);
    }
}

// ---------------------------------------------------------------------------
// Direct writes win
// ---------------------------------------------------------------------------

proptest! {
    /// `fill_unset` agrees cell-for-cell with the TypeScript Arrow fill loop: a
    /// direct write always survives, a null takes the scope value, and no row past
    /// the row count is touched.
    #[test]
    fn fill_unset_matches_the_typescript_arrow_fill(
        writes in prop::collection::vec(prop::option::of(-100i32..100), 0..64),
        scope in prop::option::of(-1000i32..1000),
    ) {
        const CAPACITY: usize = 64;
        let rows = writes.len();
        let scope = scope.map(f64::from);

        let mut column = F64Column::new();
        let mut direct = vec![None; CAPACITY];
        for (row, write) in writes.iter().enumerate() {
            if let Some(value) = write {
                let value = f64::from(*value);
                column.set(row, CAPACITY, value);
                direct[row] = Some(value);
            }
        }

        let expected = ts_arrow_fill(&direct, rows, scope);
        if let Some(scope) = scope {
            column.fill_unset(rows, CAPACITY, scope);
        }

        for (row, want) in expected.iter().enumerate() {
            prop_assert_eq!(
                column.get(row),
                *want,
                "row {} disagrees with the TypeScript fill",
                row,
            );
        }
    }

    /// The count `fill_unset` returns is exactly the number of cells it changed —
    /// the honest answer to "how much of this trace came from scope".
    #[test]
    fn fill_unset_reports_what_it_filled(
        writes in prop::collection::vec(prop::option::of(-100i32..100), 0..64),
    ) {
        const CAPACITY: usize = 64;
        let rows = writes.len();
        let mut column = F64Column::new();
        let mut direct_writes = 0usize;
        for (row, write) in writes.iter().enumerate() {
            if let Some(value) = write {
                column.set(row, CAPACITY, f64::from(*value));
                direct_writes += 1;
            }
        }
        prop_assert_eq!(column.fill_unset(rows, CAPACITY, 9.0), rows - direct_writes);
        prop_assert_eq!(column.fill_unset(rows, CAPACITY, 8.0), 0, "the fill is idempotent");
    }
}

// ---------------------------------------------------------------------------
// Column placement, through the generated surface
// ---------------------------------------------------------------------------

define_log_schema!(pub ParitySchema {
    hits: number,
    tokens: uint64,
    cached: boolean,
    route: category,
    detail: text,
    outcome: enum["ok", "err", "timeout"],
});

/// End-to-end column placement, matching `01i`'s worked "Arrow output" table: `tag`
/// wins on row 0, `set_` wins on the row it names, scope fills every other row of
/// every column it names, and a column no scope field names stays null.
#[test]
fn generated_fill_scope_places_values_like_the_spec_table() {
    let t = trace();
    let (_, span) = t.span("processOrder", None, 8, |ctx| {
        ctx.set_scope(&[
            ParitySchema::scope_route(Some(SharedStr::Static("processing"))),
            ParitySchema::scope_hits(Some(1.0)),
        ]);
        ctx.log(EntryType::Info, "step 1", 1);
        ctx.log(EntryType::Info, "step 2", 2);
        Ok::<_, ()>(())
    });

    let mut traced = ParitySchema::from_span(span);
    traced.tag_route("started"); // row 0 direct write
    traced.set_route(2, "validating"); // one log row direct write
    let filled = traced.fill_scope();

    // Rows 0..write_index = span-start, completion, and the two log entries.
    assert_eq!(traced.span.write_index(), 4);
    assert_eq!(traced.get_route(0), Some("started"), "tag wins on row 0");
    assert_eq!(
        traced.get_route(1),
        Some("processing"),
        "scope fills completion"
    );
    assert_eq!(
        traced.get_route(2),
        Some("validating"),
        "direct write wins on its row"
    );
    assert_eq!(
        traced.get_route(3),
        Some("processing"),
        "scope fills the rest"
    );

    // A second scoped column with no direct writes anywhere is filled entirely.
    for row in 0..4 {
        assert_eq!(traced.get_hits(row), Some(1.0), "row {row}");
    }
    // Columns no scope field names stay untouched and unallocated.
    assert_eq!(traced.get_tokens(0), None);
    assert_eq!(traced.get_detail(0), None);
    assert_eq!(traced.get_outcome(0), None);

    // 4 rows of `hits` + 2 unwritten rows of `route`.
    assert_eq!(filled, 6);
}

/// An enum scope value lands as its dictionary index and reads back as the label.
#[test]
fn generated_fill_scope_handles_enum_fields() {
    let t = trace();
    let (_, span) = t.span("op", None, 8, |ctx| {
        ctx.set_scope(&[ParitySchema::scope_outcome(Some(2)).expect("in-range index")]);
        Ok::<_, ()>(())
    });
    let mut traced = ParitySchema::from_span(span);
    traced.fill_scope();
    assert_eq!(traced.get_outcome(0), Some("timeout"));
    assert_eq!(traced.get_outcome(1), Some("timeout"));
}

#[test]
fn enum_scope_constructor_refuses_an_out_of_range_index() {
    let error = ParitySchema::scope_outcome(Some(3)).expect_err("3 is past the dictionary");
    assert_eq!(error.field, "outcome");
    assert_eq!(error.variants, 3);
}

/// One span's overflow chain is ONE span, so every buffer in it answers the same
/// scope even when `set_scope` happens after the overflow was created. This is the
/// invariant that keeps Rust free of the staleness the TypeScript eager prefill has.
#[test]
fn overflow_chain_shares_one_scope() {
    let t = trace();
    let (_, span) = t.span("overflowing", None, 8, |ctx| {
        // Capacity 8 with rows 0..1 reserved: 6 appends fill it, the 7th overflows.
        for _ in 0..7 {
            ctx.log(EntryType::Info, "filler", 0);
        }
        // Scope set AFTER the overflow buffer already exists.
        ctx.set_scope(&[ParitySchema::scope_route(Some(SharedStr::Static("late")))]);
        Ok::<_, ()>(())
    });

    let overflow = span
        .overflow()
        .expect("7 appends into capacity 8 overflows");
    let head = span.scope().expect("scope was set").clone();
    assert_eq!(
        overflow.scope(),
        Some(&head),
        "the continuation buffer must not hold a stale snapshot",
    );
}

/// A scope field naming no schema column is ignored, exactly as a `_scopeValues` key
/// with no matching column is in TypeScript. It must not panic and must not
/// spuriously allocate a column.
#[test]
fn unknown_scope_fields_are_ignored() {
    let t = trace();
    let (_, span) = t.span("op", None, 8, |ctx| {
        ctx.set_scope(&[("not_a_column", Some(ScopeValue::Uint64(1)))]);
        Ok::<_, ()>(())
    });
    let mut traced = ParitySchema::from_span(span);
    assert_eq!(traced.fill_scope(), 0);
    assert_eq!(traced.attribute_bytes(), 0);
}

/// A scope value whose variant contradicts the column it names is a schema-level
/// programmer error, so debug builds refuse it rather than dropping the attribute
/// silently. Release builds ignore it, matching the TypeScript typed readers.
#[cfg(debug_assertions)]
#[test]
#[should_panic(expected = "expects ScopeValue::Number")]
fn mismatched_scope_variant_panics_in_debug() {
    let t = trace();
    let (_, span) = t.span("op", None, 8, |ctx| {
        ctx.set_scope(&[("hits", Some(ScopeValue::Boolean(true)))]);
        Ok::<_, ()>(())
    });
    ParitySchema::from_span(span).fill_scope();
}

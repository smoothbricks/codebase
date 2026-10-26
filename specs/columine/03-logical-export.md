# Logical slot export and import

## 1. Scope

A program that changes its slot layout cannot read state images written under the old layout. This module gives
embedders a layout-independent logical form of one slot that carries everything a fold needs to continue — not only
reader-visible values: live rows, an aggregate's value and count, a scalar's and each keyed row's comparison timestamp,
bitmap members, a nested slot's inner containers, a condition-tree slot's lifecycle state and derived-facts cells, and
TTL eviction entries, which a truncated event log cannot re-derive. Exporting under the old layout and importing under
the new one rebuilds the slot without replaying its history. The surface is columine_vm::logical_export: export_slot,
import_slot, apply_transform, and rewrite_slot_set.

The VM owns no interner and no expression language. A u32 lane decodes by the SlotEncoding its descriptor owner supplies
(raw, boolean, or interned id); interned cells travel as text through the caller's InternLookup and Interner. Struct
fields carry their encoding in their field type: String and ArrayString are interned, Bool is boolean, Vo is opaque
value-object bytes the caller's codec projects. A bitmap member under an ordinal codec is an interned lane whose
resolver is that codec, which is how ordinal remapping travels.

LogicalValue<'text>::Text carries Cow<'text, str>. Import adapters borrow text from their certified input for the
synchronous import rather than copying each element. The caller keeps that source alive until import completes; cloning
a borrowed cell preserves the same source. Export is a cold ownership boundary: its LogicalSlot<'static> owns text so
the eager rewrite's frozen basis survives mutation of the source interner. TransformRowView separates the row borrow
from the text-source lifetime; Copy and evaluator results preserve that lifetime, while computed text may own.

## 2. Export and import

Export validates the image's geometry through bind_slot before reading any lane; a malformed image is a typed
LogicalExportError naming the operation, the slot, and the cause, never a partial slot. Keyed rows export in ascending
logical-key order, list and array rows in stored order, and floats compare by bits, so equal folds export equal logical
slots. Metadata byte 13 is exported as agg_type only where it is a subtype (aggregate, scalar, and a nested slot's inner
aggregate from its prefix); on struct kinds the byte counts fields and is not a subtype.

Import writes into a freshly initialized image the caller owns exclusively, with the same unjournaled raw writers
init_state and grow_state use. It never runs on an image inside a speculation window: there is no undo log for these
writes because the imported image is a new snapshot, not a mutation of a live one. The target slot must be empty
(aggregates and scalars are overwritten whole), duplicate and sentinel keys refuse, a row whose comparison timestamp the
target cannot store (or that lacks one the target stores) refuses rather than inventing or dropping it, an aggregate
whose stated subtype differs from the target's (including a nested slot's inner aggregate) refuses, and when rows, a
bitmap image, a struct arena, or an eviction index outgrow the target slot the image grows through grow_state, so
unchanged regions copy exactly the way growth copies them. Change flags stay clear.

A condition-tree import writes derived-facts cells at their exported positions, tombstones included, so probe chains
survive; importing into a table of another capacity leaves rehashing to the owner of the derived-facts table, as after
grow_state.

## 3. Field transforms and the eager rewrite

A TransformPlan produces each target field from the old row: Copy an old field, Absent, an opaque Expr evaluated by the
embedder's ExprEvaluator over a frozen view of the old row plus the whole exported slot set as its basis, or a Project
of an old value-object field's writer bytes through the reader codec named by root commitment (the embedder's
ExprEvaluator::project; the default refuses). apply_transform evaluates rows in export order and fields in plan order,
coerces each answer to the target field type by a fixed exact table (an integral float becomes an integer only when the
conversion is exact), and carries keys, comparison timestamps, aggregate counts, and eviction entries unchanged: a plan
rewrites values, never row identity or fold continuation. Its allocation is one new logical slot sized by the old rows
times the plan's fields, and evaluation is a pure function of the frozen export — no clock, no live state.

rewrite_slot_set rewrites a set of slots from the old image into one new image of the target program, eagerly: it
exports every source first, initializes the new image, and per slot either region-copies (geometry identical and no
transform: primary bytes, arena, and TTL regions move whole as growth copies a slot it is not growing, intern ids
untouched, so the interner is never re-based) or imports through the logical form with the slot's transform plan
applied. Nested slots always take the logical path because their outer pointers are absolute. Target slots the transfers
do not name stay at their initial values. The result is geometry-validated before it is returned.

For embedders evaluating stored-row expressions over a frozen image, StoredRow::lookup constructs the validated
stored-row view by key (with an optional second key for two-key struct maps), and BatchView::with_stored_row attaches it
— the same read the stored-row sweep supplies per visited row.

## 4. Behavioral acceptance

Tests prove, per slot kind, that export of import is the identity including timestamps, counts, eviction entries, and
interned text; that generated latest-wins map states continue identically after import under a larger layout (a lost
comparison lane lets a stale row win and fails); that an eager rewrite region-copies a geometry-identical slot whole and
applies Copy, Expr, and Project fields to a reshaped one; and that refusals (truncated image, kind mismatch, duplicate
key, unknown interned id, comparison-lane mismatch, aggregate subtype mismatch) are typed with operation, slot, and
cause.

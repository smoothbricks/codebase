# Thread lane records thrown exceptions as `span-err`

A thrown exception on `ThreadBufferStrategy` is stored in the wasm row store as `span-err` (3), not `span-exception`
(4). The js-heap lane records the same throw as `span-exception`. This is a thread-lane-only lie: an operator reading
`span-err` concludes the span completed through `ctx.err(...)` / `Err` (an expected operational failure). The taxonomy's
point is that `span-exception` means an unexpected throw — a bug or broken invariant.

This is not a dropped-row bug. Logs written before the throw are still in the store, in order. The completion _type_ is
wrong.

## Reproduction (bun 1.4.0 arm64-darwin)

```ts
tracer.trace_fn(0, 'boom', {}, (span) => {
  span.log.info('before-throw').n(1);
  throw new Error('boom');
});
```

OBSERVED:

| lane                            | `entry_type[1]` (JS view) | wasm/row-store header         |
| ------------------------------- | ------------------------- | ----------------------------- |
| js-heap (`JsBufferStrategy`)    | 4 `span-exception`        | n/a (TypedArray is the store) |
| thread (`ThreadBufferStrategy`) | 4 `span-exception`        | **3 `span-err`**              |

js-heap is truthful. The thread JS facade is truthful. The wasm row that Arrow/flush will publish is not.

## Why

`ThreadSpanBuffer` already arms the completion row as `SpanException` at open:

```555:556:packages/lmao/crates/lmao-core/src/thread_buffer.rs
            header: pack_dynamic(EntryType::SpanException),
```

`complete` can stamp any `EntryType`. `end_err` then hardcodes `SpanErr`:

```620:621:packages/lmao/crates/lmao-core/src/thread_buffer.rs
    pub fn end_err(&mut self, span_id: u32, timestamp: i64) -> Result<(), ThreadBufferError> {
        self.complete(span_id, EntryType::SpanErr, timestamp)
```

There is no `end_exception` on the ABI. The TS adapter maps both completions onto that one export:

```280:286:packages/lmao/src/lib/wasm/threadSpanView.ts
  end(entryType: number): void {
    if (!this.opened) this.openSpan(this._spanName ?? 'span');
    const timestamp = this._traceRoot._timestampNow(this._traceRoot);
    const status =
      entryType === ENTRY_TYPE_SPAN_ERR || entryType === ENTRY_TYPE_SPAN_EXCEPTION
        ? this.binding.endErr(this.spanId, timestamp)
        : this.binding.endOk(this.spanId, timestamp);
```

The thread appender is the only `writeSpanEnd` that goes through that collapse:

```441:442:packages/lmao/src/lib/physicalLayoutPlan.ts
  writeSpanEnd(buffer: AnySpanBuffer, entryType: number): void {
    requireThreadSpanView(buffer).end(entryType);
```

The tracer asks for exception:

```821:821:packages/lmao/src/lib/tracer.ts
      writeSpanEndEntry(buffer, ENTRY_TYPE_SPAN_EXCEPTION);
```

```1210:1211:packages/lmao/src/lib/spanContext.ts
    _spanException(childBuffer: SpanBuffer<Ctx['logSchema']>, error: unknown): void {
      writeSpanEndEntry(childBuffer, ENTRY_TYPE_SPAN_EXCEPTION);
```

js-heap writes the argument through, no collapse:

```75:80:packages/lmao/src/lib/traceRoot.node.ts
const writeSpanEndPrimitive: SpanEndPrimitive = (traceRoot, buffer, entryType) => {
  const entryTypes = buffer.entry_type;
  if (entryTypes === undefined) throw new TypeError('Split span-end appender requires entry_type storage');
  buffer.timestamp[1] = nextTimestamp(traceRoot as TraceRoot);
  entryTypes[1] = entryType;
```

`lmao-core::EntryType` already distinguishes `SpanOk | SpanErr | SpanException` (`crates/lmao-core/src/entry_type.rs`).
The per-span `buffer.rs` path has a real `end` that can take `SpanException`. The thread ABI is the one that cannot.

## Scope

- **Thread lane only.** js-heap `entry_type[1] === 4` on the same throw.
- Not a `lmao-core` completion-path bug in the per-span buffer. It is the thread ABI (`end_err` hardcodes `SpanErr`)
  plus `ThreadSpanView.end` folding exception into that export, which then overwrites the pre-armed `SpanException` row.
- Existing `ThreadBufferStrategy` tests do not assert completion type on throw. `threadSpanBuffer.test.ts` stub is also
  missing `thread_span_buffer_reset` after that export was added (separate test gap).

## Fix shape

Do not route `ENTRY_TYPE_SPAN_EXCEPTION` through `end_err`. Either:

- add `thread_span_buffer_end_exception` that `complete`s with `EntryType::SpanException`, or
- pass the entry type through `end_*` so `complete` stamps what the tracer asked for.

Stamp the timestamp. Leave the `span-err` / `span-exception` distinction intact. The pre-armed completion row is already
`SpanException`; the exception path should not overwrite it with `SpanErr`.

## Landed

`0cb5e10d fix(lmao): record a thrown exception as SpanException, not SpanErr` — OBSERVED bun 1.4.0 arm64-darwin: the
same throw now stores wasm header 4 (`span-exception`) on the thread lane, matching js-heap `entry_type[1] === 4`.
`ThreadSpanView.end` passes the tracer's entry type through `binding.end(spanId, entryType, timestamp)`
(`threadSpanView.ts:324-331`); wasm export is `thread_span_buffer_end`.

`c9364da0 fix(lmao): give the thread lane the static-vocabulary message lane` — `_infoTemplate` no longer throws.
`ThreadSpanView` has a `_messageIds` getter. It still forwards into `commitLog` → intern + `appendLog`, so
`appendLogStatic` stays 0 until static routing lands.

Still open: flat-class cutover and `appendLogStatic` on the write path. Jurisdiction not re-judged until those land.

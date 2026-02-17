# Trace-Testing with LMAO

> Based on Akkartik's "tracing tests": https://akkartik.name/post/tracing-tests

## Core Principle

**Test WHAT happened (domain facts), not HOW (implementation details).**

Traditional tests couple to internal structure:

```typescript
const result = await myOp(args);
expect(result.status).toBe('success'); // Brittle - breaks on refactor
```

Trace-tests assert on facts emitted during execution:

```typescript
await trace('my-op', myOp);
expect(facts.has(spanOk('my-op'))).toBe(true); // Robust - survives refactoring
```

## LMAO Fact Types

Facts are strongly-typed template literals. Invalid facts = compile errors.

| Type        | Format                   | Example                    |
| ----------- | ------------------------ | -------------------------- |
| `SpanFact`  | `span:${name}: ${state}` | `span:fetch-user: ok`      |
| `LogFact`   | `log:${level}: ${msg}`   | `log:info: Processing`     |
| `TagFact`   | `tag:${key}: ${value}`   | `tag:userId: 123`          |
| `ScopeFact` | `scope:${key}: ${value}` | `scope:requestId: req-abc` |
| `FFFact`    | `ff:${flag}: ${value}`   | `ff:darkMode: true`        |

Span states: `started`, `ok`, `err(CODE)`, `exception(msg)`

## Usage

```typescript
import { TestTracer, defineOpContext } from '@smoothbricks/lmao';
import { extractFacts, spanStarted, spanOk, spanErr, tagFact, logInfo } from '@smoothbricks/lmao/testing';

// 1. Run traced code
const tracer = new TestTracer(opContext);
await tracer.trace('process-order', async (ctx) => {
  ctx.tag.orderId('ord-123');
  await ctx.span('validate', async (c) => c.ok(null));
  await ctx.span('charge', async (c) => c.ok(null));
  return ctx.ok('done');
});

// 2. Extract facts from buffer tree
const facts = extractFacts(tracer.rootBuffers[0]);

// 3. Assert on WHAT happened
expect(facts.has(spanOk('process-order'))).toBe(true);
expect(facts.has(tagFact('orderId', 'ord-123'))).toBe(true);
expect(facts.hasInOrder([spanStarted('validate'), spanOk('validate'), spanStarted('charge'), spanOk('charge')])).toBe(
  true
);
expect(facts.hasMatch('span:*: err(*)')).toBe(false);
```

## FactArray API

| Method                          | Purpose                                |
| ------------------------------- | -------------------------------------- |
| `has(fact)`                     | Exact match                            |
| `hasMatch(pattern)`             | Glob match (`*` = any non-colon chars) |
| `hasInOrder([...])`             | Facts appear in order (not adjacent)   |
| `byNamespace('span')`           | Filter to namespace                    |
| `spans()` / `logs()` / `tags()` | Typed filters                          |
| `match(pattern)`                | Return matching facts                  |

## Buffer Layout (Why This Works)

Per `specs/lmao/01h_entry_types_and_logging_primitives.md`:

- **Row 0**: `span-start` + tag writes (`ctx.tag.*` overwrites here)
- **Row 1**: Completion (`span-ok`/`span-err`/`span-exception`)
- **Row 2+**: Log entries (`ctx.log.*` appends here)

`extractFacts()` walks the buffer tree depth-first, emitting facts in execution order.

## When to Use Trace-Testing

✅ **Use for**: Operation ordering, error handling, tag/scope propagation, multi-span workflows ❌ **Don't use for**:
Return value validation, performance benchmarks, schema validation

## Key Insight

> "Tests no longer need call fine-grained helpers directly... The trace checks that the program correctly computed a
> specific _fact_, while remaining oblivious about _how_ it was computed." — Akkartik

This makes tests resilient to:

- Function boundary changes
- Sync → async refactoring
- Internal restructuring
- Adding/removing intermediate steps

# lmao

This library was generated with [Nx](https://nx.dev).

## Building

Run `nx build lmao` to build the library.

## Trace Testing

LMAO provides a trace-testing system where each test creates queryable trace spans. Instead of testing return values
directly, you execute code that emits trace facts and assert on WHAT happened.

### Quick Start (bun:test)

1. Create a preload file:

```typescript
// test-setup.ts
import { initTraceTestRun } from '@smoothbricks/lmao/testing/bun';
import { myOpContext } from './src/opContext.js';

initTraceTestRun(myOpContext, {
  sqlite: { dbPath: '.trace-results.db' },
});
```

2. Add to `bunfig.toml`:

```toml
[test]
preload = ["./test-setup.ts"]
```

3. Write tests:

```typescript
import { describe, it, expect, useTestSpan } from '@smoothbricks/lmao/testing/bun';
import { spanOk, tagFact, extractFacts } from '@smoothbricks/lmao/testing';

describe('Order Processing', () => {
  it('validates and saves order', async () => {
    const ctx = useTestSpan();
    const result = await ctx.span('processOrder', async (childCtx) => {
      childCtx.tag.orderId('123');
      return childCtx.ok({ status: 'saved' });
    });
    // Assert on trace facts
    expect(extractFacts(ctx.buffer).has(spanOk('processOrder'))).toBe(true);
  });
});
```

### Quick Start (vitest)

```typescript
// vitest.config.ts
export default defineConfig({
  test: { setupFiles: ['./test-setup.ts'] },
});

// test-setup.ts
import { initTraceTestRun } from '@smoothbricks/lmao/testing/vitest';
initTraceTestRun(myOpContext, {
  sqlite: {
    dbPath: '.trace-results.db',
    createDatabase: (path) => new (require('better-sqlite3'))(path),
  },
});
```

### Querying Span Results

**QueryableSpan** wraps a SpanBuffer with ergonomic helpers:

```typescript
import { querySpan } from '@smoothbricks/lmao/testing';

const q = querySpan(tracer.rootBuffers[0]);
q.name; // span name
q.facts(); // all facts from this span tree
q.find('validate'); // first child span by name
q.findAll('db-query'); // all matching descendants
q.children; // direct child QueryableSpans
q.names(); // all descendant span names
```

**Standalone functions** (tree-shakable):

```typescript
import { findSpan, extractFactsFor, spanNames } from '@smoothbricks/lmao/testing';

const span = findSpan(rootBuffer, 'validate');
const facts = extractFactsFor(rootBuffer, 'save');
const names = spanNames(rootBuffer);
```

### SQLite Persistence

When configured, the trace database is written after all tests complete. Schema columns evolve automatically based on
your LogSchema fields.

Query post-run:

```typescript
import { Database } from 'bun:sqlite';
import { TraceQuery } from '@smoothbricks/lmao/testing';

const query = new TraceQuery(new Database('.trace-results.db'));
query.failures(); // all failed tests
query.slowest(undefined, 10); // 10 slowest tests
query.findSpans('%validate%'); // spans matching pattern
query.testTree('my-test'); // full span tree for a test
query.close();
```


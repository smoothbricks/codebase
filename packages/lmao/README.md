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
// test-trace-setup.ts (preload)
import * as bunTest from 'bun:test';
import { mock } from 'bun:test';
import { createBunTestMock, initTraceTestRun } from '@smoothbricks/lmao/testing/bun';
import { myOpContext } from './src/opContext.js';

initTraceTestRun(myOpContext, { sqlite: { dbPath: '.trace-results.db' } });
mock.module('bun:test', () => createBunTestMock(bunTest));
```

`mock.module` MUST be called from the preload file itself — bun only intercepts subsequent imports when the mock is
registered from the entry module context.

2. Add to `bunfig.toml`:

```toml
[test]
preload = ["./test-trace-setup.ts"]
```

3. Write tests — import from `bun:test` as normal, `mock.module` intercepts transparently:

```typescript
import { describe, it, expect } from 'bun:test';
import { useTestSpan } from '@smoothbricks/lmao/testing/bun';
import { querySpan, findSpan } from '@smoothbricks/lmao/testing';

describe('Order Processing', () => {
  it('validates and saves order', async () => {
    const ctx = useTestSpan();

    await ctx.span('processOrder', async (child) => {
      child.tag.orderId('123');
      await child.span('validate', async (v) => v.ok(true));
      await child.span('save', async (s) => s.ok({ id: 'order-123' }));
      return child.ok({ status: 'saved' });
    });

    // Query the trace tree
    const q = querySpan(ctx.buffer);
    expect(q.names()).toEqual(['processOrder', 'validate', 'save']);
    expect(q.find('validate')).toBeDefined();
    expect(findSpan(ctx.buffer, 'save')).toBeDefined();
  });
});
```

Note: `describe`/`it`/`expect` are imported from `bun:test` as normal — `mock.module` intercepts transparently. Only
`useTestSpan` comes from `@smoothbricks/lmao/testing/bun`.

### Quick Start (vitest)

1. Configure vitest:

```typescript
// vitest.config.ts
export default defineConfig({
  test: { setupFiles: ['./test-setup.ts'] },
});
```

2. Create the setup file with `vi.mock` for transparent interception:

```typescript
// test-setup.ts
import { vi } from 'vitest';
import BetterSqlite3 from 'better-sqlite3';
import { myOpContext } from './src/opContext.js';

vi.mock('vitest', async (importOriginal) => {
  const [mod, { createVitestMock }] = await Promise.all([
    importOriginal(),
    import('@smoothbricks/lmao/testing/vitest'),
  ]);
  return createVitestMock(mod as Record<string, unknown>);
});

import { initTraceTestRun } from '@smoothbricks/lmao/testing/vitest';
initTraceTestRun(myOpContext, {
  sqlite: { dbPath: '.trace-results.db', createDatabase: (p) => new BetterSqlite3(p) },
});
```

3. Write tests — import from `vitest` as normal, `vi.mock` intercepts transparently:

```typescript
import { describe, it, expect } from 'vitest';
import { useTestSpan } from '@smoothbricks/lmao/testing/vitest';
import { querySpan, findSpan } from '@smoothbricks/lmao/testing';

describe('Order Processing', () => {
  it('validates and saves order', async () => {
    const ctx = useTestSpan();

    await ctx.span('processOrder', async (child) => {
      child.tag.orderId('123');
      await child.span('validate', async (v) => v.ok(true));
      return child.ok({ status: 'saved' });
    });

    const q = querySpan(ctx.buffer);
    expect(q.names()).toContain('validate');
    expect(findSpan(ctx.buffer, 'processOrder')).toBeDefined();
  });
});
```

Both bun:test and vitest use the same transparent interception pattern — tests import from their native test module as
normal. Only `useTestSpan` (for accessing the it-local trace root) comes from the lmao testing module.

### Setting Up Trace Testing for a New Package

1. Create the preload file (`test-trace-setup.ts`):

```typescript
import * as bunTest from 'bun:test';
import { mock } from 'bun:test';
import { createBunTestMock, initTraceTestRun } from '@smoothbricks/lmao/testing/bun';
import { myOpContext } from './src/opContext.js';

initTraceTestRun(myOpContext, { sqlite: { dbPath: '.trace-results.db' } });
mock.module('bun:test', () => createBunTestMock(bunTest));
```

2. Add `bunfig.toml`:

```toml
[test]
preload = ["./test-trace-setup.ts"]
```

3. Tests import `describe`/`it`/`expect` from `bun:test` as normal — the mock intercepts transparently.

4. Add `.trace-results.db` to `.gitignore`.

### Querying Trace Results

After a test run, the trace database is written to the configured path. The `trace_id` is printed at the end:

```
[trace] trace_id: 550e8400-e29b-41d4-a716-446655440000 → .trace-results.db
```

The `trace_id` IS the run identifier — one root span per test run, with each `it()` as a child span.

**SQLite CLI queries:**

```bash
# All spans for the latest trace (root span name = 'test-run')
sqlite3 .trace-results.db "SELECT s0.message, s0.describe FROM spans s0 WHERE s0.row_index = 0 ORDER BY s0.timestamp_ns"

# Find root span_id, then query it-level spans
sqlite3 .trace-results.db "
  SELECT s0.message AS test_name, s0.describe,
         CASE WHEN s1.entry_type = 2 THEN 'ok'
              WHEN s1.entry_type = 3 THEN 'err'
              WHEN s1.entry_type = 4 THEN 'exception'
              ELSE 'running' END AS status,
         s1.timestamp_ns - s0.timestamp_ns AS duration_ns
  FROM spans s0
  LEFT JOIN spans s1 ON s1.trace_id = s0.trace_id AND s1.span_id = s0.span_id AND s1.row_index = 1
  WHERE s0.trace_id = (SELECT trace_id FROM spans WHERE parent_span_id = 0 AND row_index = 0 ORDER BY timestamp_ns DESC LIMIT 1)
    AND s0.parent_span_id = (SELECT span_id FROM spans WHERE parent_span_id = 0 AND row_index = 0 ORDER BY timestamp_ns DESC LIMIT 1)
    AND s0.row_index = 0
  ORDER BY s0.timestamp_ns"

# All tests under a specific describe group
sqlite3 .trace-results.db "SELECT message FROM spans WHERE describe = 'Order Processing > validation' AND row_index = 0"

# Nested describe paths use ' > ' separator
sqlite3 .trace-results.db "SELECT DISTINCT describe FROM spans WHERE describe IS NOT NULL AND row_index = 0"
```

**Schema:**

| Column           | Description                                                      |
| ---------------- | ---------------------------------------------------------------- |
| `trace_id`       | Run identifier (= root span's trace_id)                          |
| `span_id`        | Unique span counter within trace                                 |
| `parent_span_id` | Parent span (0 = root, root's span_id = it-level)                |
| `row_index`      | Row within span (0 = span-start, 1 = span-end, 2+ = log entries) |
| `entry_type`     | 1=span-start, 2=span-ok, 3=span-err, 4=span-exception            |
| `timestamp_ns`   | Nanosecond timestamp                                             |
| `message`        | Span name (row 0), log message (rows 2+)                         |
| `describe`       | `' > '`-separated describe path (user schema column)             |
| `...`            | Additional user schema columns added dynamically via ALTER TABLE |

Tree structure is encoded via `span_id` / `parent_span_id`. The root span (`parent_span_id = 0`) represents the entire
test run. Each `it()` is a direct child of the root. User operations create deeper children.

**TraceQuery API (programmatic access):**

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

### Querying Span Results (in-test)

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


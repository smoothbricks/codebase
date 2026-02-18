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

After a test run, the trace database is written to the configured path. The `run_id` is printed at the end:

```
[trace] run_id: 1718900000000-abc123 → .trace-results.db
```

**SQLite CLI queries:**

```bash
# All spans for the latest run
sqlite3 .trace-results.db "SELECT span_name, status, describe FROM spans ORDER BY started_at"

# Failed tests with their describe group
sqlite3 .trace-results.db "SELECT span_name, describe FROM spans WHERE status != 'ok' AND depth = 0"

# All tests under a specific describe group
sqlite3 .trace-results.db "SELECT span_name, status FROM spans WHERE describe = 'Order Processing > validation' AND depth = 0"

# Nested describe paths use ' > ' separator
sqlite3 .trace-results.db "SELECT DISTINCT describe FROM spans WHERE describe IS NOT NULL"
```

**Key tables:**

| Table         | Purpose                                                                         |
| ------------- | ------------------------------------------------------------------------------- |
| `runs`        | Test run summary (run_id, started_at, completed_at, pass/fail counts)           |
| `spans`       | Span tree (span_name, parent_span_name, describe, status, duration_ns, depth)   |
| `log_entries` | Per-row data within spans (entry_type, timestamp, message, user schema columns) |

The `describe` column contains the `' > '`-separated describe path for each root span (depth=0). Child spans inherit the
same describe path from their root. Tests outside any `describe()` block have `describe = NULL`.

**`run_id`** groups all spans from a single test run. Each `bun test` invocation produces a unique `run_id` (timestamp +
random suffix). Use it to compare runs or query specific executions.

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


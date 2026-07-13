/**
 * Log-call partial-inline benchmark — measures what the ttsc plugin's
 * loginline pass wins over the fluent generated-SpanLogger path.
 *
 * Both paths share the SAME runtime row allocation (_checkOverflow +
 * writeLogEntry); the inlined path replaces the fluent dispatches
 * (message/line/attr setters, FluentLogEntry returns) with direct
 * TypedArray writes at the returned index — the emitted output shape of
 * plugin/loginline.go, hand-written here.
 *
 * Run: bun benchmarks/log-inline.bench.ts
 */
import { bench, group, run } from 'mitata';

const CAP = 1024;
const OPS = ['DELETE', 'INSERT', 'SELECT', 'UPDATE'] as const;

interface BenchBuffer {
  _writeIndex: number;
  _capacity: number;
  timestamp: Float64Array;
  entry_type: Uint8Array;
  message_values: unknown[];
  _logHeaders: Uint32Array;
  message_nulls: Uint8Array;
  line_values: Float64Array;
  line_nulls: Uint8Array;
  userId_values: unknown[];
  userId_nulls: Uint8Array;
  retries_values: Float64Array;
  retries_nulls: Uint8Array;
  operation_values: Uint8Array;
  operation_nulls: Uint8Array;
  _traceRoot: { writeLogEntry(buffer: BenchBuffer, entryType: number): number };
  constructor: { stats: { totalWrites: number } };
}

function makeBuffer(): BenchBuffer {
  const buf: BenchBuffer = {    _writeIndex: 2,
    _capacity: CAP,
    timestamp: new Float64Array(CAP),
    entry_type: new Uint8Array(CAP),
    message_values: new Array(CAP),
    _logHeaders: new Uint32Array(CAP),    message_nulls: new Uint8Array(CAP >> 3),
    line_values: new Float64Array(CAP),
    line_nulls: new Uint8Array(CAP >> 3),
    userId_values: new Array<string>(CAP),
    userId_nulls: new Uint8Array(CAP >> 3),
    retries_values: new Float64Array(CAP),
    retries_nulls: new Uint8Array(CAP >> 3),
    operation_values: new Uint8Array(CAP),
    operation_nulls: new Uint8Array(CAP >> 3),
    _traceRoot: {
      writeLogEntry(b: BenchBuffer, entryType: number) {
        const idx = b._writeIndex++;
        if (idx >= CAP) {
          b._writeIndex = 2;
          return 2;
        } // wrap for bench
        b.timestamp[idx] = idx;
        b.entry_type[idx] = entryType;
        b._logHeaders[idx] = 0;
        return idx;      },
    },
    constructor: { stats: { totalWrites: 0 } },
  };
function makeRepeatedStringStore(buffer: BenchBuffer) {
  let iteration = 0;  return () => {
    const row = buffer._traceRoot.writeLogEntry(buffer, 8);
    buffer.message_values[row] = 'request completed';
    buffer.constructor.stats.totalWrites++;
  };
}

function makeRepeatedDenseHeaderStore(buffer: BenchBuffer) {
  let iteration = 0;  return () => {
    const row = buffer._traceRoot.writeLogEntry(buffer, 8);
    buffer._logHeaders[row] = (1 << 8) | 8;
    buffer.constructor.stats.totalWrites++;
  };
}

function makeCallsiteStringStores(buffer: BenchBuffer) {
  let iteration = 0;
  return () => {
    const row = buffer._traceRoot.writeLogEntry(buffer, 8);
    switch (iteration & 3) {
      case 0:
        buffer.message_values[row] = 'user created';
        break;
      case 1:
        buffer.message_values[row] = 'order submitted';
        break;
      case 2:
        buffer.message_values[row] = 'cache refreshed';
        break;
      default:
        buffer.message_values[row] = 'request completed';
        break;
    }
    buffer.constructor.stats.totalWrites++;
    iteration++;
  };
}

function makeCallsiteDenseHeaderStores(buffer: BenchBuffer) {
  let iteration = 0;
  return () => {
    const row = buffer._traceRoot.writeLogEntry(buffer, 8);
    switch (iteration & 3) {
      case 0:
        buffer._logHeaders[row] = (1 << 8) | 8;
        break;
      case 1:
        buffer._logHeaders[row] = (2 << 8) | 8;
        break;
      case 2:
        buffer._logHeaders[row] = (3 << 8) | 8;
        break;
      default:
        buffer._logHeaders[row] = (4 << 8) | 8;
        break;
    }
    buffer.constructor.stats.totalWrites++;
    iteration++;
  };
}

function makeStringStoreWithAttrs(buffer: BenchBuffer) {
  let iteration = 0;
  return () => {
    const row = buffer._traceRoot.writeLogEntry(buffer, 8);
    buffer.message_values[row] = 'user created';
    buffer.constructor.stats.totalWrites++;
    buffer.line_values[row] = 24;
    buffer.line_nulls[row >>> 3] |= 1 << (row & 7);
    buffer.userId_values[row] = 'u42';
    buffer.userId_nulls[row >>> 3] |= 1 << (row & 7);
    buffer.retries_values[row] = iteration;
    buffer.retries_nulls[row >>> 3] |= 1 << (row & 7);
    iteration++;
  };
}

function makeDenseHeaderStoreWithAttrs(buffer: BenchBuffer) {
  let iteration = 0;
  return () => {
    const row = buffer._traceRoot.writeLogEntry(buffer, 8);
    buffer._logHeaders[row] = (1 << 8) | 8;
    buffer.constructor.stats.totalWrites++;
    buffer.line_values[row] = 24;
    buffer.line_nulls[row >>> 3] |= 1 << (row & 7);
    buffer.userId_values[row] = 'u42';
    buffer.userId_nulls[row >>> 3] |= 1 << (row & 7);
    buffer.retries_values[row] = iteration;
    buffer.retries_nulls[row >>> 3] |= 1 << (row & 7);
    iteration++;
  };
}

function makeMixedStringStores(buffer: BenchBuffer) {
  let iteration = 0;
  return () => {
    const row = buffer._traceRoot.writeLogEntry(buffer, 8);
    if (iteration % 10 === 0) {
      buffer.message_values[row] = DYNAMIC_MESSAGES[(iteration / 10) & 3];
    } else {
      buffer.message_values[row] = 'request completed';
    }
    buffer.constructor.stats.totalWrites++;
    iteration++;
  };
}

function makeMixedDenseHeaderStores(buffer: BenchBuffer) {
  let iteration = 0;
  return () => {
    const row = buffer._traceRoot.writeLogEntry(buffer, 8);
    if (iteration % 10 === 0) {
      buffer.message_values[row] = DYNAMIC_MESSAGES[(iteration / 10) & 3];
    } else {
      buffer._logHeaders[row] = (1 << 8) | 8;
    }
    buffer.constructor.stats.totalWrites++;
    iteration++;
  };
}

function makeDynamicControl(buffer: BenchBuffer) {
  let iteration = 0;
  return () => {
    const row = buffer._traceRoot.writeLogEntry(buffer, 8);
    buffer.message_values[row] = DYNAMIC_MESSAGES[iteration & 3];
    buffer.constructor.stats.totalWrites++;
    iteration++;
  };
}

function addPositionBalancedPair(
  stringLabel: string,
  denseHeaderLabel: string,
  makeStringCase: (buffer: BenchBuffer) => () => void,
  makeDenseHeaderCase: (buffer: BenchBuffer) => () => void,
) {
  const [stringFirst, denseSecond, denseFirst, stringSecond] = makePositionBalancedBuffers();
  bench(`A1 ${stringLabel} [pair 1: first]`, makeStringCase(stringFirst));
  bench(`B1 ${denseHeaderLabel} [pair 1: second]`, makeDenseHeaderCase(denseSecond));
  bench(`B2 ${denseHeaderLabel} [pair 2: first]`, makeDenseHeaderCase(denseFirst));
  bench(`A2 ${stringLabel} [pair 2: second]`, makeStringCase(stringSecond));
}

group('matched message store: one repeated literal', () => {
  addPositionBalancedPair(
    'JS Array string-reference store',
    'Uint32Array packed dense-header store',
    makeRepeatedStringStore,
    makeRepeatedDenseHeaderStore,
  );
});

group('matched message store: four literal callsites', () => {
  addPositionBalancedPair(
    'JS Array string-reference stores',
    'Uint32Array packed dense-header stores',
    makeCallsiteStringStores,
    makeCallsiteDenseHeaderStores,
  );
});

group('matched message store: literal + line + 2 attrs', () => {
  addPositionBalancedPair(
    'JS Array string-reference store + matched attrs',
    'Uint32Array packed dense-header store + matched attrs',
    makeStringStoreWithAttrs,
    makeDenseHeaderStoreWithAttrs,
  );
});

group('matched message store: 90% literal / 10% dynamic', () => {
  addPositionBalancedPair(
    'JS Array string-reference store for both branches',
    'Uint32Array dense header / JS Array dynamic fallback',
    makeMixedStringStores,
    makeMixedDenseHeaderStores,
  );
});

group('matched message store: dynamic-only position control', () => {
  const [firstA, secondB, firstB, secondA] = makePositionBalancedBuffers();
  bench('A1 dynamic JS Array control [pair 1: first]', makeDynamicControl(firstA));
  bench('B1 dynamic JS Array control [pair 1: second]', makeDynamicControl(secondB));
  bench('B2 dynamic JS Array control [pair 2: first]', makeDynamicControl(firstB));
  bench('A2 dynamic JS Array control [pair 2: second]', makeDynamicControl(secondA));
});

// --- result-chain partial inline (row 1, fires on every op completion) -------
interface ResultBenchBuffer {
  readonly line_values: Float64Array;
  readonly line_nulls: Uint8Array;
  readonly userId_values: string[];
  readonly userId_nulls: Uint8Array;
  readonly retries_values: Float64Array;
  readonly retries_nulls: Uint8Array;
}

interface BenchResult {
  readonly _buffer: ResultBenchBuffer;
  line(value: number): BenchResult;
  userId(value: string): BenchResult;
  retries(value: number): BenchResult;
  with(values: { readonly line?: number; readonly userId?: string; readonly retries?: number }): BenchResult;
}

const rbuf: ResultBenchBuffer = {
  line_values: new Float64Array(4),
  line_nulls: new Uint8Array(1),
  userId_values: new Array(4),
  userId_nulls: new Uint8Array(1),
  retries_values: new Float64Array(4),
  retries_nulls: new Uint8Array(1),
};
function makeOk(buffer: ResultBenchBuffer): BenchResult {
  const result: BenchResult = {
    _buffer: buffer,
    line(value) {
      buffer.line_values[1] = value;
      buffer.line_nulls[0] |= 2;
      return result;
    },
    userId(value) {
      buffer.userId_values[1] = value;
      buffer.userId_nulls[0] |= 2;
      return result;
    },
    retries(value) {
      buffer.retries_values[1] = value;
      buffer.retries_nulls[0] |= 2;
      return result;
    },
    with(values) {
      if (values.line !== undefined) {
        result.line(values.line);
      }
      if (values.userId !== undefined) {
        result.userId(values.userId);
      }
      if (values.retries !== undefined) {
        result.retries(values.retries);
      }
      return result;
    },
  };
  return result;
}
const rctx = { ok: (_v: unknown) => makeOk(rbuf) };

group('ctx.ok + line + with(2 fields)', () => {
  bench('A result fluent', () => {
    rctx
      .ok(n)
      .line(19)
      .with({ userId: `u${n}`, retries: 2 });
    n++;
  });
  bench('B result inlined', () => {
    const $$r = rctx.ok(n);
    const $$b = $$r._buffer;
    if ($$b) {
      if ($$b.line_values) {
        $$b.line_values[1] = 19;
        if ($$b.line_nulls) {
          $$b.line_nulls[1 >>> 3] |= 1 << (1 & 7);
        }
      }
      if ($$b.userId_values) {
        $$b.userId_values[1] = `u${n}`;
        if ($$b.userId_nulls) {
          $$b.userId_nulls[1 >>> 3] |= 1 << (1 & 7);
        }
      }
      if ($$b.retries_values) {
        $$b.retries_values[1] = 2;
        if ($$b.retries_nulls) {
          $$b.retries_nulls[1 >>> 3] |= 1 << (1 & 7);
        }
      }
    }
    n++;
  });
});

await run();

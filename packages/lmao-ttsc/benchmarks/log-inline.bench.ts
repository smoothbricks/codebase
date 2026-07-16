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
  readonly _capacity: number;
  readonly timestamp: Float64Array;
  readonly entry_type: Uint8Array;
  readonly message_values: string[];
  readonly _messageTemplateIds: Uint16Array;
  readonly message_nulls: Uint8Array;
  readonly line_values: Float64Array;
  readonly line_nulls: Uint8Array;
  readonly userId_values: string[];
  readonly userId_nulls: Uint8Array;
  readonly retries_values: Float64Array;
  readonly retries_nulls: Uint8Array;
  readonly operation_values: Uint8Array;
  readonly operation_nulls: Uint8Array;
  readonly _traceRoot: {
    writeLogEntry(buffer: BenchBuffer, entryType: number): number;
  };
  readonly constructor: {
    readonly stats: {
      totalWrites: number;
    };
  };
}

interface BenchLogger {
  readonly _buffer: BenchBuffer;
  _writeIndex: number;
  _checkOverflow(): void;
  trace(message: string): BenchLogger;
  debug(message: string): BenchLogger;
  info(message: string): BenchLogger;
  warn(message: string): BenchLogger;
  error(message: string): BenchLogger;
  line(value: number): BenchLogger;
  userId(value: string): BenchLogger;
  retries(value: number): BenchLogger;
  operation(value: (typeof OPS)[number]): BenchLogger;
}

function makeBuffer(): BenchBuffer {
  return {
    _writeIndex: 2,
    _capacity: CAP,
    timestamp: new Float64Array(CAP),
    entry_type: new Uint8Array(CAP),
    message_values: new Array<string>(CAP),
    _messageTemplateIds: new Uint16Array(CAP),
    message_nulls: new Uint8Array(CAP >> 3),
    line_values: new Float64Array(CAP),
    line_nulls: new Uint8Array(CAP >> 3),
    userId_values: new Array<string>(CAP),
    userId_nulls: new Uint8Array(CAP >> 3),
    retries_values: new Float64Array(CAP),
    retries_nulls: new Uint8Array(CAP >> 3),
    operation_values: new Uint8Array(CAP),
    operation_nulls: new Uint8Array(CAP >> 3),
    _traceRoot: {
      writeLogEntry(buffer, entryType) {
        const index = buffer._writeIndex++;
        if (index >= CAP) {
          buffer._writeIndex = 2;
          return 2;
        }
        buffer.timestamp[index] = index;
        buffer.entry_type[index] = entryType;
        return index;
      },
    },
    constructor: { stats: { totalWrites: 0 } },
  };
}

const setNullBit = (bm: Uint8Array, i: number) => {
  bm[i >>> 3] |= 1 << (i & 7);
};

// Fluent generated-SpanLogger reference (spanLoggerGenerator.ts semantics)
function makeLogger(buffer: BenchBuffer): BenchLogger {
  const logger: BenchLogger = {
    _buffer: buffer,
    _writeIndex: 1,
    _checkOverflow() {
      if (buffer._writeIndex >= buffer._capacity) {
        buffer._writeIndex = 2;
      }
    },
    trace(message) {
      logger._checkOverflow();
      const index = buffer._traceRoot.writeLogEntry(buffer, 6);
      logger._writeIndex = index;
      buffer.message_values[index] = message;
      setNullBit(buffer.message_nulls, index);
      buffer.constructor.stats.totalWrites++;
      return logger;
    },
    debug(message) {
      logger._checkOverflow();
      const index = buffer._traceRoot.writeLogEntry(buffer, 7);
      logger._writeIndex = index;
      buffer.message_values[index] = message;
      setNullBit(buffer.message_nulls, index);
      buffer.constructor.stats.totalWrites++;
      return logger;
    },
    info(message) {
      logger._checkOverflow();
      const index = buffer._traceRoot.writeLogEntry(buffer, 8);
      logger._writeIndex = index;
      buffer.message_values[index] = message;
      setNullBit(buffer.message_nulls, index);
      buffer.constructor.stats.totalWrites++;
      return logger;
    },
    warn(message) {
      logger._checkOverflow();
      const index = buffer._traceRoot.writeLogEntry(buffer, 9);
      logger._writeIndex = index;
      buffer.message_values[index] = message;
      setNullBit(buffer.message_nulls, index);
      buffer.constructor.stats.totalWrites++;
      return logger;
    },
    error(message) {
      logger._checkOverflow();
      const index = buffer._traceRoot.writeLogEntry(buffer, 10);
      logger._writeIndex = index;
      buffer.message_values[index] = message;
      setNullBit(buffer.message_nulls, index);
      buffer.constructor.stats.totalWrites++;
      return logger;
    },
    line(value) {
      buffer.line_values[logger._writeIndex] = value;
      setNullBit(buffer.line_nulls, logger._writeIndex);
      return logger;
    },
    userId(value) {
      buffer.userId_values[logger._writeIndex] = value;
      setNullBit(buffer.userId_nulls, logger._writeIndex);
      return logger;
    },
    retries(value) {
      buffer.retries_values[logger._writeIndex] = value;
      setNullBit(buffer.retries_nulls, logger._writeIndex);
      return logger;
    },
    operation(value) {
      buffer.operation_values[logger._writeIndex] = OPS.indexOf(value);
      setNullBit(buffer.operation_nulls, logger._writeIndex);
      return logger;
    },
  };
  return logger;
}

const buf = makeBuffer();
const log = makeLogger(buf);
const ctx = { log, _buffer: buf };
let n = 0;

group('log.info + line + 2 attrs', () => {
  bench('A fluent (runtime path)', () => {
    ctx.log.info('user created').line(24).userId(`u${n}`).retries(n);
    n++;
  });
  bench('B inlined (plugin output shape)', () => {
    const $$l = ctx.log;
    $$l._checkOverflow();
    const $$b = $$l._buffer;
    const $$i = $$b._traceRoot.writeLogEntry($$b, 8);
    $$l._writeIndex = $$i;
    if ($$b.message_values) {
      $$b.message_values[$$i] = 'user created';
      if ($$b.message_nulls) {
        $$b.message_nulls[$$i >>> 3] |= 1 << ($$i & 7);
      }
    }
    $$b.constructor.stats.totalWrites++;
    if ($$b.line_values) {
      $$b.line_values[$$i] = 24;
      if ($$b.line_nulls) {
        $$b.line_nulls[$$i >>> 3] |= 1 << ($$i & 7);
      }
    }
    if ($$b.userId_values) {
      $$b.userId_values[$$i] = `u${n}`;
      if ($$b.userId_nulls) {
        $$b.userId_nulls[$$i >>> 3] |= 1 << ($$i & 7);
      }
    }
    if ($$b.retries_values) {
      $$b.retries_values[$$i] = n;
      if ($$b.retries_nulls) {
        $$b.retries_nulls[$$i >>> 3] |= 1 << ($$i & 7);
      }
    }
    n++;
  });
});

group('log.warn + line + literal enum', () => {
  bench('A fluent (runtime path)', () => {
    ctx.log.warn('slow query').line(25).operation('SELECT');
    n++;
  });
  bench('B inlined (enum constant-folded)', () => {
    const $$l = ctx.log;
    $$l._checkOverflow();
    const $$b = $$l._buffer;
    const $$i = $$b._traceRoot.writeLogEntry($$b, 9);
    $$l._writeIndex = $$i;
    if ($$b.message_values) {
      $$b.message_values[$$i] = 'slow query';
      if ($$b.message_nulls) {
        $$b.message_nulls[$$i >>> 3] |= 1 << ($$i & 7);
      }
    }
    $$b.constructor.stats.totalWrites++;
    if ($$b.line_values) {
      $$b.line_values[$$i] = 25;
      if ($$b.line_nulls) {
        $$b.line_nulls[$$i >>> 3] |= 1 << ($$i & 7);
      }
    }
    $$b.operation_values[$$i] = 2;
    $$b.operation_nulls[$$i >>> 3] |= 1 << ($$i & 7);
    n++;
  });
});

group('bare log.info (floor: runtime calls dominate)', () => {
  bench('A fluent', () => {
    ctx.log.info('hello');
    n++;
  });
  bench('B inlined', () => {
    const $$l = ctx.log;
    $$l._checkOverflow();
    const $$b = $$l._buffer;
    const $$i = $$b._traceRoot.writeLogEntry($$b, 8);
    $$l._writeIndex = $$i;
    if ($$b.message_values) {
      $$b.message_values[$$i] = 'hello';
      if ($$b.message_nulls) {
        $$b.message_nulls[$$i >>> 3] |= 1 << ($$i & 7);
      }
    }
    $$b.constructor.stats.totalWrites++;
    n++;
  });
});

// --- matched literal-message storage ----------------------------------------
const DYNAMIC_MESSAGES = ['request 0', 'request 1', 'request 2', 'request 3'] as const;

function makePositionBalancedBuffers() {
  return [makeBuffer(), makeBuffer(), makeBuffer(), makeBuffer()] as const;
}

function makeRepeatedStringStore(buffer: ReturnType<typeof makeBuffer>) {
  return () => {
    const row = buffer._traceRoot.writeLogEntry(buffer, 8);
    buffer.message_values[row] = 'request completed';
    buffer.constructor.stats.totalWrites++;
  };
}

function makeRepeatedTemplateIdStore(buffer: ReturnType<typeof makeBuffer>) {
  return () => {
    const row = buffer._traceRoot.writeLogEntry(buffer, 8);
    buffer._messageTemplateIds[row] = 1;
    buffer.constructor.stats.totalWrites++;
  };
}

function makeCallsiteStringStores(buffer: ReturnType<typeof makeBuffer>) {
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

function makeCallsiteTemplateIdStores(buffer: ReturnType<typeof makeBuffer>) {
  let iteration = 0;
  return () => {
    const row = buffer._traceRoot.writeLogEntry(buffer, 8);
    switch (iteration & 3) {
      case 0:
        buffer._messageTemplateIds[row] = 1;
        break;
      case 1:
        buffer._messageTemplateIds[row] = 2;
        break;
      case 2:
        buffer._messageTemplateIds[row] = 3;
        break;
      default:
        buffer._messageTemplateIds[row] = 4;
        break;
    }
    buffer.constructor.stats.totalWrites++;
    iteration++;
  };
}

function makeStringStoreWithAttrs(buffer: ReturnType<typeof makeBuffer>) {
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

function makeTemplateIdStoreWithAttrs(buffer: ReturnType<typeof makeBuffer>) {
  let iteration = 0;
  return () => {
    const row = buffer._traceRoot.writeLogEntry(buffer, 8);
    buffer._messageTemplateIds[row] = 1;
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

function makeMixedStringStores(buffer: ReturnType<typeof makeBuffer>) {
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

function makeMixedTemplateIdStores(buffer: ReturnType<typeof makeBuffer>) {
  let iteration = 0;
  return () => {
    const row = buffer._traceRoot.writeLogEntry(buffer, 8);
    if (iteration % 10 === 0) {
      buffer.message_values[row] = DYNAMIC_MESSAGES[(iteration / 10) & 3];
    } else {
      buffer._messageTemplateIds[row] = 1;
    }
    buffer.constructor.stats.totalWrites++;
    iteration++;
  };
}

function makeDynamicControl(buffer: ReturnType<typeof makeBuffer>) {
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
  templateIdLabel: string,
  makeStringCase: (buffer: ReturnType<typeof makeBuffer>) => () => void,
  makeTemplateIdCase: (buffer: ReturnType<typeof makeBuffer>) => () => void,
) {
  const [stringFirst, templateSecond, templateFirst, stringSecond] = makePositionBalancedBuffers();
  bench(`A1 ${stringLabel} [pair 1: first]`, makeStringCase(stringFirst));
  bench(`B1 ${templateIdLabel} [pair 1: second]`, makeTemplateIdCase(templateSecond));
  bench(`B2 ${templateIdLabel} [pair 2: first]`, makeTemplateIdCase(templateFirst));
  bench(`A2 ${stringLabel} [pair 2: second]`, makeStringCase(stringSecond));
}

group('matched message store: one repeated literal', () => {
  addPositionBalancedPair(
    'JS Array string-reference store',
    'Uint16Array Op-local template-ID store',
    makeRepeatedStringStore,
    makeRepeatedTemplateIdStore,
  );
});

group('matched message store: four literal callsites', () => {
  addPositionBalancedPair(
    'JS Array string-reference stores',
    'Uint16Array Op-local template-ID stores',
    makeCallsiteStringStores,
    makeCallsiteTemplateIdStores,
  );
});

group('matched message store: literal + line + 2 attrs', () => {
  addPositionBalancedPair(
    'JS Array string-reference store + matched attrs',
    'Uint16Array Op-local template-ID store + matched attrs',
    makeStringStoreWithAttrs,
    makeTemplateIdStoreWithAttrs,
  );
});

group('matched message store: 90% literal / 10% dynamic', () => {
  addPositionBalancedPair(
    'JS Array string-reference store for both branches',
    'Uint16Array literal ID / JS Array dynamic fallback',
    makeMixedStringStores,
    makeMixedTemplateIdStores,
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

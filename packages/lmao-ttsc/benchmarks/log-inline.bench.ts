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

function makeBuffer() {
  const buf: any = {
    _writeIndex: 2,
    _capacity: CAP,
    timestamp: new Float64Array(CAP),
    entry_type: new Uint8Array(CAP),
    message_values: new Array(CAP),
    _messageTemplateIds: new Uint16Array(CAP),
    message_nulls: new Uint8Array(CAP >> 3),
    line_values: new Float64Array(CAP),
    line_nulls: new Uint8Array(CAP >> 3),
    userId_values: new Array(CAP),
    userId_nulls: new Uint8Array(CAP >> 3),
    retries_values: new Float64Array(CAP),
    retries_nulls: new Uint8Array(CAP >> 3),
    operation_values: new Uint8Array(CAP),
    operation_nulls: new Uint8Array(CAP >> 3),
    _traceRoot: {
      writeLogEntry(b: any, entryType: number) {
        const idx = b._writeIndex++;
        if (idx >= CAP) {
          b._writeIndex = 2;
          return 2;
        } // wrap for bench
        b.timestamp[idx] = idx;
        b.entry_type[idx] = entryType;
        return idx;
      },
    },
  };
  buf.constructor = { stats: { totalWrites: 0 } };
  return buf;
}

const setNullBit = (bm: Uint8Array, i: number) => {
  bm[i >>> 3] |= 1 << (i & 7);
};

// Fluent generated-SpanLogger reference (spanLoggerGenerator.ts semantics)
function makeLogger(buf: any) {
  const ENTRY: Record<string, number> = { trace: 6, debug: 7, info: 8, warn: 9, error: 10 };
  const l: any = { _buffer: buf, _writeIndex: 1 };
  l._checkOverflow = () => {
    if (buf._writeIndex >= buf._capacity) buf._writeIndex = 2;
  };
  for (const level of Object.keys(ENTRY)) {
    l[level] = (message: string) => {
      l._checkOverflow();
      const idx = buf._traceRoot.writeLogEntry(buf, ENTRY[level]);
      l._writeIndex = idx;
      if (buf.message_values) {
        buf.message_values[idx] = message;
        if (buf.message_nulls) setNullBit(buf.message_nulls, idx);
      }
      buf.constructor.stats.totalWrites++;
      return l;
    };
  }
  const raw = (field: string) => (v: unknown) => {
    const idx = l._writeIndex;
    if (buf[`${field}_values`]) {
      buf[`${field}_values`][idx] = v;
      if (buf[`${field}_nulls`]) setNullBit(buf[`${field}_nulls`], idx);
    }
    return l;
  };
  l.line = raw('line');
  l.userId = raw('userId');
  l.retries = raw('retries');
  l.operation = (v: string) => {
    const idx = l._writeIndex;
    buf.operation_values[idx] = Math.max(0, OPS.indexOf(v as (typeof OPS)[number]));
    setNullBit(buf.operation_nulls, idx);
    return l;
  };
  return l;
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
  let iteration = 0;
  return () => {
    const row = buffer._traceRoot.writeLogEntry(buffer, 8);
    buffer.message_values[row] = 'request completed';
    buffer.constructor.stats.totalWrites++;
    iteration++;
  };
}

function makeRepeatedTemplateIdStore(buffer: ReturnType<typeof makeBuffer>) {
  let iteration = 0;
  return () => {
    const row = buffer._traceRoot.writeLogEntry(buffer, 8);
    buffer._messageTemplateIds[row] = 1;
    buffer.constructor.stats.totalWrites++;
    iteration++;
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
const rbuf: any = {
  line_values: new Float64Array(4),
  line_nulls: new Uint8Array(1),
  userId_values: new Array(4),
  userId_nulls: new Uint8Array(1),
  retries_values: new Float64Array(4),
  retries_nulls: new Uint8Array(1),
};
function makeOk(b: any) {
  const raw = (field: string) => (v: unknown) => {
    if (b[`${field}_values`]) {
      b[`${field}_values`][1] = v;
      if (b[`${field}_nulls`]) b[`${field}_nulls`][0] |= 2;
    }
    return r;
  };
  const r: any = { line: raw('line'), userId: raw('userId'), retries: raw('retries'), _buffer: b };
  r.with = (obj: Record<string, unknown>) => {
    for (const [k, v] of Object.entries(obj)) r[k](v);
    return r;
  };
  return r;
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

/**
 * Op._opContextBinding Tests
 *
 * Tests that the full context wiring pipeline works:
 * - defineOpContext().defineOp() produces Ops with _opContextBinding set
 * - The binding's logBinding.logSchema.fields contains the schema fields
 * - .prefix() on an OpGroup preserves _opContextBinding on resulting Ops
 * - .mapColumns() preserves _opContextBinding
 * - Two Ops from the same defineOpContext share the same binding reference (identity dedup)
 */

import { describe, expect, it } from 'bun:test';
import { defineOpContext } from '../defineOpContext.js';
import { Op } from '../op.js';
import type { OpContextBinding } from '../opContext/types.js';
import { S } from '../schema/builder.js';
import { defineLogSchema } from '../schema/defineLogSchema.js';

// WHY: MappedOpGroup types don't carry named op properties, but ops are
// spread as own properties at runtime. This accessor type lets us reach them.
type OpRecord = Record<string, { _opContextBinding?: OpContextBinding }>;

const testSchema = defineLogSchema({
  userId: S.category(),
  endpoint: S.text(),
});

describe('Op._opContextBinding', () => {
  it('defineOp produces an Op with _opContextBinding set', () => {
    const ctx = defineOpContext({ logSchema: testSchema });
    const op = ctx.defineOp('test-op', (ctx) => ctx.ok('done'));

    expect(op).toBeInstanceOf(Op);
    expect(op._opContextBinding).toBeDefined();
  });

  it('binding logBinding.logSchema.fields contains the schema fields', () => {
    const ctx = defineOpContext({ logSchema: testSchema });
    const op = ctx.defineOp('test-op', (ctx) => ctx.ok('done'));

    const binding = op._opContextBinding!;
    const fields = binding.logBinding.logSchema.fields;

    // WHY: user-defined fields should be present in the schema
    expect(fields).toHaveProperty('userId');
    expect(fields).toHaveProperty('endpoint');
  });

  it('.prefix() on an OpGroup preserves _opContextBinding on resulting Ops', () => {
    const ctx = defineOpContext({ logSchema: testSchema });
    const ops = ctx.defineOps({
      fetch: (ctx) => ctx.ok('fetched'),
      save: (ctx) => ctx.ok('saved'),
    });

    const prefixed = ops.prefix('http');

    // WHY: bracket access — MappedOpGroup types don't carry named op properties,
    // but ops are spread as own properties at runtime
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- WHY: MappedOpGroup runtime properties not reflected in types
    const fetchOp = (prefixed as unknown as OpRecord).fetch;
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- WHY: MappedOpGroup runtime properties not reflected in types
    const saveOp = (prefixed as unknown as OpRecord).save;

    expect(fetchOp._opContextBinding).toBeDefined();
    expect(saveOp._opContextBinding).toBeDefined();

    // Binding should still reference the original schema fields
    expect(fetchOp._opContextBinding!.logBinding.logSchema.fields).toHaveProperty('userId');
  });

  it('.mapColumns() preserves _opContextBinding', () => {
    const ctx = defineOpContext({ logSchema: testSchema });
    const ops = ctx.defineOps({
      fetch: (ctx) => ctx.ok('fetched'),
    });

    const mapped = ops.mapColumns({ userId: 'mapped_user' });

    // WHY: bracket access — MappedOpGroup types don't carry named op properties
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- WHY: MappedOpGroup runtime properties not reflected in types
    const fetchOp = (mapped as unknown as OpRecord).fetch;
    expect(fetchOp._opContextBinding).toBeDefined();
    expect(fetchOp._opContextBinding!.logBinding.logSchema.fields).toHaveProperty('userId');
  });

  it('two Ops from the same defineOpContext share the same binding reference', () => {
    const ctx = defineOpContext({ logSchema: testSchema });
    const opA = ctx.defineOp('op-a', (ctx) => ctx.ok('a'));
    const opB = ctx.defineOp('op-b', (ctx) => ctx.ok('b'));

    // WHY: identity dedup — same defineOpContext produces one binding object
    expect(opA._opContextBinding).toBe(opB._opContextBinding);
  });

  it('two Ops from different defineOpContext have different bindings', () => {
    const ctxA = defineOpContext({ logSchema: testSchema });
    const ctxB = defineOpContext({ logSchema: testSchema });

    const opA = ctxA.defineOp('op-a', (ctx) => ctx.ok('a'));
    const opB = ctxB.defineOp('op-b', (ctx) => ctx.ok('b'));

    // WHY: different factories produce different binding objects
    expect(opA._opContextBinding).not.toBe(opB._opContextBinding);
  });

  it('Ops from defineOps share the same binding as defineOp from same context', () => {
    const ctx = defineOpContext({ logSchema: testSchema });
    const singleOp = ctx.defineOp('single', (ctx) => ctx.ok('single'));
    const group = ctx.defineOps({
      grouped: (ctx) => ctx.ok('grouped'),
    });

    const groupedOp = group.grouped;

    // WHY: all Ops from one defineOpContext share the same binding reference
    expect(singleOp._opContextBinding).toBe(groupedOp._opContextBinding);
  });

  it('prefixed Ops from different groups share binding when from same context', () => {
    const ctx = defineOpContext({ logSchema: testSchema });
    const group = ctx.defineOps({
      fetch: (ctx) => ctx.ok('fetched'),
      save: (ctx) => ctx.ok('saved'),
    });

    const prefixed = group.prefix('http');
    // WHY: bracket access — MappedOpGroup types don't carry named op properties
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- WHY: MappedOpGroup runtime properties not reflected in types
    const fetchOp = (prefixed as unknown as OpRecord).fetch;
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- WHY: MappedOpGroup runtime properties not reflected in types
    const saveOp = (prefixed as unknown as OpRecord).save;

    // WHY: prefix creates new Op instances but preserves the binding reference
    expect(fetchOp._opContextBinding).toBe(saveOp._opContextBinding);
  });
});

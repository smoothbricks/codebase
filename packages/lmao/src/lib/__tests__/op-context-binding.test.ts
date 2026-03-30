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
import type { OpContext } from '../opContext/types.js';
import { S } from '../schema/builder.js';
import { defineLogSchema } from '../schema/defineLogSchema.js';

const testSchema = defineLogSchema({
  userId: S.category(),
  endpoint: S.text(),
});

function requireOpContextBinding<Ctx extends OpContext, Args extends unknown[], S, E>(op: Op<Ctx, Args, S, E>) {
  const binding = op._opContextBinding;
  if (!binding) {
    throw new Error('Expected Op to carry _opContextBinding');
  }
  return binding;
}

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

    const binding = requireOpContextBinding(op);
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

    // WHY: after prefix, each Op in the group should still carry the same binding
    const fetchOp = prefixed.fetch;
    const saveOp = prefixed.save;

    expect(fetchOp._opContextBinding).toBeDefined();
    expect(saveOp._opContextBinding).toBeDefined();

    // Binding should still reference the original schema fields
    expect(requireOpContextBinding(fetchOp).logBinding.logSchema.fields).toHaveProperty('userId');
  });

  it('.mapColumns() preserves _opContextBinding', () => {
    const ctx = defineOpContext({ logSchema: testSchema });
    const ops = ctx.defineOps({
      fetch: (ctx) => ctx.ok('fetched'),
    });

    const mapped = ops.mapColumns({ userId: 'mapped_user' });

    const fetchOp = mapped.fetch;
    expect(fetchOp._opContextBinding).toBeDefined();
    expect(requireOpContextBinding(fetchOp).logBinding.logSchema.fields).toHaveProperty('userId');
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
    const fetchOp = prefixed.fetch;
    const saveOp = prefixed.save;

    // WHY: prefix creates new Op instances but preserves the binding reference
    expect(fetchOp._opContextBinding).toBe(saveOp._opContextBinding);
  });
});

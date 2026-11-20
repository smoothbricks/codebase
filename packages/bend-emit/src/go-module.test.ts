import { expect, test } from 'bun:test';
import { checkBendFile } from './emit.js';

const model = `${import.meta.dir}/../test/conformance.bend`;

test('Go module mode emits a callable package deterministically from the same checked book', async () => {
  const book = await checkBendFile(model);
  const options = {
    target: { kind: 'go-module', modelName: 'conformance.bend', flags: [] },
    functions: ['number'],
    laws: ['packets_agree', 'guarded_lists_agree', 'all_packets_are_zero'],
  } as const;
  const first = book.emit(options);
  if (first.kind !== 'module') throw new Error('expected a binding module');
  expect(first.source).toContain('package bendmodel');
  expect(first.source).toContain('func Drive_all_packets_are_zero(');
  expect(() => book.emit({ ...options, functions: ['List.any.if'] })).toThrow('bend-emit refuses: type form All');
  expect(book.emit(options)).toEqual(first);
});

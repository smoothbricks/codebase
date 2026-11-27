import { expect, test } from 'bun:test';

import { Opcode } from '../index.js';

test('BATCH_STRUCT_MAP_UPSERT_MAX keeps the public 0x82 ABI value', () => {
  expect(Opcode.BATCH_STRUCT_MAP_UPSERT_MAX).toBe(0x82);
  expect(Opcode[0x82]).toBe('BATCH_STRUCT_MAP_UPSERT_MAX');
});

test('NESTED_LIST_APPEND_STRUCT keeps the public 0x93 ABI value', () => {
  expect(Opcode.NESTED_LIST_APPEND_STRUCT).toBe(0x93);
  expect(Opcode[0x93]).toBe('NESTED_LIST_APPEND_STRUCT');
});

/**
 * @smoothbricks/columine
 *
 * Generic columnar processing pipeline for Arrow column data.
 * Stages: Parse | Reduce | Compact | Undo
 *
 * Usage standalone:
 *   import { getBackend } from '@smoothbricks/columine';
 *   const backend = await getBackend(); // lazy-loads WASM
 *
 *   import { setBackend } from '@smoothbricks/columine';
 */

// Backend dependency injection
export {
  getBackend,
  hasBackend,
  resetBackend,
  setBackend,
  setBackendLoader,
} from './backend.js';
// Types and interfaces
export type {
  ColumineBackend,
  ColumnInput,
  ReducerProgram,
  SlotDef,
  StateHandle,
} from './types.js';
export {
  AggType,
  ErrorCode,
  HEADER_SIZE,
  MAGIC,
  Opcode,
  SlotType,
  ValueType,
} from './types.js';
// WASM backend (for standalone columine usage)
export { createColumineWasmBackend, loadColumineWasm } from './wasm-backend.js';

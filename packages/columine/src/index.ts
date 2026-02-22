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
export { getBackend, hasBackend, resetBackend, setBackend, setBackendLoader } from './backend.js';
// Parse/Compact backend (event_processor WASM bridge)
export type { EventProcessorWasmExports, ParseCompactBackend } from './parse-backend.js';
export { createParseCompactWasmBackend, loadParseBackend } from './parse-backend.js';
// Pipeline composition API
export type {
  ColumineStages,
  CompactStage,
  ParseConfig,
  ParseResult,
  ParseStage,
  PipelineOptions,
  ReduceStage,
  UndoStage,
  UndoToken,
} from './pipeline.js';
export { createPipeline } from './pipeline.js';
// Canonical reducer bytecode parser
export { parseReducerProgram, parseReducerSlotDefs } from './reducer-bytecode.js';
// Types and interfaces
export type {
  ColumineBackend,
  ColumnInput,
  ReducerProgram,
  SlotDef,
  SlotTtlMetadata,
  StateHandle,
  UndoCapableColumineBackend,
} from './types.js';
export {
  AggType,
  assertUndoCapableBackend,
  ErrorCode,
  HEADER_SIZE,
  isUndoCapableBackend,
  MAGIC,
  Opcode,
  PROGRAM_HASH_PREFIX,
  SlotType,
  StructFieldType,
  TtlStartOf,
  ValueType,
} from './types.js';
// WASM backend (for standalone columine usage)
export { createColumineWasmBackend, loadColumineWasm } from './wasm-backend.js';
// Shared WASM memory sizing contract (used by wrappers)
export type {
  WasmMemoryContractErrorCode,
  WasmMemoryGrowthOptions,
  WasmMemoryGrowthResult,
  WasmWorkingSetBytes,
} from './wasm-memory-contract.js';
export {
  calculateRequiredWasmBytes,
  calculateRequiredWasmPages,
  createInvalidWasmMemoryConfigError,
  createInvalidWasmWorkingSetError,
  createWasmMemoryCapExceededError,
  ensureWasmMemoryForWorkingSet,
  validateWasmWorkingSetBytes,
  WASM_MAX_BYTES,
  WASM_MAX_PAGES,
  WASM_PAGE_BYTES,
} from './wasm-memory-contract.js';

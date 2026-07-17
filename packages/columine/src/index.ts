/**
 * @smoothbricks/columine
 *
 * Generic columnar processing pipeline for Arrow column data.
 * Stages: Parse | Reduce | Compact | Undo
 *
 * Usage:
 *   import { createPipeline, loadColumineWasm, loadParseBackend } from '@smoothbricks/columine';
 *   const [backend, parseBackend] = await Promise.all([loadColumineWasm(), loadParseBackend()]);
 *   const stages = createPipeline({ backend, parseBackend });
 */
// Parse/Compact backend (event_processor WASM bridge)
export type {
  CompactDiagnostic,
  CompactEncodingErrorCode,
  EventProcessorWasmExports,
  ParseCompactBackend,
} from './parse-backend.js';
export { CompactEncodingError, createParseCompactWasmBackend, loadParseBackend } from './parse-backend.js';
// Pipeline composition API
export type {
  ColumineStages,
  CompactBatch,
  CompactColumn,
  CompactStage,
  EncodedArrowSchema,
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
  EvictedRow,
  EvictionResult,
  ReducerProgram,
  ScalarValue,
  SlotDef,
  SlotTtlMetadata,
  StateHandle,
  StructMap2RowRef,
} from './types.js';
export {
  AggType,
  ComparisonType,
  ErrorCode,
  HEADER_SIZE,
  MAGIC,
  Opcode,
  PROGRAM_HASH_PREFIX,
  SlotType,
  SlotTypeFlag,
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

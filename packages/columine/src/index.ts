/**
 * @smoothbricks/columine
 *
 * Generic columnar processing pipeline for Arrow column data.
 * Stages: Parse | Reduce | Compact | Undo
 *
 * Artifact profiles follow [Cargo profiles](../../nx-plugin/README.md#cargo-profiles): a checkout holding the
 * unpublished crates/columine-wasm/src/lib.rs entry loads .cache/wasm-dev/, including from compiled dist/ts.
 * Packages and sealed snapshots without that entry load the shipping dist/ pair. A missing selected development
 * artifact reports its cause and the cached producer, nx run columine:cargo-wasm; it never loads a shipping fallback.
 *
 * Usage:
 *   import { createPipeline, loadColumineWasm, loadParseBackend } from '@smoothbricks/columine';
 *   const [backend, parseBackend] = await Promise.all([loadColumineWasm(), loadParseBackend()]);
 *   const stages = createPipeline({ backend, parseBackend });
 */
// Parse/Compact backend (event_processor WASM bridge)

// Counts-fold capacities (generated from the native definition)
export type { CountBounds, CountsFoldCapacities } from './counts-fold-capacities.js';
export { COUNTS_FOLD_CAPACITIES } from './counts-fold-capacities.js';
// Index operands (column, slot and match-count encoding)
export { appendIndex, MAX_INDEX_LEN, readIndex } from './operand.js';
export type {
  CompactDiagnostic,
  CompactEncodingErrorCode,
  EventProcessorCreateErrorCode,
  EventProcessorWasmExports,
  ParseCompactBackend,
} from './parse-backend.js';
export {
  CompactEncodingError,
  createParseCompactWasmBackend,
  EventProcessorCreateError,
  loadParseBackend,
} from './parse-backend.js';
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
export {
  encodeProgramHeader,
  type ProgramHeaderFields,
  parseReducerProgram,
  parseReducerSlotDefs,
} from './reducer-bytecode.js';
// Types and interfaces
export type {
  ColumineBackend,
  ColumnInput,
  DeltaSegmentExport,
  EvictedRow,
  EvictionResult,
  ReducerProgram,
  ScalarValue,
  SlotDef,
  SlotTtlMetadata,
  StateHandle,
  StructMap2RowRef,
  StructMapArenaField,
  VmReadResult,
  VoCountsRole,
  VoDefinitionMetadata,
  VoFieldLink,
  VoProgramMetadata,
  VoSchemaMetadata,
} from './types.js';
export {
  AggType,
  ComparisonType,
  ErrorCode,
  HEADER_SIZE,
  NO_PARENT_TS_COL,
  Opcode,
  PROGRAM_FORMAT_VERSION,
  PROGRAM_HASH_PREFIX,
  PROGRAM_MAGIC,
  SlotType,
  SlotTypeFlag,
  StructFieldType,
  TtlStartOf,
  UNDO_POSITION_OVERFLOWED,
  ValueType,
} from './types.js';
export { encodeVoProgramMetadata, VoProgramAdmissionUnavailableError } from './vo-program-metadata.js';
// WASM backend (for standalone columine usage)
export type { VmProgramInvalidCause } from './wasm-backend.js';
export {
  createColumineWasmBackend,
  loadColumineWasm,
  VmOutOfMemoryError,
  VmProgramAdmissionError,
  VmStateAdmissionError,
} from './wasm-backend.js';
// Shared WASM host memory contract (used by wrappers): host bytes live only in
// a WasmHostRegion above the module's linker-derived memory.
export type { WasmMemoryContractErrorCode } from './wasm-memory-contract.js';
export {
  calculateRequiredWasmPages,
  createInvalidWasmMemoryConfigError,
  createInvalidWasmWorkingSetError,
  createWasmMemoryCapExceededError,
  WASM_MAX_BYTES,
  WASM_MAX_PAGES,
  WASM_PAGE_BYTES,
  WasmHostRegion,
  WasmMemoryContractError,
} from './wasm-memory-contract.js';

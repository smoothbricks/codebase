/**
 * Op-Centric API - Module Index
 *
 * Re-exports all types and implementation functions for the Op-centric API.
 * Consumers should import from this file for a single entry point.
 *
 * @module opContext
 */

// Re-export implementation functions
export { createOpGroup } from './createOpGroup.js';
export { createDefineOp, createDefineOps, createOpMetadata, DEFAULT_METADATA } from './defineOp.js';
// Re-export OpGroup types and helpers
export * from './opGroupTypes.js';
// Re-export all types from the hub
export * from './types.js';

/**
 * Schema type definitions for arrow-builder
 *
 * Arrow-builder is a low-level columnar buffer engine that works with field nodes (schema/field.ts).
 */

import type { Field } from './schema/field.js';

/** A record of field nodes: column name → field. */
export type SchemaFields = Record<string, Field>;

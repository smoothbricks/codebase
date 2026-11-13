/**
 * Library Prefix/Remapping Utilities
 *
 * WHY: Libraries need to write clean, domain-focused code (ctx.tag.status(200))
 * while avoiding naming conflicts when composed with other libraries.
 *
 * HOW: At composition time (cold path), we create immutable Arrow remap descriptors.
 * Child buffers retain canonical schema/layout.
 *
 * Per specs/lmao/01e_library_integration_pattern.md:
 * - Libraries define clean schemas without prefixes (status, method, url)
 * - Prefixing happens at composition time (http_status, http_method, http_url)
 * - Zero hot path overhead - remapping is composed at module creation
 *
 * WHAT: This module provides schema/prefix mapping utilities and immutable remap
 * descriptors for cold Arrow conversion.
 */

import type { RemapDescriptor, RemappedColumn } from './logBinding.js';
import type { LogSchema } from './schema/types.js';
/**
 * Create immutable output-to-source metadata for cold Arrow traversal.
 *
 * The mapping is already fully composed by OpGroup prefix/map operations. Keeping
 * both the frozen lookup and output schema entries avoids wrapping child buffers
 * or reconstructing schema metadata during each traversal.
 */
export function createRemapDescriptor(
  sourceSchema: LogSchema,
  outputToSourceMapping: Record<string, string>,
): RemapDescriptor {
  const sourceNames: Record<string, string> = {};
  const columns: RemappedColumn[] = [];

  for (const [outputName, sourceName] of Object.entries(outputToSourceMapping)) {
    sourceNames[outputName] = sourceName;
    const fieldSchema = sourceSchema.fields[sourceName];
    if (fieldSchema !== undefined) {
      const column: RemappedColumn = Object.freeze([
        outputName,
        sourceName,
        fieldSchema,
        `${sourceName}_values`,
        `${sourceName}_nulls`,
      ]);
      columns.push(column);
    }
  }

  return Object.freeze({
    sourceNames: Object.freeze(sourceNames),
    columns: Object.freeze(columns),
  });
}

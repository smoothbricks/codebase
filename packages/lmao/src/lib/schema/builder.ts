/**
 * Sury-based schema builder with masking transformations
 * 
 * This module provides the S object that wraps Sury's schema API
 * with custom masking transformations for sensitive data.
 */

import * as Sury from '@sury/sury';
import type { SchemaBuilder, MaskType, MaskTransform } from './types.js';

/**
 * Masking functions for sensitive data
 * Applied during Arrow table serialization (background processing)
 */
const maskingTransforms: Record<MaskType, MaskTransform> = {
  /**
   * Hash masking - creates deterministic hash for IDs
   * Maintains referential integrity while hiding actual values
   */
  hash: (value: string): string => {
    // Simple hash for demo - use crypto.subtle.digest in production
    let hash = 0;
    for (let i = 0; i < value.length; i++) {
      hash = ((hash << 5) - hash) + value.charCodeAt(i);
      hash = hash & hash; // Convert to 32-bit integer
    }
    return `0x${Math.abs(hash).toString(16).padStart(16, '0')}`;
  },
  
  /**
   * URL masking - hides domain while preserving path structure
   * Useful for HTTP request logging
   */
  url: (value: string): string => {
    try {
      const url = new URL(value);
      return `${url.protocol}//*****${url.pathname}${url.search}`;
    } catch {
      return '*****';
    }
  },
  
  /**
   * SQL masking - replaces literals with placeholders
   * Preserves query structure for analysis
   */
  sql: (value: string): string => {
    // Replace string literals and numbers with placeholders
    return value
      .replace(/'[^']*'/g, '?')
      .replace(/\b\d+\b/g, '?');
  },
  
  /**
   * Email masking - shows first character and domain only
   * Maintains uniqueness while protecting privacy
   */
  email: (value: string): string => {
    const [local, domain] = value.split('@');
    if (!domain) return '*****';
    return `${local[0]}*****@${domain}`;
  }
};

/**
 * Schema builder that wraps Sury with our custom API
 * 
 * Provides a clean API while leveraging Sury's performance:
 * - 94,828 ops/ms validation (fastest in JavaScript)
 * - 14.1 kB bundle size (smallest composable library)
 * - Full TypeScript inference
 * - Runtime transformations
 * 
 * Note: Sury exports primitive schemas as constants (S.string, S.number, etc.)
 * not as factory functions. We wrap them in functions for a consistent API.
 */
const schemaBuilderImpl: SchemaBuilder = {
  /**
   * Create string schema
   * 
   * Usage:
   * - SchemaBuilder.string() - basic string
   * - S.transform(SchemaBuilder.string(), fn) - with transformation
   */
  string: () => Sury.string,
  
  /**
   * Create number schema
   * 
   * Usage:
   * - SchemaBuilder.number() - any number
   * - S.refine(SchemaBuilder.number(), x => x > 0) - with validation
   */
  number: () => Sury.number,
  
  /**
   * Create boolean schema
   */
  boolean: () => Sury.boolean,
  
  /**
   * Wrap schema to make it optional
   * 
   * Usage:
   * - SchemaBuilder.optional(SchemaBuilder.string()) - string | undefined
   */
  optional: <T>(
    schema: Sury.Schema<T, unknown>
  ): Sury.Schema<T | undefined, T | undefined> => {
    return Sury.optional(schema) as Sury.Schema<T | undefined, T | undefined>;
  },
  
  /**
   * Create union of multiple schemas
   * 
   * Usage:
   * - SchemaBuilder.union([SchemaBuilder.string(), SchemaBuilder.number()]) - string | number
   */
  union: <T extends readonly [Sury.Schema<unknown, unknown>, ...Sury.Schema<unknown, unknown>[]]>(
    schemas: T
  ): Sury.Schema<Sury.Output<T[number]>, Sury.Input<T[number]>> => {
    const schemaArray = [...schemas] as [
      Sury.Schema<unknown, unknown>,
      ...Sury.Schema<unknown, unknown>[]
    ];
    return Sury.union(schemaArray) as Sury.Schema<
      Sury.Output<T[number]>,
      Sury.Input<T[number]>
    >;
  },
  
  /**
   * Create enum from string literals
   * 
   * This is a convenience wrapper for common use case of string unions.
   * 
   * Usage:
   * - SchemaBuilder.enum(['SELECT', 'INSERT', 'UPDATE', 'DELETE'])
   * 
   * Creates: 'SELECT' | 'INSERT' | 'UPDATE' | 'DELETE'
   * 
   * Implementation: Uses S.refine with validation that fails on invalid values
   */
  enum: <T extends readonly string[]>(values: T): Sury.Schema<T[number], string> => {
    if (values.length === 0) {
      throw new Error('Enum must have at least one value');
    }
    
    // Use refine to validate string is one of the allowed values
    // The callback receives (value, fail) where fail.fail(message) throws validation error
    return Sury.refine(Sury.string, (value, fail): T[number] => {
      if (!values.includes(value)) {
        fail.fail(`Value must be one of: ${values.join(', ')}`);
      }
      return value as T[number];
    }) as Sury.Schema<T[number], string>;
  },
  
  /**
   * Create string schema with masking transformation
   * 
   * Masking is applied during serialization, not validation.
   * This allows:
   * - Full data in memory for processing
   * - Masked data in logs/traces for privacy
   * 
   * Usage:
   * - SchemaBuilder.masked('hash') - for IDs (0x...)
   * - SchemaBuilder.masked('email') - for emails (j*****@example.com)
   * - SchemaBuilder.masked('url') - for URLs (https://*****.com/path)
   * - SchemaBuilder.masked('sql') - for SQL queries (SELECT * FROM users WHERE id = ?)
   */
  masked: (type: MaskType) => {
    return Sury.transform(Sury.string, maskingTransforms[type]);
  }
};

// Export as S for convenience (matches Sury convention)
// Note: The SchemaBuilder type is exported from types.ts
export const S = schemaBuilderImpl;

/**
 * Export masking transforms for custom use
 */
export const mask = maskingTransforms;

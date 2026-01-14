/**
 * Strongly-typed trace facts using TypeScript template literal types.
 *
 * Instead of `facts: string[]`, we use branded template literal types that
 * ensure facts follow a consistent format and enable autocomplete.
 *
 * Format: `${namespace}:${name}: ${content}`
 *
 * Examples:
 *   - `span:fetch-user: started`
 *   - `span:fetch-user: ok`
 *   - `span:fetch-user: err(NOT_FOUND)`
 *   - `log:info: Processing order`
 *   - `tag:userId: 123`
 *   - `scope:requestId: req-abc`
 *   - `ff:darkMode: true`
 *
 * @module testing/facts
 */

// =============================================================================
// FACT NAMESPACES
// =============================================================================

/**
 * All valid fact namespaces.
 */
export type FactNamespace = 'span' | 'log' | 'tag' | 'scope' | 'ff' | 'metric';

// =============================================================================
// SPAN FACTS
// =============================================================================

/**
 * Span lifecycle states.
 */
export type SpanState = 'started' | 'ok' | `err(${string})` | `exception(${string})`;

/**
 * A span fact: `span:${name}: ${state}`
 *
 * @example
 * type F1 = SpanFact<'fetch-user', 'started'>;  // "span:fetch-user: started"
 * type F2 = SpanFact<'fetch-user', 'ok'>;       // "span:fetch-user: ok"
 * type F3 = SpanFact<'fetch-user', 'err(NOT_FOUND)'>; // "span:fetch-user: err(NOT_FOUND)"
 */
export type SpanFact<Name extends string = string, State extends SpanState = SpanState> = `span:${Name}: ${State}`;

/**
 * Type guard for span facts.
 */
export function isSpanFact(fact: TraceFact): fact is SpanFact {
  return fact.startsWith('span:');
}

/**
 * Create a span:started fact.
 */
export function spanStarted<N extends string>(name: N): SpanFact<N, 'started'> {
  return `span:${name}: started` as SpanFact<N, 'started'>;
}

/**
 * Create a span:ok fact.
 */
export function spanOk<N extends string>(name: N): SpanFact<N, 'ok'> {
  return `span:${name}: ok` as SpanFact<N, 'ok'>;
}

/**
 * Create a span:err fact.
 */
export function spanErr<N extends string, C extends string>(name: N, code: C): SpanFact<N, `err(${C})`> {
  return `span:${name}: err(${code})` as SpanFact<N, `err(${C})`>;
}

/**
 * Create a span:exception fact.
 */
export function spanException<N extends string, M extends string>(name: N, message: M): SpanFact<N, `exception(${M})`> {
  return `span:${name}: exception(${message})` as SpanFact<N, `exception(${M})`>;
}

// =============================================================================
// LOG FACTS
// =============================================================================

/**
 * Log levels.
 */
export type LogLevel = 'debug' | 'info' | 'warn' | 'error';

/**
 * A log fact: `log:${level}: ${message}`
 *
 * @example
 * type F = LogFact<'info', 'Processing order'>; // "log:info: Processing order"
 */
export type LogFact<Level extends LogLevel = LogLevel, Message extends string = string> = `log:${Level}: ${Message}`;

/**
 * Type guard for log facts.
 */
export function isLogFact(fact: TraceFact): fact is LogFact {
  return fact.startsWith('log:');
}

/**
 * Create a log fact.
 */
export function logFact<L extends LogLevel, M extends string>(level: L, message: M): LogFact<L, M> {
  return `log:${level}: ${message}` as LogFact<L, M>;
}

// Convenience functions
export const logDebug = <M extends string>(msg: M) => logFact('debug', msg);
export const logInfo = <M extends string>(msg: M) => logFact('info', msg);
export const logWarn = <M extends string>(msg: M) => logFact('warn', msg);
export const logError = <M extends string>(msg: M) => logFact('error', msg);

// =============================================================================
// TAG FACTS
// =============================================================================

/**
 * A tag fact: `tag:${key}: ${value}`
 *
 * @example
 * type F = TagFact<'userId', '123'>; // "tag:userId: 123"
 */
export type TagFact<Key extends string = string, Value extends string = string> = `tag:${Key}: ${Value}`;

/**
 * Type guard for tag facts.
 */
export function isTagFact(fact: TraceFact): fact is TagFact {
  return fact.startsWith('tag:');
}

/**
 * Create a tag fact.
 */
export function tagFact<K extends string, V extends string | number | boolean>(key: K, value: V): TagFact<K, `${V}`> {
  return `tag:${key}: ${value}` as TagFact<K, `${V}`>;
}

// =============================================================================
// SCOPE FACTS
// =============================================================================

/**
 * A scope fact: `scope:${key}: ${value}`
 *
 * Scope facts represent values propagated through the span tree.
 *
 * @example
 * type F = ScopeFact<'requestId', 'req-abc'>; // "scope:requestId: req-abc"
 */
export type ScopeFact<Key extends string = string, Value extends string = string> = `scope:${Key}: ${Value}`;

/**
 * Type guard for scope facts.
 */
export function isScopeFact(fact: TraceFact): fact is ScopeFact {
  return fact.startsWith('scope:');
}

/**
 * Create a scope fact.
 */
export function scopeFact<K extends string, V extends string | number | boolean>(
  key: K,
  value: V,
): ScopeFact<K, `${V}`> {
  return `scope:${key}: ${value}` as ScopeFact<K, `${V}`>;
}

// =============================================================================
// FEATURE FLAG FACTS
// =============================================================================

/**
 * A feature flag fact: `ff:${flag}: ${value}`
 *
 * @example
 * type F = FFFact<'darkMode', 'true'>; // "ff:darkMode: true"
 */
export type FFFact<Flag extends string = string, Value extends string = string> = `ff:${Flag}: ${Value}`;

/**
 * Type guard for feature flag facts.
 */
export function isFFFact(fact: TraceFact): fact is FFFact {
  return fact.startsWith('ff:');
}

/**
 * Create a feature flag fact.
 */
export function ffFact<F extends string, V extends string | number | boolean>(flag: F, value: V): FFFact<F, `${V}`> {
  return `ff:${flag}: ${value}` as FFFact<F, `${V}`>;
}

// =============================================================================
// METRIC FACTS
// =============================================================================

/**
 * A metric fact: `metric:${name}: ${value}`
 *
 * @example
 * type F = MetricFact<'duration_ms', '42'>; // "metric:duration_ms: 42"
 */
export type MetricFact<Name extends string = string, Value extends string = string> = `metric:${Name}: ${Value}`;

/**
 * Type guard for metric facts.
 */
export function isMetricFact(fact: TraceFact): fact is MetricFact {
  return fact.startsWith('metric:');
}

/**
 * Create a metric fact.
 */
export function metricFact<N extends string, V extends number>(name: N, value: V): MetricFact<N, `${V}`> {
  return `metric:${name}: ${value}` as MetricFact<N, `${V}`>;
}

// =============================================================================
// UNION TYPE: TraceFact
// =============================================================================

/**
 * Any valid trace fact.
 *
 * This is a union of all fact types, providing type safety while
 * allowing heterogeneous fact arrays.
 */
export type TraceFact = SpanFact | LogFact | TagFact | ScopeFact | FFFact | MetricFact;

// =============================================================================
// FACT PARSING
// =============================================================================

/**
 * Parsed fact structure.
 */
export interface ParsedFact {
  namespace: FactNamespace;
  name: string;
  content: string;
  raw: TraceFact;
}

/**
 * Parse a fact string into its components.
 */
export function parseFact(fact: TraceFact): ParsedFact {
  const colonIndex = fact.indexOf(':');
  const secondColonIndex = fact.indexOf(':', colonIndex + 1);

  const namespace = fact.slice(0, colonIndex) as FactNamespace;
  const name = fact.slice(colonIndex + 1, secondColonIndex).trim();
  const content = fact.slice(secondColonIndex + 1).trim();

  return { namespace, name, content, raw: fact };
}

// =============================================================================
// FACT ARRAY TYPE
// =============================================================================

/**
 * A readonly array of trace facts with helper methods.
 *
 * This is the primary interface for working with collected facts.
 */
export interface FactArray extends ReadonlyArray<TraceFact> {
  /**
   * Check if a fact exists (exact match).
   */
  has(fact: TraceFact): boolean;

  /**
   * Check if any fact matches a pattern.
   * Pattern supports * wildcards in any segment.
   *
   * @example
   * facts.hasMatch('span:*: ok')        // Any span completed ok
   * facts.hasMatch('span:fetch-*: *')   // Any fetch span, any state
   * facts.hasMatch('log:error: *')      // Any error log
   */
  hasMatch(pattern: string): boolean;

  /**
   * Filter to facts of a specific namespace.
   */
  byNamespace<N extends FactNamespace>(namespace: N): FactArray;

  /**
   * Get all span facts.
   */
  spans(): SpanFact[];

  /**
   * Get all log facts.
   */
  logs(): LogFact[];

  /**
   * Get all tag facts.
   */
  tags(): TagFact[];

  /**
   * Check if facts appear in order (not necessarily adjacent).
   */
  hasInOrder(facts: TraceFact[]): boolean;

  /**
   * Find the index of a fact (or -1 if not found).
   */
  indexOf(fact: TraceFact): number;

  /**
   * Get facts matching a pattern.
   */
  match(pattern: string): FactArray;
}

/**
 * Create a FactArray from raw facts.
 */
export function createFactArray(facts: TraceFact[]): FactArray {
  const arr = [...facts] as TraceFact[] & FactArray;

  arr.has = function (fact: TraceFact): boolean {
    return this.includes(fact);
  };

  arr.hasMatch = function (pattern: string): boolean {
    const regex = patternToRegex(pattern);
    return this.some((f) => regex.test(f));
  };

  arr.byNamespace = function <N extends FactNamespace>(namespace: N): FactArray {
    const filtered = this.filter((f) => f.startsWith(`${namespace}:`));
    return createFactArray(filtered);
  };

  arr.spans = function (): SpanFact[] {
    return this.filter(isSpanFact);
  };

  arr.logs = function (): LogFact[] {
    return this.filter(isLogFact);
  };

  arr.tags = function (): TagFact[] {
    return this.filter(isTagFact);
  };

  arr.hasInOrder = function (expected: TraceFact[]): boolean {
    let lastIndex = -1;
    for (const fact of expected) {
      const index = this.indexOf(fact);
      if (index === -1 || index <= lastIndex) {
        return false;
      }
      lastIndex = index;
    }
    return true;
  };

  arr.match = function (pattern: string): FactArray {
    const regex = patternToRegex(pattern);
    const filtered = this.filter((f) => regex.test(f));
    return createFactArray(filtered);
  };

  return arr;
}

// =============================================================================
// PATTERN MATCHING
// =============================================================================

/**
 * Convert a glob-like pattern to a regex.
 *
 * Supports:
 * - `*` matches any characters except `:`
 * - `**` matches any characters including `:`
 */
function patternToRegex(pattern: string): RegExp {
  let result = '';
  let i = 0;

  while (i < pattern.length) {
    if (pattern[i] === '*') {
      if (pattern[i + 1] === '*') {
        // ** matches anything
        result += '.*';
        i += 2;
      } else {
        // * matches anything except colon
        result += '[^:]*';
        i += 1;
      }
    } else if (/[.+^${}()|[\]\\]/.test(pattern[i])) {
      // Escape regex special chars
      result += `\\${pattern[i]}`;
      i += 1;
    } else {
      result += pattern[i];
      i += 1;
    }
  }

  return new RegExp(`^${result}$`);
}

// =============================================================================
// SCHEMA-AWARE FACT TYPES (for type inference from LogSchema)
// =============================================================================

/**
 * Extract tag fact types from a schema.
 *
 * Given a LogSchema, this produces a union of valid TagFact types
 * for that schema's fields.
 *
 * @example
 * type Schema = { userId: S.category(), count: S.number() };
 * type Tags = SchemaTagFacts<Schema>;
 * // = TagFact<'userId', string> | TagFact<'count', string>
 */
export type SchemaTagFacts<Schema extends Record<string, unknown>> = {
  [K in keyof Schema & string]: TagFact<K, string>;
}[keyof Schema & string];

/**
 * Extract scope fact types from a schema.
 */
export type SchemaScopeFacts<Schema extends Record<string, unknown>> = {
  [K in keyof Schema & string]: ScopeFact<K, string>;
}[keyof Schema & string];

/**
 * Fixed-position TagWriter and ResultWriter classes.
 *
 * TagWriter uses source-authored constructors and schema-driven prototype methods so
 * Hermes can compile the implementation ahead of time. ResultWriter selects between a
 * compiled renderer (new Function, string-codegen runtimes) and a closure-composed
 * materializer (workerd-safe, no eval) via activeMaterializerMode().
 *
 * These classes provide fluent setter methods that write to a fixed position in a SpanBuffer:
 * - TagWriter: writes to position 0 (span-start row) via ctx.tag.userId("123")
 * - ResultWriter: writes to position 1 (result row) via ctx.ok(data).userId("123")
 *
 * WHY Fixed Positions:
 * - Row 0: span-start entry (written at span creation, attributes set via ctx.tag)
 * - Row 1: span-end entry (written at ctx.ok()/ctx.err(), attributes set via fluent API)
 *
 * Each wrapper stores only the SpanContext state identity. Setters write through
 * `state._spanBuffer` to their embedded row and return the same wrapper for fluent chaining.
 */

import { activeMaterializerMode, bufferHelpers, type ColumnEntry } from '@smoothbricks/arrow-builder';
import { resolveEnumLookupDescriptor, type SchemaEnumLookupDescriptor } from '../enumMetadata.js';
import type { MessageLayoutFamily, MessagePhysicalLayout } from '../runtimeHint.js';
import type { InferSchema, LogSchema } from '../schema/types.js';
import type { TimestampAppendPrimitive } from '../traceRoot.js';
import type { AnySpanBuffer } from '../types.js';

/** Extension methods injected into a generated fixed-position writer class. */
export interface FixedPositionWriterExtension {
  methods?: string;
}

/**
 * TagWriter interface - fluent setter API for span-start attributes (row 0)
 */
export type TagWriter<T extends LogSchema> = {
  /**
   * Bulk set multiple attributes at once.
   */
  with(attributes: Partial<InferSchema<T>>): TagWriter<T>;
  /**
   * Set the reserved `uint64_value` column for this span-start row.
   */
  uint64_value(value: bigint): TagWriter<T>;
} & {
  /**
   * Individual attribute setters - each returns `this` for chaining.
   */
  [K in keyof InferSchema<T>]: (value: InferSchema<T>[K]) => TagWriter<T>;
};

/** SpanContext-owned hot writer state shared by logger, tag, and result writers. */
export interface WriterState {
  /** Immutable span root used by fixed row 0/1 writers. */
  readonly _spanBuffer: AnySpanBuffer;
  /** Mutable active buffer used by append writers and updated on overflow. */
  _buffer: AnySpanBuffer;
  /** Plan-bound timestamp append operand retained by the SpanContext. */
  readonly _appendLogEntry: TimestampAppendPrimitive;
  readonly _physicalLayoutPlan: {
    readonly ResultWriterClass: ResultWriterConstructor;
    readonly enumLookup: SchemaEnumLookupDescriptor;
  };
  /** Append one dynamic row, centralizing overflow and active-buffer updates. */
  _appendWriterEntry(entryType: number): number;
}

/** Result-row writer with schema-specific fluent setters. */
export type ResultWriter<T extends LogSchema, R = unknown, E = unknown> = {
  /** Phantom parameters preserve the public generic contract without duplicating result values. */
  readonly _resultType?: R;
  readonly _errorType?: E;
  with(attributes: Partial<InferSchema<T>>): ResultWriter<T, R, E>;
  message(text: string): ResultWriter<T, R, E>;
  line(lineNumber: number): ResultWriter<T, R, E>;
  uint64_value(value: bigint): ResultWriter<T, R, E>;
} & {
  [K in keyof InferSchema<T>]: (value: InferSchema<T>[K]) => ResultWriter<T, R, E>;
};

export type TagWriterConstructor<T extends LogSchema> = new (state: WriterState) => TagWriter<T>;

export type ResultWriterConstructor = new <T extends LogSchema = LogSchema, R = unknown, E = unknown>(
  state: WriterState,
) => ResultWriter<T, R, E>;

function isTagWriterConstructor<T extends LogSchema>(value: unknown): value is TagWriterConstructor<T> {
  return typeof value === 'function';
}

function isResultWriterConstructor(value: unknown): value is ResultWriterConstructor {
  return typeof value === 'function';
}

/** Generate a state-bound setter for one schema attribute and literal row. */
function generateSetterMethod(fieldName: string, enumEncoderName: string | undefined, position: number): string {
  const value = enumEncoderName ? `${enumEncoderName}(value)` : 'value';
  return `
    ${fieldName}(value) {
      this._state._spanBuffer.${fieldName}(${position}, ${value});
      return this;
    }`;
}

/**
 * Generate with() method code - bulk attribute setting.
 * UNROLLED per-column code for zero Object.entries overhead.
 */
function generateWithMethod(
  schemaFields: readonly ColumnEntry[],
  enumEncoderNames: Readonly<Record<string, string>>,
  position: number,
): string {
  const columnWrites = schemaFields.map(([fieldName]) => {
    const enumEncoderName = enumEncoderNames[fieldName];
    const valueExpr = enumEncoderName ? `${enumEncoderName}(attributes.${fieldName})` : `attributes.${fieldName}`;

    return `
      if ('${fieldName}' in attributes && attributes.${fieldName} !== null && attributes.${fieldName} !== undefined) {
        this._state._spanBuffer.${fieldName}(${position}, ${valueExpr});
      }`;
  });

  return `
    with(attributes) {
      ${columnWrites.join('\n')}
      return this;
    }`;
}

//#region smoo/lmao!n/codegen-architecture
/**
 * Generate the complete fixed-position writer class code.
 *
 * @param schema - Tag attribute schema defining columns
 * @param position - Fixed position to write to (0 for TagWriter, 1 for ResultWriter)
 * @param className - Name for the generated class
 * @param extension - Optional extension for constructor code, methods, params
 * @returns JavaScript code string for the class
 */
export function generateFixedPositionWriterClass(
  schema: LogSchema,
  position: number,
  className: string,
  extension?: FixedPositionWriterExtension,
  eagerColumns: readonly string[] = [],
  enumLookup: SchemaEnumLookupDescriptor = resolveEnumLookupDescriptor(schema),
): string {
  const schemaFields = schema._columns;
  void eagerColumns;
  const enumEncoderNames: Record<string, string> = Object.create(null);
  const enumEncoderBindings = enumLookup.ordered.map(({ fieldName }, index) => {
    const encoderName = `encodeEnum${index}`;
    enumEncoderNames[fieldName] = encoderName;
    return `  const ${encoderName} = enumLookup.byField[${JSON.stringify(fieldName)}].encode;`;
  });

  const setterMethods = schemaFields.map(([fieldName]) =>
    generateSetterMethod(fieldName, enumEncoderNames[fieldName], position),
  );

  const withMethod = generateWithMethod(schemaFields, enumEncoderNames, position);

  const constructorSignature = 'state';
  const constructorBody = ['    this._state = state;'];

  // Build extension methods (additional methods injected by the caller)
  const extensionMethods = extension?.methods
    ? '\n' +
      extension.methods
        .trim()
        .split('\n')
        .map((line) => `    ${line}`)
        .join('\n')
    : '';

  // Generated class stores only `_state`; every method embeds its fixed row literal.
  const classCode = `
  'use strict';
${enumEncoderBindings.join('\n')}

  class ${className} {
    constructor(${constructorSignature}) {
${constructorBody.join('\n')}
    }

    ${withMethod}

    ${setterMethods.join('\n')}
${extensionMethods}
  }

  return ${className};
`;

  return classCode;
}

// ============================================================================
// Class Caches
// ============================================================================

/**
 * Cache for TagWriter classes (position 0).
 * WeakMap allows garbage collection when schema is no longer referenced.
 */
const tagWriterClassCache = new WeakMap<LogSchema, Map<string, unknown>>();

/**
 * Cache for ResultWriter classes (position 1).
 */
const resultWriterClassCache = new WeakMap<LogSchema, Map<string, unknown>>();

// ============================================================================
// Closure-composed materializer internals (no eval; shared by tag/result rows)
// ============================================================================

interface RuntimeFixedWriter {
  readonly _state: WriterState;
}

export interface FixedFieldPlan {
  readonly fieldName: string;
  readonly encode: ((value: unknown) => number) | undefined;
}

/** Invoke one schema-specific SpanBuffer setter without generating source. */
function writeFixedField(writer: RuntimeFixedWriter, fieldName: string, position: number, value: unknown): void {
  const buffer = writer._state._spanBuffer;
  const columnWriter: unknown = Reflect.get(buffer, fieldName);
  if (typeof columnWriter !== 'function') {
    throw new TypeError(`SpanBuffer column writer ${JSON.stringify(fieldName)} is not callable`);
  }
  columnWriter.call(buffer, position, value);
}

/** Preserve the generated writer's schema order and enum-binding semantics. */
export function createFixedFieldPlans(
  schema: LogSchema,
  enumLookup: SchemaEnumLookupDescriptor,
): readonly FixedFieldPlan[] {
  const enumFieldNames = new Set<string>();
  for (const { fieldName } of enumLookup.ordered) enumFieldNames.add(fieldName);

  return schema._columns.map(([fieldName]) => {
    if (!enumFieldNames.has(fieldName)) return { fieldName, encode: undefined };

    const descriptor = enumLookup.byField[fieldName];
    if (!descriptor) {
      throw new TypeError(`Enum lookup for ${JSON.stringify(fieldName)} is missing`);
    }
    return { fieldName, encode: descriptor.encode };
  });
}

/** Compiled class members are non-enumerable; every closure-installed member must match. */
function asClassMemberDescriptor(descriptor: PropertyDescriptor): PropertyDescriptor {
  descriptor.enumerable = false;
  return descriptor;
}

/** Create a class-method descriptor whose function name is the schema field name. */
function createFieldSetterDescriptor(plan: FixedFieldPlan, position: number): PropertyDescriptor {
  const encode = plan.encode;
  const methods = encode
    ? {
        [plan.fieldName](this: RuntimeFixedWriter, value: string) {
          writeFixedField(this, plan.fieldName, position, encode(value));
          return this;
        },
      }
    : {
        [plan.fieldName](this: RuntimeFixedWriter, value: unknown) {
          writeFixedField(this, plan.fieldName, position, value);
          return this;
        },
      };
  const descriptor = Object.getOwnPropertyDescriptor(methods, plan.fieldName);
  if (!descriptor) throw new Error(`Failed to create writer setter ${JSON.stringify(plan.fieldName)}`);
  return asClassMemberDescriptor(descriptor);
}

/** Install the schema-specific fluent API with class-member-equivalent descriptors. */
export function installFixedWriterMethods(
  prototype: object,
  plans: readonly FixedFieldPlan[],
  position: number,
): void {
  const bulkMethods = {
    with(this: RuntimeFixedWriter, attributes: Readonly<Record<string, unknown>>) {
      for (const plan of plans) {
        if (
          plan.fieldName in attributes &&
          attributes[plan.fieldName] !== null &&
          attributes[plan.fieldName] !== undefined
        ) {
          const value = attributes[plan.fieldName];
          writeFixedField(this, plan.fieldName, position, plan.encode ? plan.encode(value) : value);
        }
      }
      return this;
    },
    uint64_value(this: RuntimeFixedWriter, value: bigint) {
      this._state._spanBuffer.uint64_value(position, value);
      return this;
    },
  };
  const withDescriptor = Object.getOwnPropertyDescriptor(bulkMethods, 'with');
  if (!withDescriptor) throw new Error('Failed to create writer.with');
  Object.defineProperty(prototype, 'with', asClassMemberDescriptor(withDescriptor));

  for (const plan of plans) {
    Object.defineProperty(prototype, plan.fieldName, createFieldSetterDescriptor(plan, position));
  }

  const uint64Descriptor = Object.getOwnPropertyDescriptor(bulkMethods, 'uint64_value');
  if (!uint64Descriptor) throw new Error('Failed to create writer.uint64_value');
  Object.defineProperty(prototype, 'uint64_value', asClassMemberDescriptor(uint64Descriptor));
}

// ============================================================================
// TagWriter API
// ============================================================================

/** Build one source-authored constructor for an existing TagWriter cache key. */
function createStaticTagWriterConstructor(schema: LogSchema, enumLookup: SchemaEnumLookupDescriptor): unknown {
  class GeneratedTagWriter {
    readonly _state: WriterState;

    constructor(state: WriterState) {
      this._state = state;
    }
  }

  installFixedWriterMethods(GeneratedTagWriter.prototype, createFixedFieldPlans(schema, enumLookup), 0);
  return GeneratedTagWriter;
}

/**
 * Get or create a cached TagWriter class for a schema.
 *
 * @param schema - Tag attribute schema
 * @returns TagWriter class constructor
 */
export function getTagWriterClass<T extends LogSchema>(
  schema: T,
  eagerColumns: readonly string[] = [],
  enumLookup: SchemaEnumLookupDescriptor = resolveEnumLookupDescriptor(schema),
): TagWriterConstructor<T> {
  const cacheKey = eagerColumns.join('\u0000');
  let classes = tagWriterClassCache.get(schema);
  let WriterClass = classes?.get(cacheKey);

  if (!WriterClass) {
    WriterClass = createStaticTagWriterConstructor(schema, enumLookup);

    if (!isTagWriterConstructor<LogSchema>(WriterClass)) {
      throw new Error('Failed to create TagWriter constructor');
    }

    classes ??= new Map();
    classes.set(cacheKey, WriterClass);
    tagWriterClassCache.set(schema, classes);
  }

  if (!isTagWriterConstructor<T>(WriterClass)) {
    throw new Error('Invalid cached TagWriter constructor');
  }

  return WriterClass;
}

/** Create a TagWriter bound to an existing SpanContext writer state. */
export function createTagWriter<T extends LogSchema>(schema: T, state: WriterState): TagWriter<T> {
  return new (getTagWriterClass(schema))(state);
}

// ============================================================================
// ResultWriter API
// ============================================================================

/** Result-row system setters, specialized once for the plan's message layout family. */
function createResultWriterExtension(messageLayoutFamily: MessageLayoutFamily): FixedPositionWriterExtension {
  const messageWrite =
    messageLayoutFamily === 'static-only'
      ? 'this._state._spanBuffer._terminalMessage = text;'
      : 'this._state._spanBuffer.message_values[1] = text;';
  return {
    methods: `
message(text) {
  ${messageWrite}
  return this;
}

line(lineNumber) {
  this._state._spanBuffer.line(1, lineNumber);
  return this;
}

uint64_value(value) {
  this._state._spanBuffer.uint64_value(1, value);
  return this;
}
`,
  };
}

/**
 * Generate ResultWriter class code for a schema.
 * ResultWriter writes to position 1 (result row).
 */
export function generateResultWriterClass(
  schema: LogSchema,
  messageLayoutFamily: MessageLayoutFamily = 'mixed',
  _messagePhysicalLayout: MessagePhysicalLayout = 'current',
  eagerColumns: readonly string[] = [],
  enumLookup: SchemaEnumLookupDescriptor = resolveEnumLookupDescriptor(schema),
): string {
  return generateFixedPositionWriterClass(
    schema,
    1,
    'GeneratedResultWriter',
    createResultWriterExtension(messageLayoutFamily),
    eagerColumns,
    enumLookup,
  );
}

/** Result-row message lane accessor for mixed/dynamic layouts. */
function messageLaneOf(buffer: AnySpanBuffer): (string | undefined)[] {
  const lane = buffer.message_values;
  if (lane === undefined) {
    throw new TypeError('SpanBuffer has no message_values lane for the result row');
  }
  return lane;
}

/** Install result-row system setters mirroring createResultWriterExtension without source text. */
export function installResultSystemMethods(prototype: object, messageLayoutFamily: MessageLayoutFamily): void {
  const methods =
    messageLayoutFamily === 'static-only'
      ? {
          message(this: RuntimeFixedWriter, text: string) {
            this._state._spanBuffer._terminalMessage = text;
            return this;
          },
        }
      : {
          message(this: RuntimeFixedWriter, text: string) {
            messageLaneOf(this._state._spanBuffer)[1] = text;
            return this;
          },
        };
  const messageDescriptor = Object.getOwnPropertyDescriptor(methods, 'message');
  if (!messageDescriptor) throw new Error('Failed to create ResultWriter.message');
  Object.defineProperty(prototype, 'message', asClassMemberDescriptor(messageDescriptor));

  const lineMethods = {
    line(this: RuntimeFixedWriter, lineNumber: number) {
      this._state._spanBuffer.line(1, lineNumber);
      return this;
    },
  };
  const lineDescriptor = Object.getOwnPropertyDescriptor(lineMethods, 'line');
  if (!lineDescriptor) throw new Error('Failed to create ResultWriter.line');
  Object.defineProperty(prototype, 'line', asClassMemberDescriptor(lineDescriptor));
}

/** Assemble a ResultWriter class from closures (workerd-safe: no string codegen). */
function createClosureResultWriterConstructor(
  schema: LogSchema,
  messageLayoutFamily: MessageLayoutFamily,
  enumLookup: SchemaEnumLookupDescriptor,
): unknown {
  class GeneratedResultWriter {
    readonly _state: WriterState;

    constructor(state: WriterState) {
      this._state = state;
    }
  }

  installFixedWriterMethods(GeneratedResultWriter.prototype, createFixedFieldPlans(schema, enumLookup), 1);
  installResultSystemMethods(GeneratedResultWriter.prototype, messageLayoutFamily);
  return GeneratedResultWriter;
}

/** Compile the rendered result-writer source with new Function (string-codegen runtimes only). */
function compileResultWriterClass(
  schema: LogSchema,
  messageLayoutFamily: MessageLayoutFamily,
  messagePhysicalLayout: MessagePhysicalLayout,
  eagerColumns: readonly string[],
  enumLookup: SchemaEnumLookupDescriptor,
): unknown {
  const classCode = generateResultWriterClass(
    schema,
    messageLayoutFamily,
    messagePhysicalLayout,
    eagerColumns,
    enumLookup,
  ).trim();

  const factory = new Function('helpers', 'enumLookup', classCode);
  return factory(bufferHelpers, enumLookup);
}

/**
 * Get or create a cached ResultWriter class for a schema.
 *
 * Selects the compiled or closure-composed materializer via activeMaterializerMode();
 * cache keys are mode-prefixed so an override flip never returns the wrong kind.
 *
 * @param schema - Tag attribute schema
 * @returns ResultWriter class constructor
 */
export function getResultWriterClass<T extends LogSchema>(
  schema: T,
  messageLayoutFamily: MessageLayoutFamily = 'mixed',
  messagePhysicalLayout: MessagePhysicalLayout = 'current',
  eagerColumns: readonly string[] = [],
  enumLookup: SchemaEnumLookupDescriptor = resolveEnumLookupDescriptor(schema),
): ResultWriterConstructor {
  const mode = activeMaterializerMode();
  const cacheKey = `${mode}:${messageLayoutFamily}:${messagePhysicalLayout}:${eagerColumns.join('\u0000')}`;
  let familyClasses = resultWriterClassCache.get(schema);
  let WriterClass = familyClasses?.get(cacheKey);

  if (!WriterClass) {
    WriterClass =
      mode === 'compiled'
        ? compileResultWriterClass(schema, messageLayoutFamily, messagePhysicalLayout, eagerColumns, enumLookup)
        : createClosureResultWriterConstructor(schema, messageLayoutFamily, enumLookup);

    if (!isResultWriterConstructor(WriterClass)) {
      throw new Error('Failed to generate ResultWriter constructor');
    }

    familyClasses ??= new Map();
    familyClasses.set(cacheKey, WriterClass);
    resultWriterClassCache.set(schema, familyClasses);
  }

  if (!isResultWriterConstructor(WriterClass)) {
    throw new Error('Invalid cached ResultWriter constructor');
  }

  return WriterClass;
}

/** Create a ResultWriter bound to an existing SpanContext writer state. */
export function createResultWriter<T extends LogSchema, R = unknown, E = unknown>(
  schema: T,
  state: WriterState,
): ResultWriter<T, R, E> {
  return new (getResultWriterClass(
    schema,
    state._spanBuffer._messageLayoutFamily,
    state._spanBuffer._messagePhysicalLayout,
  ))<T, R, E>(state);
}
//#endregion smoo/lmao!n/codegen-architecture

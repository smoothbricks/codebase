/**
 * Reader projection (07_api.md, "Reader projection"): writers are strict and readers project.
 * The TypeScript twin of cowshed-core's `api/reader.rs`, walking the same reader table
 * cowshed-api-gen derives from the declarations (`reader.generated.ts`): every object member a
 * declaration names is kept, every other is dropped, at every depth. The strict generated
 * validator then certifies the projection, so a missing field, a retyped value, an unknown
 * variant and members of two arms at once still refuse.
 */

/** The members of one closed object: each wire name with the reader of its value. */
export type Fields<N extends string> = ReadonlyArray<readonly [string, N]>;

/** How a client projects one declared wire value; references name other readers of the table. */
export type Reader<N extends string> =
  /** A value with no declared object inside it. */
  | { readonly kind: 'leaf' }
  | { readonly kind: 'seq'; readonly element: N }
  /** An object keyed by data, every value of one type: no key is dropped. */
  | { readonly kind: 'map'; readonly value: N }
  | { readonly kind: 'tuple'; readonly positions: readonly N[] }
  | { readonly kind: 'record'; readonly fields: Fields<N> }
  /** A unit variant's name, or an object of exactly one variant name. */
  | { readonly kind: 'external'; readonly arms: Fields<N> }
  /** The tag's value names the arm whose object reader reads the members beside it. */
  | { readonly kind: 'internal'; readonly tag: string; readonly arms: Fields<N> }
  | { readonly kind: 'adjacent'; readonly tag: string; readonly content: string; readonly arms: Fields<N> }
  /** An untagged enum's object arms, in declaration order. */
  | { readonly kind: 'untagged'; readonly arms: ReadonlyArray<Fields<N>> };

/** A generated reader table: every name a reader references is one of its own. */
export type Readers<N extends string> = { readonly [Name in N]: Reader<N> };

type JsonObject = { readonly [key: string]: unknown };

function isObject(value: unknown): value is JsonObject {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function lookup<N extends string>(fields: Fields<N>, key: string): N | undefined {
  for (const [name, reader] of fields) {
    if (name === key) {
      return reader;
    }
  }
  return undefined;
}

/**
 * `value` read through the reader `name` of `readers`: each object member its declaration does not
 * name dropped. A value whose shape is not the declared one is returned as it is, for the strict
 * validator to name.
 */
export function project<N extends string>(value: unknown, name: N, readers: Readers<N>): unknown {
  const reader = readers[name];
  switch (reader.kind) {
    case 'leaf':
      return value;
    case 'seq':
      return Array.isArray(value) ? value.map((element) => project(element, reader.element, readers)) : value;
    case 'map':
      return isObject(value)
        ? Object.fromEntries(
            Object.entries(value).map(([key, member]) => [key, project(member, reader.value, readers)]),
          )
        : value;
    case 'tuple':
      return Array.isArray(value)
        ? value.map((element, index) => {
            const position = reader.positions[index];
            return position === undefined ? element : project(element, position, readers);
          })
        : value;
    case 'external': {
      if (!isObject(value)) {
        return value;
      }
      const members = Object.entries(value);
      const only = members.length === 1 ? members[0] : undefined;
      const arm = only === undefined ? undefined : lookup(reader.arms, only[0]);
      return only === undefined || arm === undefined
        ? value
        : Object.fromEntries([[only[0], project(only[1], arm, readers)]]);
    }
    case 'internal': {
      if (!isObject(value)) {
        return value;
      }
      const arm = armOf(value, reader.tag, reader.arms);
      return arm === undefined ? value : object(value, arm, readers, reader.tag);
    }
    case 'adjacent': {
      if (!isObject(value)) {
        return value;
      }
      const arm = armOf(value, reader.tag, reader.arms);
      if (arm === undefined) {
        return value;
      }
      const kept: Array<readonly [string, unknown]> = [[reader.tag, value[reader.tag]]];
      if (Object.hasOwn(value, reader.content)) {
        kept.push([reader.content, project(value[reader.content], arm, readers)]);
      }
      return Object.fromEntries(kept);
    }
    case 'record':
    case 'untagged':
      return isObject(value) ? object(value, name, readers, undefined) : value;
  }
}

/** The arm an enum's tag member names, when it names one. */
function armOf<N extends string>(value: JsonObject, tag: string, arms: Fields<N>): N | undefined {
  const arm = value[tag];
  return typeof arm === 'string' ? lookup(arms, arm) : undefined;
}

/**
 * Projects an object through a `record` or an `untagged` reader, keeping an enclosing enum's `tag`
 * beside the members.
 */
function object<N extends string>(value: JsonObject, name: N, readers: Readers<N>, tag: string | undefined): unknown {
  const reader = readers[name];
  const fields =
    reader.kind === 'record' ? reader.fields : reader.kind === 'untagged' ? chosen(value, reader.arms) : undefined;
  if (fields === undefined) {
    return value;
  }
  const kept: Array<readonly [string, unknown]> = [];
  for (const [key, member] of Object.entries(value)) {
    const field = lookup(fields, key);
    if (field !== undefined) {
      kept.push([key, project(member, field, readers)]);
    } else if (key === tag) {
      kept.push([key, member]);
    }
  }
  return Object.fromEntries(kept);
}

/**
 * The untagged arm: the first that declares every key the object shares with any arm. A key no arm
 * declares (an enclosing tag among them) is a newer writer's, and selects nothing; members of two
 * arms select none, and the strict validator refuses them.
 */
function chosen<N extends string>(value: JsonObject, arms: ReadonlyArray<Fields<N>>): Fields<N> | undefined {
  const keys = Object.keys(value);
  return arms.find((fields) =>
    keys.every((key) => lookup(fields, key) !== undefined || !arms.some((other) => lookup(other, key) !== undefined)),
  );
}

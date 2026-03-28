import { batchType, bool, Column, type DataType, type dictionary, type IntType, type utf8 } from '@uwdata/flechette';

const EMPTY_VALIDITY = new Uint8Array(0);
const BOOL_TYPE_ID = bool().typeId;

export type ArrowIndexArray = Uint8Array | Uint16Array | Uint32Array;
export type ArrowIndexArrayConstructor = Uint8ArrayConstructor | Uint16ArrayConstructor | Uint32ArrayConstructor;
export type BoolType = ReturnType<typeof bool>;
export type DictionaryType = ReturnType<typeof dictionary>;
export type Utf8Type = ReturnType<typeof utf8>;

type ColumnDataBase<TType, TValues = unknown> = {
  type: TType;
  length: number;
  nullCount: number;
  values?: TValues;
  validity?: Uint8Array;
};

export type DictionaryColumnData<TValues = unknown> = ColumnDataBase<DictionaryType, TValues> & {
  dictionary: Column<unknown>;
};
export type Utf8ColumnData = ColumnDataBase<Utf8Type, Uint8Array> & { offsets: Int32Array };
export type BoolColumnData = ColumnDataBase<BoolType, Uint8Array>;
export type GenericColumnData<TType extends DataType = DataType, TValues = unknown> = ColumnDataBase<TType, TValues>;

export function hasTypeId(value: unknown): value is { readonly typeId: unknown } {
  return typeof value === 'object' && value !== null && 'typeId' in value;
}

export function isBoolType(value: unknown): value is BoolType {
  return hasTypeId(value) && value.typeId === BOOL_TYPE_ID;
}

function isArrowIndexArrayConstructor(value: unknown): value is ArrowIndexArrayConstructor {
  return value === Uint8Array || value === Uint16Array || value === Uint32Array;
}

export function hasArrowIndexType(value: unknown): value is { readonly __arrow_index_type: IntType } {
  return typeof value === 'object' && value !== null && '__arrow_index_type' in value;
}

export function hasArrowIndexMetadata(
  value: unknown,
): value is { readonly __arrow_index_type: IntType; readonly __index_array_ctor: ArrowIndexArrayConstructor } {
  return (
    hasArrowIndexType(value) && '__index_array_ctor' in value && isArrowIndexArrayConstructor(value.__index_array_ctor)
  );
}

export function getArrowIndexTypeOr(value: unknown, fallback: IntType): IntType {
  return hasArrowIndexType(value) ? value.__arrow_index_type : fallback;
}

export function getArrowIndexArrayConstructorOr(
  value: unknown,
  fallback: ArrowIndexArrayConstructor = Uint8Array,
): ArrowIndexArrayConstructor {
  return hasArrowIndexMetadata(value) ? value.__index_array_ctor : fallback;
}

export function makeArrowColumn(data: DictionaryColumnData): Column<unknown>;
export function makeArrowColumn(data: Utf8ColumnData): Column<unknown>;
export function makeArrowColumn(data: BoolColumnData): Column<unknown>;
export function makeArrowColumn(data: GenericColumnData): Column<unknown>;
export function makeArrowColumn(
  data: DictionaryColumnData | Utf8ColumnData | BoolColumnData | GenericColumnData,
): Column<unknown> {
  const validity = data.validity ?? EMPTY_VALIDITY;

  if ('dictionary' in data) {
    const batch = new (batchType(data.type))({
      type: data.type,
      length: data.length,
      nullCount: data.nullCount,
      validity,
      values: data.values,
    });
    batch.setDictionary(data.dictionary);
    return new Column([batch]);
  }

  if ('offsets' in data) {
    const batch = new (batchType(data.type))({
      type: data.type,
      length: data.length,
      nullCount: data.nullCount,
      validity,
      offsets: data.offsets,
      values: data.values,
    });
    return new Column([batch]);
  }

  if (isBoolType(data.type)) {
    const batch = new (batchType(data.type))({
      type: data.type,
      length: data.length,
      nullCount: data.nullCount,
      validity,
      values: data.values,
    });
    return new Column([batch]);
  }

  const batch = new (batchType(data.type))({
    type: data.type,
    length: data.length,
    nullCount: data.nullCount,
    validity,
    values: data.values,
  });
  return new Column([batch]);
}

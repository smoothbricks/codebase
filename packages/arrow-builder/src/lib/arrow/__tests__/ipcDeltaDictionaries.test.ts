/**
 * The carried Flechette patch's `deltaDictionaries` option (patches/README.md, "Flechette delta dictionaries"): a
 * stream whose dictionary continues one an earlier stream defined writes its values as a delta from its first batch
 * (columine 10 §5: "The message is a delta exactly when the mark is above zero"), and a call without the option
 * writes the same bytes as before.
 */
import { describe, expect, it } from 'bun:test';
import {
  Column,
  columnFromArray,
  dictionary,
  tableFromColumns,
  tableFromIPC,
  tableToIPC,
  uint32,
  utf8,
} from '@uwdata/flechette';

/** One dictionary column of `values`, a row per value, and the values column its batch holds. */
function dictionaryTable(values: readonly string[]) {
  const column = columnFromArray([...values], dictionary(utf8(), uint32()));
  const held: unknown = column.data[0] === undefined ? undefined : Reflect.get(column.data[0], 'dictionary');
  if (!(held instanceof Column)) throw new Error('a dictionary batch holds its values column');
  return { table: tableFromColumns({ tier: column }), values: held };
}

function encoded(bytes: Uint8Array | null): Uint8Array {
  if (bytes === null) throw new Error('an in-memory sink answers its bytes');
  return bytes;
}

//#region smoo/columine!n/vocab-stream-dictionary
describe('tableToIPC deltaDictionaries', () => {
  it("writes a listed dictionary's first batch as a delta, which no stream may open with", () => {
    const { table, values } = dictionaryTable(['silver', 'gold']);
    const bytes = encoded(tableToIPC(table, { format: 'stream', deltaDictionaries: new Set([values]) }));
    expect(() => tableFromIPC(bytes)).toThrow('Delta update can not be first dictionary batch.');
  });

  it('writes the same bytes as before for a call that lists no dictionary', () => {
    const { table } = dictionaryTable(['silver', 'gold']);
    const plain = encoded(tableToIPC(table, { format: 'stream' }));
    expect(tableFromIPC(plain).getChild('tier')?.toArray()).toEqual(['silver', 'gold']);
    expect(encoded(tableToIPC(table, { format: 'stream', deltaDictionaries: new Set() }))).toEqual(plain);
  });
});
//#endregion smoo/columine!n/vocab-stream-dictionary

import { execFileSync } from 'node:child_process';
import { tableToIPC } from '@uwdata/flechette';
import { MessageHeader } from '../../../extern/flechette/src/constants.js';
import { decodeMessage } from '../../../extern/flechette/src/decode/message.js';
import {
  createDictionary16Data,
  createFloat64Data,
  createTableFromBatches,
  createUint8Data,
  createUtf8Data,
} from '../src/lib/arrow/data.ts';

const OLD_IPC_SCRIPT = '../../extern/arrow-builder-pre-flechette/packages/arrow-builder/benchmarks/ipc-bytes.ts';
const ROWS = 4096;

function makeNullBitmap(length: number): Uint8Array {
  const bytes = Math.ceil(length / 8);
  const bitmap = new Uint8Array(bytes);
  for (let i = 0; i < bytes; i++) {
    bitmap[i] = i % 2 === 0 ? 0b1111_1111 : 0b1111_0000;
  }
  return bitmap;
}

function makeUtf8Fixture(length: number): { data: Uint8Array; offsets: Int32Array } {
  const encoder = new TextEncoder();
  const chunks = new Array<Uint8Array>(length);
  const offsets = new Int32Array(length + 1);
  let total = 0;

  for (let i = 0; i < length; i++) {
    const chunk = encoder.encode(`value-${i % 1000}`);
    chunks[i] = chunk;
    offsets[i] = total;
    total += chunk.length;
  }
  offsets[length] = total;

  const data = new Uint8Array(total);
  let pos = 0;
  for (const chunk of chunks) {
    data.set(chunk, pos);
    pos += chunk.length;
  }

  return { data, offsets };
}

function readInt32LE(buf: Uint8Array, offset: number): number {
  return new DataView(buf.buffer, buf.byteOffset, buf.byteLength).getInt32(offset, true);
}

function getMessageHeaderName(type: number): string {
  for (const [name, code] of Object.entries(MessageHeader)) {
    if (code === type) {
      return name;
    }
  }
  return `Unknown(${type})`;
}

type DecodedField = {
  name?: string;
  nullable?: boolean;
  type?: {
    typeId?: number;
    id?: number;
    indices?: {
      typeId?: number;
    };
    dictionary?: {
      typeId?: number;
    };
  };
};

type DecodedContent = {
  body?: {
    length?: number;
  };
  fields?: DecodedField[];
  length?: number;
  nodes?: unknown;
  regions?: unknown;
  id?: number;
  isDelta?: boolean;
  data?: {
    length?: number;
    nodes?: unknown;
    regions?: unknown;
  };
};

function summarizeMessages(buf: Uint8Array) {
  const messages: unknown[] = [];
  let index = 0;

  while (index + 4 <= buf.length) {
    const start = index;
    let metadataLength = readInt32LE(buf, index);
    index += 4;

    if (metadataLength === -1) {
      metadataLength = readInt32LE(buf, index);
      index += 4;
    }
    if (metadataLength === 0) {
      messages.push({
        start,
        type: 'EOS',
      });
      break;
    }

    const message = decodeMessage(buf, start);
    if (message == null) {
      break;
    }

    const typeName = getMessageHeaderName(message.type);
    const content = message.content as DecodedContent | undefined;
    const bodyLength = content?.body?.length ?? 0;

    const summary: Record<string, unknown> = {
      start,
      metadataLength,
      type: typeName,
      bodyLength,
    };

    if (typeName === 'Schema') {
      summary.schema = {
        version: message.version,
        fields: (content?.fields ?? []).map((f) => ({
          name: f.name,
          nullable: f.nullable,
          typeId: f.type?.typeId,
          dictionaryId: f.type?.id,
          dictionaryIndexType: f.type?.indices?.typeId,
          dictionaryValueType: f.type?.dictionary?.typeId,
        })),
      };
    } else if (typeName === 'RecordBatch') {
      summary.recordBatch = {
        length: content?.length,
        nodes: content?.nodes,
        regions: content?.regions,
      };
    } else if (typeName === 'DictionaryBatch') {
      summary.dictionaryBatch = {
        id: content?.id,
        isDelta: content?.isDelta,
        dataLength: content?.data?.length,
        nodes: content?.data?.nodes,
        regions: content?.data?.regions,
      };
    }

    messages.push(summary);
    index = message.index;
  }

  return messages;
}

function firstByteDiff(a: Uint8Array, b: Uint8Array): number {
  const n = Math.min(a.length, b.length);
  for (let i = 0; i < n; i++) {
    if (a[i] !== b[i]) {
      return i;
    }
  }
  return a.length === b.length ? -1 : n;
}

function getOldIpcBytes(testCase: string): Uint8Array {
  const out = execFileSync('bun', ['run', OLD_IPC_SCRIPT, testCase]);
  return new Uint8Array(out.buffer, out.byteOffset, out.byteLength);
}

function getNewIpcBytes(testCase: string): Uint8Array {
  const nullBitmap = makeNullBitmap(ROWS);
  const uint8Values = Uint8Array.from({ length: ROWS }, (_, i) => i & 0xff);
  const float64Values = Float64Array.from({ length: ROWS }, (_, i) => i * 0.5 + 3.14);
  const utf8Fixture = makeUtf8Fixture(ROWS);
  const dictCardinality = 256;
  const dictEncoded = makeUtf8Fixture(dictCardinality);
  const dictIndices = Uint16Array.from({ length: ROWS }, (_, i) => i % dictCardinality);

  const toBytes = (bytes: Uint8Array | null): Uint8Array => {
    if (bytes == null) {
      throw new Error('new tableToIPC returned null');
    }
    return bytes;
  };

  switch (testCase) {
    case 'uint8/no-nulls':
      return toBytes(
        tableToIPC(createTableFromBatches({ value: createUint8Data(uint8Values, ROWS) }), { format: 'stream' }),
      );
    case 'float64/with-nulls':
      return toBytes(
        tableToIPC(createTableFromBatches({ value: createFloat64Data(float64Values, ROWS, nullBitmap) }), {
          format: 'stream',
        }),
      );
    case 'utf8/with-nulls':
      return toBytes(
        tableToIPC(
          createTableFromBatches({ value: createUtf8Data(utf8Fixture.data, utf8Fixture.offsets, ROWS, nullBitmap) }),
          {
            format: 'stream',
          },
        ),
      );
    case 'dict16/with-nulls':
      return toBytes(
        tableToIPC(
          createTableFromBatches({
            value: createDictionary16Data(dictIndices, dictEncoded.data, dictEncoded.offsets, ROWS, nullBitmap),
          }),
          { format: 'stream' },
        ),
      );
    default:
      throw new Error(`unknown case: ${testCase}`);
  }
}

for (const testCase of ['uint8/no-nulls', 'float64/with-nulls', 'utf8/with-nulls', 'dict16/with-nulls']) {
  const oldBytes = getOldIpcBytes(testCase);
  const newBytes = getNewIpcBytes(testCase);

  console.log(`\n=== ${testCase} ===`);
  console.log(`oldLen=${oldBytes.length} newLen=${newBytes.length} firstDiff=${firstByteDiff(oldBytes, newBytes)}`);

  console.log('\nold messages:');
  console.log(JSON.stringify(summarizeMessages(oldBytes), null, 2));

  console.log('\nnew messages:');
  console.log(JSON.stringify(summarizeMessages(newBytes), null, 2));
}

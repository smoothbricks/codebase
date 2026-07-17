import type { ReducerProgram, SlotDef, SlotTtlMetadata, StructFieldType } from './types.js';
import {
  AggType,
  HEADER_SIZE,
  MAGIC,
  Opcode,
  PROGRAM_HASH_PREFIX,
  SlotType,
  SlotTypeFlag,
  type TtlStartOf,
} from './types.js';

const SLOT_TYPE_MASK = 0x0f;
const HAS_TTL_FLAG = SlotTypeFlag.HAS_TTL;
const HAS_EVICT_TRIGGER_FLAG = SlotTypeFlag.HAS_EVICT_TRIGGER;
const NO_HASHMAP_TIMESTAMPS_FLAG = SlotTypeFlag.NO_HASHMAP_TIMESTAMPS;

interface ParsedTtl {
  ttl: SlotTtlMetadata;
  nextPc: number;
}

function parseAggType(value: number): AggType {
  switch (value) {
    case AggType.SUM:
    case AggType.COUNT:
    case AggType.MIN:
    case AggType.MAX:
    case AggType.AVG:
    case AggType.SCALAR_U32:
    case AggType.SCALAR_F64:
    case AggType.SCALAR_I64:
    case AggType.SUM_I64:
    case AggType.MIN_I64:
    case AggType.MAX_I64:
      return value;
    default:
      throw new Error(`Invalid program: unknown aggregate type ${value}`);
  }
}

function parseScalarAggType(value: number): AggType {
  switch (value) {
    case AggType.SCALAR_U32:
    case AggType.SCALAR_F64:
    case AggType.SCALAR_I64:
      return value;
    default:
      throw new Error(`Invalid program: unknown scalar type ${value}`);
  }
}

function requireInitBytes(initCode: Uint8Array, pc: number, count: number, context: string): void {
  if (pc + count > initCode.length) {
    throw new Error(`Invalid program: truncated ${context}`);
  }
}

function defineSlot(slotDefs: Array<SlotDef | undefined>, expectedSlots: number, slot: number, slotDef: SlotDef): void {
  if (slot >= expectedSlots) {
    throw new Error(`Invalid program: slot index ${slot} out of range`);
  }
  if (slotDefs[slot] !== undefined) {
    throw new Error(`Invalid program: duplicate slot definition ${slot}`);
  }
  slotDefs[slot] = slotDef;
}

function parseStructFieldType(value: number): StructFieldType {
  switch (value) {
    case 0:
    case 1:
    case 2:
    case 3:
    case 4:
    case 5:
    case 6:
    case 7:
    case 8:
    case 9:
      return value;
    default:
      throw new Error(`Invalid program: unknown struct field type ${value}`);
  }
}

function parseTtlStartOf(value: number): TtlStartOf {
  switch (value) {
    case 0:
    case 1:
    case 2:
    case 3:
    case 4:
    case 5:
    case 6:
    case 7:
    case 8:
      return value;
    default:
      throw new Error(`Invalid program: unknown TTL startOf ${value}`);
  }
}

export function parseReducerProgram(bytecode: Uint8Array, defaultCapacity = 1024): ReducerProgram {
  const minLen = PROGRAM_HASH_PREFIX + HEADER_SIZE;
  if (bytecode.length < minLen) {
    throw new Error('Invalid program: too short');
  }

  const content = bytecode.subarray(PROGRAM_HASH_PREFIX);
  const magic = content[0] | (content[1] << 8) | (content[2] << 16) | (content[3] << 24);
  if (magic !== MAGIC) {
    throw new Error('Invalid program: bad magic');
  }

  if (content[4] !== 1 || content[5] !== 0) {
    throw new Error('Invalid program: unsupported version');
  }

  const numSlots = content[6];
  const numInputs = content[7];
  const initLen = content[10] | (content[11] << 8);

  if (PROGRAM_HASH_PREFIX + HEADER_SIZE + initLen > bytecode.length) {
    throw new Error('Invalid program: init section overflow');
  }

  const initCode = content.subarray(HEADER_SIZE, HEADER_SIZE + initLen);
  const slotDefs = parseReducerSlotDefs(initCode, numSlots, defaultCapacity);

  return { bytecode, numSlots, numInputs, slotDefs };
}

export function parseReducerSlotDefs(initCode: Uint8Array, expectedSlots: number, defaultCapacity: number): SlotDef[] {
  const slotDefs: Array<SlotDef | undefined> = new Array(expectedSlots);
  let halted = false;

  let pc = 0;
  while (pc < initCode.length) {
    const op = initCode[pc++];

    switch (op) {
      case Opcode.SLOT_DEF: {
        requireInitBytes(initCode, pc, 4, 'SLOT_DEF operands');
        const slot = initCode[pc];
        const typeFlags = initCode[pc + 1];
        const slotType = typeFlags & SLOT_TYPE_MASK;
        const capLo = initCode[pc + 2];
        const capHi = initCode[pc + 3];
        pc += 4;

        const ttlParsed = parseTtlIfPresent(initCode, pc, typeFlags);
        if (ttlParsed) {
          pc = ttlParsed.nextPc;
        }

        const capacity = (capHi << 8) | capLo || defaultCapacity;
        const ttl = ttlParsed?.ttl;
        let slotDef: SlotDef;

        switch (slotType) {
          case SlotType.HASHMAP:
            slotDef = {
              type: SlotType.HASHMAP,
              capacity,
              storesTimestamps: (typeFlags & NO_HASHMAP_TIMESTAMPS_FLAG) === 0,
              ttl,
            };
            break;
          case SlotType.HASHSET:
            slotDef = { type: SlotType.HASHSET, capacity, ttl };
            break;
          case SlotType.BITMAP:
            slotDef = { type: SlotType.BITMAP, capacity, ttl };
            break;
          case SlotType.AGGREGATE:
            slotDef = { type: SlotType.AGGREGATE, aggType: parseAggType(capLo || AggType.SUM) };
            break;
          case SlotType.SCALAR:
            slotDef = { type: SlotType.SCALAR, aggType: parseScalarAggType(capLo) };
            break;
          case SlotType.CONDITION_TREE:
            slotDef = { type: SlotType.CONDITION_TREE };
            break;
          default:
            throw new Error(`Invalid program: unsupported SLOT_DEF type ${slotType}`);
        }
        defineSlot(slotDefs, expectedSlots, slot, slotDef);
        break;
      }

      case Opcode.SLOT_STRUCT_MAP:
      case Opcode.SLOT_STRUCT_MAP2: {
        requireInitBytes(initCode, pc, 5, 'struct-map slot operands');
        const slot = initCode[pc];
        const typeFlags = initCode[pc + 1];
        const capLo = initCode[pc + 2];
        const capHi = initCode[pc + 3];
        const numFields = initCode[pc + 4];
        pc += 5;
        requireInitBytes(initCode, pc, numFields, 'struct-map field metadata');

        const fieldTypes: StructFieldType[] = [];
        for (let i = 0; i < numFields; i++) {
          fieldTypes.push(parseStructFieldType(initCode[pc++]));
        }

        const ttlParsed = op === Opcode.SLOT_STRUCT_MAP ? parseTtlIfPresent(initCode, pc, typeFlags) : undefined;
        if (ttlParsed) {
          pc = ttlParsed.nextPc;
        }

        defineSlot(
          slotDefs,
          expectedSlots,
          slot,
          op === Opcode.SLOT_STRUCT_MAP2
            ? {
                type: SlotType.STRUCT_MAP2,
                capacity: (capHi << 8) | capLo || defaultCapacity,
                fieldTypes,
              }
            : {
                type: SlotType.STRUCT_MAP,
                capacity: (capHi << 8) | capLo || defaultCapacity,
                fieldTypes,
                ttl: ttlParsed?.ttl,
              },
        );
        break;
      }

      case Opcode.SLOT_ORDERED_LIST: {
        requireInitBytes(initCode, pc, 5, 'SLOT_ORDERED_LIST operands');
        const slot = initCode[pc];
        const capLo = initCode[pc + 2];
        const capHi = initCode[pc + 3];
        const elemTypeByte = initCode[pc + 4];
        pc += 5;

        const capacity = (capHi << 8) | capLo || defaultCapacity;
        let slotDef: SlotDef;
        if (elemTypeByte === 0xff) {
          requireInitBytes(initCode, pc, 1, 'ordered-list field count');
          const numFields = initCode[pc++];
          requireInitBytes(initCode, pc, numFields, 'ordered-list field metadata');
          const fieldTypes: StructFieldType[] = [];
          for (let i = 0; i < numFields; i++) {
            fieldTypes.push(parseStructFieldType(initCode[pc++]));
          }
          slotDef = { type: SlotType.ORDERED_LIST, capacity, fieldTypes };
        } else {
          slotDef = { type: SlotType.ORDERED_LIST, capacity, elemType: parseStructFieldType(elemTypeByte) };
        }
        defineSlot(slotDefs, expectedSlots, slot, slotDef);
        break;
      }

      case Opcode.HALT:
        if (pc !== initCode.length) {
          throw new Error('Invalid program: bytes after init HALT');
        }
        halted = true;
        break;

      default:
        throw new Error(`Invalid program: unknown init opcode ${op}`);
    }

    if (halted) {
      break;
    }
  }

  if (!halted) {
    throw new Error('Invalid program: init section missing HALT');
  }

  return Array.from({ length: expectedSlots }, (_, slot) => {
    const slotDef = slotDefs[slot];
    if (slotDef === undefined) {
      throw new Error(`Invalid program: missing slot definition ${slot}`);
    }
    return slotDef;
  });
}

function parseTtlIfPresent(initCode: Uint8Array, pc: number, typeFlags: number): ParsedTtl | undefined {
  if ((typeFlags & HAS_TTL_FLAG) === 0) {
    return undefined;
  }

  if (pc + 10 > initCode.length) {
    throw new Error('Invalid program: TTL metadata overflow');
  }

  const view = new DataView(initCode.buffer, initCode.byteOffset + pc, 10);
  return {
    ttl: {
      ttlSeconds: view.getFloat32(0, true),
      graceSeconds: view.getFloat32(4, true),
      timestampFieldIndex: view.getUint8(8),
      startOf: parseTtlStartOf(view.getUint8(9)),
      hasEvictTrigger: (typeFlags & HAS_EVICT_TRIGGER_FLAG) !== 0,
    },
    nextPc: pc + 10,
  };
}

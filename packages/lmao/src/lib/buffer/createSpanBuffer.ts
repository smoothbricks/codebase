import { getCacheAlignedCapacity } from './capacity.js';

export function createEmptySpanBuffer(
  spanId: number,
  requestedCapacity: number,
  attributeCount: number
){
  // Choose bitmap type based on attribute count
  let BitmapType: Uint8ArrayConstructor | Uint16ArrayConstructor | Uint32ArrayConstructor;
  if (attributeCount <= 8) {
    BitmapType = Uint8Array;
  } else if (attributeCount <= 16) {
    BitmapType = Uint16Array;
  } else if (attributeCount <= 32) {
    BitmapType = Uint32Array;
  } else {
    throw new Error(`Too many attributes: ${attributeCount}. Maximum 32 supported.`);
  }
  
  const alignedCapacity = getCacheAlignedCapacity(requestedCapacity, 1);
  
  return {
    spanId,
    timestamps: new Float64Array(alignedCapacity),
    operations: new Uint8Array(alignedCapacity),
    nullBitmap: new BitmapType(alignedCapacity),
    children: [],
    writeIndex: 0,
    capacity: requestedCapacity,
  };
}

/**
 * Scratch instrument: trace-id hex encoding, LUT against the toString(16) loop.
 *
 * Both models run in one process, ABBA-interleaved (forward pass paired with a
 * reverse pass), so a drift in machine state cannot be read as a model
 * difference. Random-byte generation is excluded: it is identical in both models
 * and would otherwise dominate.
 */

import { generateTraceId } from '../src/lib/traceId.js';

const HEX_DIGITS = '0123456789abcdef';
const HEX_BYTE: readonly string[] = Array.from(
  { length: 256 },
  (_, byte) => `${HEX_DIGITS[byte >>> 4]}${HEX_DIGITS[byte & 0xf]}`,
);

/** The encoder that shipped before the table: 16 radix conversions plus 16 pads. */
function legacyHex(bytes: Uint8Array): string {
  let hex = '';
  for (let i = 0; i < 16; i++) {
    hex += bytes[i].toString(16).padStart(2, '0');
  }
  return hex;
}

/** The encoder that ships now, lifted out of generateTraceId verbatim. */
function lutHex(bytes: Uint8Array): string {
  return (
    HEX_BYTE[bytes[0]] +
    HEX_BYTE[bytes[1]] +
    HEX_BYTE[bytes[2]] +
    HEX_BYTE[bytes[3]] +
    HEX_BYTE[bytes[4]] +
    HEX_BYTE[bytes[5]] +
    HEX_BYTE[bytes[6]] +
    HEX_BYTE[bytes[7]] +
    HEX_BYTE[bytes[8]] +
    HEX_BYTE[bytes[9]] +
    HEX_BYTE[bytes[10]] +
    HEX_BYTE[bytes[11]] +
    HEX_BYTE[bytes[12]] +
    HEX_BYTE[bytes[13]] +
    HEX_BYTE[bytes[14]] +
    HEX_BYTE[bytes[15]]
  );
}

// A pool of distinct byte patterns, so neither model can cache one result.
const POOL = 256;
const inputs = Array.from({ length: POOL }, () => crypto.getRandomValues(new Uint8Array(16)));

for (const bytes of inputs) {
  if (legacyHex(bytes) !== lutHex(bytes)) throw new Error(`hex model divergence on ${bytes.join(',')}`);
}

let sink = 0;
function price(encode: (bytes: Uint8Array) => string): number {
  const ITERATIONS = 200_000;
  for (let i = 0; i < 100_000; i++) sink += encode(inputs[i & (POOL - 1)]).length;
  let best = Number.POSITIVE_INFINITY;
  for (let round = 0; round < 7; round++) {
    const start = Bun.nanoseconds();
    for (let i = 0; i < ITERATIONS; i++) sink += encode(inputs[i & (POOL - 1)]).length;
    const ns = (Bun.nanoseconds() - start) / ITERATIONS;
    if (ns < best) best = ns;
  }
  return best;
}

/** Whole-generator cost, random bytes included — what a span actually pays. */
function priceGenerator(): number {
  const ITERATIONS = 200_000;
  for (let i = 0; i < 100_000; i++) sink += generateTraceId().length;
  let best = Number.POSITIVE_INFINITY;
  for (let round = 0; round < 7; round++) {
    const start = Bun.nanoseconds();
    for (let i = 0; i < ITERATIONS; i++) sink += generateTraceId().length;
    const ns = (Bun.nanoseconds() - start) / ITERATIONS;
    if (ns < best) best = ns;
  }
  return best;
}

const legacyA = price(legacyHex);
const lutA = price(lutHex);
const lutB = price(lutHex);
const legacyB = price(legacyHex);

const legacy = (legacyA + legacyB) / 2;
const lut = (lutA + lutB) / 2;
const loadavg = (await Bun.$`sysctl -n vm.loadavg`.text()).trim();

console.log(`trace-id hex encoding  loadavg=${loadavg}`);
console.log(
  `  legacy toString(16).padStart loop  ${legacy.toFixed(2)} ns  (A ${legacyA.toFixed(2)} / B ${legacyB.toFixed(2)})`,
);
console.log(`  256-entry LUT                      ${lut.toFixed(2)} ns  (A ${lutA.toFixed(2)} / B ${lutB.toFixed(2)})`);
console.log(
  `  encoding saved                     ${(legacy - lut).toFixed(2)} ns/span  (${(legacy / lut).toFixed(2)}x)`,
);
console.log(`  generateTraceId (with getRandomValues) ${priceGenerator().toFixed(2)} ns`);
console.log(`sink ${sink}`);

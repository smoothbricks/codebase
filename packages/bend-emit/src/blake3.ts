// Pure TypeScript BLAKE3 (256-bit unkeyed hash), matching the official BLAKE3 specification.

const IV = new Uint32Array([
  0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19,
]);

const MSG_PERM = [2, 6, 3, 10, 7, 0, 4, 13, 1, 11, 12, 5, 9, 14, 15, 8] as const;

const CHUNK_START = 1;
const CHUNK_END = 2;
const PARENT = 4;
const ROOT = 8;

const BLOCK_LEN = 64;
const CHUNK_LEN = 1024;

function rotr32(x: number, n: number): number {
  return (x >>> n) | (x << (32 - n));
}

function g(s: Uint32Array, a: number, b: number, c: number, d: number, mx: number, my: number): void {
  s[a] = ((s[a] ?? 0) + (s[b] ?? 0) + mx) >>> 0;
  s[d] = rotr32((s[d] ?? 0) ^ (s[a] ?? 0), 16) >>> 0;
  s[c] = ((s[c] ?? 0) + (s[d] ?? 0)) >>> 0;
  s[b] = rotr32((s[b] ?? 0) ^ (s[c] ?? 0), 12) >>> 0;
  s[a] = ((s[a] ?? 0) + (s[b] ?? 0) + my) >>> 0;
  s[d] = rotr32((s[d] ?? 0) ^ (s[a] ?? 0), 8) >>> 0;
  s[c] = ((s[c] ?? 0) + (s[d] ?? 0)) >>> 0;
  s[b] = rotr32((s[b] ?? 0) ^ (s[c] ?? 0), 7) >>> 0;
}

function round(s: Uint32Array, m: Uint32Array): void {
  g(s, 0, 4, 8, 12, m[0] ?? 0, m[1] ?? 0);
  g(s, 1, 5, 9, 13, m[2] ?? 0, m[3] ?? 0);
  g(s, 2, 6, 10, 14, m[4] ?? 0, m[5] ?? 0);
  g(s, 3, 7, 11, 15, m[6] ?? 0, m[7] ?? 0);
  g(s, 0, 5, 10, 15, m[8] ?? 0, m[9] ?? 0);
  g(s, 1, 6, 11, 12, m[10] ?? 0, m[11] ?? 0);
  g(s, 2, 7, 8, 13, m[12] ?? 0, m[13] ?? 0);
  g(s, 3, 4, 9, 14, m[14] ?? 0, m[15] ?? 0);
}

function compress(
  cv: Uint32Array,
  blockWords: Uint32Array,
  counter: bigint,
  blockLen: number,
  flags: number,
): Uint32Array {
  const s = new Uint32Array(16);
  for (let i = 0; i < 8; i++) s[i] = cv[i] ?? 0;
  s[8] = IV[0] ?? 0;
  s[9] = IV[1] ?? 0;
  s[10] = IV[2] ?? 0;
  s[11] = IV[3] ?? 0;
  s[12] = Number(counter & 0xffffffffn);
  s[13] = Number((counter >> 32n) & 0xffffffffn);
  s[14] = blockLen >>> 0;
  s[15] = flags >>> 0;

  let m = new Uint32Array(blockWords);
  for (let r = 0; r < 7; r++) {
    round(s, m);
    if (r < 6) {
      const next = new Uint32Array(16);
      for (let i = 0; i < 16; i++) next[i] = m[MSG_PERM[i] ?? 0] ?? 0;
      m = next;
    }
  }
  for (let i = 0; i < 8; i++) {
    s[i] = ((s[i] ?? 0) ^ (s[i + 8] ?? 0)) >>> 0;
    s[i + 8] = ((s[i + 8] ?? 0) ^ (cv[i] ?? 0)) >>> 0;
  }
  return s;
}

function wordsFromBytes(bytes: Uint8Array, offset: number, len: number): Uint32Array<ArrayBuffer> {
  const block = new Uint8Array(BLOCK_LEN);
  block.set(bytes.subarray(offset, offset + len), 0);
  const view = new DataView(block.buffer, block.byteOffset, BLOCK_LEN);
  const words = new Uint32Array(16);
  for (let i = 0; i < 16; i++) words[i] = view.getUint32(i * 4, true);
  return words;
}

function parentWords(left: Uint32Array, right: Uint32Array): Uint32Array<ArrayBuffer> {
  const words = new Uint32Array(16);
  for (let i = 0; i < 8; i++) {
    words[i] = left[i] ?? 0;
    words[i + 8] = right[i] ?? 0;
  }
  return words;
}

function first8(s: Uint32Array): Uint32Array<ArrayBuffer> {
  return new Uint32Array(s.subarray(0, 8));
}

/** Compute the 256-bit unkeyed BLAKE3 digest as 64 lowercase hex characters. */
export function blake3Hex(input: string | Uint8Array): string {
  const data = typeof input === 'string' ? new TextEncoder().encode(input) : input;
  const cvStack: Uint32Array[] = [];

  const totalChunks = Math.max(1, Math.ceil(data.length / CHUNK_LEN));
  let rootWords = new Uint32Array(16);
  let rootCv: Uint32Array = IV;
  let rootCounter = 0n;
  let rootBlockLen = 0;
  let rootFlags = 0;

  for (let chunkIdx = 0; chunkIdx < totalChunks; chunkIdx++) {
    const chunkStart = chunkIdx * CHUNK_LEN;
    const chunkEnd = Math.min(data.length, chunkStart + CHUNK_LEN);
    const chunkLen = chunkEnd - chunkStart;
    const blocks = Math.max(1, Math.ceil(chunkLen / BLOCK_LEN));
    let cv: Uint32Array = IV;

    for (let b = 0; b < blocks; b++) {
      const bStart = chunkStart + b * BLOCK_LEN;
      const bLen = Math.min(BLOCK_LEN, chunkEnd - bStart);
      const words = wordsFromBytes(data, bStart, bLen);
      const flags = (b === 0 ? CHUNK_START : 0) | (b === blocks - 1 ? CHUNK_END : 0);

      if (b < blocks - 1) {
        cv = first8(compress(cv, words, BigInt(chunkIdx), bLen, flags));
      } else if (totalChunks === 1) {
        rootCv = cv;
        rootWords = words;
        rootCounter = BigInt(chunkIdx);
        rootBlockLen = bLen;
        rootFlags = flags | ROOT;
      } else {
        let newCv = first8(compress(cv, words, BigInt(chunkIdx), bLen, flags));
        if (chunkIdx < totalChunks - 1) {
          let merged = chunkIdx + 1;
          while ((merged & 1) === 0) {
            const left = cvStack.pop();
            if (left === undefined) break;
            newCv = first8(compress(IV, parentWords(left, newCv), 0n, BLOCK_LEN, PARENT));
            merged >>>= 1;
          }
          cvStack.push(newCv);
        } else {
          // Finalize tree by folding remaining stack entries right-to-left.
          let right = newCv;
          while (cvStack.length > 1) {
            const left = cvStack.pop();
            if (left === undefined) break;
            right = first8(compress(IV, parentWords(left, right), 0n, BLOCK_LEN, PARENT));
          }
          const left = cvStack.pop();
          if (left === undefined) throw new Error('blake3 stack underflow');
          rootCv = IV;
          rootWords = parentWords(left, right);
          rootCounter = 0n;
          rootBlockLen = BLOCK_LEN;
          rootFlags = PARENT | ROOT;
        }
      }
    }
  }

  const out = compress(rootCv, rootWords, rootCounter, rootBlockLen, rootFlags);
  let hex = '';
  for (let i = 0; i < 8; i++) {
    const w = out[i] ?? 0;
    for (let shift = 0; shift < 32; shift += 8) {
      hex += ((w >>> shift) & 0xff).toString(16).padStart(2, '0');
    }
  }
  return hex;
}

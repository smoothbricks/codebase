#!/usr/bin/env bun

import { createHash, randomUUID } from 'node:crypto';
import { mkdir, mkdtemp, readFile, rename, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const KIND_TAG = Object.freeze({ log_template: 1, span_name: 2 });
const ID_ALGORITHM = 'sha256-24-v1';
const SCRIPT_DIR = dirname(fileURLToPath(import.meta.url));
const REPOSITORY_ROOT = resolve(SCRIPT_DIR, '..');
const CONFIG_PATH = join(REPOSITORY_ROOT, 'lmao.vocabulary.config.json');
const COLLECTOR_PATH = join(REPOSITORY_ROOT, 'packages/lmao-ttsc/scripts/sync-vocabulary.mjs');
const REPAIR_COMMAND = 'bun run vocabulary:sync';

const mode = process.argv[2];
if (mode !== 'check' && mode !== 'sync') throw new Error('usage: bun tooling/lmao-vocabulary.mjs <check|sync>');

const config = JSON.parse(await readFile(CONFIG_PATH, 'utf8'));
validateConfig(config);
const temporaryDirectory = await mkdtemp(join(tmpdir(), 'lmao-vocabulary-orchestrator-'));

try {
  const manifests = [];
  for (const [index, project] of config.projects.entries()) {
    const output = join(temporaryDirectory, `project-${index}.json`);
    const cwd = resolve(REPOSITORY_ROOT, project.cwd);
    const collector = Bun.spawnSync([
      process.execPath,
      COLLECTOR_PATH,
      '--write',
      `--cwd=${cwd}`,
      `--tsconfig=${project.tsconfig}`,
      `--lmao-vocabulary-manifest=${output}`,
    ], { cwd: REPOSITORY_ROOT, stdio: ['ignore', 'inherit', 'inherit'] });
    if (collector.exitCode !== 0) {
      throw new Error(`native vocabulary collector failed for ${project.cwd}/${project.tsconfig} (exit ${collector.exitCode})`);
    }
    manifests.push(JSON.parse(await readFile(output, 'utf8')));
  }

  const manifest = mergeManifests(manifests);
  const rendered = new Map([
    [resolveOutput(config.outputs.manifest), canonicalManifestBytes(manifest)],
    [resolveOutput(config.outputs.typescript), renderTypeScript(manifest)],
    [resolveOutput(config.outputs.rust), renderRust(manifest)],
  ]);

  if (mode === 'check') {
    const stale = [];
    for (const [path, expected] of rendered) {
      let actual;
      try {
        actual = await readFile(path, 'utf8');
      } catch (error) {
        if (error?.code !== 'ENOENT') throw error;
      }
      if (actual !== expected) stale.push(path.slice(REPOSITORY_ROOT.length + 1));
    }
    if (stale.length !== 0) {
      throw new Error(`stale vocabulary artifacts:\n${stale.map((path) => `  ${path}`).join('\n')}\nrepair with: ${REPAIR_COMMAND}`);
    }
    console.log(`Vocabulary artifacts are byte-for-byte fresh (${manifest.entries.length} records, ${manifest.contentHash}).`);
  } else {
    for (const [path, expected] of rendered) await replaceAtomicallyIfChanged(path, expected);
    console.log(`Synchronized vocabulary artifacts (${manifest.entries.length} records, ${manifest.contentHash}).`);
  }
} finally {
  await rm(temporaryDirectory, { recursive: true, force: true });
}

function validateConfig(value) {
  if (value?.schemaVersion !== 1 || !Array.isArray(value.projects) || value.projects.length === 0) {
    throw new Error(`${CONFIG_PATH} must declare schemaVersion 1 and at least one explicit project`);
  }
  for (const project of value.projects) {
    if (typeof project?.cwd !== 'string' || typeof project?.tsconfig !== 'string') {
      throw new Error(`${CONFIG_PATH} project entries require string cwd and tsconfig fields`);
    }
    if (project.cwd.startsWith('/') || project.tsconfig.startsWith('/')) {
      throw new Error(`${CONFIG_PATH} project paths must be repository-relative`);
    }
  }
  for (const name of ['manifest', 'typescript', 'rust']) {
    if (typeof value.outputs?.[name] !== 'string' || value.outputs[name].startsWith('/')) {
      throw new Error(`${CONFIG_PATH} outputs.${name} must be repository-relative`);
    }
  }
}

function resolveOutput(path) {
  const absolute = resolve(REPOSITORY_ROOT, path);
  if (!absolute.startsWith(`${REPOSITORY_ROOT}/`)) throw new Error(`output escapes repository: ${path}`);
  return absolute;
}

function mergeManifests(manifests) {
  const byRecord = new Map();
  const byId = new Map();
  for (const manifest of manifests) {
    if (manifest?.schemaVersion !== 1 || manifest?.idAlgorithm !== ID_ALGORITHM || !Array.isArray(manifest.entries)) {
      throw new Error('native collector emitted an unsupported vocabulary manifest');
    }
    const canonicalEntries = manifest.entries.map(validateRecord).sort(compareEntries);
    if (JSON.stringify(canonicalEntries) !== JSON.stringify(manifest.entries)) {
      throw new Error('native collector emitted noncanonical record shape or ordering');
    }
    const fragment = fragmentData(canonicalEntries);
    if (manifest.contentHash !== fragmentContentHash(canonicalEntries, fragment)) {
      throw new Error('native collector content hash disagrees with the section 6 vocabulary fragment contract');
    }
    for (const entry of canonicalEntries) {
      const bytes = recordBytes(entry);
      const key = `${KIND_TAG[entry.kind]}:${bytes.toString('hex')}`;
      const previous = byId.get(entry.id);
      if (previous !== undefined && previous !== key) {
        throw new Error(`vocabulary ID collision at ${entry.id}: ${previous} vs ${key}`);
      }
      byId.set(entry.id, key);
      byRecord.set(key, entry);
    }
  }
  const entries = [...byRecord.values()].sort(compareEntries);
  const fragment = fragmentData(entries);
  return { schemaVersion: 1, idAlgorithm: ID_ALGORITHM, contentHash: fragmentContentHash(entries, fragment), entries };
}

function validateRecord(raw) {
  if (KIND_TAG[raw?.kind] === undefined || typeof raw.text !== 'string' || !Array.isArray(raw.fields)) {
    throw new Error(`native collector emitted an invalid vocabulary record: ${JSON.stringify(raw)}`);
  }
  if (!Number.isInteger(raw.id) || raw.id < 1 || raw.id > 0xffffff) {
    throw new Error(`native collector emitted an invalid u24 ID: ${JSON.stringify(raw?.id)}`);
  }
  const fields = raw.fields.map((field) => {
    if (typeof field?.name !== 'string' || typeof field?.column !== 'string') {
      throw new Error(`native collector emitted an invalid field descriptor: ${JSON.stringify(field)}`);
    }
    return { name: field.name, column: field.column };
  });
  const entry = { id: raw.id, kind: raw.kind, text: raw.text, fields };
  const digest = createHash('sha256').update(Uint8Array.of(KIND_TAG[entry.kind])).update(recordBytes(entry)).digest();
  const expectedId = (digest[0] << 16) | (digest[1] << 8) | digest[2];
  if (expectedId === 0 || entry.id !== expectedId) {
    throw new Error(`native collector emitted wrong record-derived ID for ${entry.kind} ${JSON.stringify(entry.text)}`);
  }
  return entry;
}

function recordBytes(entry) {
  const text = Buffer.from(entry.text, 'utf8');
  if (text.length > 0xffffffff || entry.fields.length > 0xffff) throw new Error('vocabulary record exceeds binary format limits');
  const chunks = [u32le(text.length), text, u16le(entry.fields.length)];
  for (const field of entry.fields) {
    const name = Buffer.from(field.name, 'utf8');
    const column = Buffer.from(field.column, 'utf8');
    if (name.length > 0xffff || column.length > 0xffff) throw new Error('vocabulary field descriptor exceeds binary format limits');
    chunks.push(u16le(name.length), name, u16le(column.length), column);
  }
  return Buffer.concat(chunks);
}

function compareEntries(left, right) {
  return left.id - right.id || KIND_TAG[left.kind] - KIND_TAG[right.kind] || Buffer.compare(recordBytes(left), recordBytes(right));
}

function fragmentData(entries) {
  const records = entries.map(recordBytes);
  const offsets = [0];
  for (const record of records) offsets.push(offsets.at(-1) + record.length);
  if (offsets.at(-1) > 0x7fffffff) throw new Error('vocabulary fragment exceeds signed i32 offsets');
  return { bytes: records.flatMap((record) => [...record]), offsets };
}

function fragmentContentHash(entries, fragment) {
  const algorithm = Buffer.from(ID_ALGORITHM, 'utf8');
  const ids = entries.map((entry) => entry.id);
  const kinds = entries.map((entry) => KIND_TAG[entry.kind]);
  const chunks = [Buffer.from([1]), u16le(algorithm.length), algorithm, u32le(ids.length)];
  for (const id of ids) chunks.push(u32le(id));
  chunks.push(u32le(kinds.length), Buffer.from(kinds), u32le(fragment.bytes.length), Buffer.from(fragment.bytes), u32le(fragment.offsets.length));
  for (const offset of fragment.offsets) chunks.push(i32le(offset));
  return createHash('sha256').update(Buffer.concat(chunks)).digest('hex');
}

function u16le(value) {
  const bytes = Buffer.allocUnsafe(2);
  bytes.writeUInt16LE(value);
  return bytes;
}
function u32le(value) {
  const bytes = Buffer.allocUnsafe(4);
  bytes.writeUInt32LE(value);
  return bytes;
}
function i32le(value) {
  const bytes = Buffer.allocUnsafe(4);
  bytes.writeInt32LE(value);
  return bytes;
}

function canonicalManifestBytes(manifest) {
  const json = JSON.stringify(manifest, null, 2).replace(/[<>&\u2028\u2029]/g, (character) => {
    switch (character) {
      case '<': return '\\u003c';
      case '>': return '\\u003e';
      case '&': return '\\u0026';
      case '\u2028': return '\\u2028';
      case '\u2029': return '\\u2029';
      default: throw new Error('unreachable JSON escape character');
    }
  });
  return `${json}\n`;
}

function denseVocabulary(entries) {
  const values = [...new Set(entries.map((entry) => entry.text))].sort((left, right) => Buffer.compare(Buffer.from(left), Buffer.from(right)));
  const denseByValue = new Map(values.map((value, index) => [value, index]));
  const encoded = values.map((value) => [...Buffer.from(value, 'utf8')]);
  const offsets = [0];
  for (const value of encoded) offsets.push(offsets.at(-1) + value.length);
  return { values, denseIndices: entries.map((entry) => denseByValue.get(entry.text)), bytes: encoded.flat(), offsets };
}

function renderTypeScript(manifest) {
  const dense = denseVocabulary(manifest.entries);
  const fragment = fragmentData(manifest.entries);
  return `// Generated from the canonical root lmao.vocabulary.json manifest.\n// Do not edit; run bun run vocabulary:sync.\n\nimport { registerVocabularyFragment } from '../lib/vocabularyRegistry.js';\n\nexport const VOCABULARY_SCHEMA_VERSION = 1 as const;\nexport const VOCABULARY_ID_ALGORITHM = ${JSON.stringify(ID_ALGORITHM)} as const;\nexport const VOCABULARY_CONTENT_HASH = ${JSON.stringify(manifest.contentHash)} as const;\nexport const VOCABULARY_IDS = new Uint32Array(${JSON.stringify(manifest.entries.map((entry) => entry.id))});\nexport const VOCABULARY_KIND_TAGS = new Uint8Array(${JSON.stringify(manifest.entries.map((entry) => KIND_TAG[entry.kind]))});\nexport const VOCABULARY_VALUES: readonly string[] = Object.freeze(${JSON.stringify(dense.values)});\nexport const VOCABULARY_UTF8 = new Uint8Array(${JSON.stringify(dense.bytes)});\nexport const VOCABULARY_UTF8_OFFSETS = new Int32Array(${JSON.stringify(dense.offsets)});\nexport const VOCABULARY_DENSE_INDICES = new Uint32Array(${JSON.stringify(dense.denseIndices)});\nexport const VOCABULARY_FRAGMENT_UTF8 = new Uint8Array(${JSON.stringify(fragment.bytes)});\nexport const VOCABULARY_FRAGMENT_OFFSETS = new Int32Array(${JSON.stringify(fragment.offsets)});\n\nfunction assertVocabularyStructure(): void {\n  const count = VOCABULARY_IDS.length;\n  if (VOCABULARY_KIND_TAGS.length !== count || VOCABULARY_DENSE_INDICES.length !== count || VOCABULARY_FRAGMENT_OFFSETS.length !== count + 1) throw new Error('invalid generated vocabulary: parallel lengths differ');\n  if (VOCABULARY_UTF8_OFFSETS.length !== VOCABULARY_VALUES.length + 1 || VOCABULARY_UTF8_OFFSETS[0] !== 0 || VOCABULARY_UTF8_OFFSETS.at(-1) !== VOCABULARY_UTF8.length) throw new Error('invalid generated vocabulary: dense UTF-8 coverage');\n  if (VOCABULARY_FRAGMENT_OFFSETS[0] !== 0 || VOCABULARY_FRAGMENT_OFFSETS.at(-1) !== VOCABULARY_FRAGMENT_UTF8.length) throw new Error('invalid generated vocabulary: fragment UTF-8 coverage');\n  for (let index = 0; index < count; index++) {\n    const id = VOCABULARY_IDS[index];\n    const kind = VOCABULARY_KIND_TAGS[index];\n    if (id === 0 || id > 0xffffff || (index > 0 && VOCABULARY_IDS[index - 1] >= id)) throw new Error('invalid generated vocabulary: IDs');\n    if (kind !== 1 && kind !== 2) throw new Error('invalid generated vocabulary: kind tag');\n    if (VOCABULARY_DENSE_INDICES[index] >= VOCABULARY_VALUES.length) throw new Error('invalid generated vocabulary: dense index');\n    if (VOCABULARY_FRAGMENT_OFFSETS[index] > VOCABULARY_FRAGMENT_OFFSETS[index + 1]) throw new Error('invalid generated vocabulary: fragment offsets');\n  }\n  for (let index = 1; index < VOCABULARY_UTF8_OFFSETS.length; index++) if (VOCABULARY_UTF8_OFFSETS[index - 1] > VOCABULARY_UTF8_OFFSETS[index]) throw new Error('invalid generated vocabulary: dense offsets');\n}\nassertVocabularyStructure();\n\nfunction vocabularyIndex(id: number): number {\n  let low = 0;\n  let high = VOCABULARY_IDS.length - 1;\n  while (low <= high) {\n    const middle = (low + high) >>> 1;\n    const candidate = VOCABULARY_IDS[middle];\n    if (candidate === id) return middle;\n    if (candidate < id) low = middle + 1;\n    else high = middle - 1;\n  }\n  return -1;\n}\n\nexport function lookupVocabularyDenseIndex(id: number): number { const index = vocabularyIndex(id); return index < 0 ? -1 : VOCABULARY_DENSE_INDICES[index]; }\nexport function lookupVocabularyValue(id: number): string | undefined { const index = vocabularyIndex(id); return index < 0 ? undefined : VOCABULARY_VALUES[VOCABULARY_DENSE_INDICES[index]]; }\nexport function lookupVocabularyKindTag(id: number): 1 | 2 | undefined { const index = vocabularyIndex(id); return index < 0 ? undefined : VOCABULARY_KIND_TAGS[index] as 1 | 2; }\n\nexport const VOCABULARY_FRAGMENT = { schemaVersion: VOCABULARY_SCHEMA_VERSION, idAlgorithm: VOCABULARY_ID_ALGORITHM, contentHash: VOCABULARY_CONTENT_HASH, ids: VOCABULARY_IDS, kindTags: VOCABULARY_KIND_TAGS, utf8: VOCABULARY_FRAGMENT_UTF8, offsets: VOCABULARY_FRAGMENT_OFFSETS } as const;\nexport const VOCABULARY_BINDING = registerVocabularyFragment(VOCABULARY_FRAGMENT);\n`;
}

function renderRust(manifest) {
  const dense = denseVocabulary(manifest.entries);
  const fragment = fragmentData(manifest.entries);
  const list = (values) => values.join(', ');
  return `// Generated from the canonical root lmao.vocabulary.json manifest.\n// Do not edit; run bun run vocabulary:sync.\n\npub const VOCABULARY_SCHEMA_VERSION: u32 = 1;\npub const VOCABULARY_ID_ALGORITHM: &str = ${rustString(ID_ALGORITHM)};\npub const VOCABULARY_CONTENT_HASH: &str = ${rustString(manifest.contentHash)};\npub static VOCABULARY_IDS: &[u32] = &[${list(manifest.entries.map((entry) => entry.id))}];\npub static VOCABULARY_KIND_TAGS: &[u8] = &[${list(manifest.entries.map((entry) => KIND_TAG[entry.kind]))}];\npub static VOCABULARY_VALUES: &[&str] = &[${list(dense.values.map(rustString))}];\npub static VOCABULARY_UTF8: &[u8] = &[${list(dense.bytes)}];\npub static VOCABULARY_UTF8_OFFSETS: &[i32] = &[${list(dense.offsets)}];\npub static VOCABULARY_DENSE_INDICES: &[u32] = &[${list(dense.denseIndices)}];\npub static VOCABULARY_FRAGMENT_UTF8: &[u8] = &[${list(fragment.bytes)}];\npub static VOCABULARY_FRAGMENT_UTF8_OFFSETS: &[i32] = &[${list(fragment.offsets)}];\n\n#[inline]\npub fn lookup_vocabulary_id(id: u32) -> Option<(u32, u8)> {\n    VOCABULARY_IDS.binary_search(&id).ok().map(|index| (VOCABULARY_DENSE_INDICES[index], VOCABULARY_KIND_TAGS[index]))\n}\n`;
}

function rustString(value) {
  return `"${value.replaceAll('\\', '\\\\').replaceAll('"', '\\"').replaceAll('\0', '\\0').replaceAll('\n', '\\n').replaceAll('\r', '\\r').replaceAll('\t', '\\t').replace(/[\u0001-\u0008\u000b\u000c\u000e-\u001f\u007f]/g, (character) => `\\u{${character.codePointAt(0).toString(16)}}`)}"`;
}

async function replaceAtomicallyIfChanged(path, expected) {
  let actual;
  try {
    actual = await readFile(path, 'utf8');
  } catch (error) {
    if (error?.code !== 'ENOENT') throw error;
  }
  if (actual === expected) return;
  await mkdir(dirname(path), { recursive: true });
  const temporaryPath = join(dirname(path), `.${path.split('/').at(-1)}.${process.pid}.${randomUUID()}.tmp`);
  try {
    await writeFile(temporaryPath, expected, { encoding: 'utf8', flag: 'wx' });
    await rename(temporaryPath, path);
  } finally {
    await rm(temporaryPath, { force: true });
  }
}

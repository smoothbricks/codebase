'use strict';

// Witness for scripts/wire-fixtures.sh. cowshed:lint reads src/wire-fixtures.json while
// cowshed:wire-fixtures regenerates it, so every state a reader can observe must be a whole
// formatted corpus, the one before or the one after: never the raw render or a truncated file.

const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { spawn, spawnSync } = require('node:child_process');

const script = path.join(__dirname, 'wire-fixtures.sh');
// The workspace's biome, the one the generator target formats with.
const modules = path.resolve(__dirname, '../../../node_modules/.bin');

// serde_json's pretty printer puts each array element on its own line; biome joins short arrays.
function render(values) {
  return `${JSON.stringify({ corpus: values }, null, 2)}\n`;
}

function format(root, text) {
  const result = spawnSync(path.join(modules, 'biome'), ['format', '--stdin-file-path=src/wire-fixtures.json'], {
    cwd: root,
    input: text,
    encoding: 'utf8',
  });
  assert.equal(result.status, 0, result.stderr);
  return result.stdout;
}

// One event-loop turn, so the child's close event can land between two reads.
function nextTurn() {
  const { promise, resolve } = Promise.withResolvers();
  setImmediate(resolve);
  return promise;
}

function read(file) {
  try {
    return fs.readFileSync(file, 'utf8');
  } catch (error) {
    if (error.code === 'ENOENT') return '<missing>';
    throw error;
  }
}

test('a reader sees the old corpus or the new one, never a partial or unformatted file', async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'wire-fixtures-'));
  try {
    const bin = path.join(root, 'bin');
    fs.mkdirSync(path.join(root, 'src'));
    fs.mkdirSync(bin);
    const before = format(root, render([1, 2]));
    const rendered = render([3, 4]);
    const after = format(root, rendered);
    assert.notEqual(rendered, after, 'the fixture render is one biome reformats');
    const corpus = path.join(root, 'src/wire-fixtures.json');
    fs.writeFileSync(corpus, before);
    fs.writeFileSync(path.join(root, 'render.json'), rendered);
    // Stands in for `cargo test`: the renderer writes serde's render where the script names.
    fs.writeFileSync(path.join(bin, 'cargo'), '#!/bin/sh\ncat "$WITNESS_RENDER" >"$COWSHED_WIRE_FIXTURES"\n', {
      mode: 0o755,
    });
    const child = spawn('sh', [script], {
      cwd: root,
      env: {
        ...process.env,
        PATH: `${bin}:${modules}:${process.env.PATH}`,
        WITNESS_RENDER: path.join(root, 'render.json'),
      },
      stdio: ['ignore', 'ignore', 'pipe'],
    });
    let stderr = '';
    child.stderr.on('data', (chunk) => {
      stderr += chunk;
    });
    const { promise: closed, resolve: close } = Promise.withResolvers();
    child.on('close', close);
    let status = null;
    closed.then((code) => {
      status = code;
    });
    const observed = new Set();
    while (status === null) {
      observed.add(read(corpus));
      await nextTurn();
    }
    assert.equal(status, 0, stderr);
    observed.add(read(corpus));
    assert.deepEqual(
      [...observed].filter((state) => state !== before && state !== after),
      [],
      'a reader observed a corpus that is neither the old nor the new formatted file',
    );
    assert.equal(fs.readFileSync(corpus, 'utf8'), after);
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
  }
});

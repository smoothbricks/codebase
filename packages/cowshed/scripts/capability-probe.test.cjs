'use strict';

// Witness for scripts/capability-probe.sh. cowshed:capability-probe is cached by Nx and every cowshed-core runner keys
// on what it publishes, so the script publishes a verdict only when rustc answered for every case, publishes exactly the
// errors rustc located in each case's crate, and leaves no verdict and no staged workspace behind when it refuses. Cargo
// is the one thing faked, at its published interface: `cargo metadata`, and the JSON messages of `cargo check`, which
// exits 101 when a case fails, as it does whenever the probe works. The real compiler run is the target itself.

const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { spawnSync } = require('node:child_process');

const script = path.join(__dirname, 'capability-probe.sh');
const cases = 'crates/cowshed-core/tests/capability_probe';
const verdict = '.cache/capability-probe/diagnostics.json';

const cargo = `#!/bin/sh
set -eu
case "$1" in
metadata)
  printf '{"workspace_root":"%s","target_directory":"%s"}\\n' "$FIXTURE/workspace" "$FIXTURE/target"
  ;;
check)
  pwd -P >"$FIXTURE/check-cwd"
  printf '%s\\n' "$@" >"$FIXTURE/check-args"
  cp Cargo.lock "$FIXTURE/staged-lock"
  cp alpha/Cargo.toml "$FIXTURE/staged-manifest"
  cat "$FIXTURE/messages"
  cat "$FIXTURE/cargo-log" >&2
  exit 101
  ;;
esac
`;

function message(target, level, rendered, spans = [{}]) {
  return JSON.stringify({ reason: 'compiler-message', target: { name: target }, message: { level, rendered, spans } });
}

function artifact(target) {
  return JSON.stringify({ reason: 'compiler-artifact', target: { name: target } });
}

// A package with the cases alpha and beta, a workspace holding the checkout's lockfile, and the script run in the
// package with the fake cargo first on the PATH. `stale` is a verdict an earlier run left.
function probe({ messages, log, stale = false, withCases = true }) {
  const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'capability-probe-')));
  const cwd = path.join(root, 'package');
  fs.mkdirSync(cwd);
  if (withCases) {
    fs.mkdirSync(path.join(cwd, cases), { recursive: true });
    for (const name of ['alpha', 'beta']) {
      fs.writeFileSync(path.join(cwd, cases, `${name}.rs`), `//! case ${name}\n`);
    }
  }
  if (stale) {
    fs.mkdirSync(path.dirname(path.join(cwd, verdict)), { recursive: true });
    fs.writeFileSync(path.join(cwd, verdict), '{"stale":[]}\n');
  }
  fs.mkdirSync(path.join(root, 'workspace'));
  fs.writeFileSync(path.join(root, 'workspace/Cargo.lock'), "# the checkout's lock\n");
  fs.mkdirSync(path.join(root, 'bin'));
  fs.writeFileSync(path.join(root, 'bin/cargo'), cargo, { mode: 0o755 });
  fs.writeFileSync(path.join(root, 'messages'), messages.map((line) => `${line}\n`).join(''));
  fs.writeFileSync(path.join(root, 'cargo-log'), log);
  const result = spawnSync('sh', [script, verdict], {
    cwd,
    encoding: 'utf8',
    env: { ...process.env, FIXTURE: root, PATH: `${path.join(root, 'bin')}${path.delimiter}${process.env.PATH}` },
  });
  return { root, cwd, result };
}

test('publishes the errors rustc located in each case, none for a case that compiled, and nothing else', () => {
  const { root, cwd, result } = probe({
    messages: [
      artifact('serde'),
      message('alpha', 'warning', 'alpha/src/lib.rs:1:1: warning: unused import\n'),
      message('alpha', 'error', 'alpha/src/lib.rs:2:3: error[E0599]: no method named `attach` found\n'),
      message('alpha', 'error', 'alpha/src/lib.rs:4:3: error[E0599]: no method named `exec` found\n'),
      message('alpha', 'error', 'error: aborting due to 2 previous errors\n', []),
      message('stranger', 'error', 'stranger/src/lib.rs:1:1: error: not a case\n'),
      artifact('beta'),
    ],
    log: 'the cargo log\n',
  });
  try {
    assert.equal(result.status, 0, result.stderr);
    const published = {
      alpha: [
        'alpha/src/lib.rs:2:3: error[E0599]: no method named `attach` found',
        'alpha/src/lib.rs:4:3: error[E0599]: no method named `exec` found',
      ],
      beta: [],
    };
    assert.equal(fs.readFileSync(path.join(cwd, verdict), 'utf8'), `${JSON.stringify(published, null, 2)}\n`);
    // One cargo checks every case, in a workspace staged beside the verdict and gone when the run ends.
    assert.deepEqual(fs.readdirSync(path.dirname(path.join(cwd, verdict))), ['diagnostics.json']);
    assert.equal(
      path.dirname(fs.readFileSync(path.join(root, 'check-cwd'), 'utf8').trim()),
      path.dirname(path.join(cwd, verdict)),
    );
    assert.deepEqual(fs.readFileSync(path.join(root, 'check-args'), 'utf8').split('\n').slice(0, -1), [
      'check',
      '--workspace',
      '--keep-going',
      '--offline',
      '--message-format=json-diagnostic-short',
      '--target-dir',
      path.join(root, 'target'),
    ]);
    // The checkout's own lockfile pins it, and each case depends on this checkout's cowshed-core.
    assert.equal(fs.readFileSync(path.join(root, 'staged-lock'), 'utf8'), "# the checkout's lock\n");
    assert.ok(
      fs
        .readFileSync(path.join(root, 'staged-manifest'), 'utf8')
        .includes(`cowshed-core = { path = '${path.join(cwd, 'crates/cowshed-core')}' }`),
    );
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
  }
});

for (const [name, messages, log, unanswered, errors] of [
  [
    'a case rustc never answered',
    [message('alpha', 'error', 'alpha/src/lib.rs:2:3: error[E0599]: no method named `attach` found\n')],
    "error: process didn't exit successfully (signal: 9, SIGKILL: kill)\n",
    'beta',
    ['alpha/src/lib.rs:2:3: error[E0599]: no method named `attach` found'],
  ],
  ['a cargo that never reached the compiler', [], 'error: failed to select a version for `serde`\n', 'alpha', []],
  [
    'a dependency rustc could not finish',
    [
      message(
        'cowshed-core',
        'error',
        'error: couldn\'t create a temp dir: No such file or directory (os error 2) at path "target/out/rmetassHYTQ"\n',
        [],
      ),
    ],
    'error: could not compile `cowshed-core` (lib) due to 1 previous error\n',
    'alpha',
    ['error: couldn\'t create a temp dir: No such file or directory (os error 2) at path "target/out/rmetassHYTQ"'],
  ],
]) {
  test(`refuses ${name}: no verdict is left, the last run's included, no staged workspace, and rustc's errors are said`, () => {
    const { root, cwd, result } = probe({ messages, log, stale: true });
    try {
      assert.equal(result.status, 1);
      assert.ok(result.stderr.includes(log), result.stderr);
      // cargo's log names the crate that failed; what rustc said of it is in the JSON messages.
      for (const error of errors) assert.ok(result.stderr.includes(error), result.stderr);
      assert.match(result.stderr, new RegExp(`rustc gave no verdict for case ${unanswered}`));
      assert.deepEqual(fs.readdirSync(path.dirname(path.join(cwd, verdict))), []);
    } finally {
      fs.rmSync(root, { recursive: true, force: true });
    }
  });
}

test('refuses a package with no case file by naming the missing case directory', () => {
  const { root, cwd, result } = probe({ messages: [], log: '', withCases: false });
  try {
    assert.equal(result.status, 1);
    assert.match(result.stderr, /no case file/);
    assert.deepEqual(fs.readdirSync(path.dirname(path.join(cwd, verdict))), []);
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
  }
});

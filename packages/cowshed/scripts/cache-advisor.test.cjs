'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { spawnSync } = require('node:child_process');
const { DatabaseSync } = require('node:sqlite');
const { run, parse, candidate, summarize, fingerprint, assertUnchanged } = require('../bin/cache-advisor.cjs');

const repo = path.resolve(__dirname, '../../..');
const nx = require.resolve('nx/bin/nx.js');
const schema = `CREATE TABLE task_details(hash TEXT PRIMARY KEY, project TEXT, target TEXT, configuration TEXT);
 CREATE TABLE cache_outputs(hash TEXT PRIMARY KEY, code INTEGER);
 CREATE TABLE task_history(hash TEXT, status TEXT, code INTEGER, start INTEGER, end INTEGER);`;

function json(file, value) {
  fs.writeFileSync(file, JSON.stringify(value));
}
function database(root) {
  const data = path.join(root, '.nx/workspace-data');
  fs.mkdirSync(data, { recursive: true });
  const filename = path.join(data, 'fixture.db');
  const db = new DatabaseSync(filename);
  db.exec(schema);
  db.close();
  return filename;
}
function snapshot(root) {
  const result = [];
  function visit(directory) {
    for (const name of fs.readdirSync(directory).sort()) {
      const file = path.join(directory, name);
      // The deliberately live daemon owns its append-only log. Probe confinement,
      // DB/cache bytes and the daemon record are the oracle, not that writer's log.
      if (path.relative(root, file) === 'workspace-data/d/daemon.log') continue;
      if (fs.statSync(file).isDirectory()) visit(file);
      else result.push([path.relative(root, file), fs.readFileSync(file).toString('base64')]);
    }
  }
  visit(root);
  return result;
}
function seed(filename, task, seconds, cached) {
  const db = new DatabaseSync(filename);
  db.prepare('INSERT INTO task_details VALUES (?,?,?,?)').run(task.hash, task.project, task.target, task.configuration);
  for (const [index, duration] of seconds.entries()) {
    db.prepare('INSERT INTO task_history VALUES (?,?,?,?,?)').run(
      task.hash,
      'success',
      0,
      index * 100000,
      index * 100000 + duration * 1000,
    );
  }
  // A replay is not a successful uncached sample, despite code=0.
  db.prepare('INSERT INTO task_history VALUES (?,?,?,?,?)').run(task.hash, 'local-cache', 0, 500000, 999999);
  if (cached) db.prepare('INSERT INTO cache_outputs VALUES (?,0)').run(task.hash);
  db.close();
}

test('real Nx: no candidate writes or daemon stop; asymmetric hits, deferred identities and deduplicated means', {
  skip: process.platform !== 'darwin',
}, () => {
  const temp = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'nx-advisor-test-')));
  const workspace = path.join(temp, 'own');
  const main = path.join(temp, 'main');
  fs.mkdirSync(workspace);
  fs.mkdirSync(main);
  const env = { ...process.env };
  for (const key of Object.keys(env)) if (key.startsWith('NX_')) delete env[key];
  env.NX_SOCKET_DIR = path.join(temp, 's');
  env.NX_NATIVE_FILE_CACHE_DIRECTORY = path.join(temp, 'native');
  env.NX_ISOLATE_PLUGINS = 'false';
  const nxCommand = (args) => spawnSync(process.execPath, [nx, ...args], { cwd: workspace, env, encoding: 'utf8' });
  try {
    fs.mkdirSync(path.join(workspace, 'node_modules/@smoothbricks'), { recursive: true });
    fs.symlinkSync(path.join(repo, 'node_modules/nx'), path.join(workspace, 'node_modules/nx'));
    fs.symlinkSync(path.join(repo, 'packages/nx-plugin'), path.join(workspace, 'node_modules/@smoothbricks/nx-plugin'));
    json(path.join(workspace, 'package.json'), { name: 'advisor-fixture', private: true });
    json(path.join(workspace, 'nx.json'), {
      cacheDirectory: '.nx/cache',
      namedInputs: { default: ['{projectRoot}/input.txt'] },
    });
    fs.writeFileSync(path.join(workspace, 'input.txt'), 'current tree');
    const task = {
      executor: 'nx:run-commands',
      cache: true,
      inputs: ['default'],
      options: { command: 'touch TARGET_EXECUTED' },
    };
    json(path.join(workspace, 'project.json'), {
      name: 'fixture',
      targets: {
        own: task,
        main: task,
        missing: task,
        deferred: { ...task, dependsOn: ['own'], inputs: ['default', { dependentTasksOutputFiles: '**/output.txt' }] },
        runtime: {
          ...task,
          inputs: [
            {
              runtime:
                "sh node_modules/@smoothbricks/nx-plugin/runtime-input.sh node -e \"require('fs').writeFileSync('RUNTIME_WROTE', 'bad')\"",
            },
          ],
        },
        gate: { executor: 'nx:noop', cache: false, dependsOn: ['own', 'main', 'missing', 'deferred', 'runtime'] },
      },
    });
    const ownDb = database(workspace);
    const mainDb = database(main);
    fs.mkdirSync(path.join(workspace, '.nx/cache'), { recursive: true });
    fs.mkdirSync(path.join(main, '.nx/cache'), { recursive: true });
    const daemon = nxCommand(['daemon', '--start']);
    assert.equal(daemon.status, 0, daemon.stderr);
    const record = path.join(workspace, '.nx/workspace-data/d/server-process.json');
    const beforeRecord = fs.readFileSync(record, 'utf8');
    const pid = JSON.parse(beforeRecord).processId;
    process.kill(pid, 0);
    const options = { workspace, main, runtime: 'deny', command: ['-t', 'gate', '--parallel=2', '--nx-bail=false'] };
    const first = run(options);
    const byId = Object.fromEntries(first.tasks.map((row) => [row.id, row]));
    assert.equal(first.tasks.length, 5);
    assert.equal(byId['fixture:deferred'].hash, null);
    assert.match(byId['fixture:deferred'].reason, /dependency outputs/);
    assert.equal(byId['fixture:runtime'].hash, null);
    const a = byId['fixture:own'];
    const b = byId['fixture:main'];
    const c = byId['fixture:missing'];
    assert.match(a.hash, /^\d+$/);
    seed(ownDb, a, [1, 1, 10], true);
    seed(mainDb, a, [1, 1, 10], false); // inherited histories count once; mean 4 differs from median 1
    seed(ownDb, b, [2, 4], false);
    seed(mainDb, b, [2, 4], true);
    seed(ownDb, c, [7], false);
    seed(ownDb, { ...byId['fixture:deferred'], hash: '123456789012345' }, [2, 14], false);
    seed(ownDb, { ...byId['fixture:runtime'], hash: '123456789012346' }, [6], false);
    fs.mkdirSync(path.join(workspace, '.nx/cache', a.hash));
    fs.mkdirSync(path.join(main, '.nx/cache', b.hash));
    const beforeOwn = snapshot(path.join(workspace, '.nx'));
    const beforeMain = snapshot(path.join(main, '.nx'));
    const result = run(options);
    const rows = Object.fromEntries(result.tasks.map((row) => [row.id, row]));
    assert.equal(rows['fixture:own'].classification, 'HIT_OWN');
    assert.equal(rows['fixture:main'].classification, 'HIT_MAIN');
    assert.equal(rows['fixture:missing'].classification, 'MISS_BOTH');
    assert.deepEqual(rows['fixture:own'].weight, { meanSeconds: 4, samples: 3, minSeconds: 1, maxSeconds: 10 });
    assert.equal(result.totals.ownMeasuredMissWorkSeconds, 10);
    assert.equal(result.totals.mainMeasuredMissWorkSeconds, 11);
    assert.equal(result.totals.keylessWorstCaseSeconds, 14);
    assert.equal(result.totals.weightedKeylessTasks, 2);
    assert.equal(result.totals.ownMissWorkSeconds, 24);
    assert.equal(result.totals.mainMissWorkSeconds, 25);
    assert.equal(result.totals.ownMinusMainSeconds, -1);
    assert.equal(result.coverage.withDuration, 5);
    assert.equal(result.coverage.resolvedWithDuration, 3);
    assert.equal(rows['fixture:deferred'].classification, 'UNRESOLVED');
    assert.equal(rows['fixture:deferred'].own, 'ASSUMED_MISS');
    assert.equal(rows['fixture:deferred'].main, 'ASSUMED_MISS');
    assert.deepEqual(rows['fixture:deferred'].cost, {
      basis: 'worst-case-keyless',
      label: 'unpriced key, worst case: misses in both',
      ownSeconds: 8,
      mainSeconds: 8,
    });
    assert.equal(result.decision.timeDecisionAvailable, true);
    assert.equal(result.decision.basis, 'time');
    assert.equal(result.decision.workScoreSeconds, -1);
    assert.equal(result.totals.complete, true);
    assert.deepEqual(
      snapshot(path.join(workspace, '.nx')),
      beforeOwn,
      'no rows, graph, cache, lock or SHM bytes changed',
    );
    assert.deepEqual(snapshot(path.join(main, '.nx')), beforeMain);
    assert.equal(fs.readFileSync(record, 'utf8'), beforeRecord);
    process.kill(pid, 0);
    assert.equal(fs.existsSync(path.join(workspace, 'TARGET_EXECUTED')), false);
    assert.equal(fs.existsSync(path.join(workspace, 'RUNTIME_WROTE')), false);
    const confinedRuntime = run({ ...options, runtime: 'read-only' });
    assert.ok(confinedRuntime.tasks.filter((row) => row.id !== 'fixture:deferred').every((row) => row.hash === null));
    assert.match(confinedRuntime.tasks.find((row) => row.id === 'fixture:runtime').reason, /hash pass failed/);
    assert.equal(
      fs.existsSync(path.join(workspace, 'RUNTIME_WROTE')),
      false,
      'runtime descendant inherits write denial',
    );
    assert.deepEqual(snapshot(path.join(main, '.nx')), beforeMain);
    const verify = new DatabaseSync(ownDb, { readOnly: true });
    assert.equal(verify.prepare('SELECT count(*) AS count FROM task_details').get().count, 5);
    verify.close();
  } finally {
    const stopped = nxCommand(['daemon', '--stop']);
    fs.rmSync(temp, { recursive: true, force: true });
    assert.equal(stopped.status, 0, stopped.stderr);
  }
});

test('command parsing preserves gate arguments and refuses shell indirection', () => {
  const root = fs.realpathSync(repo);
  const parsed = parse([
    '--workspace',
    root,
    '--main',
    root,
    '--',
    'bun',
    'nx',
    'run-many',
    '-t',
    'gate',
    '--parallel=2',
  ]);
  assert.deepEqual(parsed.command, ['-t', 'gate', '--parallel=2']);
  assert.throws(() => parse(['--workspace', root, '--main', root, '--', 'bun', 'run', 'gate']), /explicit nx run-many/);
});

test('private snapshot includes live WAL-only commits without touching candidate bytes', () => {
  const temp = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'advisor-wal-')));
  const root = path.join(temp, 'candidate');
  fs.mkdirSync(root);
  const filename = database(root);
  const db = new DatabaseSync(filename);
  try {
    db.exec('PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0');
    db.exec("INSERT INTO task_details VALUES ('123','fixture','task',NULL)");
    db.exec("INSERT INTO task_history VALUES ('123','success',0,1000,8000)");
    db.exec("INSERT INTO cache_outputs VALUES ('123',0)");
    fs.mkdirSync(path.join(root, '.nx/cache/123'), { recursive: true });
    assert.ok(fs.statSync(filename + '-wal').size > 0);
    const before = snapshot(root);
    const observed = candidate(root, path.join(temp, 'private'));
    assert.equal(observed.hashes.has('123'), true);
    assert.equal(observed.samples.length, 1);
    assert.equal(observed.samples[0].end - observed.samples[0].start, 7000);
    assert.deepEqual(snapshot(root), before);
    assert.ok(observed.snapshotBytes > fs.statSync(filename).size, 'the WAL bytes were copied too');
    const selection = {
      tasks: [
        { id: 'fixture:task', project: 'fixture', target: 'task', configuration: null, hash: '123', reason: null },
      ],
    };
    const priced = summarize(selection, observed, observed);
    assert.equal(priced.decision.basis, 'time');
    assert.equal(priced.decision.workScoreSeconds, 0);
    const unavailable = { root, databases: [], hashes: null, samples: [], error: 'busy: fixture WAL changed' };
    const refused = summarize(selection, unavailable, observed);
    assert.equal(refused.decision.basis, 'space');
    assert.equal(refused.decision.workScoreSeconds, null);
    assert.equal(refused.tasks[0].classification, 'UNRESOLVED');
    assert.match(refused.unresolved[0].reason, /busy/);
    const noHistory = summarize(selection, { ...observed, samples: [] }, { ...observed, samples: [] });
    assert.equal(noHistory.decision.timeDecisionAvailable, false);
    assert.equal(noHistory.tasks[0].weight, null);
    assert.match(noHistory.unpriced[0].reason, /no successful uncached duration/);
    const partiallyPriced = summarize(
      {
        tasks: [
          ...selection.tasks,
          {
            id: 'fixture:no-history',
            project: 'fixture',
            target: 'no-history',
            configuration: null,
            hash: '456',
            reason: null,
          },
        ],
      },
      observed,
      observed,
    );
    assert.equal(partiallyPriced.decision.timeDecisionAvailable, true);
    assert.equal(partiallyPriced.totals.complete, false);
    assert.equal(partiallyPriced.unpriced[0].id, 'fixture:no-history');
    const noCache = summarize(selection, { ...observed, hashes: new Set() }, { ...observed, hashes: new Set() });
    assert.equal(noCache.decision.timeDecisionAvailable, false);
    assert.match(noCache.decision.reason, /neither candidate has a usable Nx cache/);
  } finally {
    db.close();
    fs.rmSync(temp, { recursive: true, force: true });
  }
});

test('snapshot witness refuses changed, appeared and disappeared DB/WAL/journal files by name', () => {
  const temp = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'advisor-busy-')));
  try {
    for (const suffix of ['', '-wal', '-journal']) {
      const filename = path.join(temp, 'fixture.db' + suffix);
      const absent = fingerprint(filename);
      fs.writeFileSync(filename, 'first');
      assert.throws(
        () => assertUnchanged(filename, absent),
        (error) => error.message.includes(filename) && /busy/.test(error.message),
      );
      const before = fingerprint(filename);
      fs.writeFileSync(filename, 'changed contents');
      assert.throws(() => assertUnchanged(filename, before), /no time decision available/);
      const present = fingerprint(filename);
      fs.unlinkSync(filename);
      assert.throws(() => assertUnchanged(filename, present), /busy/);
    }
  } finally {
    fs.rmSync(temp, { recursive: true, force: true });
  }
});

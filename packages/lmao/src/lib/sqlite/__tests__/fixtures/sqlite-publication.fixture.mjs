import { statSync } from 'node:fs';
import { basename, dirname, join } from 'node:path';
import { openWalDatabase } from '@smoothbricks/lmao/sqlite/node';

function waitForMessage(expected) {
  return new Promise((resolve) => {
    function onMessage(message) {
      if (message === expected) {
        process.off('message', onMessage);
        resolve();
      }
    }
    process.on('message', onMessage);
  });
}

const [dbPath, worker] = process.argv.slice(2);
if (!dbPath || worker === undefined || !process.send) throw new Error('expected path, worker and IPC');
const sqlite = await import(process.versions.bun ? 'bun:sqlite' : 'node:sqlite');
const openDatabase = process.versions.bun
  ? (path) => new sqlite.Database(path)
  : (path) => new sqlite.DatabaseSync(path);
const opening = waitForMessage('open');
process.send('ready');
await opening;

if (worker === '-1') {
  const gate = openDatabase(join(dirname(dbPath), `.${basename(dbPath)}.publish`));
  try {
    gate.exec('PRAGMA busy_timeout = 10000; BEGIN EXCLUSIVE');
    const release = waitForMessage('release');
    process.send('initialized');
    await release;
  } finally {
    gate.close();
    process.disconnect();
  }
} else {
  process.send('publishing');
  const db = openWalDatabase(dbPath, openDatabase);
  try {
    db.exec('PRAGMA busy_timeout = 10000; CREATE TABLE IF NOT EXISTS publication_test (worker INTEGER PRIMARY KEY)');
    const mode = db.prepare('PRAGMA journal_mode').get().journal_mode;
    const commit = waitForMessage('commit');
    process.send({ ino: statSync(dbPath, { bigint: true }).ino.toString(), mode });
    process.send('initialized');
    await commit;
    db.prepare('INSERT INTO publication_test VALUES (?)').run(Number(worker));
  } finally {
    db.close();
    process.disconnect();
  }
}

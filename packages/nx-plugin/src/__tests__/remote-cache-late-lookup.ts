/**
 * The process half of `nx-remote-cache-outage.test.ts`'s late-lookup case. The native
 * `HttpRemoteCache` reads its server from the C environment when it is built, which a Bun
 * test cannot set for itself (`process.env` does not reach it), so this runs as a child with
 * `NX_SELF_HOSTED_REMOTE_CACHE_SERVER` set. It prints one JSON line per step. The guard gets the
 * native client through a wrapper that only records when the native lookup settles, so the last
 * line comes once the late artifact has been unpacked wherever the guard sent it.
 *
 * The guard's deadline is `<withinMs>`, so the case waits only until its held lookup is overtaken.
 *
 * Usage: bun remote-cache-late-lookup.ts <root> <hash> <withinMs>
 */
import { mkdir, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { HttpRemoteCache } from 'nx/src/native/index.js';
import { RemoteCacheOutageGuard } from 'nx/src/tasks-runner/remote-cache-outage.js';

const [root, hash, within] = process.argv.slice(2);
const server = process.env.NX_SELF_HOSTED_REMOTE_CACHE_SERVER;
const withinMs = Number(within);
if (
  root === undefined ||
  hash === undefined ||
  server === undefined ||
  !Number.isSafeInteger(withinMs) ||
  withinMs < 1
) {
  throw new Error(
    'usage: remote-cache-late-lookup.ts <root> <hash> <withinMs>, with NX_SELF_HOSTED_REMOTE_CACHE_SERVER set',
  );
}

/** A cache entry holding the one output `app/result.txt`. */
async function writeEntry(cacheDirectory: string, text: string): Promise<void> {
  await mkdir(join(cacheDirectory, hash, 'app'), { recursive: true });
  await writeFile(join(cacheDirectory, hash, 'app', 'result.txt'), text);
}

const native = new HttpRemoteCache();

// The artifact the server answers late is one the native client built.
const produced = join(root, 'produced');
await writeEntry(produced, 'late');
console.log(JSON.stringify({ stored: await native.store(hash, produced, 'late output', 0) }));

// The hazard: a native lookup unpacks into the directory it is given, over what is there.
const exposed = join(root, 'exposed');
await writeEntry(exposed, 'local');
console.log(JSON.stringify({ exposedHit: (await native.retrieve(hash, exposed)) !== null }));

// The guarded lookup the server holds past the deadline; the task then writes its own entry.
const cache = join(root, 'cache');
await mkdir(cache);
let lookup: Promise<string> | undefined;
const observed = {
  retrieve(key: string, cacheDirectory: string) {
    const pending = native.retrieve(key, cacheDirectory);
    lookup = pending.then(
      (result) => (result === null ? 'miss' : 'hit'),
      (error: unknown) => String(error),
    );
    return pending;
  },
  store: (key: string, cacheDirectory: string, terminalOutput: string, code: number) =>
    native.store(key, cacheDirectory, terminalOutput, code),
};
const guarded = await new RemoteCacheOutageGuard(observed, server, withinMs).retrieve(hash, cache);
await writeEntry(cache, 'local');
console.log(JSON.stringify({ guarded }));

// The server answers the held lookup once it has read the line above.
if (lookup === undefined) throw new Error('the guard never asked the native client');
console.log(JSON.stringify({ late: await lookup }));

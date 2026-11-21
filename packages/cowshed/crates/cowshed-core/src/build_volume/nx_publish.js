// A land's publish (05_gateway.md, "Publication"; 03_caches.md, "Nx accepted-task artifact service"): after the gate
// passed, every task result the gate's Nx runs named is stored to the project's Nx cache from this checkout's own cache.
// Each key is looked up in the checkout's cache in chunks of Nx's own getBatch, then asked for in the store: a stored
// key is counted and never archived. Only a missing key is archived, by
// cache_adapter's nxArchive in Nx's own layout at gzip level 1, and sent in one plain PUT. Prefixed once by
// cache_adapter::script.
// Usage: node - <absolute Nx root> <JSON array of task hashes>; answers one JSON line on stdout.
const answer = process.stdout.write.bind(process.stdout);
process.stdout.write = process.stderr.write.bind(process.stderr);
const fs = require('node:fs');
const path = require('node:path');
const [root, hashesJson] = process.argv.slice(2);
if (!root || !path.isAbsolute(root) || hashesJson === undefined) {
  throw new Error('usage: node - <absolute Nx root> <JSON array of task hashes>');
}
const hashes = JSON.parse(hashesJson);
if (!Array.isArray(hashes) || new Set(hashes).size !== hashes.length) {
  throw new Error('the land must name distinct task hashes');
}
// One validated Nx task key (05_gateway.md): the charset the gateway's route and the store accept.
for (const hash of hashes) {
  if (typeof hash !== 'string' || !/^[A-Za-z0-9_-]{1,128}$/.test(hash)) {
    throw new Error(`invalid Nx task hash ${JSON.stringify(hash)}`);
  }
}
const server = process.env.NX_SELF_HOSTED_REMOTE_CACHE_SERVER;
if (!server) throw new Error('NX_SELF_HOSTED_REMOTE_CACHE_SERVER names no project Nx cache');
// The headers Nx's own client sends (`HttpRemoteCache::new`): the configured bearer, and an octet-stream body.
const token = process.env.NX_SELF_HOSTED_REMOTE_CACHE_ACCESS_TOKEN;
const bearer = token === undefined ? {} : { authorization: `Bearer ${token}` };

/**
 * Keys looked up in the checkout's cache per Nx getBatch: one query and one commit of their access times per chunk, where
 * a native get commits (and fsyncs) once per key. A chunk's terminal outputs are held until it is published, so the
 * chunk bounds what a lookup retains (measured on a real 1,050-key gate list with 99 MiB of terminal output: one batch
 * peaked at 367 MiB resident, chunks of 100 at 242 MiB, one get per key at 143 MiB).
 */
const LOOKUP_CHUNK = 100;

async function publish() {
  const { getLocalBatch, archive } = openNxCache(root);
  let published = 0;
  let existing = 0;
  let skipped = 0;
  for (let at = 0; at < hashes.length; at += LOOKUP_CHUNK) {
    const chunk = hashes.slice(at, at + LOOKUP_CHUNK);
    const locals = getLocalBatch(chunk);
    for (const [index, hash] of chunk.entries()) {
      const result = locals[index];
      // A task Nx does not cache, or one that failed, has no complete entry to share; nor has a row
      // whose entry directory is gone, which Nx 23.2.1's lookup still answers.
      if (result?.code !== 0 || !fs.statSync(result.outputsPath, { throwIfNoEntry: false })?.isDirectory()) {
        skipped += 1;
        continue;
      }
      const url = `${server}/v1/cache/${hash}`;
      const probe = await fetch(url, { headers: { ...bearer, accept: 'application/octet-stream' } });
      await probe.body?.cancel();
      if (probe.status === 200) {
        existing += 1;
        continue;
      }
      if (probe.status !== 404) {
        throw new Error(`task ${hash} was not stored: the project cache answers GET ${probe.status}`);
      }
      const { chunks, length } = await archive(hash, result.terminalOutput ?? '', 0);
      const response = await fetch(url, {
        method: 'PUT',
        headers: { ...bearer, 'content-type': 'application/octet-stream', 'content-length': String(length) },
        body: (async function* () {
          yield* chunks;
        })(),
        duplex: 'half',
      });
      const said = (await response.text()).trim();
      if (response.status === 200) {
        published += 1;
      } else if (response.status === 409) {
        // Stored between the GET and this PUT: the store keeps the archive it holds.
        existing += 1;
      } else {
        throw new Error(
          `task ${hash} was not stored: the project cache answers PUT ${response.status}${said === '' ? '' : `: ${said}`}`,
        );
      }
    }
  }
  return { published, existing, skipped };
}
publish().then(
  (result) => answer(`${JSON.stringify(result)}\n`),
  (error) => {
    console.error(error);
    process.exitCode = 1;
  },
);

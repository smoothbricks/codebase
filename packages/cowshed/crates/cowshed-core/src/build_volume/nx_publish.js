// A land's publish (05_gateway.md "`/nx/v1/cache/{hash}`"): after the gate passed, every task result
// the gate's Nx runs named is stored to the project's Nx cache from this checkout's own cache, as
// plain PUTs by the stock native client. Prefixed once by cache_adapter::script.
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

async function publish() {
  const { requireNx, cacheDir, getLocal } = openNxCache(root);
  const { HttpRemoteCache } = requireNx('nx/src/native');
  const remote = new HttpRemoteCache();
  let published = 0;
  let existing = 0;
  let skipped = 0;
  for (const hash of hashes) {
    const result = getLocal(hash);
    // A task Nx does not cache, or one that failed, has no complete entry to share.
    if (result?.code !== 0 || !fs.statSync(result.outputsPath, { throwIfNoEntry: false })?.isDirectory()) {
      skipped += 1;
      continue;
    }
    if (await remote.store(hash, cacheDir, result.terminalOutput ?? '', 0)) {
      published += 1;
      continue;
    }
    // The native store answers false for an existing key (409) and for a refusal: tell them apart.
    const response = await fetch(`${server}/v1/cache/${hash}`);
    await response.body?.cancel();
    if (response.status !== 200) {
      throw new Error(`task ${hash} was not stored, and the project cache answers GET ${response.status}`);
    }
    existing += 1;
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

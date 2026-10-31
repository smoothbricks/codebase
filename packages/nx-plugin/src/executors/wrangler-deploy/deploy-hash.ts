/**
 * The hash a wrangler deploy runs under when its hasher could not confirm that the stage serves its artifact's
 * version: the artifact hash, then a nonce no cache holds, so the run misses whatever an earlier run stored, rolled
 * back to or not. A miss is the right answer there. The deploy still tags the artifact's version
 * (`deployArtifactHash`), so once that version is live the next hash is the artifact's own.
 */
export function unconfirmedDeployHash(artifactHash: string, nonce: string): string {
  return `${artifactHash}-${nonce}`;
}

/** The artifact hash a wrangler deploy task's hash carries: all of a confirmed one, the part before an unconfirmed one's nonce. */
export function deployArtifactHash(taskHash: string): string {
  const nonce = taskHash.indexOf('-');
  return nonce === -1 ? taskHash : taskHash.slice(0, nonce);
}

# Orphan group ledgers are named, and retired from an authoritative digest set

Measured 2026-11-25: of 500 `<store>/run/<digest>.groups` ledgers, 473 had no live supervisor, many of them for
workspaces that no longer exist. The run-dir unit makes every workspace removal retire its ledger through `take_lost`
from now on (11_shell.md, "Supervisor recovery"), but ledgers orphaned before it stay: no `rm`, `gc` or `forget` will
revisit a workspace already gone, and its digest never gets a next supervisor to inherit the ledger.

Two gaps keep them:

- `doctor` does not report a `<digest>.groups` whose digest names no known workspace; it inspects unresolved groups only
  per live workspace of the opened project.
- Daemon-start recovery (`SupervisorManager::recovery`, which runs before the daemon claims its control socket) holds no
  authoritative store-wide set of workspace digests: `discover_owned_identities` skips unreadable owners and directories
  without a readable `repository.json`, so it cannot prove a digest names no workspace.

Owed: `doctor` names each orphan ledger (its digest, its unresolved groups if any); and once an authoritative digest set
exists at a point the store's records are complete, an orphan ledger with nothing unresolved is retired through

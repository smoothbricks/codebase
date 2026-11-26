# A long-running workspace supervisor refuses a newer CLI's frame

Found 2026-11-26 in a consumer workspace after a test's idle kill. Owner: cowshed.

`cowshed retire-descendants` exited 7:

```
workspace supervisor protocol: malformed frame: unknown field `gateActive`
```

The workspace supervisor (pid 17891) had been running for about 5 hours, from before the installed `cowshed` CLI gained
the `gateActive` field. The retirement did not happen, and a `bun` process stayed a zombie under Nx.

## What must hold

- A CLI and a workspace supervisor of different builds either speak a frame both understand, or the CLI detects the
  older supervisor and replaces it (or retires through a path that does not need the new field) before sending.
- A refused frame never leaves descendants unretired: the caller is told which process to restart.

## Red

A supervisor built without a field the CLI now sends receives a retirement request: the request is served, or the
supervisor is restarted and then serves it, with no zombie left behind.

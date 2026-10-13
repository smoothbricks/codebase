# cowshed-mcp server

`specs/cowshed/12_mcp.md` specifies `cowshed-mcp`: a separate binary on `cowshed-core` and `cowshed-shell`, tools-only
v1, the coordinator/worker capability split with one-use worker descriptors, the tool set, and the
`CowshedError → JSON-RPC` mapping. It reuses the canonical generated API declaration of `07_api.md` for job results,
cursor tails and progress events.

## What exists

No MCP crate, binary, tool or handshake exists in `packages/cowshed/crates`. The Rust core, the generated API
declaration and N-API bindings it would sit on exist.

## Work

1. A `cowshed-mcp` crate and binary over `cowshed-core`/`cowshed-shell`, never shelling out to the CLI.
2. The coordinator and worker tool sets of `12_mcp.md` "Tools", with the descriptor transfer of "Transports".
3. The error table of `12_mcp.md` "Error mapping".
4. Add the crate to the crate map in `specs/cowshed/00_overview.md` when it exists.

## Acceptance

- The MCP authority-lifecycle tests of `specs/cowshed/08_testing.md:418-421`: no authority on stdout/stderr/argv/env,
  descriptor TTL/replay/consume semantics, and `-32005` for a worker calling a coordinator-only tool before dispatch.
- `land` through MCP follows the same checkout-state contract as `Coordinator::land` (`12_mcp.md:116-121`).

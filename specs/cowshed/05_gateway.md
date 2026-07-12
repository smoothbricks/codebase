# cowshed-gateway

A host daemon that is the only path from a sandboxed workspace to external networks. For a **granted** egress host it
terminates client TLS with a leaf signed by the workspace CA, then establishes and verifies a separate upstream TLS
connection so it can inject narrowly scoped credentials and trace context. Protocol-aware mirrors cache registry
traffic; cert-pinning or explicitly incompatible clients may use an **opaque** allowlisted CONNECT tunnel. Secrets exist
only in the gateway's platform credential store, never in a workspace. The workspace receives only a public CA
certificate that trusts the gateway-as-server; it receives no client identity and no upstream credential. Git never
crosses the data plane as a protocol: workspace git uses local filesystem remotes, while project-scoped mirror fetches
are coordinator-only.

## Placement and identity

The host-only control plane is host-netns `127.0.0.1:7644` (override `COWSHED_GATEWAY_PORT`) plus
`~/.cowshed/gateway.sock` for status, audit tail, and coordinator verbs. Neither host endpoint is reachable from a
workspace, and the sandbox baseline denies both. Linux separately reuses the numeric address `127.0.0.1:7644` inside
each private netns for its data-plane connector; namespace separation makes it a different listener. Data-plane topology
is platform-specific:

- **macOS — port block.** Every workspace, main included, gets a contiguous block of 16 ports from 40960–49151.
  `portBlock` is allocated at new/fork (adopt for main), preserved across restore, and present only in macOS grant
  files. The gateway binds `base`; `base+1 … base+15` are workspace service ports. Seatbelt permits a workspace to
  connect only to its own block. The destination `base` listener is the primary, kernel-enforced workspace identity.
- **Linux — Unix socket, private netns, and trusted connector.** No `portBlock` is allocated. The controller creates
  `~/.cowshed/run/gateway/<workspaceIncarnation>.sock` under a 0700 directory with mode 0600 and bind-mounts that one
  socket as `/run/cowshed/gateway.sock` inside the workspace's private network namespace. The namespace has loopback up
  but no veth, routed interface, or default route. For ordinary package/proxy clients, the controller launches exactly
  one trusted minimal connector in that netns under a controller-owned process identity and dedicated cgroup that
  workspace processes cannot signal, ptrace, inspect, or join. It binds only IPv4 `127.0.0.1:7644`, accepts no
  non-loopback traffic, and forwards bytes bidirectionally and unchanged only to `/run/cowshed/gateway.sock`. It parses
  no protocol, owns no policy, token, CA key, registry credential, or upstream-network authority, and cannot select a
  different Unix socket. The socket inode plus private netns is the primary workspace identity; `127.0.0.1:7644` is only
  a namespace-local compatibility endpoint, so fixed service ports neither collide nor reach siblings.

Every data-plane request additionally carries exactly `Proxy-Authorization: Bearer <opaque-token>`. The token is 32
random bytes encoded as unpadded base64url, lives at `.cowshed/token` mode 0600, and is defense in depth rather than the
workspace selector: the already-selected macOS listener or Linux socket chooses the workspace before comparison. The
gateway accepts no alternate header, cookie, query parameter, URL userinfo, or path token; it strips
`Proxy-Authorization` before any upstream request. It decodes the presented value to bytes, rejects malformed or
wrong-length values, and compares all 32 bytes in constant time. Missing or mismatched token is 401.

Create and fork mint a token. Restore stops admissions, drains the Linux connector and gateway connections, kills the
connector cgroup, rotates the token, unlinks/recreates the Linux socket and namespace-local connector when applicable,
and atomically rewrites the in-image token before new execution is admitted. Detach performs the same drain, cgroup
kill, and socket unlink without rotating persistent authority; attach creates the socket and connector before admitting
execs. The old token and every pre-restore keep-alive or tunnel are therefore invalid immediately; a preserved macOS
port does not preserve authority. A policy miss after successful endpoint and token authentication is 403 with a
machine-parsable grant hint.

Main is a first-class data-plane client with identical platform wiring and policy. Gateway startup and absence are
covered under "Availability and offline behavior".

## Egress modes

Three tiers, never mixed:

- **Public registry mirrors are baseline broker policy.** Anonymous public npm, crates.io, `proxy.golang.org`, and the
  public checksum database are available with zero grants. Baseline does not include scoped registries, private module
  namespaces, alternate credentialed registries, or any request that would attach a credential.
- **Admitted private/credentialed registry routes.** A private upstream is usable only when the current project's
  trusted policy for its stable `repo_id` admits the exact registry origin and package/module scope. Repository config
  may request a route but cannot admit itself. The gateway rejects an unadmitted route before credential lookup;
  admission is project-scoped and does not become fleet-wide baseline access.
- **Granted hosts.** `cowshed grant <ws> --egress <host>` defaults to `mode: "intercept"`; `--opaque` selects a byte
  tunnel with host-only audit and no injection. `--impersonate <profile>` affects the outbound intercepted leg only and
  suppresses all injected headers. Unmatched destinations are denied.

## Endpoints (data plane, per-workspace endpoint)

The HTTP URL base is `http://127.0.0.1:<portBlock.base>` on macOS and exactly `http://127.0.0.1:7644` on Linux. Thus
Bun/npm uses `<base>/npm`, Cargo uses `sparse+<base>/cargo/`, Go uses `<base>/go`, and generic
`HTTP_PROXY`/`HTTPS_PROXY` (and lowercase equivalents) use `<base>`. On Linux the trusted connector carries these byte
streams to the mounted Unix socket; clients never speak HTTP over Unix sockets. Endpoint selection and the token still
authenticate every request.

### `/npm/*` — npm registry mirror

Speaks packument and tarball protocols. Anonymous `registry.npmjs.org` is baseline. Scoped or private origins require a
trusted project-policy admission binding an exact origin to allowed package scopes; only then may the gateway select the
matching credential. Metadata TTL is 5 minutes. Tarballs are content-addressed by their declared integrity digest,
verified while filling and again on every cache read, and committed by atomic rename.

### `/cargo/` — cargo sparse registry mirror

Serves `config.json`, sparse index files, and crate downloads. crates.io is anonymous baseline; alternate or
credentialed registries require exact-origin and crate-scope admission. Workspaces use source replacement pointing to
their own platform endpoint.

### `/go/` — Go module proxy mirror

Implements the GOPROXY protocol and public checksum-database pass-through. `proxy.golang.org` and the public checksum
database are anonymous baseline. Private module prefixes and alternate proxies require trusted project-policy admission
for the exact origin and module prefix; there is no `,direct` VCS fallback. Immutable `.zip` and `.mod` responses are
digest-validated and metadata uses the 5-minute TTL.

### `/sim/` — personal-session simulator broker (posture B)

The sandbox-visible grant enum contains only `openurl` and `install`. `openurl` is restricted to URL schemes registered
for the project; `install` accepts drop-directory artifacts only and remains human-gated. Personal-device `list` and
`boot` are dev-side controller operations and are not gateway grant verbs. Dev-side headless simulators are reached
directly. Unknown verbs are rejected before broker forwarding, and every decision is audited.

### Granted egress — intercept (default) or `--opaque`

Proxy-aware clients use their platform endpoint. On CONNECT, the gateway canonicalizes the authority as DNS-name plus
explicit port, resolves the workspace grant, and requires TLS SNI to equal that DNS name after IDNA A-label and
lowercase normalization. IP CONNECT is allowed only by an exact IP grant and must not carry a conflicting DNS SNI.
Missing SNI for a DNS CONNECT, authority/SNI mismatch, port mismatch, wildcard crossing more than one label, or a
request whose HTTP `:authority`/`Host` differs from the CONNECT authority is denied before upstream connect. The gateway
never resolves an unvalidated secondary authority.

Intercept mode presents a workspace-CA leaf to the client and separately verifies upstream certificate name, chain,
validity, and revocation policy against system roots. The workspace CA certificate authenticates only the gateway's
server side; it is not sent upstream and cannot authenticate a workspace. Opaque mode forwards encrypted bytes only
after the same endpoint, token, authority, and grant checks and never injects credentials or trace headers.

## Interception engine (normative)

The engine is `hyper` + `rustls` + `rcgen` + `tokio`:

- One acceptor per workspace endpoint uses dynamic SNI; never create a listener per host. h1 and h2 ALPN are supported
  on both legs, with SSE and streaming preserved.
- Leaves are minted in process and cached, not listeners. The leaf LRU is capped at 256 entries per workspace and 4096
  globally; entries expire after 24 hours and are usable only when hostname, workspace CA fingerprint, validity window,
  and at least 5 minutes of remaining lifetime all match. Rotation of a workspace CA drops that workspace's entries.
- Limits are 32 active requests/tunnels and 64 queued requests per workspace, 256 active and 512 queued globally, and 8
  active upstream connections per workspace+origin. Queue overflow returns 429 without reading a body. Each direction
  gets a 1 MiB bounded streaming buffer; producers pause when it fills, and cancellation closes both legs.
- Timeouts are 10 s for request headers, 5 s for TCP connect, 10 s for each TLS handshake, 60 s to upstream response
  headers, 120 s idle between body bytes, 15 min total for ordinary requests, and 60 min total for opaque tunnels or
  explicitly detected streaming responses. Timeouts close both legs and emit a classified audit event.
- Parsed request limits are an 8 KiB request target, 100 header fields, 16 KiB per field, 64 KiB aggregate headers, and
  64 MiB request body for intercepted generic egress. Mirror artifacts may stream to 2 GiB only when protocol metadata
  declares an expected digest/length; larger or indeterminate artifacts are rejected without caching.
- Request-smuggling defenses are fail-closed before forwarding: reject obs-fold, invalid header names/values, whitespace
  before `:`, duplicate `Host`/`:authority`, conflicting or repeated `Content-Length`, any `Transfer-Encoding` with
  `Content-Length`, transfer codings other than a single terminal `chunked`, absolute-form URI/authority disagreement,
  CONNECT with a body, and h2 connection-specific headers. The gateway reserializes parsed requests rather than relaying
  client framing and never downgrades ambiguous h2 requests into h1.
- Cache storage has a 20 GiB high-water mark and evicts least-recently-used inactive objects to 16 GiB. Active readers
  and in-progress fills are pinned. Metadata older than 5 minutes is conditionally revalidated with ETag/Last-Modified;
  304 refreshes metadata, 200 replaces atomically. Immutable objects require expected length plus digest verification on
  fill and every read; corruption deletes the entry and becomes a miss. Fills use a same-filesystem temporary file,
  fsync, atomic rename, and parent fsync; concurrent misses coalesce by cache key.

### Credential and redirect boundary

Client-supplied `Authorization`, `Proxy-Authorization`, `Cookie`, `Set-Cookie`, and protocol-specific token headers are
stripped before forwarding; a sandbox cannot choose or override an upstream credential. A credential record binds one
protocol, HTTPS origin (scheme, canonical host, explicit port), allowed methods, and normalized path/package/module
prefixes. Injection occurs only after all fields match and only on the freshly serialized upstream request. The default
credential policy permits `GET`/`HEAD` for mirrors; write methods require an explicit trusted-policy admission. Path
normalization rejects encoded separators, dot segments, backslashes, NUL, and ambiguous double decoding before prefix
comparison.

Redirects are never followed implicitly. For each 3xx, the gateway resolves and normalizes `Location`, strips every
credential and client authorization value, then re-runs project admission, egress mode, origin, method, and path checks.
It may reinject only a credential independently bound to the new exact origin and scope. Cross-origin redirects,
HTTPS-to-HTTP downgrade, method rewriting (including 301/302/303 POST-to-GET), or a redirect outside the admitted prefix
are returned to the client without following; mirror fetches fail closed instead. At most 5 same-origin redirects are
followed. DNS is resolved after authorization and each connection is pinned to the authorized resolution; redirects and
retries repeat resolution and private/link-local/loopback targets require an explicit exact policy entry.

## Git: coordinator-only project mirrors, never a data-plane protocol

There is no git data-plane endpoint. Only the host-side coordinator may request a mirror; a workspace, sandbox token, or
sandbox process cannot invoke the control verb. The coordinator supplies the bound project identity and canonical remote
URL. The gateway verifies the stable `repo_id`, requires the trusted project policy to admit that exact repository or
namespace, and stores/fetches only within that project's mirror scope. A mirror admitted for one project is not thereby
visible or authorized for another project, even when content storage deduplicates identical objects.

The gateway performs `git fetch` with gateway-owned config, hooks disabled, protocol restricted to HTTPS, redirects and
credential scope checked as above, and credentials selected only after exact project+origin+repository-path admission.
The resulting bare mirror is sandbox-readable and never sandbox-writable. Pushes and all real-origin mutation remain
coordinator-side; the data plane supplies neither git credentials nor a push path.

## Control plane (Unix socket + optional 7644)

The control plane provides status and audit to host tools and a project-bound repo-mirror operation to coordinators.
Peer credentials on the Unix socket (and equivalent local authentication on TCP) must identify an authorized host
process; the data-plane token is never accepted on the control plane.

## Credentials

- Secrets use macOS Keychain generic passwords under service `dev.cowshed.gateway`; Linux runner credentials use the
  platform mechanism specified in 10_ci.md. Records carry protocol, exact HTTPS origin, admitted methods, normalized
  path/package/module scopes, and project `repo_id` where applicable; a bare host-only secret record is invalid.
- Credential lookup happens only after endpoint identity, token, project admission, CONNECT authority/SNI, method, and
  normalized path checks. Rotation is observed on next use. Values are never logged, cached in telemetry, placed in
  URLs, or returned to clients.
- Each workspace CA private key is controller-owned mode 0600 outside every mount. The matching public certificate is an
  in-image server trust anchor only. Create/fork mint a key; destroy removes it; restore rotates it and closes existing
  intercepted connections before admitting execution.
- The gateway process is host-side and credential-store access is restricted to that executable identity.

## Availability and offline behavior

Two distinct situations, not to be conflated:

- **Gateway absent** (daemon not running): there is no local process to serve anything — registry requests fail until it
  starts. `cowshed ensure` warns and `cowshed doctor` exits 5 with the kickstart command in the `next:` hint. Builds
  that need no new packages still work: the in-image bun cache covers locked, previously-installed dependencies.
- **Gateway up, upstream offline**: mirror cache hits are served without upstream — anything previously seen installs on
  a plane. Misses fail fast with a `cowshed:` note distinguishing "offline" from "denied" so agents don't request grants
  to fix a network outage.

## Audit events

One audit event per decision, written as **Arrow segments** under `~/.cowshed/telemetry/` (schema, flush policy, and
durability window in 13_telemetry.md) — on the store volume, denied to every sandbox (04_sandbox.md), because this is
the authoritative egress record. There is no separate audit file and no separate gateway log file: audit events and
gateway operational events are rows in the same telemetry store, distinguished by `kind`.

Read it with `cowshed audit` (06_cli.md) — human tables by default, `--json`/`--ndjson` to pipe. The same events,
rendered:

```
$ cowshed audit --ws raven --ndjson | head -5
{"ts":"2026-07-11T12:34:56.789Z","ws":"raven","port":40976,"rev":7,"kind":"npm","name":"react","status":200,"bytes":31245,"cache":"hit","traceId":"4bf92f…"}
{"ts":"…","ws":"raven","port":40976,"rev":7,"kind":"intercept","host":"api.example.com","method":"POST","path":"/v1/run","status":200,"bytes":8123,"traceId":"4bf92f…"}
{"ts":"…","ws":"raven","port":40976,"rev":7,"kind":"opaque","host":"pinned.example.com:443","status":200,"bytes":51200}
{"ts":"…","ws":"raven","port":40976,"rev":7,"kind":"connect","host":"api.example.net:443","status":"denied"}
{"ts":"…","ws":"raven","port":40976,"rev":7,"kind":"repo-mirror","url":"https://github.com/tinylibs/tinybench","status":200,"bytes":184201}
{"ts":"…","ws":"raven","port":40976,"rev":7,"kind":"sim","verb":"openurl","target":"booted","status":"ok","traceId":"4bf92f…"}
```

Intercepted requests carry request-granular fields (`method`, `path`); an `--opaque` tunnel is host-only by
construction. `traceId`/`spanId` columns tie each event to the trace that caused it (13_telemetry.md). Every denial
names the grant that would permit it — the audit store doubles as the debugging tool for "why can't my agent reach X".

## Tradeoffs

**Per-workspace CA interception.** A workspace-scoped CA limits a signing-key compromise to one workspace and lives with
the gateway, which already mediates upstream credentials. The explicit costs are per-tool trust-anchor wiring, opaque
fallback for pinned clients, and gateway visibility into intercepted plaintext.

**Registry mirrors retained.** Protocol-aware mirrors provide digest validation, fleet-wide deduplication, bounded cache
storage, and offline reads that generic interception cannot.

**Git data-plane protocol rejected.** Host-granularity egress cannot constrain repository mutation. Coordinator-only,
project-scoped mirror fetches make the no-push boundary structural.

**Platform-specific data-plane identity.** macOS uses a port block because it lacks per-process network namespaces;
Linux uses a private netns, one per-incarnation Unix socket, and its namespace-local compatibility connector, allocating
no port block. A host-shared listener or token-only identity is rejected because endpoint isolation, not a bearer
secret, must select the workspace.

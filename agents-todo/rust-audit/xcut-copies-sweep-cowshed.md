# XCUT copies sweep: cowshed

Scope: grep-driven sweep of all `packages/cowshed/crates/*/src/**/*.rs` (91 files, ~103k lines including in-file
`#[cfg(test)]` modules). Production-only pattern counts below were taken after stripping `#[cfg(test)]` items
(approximate; brace-matched). Deep-read for ranking: `cowshed-core/src/copy.rs` (1275), `cowshed-gateway/src/proxy.rs`
(3311), `cowshed-gateway/src/cache.rs` (1532), `cowshed-gateway/src/actor.rs` (2206),
`cowshed-core/src/runtime/supervisor.rs` (4102), `cowshed-core/src/git.rs` (3246),
`cowshed-core/src/storage/job_artifact.rs` (4574), `cowshed-core/src/runtime/project.rs` (10762),
`cowshed-core/src/storage/apfs.rs` (3217), `cowshed-core/src/storage/apfs/native.rs` (lock helper),
`cowshed-core/src/storage/fstab.rs` (115), `cowshed-core/src/storage/job_artifact/publication.rs` (882),
`cowshed-cli/src/runtime.rs` (4008), `cowshed-core/src/api/dto.rs` (2517), `cowshed-napi/src/lib.rs` (1036). Tests under
`crates/*/tests/` were counted in greps but not ranked as production findings.

Pattern counts (prod / in-file `#[cfg(test)]` remainder):

| pattern             | prod | cfg(test) |
| ------------------- | ---: | --------: |
| `.clone()`          | 1013 |       303 |
| `format!`           | 1056 |       145 |
| `.to_owned()`       |  520 |       227 |
| `.to_string()`      |  390 |        71 |
| `Arc::clone`        |  143 |        27 |
| `.collect::<Vec`    |   82 |        31 |
| `.collect()`        |  109 |        28 |
| `.to_path_buf()`    |   74 |         2 |
| `.to_vec()`         |   28 |        22 |
| `.cloned()`         |   35 |         6 |
| `async move`        |   83 |        30 |
| `HashMap<String`    |   13 |         0 |
| `BTreeMap<String`   |   22 |         2 |
| `BTreeMap<PathBuf`  |    3 |         3 |
| `read_to_end`       |   10 |         0 |
| `read_to_string`    |    6 |        12 |
| `Vec<Vec`           |    2 |         0 |
| `into_owned()`      |    9 |         5 |
| `spawn_blocking`    |   23 |         0 |
| `dispatch_blocking` |   46 |         0 |

REGIME key: **H** = per HTTP request or per file in an adopt copy pass; **M** = per exec / per git invocation / per
staged APFS op / per cache fill; **L** = CLI render, setup, error, process-open.

## Summary

- Highest-leverage copies are in `copy.rs`: `BTreeMap<PathBuf, _>` snapshots plus PathBuf clones per entry per pass
  (adopt copy loop).
- Gateway per-request: `AcceptContext.clone()`, `CachedResponse`/`HeaderMap` clone on cache hit, 64 KiB read buffer per
  open, `proxy_token` owns a String to compare.
- Actor/supervisor: `HashMap<String, _>` session maps and `BTreeMap<String, String>` env cloned on every admit/exec.
- Git stdout parsers allocate `Vec<String>` then immediately walk the lines; remotes clone the name per URL.
- Arrow commit encoding collect-then-collects one `Vec` per column; APFS staged ops `to_vec()` the expected-path list
  twice.
- `format!` (1056 prod) and most `.to_owned()` sit on error/CLI paths — not ranked HIGH.
- `Arc::clone` (143) is refcount, not a data copy; not a finding.
- No CRITICAL (no live correctness bug from copies). 10 HIGH / 10 MEDIUM / 6 LOW.

## Findings

### F1 — HIGH — COPIES — Adopt copy keys the whole tree by owned `PathBuf` and clones those keys per entry per pass

Evidence: `packages/cowshed/crates/cowshed-core/src/copy.rs:105-106`, `:190-193`, `:280-308`, `:428-437`

```
type Snapshot = BTreeMap<PathBuf, Entry>;
type MirrorState = BTreeMap<PathBuf, Mirrored>;
    let mut mirrored: MirrorState = snapshot(destination)?
        .iter()
        .map(|(path, entry)| (path.clone(), Mirrored::from(entry)))
        .collect();
                mirrored.insert(
                    relative.clone(),
                    Mirrored { ... },
                );
            leaf_copies.push(LeafCopy {
                relative: relative.clone(),
                state: Mirrored::from(entry),
            });
        .map(|(path, _)| path.clone())
        .collect()
```

Problem: REGIME H — every adopt pass walks every source entry. The key _is_ a full owned path; lookup is a memcmp of the
whole path (PH §7.10 / Byproduct L0: the path bytes are already the identity, then they are copied again into
`outdated`/`obsolete`/`LeafCopy`/`MirrorState`). `converge` additionally clones every destination path to seed
`mirrored`. `Entry`/`Mirrored` themselves are `Copy`; the PathBuf traffic is the cost.

Fix: intern relative paths once into a flat byte arena + `u32` ids (parent-id + name-span; successor-is-the-boundary for
the walk). `Snapshot` becomes `Vec<Entry>` indexed by id; dirty sets become bitsets or id vecs.
`copy.rs::{snapshot, reconcile, outdated_paths, obsolete_paths, staging_directories, LeafCopy}`.

Cost/Risk: adopt/copy is the hot tree walk. Callers of `CopyReport` unchanged. CsCoreCopy owns the module.

### F2 — HIGH — COPIES — Snapshot clones a PathBuf it just built in order to both enqueue and insert it

Evidence: `packages/cowshed/crates/cowshed-core/src/copy.rs:529-533`

```
            let path = relative.join(&name);
            if matches!(kind, EntryKind::Directory) {
                pending.push(path.clone());
            }
            entries.insert(path, Entry::new(kind, &metadata));
```

Problem: REGIME H — per directory in the tree, `join` allocates a PathBuf, then `clone` copies it again so the same
bytes can live in `pending` and as a BTreeMap key. Plus `root.join(&relative)` on every directory visit.

Fix: with interned ids (F1), `pending` is `Vec<u32>` and insert does not need a second PathBuf. Without that,
`entries.insert(path.clone(), ...)` only for directories, or store the path once in an arena and key by offset.

Cost/Risk: local to `snapshot`; correctness is “one owned path per entry”.

### F3 — HIGH — COPIES — Parallel leaf copy clones `PathBuf` again and `join`s source/dest per file

Evidence: `packages/cowshed/crates/cowshed-core/src/copy.rs:394-401`

```
                    outcomes.push(LeafCopyOutcome {
                        relative: leaf.relative.clone(),
                        state: leaf.state,
                        materialized: copy_leaf(
                            &source.join(&leaf.relative),
                            &destination.join(&leaf.relative),
                        )?,
                    });
```

Problem: REGIME H — per outdated file: clone the relative path into the outcome, then two `PathBuf` joins.
`LeafCopy.relative` already owns the path; `copy_leaf` only needs `&Path`.

Fix: `LeafCopyOutcome { relative: PathBuf }` can take `std::mem::take` if `LeafCopy` is consumed, or store
`relative: &Path` in the outcome (the `leaves` slice outlives the scope). Precompute dest-root + relative without
allocating two joins (write into a reused `PathBuf` scratch per worker, PH §7.2).

Cost/Risk: `thread::scope` already shares `leaves`; changing outcome ownership is local.

### F4 — HIGH — COPIES — `AcceptContext.clone()` (owns `workspace_id: String`) on every HTTP request

Evidence: `packages/cowshed/crates/cowshed-gateway/src/proxy.rs:71-86`, `:130-135`

```
pub(crate) struct AcceptContext {
    pub workspace_id: String,
    pub commands: mpsc::Sender<Command>,
    ...
    pub mirror_service: MirrorService,
    ...
}
        let service_context = context.clone();
        let service = service_fn(move |request| {
            handle_request(
                request,
                service_context.clone(),
```

Problem: REGIME H — per request. `#[derive(Clone)]` copies `workspace_id: String` plus watch receivers and
`MirrorService` (itself `#[derive(Clone)]` around `Cache` + channel). PH §7.10b: a fat by-value context whose address is
not even needed — `handle_request` could take `&AcceptContext` except the Hyper service_fn requires `'static`. The clone
is the escape.

Fix: `workspace_id: Arc<str>` (or interned `WorkspaceId`). Put `AcceptContext` behind `Arc<AcceptContext>` once per
connection; `service_fn` clones the Arc. CsGwProxy owns this.

Cost/Risk: every proxy entry. Arc is already used for `credentials`/`connector`.

### F5 — HIGH — COPIES — Cache hit clones `CachedResponse` (`HeaderMap`) and the object path

Evidence: `packages/cowshed/crates/cowshed-gateway/src/cache.rs:176-184`, `:968-972`

```
pub struct CachedResponse {
    pub status: StatusCode,
    pub headers: HeaderMap,
    pub content_length: u64,
    pub content_sha256: [u8; 32],
    ...
}
                return Ok(CacheAcquire::Hit(CacheCandidate {
                    digest,
                    generation: entry.generation,
                    path: entry.path.clone(),
                    response: entry.response.clone(),
                }));
```

Problem: REGIME H — the cache-hit path (the one that should be cheap) memcpy's the full header map and PathBuf out of
the actor entry. `open_candidate` then moves that owned copy. Stale-fill clones it a second time (`:975-979`).

Fix: store headers in an interned/Arc'd `Arc<CachedResponse>`; hit returns `Arc`. Path can be `Arc<Path>` keyed by
digest (the digest already _is_ the index — Byproduct L0). CsGwCache owns this.

Cost/Risk: actor command replies change type; fill/corrupt still need generation.

### F6 — HIGH — COPIES — 64 KiB buffer allocated on every cache-body open

Evidence: `packages/cowshed/crates/cowshed-gateway/src/cache.rs:29`, `:257-262`

```
const STREAM_CHUNK_BYTES: usize = 64 * 1024;
                    body: CacheReadBody {
                        file,
                        remaining: content_length,
                        buffer: vec![0; STREAM_CHUNK_BYTES],
                        commands: self.commands.clone(),
```

Problem: REGIME H — per cache hit. PH §7.2 / L4: chunk size is a closed form; the buffer is re-allocated under load
instead of a per-connection or thread-local scratch. Zeroing 64 KiB is also RFO traffic (PH §7.12.6).

Fix: reuse one buffer on `CacheReadBody` via a pool / thread-local `Vec<u8>` with `clear`+`reserve`, or read into a
caller-supplied slot. Same constant at `:1404` for validation reads.

Cost/Risk: body type is `'static` in Hyper; pool must be Send. Wrong reuse across concurrent bodies is a correctness
hole — one buffer per in-flight body, pooled on drop.

### F7 — HIGH — COPIES — `proxy_token` owns a `String` to hand to a compare

Evidence: `packages/cowshed/crates/cowshed-gateway/src/proxy.rs:2363-2384`

```
fn proxy_token(headers: &HeaderMap) -> Option<String> {
    let values: Vec<_> = headers
        .get_all(header::PROXY_AUTHORIZATION)
        .iter()
        .collect();
    ...
        Some(("Bearer", token)) => token.to_owned(),
        Some(("Basic", credentials)) => {
            let decoded = STANDARD.decode(credentials).ok()?;
            let decoded = String::from_utf8(decoded).ok()?;
            decoded.split_once(':')?.1.to_owned()
        }
```

Problem: REGIME H — every non-CONNECT request. Bearer path: collect header refs into a Vec, then `to_owned` the token.
The consumer (`Authentication::Bearer`) only needs the bytes for a constant-time compare against the workspace token.
Basic must decode, but Bearer does not.

Fix: `fn proxy_token(...) -> Option<Cow<'_, str>>` or compare `&str` in place for Bearer; only allocate the decoded
Basic password. Drop the `Vec` collect — `get_all` can be counted without storing.

Cost/Risk: `Authentication` enum currently stores `Option<String>` [INFERENCE from the Bearer constructor at `:185`].
Changing that type touches admit/compare.

### F8 — HIGH — COPIES — Gateway actor keys sessions by `String` and clones the id on every admit/permit

Evidence: `packages/cowshed/crates/cowshed-gateway/src/actor.rs:774-777`, `:956-957`, `:1278-1293`

```
    sessions: HashMap<String, SessionState>,
    revisions: HashMap<String, u64>,
    origins: HashMap<(String, String), usize>,
        let workspace_id = session.workspace_id.clone();
        let workspace_id = seed.workspace_id.clone();
        *self
            .origins
            .entry((workspace_id.clone(), origin.clone()))
            .or_default() += 1;
```

Problem: REGIME H — per request admit/activate. String keys hash the bytes every time; `(String, String)` origin keys
clone both on every permit. Workspace id is already a validated identity (typed `WorkspaceName` elsewhere).

Fix: intern `WorkspaceId` as `Arc<str>` or a generation-local `u32`; `sessions: HashMap<WorkspaceId, _>` or a slab.
Origin keys: intern origin or `(u32, u32)`. CsGwActor owns this.

Cost/Risk: every Command that carries `workspace_id: String` must share the intern. Cross-slice with proxy
`AcceptContext.workspace_id`.

### F9 — HIGH — COPIES — Supervisor clones argv and the whole env map on every exec

Evidence: `packages/cowshed/crates/cowshed-core/src/runtime/supervisor.rs:163`, `:205`, `:2370-2386`

```
    pub env: BTreeMap<String, String>,
            env: BTreeMap<String, String>,
        let info_argv = argv.clone();
        ...
                state.env.extend(env);
                (state.cwd.clone(), state.env.clone(), Some(state.identity))
            None => (
                cwd.or_else(|| self.default_cwd.clone()),
                env.into_iter().collect(),
```

Problem: REGIME H-for-exec (per `worker.exec`, not per HTTP). `BTreeMap<String, String>` is cloned in full after extend
so `environment_for_spawn` can take it by value, then it is applied again via `.envs(&request.env)` at `:1251`.
`info_argv` clones argv solely to put it on JobInfo.

Fix: keep session env as `Arc<BTreeMap<String, String>>` (replace on extend). Pass `&BTreeMap` into spawn. JobInfo can
borrow argv until the job record is stored, or store `Arc<[CommandArg]>`. CsCoreSupervisor owns this.

Cost/Risk: devenv merge (`merge_devenv_environment`) currently takes maps by value — change it to extend in place.

### F10 — HIGH — COPIES — Credential lookup rebuilds owned workspace/repo/path/method per upstream request

Evidence: `packages/cowshed/crates/cowshed-gateway/src/proxy.rs:1913-1919`

```
        let query = CredentialQuery {
            workspace_id: admission.workspace_id.clone(),
            repo_id: admission.repo_id.clone(),
            protocol,
            origin: admission.target.origin(),
            method: parts.method.clone(),
            path: path.to_owned(),
        };
```

Problem: REGIME H — every credentialed proxy request. Four owned copies to build a query that is only borrowed by
`lookup(&query)`.

Fix: `CredentialQuery<'a>` with `&str`/`&Method`. If the trait needs `'static`, intern as in F4/F8.

Cost/Risk: `CredentialProvider` trait across gateway; CsGwPolicy/credentials slice.

### F11 — MEDIUM — COPIES — `git cherry` stdout is collected into `Vec<String>` then immediately split

Evidence: `packages/cowshed/crates/cowshed-core/src/git.rs:1084-1093`, `:1911-1918`

```
        for line in parse_lines(&output.stdout, "patch identity comparison")? {
            let (marker, oid) = line.split_at_checked(2).ok_or_else(|| {
...
                "- " => equivalent.push(oid.to_owned()),
fn parse_lines(bytes: &[u8], description: &str) -> Result<Vec<String>> {
    ...
        .map(str::to_owned)
        .collect())
```

Problem: REGIME M — per land/abandon identity proof, once per commit line. Collect-then-iterate (PH §7.2): allocate
every line, then take `&str` slices, then `oid.to_owned()` a second time for the `- ` set. `commits_changing_something`
(`:1189-1191`) collects `Vec<String>` only to `.into_iter().collect()` a `BTreeSet<String>`.

Fix: walk `output.stdout.split(|b| *b == b'\n')` like `content_free_merge_count` already does at `:1145`. Hold
equivalent oids as `&str` into `stdout` or as `[u8; 40]` / `GitOid`. Delete `parse_lines` as a String factory. CsCoreGit
owns this.

Cost/Risk: every `parse_lines` caller (remotes, F12) must switch to byte splits.

### F12 — MEDIUM — COPIES — Remote listing clones the remote name per URL

Evidence: `packages/cowshed/crates/cowshed-core/src/git.rs:423-435`

```
        let names = parse_lines(&names_output.stdout, "remote name")?;
        let mut remotes = Vec::new();
        for name in names {
            ...
            for url in parse_lines(&output.stdout, "remote URL")? {
                remotes.push(RemoteUrl {
                    name: name.clone(),
                    url,
                });
```

Problem: REGIME M — per `git remote` enumeration. Name is owned once in `names`, then cloned per URL, while URLs are
themselves freshly owned Strings.

Fix: `name: Arc<str>` on `RemoteUrl`, or one name + `Vec<url>` grouping. Parse URLs from bytes (F11).

Cost/Risk: `RemoteUrl` public shape in git.rs.

### F13 — MEDIUM — COPIES — Arrow commit encoding collect-then-collects one Vec per column

Evidence: `packages/cowshed/crates/cowshed-core/src/storage/job_artifact.rs:2772-2805`, `:3317-3351`

```
    let values: Vec<_> = jobs.iter().map(select).collect();
            Arc::new(StringArray::from(
                values
                    .iter()
                    .map(|value| visible_storage_name(value.storage_kind))
                    .collect::<Vec<_>>(),
    let rows: Vec<_> = commitments.iter().map(flatten_controller).collect();
        Arc::new(StringArray::from(
            rows.iter().map(|row| row.kind).collect::<Vec<_>>(),
```

Problem: REGIME M — per seal/commit batch. N rows × ~20 columns each materialise an intermediate `Vec` that Arrow
immediately copies into buffers (L0 evaporating collect). `visible_stream_array` first collects
`&VisibleStreamCommitment`, then four more Vecs.

Fix: `StringBuilder`/`UInt64Builder`/`BinaryBuilder` filled in one pass over `commitments` (fused emission, PH §7.8).
Drop `flatten_controller` as a staging struct if builders can match on the enum directly. CsCoreJobArtifact owns this.

Cost/Risk: arrow-rs builder APIs already in-tree; tests assert batch contents not Vec identity.

### F14 — MEDIUM — COPIES — Staged APFS ops `to_vec()` the expected-path list twice per operation

Evidence: `packages/cowshed/crates/cowshed-core/src/storage/apfs.rs:796-808` (same shape at `:918-930`, `:1019-1029`,
`:1086-1098`)

```
            expected: plan.expected().to_vec(),
        ...
        let expected = plan.expected().to_vec();
        let operation = plan.operation().clone();
        let prepared = self
            .lane
            .dispatch(move || {
```

Problem: REGIME M — per adopt/fork/checkpoint/restore. `plan.expected()` is cloned into `CheckedApfsBackend` and again
into the blocking closure; `operation.clone()` is a third copy of the plan enum (paths inside).

Fix: `Arc<[PathBuf]>` on the plan, or move expected into the backend and pass `&expected` into `read_authoritative`
before dispatch. One clone into the `'static` closure is the floor; two is not.

Cost/Risk: four near-identical staged executors; fix all or they diverge. CsCoreApfsTriad / storage lifecycle.

### F15 — MEDIUM — COPIES — Image lock acquisition copies the path slice to sort it

Evidence: `packages/cowshed/crates/cowshed-core/src/storage/apfs/native.rs:186-191`

```
    let mut paths = paths.to_vec();
    paths.sort();
    paths.dedup();
    let mut files = Vec::with_capacity(paths.len());
    for path in paths {
```

Problem: REGIME M — per APFS lock. Sort needs owned or indices; `to_vec` clones every PathBuf. The caller already owns
the list.

Fix: sort indices into a `Vec<usize>` (closed-form size = `paths.len()`), or take `&mut [PathBuf]` if the caller can
donate order. Dedup by index.

Cost/Risk: lock order is load-bearing (deadlock avoidance); keep the sort, drop the path clones.

### F16 — MEDIUM — COPIES — Cache disk record re-owns every header as `(String, String)`

Evidence: `packages/cowshed/crates/cowshed-gateway/src/cache.rs:1191`, `:1206-1212`

```
    headers: Vec<(String, String)>,
        for (name, value) in &response.headers {
            ...
            headers.push((name.as_str().to_owned(), text.to_owned()));
```

Problem: REGIME M — per cache fill (not hit). HeaderMap is already stored in `CachedResponse`; serialising to disk
copies every name/value into Strings, then JSON will copy again.

Fix: write the header block as length-prefixed bytes (the on-disk region already has a size bound, `MAX_HEADER_BYTES`).
Keep one binary form; JSON DiskRecord is a second encoding of the same bytes.

Cost/Risk: cache format bump (`CACHE_VERSION`). CsGwCache.

### F17 — MEDIUM — COPIES — Churn description formats every changed path, then keeps eight

Evidence: `packages/cowshed/crates/cowshed-core/src/copy.rs:201-217`, `:559-575`

```
        let changes = describe_churn(&observed, &current);
        ...
        .take(CHURN_SAMPLE_LIMIT)
        .map(String::as_str)
        .collect::<Vec<_>>()
        .join(", ");
            None => changes.push(format!("added {}", render_path(path))),
```

Problem: REGIME M — on a churning adopt (up to 6 passes). `describe_churn` allocates a `String` per changed path
(`render_path` also allocates); the success path (empty changes) is fine. Exhaustion then `collect::<Vec<_>>().join`
of 8.

Fix: count + sample in one walk; stop allocating after `CHURN_SAMPLE_LIMIT`. `render_path` can write into a `String`
buffer. Empty-change detect can compare snapshots without formatting.

Cost/Risk: only the conflict error path; still paid on the live-tree adopt that is already I/O bound — keep as MEDIUM.

### F18 — MEDIUM — COPIES — Workspace token `read_to_string` on every spawn

Evidence: `packages/cowshed/crates/cowshed-core/src/runtime/supervisor.rs:1220-1244`

```
        let workspace_token = tokio::fs::read_to_string(&token_path)
            .await
            ...
        let gateway_http = gateway_proxy_url(&port_base, &workspace_token);
            .env("COWSHED_WORKSPACE_TOKEN", workspace_token)
            .env("HTTP_PROXY", &gateway_http)
            .env("HTTPS_PROXY", &gateway_http)
            .env("http_proxy", &gateway_http)
            .env("https_proxy", &gateway_http)
```

Problem: REGIME M — per exec. Token bytes are immutable for the workspace generation (L7: re-validate/re-read immutable
bytes). Proxy URL is built once then cloned into four env keys via `.env` (each copies the value into the Command).

Fix: cache token+proxy URL on `SupervisorState` at attach; refresh on credential rotation. Set
`HTTP_PROXY`/`HTTPS_PROXY` once and skip the lowercase duplicates if the wrapper already canonicalises.

Cost/Risk: token rotation must invalidate the cache (workspace_credentials).

### F19 — MEDIUM — COPIES — Exec DTO / napi / supervisor restated `HashMap`/`BTreeMap<String, String>` env

Evidence: `packages/cowshed/crates/cowshed-core/src/api/dto.rs:1846`,
`packages/cowshed/crates/cowshed-core/src/runtime/project.rs:1310`,
`packages/cowshed/crates/cowshed-napi/src/lib.rs:117`,
`packages/cowshed/crates/cowshed-core/src/runtime/supervisor.rs:163`

```
    pub env: HashMap<String, String>,
    env: std::collections::HashMap<String, String>,
    env: HashMap<String, String>,
    pub env: BTreeMap<String, String>,
```

Problem: REGIME M — per exec. String keys for env are honest (POSIX env is bytes/strings) but the same map is rebuilt
HashMap→BTreeMap across the wire (`into_iter().collect()` at supervisor `:2386`). That is a full copy of every key and
value at the boundary.

Fix: one map type end-to-end (`BTreeMap` if order is part of the audit story, else `HashMap`). Convert once at the
process-env apply site, not at every layer. Cross-slice SSOT with XcutCowshedDup / CsCoreApi.

Cost/Risk: JSON DTO stability; napi serde.

### F20 — MEDIUM — COPIES — Git hook files `read_to_end` into a fresh `Vec` twice on the write path

Evidence: `packages/cowshed/crates/cowshed-core/src/git.rs:237-238`, `:266-267`

```
    let mut existing = Vec::new();
    file.read_to_end(&mut existing).map_err(|error| {
...
    let mut existing = Vec::new();
    file.read_to_end(&mut existing).map_err(|error| {
```

Problem: REGIME M — per workspace prepare. `append_environment_hook` opens the file then reads it whole to decide
whether to append; `read_environment_hook` is the same shape. Hooks are small, but the pattern is unbounded
(`read_to_end` with no cap — unlike `gateway_inventory.rs:1250` which `take(maximum + 1)`).

Fix: one helper with a size cap; `append` can reuse the read. Streaming search for the marker bytes without holding the
whole file if hooks ever grow.

Cost/Risk: missing cap is the real defect if a hook is replaced with a large file. CsCoreGit.

### F21 — LOW — COPIES — CLI workspace tables are `Vec<Vec<String>>`

Evidence: `packages/cowshed/crates/cowshed-cli/src/runtime.rs:2250-2284`

```
    let rows: Vec<Vec<String>> = workspaces
        .iter()
        .map(|workspace| workspace_row(None, workspace))
        .collect();
        .to_owned(),
```

Problem: REGIME L — once per `ls`. Nested Vec plus `.to_owned()` of `"mounted"`/`"detached"` literals. Not a hot loop.

Fix: emit into one `String` / row buffer; write `&str` cells. Literal states do not need `to_owned`.

Cost/Risk: output alignment helper currently wants `Vec<Vec<String>>`.

### F22 — LOW — COPIES — fstab rebuild owns every existing line then `join`s

Evidence: `packages/cowshed/crates/cowshed-core/src/storage/fstab.rs:20-45`

```
    let mut lines = existing_text
        .lines()
        ...
        .map(str::to_owned)
        .collect::<Vec<_>>();
    Ok(format!("{}\n", lines.join("\n")))
```

Problem: REGIME L — once per setup/uninstall. Collect-then-join copies every line twice (String then join).

Fix: write into one `String` with `push_str` + `\n`, skip cowshed-tagged lines by scan. Size is closed form:
`existing_text.len() + pins * line`.

Cost/Risk: none; setup path.

### F23 — LOW — COPIES — Project recover collect-then-iterate attached workspaces

Evidence: `packages/cowshed/crates/cowshed-core/src/runtime/project.rs:4368-4378`

```
        let attached = authoritative
            .into_iter()
            .filter(|workspace| {
                matches!(
                    workspace.derived.mount_state,
                    crate::storage::lifecycle::MountState::Mounted { .. }
                )
            })
            .collect::<Vec<_>>();
        for workspace in attached {
            self.ensure_supervisor_for(workspace).await?;
```

Problem: REGIME L — once per project open. Collect only exists because the loop `.await`s; `into_iter().filter()` could
feed the loop directly.

Fix: `for workspace in authoritative.into_iter().filter(...) { ... }`.

Cost/Risk: none.

### F24 — LOW — COPIES — `parse_one_string` / `parse_one_path` `to_vec` out of an already-sliced stdout

Evidence: `packages/cowshed/crates/cowshed-core/src/git.rs:1921-1929`

```
    let value = parse_one_line(bytes, description)?;
    String::from_utf8(value.to_vec())
...
    Ok(PathBuf::from(OsString::from_vec(value.to_vec())))
```

Problem: REGIME L/M — one extra copy per git one-liner (`branch name`, `commit revision`, …). `from_utf8(vec)` reuses
the vec, so this is one copy, not two; still a copy out of `Output.stdout` that could be
`str::from_utf8(value)?.to_owned()` or `from_utf8` on the whole stdout then split.

Fix: `str::from_utf8(value).map(str::to_owned)` (same copies, clearer); or return `&str` into `stdout` at the call site
when the Output is kept.

Cost/Risk: tiny; fold into F11.

### F25 — LOW — COPIES — Publication / artifact path walk collects `components()` into a Vec

Evidence: `packages/cowshed/crates/cowshed-core/src/storage/job_artifact.rs:1609`,
`packages/cowshed/crates/cowshed-core/src/storage/job_artifact/publication.rs:87`

```
        let components = relative.as_path().components().collect::<Vec<_>>();
        let components = relative.components().collect::<Vec<_>>();
```

Problem: REGIME M-for-open, small N (path depth). Collect exists to index `last()` / `len()-1`. Components iterator can
be peeked without a Vec.

Fix: iterate with `Peekable`; last component is the file. Depth is bounded — if you keep a Vec, `with_capacity(8)`.

Cost/Risk: none. Two copies of the same helper — SSOT for XcutCowshedDup.

### F26 — LOW — COPIES — `format!` density is error/CLI, not a hot kernel

Evidence: prod count 1056 `format!` (this sweep); representative
`packages/cowshed/crates/cowshed-core/src/git.rs:1901-1907`,
`packages/cowshed/crates/cowshed-cli/src/runtime.rs:202-203`

```
        format!("failed to {operation} (git status {})", output.status)
        format!("failed to {operation}: {detail}")
```

Problem: REGIME L — almost all `format!` sites are `CowshedError::*` construction. Not a finding to delete them; listing
so the 1056 count is not mistaken for a hot-loop verdict (PH §4.1 profile trap / “say the regime first”).

Fix: none on the error paths. Do not “optimise” error formatting.

Cost/Risk: n/a.

## Cross-slice questions

- `copy.rs` PathBuf-intern design: CsCoreCopy owns the module; this slice only ranked the clones.
- `git.rs` `parse_lines` / hook `read_to_end`: CsCoreGit.
- `actor.rs` String session keys vs typed `WorkspaceName`: CsGwActor; proxy `AcceptContext.workspace_id` must match
  (CsGwProxy).
- `CachedResponse.clone` / 64 KiB buffer: CsGwCache.
- Env `HashMap<String,String>` (dto/napi/project) vs `BTreeMap<String,String>` (supervisor): XcutCowshedDup / CsCoreApi
  — same concept restated, and the conversion is a copy (F19).
- Arrow column `collect::<Vec<_>>`: CsCoreJobArtifact; ColArrow / XcutArrow may already have a builder pattern in
  columine/lmao.
- `plan.expected().to_vec()` duplicated across staged APFS executors: CsCoreApfsTriad.
- `components().collect::<Vec<_>>` in `job_artifact.rs` and `publication.rs`: same helper twice — XcutCowshedDup.

## Non-findings (checked, clean)

- `Arc::clone` (143 prod): refcount; not a data copy. napi `spawn_promise` Arc-clones the coordinator per JS call —
  correct `'static` boundary.
- Most `.to_owned()` on `&Path` into error structs (`MetadataError::Io { path: path.to_owned() }`): once per failure,
  owned error type requires it.
- `spawn_blocking` / `dispatch_blocking` PathBuf `to_owned` before `move` (e.g. `copy.rs:170-171`): required to cross
  the thread boundary; one copy, not a loop.
- `Vec<Vec<_>>` outside CLI tables: only test argv recorders (`cowshed-cli/tests`, `cowshed-gateway/tests`).
- Cache `read_to_end` with `take(MAX_* + 1)` in gateway control/helper/platform: bounded, correct.
- `HashMap<u64, PermitState>`: integer key, fine.
- In-file tests’ `b"...".to_vec()` fixtures: not production.
- `format!` on usage/help/error: regime L, left standing (F26).
- No `to_os_string()` in src.
- No evidence of `clone()` of a 500-byte by-value `self` in a per-element scan (PH §7.10b) in this workspace — cowshed
  is not a row engine. Gateway `AcceptContext` is the closest (F4), cloned per request not per byte.

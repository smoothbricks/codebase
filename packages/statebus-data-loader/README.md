# StateBus Data Loader

Typed resource lifecycle data, pure admission/progress reducers, exact-interest ports, and measured-byte stream helpers.
There is no React dependency, QueryClient state mirror, module-global loader, or writable application-state port.

## Data flow

```text
screen interest -> loadRequested -> application reducer admission
                -> execution adapter -> progress/result event -> application reducer
```

`LoadState<T, Failure>` distinguishes `not-requested`, `loading`, `ready`, `failed`, and `cancelled`. Loading, failure,
and cancellation retain previous successful data. A success is not confused with an empty or missing result. Every
request carries an exact `{ key, id? }` interest, request ID, fingerprint, reason, supplied timestamp, and admission
policy. `reduceLoadState` writes only the addressed resource; other resource identities and stale outcomes are neutral.
`admitLoadRequest` exposes duplicate and request-ID/fingerprint conflicts without running an effect. `latest-wins`
supersedes an earlier request; `drop-duplicate` retains the first equivalent running fingerprint. IDs must be unique for
logical operations. Retries retain the original ID; a new refresh gets a new ID. These read helpers do not establish
server-side mutation idempotency, serialize writes, or implement publication workflows.

The application owns the event topic, state declaration, failure codec, and reducer. `LoaderChannel` binds this contract
to that application's typed event capability and readonly state. Its subscriber must observe **batch-final reduced
state**, not a synchronous raw event emitter. Runtime objects, signals, and functions live only in injected ports. Keep
`T`, `Failure`, request metadata, and emitted events plain and serializable.

## Exact interest

`statebusInterestSource(bus)` forwards the core's exact interest changes and supplies a snapshot for late installation.
A number ID and the same printed string ID are different resources. A missing ID is scalar/whole-property interest, not
a request to guess a ByID entry. Namespace the key and encode all semantic resource dimensions in its ID. An app might
own `cms.content` and another `host.members`; these packages declare neither application's ambient schema.

Final zero is demand withdrawal, **not eviction**. The application reducer retains data until an explicit domain policy
removes it. React's `useSubstate(key, id)` reports the exact ID, and computed hooks can declare interest as a pure
function of their props.

## Byte progress

`createByteProgressReporter` coalesces actual cumulative measurements independently for upload and download. It emits at
most once per direction per interval, plus an explicit final flush. The interval is a throttle, not a debounce:
continuous chunk arrival cannot postpone notification forever. Disposal drops pending measurements and cancels timers.
Idle time does not manufacture progress.

`measureByteStream(source, report, { direction, total? })` forwards an async byte stream while measuring actual
`Uint8Array.byteLength`. Early iterator return closes the source. Feed these samples into the execution adapter's
`reportBytes` callback. The transport must itself honour its AbortSignal, including any blocked read.

Only supply `total` when it describes the measured stream. A compressed wire Content-Length may not match decompressed
response bytes; omit an incomparable or unknown total. The reducer rejects invalid byte counts, preserves monotonicity
within an attempt/direction, and resets counts for a new retry attempt. No raw stream, Response, uploaded buffer, or
AbortSignal belongs in a progress event. Mutation upload progress needs a transport that reports actual upload bytes;
this package does not claim ordinary `fetch` supplies those measurements automatically.

## Validation

`nx lint statebus-data-loader` and `nx test statebus-data-loader` use the repository's inferred TypeScript targets and
Bun lane. Properties cover stale results, cross-resource isolation, admission, previous-data retention, event streams,
and progress. The streaming/reporter tests use measured chunks and a deterministic timer primitive.

The complete post-reducer StateBus/QueryClient binding is exercised in
`../statebus-tanstack-query/src/__tests__/fixture.ts` and `loader.test.ts`; these use the production runtime and
reducer, not mocked hooks. This package is a resource lifecycle primitive, not the separate key-batching DataLoader
capability specified for future library composition.

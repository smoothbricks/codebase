# A new client cannot retire descendants under an older supervisor

Found 2026-11-26 on a consumer repository. Owner: cowshed.

A bounded-exec test target exited 1 after its tests passed:
`could not retire the command's processes: cowshed retire-descendants exited 7: cowshed: workspace supervisor protocol: malformed frame: unknown field gateActive, expected turns or busy`.
The workspace's supervisor was started by an older cowshed than the installed client, and the client sent a frame field
the older supervisor does not know. A cowshed install replaces the client while supervisors of live workspaces keep
running, so this mismatch is ordinary after every release.

## What must hold

- A client and a live supervisor of different releases either speak a common frame or the client restarts the supervisor
  first; a test's verdict never fails on the version skew.

## Red

A supervisor of release N serving a client of release N+1 that added a frame field answers `retire-descendants` without
a protocol error.

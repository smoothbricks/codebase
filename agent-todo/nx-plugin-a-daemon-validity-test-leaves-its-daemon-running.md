# A daemon-validity fixture's Nx daemon outlived its test run

`packages/nx-plugin/src/nx-daemon-validity.test.ts`, the fixture whose scratch directory is named
`nx-daemon-validity-lock-*`.

## What is measured

On 2026-11-16 the nx-plugin tests ran in a workspace on the host lane. An hour after its parent had gone, one process of
that run was still alive, reparented to launchd:

```
bun <workspace>/node_modules/.bun/nx@24.0.3+…/node_modules/nx/dist/src/daemon/server/start.js
cwd <store>/tmp/<workspace>/smoothbricks-fixtures/nx-plugin/run-42311/nx-daemon-validity-lock-Sxzg83/workspace
```

94 MB resident, 3 s of CPU in all. By its working directory it is the Nx daemon of the fixture's scratch workspace. It
was ended by pid.

Not checked: whether the test tries to stop that daemon and the stop failed, and whether the run that left it had
finished or was cut short.

A process whose working directory is inside a workspace is one of the holders that refuse that workspace's detach.

## What to change

The test that starts a daemon in a scratch workspace ends it before the test returns, on the passing and on the failing
path: by the daemon's own stop for that workspace, and by its pid if the stop does not answer. If it already does, the
case in which the daemon survived is found first and named here.

## Done when

- After the test file has run, no process has its working directory under the fixture's scratch directory. The test
  asserts this itself, by the pids it started.
- The same holds when an assertion in the test fails first, and when the run is ended by a signal.

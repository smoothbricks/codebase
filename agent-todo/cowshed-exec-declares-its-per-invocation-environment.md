# cowshed exec declares the variables it sets per invocation

Found 2026-11-26 while measuring ttsc's warm launch inside sheds. Owner: cowshed.

Two identical `cowshed exec <ws> -- …` runs of one shed hand their command environments that differ in `MAKEFLAGS` and
`CARGO_MAKEFLAGS` (the jobserver fifo path) and in `COWSHED_INSTALL_OBSERVED`. Through direnv they also differ in
`DIRENV_DIFF` and `DIRENV_WATCHES`. A tool that keys a cache on its environment misses on every exec. ttsc's plugin
build-environment record did: four records rewritten per launch, with `go version` and `go env` spawned again each time.
ttsc now leaves these variables out of the key (`runtimeBuildEnvironment.unkeyed`, samchon/ttsc,
`fix(ttsc): a warm launch replays its plugin build environment`). That list only holds while it matches what cowshed
actually varies, and nothing states that today.

## What must hold

- cowshed declares, in one place, the variables whose value an exec sets per invocation, each with why it varies.
- Between two execs of one shed with the same command, the job's environment differs only in declared variables.
- A consumer that keys on the environment (ttsc's `unkeyed`) cites the declared list, so a new per-invocation variable
  is added in one reviewed place.

## Red

Two `cowshed exec` runs of one shed print their environments. The environments differ in exactly the declared variables.
A variable added to the per-invocation set without being declared fails the test.

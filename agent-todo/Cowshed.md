# Cowshed

- Post-reboot proof owed for fixture-attach-pool (landed): run `COWSHED_TIMING=1` nextest `apfs_native` + `clone_intent`
  real-APFS subset and count `apfs-command attach.*start` lines (one shared attach in the flag table). New
  `diskutil image attach` calls hang host-wide until the attach pool recovers, so this proof could not run before the
  reboot; compile and merge evidence are recorded in the landing.

- Qualify the initial `jobCpuSamplePeriod` (1 s) and observed-CPU leaf coverage threshold (900‰) on real Cargo/Nx/Bun
  gate jobs; report sampler cost, `cpuPct` stability and coverage evidence (specs/cowshed/07_api.md, "Job monitoring"
  and "Process-tree observations").

- Specify and implement a supported record-driven forget for corrupt or orphaned project registrations. The current
  `specs/cowshed/06_cli.md` §Commands (`mount main`, `rm`) and `02_workspaces.md` §`cowshed rm` provide remount and
  checkout-based removal, not removal when the recorded checkout and its marker are unavailable. Five smoke
  registrations keep setup at integrity exit 7 while the installed daemon is correct and running. Their supported
  remount/removal attempts refuse missing checkout parents, duplicate image attachments, or a missing workspace marker;
  no store records are deleted manually. Define the ownership and preservation contract before adding the verb; prove
  exact-project removal without damaging an uncertain attachment or another project.
- Make smoke fixture disposal and interrupted-run recovery leave no orphaned project registrations. Keep
  `specs/cowshed/08_testing.md` §Integration tests' owning teardown and retained-failure evidence, and
  `02_workspaces.md` §`cowshed rm`'s verified preservation fences. Cover success, assertion failure, panic, and killed
  runs whose temporary checkout root disappears; an unavailable root must not make its durable registration invisible to
  recovery or require raw store deletion.

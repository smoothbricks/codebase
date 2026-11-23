# A fresh cowshed controller says nothing for 15 s when it rebases after a killed snapshot job

Seen in a consumer's host test gate (a scratch workspace driven by the consumer's scratch driver through the cowshed
controller on a socketpair):

- the workspace's retirement repair reported "still ran jobs besides its Nx workers at retirement: job 9" (a worktree
  snapshot `git add -A`);
- the driver ended the job (SIGTERM) and asked the controller to rebase the workspace;
- the controller, spawned 0.5 s earlier, wrote no progress at all for 15 s ("last progress: nothing"), and the client's
  silence bound refused the call;
- a `sample` of the controller could not answer inside 10 s (`task_threads failed`), so what it waited on is not
  recorded. Other cowshed volume work was live at the same moment.

Owed:

- the controller writes progress from its first step, including any wait on a disk lease, the workspace's dying job or a
  lock, each wait named;
- a rebase after a killed snapshot job either waits for that job's exit as a named, bounded step or refuses at once
  naming the job.

Red: a controller asked to rebase a workspace whose snapshot job was just signalled reports progress (or a named
refusal) within one heartbeat.

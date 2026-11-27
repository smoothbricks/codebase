# Cowshed fails a green task when a killed descendant is still in a kernel wait

Measured 2026-11-28 00:5x, SB land gate of hts-aspects-lmao (gate output
/private/cowshed/store/smoothbricks/codebase/tmp/main/land-gate.ftEyzX:343-390):

- `cli:test-shard-1` finished green: 487 pass, 0 fail, 74 s.
- The task still failed: "cowshed: could not retire the command's processes: cowshed retire-descendants exited 4: job
  1284's processes still run 2s after SIGKILL, every exit queued for them folded: pid 3292 in an uninterruptible kernel
  wait … next: they stay owned and watched; retry the retirement".
- Minutes later pid 3292 had exited on its own.

A process in an uninterruptible kernel wait cannot act after SIGKILL; its exit is already queued and arrives when the
kernel call returns. Failing the task's verdict on it turns a slow kernel return into a red gate.

Owed (cowshed retirement, owner of retire-descendants):

- The task's verdict is its command's verdict. A descendant that is SIGKILLed and sits in an uninterruptible wait stays
  owned and watched (as the message already says) and is reaped when its exit arrives, without failing a task whose
  command succeeded. The job's own lease or slot is what stays held until then, not the task's result.
- Red: a command that exits 0 while a descendant blocks in an uninterruptible wait past the 2 s window yields a green
  task, and the descendant is reaped once the wait returns.

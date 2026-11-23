# script_host's leader FIFO read waits for an EOF nobody in its tree owes

`cowshed-shell::script_host killing_a_script_job_group_reaches_its_grandchildren_and_spares_the_host` hung in a land
gate (other-build-wait, 2026-11-23 16:47) until the 180 s idle bound: the test process sat in `read_to_string(leader)`
holding the FIFO's read end with 6 bytes read (the leader's pid), waiting for EOF, while no process of its tree held the
write end (lsof of the tree's pids; the leader brush process and the descendant `sh` held only /dev/null, pipes,
kqueues). Alone it passes in 0.05 s, so a write end outlived its writer somewhere outside the tree (an inherited
descriptor is the likely shape, unproven).

Owed:

- the read does not wait on EOF: the leader writes one line and the test reads one line;
- the FIFO write end is opened close-on-exec wherever the host or the fixture opens it, so no spawned process inherits
  it;
- the watchdog's hang snapshot samples the test process itself and runs `sudo -n lsof <fifo path>`, which names a holder
  outside the tree.

Red: a fixture that forks a process holding an inherited copy of the write end still finishes the read.

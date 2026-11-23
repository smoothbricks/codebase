# A draining workspace supervisor waits on jobs that never end

Seen 2026-11-23 in a consumer repository: a land rebuilt cowshed, so a busy shed's workspace supervisor (old build) went
into drain. A drain refuses all new work and waits for the old supervisor's jobs to end before it retires. That shed's
jobs were a long-lived Nx worker and shell hosts, which end only when their owner stops them, so every new command in
the shed failed `executor_internal` ("the shed's workspace supervisor … is draining and refuses new work") until the old
supervisor was killed by hand.

A drain must not hold new work hostage to jobs with no end of their own: either the new build's supervisor serves new
commands while the old one finishes its jobs, or a long-lived job is handed over or ended at a bounded point and
restarted by its owner. Red: a shed with a long-lived job keeps answering new commands across a cowshed rebuild.

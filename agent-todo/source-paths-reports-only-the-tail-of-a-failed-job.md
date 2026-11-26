# source-paths reports only the tail of a failed job

Found 2026-11-26 on a consumer repository's land. Owner: cowshed.

`cowshed land`'s `source-paths` step (`project.rs`, `source_paths`) printed only the tail of its job's stderr. The job
had failed in three steps: a `cargo metadata` call timed out, the Nx plugin's node pass threw, and Nx then judged the
partial graph and reported bogus overlapping `dist` outputs. Only the last error reached the land's output, so the cause
(the cargo timeout) was hidden and the report pointed at outputs that are not wrong.

## What must hold

- A failed `source-paths` job reports its first error, or the whole stderr bounded in size, not only its tail.

## Red

A `source-paths` job whose stderr carries an early error and a later one reports the early one.

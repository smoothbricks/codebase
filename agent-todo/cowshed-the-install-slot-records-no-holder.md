# The install slot records no holder, so a waiter names every waiter as holding it

`packages/cowshed/crates/cowshed-cli/src/install_basis.rs:47`, `InstallSlot::take`, takes an exclusive lock on
`dev.cowshed/install.lock` and records nothing.

A waiter's report comes from `cowshed_os::flock::queued` (`cowshed-os/src/flock.rs:193`, "still waiting for lock …, held
by …"). It lists every descriptor on the inode through `held_lock::holders` (`cowshed-core/src/held_lock.rs:326`). On
Darwin the kernel does not expose flock custody, so each descriptor shows as `Custody::Unexposed`, "has it open, custody
unexposed by the kernel". Only a holder that records itself is told apart as `Custody::Recorded` (`held_lock.rs:25`,
`:388-402`).

On 2026-11-26, land `bun-devenv` (`/private/tmp/land-bun-devenv.out:224-227`) waited in `reconnect_and_resume`
(`land_turn.rs:1072`) for the install of the dbd2a0788 release, `cowshed --json setup` (pid 37868). The report named
that install, a consumer workspace's `cowshed exec` (pid 40074), a `cowshed doc-commit` (pid 38755) and the land itself
(pid 6363), all in the same words. The `exec` and `doc-commit` were waiters, not holders: each was in
`wait_for_running_install`, called from `supervisor_manager.rs:1079` when its supervisor connect was refused during the
gateway restart. The report reads as if a long `cowshed exec` held the install slot for its whole run, and it was
reported that way.

To do: have the install slot record its holder the way the locks with `Custody::Recorded` do. A waiter then names the
install as holding the slot and every other opener as waiting. Red: a `held_lock` test with one `InstallSlot` holder and
one `wait_for_running_install` waiter on the same file asserts that the report names only the holder as holding it.
Today both show as `Unexposed`.

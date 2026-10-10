# Land compares-and-swaps a target branch nothing has checked out

`specs/cowshed/02_workspaces.md:795-804` (step 3 of `cowshed land`) has three cases, chosen by checkout state read under
the repository lock just before the update:

- the target branch is checked out in the target's checkout: fast-forward through that checkout (built);
- nothing has it checked out: compare-and-swap `refs/heads/<target>` from the expected target head to the validated
  source head, leaving the checkout's branch, `HEAD`, index and working tree untouched; a failed swap is a `Conflict`
  and changes nothing;
- a linked worktree holds it: refuse.

Step 4 (build-volume adoption) is skipped for the bare-ref case (`02_workspaces.md:819-821`). The same contract is in
`07_api.md:524-532`, `06_cli.md:226`, `12_mcp.md:116-121` and the test plan `08_testing.md:295-299`.

## What the code does

`packages/cowshed/crates/cowshed-core/src/runtime/project.rs:10077` `land` refuses any target the checkout does not have
checked out, and the test `land_moves_only_the_branch_main_has_checked_out` (`project.rs:7474`) pins that refusal. User
docs repeat it (`packages/cowshed/docs/cli.md:973-974`, `packages/cowshed/skills/cowshed/SKILL.md`).

## Work

1. In `land`, when the target branch is not checked out by the target's checkout or any linked worktree, update it with
   `git update-ref refs/heads/<target> <new> <expected-old>` (the primitive `preserve_workspace_branch` already uses),
   under the target lock, after revalidating the source incarnation and head. Skip the build-volume step.
2. Keep the linked-worktree refusal, and keep the checked-out path unchanged.
3. Convert the test at `project.rs:7474` from pinning the refusal to pinning the compare-and-swap.
4. Update `docs/cli.md` and the skill text once the code lands.

## Acceptance

- `08_testing.md:295-299`: land onto a branch nothing has checked out advances only `refs/heads/<target>`; the current
  branch, `HEAD`, index, status and file bytes are unchanged.
- A race that moves the target between validation and the update returns `Conflict` and changes no ref.
- A target checked out by a linked worktree is refused.

## Open decision

The pre-refusal design also rebased the source onto the target before validation. The current spec rejects an implicit
rebase (`02_workspaces.md:757-760`: it would land a head nobody named). That rejection stays until ruled otherwise.

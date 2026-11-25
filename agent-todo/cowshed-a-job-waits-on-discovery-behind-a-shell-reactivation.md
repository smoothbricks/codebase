# A job waits seven minutes on build-state discovery behind its shell's re-activation

Measured on 2026-11-25 in smoothbricks shed `mbx-pin-aging`, where `tooling/direnv/devenv.lock` had just moved the
pinned mbx (uncommitted in the worktree). The job was

```
cowshed exec mbx-pin-aging --host --class background -- sh -c 'nix flake lock --no-update-lock-file ./packages/cowshed/nix/mbx; nix build ./packages/cowshed/nix/mbx#mbx --no-link --print-out-paths'
```

It needs neither the workspace shell nor Cargo. Its wall was 466 s. Of that, 440 s went to 44 progress lines of one
message, every 10 s:

```
cowshed: build-state discovery has waited 440s on `cargo locate-project` in …/mbx-pin-aging (33 manifests, run as a job
after the workspace shell's activation); the command has not started; preparing its sandbox environment or waiting for
its workspace shell
```

The `nix build` then printed its store path within the remaining seconds: the build was already in the store.

## Reading (an inference, not yet measured)

The shell's devenv carries the pinned mbx as a package (`tooling/direnv/devenv.yaml`, input `mbx`). A devenv.lock that
moves the pin makes the next activation build that mbx from source. Discovery's `cargo locate-project` runs after the
workspace shell's activation (11_shell.md, near "Controller-only discovery runs locate-project/metadata --no-deps
--offline as background jobs in the canonical workspace supervisor, sharing its activated shell"), so the job waited for
an mbx build it never used. The job's own `nix build` then found that mbx already built.

## What to find

- Confirm the 440 s is the activation, and what the activation spent it on, from the supervisor's own spans: the shell
  host's activation start and end against the discovery job's admission, and whether a Rust build of mbx ran inside the
  activation.
- Whether a job whose command needs no build state (no Cargo, no Nx) has to wait for discovery at all, or whether its
  wait is on the job's own place in admission behind the discovery job. 11_shell says discovery shares the activated
  shell and ordinary job placement; it does not say that every exec waits for it.
- If the wait is required, whether the message can say what it waits on (activation, and the step inside it) rather than
  "preparing its sandbox environment or waiting for its workspace shell".

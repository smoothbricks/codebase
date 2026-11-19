import { githubActionPins } from './ci-workflow.js';

/** The codebase-only, read-only hosted probe; it never runs a release or land command. */
export function renderCowshedBootstrapReadinessJob(): string {
  return String.raw`
  cowshed-bootstrap-readiness:
    if: ${'$'}{{ inputs.bootstrap_readiness }}
    runs-on: macos-latest
    timeout-minutes: 60
    permissions:
      contents: read
    defaults:
      run:
        working-directory: ${'$'}{{ github.workspace }}/..
        shell: bash
    env:
      EXPECTED_MAIN: ${'$'}{{ inputs.bootstrap_main }}
    steps:
      - name: Require a fresh owned host and an exact main pin
        id: probe-root
        run: |
          set -euo pipefail
          test "$GITHUB_REPOSITORY" = smoothbricks/codebase
          case "$GITHUB_REF" in
            refs/heads/support/alex-mextner/gl668-hosted-macos-readiness-*) ;;
            *) echo "::error::Use the dedicated bootstrap support branch"; exit 1 ;;
          esac
          [[ "$EXPECTED_MAIN" =~ ^[0-9a-f]{40}$ ]]
          for path in "$HOME/Library/Application Support/dev.cowshed" /private/cowshed "$GITHUB_WORKSPACE/.cowshed"; do
            if [[ -e "$path" || -L "$path" ]]; then
              echo "::error::Refusing an existing Cowshed host or project: $path"
              exit 1
            fi
          done
          PROBE_ROOT="$RUNNER_TEMP/cowshed-bootstrap-$GITHUB_RUN_ID-$GITHUB_RUN_ATTEMPT"
          mkdir "$PROBE_ROOT"
          mkdir "$PROBE_ROOT/evidence"
          printf 'PROBE_ROOT=%s\n' "$PROBE_ROOT" >> "$GITHUB_ENV"
          printf '%s\n' "$EXPECTED_MAIN" > "$PROBE_ROOT/evidence/expected-main.txt"
          printf 'ready=true\n' >> "$GITHUB_OUTPUT"

      - name: Checkout the pinned canonical main
        uses: ${githubActionPins.checkout.ref} # ${githubActionPins.checkout.version}
        with:
          ref: ${'$'}{{ inputs.bootstrap_main }}
          fetch-depth: 0
          persist-credentials: false

      - name: Verify current main before executing its setup action
        run: |
          set -euo pipefail
          test "$(git -C "$GITHUB_WORKSPACE" rev-parse HEAD)" = "$EXPECTED_MAIN"
          current=$(git ls-remote https://github.com/smoothbricks/codebase.git refs/heads/main)
          test "$current" = "$(printf '%s\trefs/heads/main' "$EXPECTED_MAIN")"
          git -C "$GITHUB_WORKSPACE" switch --create main "$EXPECTED_MAIN"
          checkout_status=$(git -C "$GITHUB_WORKSPACE" status --porcelain)
          test -z "$checkout_status"
          git -C "$GITHUB_WORKSPACE" rev-parse HEAD 'HEAD^{tree}' > "$PROBE_ROOT/evidence/source.txt"

      - name: Install the official multi-user Nix host prerequisite
        timeout-minutes: 10
        run: |
          set -euo pipefail
          test ! -e /nix
          test ! -L /nix
          case "$(uname -m)" in
            arm64) system=aarch64-darwin; checksum=71e18301c4ea78c667f2753159156b5bdb899993720e8aa7bcca97e8312d3d6b ;;
            x86_64) system=x86_64-darwin; checksum=8bf3dadfd65be182ad3141b1224bbc82e0f2a61d4f36781938b5e6ede029c2a3 ;;
            *) echo "::error::Unsupported macOS architecture"; exit 1 ;;
          esac
          nix_root="$PROBE_ROOT/nix"
          mkdir "$nix_root"
          payload="$nix_root/nix-2.34.7-$system.tar.xz"
          curl --fail --location --proto '=https' --tlsv1.2 --max-time 120 \
            "https://releases.nixos.org/nix/nix-2.34.7/nix-2.34.7-$system.tar.xz" --output "$payload"
          printf '%s  %s\n' "$checksum" "$payload" | shasum -a 256 -c -
          tar -xJf "$payload" -C "$nix_root"
          printf 'experimental-features = nix-command flakes\naccept-flake-config = true\n' > "$nix_root/nix.conf"
          "$nix_root/nix-2.34.7-$system/install" --daemon --yes --no-channel-add \
            --nix-extra-conf-file "$nix_root/nix.conf" 2>&1 | tee "$PROBE_ROOT/evidence/nix-install.log"
          /usr/bin/dscl . -read /Groups/nixbld > "$PROBE_ROOT/evidence/nix-build-group.txt"
          /bin/launchctl print system/org.nixos.nix-daemon > "$PROBE_ROOT/evidence/nix-daemon.txt"
          /nix/var/nix/profiles/default/bin/nix --store daemon store ping
          /nix/var/nix/profiles/default/bin/nix --version > "$PROBE_ROOT/evidence/nix-version.txt"
          {
            printf '%s\n' /nix/var/nix/profiles/default/bin
            printf '%s\n' "$HOME/.nix-profile/bin"
          } >> "$GITHUB_PATH"
          printf 'NIX_REMOTE=daemon\nDEVENV_WORKDIR=%s\n' "$GITHUB_WORKSPACE" >> "$GITHUB_ENV"

      - name: Enable the canonical public devenv binary cache
        uses: ${githubActionPins.cachix.ref} # ${githubActionPins.cachix.version}
        with:
          name: devenv
          skipPush: true

      - name: Install the canonical locked devenv
        timeout-minutes: 25
        working-directory: ${'$'}{{ github.workspace }}/tooling/direnv
        run: ./github-actions-bootstrap.sh install-devenv

      - name: Prepare the canonical devenv shell
        timeout-minutes: 25
        working-directory: ${'$'}{{ github.workspace }}/tooling/direnv
        run: ./github-actions-bootstrap.sh build-shell

      - name: Use the existing rooted Bun profile
        run: |
          set -euo pipefail
          stable_bin="${'$'}{DEVENV_WORKDIR:?}/tooling/direnv/.devenv/profile/bin"
          test -x "$stable_bin/bun"
          printf '%s\n' "$stable_bin" >> "$GITHUB_PATH"
          printf 'COWSHED_BUN_PROFILE_BIN=%s\n' "$stable_bin" >> "$GITHUB_ENV"
          printf '%s\n' "$stable_bin" > "$PROBE_ROOT/evidence/bun-profile-bin.txt"

      - name: Install published Cowshed 0.5.2 with its dependencies
        run: |
          set -euo pipefail
          curl --fail --location --proto '=https' --tlsv1.2 --max-time 120 \
            https://registry.npmjs.org/@smoothbricks/cowshed/-/cowshed-0.5.2.tgz \
            --output "$PROBE_ROOT/cowshed-0.5.2.tgz"
          python3 - <<'PY'
          import base64, hashlib, os, pathlib
          root = pathlib.Path(os.environ["PROBE_ROOT"])
          payload = (root / "cowshed-0.5.2.tgz").read_bytes()
          expected = "PGF6/yb9/dpHVvFTEjy5jwF+7Qvno4k0Uxac4ZAYtiahgDbqRg/8AC4ASHg+YYB3IhtL3ZIvlpBbNXX4TGKLDw=="
          actual = base64.b64encode(hashlib.sha512(payload).digest()).decode()
          if actual != expected:
              raise SystemExit("published Cowshed package integrity mismatch")
          (root / "evidence/package-integrity.txt").write_text("sha512-" + actual + "\n")
          PY
          tar -xzf "$PROBE_ROOT/cowshed-0.5.2.tgz" -C "$PROBE_ROOT"
          mkdir "$PROBE_ROOT/install"
          printf '{"private":true,"dependencies":{"@smoothbricks/cowshed":"file:../cowshed-0.5.2.tgz"}}\n' \
            > "$PROBE_ROOT/install/package.json"
          (cd "$PROBE_ROOT/install" && bun install)
          python3 - <<'PY'
          import json, os, pathlib
          root = pathlib.Path(os.environ["PROBE_ROOT"])
          package = json.loads((root / "package/package.json").read_text())
          if package["name"] != "@smoothbricks/cowshed" or package["version"] != "0.5.2":
              raise SystemExit("unexpected Cowshed package identity")
          (root / "evidence/package.json").write_text(json.dumps({
              "name": package["name"], "version": package["version"]
          }, indent=2) + "\n")
          PY
          case "$(uname -m)" in
            arm64) platform=darwin-arm64 ;;
            x86_64) platform=darwin-x64 ;;
            *) echo "::error::Unsupported macOS architecture"; exit 1 ;;
          esac
          native="$PROBE_ROOT/install/node_modules/@smoothbricks/cowshed/dist/bin/$platform/cowshed"
          test -x "$native"
          cmp "$PROBE_ROOT/package/dist/bin/$platform/cowshed" "$native"
          shasum -a 256 "$native" > "$PROBE_ROOT/evidence/native-sha256.txt"
          printf 'COWSHED_NATIVE=%s\n' "$native" >> "$GITHUB_ENV"

      - name: Record the initial native diagnosis
        run: |
          set -euo pipefail
          set +e
          "$COWSHED_NATIVE" --json doctor > "$PROBE_ROOT/evidence/doctor-before.json" 2> "$PROBE_ROOT/evidence/doctor-before.stderr.log"
          code=$?
          set -e
          printf '%s\n' "$code" > "$PROBE_ROOT/evidence/doctor-before.exit"
          cat "$PROBE_ROOT/evidence/doctor-before.json"
          cat "$PROBE_ROOT/evidence/doctor-before.stderr.log" >&2
          # Exit 5 is the native doctor result for error findings on an unconfigured host.
          # It is retained as a diagnostic, never reported as successful setup.
          test "$code" -eq 0 || test "$code" -eq 5

      - name: Run ordinary Cowshed setup
        id: cowshed-setup
        timeout-minutes: 5
        run: |
          set -euo pipefail
          printf 'attempted=true\n' >> "$GITHUB_OUTPUT"
          "$COWSHED_NATIVE" setup 2>&1 | tee "$PROBE_ROOT/evidence/setup.log"

      - name: Start the installed gateway and require native readiness
        timeout-minutes: 5
        run: |
          set -euo pipefail
          # Native gateway start waits for healthy control-socket service.
          "$COWSHED_NATIVE" --json gateway start | tee "$PROBE_ROOT/evidence/gateway-start.json"
          "$COWSHED_NATIVE" --json gateway status | tee "$PROBE_ROOT/evidence/gateway-before-adopt.json"

      - name: Adopt only the pinned canonical main
        id: cowshed-adopt
        timeout-minutes: 10
        run: |
          set -euo pipefail
          test "$(git -C "$GITHUB_WORKSPACE" rev-parse HEAD)" = "$EXPECTED_MAIN"
          printf 'attempted=true\n' >> "$GITHUB_OUTPUT"
          "$COWSHED_NATIVE" adopt "$GITHUB_WORKSPACE" 2>&1 | tee "$PROBE_ROOT/evidence/adopt.log"
          main_path=$("$COWSHED_NATIVE" --project "$GITHUB_WORKSPACE" path main)
          test "$(git -C "$main_path" rev-parse HEAD)" = "$EXPECTED_MAIN"
          checkout_status=$(git -C "$main_path" status --porcelain)
          test -z "$checkout_status"
          git -C "$main_path" rev-parse HEAD 'HEAD^{tree}' > "$PROBE_ROOT/evidence/adopted-source.txt"
          test -x "$COWSHED_BUN_PROFILE_BIN/bun"
          "$COWSHED_NATIVE" --project "$main_path" --json doctor | tee "$PROBE_ROOT/evidence/doctor-adopted.json"
          "$COWSHED_NATIVE" --json gateway status | tee "$PROBE_ROOT/evidence/gateway-adopted.json"
          printf 'Published Cowshed setup, gateway health, and pinned main adoption passed.\n' \
            > "$PROBE_ROOT/evidence/readiness.txt"

      - name: Unadopt this probe's project through the native command
        if: ${'$'}{{ always() && steps.cowshed-adopt.outputs.attempted == 'true' }}
        timeout-minutes: 5
        run: |
          set -euo pipefail
          "$COWSHED_NATIVE" --project "$GITHUB_WORKSPACE" unadopt 2>&1 | tee "$PROBE_ROOT/evidence/unadopt.log"
          test -x "$COWSHED_BUN_PROFILE_BIN/bun"

      - name: Remove this probe's installed host presence through the native command
        if: ${'$'}{{ always() && steps.cowshed-setup.outputs.attempted == 'true' }}
        timeout-minutes: 5
        run: |
          set -euo pipefail
          "$COWSHED_NATIVE" setup --uninstall 2>&1 | tee "$PROBE_ROOT/evidence/uninstall.log"
          # Native uninstall preserves volumes and retained checkouts. The disposable
          # hosted VM removes those when the job ends; this probe never forces deletion.

      - name: Retain the readiness evidence
        if: ${'$'}{{ always() && steps.probe-root.outputs.ready == 'true' }}
        uses: ${githubActionPins.uploadArtifact.ref} # ${githubActionPins.uploadArtifact.version}
        with:
          name: cowshed-bootstrap-readiness-${'$'}{{ github.run_id }}-${'$'}{{ github.run_attempt }}
          path: ${'$'}{{ env.PROBE_ROOT }}/evidence/
          if-no-files-found: error
`;
}

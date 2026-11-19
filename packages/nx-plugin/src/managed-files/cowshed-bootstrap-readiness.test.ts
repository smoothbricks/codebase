import { expect, it } from 'bun:test';
import { renderCowshedBootstrapReadinessJob } from './cowshed-bootstrap-readiness.js';

// Daemon trust must exist at installation, before Cachix configures the CI user's cache.
it('lets only the disposable runner configure caches through the Nix daemon', () => {
  const rendered = renderCowshedBootstrapReadinessJob();
  const configuration = `printf 'experimental-features = nix-command flakes\\naccept-flake-config = true\\ntrusted-users = root %s\\n' "$(id -un)" > "$nix_root/nix.conf"`;
  expect(rendered).toContain(configuration);
  expect(rendered.indexOf(configuration)).toBeLessThan(rendered.indexOf('--nix-extra-conf-file "$nix_root/nix.conf"'));
  expect(rendered).not.toContain('trusted-users = *');
  expect(rendered).not.toContain('skipAddingSubstituter: true');
  expect(rendered).toContain('name: devenv\n          skipPush: true');
});

it('installs the locked direnv in a native-discoverable host profile before adoption', () => {
  const rendered = renderCowshedBootstrapReadinessJob();
  const install = rendered.indexOf('nix profile add --profile "$host_profile" "$direnv_store"');
  expect(rendered).toContain('host_profile="$HOME/.local/state/nix/profile"');
  expect(rendered).toContain('"$COWSHED_BUN_PROFILE_BIN/direnv"');
  expect(rendered).toContain('cmp "$direnv_bin" "$host_profile/bin/direnv"');
  expect(install).toBeGreaterThan(rendered.indexOf('name: Use the existing rooted Bun profile'));
  expect(install).toBeLessThan(rendered.indexOf('name: Install published Cowshed'));
});

it('checks adopted main after the native gateway health wait and before success', () => {
  const rendered = renderCowshedBootstrapReadinessJob();
  const doctor = rendered.indexOf('"$COWSHED_NATIVE" --project "$main_path" --json doctor');
  const health = rendered.indexOf(
    '"$COWSHED_NATIVE" --json gateway start | tee "$PROBE_ROOT/evidence/gateway-adopted.json"',
  );
  const success = rendered.indexOf("printf 'Published Cowshed setup, gateway health, and pinned main adoption passed.");
  expect(health).toBeGreaterThan(0);
  expect(doctor).toBeGreaterThan(health);
  expect(doctor).toBeLessThan(success);
  expect(rendered).not.toContain(
    '"$COWSHED_NATIVE" --json gateway status | tee "$PROBE_ROOT/evidence/gateway-adopted.json"',
  );
});

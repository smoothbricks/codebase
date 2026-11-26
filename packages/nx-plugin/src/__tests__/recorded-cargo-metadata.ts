import { readFileSync } from 'node:fs';
import { mkdir, realpath, writeFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { type CargoMetadata, parseCargoMetadata } from '@smoothbricks/build-inputs/cargo-closure';
import { type CargoResolve, cargoFromMetadata } from '../cargo-source-hash.js';

const FIXTURE_ROOT_PLACEHOLDER = '<FIXTURE_ROOT>';
const RECORDED_METADATA_TEMPLATE = readFileSync(new URL('./recorded-cargo-metadata.json', import.meta.url), 'utf8');

/**
 * The Cargo workspace fixture shared by digest and cache tests: `app` reaches `leaf` and `external` (outside the
 * workspace) through `bridge`; `unrelated` is a member nothing depends on.
 */
export const WORKSPACE_CARGO_FIXTURE_FILES: Readonly<Record<string, string>> = {
  'workspace/Cargo.toml':
    '[workspace]\nmembers=["packages/app","crates/bridge","crates/leaf","crates/unrelated"]\nresolver="2"\n',
  'workspace/packages/app/Cargo.toml':
    '[package]\nname="app"\nversion="0.1.0"\nedition="2021"\n[dependencies]\nbridge={path="../../crates/bridge"}\n',
  'workspace/packages/app/src/main.rs': 'fn main() { println!("{}", bridge::answer()); }\n',
  'workspace/crates/bridge/Cargo.toml':
    '[package]\nname="bridge"\nversion="0.1.0"\nedition="2021"\n[dependencies]\nleaf={path="../leaf"}\nexternal={path="../../../external"}\n',
  'workspace/crates/bridge/src/lib.rs': 'pub fn answer() -> u8 { leaf::answer() + external::answer() }\n',
  'workspace/crates/leaf/Cargo.toml': '[package]\nname="leaf"\nversion="0.1.0"\nedition="2021"\n',
  'workspace/crates/leaf/src/lib.rs': 'pub fn answer() -> u8 { 1 }\n',
  'workspace/crates/unrelated/Cargo.toml': '[package]\nname="unrelated"\nversion="0.1.0"\nedition="2021"\n',
  'workspace/crates/unrelated/src/lib.rs': 'pub fn unrelated() -> u8 { 3 }\n',
  'external/Cargo.toml': '[package]\nname="external"\nversion="0.1.0"\nedition="2021"\n[workspace]\n',
  'external/src/lib.rs': 'pub fn answer() -> u8 { 10 }\n',
};

/** Writes {@link WORKSPACE_CARGO_FIXTURE_FILES} under `root` without running Cargo. Returns canonical paths. */
export async function writeWorkspaceCargoFixtureFiles(root: string): Promise<{
  readonly root: string;
  readonly workspace: string;
  readonly manifest: string;
  readonly leafSource: string;
}> {
  await mkdir(root, { recursive: true });
  const canonical = await realpath(root);
  for (const [path, text] of Object.entries(WORKSPACE_CARGO_FIXTURE_FILES)) {
    const file = join(canonical, path);
    await mkdir(dirname(file), { recursive: true });
    await writeFile(file, text);
  }
  const workspace = join(canonical, 'workspace');
  return {
    root: canonical,
    workspace,
    manifest: join(workspace, 'Cargo.toml'),
    leafSource: join(workspace, 'crates/leaf/src/lib.rs'),
  };
}

/**
 * Loads the recorded `CargoMetadata` for {@link WORKSPACE_CARGO_FIXTURE_FILES}, bound to the canonical `root`.
 *
 * Captured once by running `cargo generate-lockfile --offline --manifest-path workspace/Cargo.toml`, then
 * `readCargoResolve` (which executes `cargo metadata --format-version 1 --locked --offline --manifest-path
 * workspace/Cargo.toml` and `cargo locate-project --workspace --manifest-path external/Cargo.toml --message-format
 * plain` for `external`'s `governing_manifest_path`), taking the cached `CargoMetadata` JSON written into
 * `entry.cache`, and replacing the canonical fixture root with `<FIXTURE_ROOT>`.
 */
export function loadRecordedCargoMetadata(canonicalRoot: string): CargoMetadata {
  return parseCargoMetadata(RECORDED_METADATA_TEMPLATE.replaceAll(FIXTURE_ROOT_PLACEHOLDER, canonicalRoot));
}

/** Recorded {@link CargoResolve} for {@link WORKSPACE_CARGO_FIXTURE_FILES} under `canonicalRoot`, with no Cargo process. */
export function loadRecordedCargoResolve(canonicalRoot: string): CargoResolve {
  return cargoFromMetadata(loadRecordedCargoMetadata(canonicalRoot));
}

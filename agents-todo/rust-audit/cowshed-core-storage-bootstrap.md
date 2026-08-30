# cowshed-core/storage/bootstrap

Scope: files read in full (line counts from the files themselves):

- `packages/cowshed/crates/cowshed-core/src/storage/bootstrap.rs` (1794)
- `packages/cowshed/crates/cowshed-core/src/storage/bootstrap/native.rs` (30)
- `packages/cowshed/crates/cowshed-core/src/storage/bootstrap/native/macos.rs` (6517)
- `packages/cowshed/crates/cowshed-core/src/storage/bootstrap/native/shared.rs` (365)
- `packages/cowshed/crates/cowshed-core/src/storage/bootstrap/native/linux.rs` (95) Targeted neighbour reads
  (duplication only, not audited): `device.rs:1-86`, `fstab.rs:1-46`, `process.rs:51-87`, `cowshed-core/Cargo.toml`.

## Summary

- CRITICAL: `AuthorizationExecuteWithPrivileges` always returns `CommandOutput::success`; privileged
  `security`/`install`/`launchctl` failures are invisible, including FileVault password persist-before-encrypt.
- HIGH SSOT: `valid_apfs_volume_identifier` restates `device::identifier_depth` and disagrees on leading zeros
  (`disk01s1`).
- HIGH STRUCTURE: `macos.rs` is 6517 lines (≈2971 tests + ≈3546 production). Linux is a 95-line fail-closed stub, not a
  missing port. ~200 production lines (unix marker/spawn, validation-action classifier) belong in `shared.rs`; the rest
  is genuinely macOS.
- HIGH DUPLICATION: `attest_created_apfs_info` and `attest_mounted_apfs_info` copy the same plist field/role checks.
- MEDIUM: fstab tag restated against `fstab.rs`; two `HostOperation` match tables; `HostAction` vs `HostOperation` dual
  plans; `security(1)` via AEWP while Security.framework is already linked.
- `cfg(not(target_os = "macos"))` arm: structurally compilable fail-closed stub. No cargo check (forbidden). [INFERENCE]
  from reading: no macOS-only types leak into `linux.rs`.
- FFI vs shell: keep Security.framework for the auth session, keep `libc` `statfs`/`openat`, keep the `plist` crate,
  keep `diskutil` as a CLI. Do not replace those with `plutil`/`diskutil` text scraping. The mistake is running
  `security(1)` _through_ AEWP.

## Findings

### F1 — CRITICAL — STRUCTURE — Privileged runner fabricates Exit(0)

Evidence: `packages/cowshed/crates/cowshed-core/src/storage/bootstrap/native/macos.rs:2859-2887`

```
        let status = unsafe {
            AuthorizationExecuteWithPrivileges(
                self.reference,
                program.as_ptr(),
                0,
                argument_pointers.as_mut_ptr(),
                &mut pipe,
            )
        };
        authorization_status("execute privileged command", status)?;
        // ...
        let stdout = read_authorized_output(pipe)?;
        Ok(HostCommandOutput::success(stdout))
```

`macos.rs:901-908` (`required_host_command`) and `macos.rs:2670-2679` (`run_privileged_command`) then treat
`output.succeeded()` as the child exit. `macos.rs:954-957` already documents the hole for `security` exit 44.
`macos.rs:970-1002` persists the FileVault passphrase with `required_host_command(... add-generic-password ...)`
_before_ `encryptVolume`.

Problem: AEWP's status is "tool launched", not waitpid. `HostCommandOutput::success` (`process.rs:59-64`) hard-codes
`ProcessStatus::Exit(0)`. Failed `security add-generic-password`, `install` of `/etc/fstab`, and `launchctl bootstrap`
are reported done. Encrypt-in-place of an existing store can then run with a passphrase that never landed in
System.keychain.

Fix: Stop using AEWP as a command runner that claims exit status. Either (a) a privileged helper that writes
`{stdout, stderr, wait_status}` on the pipe, or (b) in-process `SecItem*` for keychain (typed errors) and attest every
privileged mutation the way `provision_apfs_volumes_in_session` already attests mounts/owners/markers. Refuse to call
`encryptVolume` until a subsequent `find-generic-password -w` round-trip returns the same bytes.

Cost/Risk: Every `AuthorizedBootstrapHost::run_command` site (`macos.rs:202-203`:
diskutil/install/launchctl/rm/security). Tests that feed `HostCommandOutput::success` stay valid; add a case where
AEWP-shaped empty stdout on `add-generic-password` must _not_ reach `encryptVolume`.

### F2 — HIGH — SSOT — Volume-identifier grammar restated and already diverges

Evidence: `packages/cowshed/crates/cowshed-core/src/storage/bootstrap.rs:1680-1691`

```
fn valid_apfs_volume_identifier(identifier: &str) -> bool {
    let Some(rest) = identifier.strip_prefix("disk") else {
        return false;
    };
    let Some((disk, slice)) = rest.split_once('s') else {
        return false;
    };
    !disk.is_empty()
        && disk.bytes().all(|byte| byte.is_ascii_digit())
        && !slice.is_empty()
        && slice.bytes().all(|byte| byte.is_ascii_digit())
}
```

`macos.rs:1995-1999` uses the real grammar:

```
fn valid_volume_identifier(value: &[u8]) -> bool {
    str::from_utf8(value)
        .ok()
        .and_then(crate::device::identifier_depth)
        == Some(1)
}
```

`device.rs:1-32` is the stated SSOT and rejects leading zeros (`disk01`, `disk1s01`).
`valid_apfs_volume_identifier("disk01s1")` is true; `identifier_depth("disk01s1")` is `None`. Live disagreement.

Problem: `parse_created_apfs_identifier` (bootstrap.rs:1672) admits spellings the inventory parser (`macos.rs:2101`)
will later reject. Identity comparisons are textual (`device.rs:5-7`).

Fix: Delete `valid_apfs_volume_identifier`. Call `crate::device::identifier_depth(identifier) == Some(1)` from
`parse_created_apfs_identifier`.

Cost/Risk: One macos-only helper in `bootstrap.rs`. `device.rs` stays the SSOT (other slice).

### F3 — HIGH — STRUCTURE — macos.rs is a 6517-line god file; linux.rs is fail-closed, not unfinished

Evidence: `native.rs:8-11` (cfg split), `linux.rs:1-95` (every entrypoint `UnsupportedPlatform`), `macos.rs:3315-6286`
(`mod tests`), `macos.rs:6343-6517` (`mod unix`), `macos.rs:6288-6326` (`read_only_validation_actions`),
`shared.rs:1-365` (types + `execute_native_bootstrap_plan` only).

Line split of `macos.rs` (6517):

| Block                                                                                                                          | Lines              | macOS-specific?                                        |
| ------------------------------------------------------------------------------------------------------------------------------ | ------------------ | ------------------------------------------------------ |
| Host adapters, setup/uninstall, APFS evidence, inventory, provision, FileVault, launchd, Authorization FFI, reclaim heuristics | ≈1–3314 minus unix | Yes                                                    |
| `mod tests`                                                                                                                    | 3315–6286 (2971)   | Yes, keep next to adapter or split to `macos/tests.rs` |
| `read_only_validation_actions` + `require_host_canonical`                                                                      | 6288–6337          | No — policy sibling of `shared.rs:249`                 |
| `mod unix` (`spawn_with_deadline`, `write_marker_atomic`)                                                                      | 6343–6515          | Unix, not macOS                                        |

Problem: 6517 vs 95 is not "Linux needs the macOS adapter." Linux is an explicit fail-closed host (`linux.rs:1-2`). The
file is oversized because tests, unix I/O, and the validation-action classifier live inside the macOS adapter. Functions
over ~100 lines in the same file: `build_host_actions` (664–793), `execute_snapshot_actions` (1175–1275),
`execute_host_setup` (1310–1433), `gather_existing_apfs_evidence` (1778–1898), `classify_volume` (2173–2282),
`provision_apfs_volumes_in_session` (2436–2602), `write_marker_atomic` (6390–6482). `bootstrap.rs` `plan_apfs_volume`
(1018–1129) and `parse_cowshed_config` (104–210) are the same smell on the pure side.

FFI vs shell (this is the DEP-BLOAT slice; verdicts, not vibes):

- **Keep** Security.framework `AuthorizationCreate`/`CopyRights`/`Free`: one interactive session across
  mkdir/diskutil/install/launchctl; `sudo`/`osascript` lose the session and the denied-vs-canceled typing
  (`macos.rs:57-58`, `2949-2955`).
- **Keep** `libc::statfs` + `MNT_DONTBROWSE` (`macos.rs:3014-3042`): kernel mount identity, not diskutil text.
- **Keep** `open`/`openat`/`renameat`/`O_NOFOLLOW` (`macos.rs:3281-3312`, `6390-6482`): atomic no-follow marker publish;
  `mv` cannot.
- **Keep** `diskutil` as a CLI for `apfs list/addVolume/mount/unmount/encryptVolume/info`: there is no small public
  create-volume FFI that beats `/usr/sbin/diskutil`. Fork here is the platform API.
- **Keep** the `plist` crate for those `-plist` bytes (XML or binary). Do not add a `plutil` fork per inventory. The
  generated boot script (`macos.rs:406-432`) correctly uses `/usr/bin/plutil` because that script must run with no
  cowshed binary.
- **Do not** replace Authorization FFI with shelling `security`/`diskutil` as root via `osascript`.
- **Do** stop routing `security(1)` _through_ AEWP (F1/F7).

Fix: Move `mod unix` and `read_only_validation_actions` next to `mutating_setup_actions` in `shared.rs` (unix bits
`cfg(unix)`). Split `macos.rs` along the seams that already exist as types: inventory parse (`ApfsInventory`),
`MacAuthorizationSession`, provision (`provision_apfs_volumes_in_session`), setup orchestration, mount-service, evidence
gather. Leave `linux.rs` fail-closed until a real ZFS host adapter exists.

Cost/Risk: cfg surface only; no behaviour change if it is a move. Tests stay macos-only (`cfg(target_os = "macos")`).

### F4 — HIGH — DUPLICATION — Created vs mounted APFS attestation is one function with two mount rules

Evidence: `bootstrap.rs:1708-1768` (`attest_created_apfs_info`) and `macos.rs:2737-2781` (`attest_mounted_apfs_info`).
Both:

- `Value::from_reader` + require dictionary
- check `DeviceIdentifier`, `APFSContainerReference`, `VolumeName`, `FilesystemType == "apfs"`
- require `APFSSnapshot == false`
- reject nonempty `APFSVolumeRole` / `APFSVolumeRoles` / `Roles`

Created additionally allows empty/`/Volumes/{name}` mount; mounted requires an exact mountpoint.

Problem: Two copies of the volume-identity predicate. A third diskutil key will be added to one arm.

Fix: One function in `bootstrap.rs` (already macos-cfg): shared field checks + a
`MountExpectation { UnmountedOrDefault, Exact(&Path) }` argument. `macos.rs` calls it; delete
`attest_mounted_apfs_info`.

Cost/Risk: Provision paths `macos.rs:2492` and `2550`/`2579`. Tests
`created_volume_attestation_admits_only_detached_or_the_default_mount` (5231) stay; add one mounted-arm call through the
same helper.

### F5 — MEDIUM — SSOT — fstab tag string is restated, not imported

Evidence: `macos.rs:1479`

```
            line.split_once("# cowshed created volume labelled")
                .map(|(_, label)| label.trim().to_owned())
```

Neighbour `packages/cowshed/crates/cowshed-core/src/storage/fstab.rs:4`:

```
const COWSHED_FSTAB_TAG: &str = "# cowshed created volume labelled";
```

Copies currently agree. Uninstall planning (`host_uninstall_plan_from_text`) will silently no-op if `build_fstab`
retags.

Fix: Export `COWSHED_FSTAB_TAG` from `fstab.rs` (that file is the writer). Uninstall parser uses the same constant.

Cost/Risk: `fstab.rs` is another slice; one `pub(crate)` and one call-site.

### F6 — MEDIUM — DUPLICATION — Two exhaustive HostOperation tables implement two policies

Evidence: `shared.rs:249-277` (`mutating_setup_actions`: ExistingOnly may remount/reclaim/ensure-dir) vs
`macos.rs:6288-6326` (`read_only_validation_actions`: anything except `GuardMountpoint` is setup-required).

Problem: Not the same predicate, but the same enum is matched in two places with duplicated format strings
(`"create APFS volumes {}"`, `"write volume marker {}"`, `"pin cowshed APFS volumes in /etc/fstab"`). A new
`HostOperation` variant must be edited twice or one arm stops compiling while the other silently treats it as
mutating/non-mutating.

Fix: One `fn classify_operation(op) -> OperationClass` in `shared.rs` (`ReadOnly` / `UnprivilegedRemount` /
`Mutating { action: String }`). Both policies filter the class. Keep the two policies; delete the second match.

Cost/Risk: `execute_native_bootstrap_plan` and `validate_existing_plan`. Tests that assert
`StorageSetupRequired { actions }` string contents must track the single formatter.

### F7 — MEDIUM — DEP-BLOAT — System.keychain is driven with `security(1)` through AEWP

Evidence: `macos.rs:48` `const SECURITY`, `macos.rs:202` (SECURITY is AEWP-routed), `macos.rs:923-937` / `1440-1455` /
`976-999` / `1535-1548` (find/add/delete-generic-password). The crate already
`#[link(name = "Security", kind = "framework")]` at `macos.rs:2985`.

Problem: Worst of both directions. FFI is paid for Authorization; keychain still forks `/usr/bin/security`, loses stderr
and exit status (F1), and treats empty stdout as missing (`macos.rs:954-957`). This is not a case for deleting
Security.framework, and not a case for more shelling.

Fix: `SecItemCopyMatching` / `SecItemAdd` / `SecItemDelete` against System.keychain inside the already-authorized
process (or a helper that returns OSStatus). Keep `security(1)` out of the privileged session. `Uuid::parse_str` stays —
that crate is earning its weight as RFC 4122 validation of APFS volume UUIDs (`macos.rs:378`, `968`). `plist` stays
(F3). `getrandom`+`zeroize` stay for the passphrase (`macos.rs:910-920`).

Cost/Risk: Keychain ACL `-T` list (`macos.rs:992-997`: security, APFSUserAgent, CSUserAgent) must be expressed as
SecAccess, not argv. Uninstall delete path too.

### F8 — MEDIUM — STRUCTURE — Dual HostAction and HostOperation plans for one setup

Evidence: `build_host_actions` (`macos.rs:664-793`) independently maps `ExistingStorage` → `HostAction`.
`plan_bootstrap` (`bootstrap.rs:897-942`) independently maps the same evidence → `HostOperation`.
`execute_snapshot_actions` (`macos.rs:1175-1260`) then reverse-maps actions back through `operations_for_volume`
(`macos.rs:852-885`), which clones plan operations, and `unreachable!`s if a non-volume action slips through
(`macos.rs:1246`). Empty reverse-map is `CowshedError::internal("setup action for {name} has no host operations")`
(`macos.rs:1254-1257`).

Problem: Two plans. Encryption exists only as `HostAction` (executed aside). Create/mount/repair exist in both and can
disagree. `host_setup_actions` is `snapshot.actions.clone()` (`macos.rs:795-796`).

Fix: Derive `HostAction` from `BootstrapPlan` + classified volumes (one direction), or make `HostAction` the executed
plan and drop the reverse-map. Do not keep both writers.

Cost/Risk: CLI doctor/setup rendering matches `HostAction` (other slices). The serde shape in `shared.rs:55-91` is the
wire contract; keep it, change only the producer.

### F9 — MEDIUM — STRUCTURE — unsafe FFI without SAFETY comments; Send on AuthorizationRef

Evidence: `macos.rs:2788` `unsafe impl Send for MacAuthorizationSession {}` (no invariant). `macos.rs:2793-2824`
`AuthorizationCreate`/`AuthorizationCopyRights` with no SAFETY. `macos.rs:2859-2880` AEWP + `fwrite`/`fflush`/`fclose`.
`macos.rs:120-121`, `227-228`, `1719-1720` `getuid`/`getgid` unmarked. Contrast `macos.rs:3022` / `3286` / `6403` which
do write SAFETY notes.

Problem: AuthorizationRef is a raw pointer; Send is a concurrency invariant (the Mutex comment at `macos.rs:150-152` is
nearby, not on the impl). AEWP pipe ownership is easy to double-fclose.

Fix: SAFETY comments stating: AuthorizationRef is used only under `Mutex` / blocking-lane exclusive `&mut`; pipe is
uniquely owned until `read_authorized_output`'s `Pipe` Drop; getuid/getgid are process-wide reads. Pin the Send impl to
that Mutex.

Cost/Risk: Comments plus, if AEWP stays, a private wrapper so fclose happens once.

### F10 — LOW — COPIES — Remount classifier allocates three Strings to compare argv

Evidence: `shared.rs:287-297`

```
    args.starts_with(&[
            "mount".to_owned(),
            "-nobrowse".to_owned(),
            "-mountPoint".to_owned(),
        ])
```

Regime: once per bootstrap/validate, not a hot loop (handbook §4.1). Still a closed-form compare.

Fix: `args.get(0).map(String::as_str) == Some("mount") && args.get(1)...` or
`matches!(args, [m, n, p, ..] if m == "mount" && n == "-nobrowse" && p == "-mountPoint")`.

Cost/Risk: One function. Behaviour unchanged.

### F11 — LOW — STRUCTURE — expect on mount argv; no-op host gate

Evidence: `macos.rs:85` and `macos.rs:189` `command.args().last().expect("mount argv carries a device")` after
`is_diskutil_mount` (`macos.rs:60-66`), which only checks program==DISKUTIL and args[0]=="mount". `macos.rs:3010-3012`
`ensure_supported_host() -> Ok(())` is called from the live host methods and does nothing.

Problem: `expect` is an invariant claim the predicate does not prove (args `["mount"]` uses `"mount"` as the device).
The no-op function is dead policy.

Fix: Require `args.len() >= 2` in `is_diskutil_mount`; return `HostError` if not. Delete `ensure_supported_host`.

Cost/Risk: Mount path only.

## Cross-slice questions

- `packages/cowshed/crates/cowshed-core/src/device.rs` owns `identifier_depth` / `container_of` / `DISKUTIL`. F2 depends
  on that remaining the SSOT.
- `packages/cowshed/crates/cowshed-core/src/storage/fstab.rs` owns `COWSHED_FSTAB_TAG`. F5 needs it exported; this slice
  should not duplicate the writer.
- `packages/cowshed/crates/cowshed-core/src/apfs.rs` also parses diskutil/hdiutil plists with the same `plist` crate.
  Not audited. If that slice invents a third identifier parser, F2 gets worse.
- `packages/cowshed/crates/cowshed-core/src/process.rs` `CommandOutput::success` is the type AEWP abuses (F1). Changing
  its meaning would be wrong; the bootstrap runner should stop calling it.
- CLI `HostAction` rendering (`cowshed-cli` runtime/setup_service) consumes F8's dual plan. Do not "fix" rendering here.

## Non-findings (checked, clean)

- **Linux cfg arm compiles structurally.** `native.rs:8-9,26-30` compiles `linux.rs` on `not(macos)`. `linux.rs`
  implements every `BootstrapHost` method and all six public entrypoints with `UnsupportedPlatform`. macOS-only items in
  `bootstrap.rs` (`plist::Value`, `parse_created_apfs_identifier`, `CreatedMountState`, `attest_created_apfs_info`) are
  `cfg(target_os = "macos")`. `device.rs` is pure string parsing. No `std::os::unix` on the linux path. [INFERENCE] not
  cargo-checked.
- **Hand-rolled `.cowshed.toml` parser** (`bootstrap.rs:104-248`) is the right size; a `toml` crate would be the
  git2-class mistake. Fail-closed on unknown keys/sections.
- **`plan_bootstrap` is pure.** No host I/O; `BootstrapHost` is not accepted by planning functions
  (`bootstrap.rs:1428-1429`).
- **Marker JSON** (`VolumeMarker`, `MARKER_VERSION`) is one type; `require_mounted_marker` is the guard.
- **STORE_ROOT / CACHES_ROOT / volume names** are defined once in `bootstrap.rs:24-36` and imported by macos.rs. CLI
  comments say not to restated them.
- **Allocations in this slice are once-per-setup.** `format!` for the launchd script, `HostCommand` argv `Vec<String>`,
  inventory parse — not a hot loop. Not findings under §4.1.
- **`plist` / `uuid` / `getrandom` / `zeroize` / `libc` / `tokio`** used by this slice earn their weight as judged in
  F3/F7. `notify`, `walkdir`, `arrow-*`, `rcgen`, `x509-parser`, `base64` are crate deps this slice does not touch.
- **Tests** in `macos.rs` mostly assert typed `ExistingStorage` / `HostOperation` / `ProvisionEvent` sequences.
  `setup_and_uninstall_report_json_is_frozen_camel_case` (5690) pins the wire contract with full JSON (can go red).
  Mount-service test also runs `plutil`/plist parse and `/bin/sh -n`. No benches.
- **`unreachable!` in `plan_zfs_mounted_dataset`** (`bootstrap.rs:1301-1312`) is after `validate_zfs_evidence` rejected
  those variants — invariant, not operational.
- **ZFS path in this slice** is plan-only on macOS (`verify_zfs_delegation` returns `platform_host_error`); linux fails
  closed. No fake ZFS success.

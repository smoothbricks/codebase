# Disk-image lifetime: detach ownership and reboot control

## Static finding

The attachment report distinguishes three devices. In the captured RAW fixture they are the physical image media
`/dev/disk9` (empty content hint), synthesized APFS container `/dev/disk10` (`Apple_APFS_Container`), and volume
`/dev/disk10s1` (`Apple_APFS_Volume`). They are not interchangeable identities.

`apfs::attached_apfs_volume` and `attached_apfs_volumes` previously returned the synthesized container as
`AttachedImage.whole_device` / `ReleaseView.whole_device`. Therefore normal release, failed verification cleanup and
never-mounted release sent `hdiutil detach -verbose /dev/disk10`. This was not a slice, but it was also not the image
driver's physical disk. The blank-formatting path already retained and detached the physical device from its initial
unformatted attach report.

Both projections now validate the volume's synthesized parent separately and select the unique physical whole media for
release. RAW's empty hint and retained images' GUID/Apple partition-map hints identify that media; absent or ambiguous
physical media is refused, never substituted with the container. The one `addVolume` fixture caller that actually needs
a synthesized container derives it from the volume device instead. Tests use nonadjacent disk numbers and shuffled
entities, reject a container-only report, and prove a never-mounted release issues exactly one detach of the physical
image disk.

**This fixes the identity error, not a measured kernel-leak attribution yet.** The wedged-host measurements also showed
one retained device during initial image creation, whose detach already used the physical disk. The wrong target alone
therefore cannot be asserted to explain every observed instance. The post-reboot A/B below is required.

## Apple's expectations and close ownership

- The installed `hdiutil(1)` manual, `detach`, says it detaches an image **and terminates any associated process**. With
  Disk Arbitration it asks DA to unmount and detach; without DA it uses the eject ioctl and may need an explicit unmount
  first. It describes `diskutil eject` as very similar, not an additional required second step.
- The installed `diskutil(8)` manual, `eject`, says volumes are unmounted first. Its `unmountDisk` contract recommends
  naming a whole disk. Our production `MacOsApfsExecutionHost::detach_attachment` unmounts a mounted volume first;
  `release_image` unmounts every mounted volume under a temporary raw pin, drops those pins, then releases each physical
  image once. A never-mounted image skips only the unnecessary unmount, not the detach.
- `SystemCommandRunner::pin_raw_device` opens a BSD raw-volume file, not an IOKit client connection. The owned
  `Fenced<File>` is close-on-exec and its drop waits for in-flight forks. `AttachedImage::release_pin` takes and drops
  the owned file before detach; formatting does the same, while temporary unmount pins drop on return. Failed checks
  drop their local pins through normal Rust ownership.
- Cowshed does **not** call `IOServiceOpen` anywhere in its disk layer. `DIDeviceIOUserClient` connections are opened by
  Apple's `diskimagesiod` helper; cowshed reads their `IOUserClientCreator` registry properties. Iterator entries,
  providers and child iterators are owned `Object` values, whose `Drop` calls `IOObjectRelease`; CF properties use
  `CFRelease`. These are registry references, not `io_connect_t` connections.
- Apple's [IOServiceClose contract](https://developer.apple.com/documentation/iokit/1514646-ioserviceclose) closes a
  connection created by `IOServiceOpen`. Its
  [IOKitUser implementation](https://github.com/apple-oss-distributions/IOKitUser/blob/main/IOKitLib.c) calls
  `io_service_close(connect)` followed by `IOObjectRelease(connect)`. Calling `IOServiceClose` on the registry handles
  cowshed enumerates would not close the helper's connection and would violate that ownership contract.

There is no missing cowshed-owned DIDevice client close to add in either mounted or never-mounted release. Whether the
private DiskImages2 helper closes its connection after ejecting a synthesized container is not provable from cowshed's
source or the public IOKit implementation. We neither claim it does nor kill helpers to make the measurement pass.

## Exact post-reboot run

From the existing `nx-error-cause-sol` shed, with other disk-image test lanes stopped:

```sh
python3 packages/cowshed/scripts/disk-image-control.py "$PWD/target/debug/cowshed" "$PWD/.cache"
```

This requires the already-built facade binary, not an installation or gateway restart. There are four consecutive
10-cycle legs on the **same** newly created 128 MiB APFS UDIF image:

1. Plain `hdiutil attach -nomount -plist` / `hdiutil detach <physical>`; no cowshed code runs in this leg.
2. The identical argv through `cowshed disk-command hdiutil -- ...`.
3. Facade `diskutil image attach --nobrowse --noMount --plist`, then facade `hdiutil detach <synthesized-container>`
   (the old target).
4. The same attach and detach commands, changing only the detach operand to the physical image disk (the new target).

The explicit diagnostic's plain leg is deliberately the only new non-cowshed image executor; it is never part of a
normal test gate. The first two legs isolate the facade, and the last two isolate the detach target without mixing image
format, attach program, mounted state or signal policy. The existing Rust `disk_image_lifetime` example then proves the
full production attach/verify/mount/unmount/release path, independently of these command controls.

The script prints and flushes exact argv, device selection, and `ioclasscount` for `AppleDiskImageDevice`,
`DIDeviceIOUserClient`, `AppleAPFSVolume`, and `IOMedia` before creation, after creation, and before/after every cycle.
Each leg records its baseline-to-final delta. Evidence stays in `.cache/disk-image-control-<uuid>/results.jsonl`, beside
the most recent attach plist. It has no subprocess timeout, no kill, no sudo and no daemon restart. It removes the image
only after successful release; an unknown attachment outcome retains the image and evidence. Do not interrupt an
in-flight image command. These are host-wide counters, so concurrent attachers invalidate attribution.

## Pre-reboot evidence (wedged host; not a success claim)

Counts below exclude creation and use no `read_kernel` observation while attached. All ten operations in each run
returned success, but retained class instances grew:

| Ten-cycle variant  | Image devices | DIDevice clients | APFS volumes | IOMedia   |
| ------------------ | ------------- | ---------------- | ------------ | --------- |
| Production release | 607 → 618     | 594 → 604        | 2231 → 2241  | 601 → 611 |
| Never mounted      | 619 → 629     | 605 → 615        | 2242 → 2252  | 612 → 622 |
| `diskutil eject`   | 631 → 641     | 616 → 626        | 2253 → 2263  | 623 → 633 |

The production interval included one additional external image. Earlier runs with registry observation enabled grew by
ten in all four classes for each variant, so disabling that observer did not remove the growth. Plain hdiutil image
creation then failed with its own `Operation timed out`; it supplied no valid control. No image command was killed by
this investigation. A healthy-host zero-leak proof and the old/new target A/B remain required after reboot.

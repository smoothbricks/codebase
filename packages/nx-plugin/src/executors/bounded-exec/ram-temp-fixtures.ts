/** One attached image as `ioreg -r -c AppleDiskImageDevice -l -w0` lists it. */
export function registryDevice(url: string, disk: string): string[] {
  return [
    '+-o AppleDiskImageDevice@5d6  <class AppleDiskImageDevice, id 0x100659c38, registered, matched, active>',
    `  |   "DiskImageURL" = "${url}"`,
    '  +-o IOBlockStorageDriver  <class IOBlockStorageDriver, id 0x100659c3a, registered>',
    '    +-o Apple Disk Image Media  <class IOMedia, id 0x100659c3b, registered>',
    `      |   "BSD Name" = "${disk}"`,
    '        +-o Untitled 1@1  <class IOMedia, id 0x10037ce47, registered>',
    `          |   "BSD Name" = "${disk}s1"`,
  ];
}

import { mkdir, rm } from 'node:fs/promises';
import { join } from 'node:path';
import { $ } from 'bun';

// Q3: fork-mid-write clone validity. While a writer churns inside a mounted image,
// clonefile the image repeatedly (alternating sync / no-sync), then attach each clone
// -nomount, fsck_apfs -q AND -n on the volume device, and confirm it mounts + reads.
// Answers: is crash-consistent live cloning reliably fsck-clean; does sync-before-clone matter;
// what should `cowshed new` require.

const ROOT = '/private/tmp/cowshed-fork-bench';
const mounted = new Set<string>();

async function attachSparse(image: string, mnt: string) {
  await $`hdiutil attach -quiet -nobrowse -owners on -mountpoint ${mnt} ${image}`.quiet();
  mounted.add(mnt);
}
async function detachMnt(mnt: string) {
  await $`hdiutil detach -quiet ${mnt}`.nothrow().quiet();
  mounted.delete(mnt);
}

// attach a clone WITHOUT mounting, return the APFS volume /dev node
async function attachNoMount(image: string): Promise<{ container: string; vol: string }> {
  const out = await $`hdiutil attach -nomount ${image}`.text();
  const devs = [...out.matchAll(/\/dev\/(disk\d+)(s\d+)?/g)].map((m) => m[0]);
  // find the APFS Volume device (last sN with Apple_APFS_Volume) — heuristic: last /dev/diskNsM
  const container = out.match(/\/dev\/disk\d+/)?.[0] ?? devs[0];
  // volume is typically the deepest sN; grab the last device with two "s" segments or last overall
  const vol = devs[devs.length - 1] ?? container;
  return { container, vol };
}

interface CloneCheck {
  idx: number;
  synced: boolean;
  fsckQ: string; // pass|fail
  fsckN: string;
  mountable: string;
  filesReadable: string;
  latestFilePresent: string;
}

async function run() {
  await rm(ROOT, { recursive: true, force: true });
  await mkdir(ROOT, { recursive: true });
  const base = join(ROOT, 'base.sparseimage');
  await $`hdiutil create -quiet -size 2g -type SPARSE -fs ${'APFS'} -volname ForkBench -nospotlight ${base}`.quiet();
  const mnt = join(ROOT, 'mnt');
  await mkdir(mnt, { recursive: true });
  await attachSparse(base, mnt);
  const ws = join(mnt, 'ws');
  await mkdir(ws, { recursive: true });

  // background writer: continuous small-file creation + rewrite of a marker file that records
  // the highest index written (so we can test whether a clone captured recent writes).
  const writer = Bun.spawn(
    [
      'bash',
      '-c',
      `i=0; while :; do
       echo $i > ${ws}/marker.txt
       printf 'data-%08d' $i > ${ws}/f-$((i % 500)).bin
       i=$((i+1));
     done`,
    ],
    { stdout: 'ignore', stderr: 'ignore' },
  );
  // one large streaming write in parallel
  const bigWriter = Bun.spawn(
    ['bash', '-c', `while :; do /bin/dd if=/dev/zero of=${ws}/big.bin bs=1048576 count=128 2>/dev/null; done`],
    { stdout: 'ignore', stderr: 'ignore' },
  );

  await Bun.sleep(400); // let churn build up

  const checks: CloneCheck[] = [];
  for (let idx = 0; idx < 10; idx++) {
    const synced = idx % 2 === 0;
    // read the mounted marker right before cloning, to compare against the clone
    let markerBefore = '';
    try {
      markerBefore = (await Bun.file(join(ws, 'marker.txt')).text()).trim();
    } catch {}
    if (synced) await $`sync`.quiet();
    const clone = join(ROOT, `clone-${idx}.sparseimage`);
    await $`/bin/cp -c ${base} ${clone}`.quiet();

    // fsck the clone without mounting
    let fsckQ = '?',
      fsckN = '?',
      mountable = '?',
      filesReadable = '?',
      latestFilePresent = '?';
    try {
      const { container, vol } = await attachNoMount(clone);
      const rvol = vol.replace('/dev/disk', '/dev/rdisk');
      const q = await $`fsck_apfs -q ${rvol}`.nothrow().quiet();
      fsckQ = q.exitCode === 0 ? 'pass' : `fail(${q.exitCode})`;
      const n = await $`fsck_apfs -n ${rvol}`.nothrow().quiet();
      fsckN = n.exitCode === 0 ? 'pass' : `fail(${n.exitCode})`;
      await $`hdiutil detach -quiet ${container}`.nothrow().quiet();

      // now actually mount it and check readability + whether marker survived
      const cmnt = join(ROOT, `cmnt-${idx}`);
      await mkdir(cmnt, { recursive: true });
      try {
        await $`hdiutil attach -quiet -nobrowse -owners on -mountpoint ${cmnt} ${clone}`.quiet();
        mounted.add(cmnt);
        mountable = 'yes';
        const cmarker = await Bun.file(join(cmnt, 'ws', 'marker.txt'))
          .text()
          .catch(() => '<none>');
        latestFilePresent = `clone=${cmarker.trim()} vs live-before=${markerBefore}`;
        // read a sampling of files
        const ls = await $`/bin/ls ${join(cmnt, 'ws')}`.nothrow().quiet();
        filesReadable = ls.exitCode === 0 ? 'yes' : 'no';
        await detachMnt(cmnt);
      } catch (e) {
        mountable = `no(${(e as any)?.exitCode ?? 'err'})`;
      }
    } catch (e) {
      fsckQ = 'attach-err';
    }
    checks.push({ idx, synced, fsckQ, fsckN, mountable, filesReadable, latestFilePresent });
    await rm(clone, { force: true });
    await Bun.sleep(120);
  }

  writer.kill();
  bigWriter.kill();
  await Bun.sleep(100);
  await detachMnt(mnt);

  console.log('=== Q3: fork-mid-write clone validity (SPARSE, writer churning) ===');
  console.table(
    checks.map((c) => ({
      idx: c.idx,
      synced: c.synced,
      'fsck -q': c.fsckQ,
      'fsck -n': c.fsckN,
      mountable: c.mountable,
      readable: c.filesReadable,
    })),
  );
  console.log('\nMarker survival (did the clone capture the most-recent live write?):');
  for (const c of checks) console.log(`  #${c.idx} synced=${c.synced}: ${c.latestFilePresent}`);

  const allFsckPass = checks.every((c) => c.fsckQ === 'pass' && c.fsckN === 'pass');
  const allMountable = checks.every((c) => c.mountable === 'yes');
  console.log(`\nVERDICT: fsck-clean(all)=${allFsckPass}  mountable(all)=${allMountable}`);
}

try {
  await run();
} finally {
  for (const m of [...mounted].reverse()) await $`hdiutil detach -quiet -force ${m}`.nothrow().quiet();
  await rm(ROOT, { recursive: true, force: true });
  console.log('cleaned up', ROOT);
}

import { mkdir, rm } from 'node:fs/promises';
import { join } from 'node:path';
import { $ } from 'bun';
import { Bench } from 'tinybench';

// Q1 + Q2 + Q4: ASIF vs SPARSE substrate comparison, attach-flag floor, tmutil default.
// Focused harness; reuses tinybench. All under /private/tmp, cleaned up in finally.

const ROOT = '/private/tmp/cowshed-fmt-bench';
const SIZE = '4g';
const FILES = 2000;
const FILE_SIZE = 256;
const IO_MIB = 64;
const META = 2000;
const ITERS = 8;

const mounted = new Set<string>();
// ASIF must use `diskutil image attach` (hdiutil rejects the format); SPARSE uses hdiutil.
// Extension dispatch mirrors how cowshed-core will branch on the marker's imageFormat.
async function attachOnce(image: string, mnt: string, extra: string[]) {
  if (image.endsWith('.asif')) {
    await $`diskutil image attach --nobrowse --mountPoint ${mnt} ${image}`.quiet();
  } else if (extra.length) {
    await $`hdiutil attach -quiet -nobrowse -owners on -mountpoint ${mnt} ${extra} ${image}`.quiet();
  } else {
    await $`hdiutil attach -quiet -nobrowse -owners on -mountpoint ${mnt} ${image}`.quiet();
  }
}
async function attach(image: string, mnt: string, extra: string[] = []) {
  let lastErr: unknown;
  for (let tries = 0; tries < 3; tries++) {
    try {
      await attachOnce(image, mnt, extra);
      mounted.add(mnt);
      return;
    } catch (e) {
      lastErr = e;
      await Bun.sleep(150);
    }
  }
  throw lastErr;
}
async function detach(mnt: string) {
  for (let tries = 0; tries < 3; tries++) {
    try {
      await $`hdiutil detach -quiet ${mnt}`.quiet();
      mounted.delete(mnt);
      return;
    } catch {
      await Bun.sleep(150);
    }
  }
  await $`hdiutil detach -quiet -force ${mnt}`.nothrow().quiet();
  mounted.delete(mnt);
}
async function statBlocks(p: string): Promise<string> {
  return (await $`/usr/bin/stat -f ${'size=%z blocks=%b'} ${p}`.text()).trim();
}

async function createImage(format: 'SPARSE' | 'ASIF', path: string) {
  if (format === 'ASIF') {
    await $`diskutil image create blank --format ASIF --size ${SIZE} --volumeName CowBench --fs APFS ${path}`.quiet();
  } else {
    await $`hdiutil create -quiet -size ${SIZE} -type SPARSE -fs ${'APFS'} -volname CowBench -nospotlight ${path}`.quiet();
  }
}

async function populate(mnt: string) {
  const ws = join(mnt, 'workspace');
  await mkdir(ws, { recursive: true });
  const payload = Buffer.alloc(FILE_SIZE, 0x61);
  const paths: string[] = [];
  for (let i = 0; i < FILES; i++) {
    const dir = join(ws, `g-${Math.floor(i / 200)}`);
    paths.push(join(dir, `f-${i}.o`));
  }
  const dirs = [...new Set(paths.map((p) => p.slice(0, p.lastIndexOf('/'))))];
  await Promise.all(dirs.map((d) => mkdir(d, { recursive: true })));
  for (let o = 0; o < paths.length; o += 256) {
    await Promise.all(paths.slice(o, o + 256).map((p) => Bun.write(p, payload)));
  }
  await $`/bin/dd if=/dev/zero of=${join(ws, 'payload.bin')} bs=1048576 count=${IO_MIB} oflag=direct,fsync`.quiet();
}

function med(xs: number[]) {
  const s = [...xs].sort((a, b) => a - b);
  return s[Math.floor(s.length / 2)];
}
async function sample<T>(n: number, fn: () => Promise<T>): Promise<number[]> {
  const out: number[] = [];
  for (let i = 0; i < n; i++) {
    const t = Bun.nanoseconds();
    try {
      await fn();
    } catch {
      await Bun.sleep(50);
      await fn(); // one retry: transient EBADF right after (re)attach
    }
    out.push((Bun.nanoseconds() - t) / 1e6);
  }
  return out;
}

interface FmtResult {
  format: string;
  createMs: number;
  baseBlocks: string;
  cloneMs: number;
  attachFreshMs: number;
  attachWarmMs: number;
  readMs: number;
  writeMs: number;
  metaCreateMs: number;
  metaDeleteMs: number;
  grownBlocks: string;
}

async function benchFormat(format: 'SPARSE' | 'ASIF'): Promise<FmtResult> {
  const dir = join(ROOT, format.toLowerCase());
  await mkdir(dir, { recursive: true });
  const ext = format === 'ASIF' ? 'asif' : 'sparseimage'; // extension is load-bearing: wrong ext → RO attach
  const base = join(dir, `base.${ext}`);
  const tmpl = join(dir, 'mnt-template');
  await mkdir(tmpl, { recursive: true });

  const createMs = med(
    await sample(5, async () => {
      const p = join(dir, `c-${Bun.nanoseconds()}.img`);
      await createImage(format, p);
      await rm(p, { force: true });
    }),
  );

  await createImage(format, base);
  await attach(base, tmpl);
  await populate(tmpl);
  await detach(tmpl);
  const baseBlocks = await statBlocks(base);

  // clonefile of the base image
  const cloneMs = med(
    await sample(ITERS, async () => {
      const c = join(dir, `clone-${Bun.nanoseconds()}.img`);
      await $`/bin/cp -c ${base} ${c}`.quiet();
      await rm(c, { force: true });
    }),
  );

  // fresh attach: clone then attach then detach then delete
  const freshMnt = join(dir, 'mnt-fresh');
  await mkdir(freshMnt, { recursive: true });
  const attachFreshMs = med(
    await sample(ITERS, async () => {
      const c = join(dir, `fa-${Bun.nanoseconds()}.${ext}`);
      await $`/bin/cp -c ${base} ${c}`.quiet();
      await attach(c, freshMnt);
      await detach(freshMnt);
      await rm(c, { force: true });
    }),
  );

  // warm reattach: one persistent clone, attach/detach repeatedly
  const warmClone = join(dir, `warm.${ext}`);
  await $`/bin/cp -c ${base} ${warmClone}`.quiet();
  const warmMnt = join(dir, 'mnt-warm');
  await mkdir(warmMnt, { recursive: true });
  const attachWarmMs = med(
    await sample(ITERS, async () => {
      await attach(warmClone, warmMnt);
      await detach(warmMnt);
    }),
  );

  // mounted ops on the warm clone
  await attach(warmClone, warmMnt);
  const ws = join(warmMnt, 'workspace');
  const readMs = med(
    await sample(ITERS, async () => {
      await $`/bin/dd if=${join(ws, 'payload.bin')} of=/dev/null bs=8388608 iflag=direct`.quiet();
    }),
  );
  const writeMs = med(
    await sample(ITERS, async () => {
      const w = join(ws, 'w.bin');
      await $`/bin/dd if=/dev/zero of=${w} bs=1048576 count=${IO_MIB} oflag=direct,fsync`.quiet();
      await rm(w, { force: true });
    }),
  );
  const metaDir = join(ws, 'meta');
  const mkMeta = async () => {
    const payload = Buffer.from('m\n');
    const ps: string[] = [];
    for (let i = 0; i < META; i++) ps.push(join(metaDir, `d-${Math.floor(i / 200)}`, `f-${i}`));
    const ds = [...new Set(ps.map((p) => p.slice(0, p.lastIndexOf('/'))))];
    await Promise.all(ds.map((d) => mkdir(d, { recursive: true })));
    for (let o = 0; o < ps.length; o += 256) await Promise.all(ps.slice(o, o + 256).map((p) => Bun.write(p, payload)));
  };
  const metaCreateMs = med(
    await sample(ITERS, async () => {
      await mkMeta();
      await $`/bin/rm -rf ${metaDir}`.quiet();
    }),
  );
  await mkMeta();
  const metaDeleteMs = med(
    await sample(ITERS, async () => {
      await $`/bin/rm -rf ${metaDir}`.quiet();
      await mkMeta();
    }),
  );
  await $`/bin/rm -rf ${metaDir}`.quiet();
  const grownBlocks = await statBlocks(warmClone);
  await detach(warmMnt);

  return {
    format,
    createMs,
    baseBlocks,
    cloneMs,
    attachFreshMs,
    attachWarmMs,
    readMs,
    writeMs,
    metaCreateMs,
    metaDeleteMs,
    grownBlocks,
  };
}

async function attachFlagFloor(format: 'SPARSE' | 'ASIF') {
  const dir = join(ROOT, `floor-${format.toLowerCase()}`);
  await mkdir(dir, { recursive: true });
  const ext = format === 'ASIF' ? 'asif' : 'sparseimage';
  const base = join(dir, `base.${ext}`);
  await createImage(format, base);
  const mnt = join(dir, 'mnt');
  await mkdir(mnt, { recursive: true });
  const out: Record<string, number> = {};

  if (format === 'SPARSE') {
    const variants: Record<string, string[]> = {
      plain: [],
      noverify: ['-noverify'],
      'noverify+noautofsck': ['-noverify', '-noautofsck'],
    };
    for (const [name, flags] of Object.entries(variants)) {
      out[name] = med(
        await sample(10, async () => {
          await attach(base, mnt, flags);
          await detach(mnt);
        }),
      );
    }
  } else {
    // ASIF: only diskutil image attach; measure plain and --noverify if accepted
    out['diskutil plain'] = med(
      await sample(10, async () => {
        await $`diskutil image attach --nobrowse --mountPoint ${mnt} --mountOptions owners ${base}`.quiet();
        await detach(mnt);
      }),
    );
    const hasNoverify =
      (await $`diskutil image attach`.text().catch((e) => e.stdout?.toString() ?? '')).includes('noverify') ||
      (await $`diskutil image attach`.text().catch((e) => e.stdout?.toString() ?? '')).includes('noVerify');
    if (hasNoverify) {
      out['diskutil noverify'] = med(
        await sample(10, async () => {
          await $`diskutil image attach --nobrowse --noverify --mountPoint ${mnt} --mountOptions owners ${base}`.quiet();
          await detach(mnt);
        }).catch(() => [-1]),
      );
    }
  }
  return { format, ...out };
}

async function tmutilProbe() {
  const dir = join(ROOT, 'tm');
  await mkdir(dir, { recursive: true });
  const base = join(dir, 'tm.asif');
  await createImage('ASIF', base);
  const mnt = join(dir, 'mnt');
  await mkdir(mnt, { recursive: true });
  await attach(base, mnt);
  const volExcluded = (await $`tmutil isexcluded ${mnt}`.text().catch((e) => e.stdout?.toString() ?? 'ERR')).trim();
  const plainFile = join(dir, 'plain.txt');
  await Bun.write(plainFile, 'x');
  const fileExcluded = (
    await $`tmutil isexcluded ${plainFile}`.text().catch((e) => e.stdout?.toString() ?? 'ERR')
  ).trim();
  const imageFileExcluded = (
    await $`tmutil isexcluded ${base}`.text().catch((e) => e.stdout?.toString() ?? 'ERR')
  ).trim();
  await detach(mnt);
  return { mountpoint: volExcluded, plainFileInTmp: fileExcluded, imageFileInTmp: imageFileExcluded };
}

await rm(ROOT, { recursive: true, force: true });
await mkdir(ROOT, { recursive: true });
try {
  console.log('=== Q1: format comparison (medians, ms) ===');
  const sparse = await benchFormat('SPARSE');
  const asif = await benchFormat('ASIF');
  console.table(
    [sparse, asif].map((r) => ({
      format: r.format,
      create: r.createMs.toFixed(1),
      clone: r.cloneMs.toFixed(2),
      'attach-fresh': r.attachFreshMs.toFixed(1),
      'attach-warm': r.attachWarmMs.toFixed(1),
      read: r.readMs.toFixed(1),
      write: r.writeMs.toFixed(1),
      'meta-create': r.metaCreateMs.toFixed(1),
      'meta-delete': r.metaDeleteMs.toFixed(1),
    })),
  );
  console.log('base image size after populate:');
  console.log('  SPARSE:', sparse.baseBlocks, '| grown clone:', sparse.grownBlocks);
  console.log('  ASIF:  ', asif.baseBlocks, '| grown clone:', asif.grownBlocks);

  console.log('\n=== Q2: attach-flag floor (medians, ms) ===');
  const floorAsif = await attachFlagFloor('ASIF');
  const floorSparse = await attachFlagFloor('SPARSE');
  console.table([floorAsif, floorSparse]);

  console.log('\n=== Q4: tmutil defaults ===');
  console.log(await tmutilProbe());
} finally {
  for (const m of [...mounted].reverse()) await $`hdiutil detach -quiet ${m}`.nothrow().quiet();
  await rm(ROOT, { recursive: true, force: true });
  console.log('\ncleaned up', ROOT);
}

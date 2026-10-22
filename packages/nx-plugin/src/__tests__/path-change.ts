import { dlopen, FFIType, type Pointer, read } from 'bun:ffi';
import { spawn } from 'node:child_process';
import { writeSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { getSystemErrorName } from 'node:util';

/** This module run as a program is the waker `whenChanged` spawns. */
const WAKER = fileURLToPath(import.meta.url);
/** The waker's exit status when it could not arm its watch; its stderr is then one `<syscall> <errno>` line. */
const ARM_FAILED = 3;
const ARM_SYSCALLS = ['open', 'kqueue', 'kevent', 'inotify_init1', 'inotify_add_watch'] as const;
type ArmSyscall = (typeof ARM_SYSCALLS)[number];
const ARM_FAILURE = new RegExp(`^(${ARM_SYSCALLS.join('|')}) (\\d+)\\n$`);
const WAKE = new Uint8Array([10]);
const EINTR = 4;

/** The waker could not ask the kernel to watch `path`: the wait would have had nothing to wake it. */
export class WakerArmError extends Error {
  readonly path: string;
  readonly syscall: ArmSyscall;
  readonly errno: number;

  constructor(path: string, syscall: ArmSyscall, errno: number) {
    super(`cannot watch ${path}: ${syscall} failed with ${getSystemErrorName(-errno)} (errno ${errno})`);
    this.name = 'WakerArmError';
    this.path = path;
    this.syscall = syscall;
    this.errno = errno;
  }
}

/**
 * Resolves once `ready` holds. `ready` is judged whole once the kernel watches `path` and again after every change it
 * reports there, so a change made before the watch exists is still seen and one made while `ready` reads still brings
 * another judgment. For a directory a change is an entry added, removed or renamed; for a file, a write to it.
 *
 * The watch is a waker process holding a kqueue EVFILT_VNODE watch on darwin or an inotify watch on linux, which
 * prints one byte once armed and one after every event. Bun's fs.watch is not used: on darwin it is one FSEvents
 * stream per process, started since-now after the call returns and restarted whenever a watcher is added or removed,
 * and it lost 3 of 200 single creates in a watched directory. Neither is `tail -F`: GNU tail polls once a second on
 * darwin, and no tail follows a directory.
 *
 * The wait ends on a kernel event, never on a timer; one that never comes is the leg's no-progress bound to report. It
 * fails with `WakerArmError` when the watch cannot be armed, and with the waker's stderr when the waker ends first. The
 * waker is killed and its close awaited before this returns, so none outlives the wait.
 */
export async function whenChanged(path: string, ready: () => boolean): Promise<void> {
  const waker = spawn(process.execPath, [WAKER, path], { stdio: ['ignore', 'pipe', 'pipe'] });
  const closed = Promise.withResolvers<void>();
  const { promise, resolve, reject } = Promise.withResolvers<void>();
  let stderr = '';
  waker.stderr.setEncoding('utf8').on('data', (text: string) => {
    stderr += text;
  });
  waker.stdout.on('data', () => {
    try {
      if (ready()) resolve();
    } catch (error) {
      reject(error);
    }
  });
  waker.once('error', (error) => {
    closed.resolve();
    reject(error);
  });
  waker.once('close', (code, signal) => {
    closed.resolve();
    const unarmed = code === ARM_FAILED ? ARM_FAILURE.exec(stderr) : null;
    const syscall = ARM_SYSCALLS.find((name) => name === unarmed?.[1]);
    reject(
      syscall === undefined
        ? new Error(`the waker on ${path} ended (${code ?? signal}) before the wait was over: ${stderr}`)
        : new WakerArmError(path, syscall, Number(unarmed?.[2])),
    );
  });
  try {
    await promise;
  } finally {
    waker.kill();
    await closed.promise;
  }
}

function armFailed(syscall: ArmSyscall, errno: number): never {
  writeSync(2, `${syscall} ${errno}\n`);
  process.exit(ARM_FAILED);
}

/** A failure after arming: said on stderr as one line, which `whenChanged` reports with the waker's exit. */
function fail(message: string): never {
  writeSync(2, `${message}\n`);
  process.exit(1);
}

function errnoAt(location: Pointer | bigint | null): number {
  // bun:ffi hands back a bigint only for an address past 2^53, which no user-space errno location has.
  if (location === null || typeof location === 'bigint') throw new Error(`libc returned errno location ${location}`);
  return read.i32(location, 0);
}

/** Darwin: kqueue EVFILT_VNODE on the path, opened event-only so it never holds its volume busy. */
function wakeOnKqueue(path: string): never {
  const O_EVTONLY = 0x8000;
  const O_CLOEXEC = 0x1000000;
  const EVFILT_VNODE = -4;
  const EV_ADD = 0x1;
  const EV_CLEAR = 0x20;
  const NOTE_DELETE = 0x1;
  const NOTE_WRITE = 0x2;
  const NOTE_EXTEND = 0x4;
  const NOTE_RENAME = 0x20;
  const NOTE_REVOKE = 0x40;
  const GONE = NOTE_DELETE | NOTE_RENAME | NOTE_REVOKE;
  const { symbols: libc } = dlopen('/usr/lib/libSystem.B.dylib', {
    open: { args: [FFIType.ptr, FFIType.i32], returns: FFIType.i32 },
    kqueue: { args: [], returns: FFIType.i32 },
    kevent: {
      args: [FFIType.i32, FFIType.ptr, FFIType.i32, FFIType.ptr, FFIType.i32, FFIType.ptr],
      returns: FFIType.i32,
    },
    write: { args: [FFIType.i32, FFIType.ptr, FFIType.u64], returns: FFIType.i64 },
    __error: { args: [], returns: FFIType.ptr },
  });
  const errno = (): number => errnoAt(libc.__error());
  const fd = libc.open(Buffer.from(`${path}\0`), O_EVTONLY | O_CLOEXEC);
  if (fd < 0) armFailed('open', errno());
  const kq = libc.kqueue();
  if (kq < 0) armFailed('kqueue', errno());
  // struct kevent: ident u64, filter i16, flags u16, fflags u32, data i64, udata ptr.
  const change = new DataView(new ArrayBuffer(32));
  change.setBigUint64(0, BigInt(fd), true);
  change.setInt16(8, EVFILT_VNODE, true);
  change.setUint16(10, EV_ADD | EV_CLEAR, true);
  change.setUint32(12, NOTE_WRITE | NOTE_EXTEND | GONE, true);
  if (libc.kevent(kq, new Uint8Array(change.buffer), 1, null, 0, null) < 0) armFailed('kevent', errno());
  const event = new Uint8Array(32);
  const fflags = new DataView(event.buffer);
  for (;;) {
    if (libc.write(1, WAKE, 1) !== 1n) fail(`cannot wake the wait on ${path}: write errno ${errno()}`);
    let count = libc.kevent(kq, null, 0, event, 1, null);
    while (count < 0) {
      const failed = errno();
      if (failed !== EINTR) fail(`kevent on ${path} failed with errno ${failed}`);
      count = libc.kevent(kq, null, 0, event, 1, null);
    }
    if ((fflags.getUint32(12, true) & GONE) !== 0) {
      libc.write(1, WAKE, 1);
      fail(`${path} was deleted, renamed or revoked`);
    }
  }
}

/** Linux: an inotify watch on the path. */
function wakeOnInotify(path: string): never {
  const IN_CLOEXEC = 0o2000000;
  const IN_MODIFY = 0x2;
  const IN_MOVED_FROM = 0x40;
  const IN_MOVED_TO = 0x80;
  const IN_CREATE = 0x100;
  const IN_DELETE = 0x200;
  const IN_DELETE_SELF = 0x400;
  const IN_MOVE_SELF = 0x800;
  const IN_IGNORED = 0x8000;
  const GONE = IN_DELETE_SELF | IN_MOVE_SELF | IN_IGNORED;
  const { symbols: libc } = dlopen('libc.so.6', {
    inotify_init1: { args: [FFIType.i32], returns: FFIType.i32 },
    inotify_add_watch: { args: [FFIType.i32, FFIType.ptr, FFIType.u32], returns: FFIType.i32 },
    read: { args: [FFIType.i32, FFIType.ptr, FFIType.u64], returns: FFIType.i64 },
    write: { args: [FFIType.i32, FFIType.ptr, FFIType.u64], returns: FFIType.i64 },
    __errno_location: { args: [], returns: FFIType.ptr },
  });
  const errno = (): number => errnoAt(libc.__errno_location());
  const fd = libc.inotify_init1(IN_CLOEXEC);
  if (fd < 0) armFailed('inotify_init1', errno());
  const mask = IN_MODIFY | IN_MOVED_FROM | IN_MOVED_TO | IN_CREATE | IN_DELETE | IN_DELETE_SELF | IN_MOVE_SELF;
  if (libc.inotify_add_watch(fd, Buffer.from(`${path}\0`), mask) < 0) armFailed('inotify_add_watch', errno());
  // Room for at least one struct inotify_event (wd i32, mask u32, cookie u32, len u32) with a NAME_MAX name.
  const events = new Uint8Array(4096);
  const fields = new DataView(events.buffer);
  for (;;) {
    if (libc.write(1, WAKE, 1) !== 1n) fail(`cannot wake the wait on ${path}: write errno ${errno()}`);
    let length = libc.read(fd, events, events.length);
    while (length < 0n) {
      const failed = errno();
      if (failed !== EINTR) fail(`reading inotify for ${path} failed with errno ${failed}`);
      length = libc.read(fd, events, events.length);
    }
    for (let offset = 0; offset < Number(length); offset += 16 + fields.getUint32(offset + 12, true)) {
      if ((fields.getUint32(offset + 4, true) & GONE) !== 0) {
        libc.write(1, WAKE, 1);
        fail(`${path} was deleted or moved`);
      }
    }
  }
}

if (import.meta.main) {
  const [path] = process.argv.slice(2);
  if (path === undefined) throw new Error(`usage: ${process.execPath} ${WAKER} <path>`);
  if (process.platform === 'darwin') wakeOnKqueue(path);
  if (process.platform === 'linux') wakeOnInotify(path);
  throw new Error(`no kernel waker for ${process.platform}`);
}

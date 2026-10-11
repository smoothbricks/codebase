import { lstat } from 'node:fs/promises';
import { isRecord } from '../../type-guards.js';

/**
 * A dead RAM lease kept because something is attached from below it stays kept until its owner
 * detaches it, and every acquisition visits it. Proving it is still held from the host's full
 * attachment inventory (`ioreg` and `mount`) costs seconds a pass; this module proves it again
 * from a no-follow `lstat` of each mount the last inventory named.
 *
 * A verdict is only ever positive: it records holders that were there, never their absence, so
 * nothing read from it can remove a lease. A mount is keyed by its root's dev, inode and birth,
 * on a device other than the RAM volume's: files coming and going in it move only its ctime, and
 * an unmount exposes the RAM directory beneath. An attached image has no such key: a whole-disk
 * node's identity is not measured to change when DiskImages reuses its number for another image,
 * so an image holder leaves nothing to cache and its lease keeps the fresh inventory's result.
 * Any absence or change sends the caller back to a fresh inventory. This module reads only; the
 * volume's owner stores verdicts.
 */

/** Which volume incarnation and lease a verdict is about; keyed by the volume's owner. */
export interface HeldContext {
  /** The mounted RAM root's and marker's stable identity: no root ctime or mtime. */
  readonly volumeKey: string;
  /** The lease directory's identity. */
  readonly leaseKey: string;
  /** The mounted RAM root's `st_dev`, in decimal. */
  readonly ramDevice: string;
}

/** A holder the inventory named below a lease, with the text that names it to the task. */
export type HeldCandidate =
  | { readonly kind: 'image'; readonly imagePath: string; readonly diskPath: string; readonly description: string }
  | { readonly kind: 'mount'; readonly mountpoint: string; readonly description: string };

/**
 * A mount as it was keyed: the identity of the mounted root, which stays the same for as long as
 * the same volume is mounted there. Decimal strings: exact on the wire.
 */
export interface HeldProof {
  readonly kind: 'mount';
  readonly mountpoint: string;
  readonly description: string;
  readonly root: { readonly dev: string; readonly ino: string; readonly birthtimeNs: string };
}

/** A lease proven held by every one of its holders, as JSON carries it. */
export interface HeldVerdict {
  readonly version: 1;
  readonly context: HeldContext;
  readonly holders: readonly [HeldProof, ...HeldProof[]];
}

export type CacheResult<T> = { ok: true; value: T } | { ok: false; error: string };

/** The no-follow metadata a proof reads: `lstat(path, { bigint: true })` on the host. */
export interface HolderStats {
  readonly dev: bigint;
  readonly ino: bigint;
  readonly birthtimeNs: bigint;
  isDirectory(): boolean;
}

/** Proving held leases over `read`; a seam so mounts are testable on any host. */
export interface HeldProver {
  capture(context: HeldContext, candidates: readonly HeldCandidate[]): Promise<CacheResult<HeldVerdict | undefined>>;
  validate(verdict: HeldVerdict, context: HeldContext): Promise<CacheResult<boolean>>;
}

/** A holder read once: keyed, without a key (gone, unmounted, or an image), or unreadable. */
type Keyed = { kind: 'keyed'; proof: HeldProof } | { kind: 'unkeyed' } | { kind: 'failed'; error: string };

export function heldProverOn(read: (path: string) => Promise<HolderStats>): HeldProver {
  const key = async (candidate: HeldCandidate, ramDevice: string): Promise<Keyed> => {
    if (candidate.kind === 'image') {
      return { kind: 'unkeyed' };
    }
    const { kind, mountpoint, description } = candidate;
    let root: HolderStats;
    try {
      root = await read(mountpoint);
    } catch (error) {
      const code = typeof error === 'object' && error !== null && 'code' in error ? error.code : undefined;
      return code === 'ENOENT' || code === 'ENOTDIR'
        ? { kind: 'unkeyed' }
        : { kind: 'failed', error: `lstat ${mountpoint}: ${error instanceof Error ? error.message : String(error)}` };
    }
    if (!root.isDirectory() || root.dev.toString() === ramDevice) {
      return { kind: 'unkeyed' };
    }
    return {
      kind: 'keyed',
      proof: {
        kind,
        mountpoint,
        description,
        root: { dev: root.dev.toString(), ino: root.ino.toString(), birthtimeNs: root.birthtimeNs.toString() },
      },
    };
  };

  /** Every candidate keyed, or why not: the first unreadable one, else `undefined` for no proof. */
  const keyAll = async (
    candidates: readonly HeldCandidate[],
    ramDevice: string,
  ): Promise<CacheResult<readonly [HeldProof, ...HeldProof[]] | undefined>> => {
    const keyed = await Promise.all(candidates.map((candidate) => key(candidate, ramDevice)));
    const proofs: HeldProof[] = [];
    for (const holder of keyed) {
      if (holder.kind === 'failed') {
        return { ok: false, error: holder.error };
      }
      if (holder.kind === 'keyed') {
        proofs.push(holder.proof);
      }
    }
    return { ok: true, value: proofs.length === keyed.length && nonEmpty(proofs) ? proofs : undefined };
  };

  return {
    async capture(context, candidates) {
      const proofs = await keyAll(candidates, context.ramDevice);
      if (!proofs.ok) {
        return proofs;
      }
      const { volumeKey, leaseKey, ramDevice } = context;
      return {
        ok: true,
        value: proofs.value && { version: 1, context: { volumeKey, leaseKey, ramDevice }, holders: proofs.value },
      };
    },

    async validate(verdict, context) {
      const was = verdict.context;
      const sameLease =
        was.volumeKey === context.volumeKey && was.leaseKey === context.leaseKey && was.ramDevice === context.ramDevice;
      if (!sameLease) {
        return { ok: true, value: false };
      }
      const proofs = await keyAll(verdict.holders, context.ramDevice);
      if (!proofs.ok) {
        return proofs;
      }
      const now = proofs.value;
      return {
        ok: true,
        value:
          now !== undefined &&
          verdict.holders.every(({ mountpoint, root }, index) => {
            const again = now[index];
            return (
              again !== undefined &&
              again.mountpoint === mountpoint &&
              again.root.dev === root.dev &&
              again.root.ino === root.ino &&
              again.root.birthtimeNs === root.birthtimeNs
            );
          }),
      };
    },
  };
}

const hostProver = heldProverOn((path) => lstat(path, { bigint: true }));

/**
 * The verdict that `candidates`, the holders a fresh inventory found below the lease, still hold
 * it; `undefined` when there are none or one cannot be keyed, an image always among them, which
 * leaves the inventory's held result standing uncached. An unreadable holder is an error, not a
 * verdict.
 */
export function captureHeldVerdict(
  context: HeldContext,
  candidates: readonly HeldCandidate[],
): Promise<CacheResult<HeldVerdict | undefined>> {
  return hostProver.capture(context, candidates);
}

/**
 * Whether `verdict` still proves its lease held in `context`: the same volume incarnation and
 * lease, and every holder still mounted with the identity it was keyed by. `false` asks for a
 * fresh inventory; it never says the lease is free.
 */
export function validateHeldVerdict(verdict: HeldVerdict, context: HeldContext): Promise<CacheResult<boolean>> {
  return hostProver.validate(verdict, context);
}

/** A verdict from its JSON value; `undefined` for anything malformed, which asks for a fresh inventory. */
export function parseHeldVerdict(value: unknown): HeldVerdict | undefined {
  if (!isRecord(value) || value.version !== 1 || !Array.isArray(value.holders)) {
    return undefined;
  }
  const context = parseContext(value.context);
  const proofs: HeldProof[] = [];
  for (const holder of value.holders) {
    const proof = parseProof(holder);
    if (!proof) {
      return undefined;
    }
    proofs.push(proof);
  }
  const holders = nonEmpty(proofs) ? proofs : undefined;
  return context && holders && { version: 1, context, holders };
}

function parseContext(value: unknown): HeldContext | undefined {
  if (!isRecord(value)) {
    return undefined;
  }
  const volumeKey = text(value.volumeKey);
  const leaseKey = text(value.leaseKey);
  const ramDevice = decimal(value.ramDevice);
  return volumeKey && leaseKey && ramDevice ? { volumeKey, leaseKey, ramDevice } : undefined;
}

/** A mount proof; any other kind, an image's among them, is malformed. */
function parseProof(value: unknown): HeldProof | undefined {
  if (!isRecord(value) || value.kind !== 'mount' || !isRecord(value.root)) {
    return undefined;
  }
  const mountpoint = text(value.mountpoint);
  const description = text(value.description);
  const dev = decimal(value.root.dev);
  const ino = decimal(value.root.ino);
  const birthtimeNs = decimal(value.root.birthtimeNs);
  return mountpoint && description && dev && ino && birthtimeNs
    ? { kind: 'mount', mountpoint, description, root: { dev, ino, birthtimeNs } }
    : undefined;
}

/** A nonempty string. */
function text(value: unknown): string | undefined {
  return typeof value === 'string' && value.length > 0 ? value : undefined;
}

/** An integer as `bigint.toString()` writes it: the only spelling that compares equal to a fresh read. */
function decimal(value: unknown): string | undefined {
  return typeof value === 'string' && /^(?:0|-?[1-9]\d*)$/.test(value) ? value : undefined;
}

function nonEmpty(items: readonly HeldProof[]): items is readonly [HeldProof, ...HeldProof[]] {
  return items.length > 0;
}

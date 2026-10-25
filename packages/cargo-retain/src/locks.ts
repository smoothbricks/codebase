// Cargo takes flock(2) on its lock files, so a pass takes the same lock: exclusive, never waiting. The kernel lets go
// of it when the descriptor holding it closes, however its process ends. How a runtime takes it is the one thing that
// differs between them, so a pass is handed a LockProvider: `locks-node.ts` for Node, `locks-ffi.ts` for the compiled
// CLI. Whichever holds it, an attempt that cannot be made is a value, never an exception.
//
// WHY lock: Cargo builds are the other writers, and they coordinate only through these flock(2)s (each holds its
// profile's locks shared for the whole build), so no actor of ours could stand in for them. A pass holds them across its
// awaits on purpose: it reads fingerprints and renames generations in several steps, and a build must not start in
// between. Each provider says how its hold ends if the process dies.

export type LockMode = 'existing' | 'create';

export interface Hold {
  release(): Promise<void>;
}

export type HoldAttempt =
  | { readonly kind: 'held'; readonly hold: Hold }
  /** Another process holds the lock, and only that: any other failure is `unavailable`. */
  | { readonly kind: 'busy'; readonly lock: string }
  /** The attempt could not be made at all: nothing is held, and nobody is known to hold the lock. */
  | { readonly kind: 'unavailable'; readonly reason: string };

export interface LockProvider {
  /** Every lock in `paths` exclusively and without waiting, in order; the first one another holds makes it `busy`. */
  tryHold(paths: readonly string[], mode: LockMode): Promise<HoldAttempt>;
}

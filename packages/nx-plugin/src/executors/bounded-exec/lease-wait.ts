/** Peer wait classes from specs/cowshed/05_gateway.md, "Deadlines". */
export const CONTROL_MS = 5_000;
export const HEARTBEAT_MS = 5_000;
export const HEARTBEAT_SILENCE_MS = 15_000;

/** The CPU pool's actor-authored progress, shared by its server and lease client. */
export interface CpuLeaseProgress {
  ok: true;
  progress: number;
}

type CpuWaitState = { kind: 'sending' } | { kind: 'waiting'; at: number; last: string; sequence: number };

/** A failed wait, not a failed command: expiry ends this client's admission only. */
export interface CpuWaitFailure {
  code: 'TIMEOUT' | 'SILENT';
  boundMs: number;
  elapsedMs: number;
  last: string;
}

/** Pure CPU admission policy; its injected monotonic clock performs no I/O or scheduling. */
export class CpuLeaseWait {
  private readonly started: number;
  private state: CpuWaitState = { kind: 'sending' };

  constructor(private readonly now: () => number = () => performance.now()) {
    this.started = now();
  }

  /** The kernel took the complete request; only peer silence bounds the answer from here. */
  sent(): void {
    if (this.state.kind === 'sending') {
      this.state = { kind: 'waiting', at: this.now(), last: 'request sent', sequence: 0 };
    }
  }

  /** A queue entry proves actor progress, not a deadline for returning a token. */
  queued(): void {
    this.sent();
    if (this.state.kind === 'waiting') {
      this.state = { ...this.state, at: this.now(), last: 'queued' };
    }
  }

  /** Only an advancing actor sequence renews silence; duplicate or invalid progress proves nothing. */
  heartbeat(sequence: number): boolean {
    this.sent();
    if (this.state.kind !== 'waiting' || !Number.isSafeInteger(sequence) || sequence <= this.state.sequence)
      return false;
    this.state = { kind: 'waiting', at: this.now(), last: `heartbeat ${sequence}`, sequence };
    return true;
  }

  /** Milliseconds until the current phase's bound, clipped at zero. */
  remainingMs(): number {
    const due = this.state.kind === 'sending' ? this.started + CONTROL_MS : this.state.at + HEARTBEAT_SILENCE_MS;
    return Math.max(0, due - this.now());
  }

  /** No total queue age exists: an advancing heartbeat keeps the wait alive indefinitely. */
  expired(): CpuWaitFailure | null {
    if (this.remainingMs() > 0) return null;
    return {
      code: this.state.kind === 'sending' ? 'TIMEOUT' : 'SILENT',
      boundMs: this.state.kind === 'sending' ? CONTROL_MS : HEARTBEAT_SILENCE_MS,
      elapsedMs: this.now() - this.started,
      last: this.state.kind === 'sending' ? 'connecting or sending request' : this.state.last,
    };
  }
}

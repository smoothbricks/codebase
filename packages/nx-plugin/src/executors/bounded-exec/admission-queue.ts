import { admissionWaitMs, type BoundEvent, NO_ADMISSION_WAIT, stepAdmissionWait } from './bounds.js';
import { HANG_CAPTURE_MS } from './hang-diagnostics.js';
import { MBX_QUEUE_POLL_MS, type MbxQueuedRequest, snapshotMbxQueue } from './mbx-queue.js';
import { type AdmissionQueueWait, describeAdmissionQueue } from './verdict.js';

/**
 * What a Cargo or reserved leaf waits in: mbx's admission queue. The snapshots of it are said on stderr and decide
 * whether the wait is credited against the idle bound, so they can fail a run and never change a command that passes:
 * the key leaves this out (README, "Bounded exec key"). The bounds themselves (bounds.ts) stay keyed.
 */

/** The mbx host a snapshot runs `mbx` through; the real one unless a test injects another. */
export type QueueHost = Parameters<typeof snapshotMbxQueue>[3];

/** What the run tells its observation of itself. */
export interface ObservationHost {
  /** The run is over: it settled or a bound expired. Nothing more is observed or credited. */
  over(): boolean;
  /** The command's pid, once it is spawned. */
  pid(): number | undefined;
  /** The environment the command runs with, through which mbx finds its session. */
  environment(): NodeJS.ProcessEnv;
  /** Told, never thrown. */
  tell(text: string): void;
  /** The bounds' next event. */
  step(event: BoundEvent): void;
  /** Whether the command is running a test. */
  testing(): boolean;
  readonly queueHost: QueueHost | undefined;
}

/** One run's view of the admission queue its leaf waits in. */
export class AdmissionObservation {
  /** mbx granted the leaf its cores. */
  admitted = false;
  private wait = NO_ADMISSION_WAIT;
  private requests: readonly MbxQueuedRequest[] = [];
  /**
   * Why the last admission snapshot failed, while the one after it has not succeeded; null otherwise. A snapshot only
   * decides whether waiting on mbx admission is credited against the idle bound, so a failed one withholds that credit
   * and ends nothing: the bounds decide, and an idle expiry names it.
   */
  private failure: string | null = null;
  /** When the last successful snapshot completed (`Date.now()`): credit runs to it and no further. */
  private confirmedAt: number | null = null;
  private readonly abort = new AbortController();
  private timer: NodeJS.Timeout | undefined;
  private snapshot: Promise<void> | null = null;
  private lastDiagnostic = '';
  private lastDiagnosticAt: number | null = null;
  private lastSnapshotDiagnostics = '';

  constructor(private readonly host: ObservationHost) {}

  /** Why the last snapshot failed, while the one after it has not succeeded. */
  get unconfirmed(): string | null {
    return this.failure;
  }

  /**
   * A reserved leaf's own request on its run's endpoint is its admission wait; a Cargo leaf's comes from mbx's report.
   * `reserved` is whether the leaf holds a reservation of its own.
   */
  queueWait(reserved: boolean): AdmissionQueueWait | null {
    return this.admitted || (this.requests.length === 0 && !reserved)
      ? null
      : { waitedMs: admissionWaitMs(this.wait, performance.now()), requests: this.requests };
  }

  /** A reserved leaf's request began to wait on its run's endpoint. */
  waiting(): void {
    this.wait = stepAdmissionWait(this.wait, { kind: 'snapshot', at: performance.now(), waiting: true });
  }

  /** Only mbx's activity report says whether a Cargo leaf's compiler or test waits in the pool: poll it. */
  poll(): void {
    this.timer = setInterval(() => {
      void this.refresh(false);
    }, MBX_QUEUE_POLL_MS);
    void this.refresh(false);
  }

  /** The run ended: the wait stops counting and no snapshot is taken or credited again. */
  stop(): void {
    this.wait = stepAdmissionWait(this.wait, { kind: 'stop', at: performance.now() });
    clearInterval(this.timer);
    this.abort.abort();
  }

  /** The snapshot in flight, if any, has finished. */
  async idle(): Promise<void> {
    await this.snapshot;
  }

  private unconfirm(detail: string): void {
    if (this.host.over() || this.abort.signal.aborted) return;
    if (this.failure !== detail) {
      this.host.tell(
        `bounded-exec: mbx admission snapshot failed: ${detail}; admission is unconfirmed, so this wait earns no idle credit until a snapshot succeeds\n`,
      );
    }
    this.failure = detail;
    this.requests = [];
    this.wait = stepAdmissionWait(this.wait, { kind: 'snapshot', at: performance.now(), waiting: false });
    // The wait since the last confirmed snapshot is unconfirmed: the idle bound resumes from that snapshot.
    this.host.step({
      kind: 'admission-queue',
      at: this.confirmedAt ?? Date.now(),
      waiting: false,
      testing: this.host.testing(),
    });
  }

  /** Take a snapshot; `fresh` drains an older poll first. */
  async refresh(fresh: boolean): Promise<void> {
    if (fresh && this.snapshot !== null) await this.snapshot;
    const pid = this.host.pid();
    if (this.host.over() || this.abort.signal.aborted || pid === undefined) return;
    this.snapshot ??= (async () => {
      const snapshot = await snapshotMbxQueue(pid, this.host.environment(), this.abort.signal, this.host.queueHost);
      if (this.host.over() || this.abort.signal.aborted) return;
      if (!snapshot.ok) {
        this.unconfirm(snapshot.detail);
        return;
      }
      const diagnostics = snapshot.diagnostics?.join('; ') ?? '';
      if (diagnostics !== '' && diagnostics !== this.lastSnapshotDiagnostics) {
        this.host.tell(`bounded-exec: ${diagnostics}\n`);
      }
      this.lastSnapshotDiagnostics = diagnostics;
      const waiting = snapshot.requests.length > 0;
      const observedAt = Date.now();
      const observedWaitAt = performance.now();
      this.wait = stepAdmissionWait(this.wait, { kind: 'snapshot', at: observedWaitAt, waiting });
      this.failure = null;
      this.confirmedAt = observedAt;
      if (waiting) {
        this.requests = snapshot.requests;
        const positions = JSON.stringify(snapshot.requests);
        if (
          positions !== this.lastDiagnostic ||
          this.lastDiagnosticAt === null ||
          Date.now() - this.lastDiagnosticAt >= HANG_CAPTURE_MS
        ) {
          const waitedMs = admissionWaitMs(this.wait, observedWaitAt);
          this.host.tell(`cowshed: mbx ${describeAdmissionQueue({ waitedMs, requests: snapshot.requests })}\n`);
          this.lastDiagnostic = positions;
          this.lastDiagnosticAt = Date.now();
        }
      } else {
        this.lastDiagnostic = '';
      }
      this.host.step({ kind: 'admission-queue', at: observedAt, waiting, testing: this.host.testing() });
    })()
      .catch((error: unknown) => {
        this.unconfirm(error instanceof Error ? error.message : String(error));
      })
      .finally(() => {
        this.snapshot = null;
      });
    await this.snapshot;
  }
}

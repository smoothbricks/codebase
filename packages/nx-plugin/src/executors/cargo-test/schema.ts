export interface CargoTestOptions {
  phase: 'compile' | 'run';
  cwd: string;
  release?: boolean;
  timeoutMs?: number;
  killAfterMs?: number;
  jobs?: number;
  __unparsed__?: string[];
}

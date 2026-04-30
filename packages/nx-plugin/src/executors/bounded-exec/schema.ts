export interface BoundedExecOptions {
  command: string;
  cwd?: string;
  env?: Record<string, string>;
  timeoutMs: number;
  killAfterMs?: number;
  forwardAllArgs?: boolean;
  args?: string | string[];
  __unparsed__?: string[];
}

import { JsBufferStrategy } from '../JsBufferStrategy.js';
import type { OpContextBinding } from '../opContext/types.js';
import { createTraceRoot } from '../traceRoot.universal.js';
import { StdioTracer, type StdioTracerOptions } from '../tracers/StdioTracer.js';
import type { AnySpanBuffer } from '../types.js';

type StdioStream = NonNullable<StdioTracerOptions['out']>;

function makeConsoleWriteStream(write: (line: string) => void): StdioStream {
  return {
    write(chunk: unknown): boolean {
      write(String(chunk).replace(/\n$/, ''));
      return true;
    },
  } as unknown as StdioStream;
}

function replayChildSpan(tracer: StdioTracer<OpContextBinding>, buffer: AnySpanBuffer): void {
  const typedBuffer = buffer as unknown as Parameters<StdioTracer<OpContextBinding>['onSpanStart']>[0];
  tracer.onSpanStart(typedBuffer);
  for (const child of buffer._children) {
    replayChildSpan(tracer, child);
  }
  tracer.onSpanEnd(typedBuffer);
}

export function replayTraceToStdio(binding: OpContextBinding, rootBuffer: AnySpanBuffer): void {
  const processRef = (globalThis as { process?: { stdout?: unknown; stderr?: unknown } }).process;
  const out = (processRef?.stdout as StdioStream | undefined) ?? makeConsoleWriteStream((line) => console.log(line));
  const err = (processRef?.stderr as StdioStream | undefined) ?? makeConsoleWriteStream((line) => console.error(line));

  const tracer = new StdioTracer(binding, {
    bufferStrategy: new JsBufferStrategy(),
    createTraceRoot,
    out,
    err,
    colorEnabled: processRef?.stdout !== undefined,
  });

  const typedRootBuffer = rootBuffer as unknown as Parameters<StdioTracer<OpContextBinding>['onTraceStart']>[0];

  tracer.onTraceStart(typedRootBuffer);
  for (const child of rootBuffer._children) {
    replayChildSpan(tracer, child);
  }
  tracer.onTraceEnd(typedRootBuffer);
}

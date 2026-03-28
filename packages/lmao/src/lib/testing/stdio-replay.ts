import { JsBufferStrategy } from '../JsBufferStrategy.js';
import type { OpContextBinding } from '../opContext/types.js';
import { createTraceRoot } from '../traceRoot.universal.js';
import { StdioTracer, type StdioWritable } from '../tracers/StdioTracer.js';
import type { AnySpanBuffer } from '../types.js';

function isStdioWritable(value: unknown): value is StdioWritable {
  return typeof value === 'object' && value !== null && typeof Reflect.get(value, 'write') === 'function';
}

function makeConsoleWriteStream(write: (line: string) => void): StdioWritable {
  return {
    write(chunk: string): boolean {
      write(chunk.replace(/\n$/, ''));
      return true;
    },
  };
}

function replayChildSpan(tracer: StdioTracer<OpContextBinding>, buffer: AnySpanBuffer): void {
  tracer.onSpanStart(buffer);
  for (const child of buffer._children) {
    replayChildSpan(tracer, child);
  }
  tracer.onSpanEnd(buffer);
}

export function replayTraceToStdio(binding: OpContextBinding, rootBuffer: AnySpanBuffer): void {
  const processRef = (globalThis as { process?: { stdout?: unknown; stderr?: unknown } }).process;
  const out = isStdioWritable(processRef?.stdout)
    ? processRef.stdout
    : makeConsoleWriteStream((line) => console.log(line));
  const err = isStdioWritable(processRef?.stderr)
    ? processRef.stderr
    : makeConsoleWriteStream((line) => console.error(line));

  const tracer = new StdioTracer(binding, {
    bufferStrategy: new JsBufferStrategy(),
    createTraceRoot,
    out,
    err,
    colorEnabled: processRef?.stdout !== undefined,
  });

  tracer.onTraceStart(rootBuffer);
  for (const child of rootBuffer._children) {
    replayChildSpan(tracer, child);
  }
  tracer.onTraceEnd(rootBuffer);
}

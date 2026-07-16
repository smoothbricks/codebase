import { JsBufferStrategy } from '../JsBufferStrategy.js';
import type { OpContextBinding } from '../opContext/types.js';
import { createTraceRoot } from '../traceRoot.universal.js';
import { StdioTracer, type StdioWritable } from '../tracers/StdioTracer.js';
import { iterateSpanChildren } from '../traceTopology.js';
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
type ReplayFrame = {
  buffer: AnySpanBuffer;
  children: Generator<AnySpanBuffer>;
  replayLifecycle: boolean;
};

function replayChildSpans(tracer: StdioTracer<OpContextBinding>, rootBuffer: AnySpanBuffer): void {
  const stack: ReplayFrame[] = [
    { buffer: rootBuffer, children: iterateSpanChildren(rootBuffer), replayLifecycle: false },
  ];

  while (stack.length > 0) {
    const frame = stack[stack.length - 1];
    if (!frame) break;

    const next = frame.children.next();
    if (!next.done) {
      tracer.onSpanStart(next.value);
      stack.push({ buffer: next.value, children: iterateSpanChildren(next.value), replayLifecycle: true });
      continue;
    }

    stack.pop();
    if (frame.replayLifecycle) tracer.onSpanEnd(frame.buffer);
  }
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
  replayChildSpans(tracer, rootBuffer);
  tracer.onTraceEnd(rootBuffer);
}

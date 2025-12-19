/**
 * Experiment 6: Module-bound op() with pre-typed context
 *
 * Key insight: module.op() already knows the schema/deps, so it can
 * provide a TYPED OpContext without user annotation.
 *
 * ============================================================================
 * RESULTS SUMMARY - EVERYTHING WORKS!
 * ============================================================================
 *
 * SINGLE OP - op('name', (ctx, ...args) => result):
 *   ✅ ctx is typed from module (no annotation needed)
 *   ✅ Args are inferred correctly: [string, 'GET'|'POST'] not any[]
 *   ✅ Wrong args to span() errors at call site
 *
 * BATCH OPS - op({ GET(ctx, url) {}, request(ctx, url, opts) {} }):
 *   ✅ ctx is typed from module (no annotation needed)
 *   ✅ Result ops have correct types: Op<[string], Response>
 *   ✅ this binding works: this.request is Op<[string, RequestOpts], Response>
 *   ✅ Wrong args to span(this.x, wrongArgs) DOES error inside definition
 *   ✅ Wrong args to span(resultOp) errors at external call site
 *
 * KEY FIX: Op.fn must include ctx and Args:
 *   class Op<Args, Result, Ctx = unknown> {
 *     fn: (ctx: Ctx, ...args: Args) => Promise<Result>
 *   }
 *
 * Not:
 *   fn: (...args: unknown[]) => Promise<Result>  // WRONG - loses Args!
 */

// =============================================================================
// Schema and Deps types (what a module defines)
// =============================================================================

interface HttpSchema {
  method: 'GET' | 'POST' | 'PUT' | 'DELETE';
  url: string;
  status: number;
}

interface RetryOp {
  _brand: 'RetryOp';
}

interface HttpDeps {
  retry: RetryOp;
}

// =============================================================================
// Op class
// =============================================================================

class Op<Args extends unknown[], Result, Ctx = unknown> {
  constructor(
    readonly name: string,
    readonly fn: (ctx: Ctx, ...args: Args) => Promise<Result>,
  ) {}
}

// =============================================================================
// Context type - parameterized by Schema and Deps
// =============================================================================

type TagAPI<Schema> = {
  [K in keyof Schema]: (value: Schema[K]) => TagAPI<Schema>;
};

interface LogAPI {
  info(msg: string): void;
  warn(msg: string): void;
  error(msg: string): void;
}

interface OpContext<Schema, Deps> {
  span<R, A extends unknown[]>(name: string, op: Op<A, R>, ...args: A): Promise<R>;
  log: LogAPI;
  tag: TagAPI<Schema>;
  deps: Deps;
}

// =============================================================================
// Type helpers for batch ops
// =============================================================================

// Extract args from a function that takes ctx as first param
type ExtractArgs<Ctx, Fn> = Fn extends (ctx: Ctx, ...args: infer Args) => Promise<infer _R> ? Args : never;
type ExtractResult<Ctx, Fn> = Fn extends (ctx: Ctx, ...args: infer _Args) => Promise<infer R> ? R : never;

// Transform batch definitions to Op instances
type BatchResult<Ctx, T> = {
  [K in keyof T]: Op<ExtractArgs<Ctx, T[K]>, ExtractResult<Ctx, T[K]>>;
};

// =============================================================================
// Module class with op() overloads
// =============================================================================

class Module<Schema, Deps> {
  constructor(
    readonly name: string,
    readonly _schema?: Schema,
    readonly _deps?: Deps,
  ) {}

  /**
   * Overload 1: Single named op
   * op('name', async (ctx, arg1, arg2) => result)
   *
   * ctx is typed as OpContext<Schema, Deps> from the module
   */
  op<Args extends unknown[], R>(
    name: string,
    fn: (ctx: OpContext<Schema, Deps>, ...args: Args) => Promise<R>,
  ): Op<Args, R>;

  /**
   * Overload 2: Batch ops with this binding
   * op({ GET(ctx, url) {}, request(ctx, url, opts) { this.GET(...) } })
   *
   * Each function must have ctx: OpContext<Schema, Deps> as first param.
   * `this` is typed as the result object (Op instances).
   *
   * Constraint: (ctx: Ctx, ...args: any[]) - this types ctx from module.
   * BatchResult extracts the ACTUAL args, so inference should still work.
   */
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  op<T extends Record<string, (ctx: OpContext<Schema, Deps>, ...args: any[]) => Promise<any>>>(
    definitions: T & ThisType<BatchResult<OpContext<Schema, Deps>, T>>,
  ): BatchResult<OpContext<Schema, Deps>, T>;

  // Implementation
  op(
    nameOrDefs: string | Record<string, unknown>,
    fn?: unknown,
  ): Op<unknown[], unknown> | Record<string, Op<unknown[], unknown>> {
    if (typeof nameOrDefs === 'string') {
      return new Op(nameOrDefs, fn as () => Promise<unknown>);
    }
    const result: Record<string, Op<unknown[], unknown>> = {};
    for (const [name, opFn] of Object.entries(nameOrDefs)) {
      result[name] = new Op(name, opFn as () => Promise<unknown>);
    }
    for (const op of Object.values(result)) {
      (op as { fn: unknown }).fn = (op.fn as CallableFunction).bind(result);
    }
    return result;
  }
}

// =============================================================================
// Test: Create a module
// =============================================================================

const httpModule = new Module<HttpSchema, HttpDeps>('http');

// =============================================================================
// Test 1: Single op - ctx should be typed without annotation
// =============================================================================

const singleOp = httpModule.op('fetch', async (ctx, url: string, method: 'GET' | 'POST') => {
  ctx.tag.method(method).url(url);
  ctx.log.info(`${method} ${url}`);
  return fetch(url);
});

// Verify inference - should be [string, 'GET' | 'POST']
type SingleOpArgs = typeof singleOp extends Op<infer A, unknown> ? A : never;
const _checkSingle: SingleOpArgs = ['https://example.com', 'GET'];
void _checkSingle;

// =============================================================================
// Test 2: Batch ops - ctx should be typed, this should work
// =============================================================================

interface RequestOpts {
  method: 'GET' | 'POST';
  body?: unknown;
}

const batchOps = httpModule.op({
  async GET(ctx, url: string) {
    ctx.log.info(`GET ${url}`);
    return ctx.span('request', this.request, url, { method: 'GET' as const });
  },
  async request(ctx, url: string, opts: RequestOpts) {
    ctx.tag.method(opts.method).url(url);
    return fetch(url);
  },
});

// Verify inference
type BatchGETArgs = typeof batchOps.GET extends Op<infer A, unknown> ? A : never;
type BatchRequestArgs = typeof batchOps.request extends Op<infer A, unknown> ? A : never;

const _checkGET: BatchGETArgs = ['https://example.com'];
void _checkGET;
const _checkRequest: BatchRequestArgs = ['https://example.com', { method: 'GET' }];
void _checkRequest;

// =============================================================================
// Test 3: Type errors should be caught
// =============================================================================

export async function testTypeErrors(ctx: OpContext<HttpSchema, HttpDeps>) {
  // Single op - wrong arg type
  // @ts-expect-error number not assignable to string
  await ctx.span('a', singleOp, 123, 'GET');

  // Batch op - wrong arg type
  // @ts-expect-error number not assignable to string
  await ctx.span('c', batchOps.GET, 123);

  // Batch op - missing arg
  // @ts-expect-error expected 2 arguments
  await ctx.span('d', batchOps.request, 'url');

  // Valid calls
  await ctx.span('e', singleOp, 'https://example.com', 'GET');
  await ctx.span('f', batchOps.GET, 'https://example.com');
  await ctx.span('g', batchOps.request, 'https://example.com', { method: 'POST' });
}

// =============================================================================
// Test 4: this binding type errors
// =============================================================================

const _testThisBinding = httpModule.op({
  async caller(ctx, x: number) {
    // @ts-expect-error callee expects string, 123 is number
    await ctx.span('t1', this.callee, 123);

    // @ts-expect-error callee expects string, x is number
    await ctx.span('t2', this.callee, x);

    // Valid: passing string
    return ctx.span('t3', this.callee, 'hello');
  },
  async callee(ctx, s: string) {
    void ctx;
    return s.toUpperCase();
  },
});
void _testThisBinding;

// Test span outside the op definition - WORKS!
export async function testSpanWithThisCallee(ctx: OpContext<HttpSchema, HttpDeps>) {
  // This should error - _testThisBinding.callee expects string, not number
  // @ts-expect-error number not assignable to string
  await ctx.span('test', _testThisBinding.callee, 123);

  // This should work
  await ctx.span('test', _testThisBinding.callee, 'hello');
}

// Verify the types are correct
type CalleeArgs = typeof _testThisBinding.callee extends Op<infer A, unknown> ? A : never;
type CallerArgs = typeof _testThisBinding.caller extends Op<infer A, unknown> ? A : never;
const _checkCallee: CalleeArgs = ['hello'];
const _checkCaller: CallerArgs = [42];
void _checkCallee;
void _checkCaller;

// =============================================================================
// Test 5: Tag API should be typed from schema
// =============================================================================

const _testTagTyping = httpModule.op({
  async testTags(ctx) {
    // Valid - method is 'GET' | 'POST' | 'PUT' | 'DELETE'
    ctx.tag.method('GET');
    ctx.tag.url('https://example.com');
    ctx.tag.status(200);

    // @ts-expect-error 'INVALID' not assignable to method type
    ctx.tag.method('INVALID');

    // @ts-expect-error number not assignable to url (string)
    ctx.tag.url(123);

    return 'done';
  },
});
void _testTagTyping;

export { httpModule, singleOp, batchOps };

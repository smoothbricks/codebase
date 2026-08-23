package lmao

// transformCorpus is the differential-oracle input set. Coverage is organized so
// that a change to one lowering is visible against every OTHER lowering:
//
//	span-op-*        §2 lowering that already ships — must not move
//	span-bail-*      documented §2 bailouts — must keep the public dispatcher
//	span-fn-*        inline arrow/function-expression op position
//	destructured-*   §3 destructured first parameter
//	chain-*          §4/§6 tag, log, and result inlining
//	meta-*           §5 defineModule and the defineOp/defineOps hint ABI
//	task-*           §7 task line injection
//
// Entries are single-purpose on purpose: a diff names the construct it moved.
var transformCorpus = map[string]string{
	// --- §2 span lowering that already ships ---------------------------------
	"span-op-zero-args": `
declare const fetchOp: Op;
export const a = defineOp('a', async (ctx: SpanContext) => {
  await ctx.span('fetch', fetchOp);
});
`,
	"span-op-one-arg": `
declare const fetchOp: Op;
export const a = defineOp('a', async (ctx: SpanContext, userId: string) => {
  await ctx.span('fetch-user', fetchOp, userId);
});
`,
	"span-op-eight-args": `
declare const wideOp: Op;
export const a = defineOp('a', async (ctx: SpanContext) => {
  await ctx.span('wide', wideOp, 1, 2, 3, 4, 5, 6, 7, 8);
});
`,
	"span-op-this-receiver": `
declare const fetchOp: Op;
export class Holder extends SpanContext {
  async run(): Promise<void> {
    await this.span('from-this', fetchOp, 1);
  }
}
`,
	"span-op-returned-from-async": `
declare const fetchOp: Op;
export const a = defineOp('a', async (ctx: SpanContext) => {
  return ctx.span('returned', fetchOp, 7);
});
`,
	"span-op-nested-in-op": `
declare const innerOp: Op;
export const a = defineOp('a', async (ctx: SpanContext) => {
  await ctx.span('outer', innerOp, 1);
  await ctx.span('again', innerOp, 2);
});
`,
	"span-op-duplicate-names": `
declare const innerOp: Op;
export const a = defineOp('a', async (ctx: SpanContext) => {
  await ctx.span('same', innerOp);
  await ctx.span('same', innerOp);
});
`,
	"span-op-non-literal-name": `
declare const fetchOp: Op;
declare const dynamicName: string;
export const a = defineOp('a', async (ctx: SpanContext) => {
  await ctx.span(dynamicName, fetchOp, 1);
});
`,
	"span-op-template-name": `
declare const fetchOp: Op;
declare const suffix: string;
export const a = defineOp('a', async (ctx: SpanContext) => {
  await ctx.span(` + "`fetch-${suffix}`" + `, fetchOp, 1);
});
`,
	"span-op-inside-loop": `
declare const fetchOp: Op;
export const a = defineOp('a', async (ctx: SpanContext, ids: string[]) => {
  for (const id of ids) {
    await ctx.span('each', fetchOp, id);
  }
});
`,
	"span-op-inside-try": `
declare const fetchOp: Op;
export const a = defineOp('a', async (ctx: SpanContext) => {
  try {
    await ctx.span('guarded', fetchOp);
  } catch {
    await ctx.span('recover', fetchOp);
  }
});
`,

	// --- §2 documented bailouts ---------------------------------------------
	"span-bail-unstable-receiver": `
declare const fetchOp: Op;
declare function getCtx(): SpanContext;
export const a = async (): Promise<void> => {
  await getCtx().span('unstable-recv', fetchOp, 1);
};
`,
	"span-bail-unstable-op": `
declare const ctx: SpanContext;
declare function getOp(): Op;
export const a = async (): Promise<void> => {
  await ctx.span('unstable-op', getOp(), 1);
};
`,
	"span-bail-op-property-access": `
declare const ctx: SpanContext;
declare const registry: { fetch: Op };
export const a = async (): Promise<void> => {
  await ctx.span('member-op', registry.fetch, 1);
};
`,
	"span-bail-nine-args": `
declare const wideOp: Op;
export const a = defineOp('a', async (ctx: SpanContext) => {
  await ctx.span('too-wide', wideOp, 1, 2, 3, 4, 5, 6, 7, 8, 9);
});
`,
	"span-bail-override-form": `
declare const fetchOp: Op;
export const a = defineOp('a', async (ctx: SpanContext) => {
  await ctx.span('overridden', { sampled: true }, fetchOp, 1);
});
`,
	"span-bail-non-lmao-receiver": `
declare const fetchOp: Op;
declare const notACtx: { span(name: string, op: Op, arg: number): Promise<unknown> };
export const a = async (): Promise<void> => {
  await notACtx.span('foreign-recv', fetchOp, 1);
};
`,
	"span-bail-non-op-second-arg": `
declare const ctx: SpanContext;
export const a = async (): Promise<void> => {
  await ctx.span('not-an-op', 42 as unknown as Op, 1);
};
`,
	"span-bail-single-argument": `
declare const ctx: SpanContext;
export const a = async (): Promise<void> => {
  await (ctx.span as unknown as (name: string) => Promise<void>)('lonely');
};
`,
	"span-bail-bare-span-call": `
declare function span(name: string, op: Op): Promise<unknown>;
declare const fetchOp: Op;
export const a = async (): Promise<void> => {
  await span('free-function', fetchOp);
};
`,

	// --- inline closure in the op position ----------------------------------
	"span-fn-arrow-zero-args": `
export const a = defineOp('a', async (ctx: SpanContext) => {
  await ctx.span('inline', async (child: SpanContext) => child.ok(1));
});
`,
	"span-fn-arrow-one-arg": `
export const a = defineOp('a', async (ctx: SpanContext, userId: string) => {
  await ctx.span('inline-arg', async (child: SpanContext, id: string) => child.ok(id), userId);
});
`,
	"span-fn-arrow-three-args": `
export const a = defineOp('a', async (ctx: SpanContext) => {
  await ctx.span('inline-three', async (child: SpanContext, x: number, y: number, z: number) => child.ok(x + y + z), 1, 2, 3);
});
`,
	"span-fn-function-expression": `
export const a = defineOp('a', async (ctx: SpanContext) => {
  await ctx.span('inline-fnexpr', async function (child: SpanContext) {
    return child.ok(1);
  });
});
`,
	"span-fn-sync-arrow": `
export const a = defineOp('a', async (ctx: SpanContext) => {
  await ctx.span('inline-sync', (child: SpanContext) => child.ok(1));
});
`,
	"span-fn-concise-body": `
export const a = defineOp('a', async (ctx: SpanContext) => {
  await ctx.span('inline-concise', async (child: SpanContext) => child.ok(2));
});
`,
	"span-fn-tagging-body": `
export const a = defineOp('a', async (ctx: SpanContext) => {
  await ctx.span('inline-tags', async (child: SpanContext) => {
    child.tag.operation('READ');
    child.log.info('inside inline');
    return child.ok(1);
  });
});
`,
	"span-fn-this-receiver": `
export class Holder extends SpanContext {
  async run(): Promise<void> {
    await this.span('inline-this', async (child: SpanContext) => child.ok(1));
  }
}
`,
	"span-fn-eight-args": `
export const a = defineOp('a', async (ctx: SpanContext) => {
  await ctx.span('inline-wide', async (child: SpanContext) => child.ok(1), 1, 2, 3, 4, 5, 6, 7, 8);
});
`,
	"span-fn-nine-args-bails": `
export const a = defineOp('a', async (ctx: SpanContext) => {
  await ctx.span('inline-too-wide', async (child: SpanContext) => child.ok(1), 1, 2, 3, 4, 5, 6, 7, 8, 9);
});
`,
	"span-fn-override-form-bails": `
export const a = defineOp('a', async (ctx: SpanContext) => {
  await ctx.span('inline-overridden', { sampled: true } as unknown as Op, async (child: SpanContext) => child.ok(1));
});
`,
	"span-fn-unstable-receiver-bails": `
declare function getCtx(): SpanContext;
export const a = async (): Promise<void> => {
  await getCtx().span('inline-unstable', async (child: SpanContext) => child.ok(1));
};
`,
	"span-fn-non-lmao-receiver-bails": `
declare const notACtx: { span(name: string, fn: (ctx: unknown) => unknown): Promise<unknown> };
export const a = async (): Promise<void> => {
  await notACtx.span('inline-foreign', async (child: unknown) => child);
};
`,
	"span-fn-nested-inline": `
export const a = defineOp('a', async (ctx: SpanContext) => {
  await ctx.span('outer-inline', async (child: SpanContext) => {
    await child.span('inner-inline', async (grand: SpanContext) => grand.ok(1));
    return child.ok(2);
  });
});
`,
	"span-fn-mixed-with-op": `
declare const fetchOp: Op;
export const a = defineOp('a', async (ctx: SpanContext) => {
  await ctx.span('an-op', fetchOp, 1);
  await ctx.span('an-inline', async (child: SpanContext) => child.ok(2));
});
`,

	// --- §3 destructured first parameter ------------------------------------
	"destructured-span-only": `
declare const fetchOp: Op;
export const a = op(async ({ span }, userId: string) => {
  await span('fetch', fetchOp, userId);
});
`,
	"destructured-span-and-log": `
declare const fetchOp: Op;
export const a = op(async ({ span, log }, userId: string) => {
  log.info('starting');
  await span('fetch', fetchOp, userId);
});
`,
	"destructured-span-log-tag": `
declare const fetchOp: Op;
export const a = op(async ({ span, log, tag }) => {
  tag.operation('READ');
  log.info('starting');
  await span('fetch', fetchOp);
});
`,
	"destructured-aliased-span": `
declare const fetchOp: Op;
export const a = op(async ({ span: runSpan, log }) => {
  log.info('aliased');
  await runSpan('fetch', fetchOp);
});
`,
	"destructured-aliased-other": `
declare const fetchOp: Op;
export const a = op(async ({ span, log: logger }) => {
  logger.info('aliased other');
  await span('fetch', fetchOp);
});
`,
	"destructured-concise-body": `
declare const fetchOp: Op;
export const a = op(async ({ span }) => span('fetch', fetchOp));
`,
	"destructured-inline-fn": `
export const a = op(async ({ span }) => {
  await span('inline', async (child: SpanContext) => child.ok(1));
});
`,
	"destructured-in-define-op": `
declare const fetchOp: Op;
export const a = defineOp('a', async ({ span, log }, userId: string) => {
  log.info('in defineOp');
  await span('fetch', fetchOp, userId);
});
`,
	"destructured-in-define-ops": `
declare const fetchOp: Op;
export const group = defineOps({
  first: async ({ span }) => {
    await span('fetch', fetchOp);
  },
});
`,
	"destructured-in-task": `
declare const fetchOp: Op;
export const t = task('work', async ({ span, log }) => {
  log.info('task body');
  await span('fetch', fetchOp);
});
`,
	"destructured-multiple-spans": `
declare const fetchOp: Op;
declare const saveOp: Op;
export const a = op(async ({ span, log }, id: string) => {
  log.info('two spans');
  await span('fetch', fetchOp, id);
  await span('save', saveOp, id);
});
`,
	"destructured-function-expression": `
declare const fetchOp: Op;
export const a = op(async function ({ span }) {
  await span('fetch', fetchOp);
});
`,
	"destructured-bail-span-escapes": `
declare const fetchOp: Op;
declare function register(fn: unknown): void;
export const a = op(async ({ span }) => {
  register(span);
  await span('fetch', fetchOp);
});
`,
	"destructured-bail-span-reassigned-alias": `
declare const fetchOp: Op;
export const a = op(async ({ span }) => {
  const alias = span;
  await alias('fetch', fetchOp);
});
`,
	"destructured-bail-rest-binding": `
declare const fetchOp: Op;
export const a = op(async ({ span, ...rest }) => {
  await span('fetch', fetchOp);
  return rest;
});
`,
	"destructured-bail-ctx-name-collision": `
declare const fetchOp: Op;
export const a = op(async ({ span }) => {
  const __ctx = 1;
  await span('fetch', fetchOp);
  return __ctx;
});
`,
	"destructured-bail-no-span-binding": `
export const a = op(async ({ log, tag }) => {
  tag.operation('READ');
  log.info('no span here');
});
`,
	"destructured-bail-unlowerable-span-call": `
declare function getOp(): Op;
export const a = op(async ({ span }) => {
  await span('unstable', getOp());
});
`,
	"destructured-bail-nine-arg-span": `
declare const wideOp: Op;
export const a = op(async ({ span }) => {
  await span('too-wide', wideOp, 1, 2, 3, 4, 5, 6, 7, 8, 9);
});
`,
	"destructured-default-value-preserved": `
declare const fetchOp: Op;
export const a = op(async ({ span, log = undefined as unknown as SpanContext['log'] }) => {
  await span('fetch', fetchOp);
  return log;
});
`,
	"destructured-bail-second-param-only": `
declare const fetchOp: Op;
declare const ctx: SpanContext;
export const a = op(async (first: SpanContext, { span }: { span: SpanFn }) => {
  await ctx.span('normal', fetchOp);
});
`,
	"destructured-body-const-not-param": `
declare const fetchOp: Op;
export const a = defineOp('a', async (ctx: SpanContext) => {
  const { span, log } = ctx;
  log.info('body destructure');
  await span('fetch', fetchOp);
});
`,

	// --- §4/§6 chain inlining ------------------------------------------------
	"chain-tag-single": `
export const a = defineOp('a', async (ctx: SpanContext) => {
  ctx.tag.operation('READ');
});
`,
	"chain-tag-with-bulk": `
export const a = defineOp('a', async (ctx: SpanContext) => {
  ctx.tag.with({ jobId: 'job-1', attempt: 2 });
});
`,
	"chain-tag-multi-setter": `
export const a = defineOp('a', async (ctx: SpanContext) => {
  ctx.tag.operation('WRITE').jobId('job-2').success(true);
});
`,
	"chain-tag-non-literal": `
declare const dynamicJob: string;
export const a = defineOp('a', async (ctx: SpanContext) => {
  ctx.tag.jobId(dynamicJob);
});
`,
	"chain-tag-assigned-not-inlined": `
export const a = defineOp('a', async (ctx: SpanContext) => {
  const writer = ctx.tag.operation('READ');
  return writer;
});
`,
	"chain-log-literal": `
export const a = defineOp('a', async (ctx: SpanContext) => {
  ctx.log.info('a literal message');
});
`,
	"chain-log-fields": `
export const a = defineOp('a', async (ctx: SpanContext) => {
  ctx.log.warn('with fields').jobId('job-3').attempt(1);
});
`,
	"chain-log-all-levels": `
export const a = defineOp('a', async (ctx: SpanContext) => {
  ctx.log.trace('t');
  ctx.log.debug('d');
  ctx.log.info('i');
  ctx.log.warn('w');
  ctx.log.error('e');
});
`,
	"chain-log-duplicate-message": `
export const a = defineOp('a', async (ctx: SpanContext) => {
  ctx.log.info('repeated');
  ctx.log.info('repeated');
});
`,
	"chain-result-ok": `
export const a = defineOp('a', async (ctx: SpanContext) => {
  return ctx.ok({ done: true });
});
`,
	"chain-result-err-with-fields": `
export const a = defineOp('a', async (ctx: SpanContext) => {
  return ctx.err('nope').message('failed').category('io');
});
`,
	"chain-result-in-branch": `
export const a = defineOp('a', async (ctx: SpanContext, flag: boolean) => {
  if (flag) {
    return ctx.ok(1);
  }
  return ctx.err('no');
});
`,

	// --- §5 module and hint metadata ----------------------------------------
	"meta-define-module": `
export const m = defineModule({
  name: 'corpus-module',
});
`,
	"meta-define-module-existing-metadata": `
export const m = defineModule({
  name: 'corpus-module',
  metadata: { git_sha: 'preset' },
});
`,
	"meta-define-op-with-user-metadata": `
export const a = defineOp('a', async (ctx: SpanContext) => {
  ctx.log.info('has metadata');
}, { owner: 'team' });
`,
	"meta-define-op-loop-capacity": `
export const a = defineOp('a', async (ctx: SpanContext, items: string[]) => {
  for (const item of items) {
    ctx.log.info('per item');
  }
});
`,
	"meta-define-op-nested-function": `
export const a = defineOp('a', async (ctx: SpanContext) => {
  const helper = () => ctx.log.info('nested');
  helper();
});
`,
	"meta-define-op-escaping-context": `
declare function keep(value: unknown): void;
export const a = defineOp('a', async (ctx: SpanContext) => {
  keep(ctx);
});
`,
	"meta-define-op-ff-and-scope": `
export const a = defineOp('a', async (ctx: SpanContext) => {
  if (ctx.ff('flag')) {
    ctx.setScope({ region: 'eu' });
  }
});
`,
	"meta-define-ops-group": `
declare const existing: Op;
export const group = defineOps({
  inline: async (ctx: SpanContext) => {
    ctx.log.info('inline member');
  },
  reused: existing,
});
`,
	"meta-define-ops-method-declaration": `
export const group = defineOps({
  async member(ctx: SpanContext) {
    ctx.log.info('method member');
  },
});
`,

	// --- §7 task -------------------------------------------------------------
	"task-simple": `
export const t = task('simple', async (ctx: TaskContext) => {
  ctx.log.info('task ran');
});
`,
	"task-with-span": `
declare const fetchOp: Op;
export const t = task('with-span', async (ctx: TaskContext) => {
  await ctx.span('fetch', fetchOp, 1);
});
`,
	"task-non-literal-name": `
declare const taskName: string;
export const t = task(taskName, async (ctx: TaskContext) => {
  ctx.log.info('dynamic name');
});
`,
}

package lmao

import (
	"regexp"
	"strings"
	"testing"
)

func TestDestructuredContextReRootsOntoGeneratedParameter(t *testing.T) {
	output := transformCorpusEntry(t, `
declare const fetchOp: Op;
export const a = op(async ({ span, log }, userId: string) => {
  log.info('starting');
  await span('fetch', fetchOp, userId);
});
`)

	if !strings.Contains(output, "async (__ctx, userId: string) => {") {
		t.Fatalf("destructured parameter was not replaced by __ctx\n%s", output)
	}
	// The residual properties are rebound as the FIRST statement, so every later
	// statement sees the same bindings it was written against.
	if !strings.Contains(output, "const { log } = __ctx;") {
		t.Fatalf("residual destructured properties were not rebound\n%s", output)
	}
	if !strings.Contains(output, "__ctx.span1(6, 'fetch', fetchOp.callsitePlan.newCtx0(__ctx), fetchOp.callsitePlan, fetchOp.fn, userId)") {
		t.Fatalf("re-rooted span call did not receive the §2 lowering\n%s", output)
	}
	if strings.Contains(output, "{ span,") || strings.Contains(output, "span('fetch'") {
		t.Fatalf("the destructured span binding survived the rewrite\n%s", output)
	}
}

func TestDestructuredContextPreservesAliasesAndDefaults(t *testing.T) {
	aliased := transformCorpusEntry(t, `
declare const fetchOp: Op;
export const a = op(async ({ span, log: logger }) => {
  logger.info('aliased');
  await span('fetch', fetchOp);
});
`)
	if !strings.Contains(aliased, "const { log: logger } = __ctx;") {
		t.Fatalf("property alias was not preserved verbatim\n%s", aliased)
	}

	defaulted := transformCorpusEntry(t, `
declare const fetchOp: Op;
export const a = op(async ({ span, log = undefined as unknown as SpanContext['log'] }) => {
  await span('fetch', fetchOp);
  return log;
});
`)
	if !strings.Contains(defaulted, "const { log = undefined as unknown as SpanContext['log'] } = __ctx;") {
		t.Fatalf("property default was not preserved verbatim\n%s", defaulted)
	}

	// An ALIASED span binding is still the context's member; only the local name
	// differs, and the rewrite replaces it outright.
	aliasedSpan := transformCorpusEntry(t, `
declare const fetchOp: Op;
export const a = op(async ({ span: runSpan, log }) => {
  log.info('aliased span');
  await runSpan('fetch', fetchOp);
});
`)
	if !strings.Contains(aliasedSpan, "__ctx.span0(") || strings.Contains(aliasedSpan, "runSpan") {
		t.Fatalf("aliased span binding was not re-rooted\n%s", aliasedSpan)
	}
}

func TestDestructuredContextComposesWithInlineClosureLowering(t *testing.T) {
	output := transformCorpusEntry(t, `
export const a = op(async ({ span }) => {
  await span('inline', async (child: SpanContext) => child.ok(1));
});
`)

	// A destructured call whose op position is a closure lowers through the same
	// receiver-plan path, now rooted on the generated parameter.
	if !strings.Contains(output, "__ctx.span0(4, 'inline', __ctx._physicalLayoutPlan.newCtx0(__ctx), __ctx._physicalLayoutPlan, async (child: SpanContext)") {
		t.Fatalf("destructured inline closure did not lower\n%s", output)
	}
}

func TestDestructuredContextWithNoResidualKeepsConciseBody(t *testing.T) {
	output := transformCorpusEntry(t, `
declare const fetchOp: Op;
export const a = op(async ({ span }) => span('fetch', fetchOp));
`)

	// Nothing has to be prepended, so the concise body is not expanded into a
	// block for its own sake.
	if !strings.Contains(output, "op(async (__ctx) => __ctx.span0(4, 'fetch', fetchOp.callsitePlan.newCtx0(__ctx), fetchOp.callsitePlan, fetchOp.fn))") {
		t.Fatalf("concise body was not preserved\n%s", output)
	}
}

func TestDestructuredContextConvertsConciseBodyWhenRebindingIsNeeded(t *testing.T) {
	output := transformCorpusEntry(t, `
declare const fetchOp: Op;
export const a = op(async ({ span, log }) => span('fetch', fetchOp) ?? log);
`)

	if !strings.Contains(output, "const { log } = __ctx;") {
		t.Fatalf("residual rebinding was not emitted\n%s", output)
	}
	if !regexp.MustCompile(`return __ctx\.span0\(`).MatchString(output) {
		t.Fatalf("concise body was not converted to a block with an explicit return\n%s", output)
	}
}

func TestDestructuredContextRewritesEveryProvenCall(t *testing.T) {
	output := transformCorpusEntry(t, `
declare const fetchOp: Op;
declare const saveOp: Op;
export const a = op(async ({ span, log }, id: string) => {
  log.info('two spans');
  await span('fetch', fetchOp, id);
  await span('save', saveOp, id);
});
`)

	if strings.Count(output, "__ctx.span1(") != 2 {
		t.Fatalf("not every proven bare span call was lowered\n%s", output)
	}
	if !strings.Contains(output, "saveOp.callsitePlan.newCtx0(__ctx)") {
		t.Fatalf("the second call did not consume its own Op's plan\n%s", output)
	}
}

// The preflight is all-or-nothing: each input below leaves the WHOLE function
// literal untouched, so its span calls keep the public dispatcher. A partial
// rewrite would change destructuring semantics, which is the one outcome worse
// than declining.
func TestDestructuredContextDeclinesUnprovableFunctions(t *testing.T) {
	for _, probe := range []struct {
		name  string
		body  string
		still string
	}{
		{
			name:  "span escapes as a call argument",
			body:  "declare function register(fn: unknown): void;\nexport const a = op(async ({ span }) => {\n  register(span);\n  await span('fetch', fetchOp);\n});",
			still: "span('fetch', fetchOp)",
		},
		{
			name:  "span is aliased to a local",
			body:  "export const a = op(async ({ span }) => {\n  const alias = span;\n  await alias('fetch', fetchOp);\n});",
			still: "const alias = span",
		},
		{
			name:  "a rest binding would absorb the removed property",
			body:  "export const a = op(async ({ span, ...rest }) => {\n  await span('fetch', fetchOp);\n  return rest;\n});",
			still: "{ span, ...rest }",
		},
		{
			name:  "the generated name is already taken",
			body:  "export const a = op(async ({ span }) => {\n  const __ctx = 1;\n  await span('fetch', fetchOp);\n  return __ctx;\n});",
			still: "span('fetch', fetchOp)",
		},
		{
			name:  "an unlowerable call declines the whole function",
			body:  "declare function getOp(): Op;\nexport const a = op(async ({ span }) => {\n  await span('unstable', getOp());\n});",
			still: "span('unstable', getOp())",
		},
		{
			name:  "nine trailing arguments exceed the spanN ABI",
			body:  "declare const wideOp: Op;\nexport const a = op(async ({ span }) => {\n  await span('too-wide', wideOp, 1, 2, 3, 4, 5, 6, 7, 8, 9);\n});",
			still: "span('too-wide', wideOp, 1, 2, 3, 4, 5, 6, 7, 8, 9)",
		},
		{
			name:  "no span binding leaves the pattern alone",
			body:  "export const a = op(async ({ log, tag }) => {\n  tag.operation('READ');\n});",
			still: "{ log, tag }",
		},
		{
			name:  "only the FIRST parameter is a context",
			body:  "declare const ctx: SpanContext;\nexport const a = op(async (first: SpanContext, { span }: { span: SpanFn }) => {\n  await ctx.span('normal', fetchOp);\n});",
			still: "{ span }",
		},
	} {
		t.Run(probe.name, func(t *testing.T) {
			output := transformCorpusEntry(t, "declare const fetchOp: Op;\n"+probe.body+"\n")
			if !strings.Contains(output, probe.still) {
				t.Fatalf("declined function was modified, expected to still contain %q\n%s", probe.still, output)
			}
			// The collision probe's own INPUT contains `__ctx`, so the signal is
			// the generated parameter and the re-rooted call, not the bare name.
			if strings.Contains(output, "(__ctx)") || strings.Contains(output, "(__ctx,") || strings.Contains(output, "__ctx.span") {
				t.Fatalf("a declined function was re-rooted onto a generated context parameter\n%s", output)
			}
		})
	}
}

// A body-level `const { span } = ctx` is NOT a destructured first parameter, so
// §3 does not claim it. Asserting the decline keeps the boundary explicit rather
// than incidental.
func TestBodyLevelDestructuringIsNotRewritten(t *testing.T) {
	output := transformCorpusEntry(t, `
declare const fetchOp: Op;
export const a = defineOp('a', async (ctx: SpanContext) => {
  const { span, log } = ctx;
  log.info('body destructure');
  await span('fetch', fetchOp);
});
`)

	if !strings.Contains(output, "const { span, log } = ctx;") {
		t.Fatalf("body-level destructuring was altered\n%s", output)
	}
	if !strings.Contains(output, "span('fetch', fetchOp)") {
		t.Fatalf("body-level destructured span call was rewritten\n%s", output)
	}
	if strings.Contains(output, "__ctx") {
		t.Fatalf("body-level destructuring emitted the generated context name\n%s", output)
	}
}

package lmao

import (
	"regexp"
	"strings"
	"testing"
)

// These fixtures run on corpusDeclarations rather than
// templateFixtureDeclarations: the inline-closure and override span overloads
// and the destructurable callable `span` property only exist there, so a
// fixture here exercises a lowering decision instead of a type error.

func TestInlineClosureSpanLowersOntoReceiverCallsitePlan(t *testing.T) {
	output := transformCorpusEntry(t, `
export const a = defineOp('a', async (ctx: SpanContext, userId: string) => {
  await ctx.span('inline-arg', async (child: SpanContext, id: string) => child.ok(id), userId);
});
`)

	lowered := regexp.MustCompile(`ctx\.span1\(4, \$\$lmaoVocabulary_1\[0\], ctx\._physicalLayoutPlan\.newCtx0\(ctx\), ctx\._physicalLayoutPlan, async \(child: SpanContext, id: string\) =>`)
	if !lowered.MatchString(output) {
		t.Fatalf("inline closure did not lower onto the receiver's callsite plan\n%s", output)
	}
	// The closure IS the function operand. An Op's `.fn` / `.callsitePlan`
	// projections have no meaning for a literal, and emitting them would read
	// undefined properties off a function object.
	for _, stale := range []string{".fn,", ".callsitePlan", "span('inline-arg'", `span("inline-arg"`} {
		if strings.Contains(output, stale) {
			t.Fatalf("lowered inline closure retained %s\n%s", stale, output)
		}
	}
	if !strings.HasSuffix(strings.TrimSpace(lastSpanArgument(t, output)), "userId") {
		t.Fatalf("trailing span argument was not preserved in order\n%s", output)
	}
}

func TestInlineClosureSpanAritySelectsMonomorphicMethod(t *testing.T) {
	for _, probe := range []struct {
		name   string
		body   string
		method string
	}{
		{"zero", `await ctx.span('n', async (child: SpanContext) => child.ok(1));`, "ctx.span0("},
		{"one", `await ctx.span('n', async (child: SpanContext) => child.ok(1), 1);`, "ctx.span1("},
		{"three", `await ctx.span('n', async (child: SpanContext) => child.ok(1), 1, 2, 3);`, "ctx.span3("},
		{"eight", `await ctx.span('n', async (child: SpanContext) => child.ok(1), 1, 2, 3, 4, 5, 6, 7, 8);`, "ctx.span8("},
	} {
		t.Run(probe.name, func(t *testing.T) {
			output := transformCorpusEntry(t, "export const a = defineOp('a', async (ctx: SpanContext) => {\n  "+probe.body+"\n});\n")
			if !strings.Contains(output, probe.method) {
				t.Fatalf("inline closure with %s trailing arguments did not select %s\n%s", probe.name, probe.method, output)
			}
		})
	}
}

func TestInlineFunctionExpressionSpanLowers(t *testing.T) {
	output := transformCorpusEntry(t, `
export const a = defineOp('a', async (ctx: SpanContext) => {
  await ctx.span('inline-fnexpr', async function (child: SpanContext) {
    return child.ok(1);
  });
});
`)

	if !strings.Contains(output, "ctx.span0(4, $$lmaoVocabulary_1[0], ctx._physicalLayoutPlan.newCtx0(ctx), ctx._physicalLayoutPlan, async function (child: SpanContext)") {
		t.Fatalf("inline function expression did not lower\n%s", output)
	}
}

func TestNestedInlineClosureSpansLowerAgainstTheirOwnReceivers(t *testing.T) {
	output := transformCorpusEntry(t, `
export const a = defineOp('a', async (ctx: SpanContext) => {
  await ctx.span('outer-inline', async (child: SpanContext) => {
    await child.span('inner-inline', async (grand: SpanContext) => grand.ok(1));
    return child.ok(2);
  });
});
`)

	if !strings.Contains(output, "ctx.span0(4, $$lmaoVocabulary_1[0], ctx._physicalLayoutPlan.newCtx0(ctx), ctx._physicalLayoutPlan,") {
		t.Fatalf("outer inline closure did not lower against ctx\n%s", output)
	}
	// The inner span's receiver is the CHILD context the outer span created, not
	// the outer receiver: each lowering reads the plan of the context it is
	// actually called on.
	if !strings.Contains(output, "child.span0(5, $$lmaoVocabulary_1[1], child._physicalLayoutPlan.newCtx0(child), child._physicalLayoutPlan,") {
		t.Fatalf("inner inline closure did not lower against its own child receiver\n%s", output)
	}
	if strings.Contains(output, ".span(") {
		t.Fatalf("a nested inline closure was left on the variadic dispatcher\n%s", output)
	}
}

// Every bailout below keeps the public variadic dispatcher. A declined site is a
// missed optimization; a rewritten one that changed shape would be a
// miscompilation, so these are the load-bearing assertions of the lowering.
func TestInlineClosureSpanBailouts(t *testing.T) {
	for _, probe := range []struct {
		name string
		body string
		want string
	}{
		{
			name: "nine trailing arguments exceed the spanN ABI",
			body: `await ctx.span('too-wide', async (child: SpanContext) => child.ok(1), 1, 2, 3, 4, 5, 6, 7, 8, 9);`,
			want: `ctx.span('too-wide'`,
		},
		{
			name: "the context-override form is not lowered",
			body: `await ctx.span('overridden', { sampled: true } as unknown as Op, async (child: SpanContext) => child.ok(1));`,
			want: `ctx.span('overridden'`,
		},
	} {
		t.Run(probe.name, func(t *testing.T) {
			output := transformCorpusEntry(t, "export const a = defineOp('a', async (ctx: SpanContext) => {\n  "+probe.body+"\n});\n")
			if !strings.Contains(output, probe.want) {
				t.Fatalf("bailout did not keep the public dispatcher, expected %s\n%s", probe.want, output)
			}
			if regexp.MustCompile(`\.span[0-9]\(`).MatchString(output) {
				t.Fatalf("bailout was lowered to a monomorphic method\n%s", output)
			}
		})
	}
}

func TestInlineClosureSpanRequiresProvenContextReceiver(t *testing.T) {
	unstable := transformCorpusEntry(t, `
declare function getCtx(): SpanContext;
export const a = async (): Promise<void> => {
  await getCtx().span('inline-unstable', async (child: SpanContext) => child.ok(1));
};
`)
	// An unstable receiver cannot be duplicated into both the newCtx0 argument
	// and the plan operand without evaluating it twice.
	if !strings.Contains(unstable, `getCtx().span('inline-unstable'`) {
		t.Fatalf("unstable receiver was not left on the public dispatcher\n%s", unstable)
	}

	foreign := transformCorpusEntry(t, `
declare const notACtx: { span(name: string, fn: (ctx: unknown) => unknown): Promise<unknown> };
export const a = async (): Promise<void> => {
  await notACtx.span('inline-foreign', async (child: unknown) => child);
};
`)
	if !strings.Contains(foreign, `notACtx.span('inline-foreign'`) {
		t.Fatalf("non-LMAO receiver was rewritten\n%s", foreign)
	}
}

// lastSpanArgument returns the text between the lowered call's final comma and
// its closing paren, so argument ORDER can be asserted rather than mere presence.
func lastSpanArgument(t *testing.T, output string) string {
	t.Helper()
	index := strings.Index(output, "ctx.span1(")
	if index < 0 {
		t.Fatalf("no lowered span1 call in output\n%s", output)
	}
	tail := output[index:]
	end := strings.Index(tail, ";")
	if end < 0 {
		t.Fatalf("lowered call was not terminated\n%s", output)
	}
	statement := tail[:end]
	comma := strings.LastIndex(statement, ",")
	if comma < 0 {
		t.Fatalf("lowered call had no trailing argument\n%s", output)
	}
	return strings.TrimSuffix(strings.TrimSpace(statement[comma+1:]), ")")
}

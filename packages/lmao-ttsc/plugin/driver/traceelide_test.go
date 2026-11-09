package lmao

import (
	"strings"
	"testing"
)

// traceStatementsFixture holds a trace statement in every statement position:
// a list, a parenthesized chain, both if branches, a loop body and a nested
// closure, with argument expressions whose evaluation the elision must remove.
const traceStatementsFixture = `
declare function traceReason(): string;
declare function traceMessage(): string;
declare const flag: boolean;
declare const items: string[];
defineOp('trace-statements', (ctx) => {
  ctx.log.trace('cache warm no-op').jobId(traceReason());
  ctx.log.trace(traceMessage());
  (ctx.log.trace('parenthesized no-op'));
  if (flag) ctx.log.trace('branch no-op');
  else ctx.log.trace('else no-op');
  for (const item of items) ctx.log.trace('loop no-op').jobId(item);
  items.forEach((item) => { ctx.log.trace('closure no-op').jobId(item); });
  ctx.log.info('kept info');
  return ctx.ok(null);
});
`

var traceNoOpTemplates = []string{"cache warm no-op", "parenthesized no-op", "branch no-op", "else no-op", "loop no-op", "closure no-op"}

func catalogHasText(catalog vocabularyCatalog, text string) bool {
	for _, entry := range catalog.Entries {
		if entry.Text == text {
			return true
		}
	}
	return false
}

func TestProductionCompileElidesTraceStatementsWithTheirArguments(t *testing.T) {
	result := runTemplateFixtureWithTrace(t, traceStatementsFixture, traceEntriesElided)
	if result.err != nil {
		t.Fatalf("production compile failed: %v", result.err)
	}
	for _, text := range append([]string{"traceReason())", "traceMessage())", ".trace(", "_traceTemplate", "_appendWriterEntry(6)"}, traceNoOpTemplates...) {
		if strings.Contains(result.output, text) {
			t.Fatalf("production output still holds %q\n%s", text, result.output)
		}
	}
	for _, text := range traceNoOpTemplates {
		if catalogHasText(result.catalog, text) {
			t.Fatalf("production compile registered the elided template %q", text)
		}
	}
	if !catalogHasText(result.catalog, "kept info") {
		t.Fatalf("production compile dropped the info template: %+v", result.catalog.Entries)
	}
	// The user code around an elided statement still runs.
	for _, text := range []string{"if (flag)", "for (const item of items)", "items.forEach("} {
		if !strings.Contains(result.output, text) {
			t.Fatalf("production output lost the code around a trace statement, %q\n%s", text, result.output)
		}
	}
}

func TestTestCompileKeepsTraceStatementsAndEvaluatesTheirArgumentsOnce(t *testing.T) {
	result := runTemplateFixtureWithTrace(t, traceStatementsFixture, traceEntriesKept)
	if result.err != nil {
		t.Fatalf("test compile failed: %v", result.err)
	}
	for _, call := range []string{"traceReason())", "traceMessage())"} {
		if strings.Count(result.output, call) != 1 {
			t.Fatalf("test output evaluates %q %d times, want once\n%s", call, strings.Count(result.output, call), result.output)
		}
	}
	for _, text := range traceNoOpTemplates {
		if !catalogHasText(result.catalog, text) {
			t.Fatalf("test compile did not register the trace template %q: %+v", text, result.catalog.Entries)
		}
	}
}

func TestTraceEntryOutsideItsOwnStatementIsRejectedInEveryMode(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{name: "initializer", body: "defineOp('x', ctx => { const entry = ctx.log.trace('x'); return ctx.ok(entry); });"},
		{name: "consumed setter result", body: "defineOp('x', ctx => { const entry = ctx.log.trace('x').jobId('j'); return ctx.ok(entry); });"},
		{name: "argument", body: "declare function consume(value: unknown): void; defineOp('x', ctx => { consume(ctx.log.trace('x')); return ctx.ok(null); });"},
		{name: "concise arrow body", body: "defineOp('x', ctx => { [1].forEach(() => ctx.log.trace('x')); return ctx.ok(null); });"},
		{name: "void operand", body: "defineOp('x', ctx => { void ctx.log.trace('x'); return ctx.ok(null); });"},
		{name: "returned", body: "defineOp('x', ctx => { return ctx.log.trace('x'); });"},
		{name: "uncalled method", body: "defineOp('x', ctx => { const trace = ctx.log.trace; return ctx.ok(trace); });"},
	}
	for _, test := range tests {
		for _, mode := range []traceEntries{traceEntriesElided, traceEntriesKept} {
			t.Run(test.name, func(t *testing.T) {
				result := runTemplateFixtureWithTrace(t, test.body, mode)
				codes := diagnosticCodes(result.err)
				if len(codes) != 1 || codes[0] != "LMAO_TRACE_NOT_STATEMENT" {
					t.Fatalf("trace mode %d: diagnostic codes = %q, want exactly [LMAO_TRACE_NOT_STATEMENT]; error = %v", mode, codes, result.err)
				}
				if result.output != "" {
					t.Fatalf("trace mode %d: rejected source was emitted:\n%s", mode, result.output)
				}
			})
		}
	}
}

func TestProductionCompileRejectsTheTracePolicyViolationsTheTestCompileRejects(t *testing.T) {
	body := "declare const text: string; defineOp('x', ctx => { ctx.log.trace('prefix ' + text); return ctx.ok(null); });"
	for _, mode := range []traceEntries{traceEntriesElided, traceEntriesKept} {
		codes := diagnosticCodes(runTemplateFixtureWithTrace(t, body, mode).err)
		if len(codes) != 1 || codes[0] != "LMAO_AVOIDABLE_INTERPOLATION" {
			t.Fatalf("trace mode %d: diagnostic codes = %q, want exactly [LMAO_AVOIDABLE_INTERPOLATION]", mode, codes)
		}
	}
}

func TestNodeEnvTestAloneKeepsTraceStatements(t *testing.T) {
	for nodeEnv, want := range map[string]traceEntries{
		"test":        traceEntriesKept,
		"":            traceEntriesElided,
		"production":  traceEntriesElided,
		"development": traceEntriesElided,
		"Test":        traceEntriesElided,
	} {
		if got := traceEntriesFor(nodeEnv); got != want {
			t.Fatalf("traceEntriesFor(%q) = %d, want %d", nodeEnv, got, want)
		}
	}
}

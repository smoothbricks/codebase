package lmao

import (
	"regexp"
	"strings"
	"testing"
)

func TestBlockedPorcelainAndSpanNameStayAnalyzed(t *testing.T) {
	output := transformTemplateFixture(t, `
declare const gate: { blocked(): number };
export const gateState = gate.blocked();
defineOp('blocked-bare', (ctx) => {
  return ctx.blocked();
});
defineOp('blocked-span-name', (ctx) => {
  ctx.tag.alpha(ctx.spanName);
  return ctx.blocked({ maxAttempts: 2 }).beta(42);
});
`)

	wantHint := runtimeHintAnalyzed | runtimeHintMessageStatic | runtimeHintResult | 2
	hints := emittedRuntimeHints(t, output)
	if len(hints) != 2 {
		t.Fatalf("blocked hints = %v, want two hints\n%s", hints, output)
	}
	for index, got := range hints {
		if got != wantHint {
			t.Errorf("hint[%d] = %#x, want analyzed result-only %#x\n%s", index, got, wantHint, output)
		}
	}
	if !strings.Contains(output, "beta_values") {
		t.Errorf("ctx.blocked() chain did not inline its row-1 beta write\n%s", output)
	}
	if !strings.Contains(output, "ctx.spanName") {
		t.Errorf("ctx.spanName read was not preserved\n%s", output)
	}
	if strings.Contains(output, "gate.blocked().line(") {
		t.Errorf("a non-LMAO .blocked() call received line injection\n%s", output)
	}
}

func TestBlockedLineInjectionNeedsProvenContext(t *testing.T) {
	output := transformTemplateFixture(t, `
declare const gate: { blocked(): { line(n: number): unknown } };
declare const attrs: { beta: number };
export const gateState = gate.blocked();
defineOp('blocked-bare', (ctx) => {
  return ctx.blocked();
});
defineOp('blocked-with', (ctx) => {
  return ctx.blocked().with(attrs);
});
`)

	for _, pattern := range []string{`ctx\.blocked\(\)\.line\(\d+\);`, `ctx\.blocked\(\)\.line\(\d+\)\.with\(attrs\)`} {
		if !regexp.MustCompile(pattern).MatchString(output) {
			t.Errorf("proven ctx.blocked() chain lacks its row-1 line (%s)\n%s", pattern, output)
		}
	}
	if strings.Contains(output, "gate.blocked().line(") {
		t.Errorf("a non-LMAO .blocked() call received line injection\n%s", output)
	}
}

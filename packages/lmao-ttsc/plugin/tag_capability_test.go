package main

import (
	"strings"
	"testing"
)

func TestInlineableTagChainsClearCapabilityAndKeepDirectWrites(t *testing.T) {
	output := transformTemplateFixture(t, `
declare function value(label: string): string;
defineOp('single-setter', (ctx) => {
  ctx.tag.alpha(value('single'));
  return ctx.ok(null);
});
defineOp('chained-setters', (ctx) => {
  ctx.tag.alpha(value('chain alpha')).beta(42);
  return ctx.ok(null);
});
defineOp('literal-with', (ctx) => {
  ctx.tag.with({ alpha: value('with alpha'), beta: 7 });
  return ctx.ok(null);
});
`)

	wantHint := runtimeHintAnalyzed | runtimeHintMessageStatic | runtimeHintResult | 2
	hints := emittedRuntimeHints(t, output)
	if len(hints) != 3 {
		t.Fatalf("inlineable tag hints = %v, want three hints\n%s", hints, output)
	}
	for index, got := range hints {
		if got != wantHint {
			t.Errorf("inlineable tag hint %d = %#08x, want TAG-free %#08x", index, got, wantHint)
		}
	}

	for _, expression := range []string{"value('single')", "value('chain alpha')", "value('with alpha')"} {
		if count := strings.Count(output, expression); count != 1 {
			t.Errorf("%s evaluations = %d, want exactly one after direct-write lowering\n%s", expression, count, output)
		}
	}
	for _, lane := range []struct {
		name  string
		count int
	}{{"alpha_values", 3}, {"beta_values", 2}} {
		if count := strings.Count(output, lane.name); count != lane.count {
			t.Errorf("direct row-0 lane %q writes = %d, want %d\n%s", lane.name, count, lane.count, output)
		}
	}
	if strings.Contains(output, "ctx.tag") {
		t.Errorf("TAG-free lowering retained a runtime tag lookup\n%s", output)
	}
}

func TestResidualTagUsesConservativelyRetainCapability(t *testing.T) {
	output := transformTemplateFixture(t, `
declare function value(label: string): string;
declare function consume(value: unknown): void;
const nonliteralAttributes = { alpha: value('nonliteral initializer') };
defineOp('nonliteral-with', (ctx) => {
  ctx.tag.with(nonliteralAttributes);
  return ctx.ok(null);
});
defineOp('tag-call-as-value', (ctx) => {
  const writer = ctx.tag.alpha(value('value use'));
  consume(writer);
  return ctx.ok(null);
});
defineOp('tag-alias', (ctx) => {
  const writer = ctx.tag;
  writer.alpha(value('alias'));
  return ctx.ok(null);
});
defineOp('tag-destructure', (ctx) => {
  const { tag } = ctx;
  tag.alpha(value('destructure'));
  return ctx.ok(null);
});
defineOp('unproven-receiver', (ctx) => {
  const receiver = { tag: ctx.tag };
  receiver.tag.alpha(value('unproven'));
  return ctx.ok(null);
});
defineOp('bailed-chain', (ctx) => {
  ctx.tag.alpha(value('before bail')).beta();
  return ctx.ok(null);
});
`)

	analyzedTagHint := runtimeHintAnalyzed | runtimeHintMessageStatic | runtimeHintTag | runtimeHintResult | 2
	want := []uint32{analyzedTagHint, analyzedTagHint, 0, 0, 0, analyzedTagHint}
	hints := emittedRuntimeHints(t, output)
	if len(hints) != len(want) {
		t.Fatalf("residual tag hints = %v, want %v\n%s", hints, want, output)
	}
	for index, expected := range want {
		if got := hints[index]; got != expected {
			t.Errorf("residual tag hint %d = %#08x, want conservative %#08x", index, got, expected)
		}
		if got := hints[index]; got&runtimeHintAnalyzed != 0 && got&runtimeHintTag == 0 {
			t.Errorf("residual tag hint %d incorrectly analyzed without TAG: %#08x", index, got)
		}
	}

	for _, expression := range []string{
		"value('nonliteral initializer')",
		"value('value use')",
		"value('alias')",
		"value('destructure')",
		"value('unproven')",
		"value('before bail')",
		"consume(writer)",
	} {
		if count := strings.Count(output, expression); count != 1 {
			t.Errorf("residual expression %s occurrences = %d, want exactly one\n%s", expression, count, output)
		}
	}
}

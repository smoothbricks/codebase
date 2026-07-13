package main

import (
	"regexp"
	"strconv"
	"strings"
	"testing"
)

const (
	messageLayoutStaticOnly uint32 = 0x01000000
	messageLayoutDynamicOnly uint32 = 0x02000000
	messageLayoutMixed      uint32 = 0x03000000
	messageLayoutMask       uint32 = 0x03000000
)

func emittedRuntimeHints(t *testing.T, output string) []uint32 {
	t.Helper()
	matches := regexp.MustCompile(`runtimeHint:\s*(\d+)`).FindAllStringSubmatch(output, -1)
	hints := make([]uint32, 0, len(matches))
	for _, match := range matches {
		value, err := strconv.ParseUint(match[1], 10, 32)
		if err != nil {
			t.Fatalf("parse emitted runtime hint %q: %v", match[1], err)
		}
		hints = append(hints, uint32(value))
	}
	return hints
}

func TestMessageLayoutFamilyHintsClassifyWholeOpBodies(t *testing.T) {
	output := transformTemplateFixture(t, `
declare const dynamicOnlyText: () => string;
declare const mixedText: () => string;
declare const dynamicFlagName: () => string;
declare function consume(value: unknown): void;
defineOp('static-only', (ctx) => {
  ctx.log.info('static literal');
  return ctx.ok(null);
});
defineOp('structured-static', (ctx) => {
  ctx.log.warn('job {jobId}', { jobId: 'job-1' });
  return ctx.ok(null);
});
defineOp('dynamic-only', (ctx) => {
  ctx.log.debug(dynamicOnlyText());
  return ctx.ok(null);
});
defineOp('dynamic-ff', (ctx) => {
  ctx.ff(dynamicFlagName());
  return ctx.ok(null);
});
defineOp('mixed', (ctx) => {
  ctx.log.info('static in mixed');
  ctx.log.trace(mixedText());
  return ctx.ok(null);
});
defineOp('no-message-rows', (ctx) => ctx.ok(null));
defineOp('unanalyzed-bailout', (ctx) => {
  consume(ctx);
  return null;
});
`)

	want := []uint32{
		runtimeHintAnalyzed | messageLayoutStaticOnly | runtimeHintLog | runtimeHintResult | 3,
		runtimeHintAnalyzed | messageLayoutStaticOnly | runtimeHintLog | runtimeHintResult | 3,
		runtimeHintAnalyzed | messageLayoutDynamicOnly | runtimeHintLog | runtimeHintResult | 3,
		runtimeHintAnalyzed | messageLayoutDynamicOnly | runtimeHintFF | runtimeHintResult | 2,
		runtimeHintAnalyzed | messageLayoutMixed | runtimeHintLog | runtimeHintResult | 4,
		runtimeHintAnalyzed | messageLayoutStaticOnly | runtimeHintResult | 2,
		0,
	}
	got := emittedRuntimeHints(t, output)
	if len(got) != len(want) {
		t.Fatalf("emitted runtime hints = %v, want %v\n%s", got, want, output)
	}
	for index := range want {
		if got[index] != want[index] {
			t.Errorf("runtime hint %d = %#08x (family %#08x), want %#08x (family %#08x)", index, got[index], got[index]&messageLayoutMask, want[index], want[index]&messageLayoutMask)
		}
	}

	for _, once := range []string{"dynamicOnlyText()", "mixedText()", "dynamicFlagName()", "consume(ctx)"} {
		if count := strings.Count(output, once); count != 1 {
			t.Errorf("%s evaluation occurrences = %d, want exactly one\n%s", once, count, output)
		}
	}
}

func TestMessageLayoutFamilyHintBitsMatchRuntimeABI(t *testing.T) {
	if runtimeHintMessageStatic != messageLayoutStaticOnly || runtimeHintMessageDynamic != messageLayoutDynamicOnly || runtimeHintMessageMixed != messageLayoutMixed {
		t.Fatalf(
			"compiler/runtime message family ABI diverged: production static=%#x dynamic=%#x mixed=%#x; contract static=%#x dynamic=%#x mixed=%#x",
			runtimeHintMessageStatic,
			runtimeHintMessageDynamic,
			runtimeHintMessageMixed,
			messageLayoutStaticOnly,
			messageLayoutDynamicOnly,
			messageLayoutMixed,
		)
	}
	if runtimeHintMessageMixed != runtimeHintMessageStatic|runtimeHintMessageDynamic {
		t.Fatalf("mixed encoding = %#x, want static|dynamic = %#x", runtimeHintMessageMixed, runtimeHintMessageStatic|runtimeHintMessageDynamic)
	}
	if runtimeHintMessageMixed != messageLayoutMask {
		t.Fatalf("mixed encoding = %#x, want family mask %#x", runtimeHintMessageMixed, messageLayoutMask)
	}
}

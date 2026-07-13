package main

import (
	"regexp"
	"strconv"
	"strings"
	"testing"
)

const (
	messageLayoutStaticOnly    uint32 = 0x01000000
	messageLayoutDynamicOnly   uint32 = 0x02000000
	messageLayoutMixed         uint32 = 0x03000000
	messageLayoutMask          uint32 = 0x03000000
	messagePhysicalPacked      uint32 = 0x04000000
	messagePhysicalSpecialized uint32 = 0x08000000
	messagePhysicalMask        uint32 = 0x0c000000
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
		runtimeHintAnalyzed | messageLayoutDynamicOnly | runtimeHintFF | runtimeHintResult | 3,
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

func TestCompilerCapacityIsTwoReservedRowsPlusStaticallyKnownLogRows(t *testing.T) {
	output := transformTemplateFixture(t, `
defineOp('reserved-only', (ctx) => ctx.ok(null));
defineOp('one-static-log', (ctx) => {
  ctx.log.info('one');
  return ctx.ok(null);
});
defineOp('three-static-logs', (ctx) => {
  ctx.log.info('one');
  ctx.log.warn('two');
  ctx.log.error('three');
  return ctx.ok(null);
});
`)

	wantCapacities := []uint32{2, 3, 5}
	hints := emittedRuntimeHints(t, output)
	if len(hints) != len(wantCapacities) {
		t.Fatalf("capacity hints = %v, want %d hints\n%s", hints, len(wantCapacities), output)
	}
	for index, want := range wantCapacities {
		if got := hints[index] & 0xffff; got != want {
			t.Errorf("op %d capacity = %d, want two reserved rows plus logs = %d", index, got, want)
		}
	}
}

func TestCompilerCapacityBeyondUint16FallsBackToAdaptive(t *testing.T) {
	var source strings.Builder
	source.WriteString("defineOp('oversized-static-body', (ctx) => {\n")
	for range 0xfffe {
		source.WriteString("ctx.log.info('same static row');\n")
	}
	source.WriteString("return ctx.ok(null);\n});\n")

	hints := emittedRuntimeHints(t, transformTemplateFixture(t, source.String()))
	if len(hints) != 1 {
		t.Fatalf("oversized body hints = %v, want one", hints)
	}
	if capacity := hints[0] & 0xffff; capacity != 0 {
		t.Fatalf("oversized body capacity = %d, want adaptive zero", capacity)
	}
	if hints[0]&runtimeHintAnalyzed == 0 || hints[0]&runtimeHintLog == 0 || hints[0]&runtimeHintResult == 0 {
		t.Fatalf("oversized body lost safe capability analysis: %#x", hints[0])
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

func TestMessagePhysicalLayoutSelectorExactMatrix(t *testing.T) {
	testCases := []struct {
		name        string
		capacity    uint32
		staticRows  uint32
		dynamicRows uint32
		want        uint32
	}{
		{"capacity-8-static-0", 8, 0, 6, 0},
		{"capacity-8-static-25", 8, 1, 5, 0},
		{"capacity-8-static-50", 8, 3, 3, 0},
		{"capacity-8-static-75", 8, 5, 1, 0},
		{"capacity-8-static-100", 8, 6, 0, 0},
		{"capacity-64-static-0", 64, 0, 62, 0},
		{"capacity-64-static-25", 64, 15, 47, 0},
		{"capacity-64-static-50", 64, 31, 31, messagePhysicalSpecialized},
		{"capacity-64-static-75", 64, 46, 16, 0},
		{"capacity-64-static-100", 64, 62, 0, 0},
		{"capacity-1024-static-0", 1024, 0, 1022, 0},
		{"capacity-1024-static-25", 1024, 255, 767, 0},
		{"capacity-1024-static-50", 1024, 511, 511, 0},
		{"capacity-1024-static-75", 1024, 766, 256, 0},
		{"capacity-1024-static-100", 1024, 1022, 0, 0},
	}

	checksum := uint32(0)
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			got := selectMessagePhysicalLayout(testCase.capacity, testCase.staticRows, testCase.dynamicRows, 1)
			if got != testCase.want {
				t.Errorf("selectMessagePhysicalLayout(%d, %d, %d) = %#x, want %#x", testCase.capacity, testCase.staticRows, testCase.dynamicRows, got, testCase.want)
			}
		})
		checksum = (checksum << 2) | (testCase.want >> 26)
	}
	if checksum != 0x8000 {
		t.Fatalf("selector matrix checksum = %#x, want 0x8000", checksum)
	}
}

func TestMessagePhysicalLayoutSelectorFallsBackForUnsafeInputs(t *testing.T) {
	testCases := []struct {
		name           string
		capacity       uint32
		staticRows     uint32
		dynamicRows    uint32
		vocabularySize int
	}{
		{"adaptive-capacity", 0, 0, 0, 1},
		{"reserved-two-rows-only", 2, 0, 0, 1},
		{"row-count-under-capacity", 64, 30, 31, 1},
		{"row-count-over-capacity", 64, 32, 31, 1},
		{"unknown-capacity-tier", 63, 31, 30, 1},
		{"oversized-capacity", 0x10000, 0x7fff, 0x7fff, 1},
		{"dense-index-beyond-max", 64, 31, 31, int(maxPackedDenseIndex) + 2},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			if got := selectMessagePhysicalLayout(testCase.capacity, testCase.staticRows, testCase.dynamicRows, testCase.vocabularySize); got != 0 {
				t.Errorf("unsafe selector result = %#x, want split", got)
			}
		})
	}
}

func TestMessagePhysicalLayoutHintBitsMatchRuntimeABI(t *testing.T) {
	if runtimeHintMessagePhysicalPacked != messagePhysicalPacked {
		t.Fatalf("compiler packed bit = %#x, want bit 26 %#x", runtimeHintMessagePhysicalPacked, messagePhysicalPacked)
	}
	if runtimeHintMessagePhysicalSpecialized != messagePhysicalSpecialized {
		t.Fatalf("compiler specialized bit = %#x, want bit 27 %#x", runtimeHintMessagePhysicalSpecialized, messagePhysicalSpecialized)
	}
	if runtimeHintMessagePhysicalPacked|runtimeHintMessagePhysicalSpecialized != messagePhysicalMask {
		t.Fatalf("compiler physical mask = %#x, want %#x", runtimeHintMessagePhysicalPacked|runtimeHintMessagePhysicalSpecialized, messagePhysicalMask)
	}
	if messagePhysicalMask&messageLayoutMask != 0 {
		t.Fatalf("physical mask %#x overlaps family mask %#x", messagePhysicalMask, messageLayoutMask)
	}
}

func TestMessagePhysicalLayoutLoopAndUnsafeBodiesStayCurrent(t *testing.T) {
	output := transformTemplateFixture(t, `
declare const values: readonly string[];
declare function consume(value: unknown): void;
defineOp('loop-adaptive', (ctx) => {
  for (const value of values) ctx.log.debug(value);
  return ctx.ok(null);
});
defineOp('unsafe-unknown', (ctx) => {
  consume(ctx);
  return null;
});
`)
	hints := emittedRuntimeHints(t, output)
	want := []uint32{
		runtimeHintAnalyzed | runtimeHintMessageDynamic | runtimeHintLog | runtimeHintResult,
		0,
	}
	if len(hints) != len(want) {
		t.Fatalf("fallback hints = %v, want %v\n%s", hints, want, output)
	}
	for index := range want {
		if hints[index] != want[index] {
			t.Errorf("fallback hint %d = %#x, want %#x", index, hints[index], want[index])
		}
		if hints[index]&messagePhysicalMask != 0 {
			t.Errorf("fallback hint %d unexpectedly selected a specialized physical mode: %#x", index, hints[index])
		}
	}
}

func TestCurrentMessageDictionaryIsFrozenDeduplicatedAndUsesNonzeroLocalIDs(t *testing.T) {
	output := transformTemplateFixture(t, `
defineOp('current-local-dictionary', (ctx) => {
  ctx.log.info('repeated literal');
  ctx.log.debug('repeated literal');
  ctx.log.warn('distinct literal');
  return ctx.ok(null);
});
`)
	if !strings.Contains(output, "localMessageDictionary: Object.freeze([") {
		t.Fatalf("current compile metadata omitted its frozen local dictionary\n%s", output)
	}
	if count := strings.Count(output, "_messageIds[$$i] = 1"); count != 2 {
		t.Fatalf("repeated current literal local-ID writes = %d, want 2\n%s", count, output)
	}
	if count := strings.Count(output, "_messageIds[$$i] = 2"); count != 1 {
		t.Fatalf("distinct current literal local-ID writes = %d, want 1\n%s", count, output)
	}
	if strings.Contains(output, "message_nulls") || strings.Contains(output, "message_values") {
		t.Fatalf("static-only current lowering emitted raw message storage\n%s", output)
	}
	if strings.Contains(output, "_messageDictionary") {
		t.Fatalf("compiler emitted a forbidden per-buffer dictionary field\n%s", output)
	}
}

func TestMixedCurrentStaticWriteStoresIdentityOnly(t *testing.T) {
	output := transformTemplateFixture(t, `
defineOp('current-mixed-sidecar', (ctx) => {
  ctx.log.info('static literal');
  ctx.log.debug(String(Date.now()));
  return ctx.ok(null);
});
`)
	if !strings.Contains(output, "_messageIds[$$i] = 1") {
		t.Fatalf("mixed current static write omitted its local identity\n%s", output)
	}
	if strings.Contains(output, "message_values[$$i] = undefined") || strings.Contains(output, "message_nulls") {
		t.Fatalf("mixed current static write emitted a redundant raw-sidecar clear or validity write\n%s", output)
	}
}

package main

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

var (
	eagerColumnsPropertyPattern = regexp.MustCompile(`eagerColumns:\s*\[([^]]*)\]`)
	eagerColumnNamePattern      = regexp.MustCompile(`"(?:[^"\\]|\\.)*"`)
)

func emittedEagerColumns(t *testing.T, output string) [][]string {
	t.Helper()
	matches := eagerColumnsPropertyPattern.FindAllStringSubmatch(output, -1)
	columns := make([][]string, 0, len(matches))
	for _, match := range matches {
		names := eagerColumnNamePattern.FindAllString(match[1], -1)
		decoded := make([]string, 0, len(names))
		for _, name := range names {
			value, err := strconv.Unquote(name)
			if err != nil {
				t.Fatalf("decode eager column name %q: %v", name, err)
			}
			decoded = append(decoded, value)
		}
		columns = append(columns, decoded)
	}
	return columns
}

func requireEagerColumns(t *testing.T, output string, want []string) {
	t.Helper()
	got := emittedEagerColumns(t, output)
	if len(got) != 1 {
		t.Fatalf("emitted eager descriptors = %v, want exactly [%v]\n%s", got, want, output)
	}
	if strings.Join(got[0], "\x00") != strings.Join(want, "\x00") {
		t.Fatalf("emitted eager columns = %v, want %v\n%s", got[0], want, output)
	}
}

func TestCompilerProvesUnconditionalEagerColumnsAcrossRowKinds(t *testing.T) {
	output := transformTemplateFixture(t, `
declare function value(label: string): string;
defineOp('eager-row-kinds', (ctx) => {
  ctx.tag.tagField(value('tag')).with({ withField: value('with'), duplicate: value('tag duplicate') });
  ctx.log.info('{alpha}', { alpha: value('log') }).duplicate(value('log duplicate'));
  return ctx.ok(null).resultField(value('result')).duplicate(value('result duplicate'));
});
`)

	requireEagerColumns(t, output, []string{"alpha", "duplicate", "resultField", "tagField", "withField"})
	for _, expression := range []string{
		"value('tag')",
		"value('with')",
		"value('tag duplicate')",
		"value('log')",
		"value('log duplicate')",
		"value('result')",
		"value('result duplicate')",
	} {
		if count := strings.Count(output, expression); count != 1 {
			t.Fatalf("%s evaluations = %d, want exactly one\n%s", expression, count, output)
		}
	}
	for _, lane := range []string{"alpha_values", "resultField_values", "tagField_values", "withField_values"} {
		if !strings.Contains(output, lane) {
			t.Fatalf("lowered row writers omitted proven lane %q\n%s", lane, output)
		}
	}
}

func TestCompilerEagerProofIsConservativeAcrossControlFlowAndBailouts(t *testing.T) {
	output := transformTemplateFixture(t, `
declare const condition: boolean;
declare function value(label: string): string;
declare function consume(value: unknown): void;
defineOp('conditional', (ctx) => {
  if (condition) ctx.tag.conditional(value('conditional'));
  return ctx.ok(null);
});
defineOp('early-return', (ctx) => {
  if (condition) return ctx.ok(null);
  ctx.tag.conditional(value('after return'));
  return ctx.ok(null);
});
defineOp('throw-path', (ctx) => {
  if (condition) throw new Error('stop');
  ctx.tag.conditional(value('after throw'));
  return ctx.ok(null);
});
defineOp('retry-loop', (ctx) => {
  for (let retry = 0; retry < 2; retry++) ctx.tag.conditional(value('retry'));
  return ctx.ok(null);
});
defineOp('ff-conditional', (ctx) => {
  if (ctx.ff('enabled')) ctx.tag.ffConditional(value('ff one branch'));
  return ctx.ok(null);
});
defineOp('ff-both-paths', (ctx) => {
  if (ctx.ff('enabled')) ctx.tag.ffBoth(value('ff true'));
  else ctx.tag.ffBoth(value('ff false'));
  return ctx.ok(null);
});
defineOp('unanalyzed-bailout', (ctx) => {
  ctx.tag.conditional(value('before bailout'));
  consume(ctx);
  return ctx.ok(null);
});
`)

	requireEagerColumns(t, output, []string{"ffBoth"})
	for _, expression := range []string{
		"value('conditional')",
		"value('after return')",
		"value('after throw')",
		"value('retry')",
		"value('ff one branch')",
		"value('ff true')",
		"value('ff false')",
		"value('before bailout')",
		"consume(ctx)",
	} {
		if count := strings.Count(output, expression); count != 1 {
			t.Fatalf("%s evaluations = %d, want exactly one\n%s", expression, count, output)
		}
	}
}

func TestCompilerEmitsPerMemberEagerColumnsForDefineOps(t *testing.T) {
	output := transformTemplateFixture(t, `
declare const condition: boolean;
declare function value(label: string): string;
defineOps({
  method(ctx) {
    ctx.tag.beta(1).alpha(value('method'));
    return ctx.ok(null);
  },
  property: (ctx) => {
    ctx.tag.with({ withField: value('property') });
    return ctx.ok(null);
  },
  conditional(ctx) {
    if (condition) ctx.tag.conditional(value('conditional member'));
    return ctx.ok(null);
  },
});
`)

	got := emittedEagerColumns(t, output)
	want := [][]string{{"alpha", "beta"}, {"withField"}}
	if len(got) != len(want) {
		t.Fatalf("defineOps eager descriptors = %v, want %v\n%s", got, want, output)
	}
	for index := range want {
		if strings.Join(got[index], "\x00") != strings.Join(want[index], "\x00") {
			t.Fatalf("defineOps eager descriptor %d = %v, want %v\n%s", index, got[index], want[index], output)
		}
	}
	for _, expression := range []string{"value('method')", "value('property')", "value('conditional member')"} {
		if count := strings.Count(output, expression); count != 1 {
			t.Fatalf("%s evaluations = %d, want exactly one\n%s", expression, count, output)
		}
	}
}

func TestCompilerEagerColumnsAreSortedDuplicateFreeAndUnboundedByOneWord(t *testing.T) {
	var body strings.Builder
	body.WriteString("declare function numberValue(label: string): number;\n")
	body.WriteString("defineOp('more-than-thirty-two', (ctx) => {\n  ctx.tag")
	for index := 39; index >= 0; index-- {
		fmt.Fprintf(&body, ".f%02d(numberValue('f%02d'))", index, index)
	}
	body.WriteString(".f32(numberValue('f32 duplicate'));\n  return ctx.ok(null);\n});\n")

	output := transformTemplateFixture(t, body.String())
	want := make([]string, 40)
	for index := range want {
		want[index] = fmt.Sprintf("f%02d", index)
	}
	requireEagerColumns(t, output, want)
	for _, name := range want {
		expression := "numberValue('" + name + "')"
		if count := strings.Count(output, expression); count != 1 {
			t.Fatalf("%s evaluations = %d, want exactly one\n%s", expression, count, output)
		}
	}
	if count := strings.Count(output, "numberValue('f32 duplicate')"); count != 1 {
		t.Fatalf("duplicate write expression evaluations = %d, want exactly one\n%s", count, output)
	}
}

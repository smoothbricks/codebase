package main

import (
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"

	shimast "github.com/microsoft/typescript-go/shim/ast"
	shimprinter "github.com/microsoft/typescript-go/shim/printer"
	"github.com/samchon/ttsc/packages/ttsc/driver"
)

const templateFixtureDeclarations = `
export interface OpCompileMetadata {
  readonly runtimeHint: number;
  readonly eagerColumns?: readonly string[];
}
export interface UserAttributeFields {
  alpha: string;
  beta: number;
  conditional: string;
  duplicate: string;
  ffBoth: string;
  ffConditional: string;
  resultField: string;
  tagField: string;
  withField: string;
  f00: number; f01: number; f02: number; f03: number; f04: number; f05: number; f06: number; f07: number;
  f08: number; f09: number; f10: number; f11: number; f12: number; f13: number; f14: number; f15: number;
  f16: number; f17: number; f18: number; f19: number; f20: number; f21: number; f22: number; f23: number;
  f24: number; f25: number; f26: number; f27: number; f28: number; f29: number; f30: number; f31: number;
  f32: number; f33: number; f34: number; f35: number; f36: number; f37: number; f38: number; f39: number;
  jobId: string;
  elapsedMs: number;
  attempt: number;
  success: boolean;
  operation: 'READ' | 'WRITE';
}
export type FluentLogEntry<T extends Record<string, unknown> = UserAttributeFields> = { line(value: number): FluentLogEntry<T> } & { [K in keyof T]: (value: T[K]) => FluentLogEntry<T> };
export type GeneratedTagWriter<T extends Record<string, unknown> = UserAttributeFields> = { with(values: Partial<T>): GeneratedTagWriter<T> } & { [K in keyof T]: (value: T[K]) => GeneratedTagWriter<T> };
export type FluentResult<T extends Record<string, unknown> = UserAttributeFields> = {
  readonly _buffer?: unknown;
  line(value: number): FluentResult<T>;
  message(value: string): FluentResult<T>;
  with(values: Partial<T>): FluentResult<T>;
} & { [K in keyof T]: (value: T[K]) => FluentResult<T> };
export class SpanLogger {
  info(message: string, fields?: Record<string, unknown>): FluentLogEntry;
  debug(message: string, fields?: Record<string, unknown>): FluentLogEntry;
  warn(message: string, fields?: Record<string, unknown>): FluentLogEntry;
  error(message: string, fields?: Record<string, unknown>): FluentLogEntry;
  trace(message: string, fields?: Record<string, unknown>): FluentLogEntry;
  jobId(value: string): FluentLogEntry;
  elapsedMs(value: number): FluentLogEntry;
  attempt(value: number): FluentLogEntry;
  success(value: boolean): FluentLogEntry;
}
export class SpanContext {
  readonly _buffer: { constructor: unknown; _opMetadata: unknown };
  readonly log: SpanLogger;
  readonly tag: GeneratedTagWriter;
  ok(value: unknown): FluentResult;
  err(value: unknown): FluentResult;
  ff(name: string): boolean;
  span(name: string, op: Op): Promise<unknown>;
}
export class Op {
  readonly SpanBufferClass: unknown;
  readonly remappedViewClass: unknown;
  readonly metadata: unknown;
  readonly runtimeHint: number;
  readonly fn: (ctx: SpanContext) => unknown;
}
export class OpGroup {}
export function defineOp(name: string, fn: (ctx: SpanContext) => unknown, metadata?: unknown, compileMetadata?: OpCompileMetadata): Op;
export function defineOps(definitions: Record<string, Op | ((ctx: SpanContext) => unknown)>, compileMetadataByKey?: Readonly<Record<string, OpCompileMetadata>>): OpGroup;
`

type templateFixtureResult struct {
	output string
	err    error
	manifest vocabularyManifest
	inputPath string
	source    string
}

func runTemplateFixture(t *testing.T, body string) templateFixtureResult {
	t.Helper()
	root := t.TempDir()
	inputPath := filepath.Join(root, "input.ts")
	declarationDir := filepath.Join(root, "node_modules", "@smoothbricks", "lmao")
	if err := os.MkdirAll(declarationDir, 0o755); err != nil {
		t.Fatal(err)
	}
	source := "import { defineOp, defineOps, Op, SpanContext } from '@smoothbricks/lmao';\n" + body
	files := map[string]string{
		inputPath:                                     source,
		filepath.Join(root, "tsconfig.json"):          `{"compilerOptions":{"target":"esnext","module":"esnext","moduleResolution":"node"},"files":["input.ts"]}`,
		filepath.Join(declarationDir, "index.d.ts"):   templateFixtureDeclarations,
		filepath.Join(declarationDir, "package.json"): `{"name":"@smoothbricks/lmao","types":"index.d.ts"}`,
	}
	for name, content := range files {
		if err := os.WriteFile(name, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	program, _, err := driver.LoadProgram(root, filepath.Join(root, "tsconfig.json"), driver.LoadProgramOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer program.Close()
	options := compilerOptions{
		cwd:          root,
		tsconfig:     filepath.Join(root, "tsconfig.json"),
		manifestPath: filepath.Join(root, "lmao.vocabulary.json"),
	}
	_, manifest, err := collectProgramCompilation(program, options, false)
	if err != nil {
		return templateFixtureResult{err: err, inputPath: inputPath, source: source}
	}
	manifestBytes, err := canonicalManifestBytes(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(options.manifestPath, manifestBytes, 0o644); err != nil {
		t.Fatal(err)
	}
	transform, err := lmaoPluginTransform(program, options)
	if err != nil {
		return templateFixtureResult{err: err, inputPath: inputPath, source: source}
	}
	for _, sourceFile := range program.SourceFiles() {
		if sourceFile == nil || filepath.Clean(sourceFile.FileName()) != filepath.Clean(inputPath) {
			continue
		}
		emitContext := shimprinter.NewEmitContext()
		result := transform(emitContext, sourceFile)
		if result == nil {
			t.Fatal("template fixture transform returned nil")
		}
		printer := shimprinter.NewPrinter(shimprinter.PrinterOptions{}, shimprinter.PrintHandlers{}, emitContext)
		return templateFixtureResult{output: shimprinter.EmitSourceFile(printer, result), manifest: manifest, inputPath: inputPath, source: source}
	}
	t.Fatal("template fixture input source was not loaded")
	return templateFixtureResult{}
}

func transformTemplateFixture(t *testing.T, body string) string {
	t.Helper()
	result := runTemplateFixture(t, body)
	if result.err != nil {
		t.Fatalf("template fixture transform failed: %v", result.err)
	}
	return result.output
}

func collectNodeText(root *shimast.Node, kind shimast.Kind) []string {
	var values []string
	var visit func(*shimast.Node)
	visit = func(node *shimast.Node) {
		if node == nil {
			return
		}
		if node.Kind == kind {
			values = append(values, shimast.NodeText(node))
		}
		node.ForEachChild(func(child *shimast.Node) bool {
			visit(child)
			return false
		})
	}
	visit(root)
	return values
}

func containsText(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func containsTemplateIDLane(values []string) bool {
	for _, value := range values {
		if strings.HasSuffix(value, "TemplateIds") {
			return true
		}
	}
	return false
}

func emittedLogBlock(level string, templateID globalVocabularyID) *shimast.Node {
	list := factory.NewNodeList([]*shimast.Node{factory.NewExpressionStatement(ident("placeholder"))})
	transformer := &fileTransformer{}
	if templateID != 0 {
		transformer.vocabularyBinding = ident("vocabularyBinding")
		transformer.vocabularyOrdinals = map[globalVocabularyID]int{templateID: 4}
	}
	transformer.applyLogInlines([]logInline{{
		list:       list,
		index:      0,
		logExpr:    propAccess(ident("ctx"), "log"),
		level:      level,
		message:    callExpr(ident("dynamicMessage"), nil),
		templateID: templateID,
	}})
	return list.Nodes[0]
}

func TestCompileMetadataExcludesRemovedPerOpTemplateTable(t *testing.T) {
	node := compileMetadataNode(opCompileAnalysis{runtimeHint: 123})
	if strings := collectNodeText(node, shimast.KindStringLiteral); len(strings) != 0 {
		t.Fatalf("compile metadata retained obsolete per-Op template strings: %q", strings)
	}
	identifiers := collectNodeText(node, shimast.KindIdentifier)
	if len(identifiers) != 1 || identifiers[0] != "runtimeHint" {
		t.Fatalf("compile metadata fields = %q, want runtimeHint only", identifiers)
	}
	numbers := collectNodeText(node, shimast.KindNumericLiteral)
	if !containsText(numbers, "123") {
		t.Fatalf("compile metadata numeric values = %q, want runtimeHint 123", numbers)
	}
}

func TestLiteralLogInlineUsesRegisteredHeaderOperandForEveryLevel(t *testing.T) {
	entryTypes := map[string]string{
		"info":  "8",
		"debug": "7",
		"warn":  "9",
		"error": "10",
		"trace": "6",
	}
	for level, entryType := range entryTypes {
		t.Run(level, func(t *testing.T) {
			block := emittedLogBlock(level, 37)
			identifiers := collectNodeText(block, shimast.KindIdentifier)
			if !containsText(identifiers, "_logHeaders") || !containsText(identifiers, "vocabularyBinding") {
				t.Fatalf("%s literal inline does not pack the registered binding operand into _logHeaders: %q", level, identifiers)
			}
			if containsTemplateIDLane(identifiers) || containsText(identifiers, "message_values") || containsText(identifiers, "message_nulls") {
				t.Fatalf("%s literal inline writes a separate template-ID lane or dynamic message storage: %q", level, identifiers)
			}
			numbers := collectNodeText(block, shimast.KindNumericLiteral)
			if !containsText(numbers, "4") || containsText(numbers, "37") {
				t.Fatalf("%s literal inline numeric values = %q, want fragment ordinal 4 and no global ID 37", level, numbers)
			}
			if !containsText(numbers, entryType) {
				t.Fatalf("%s literal inline numeric values = %q, want entry type %s", level, numbers, entryType)
			}
		})
	}
}

func TestDynamicLogInlineKeepsSingleMessageEvaluationAndSentinelLaneClear(t *testing.T) {
	block := emittedLogBlock("info", 0)
	identifiers := collectNodeText(block, shimast.KindIdentifier)
	if containsTemplateIDLane(identifiers) {
		t.Fatalf("dynamic inline unexpectedly writes a separate template-ID lane: %q", identifiers)
	}
	if !containsText(identifiers, "message_values") || !containsText(identifiers, "message_nulls") {
		t.Fatalf("dynamic inline does not use ordinary message storage: %q", identifiers)
	}
	count := 0
	for _, identifier := range identifiers {
		if identifier == "dynamicMessage" {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("dynamic message evaluation occurrences = %d, want 1", count)
	}
}

func TestFactoryLocalOpsUseGlobalVocabularyAndStableChildRewrite(t *testing.T) {
	output := transformTemplateFixture(t, `
function createFactoryOps() {
  const child = defineOp('factory-child', (ctx) => {
    ctx.log.info('child literal');
    return ctx.ok(null);
  });
  const grouped = defineOps({
    grouped(ctx) { ctx.log.debug('group literal'); return ctx.ok(null); },
  });
  const parent = defineOp('factory-parent', async (ctx) => {
    ctx.log.warn('parent literal');
    await ctx.span('child-call', child);
    return ctx.ok(null);
  });
  return { child, grouped, parent };
}
`)

	registration := regexp.MustCompile(`const (\$\$lmaoVocabulary\w*) = \$\$registerLmaoVocabulary\w*\(\{`).FindStringSubmatch(output)
	if len(registration) != 2 {
		t.Fatalf("factory-local vocabulary registration missing\n%s", output)
	}
	binding := registration[1]
	if strings.Count(output, "_state._appendWriterEntry(") != 3 || strings.Count(output, "_state._buffer") != 3 || strings.Count(output, "_logHeaders[$$i]") != 3 {
		t.Fatalf("factory-local static logs did not lower through the state-owned packed-header seam\n%s", output)
	}
	if strings.Count(output, binding+"[") != 4 {
		t.Fatalf("registered binding uses = %d, want one packed-header operand per static log and one span operand\n%s", strings.Count(output, binding+"["), output)
	}
	if strings.Contains(output, "TemplateIds") {
		t.Fatalf("a separate template-ID lane survived global vocabulary lowering\n%s", output)
	}
	callsite := regexp.MustCompile(`ctx\.span0\(\d+,\s*` + regexp.QuoteMeta(binding) + `\[\d+\],\s*child\.callsitePlan\.newCtx0\(ctx\),\s*child\.callsitePlan,\s*child\.fn\)`)
	if !callsite.MatchString(output) {
		t.Fatalf("stable child span did not lower to the monomorphic CallsitePlan ABI\n%s", output)
	}
	for _, stale := range []string{"spanStatic0(", "Object.create(ctx)", "child.SpanBufferClass", "child.remappedViewClass", "child.metadata", "child.runtimeHint"} {
		if strings.Contains(output, stale) {
			t.Fatalf("stable child span retained per-call operand %s\n%s", stale, output)
		}
	}
}

func TestRepeatedOpCallsReuseDirectPlanOperandsAndDynamicOpBailsOut(t *testing.T) {
	output := transformTemplateFixture(t, `
declare function choose(left: Op, right: Op): Op;
const child = defineOp('child', (ctx) => ctx.ok('child'));
const alternate = defineOp('alternate', (ctx) => ctx.ok('alternate'));
defineOp('parent', async (ctx) => {
  await ctx.span('first', child);
  await ctx.span('second', child);
  await ctx.span('dynamic-op', choose(child, alternate));
  return ctx.ok(null);
});
`)

	if strings.Count(output, "child.callsitePlan.newCtx0(ctx)") != 2 {
		t.Fatalf("repeated child calls did not each consume the pre-resolved context factory\n%s", output)
	}
	directPlanOperand := regexp.MustCompile(`child\.callsitePlan\.newCtx0\(ctx\),\s*child\.callsitePlan,\s*child\.fn`)
	if len(directPlanOperand.FindAllString(output, -1)) != 2 {
		t.Fatalf("repeated child calls did not reuse the same direct plan operand shape\n%s", output)
	}
	dynamicCall := regexp.MustCompile(`ctx\.span\(["']dynamic-op["'],\s*choose\(child,\s*alternate\)\)`)
	if !dynamicCall.MatchString(output) {
		t.Fatalf("unanalyzed dynamic Op call did not bail out unchanged\n%s", output)
	}
	for _, stale := range []string{"spanStatic0(", "Object.create(ctx)", "child.SpanBufferClass", "child.remappedViewClass", "child.metadata", "child.runtimeHint"} {
		if strings.Contains(output, stale) {
			t.Fatalf("repeated monomorphic calls retained per-call operand %s\n%s", stale, output)
		}
	}
}

func TestLiteralLogsInValueContextsUseRegisteredBindingsWithoutDoubleRewrite(t *testing.T) {
	output := transformTemplateFixture(t, `
declare function dynamicMessage(): string;
declare function consume(value: unknown): void;
defineOp('value-contexts', (ctx) => {
  const initialized = ctx.log.info('initializer literal');
  const conditional = true ? ctx.log.warn('conditional literal') : ctx.log.debug(dynamicMessage());
  consume(ctx.log.error('argument literal'));
  return ctx.ok([initialized, conditional]);
});
`)

	registration := regexp.MustCompile(`const (\$\$lmaoVocabulary\w*) = \$\$registerLmaoVocabulary\w*\(\{`).FindStringSubmatch(output)
	if len(registration) != 2 {
		t.Fatalf("value-context vocabulary registration missing\n%s", output)
	}
	binding := registration[1]
	for _, method := range []string{"_infoTemplate", "_warnTemplate", "_errorTemplate"} {
		call := "ctx.log." + method + "(" + binding + "["
		if strings.Count(output, call) != 1 {
			t.Fatalf("%s registered call occurrences = %d, want exactly one\n%s", method, strings.Count(output, call), output)
		}
	}
	if strings.Contains(output, "TemplateIds") {
		t.Fatalf("a separate template-ID lane survived value-context lowering\n%s", output)
	}
	if strings.Count(output, "ctx.log.debug(dynamicMessage())") != 1 {
		t.Fatalf("raw dynamic debug evaluations = %d, want one\n%s", strings.Count(output, "ctx.log.debug(dynamicMessage())"), output)
	}
	if strings.Contains(output, "ctx.log.info('initializer literal')") || strings.Contains(output, "ctx.log.warn('conditional literal')") || strings.Contains(output, "ctx.log.error('argument literal')") {
		t.Fatalf("literal value-context call survived alongside its template rewrite\n%s", output)
	}
}

func diagnosticCodes(err error) []string {
	if err == nil {
		return nil
	}
	lines := strings.Split(err.Error(), "\n")
	codes := make([]string, 0, len(lines))
	for _, line := range lines {
		fields := strings.Fields(line)
		if len(fields) > 0 {
			codes = append(codes, fields[0])
		}
	}
	return codes
}

func TestOperationalTemplatesLowerWithoutFieldBagAllocation(t *testing.T) {
	result := runTemplateFixture(t, `
declare function value(label: string): string;
declare function numberValue(label: string): number;
defineOp('structured', (ctx) => {
  ctx.log.info('plain static');
  ctx.log.warn(` + "`no substitution`" + `);
  ctx.log.error('error static');
  ctx.log.info('info {jobId}', { jobId: value('info') });
  ctx.log.warn('warn {elapsedMs}', { elapsedMs: numberValue('warn') });
  ctx.log.error('job {{literal}} {jobId}', { jobId: value('error') });
  return ctx.ok(null);
});
	`)
	if result.err != nil {
		t.Fatalf("template fixture transform failed: %v", result.err)
	}
	output := result.output

	wantTemplates := map[string]bool{
		"plain static": false, "no substitution": false, "error static": false,
		"info {jobId}": false, "warn {elapsedMs}": false, "job {literal} {jobId}": false,
	}
	wantField := map[string]string{"info {jobId}": "jobId", "warn {elapsedMs}": "elapsedMs", "job {literal} {jobId}": "jobId"}
	for _, entry := range result.manifest.Entries {
		if _, wanted := wantTemplates[entry.Text]; wanted {
			wantTemplates[entry.Text] = true
		}
		if field, structured := wantField[entry.Text]; structured {
			if len(entry.Fields) != 1 || entry.Fields[0].Name != field {
				t.Fatalf("%q vocabulary fields = %+v, want exactly %s", entry.Text, entry.Fields, field)
			}
		}
	}
	for template, found := range wantTemplates {
		if !found {
			t.Fatalf("vocabulary manifest missing %q: %+v", template, result.manifest.Entries)
		}
	}
	for _, expression := range []string{"value('info')", "numberValue('warn')", "value('error')"} {
		if strings.Count(output, expression) != 1 {
			t.Fatalf("%s evaluations = %d, want exactly one\n%s", expression, strings.Count(output, expression), output)
		}
	}
	if strings.Contains(output, "jobId:") || strings.Contains(output, "elapsedMs:") {
		t.Fatalf("structured lowering retained an allocating object literal\n%s", output)
	}
	if !strings.Contains(output, "_logHeaders") || !strings.Contains(output, "jobId_values") {
		t.Fatalf("structured lowering must write the vocabulary ID and fixed schema field directly\n%s", output)
	}
}

func TestStructuredFieldsEvaluateExactlyOnceInSourceOrder(t *testing.T) {
	output := transformTemplateFixture(t, `
declare function numberValue(label: string): number;
declare function stringValue(label: string): string;
defineOp('order', (ctx) => {
  ctx.log.error('{jobId} took {elapsedMs} on {attempt}', {
    jobId: stringValue('first'),
    elapsedMs: numberValue('second'),
    attempt: numberValue('third'),
  });
  return ctx.ok(null);
});
`)

	markers := []string{"ctx.log", "stringValue('first')", "numberValue('second')", "numberValue('third')"}
	previous := -1
	for _, marker := range markers {
		if strings.Count(output, marker) != 1 {
			t.Fatalf("%s evaluations = %d, want exactly one\n%s", marker, strings.Count(output, marker), output)
		}
		position := strings.Index(output, marker)
		if position <= previous {
			t.Fatalf("evaluation order is not receiver, jobId, elapsedMs, attempt\n%s", output)
		}
		previous = position
	}
	for _, lane := range []string{"jobId_values", "elapsedMs_values", "attempt_values"} {
		if strings.Count(output, lane) != 1 {
			t.Fatalf("fixed field lane %s writes = %d, want one\n%s", lane, strings.Count(output, lane), output)
		}
	}
	if strings.Contains(output, "jobId:") || strings.Contains(output, "elapsedMs:") || strings.Contains(output, "attempt:") {
		t.Fatalf("structured lowering retained the source field object\n%s", output)
	}
}

func TestDynamicEnumFieldEvaluatesOnceAndWritesEncodedOrdinal(t *testing.T) {
	output := transformTemplateFixture(t, `
declare function nextOperation(): 'READ' | 'WRITE';
defineOp('dynamic-enum', (ctx) => {
  ctx.log.info('enum operation').operation(nextOperation());
  return ctx.ok(null);
});
`)

	const evaluation = "const $$f0 = nextOperation();"
	if strings.Count(output, evaluation) != 1 {
		t.Fatalf("dynamic enum captured evaluations = %d, want exactly one\n%s", strings.Count(output, evaluation), output)
	}
	for _, marker := range []string{`$$l._state._physicalLayoutPlan.enumLookup.byField["operation"].encode($$f0)`, "$$b.operation($$i"} {
		if !strings.Contains(output, marker) {
			t.Fatalf("dynamic enum lowering missing %q\n%s", marker, output)
		}
	}
	for _, stale := range []string{`case "READ":`, `case "WRITE":`, "return 1", "enumSwitchIIFE"} {
		if strings.Contains(output, stale) {
			t.Fatalf("dynamic enum lowering rebuilt stale per-call lookup %q\n%s", stale, output)
		}
	}
	if regexp.MustCompile(`operation_values\[[^]]+\]\s*=\s*\$\$f\d+`).MatchString(output) {
		t.Fatalf("dynamic enum string was written directly to its Uint8 ordinal lane\n%s", output)
	}
}

func TestEveryEnumInlineReusesPlanBoundSchemaEncoder(t *testing.T) {
	output := transformTemplateFixture(t, `
declare function nextOperation(): 'READ' | 'WRITE';
defineOp('enum-plan-reuse', (ctx) => {
  ctx.tag.operation(nextOperation());
  ctx.tag.with({ operation: nextOperation() });
  ctx.log.info('enum fluent').operation(nextOperation());
  ctx.log.info('enum {operation}', { operation: nextOperation() });
  return ctx.ok(null).operation(nextOperation()).with({ operation: nextOperation() });
});
`)

	if count := strings.Count(output, "nextOperation()") - 1; count != 6 {
		t.Fatalf("enum source evaluations = %d, want exactly 6 (excluding declaration)\n%s", count, output)
	}
	const encoder = `.enumLookup.byField["operation"].encode(`
	if count := strings.Count(output, encoder); count != 6 {
		t.Fatalf("plan-bound enum encoder calls = %d, want exactly 6\n%s", count, output)
	}
	for _, owner := range []string{
		`$$t._physicalLayoutPlan.enumLookup.byField["operation"].encode(`,
		`$$l._state._physicalLayoutPlan.enumLookup.byField["operation"].encode(`,
		`$$r._state._physicalLayoutPlan.enumLookup.byField["operation"].encode(`,
	} {
		if !strings.Contains(output, owner) {
			t.Fatalf("enum lowering missing plan owner %q\n%s", owner, output)
		}
	}
	for _, stale := range []string{`case "READ":`, `case "WRITE":`, "enumSwitchIIFE", "new Map", "resolveEnumLookupDescriptor"} {
		if strings.Contains(output, stale) {
			t.Fatalf("enum lowering rebuilt lookup metadata via %q\n%s", stale, output)
		}
	}
}

func TestDebugAndTraceRetainRawDynamicText(t *testing.T) {
	output := transformTemplateFixture(t, `
declare function debugText(): string;
declare function traceText(): string;
defineOp('diagnostic-dynamic', (ctx) => {
  ctx.log.debug(debugText());
  ctx.log.trace(traceText());
  return ctx.ok(null);
});
`)

	for _, call := range []string{"ctx.log.debug(debugText())", "ctx.log.trace(traceText())"} {
		if strings.Count(output, call) != 1 {
			t.Fatalf("raw dynamic diagnostic call %q occurrences = %d, want one\n%s", call, strings.Count(output, call), output)
		}
	}
}

func TestStructuredTemplatePolicyDiagnostics(t *testing.T) {
	tests := []struct {
		name string
		body string
		code string
	}{
		{name: "dynamic info", body: "declare const text: string; defineOp('x', ctx => { ctx.log.info(text); return ctx.ok(null); });", code: "LMAO_DYNAMIC_OPERATIONAL_TEXT"},
		{name: "interpolated warn", body: "declare const text: string; defineOp('x', ctx => { ctx.log.warn(`prefix ${text}`); return ctx.ok(null); });", code: "LMAO_DYNAMIC_OPERATIONAL_TEXT"},
		{name: "concatenated error", body: "declare const text: string; defineOp('x', ctx => { ctx.log.error('prefix ' + text); return ctx.ok(null); });", code: "LMAO_DYNAMIC_OPERATIONAL_TEXT"},
		{name: "interpolated debug", body: "declare const text: string; defineOp('x', ctx => { ctx.log.debug(`prefix ${text}`); return ctx.ok(null); });", code: "LMAO_AVOIDABLE_INTERPOLATION"},
		{name: "concatenated trace", body: "declare const text: string; defineOp('x', ctx => { ctx.log.trace('prefix ' + text); return ctx.ok(null); });", code: "LMAO_AVOIDABLE_INTERPOLATION"},
		{name: "identifier bag", body: "declare const fields: Record<string, unknown>; defineOp('x', ctx => { ctx.log.info('{jobId}', fields); return ctx.ok(null); });", code: "LMAO_FIELDS_NOT_OBJECT_LITERAL"},
		{name: "array bag", body: "defineOp('x', ctx => { ctx.log.info('{jobId}', ['x']); return ctx.ok(null); });", code: "LMAO_FIELDS_NOT_OBJECT_LITERAL"},
		{name: "null bag", body: "defineOp('x', ctx => { ctx.log.info('{jobId}', null); return ctx.ok(null); });", code: "LMAO_FIELDS_NOT_OBJECT_LITERAL"},
		{name: "spread property", body: "declare const rest: Record<string, unknown>; defineOp('x', ctx => { ctx.log.info('{jobId}', { ...rest, jobId: 'x' }); return ctx.ok(null); });", code: "LMAO_FIELDS_NOT_OBJECT_LITERAL"},
		{name: "computed property", body: "defineOp('x', ctx => { ctx.log.info('{jobId}', { ['jobId']: 'x' }); return ctx.ok(null); });", code: "LMAO_FIELDS_NOT_OBJECT_LITERAL"},
		{name: "getter property", body: "defineOp('x', ctx => { ctx.log.info('{jobId}', { get jobId() { return 'x'; } }); return ctx.ok(null); });", code: "LMAO_FIELDS_NOT_OBJECT_LITERAL"},
		{name: "method property", body: "defineOp('x', ctx => { ctx.log.info('{jobId}', { jobId() { return 'x'; } }); return ctx.ok(null); });", code: "LMAO_FIELDS_NOT_OBJECT_LITERAL"},
		{name: "unknown placeholder", body: "defineOp('x', ctx => { ctx.log.info('{unknown}', { jobId: 'x' }); return ctx.ok(null); });", code: "LMAO_PLACEHOLDER_MISMATCH"},
		{name: "duplicate placeholder", body: "defineOp('x', ctx => { ctx.log.info('{jobId} {jobId}', { jobId: 'x' }); return ctx.ok(null); });", code: "LMAO_PLACEHOLDER_MISMATCH"},
		{name: "missing placeholder", body: "defineOp('x', ctx => { ctx.log.info('job complete', { jobId: 'x' }); return ctx.ok(null); });", code: "LMAO_PLACEHOLDER_MISMATCH"},
		{name: "missing field", body: "defineOp('x', ctx => { ctx.log.info('{jobId} {elapsedMs}', { jobId: 'x' }); return ctx.ok(null); });", code: "LMAO_PLACEHOLDER_MISMATCH"},
		{name: "duplicate field", body: "defineOp('x', ctx => { ctx.log.info('{jobId}', { jobId: 'x', jobId: 'y' }); return ctx.ok(null); });", code: "LMAO_PLACEHOLDER_MISMATCH"},
		{name: "unclosed placeholder", body: "defineOp('x', ctx => { ctx.log.info('job {jobId', { jobId: 'x' }); return ctx.ok(null); });", code: "LMAO_PLACEHOLDER_MISMATCH"},
		{name: "stray closing brace", body: "defineOp('x', ctx => { ctx.log.info('job jobId}', { jobId: 'x' }); return ctx.ok(null); });", code: "LMAO_PLACEHOLDER_MISMATCH"},
		{name: "empty placeholder", body: "defineOp('x', ctx => { ctx.log.info('job {}', {}); return ctx.ok(null); });", code: "LMAO_PLACEHOLDER_MISMATCH"},
		{name: "unknown schema field", body: "defineOp('x', ctx => { ctx.log.info('{other}', { other: 'x' }); return ctx.ok(null); });", code: "LMAO_PLACEHOLDER_MISMATCH"},
		{name: "field type mismatch", body: "defineOp('x', ctx => { ctx.log.info('{elapsedMs}', { elapsedMs: 'slow' }); return ctx.ok(null); });", code: "LMAO_PLACEHOLDER_MISMATCH"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := runTemplateFixture(t, test.body)
			codes := diagnosticCodes(result.err)
			if len(codes) != 1 || codes[0] != test.code {
				t.Fatalf("diagnostic codes = %q, want exactly [%s]; error = %v", codes, test.code, result.err)
			}
			expectedPos := strings.Index(result.source, "ctx.log.") - 1
			expectedLocation := filepath.ToSlash(result.inputPath) + ":" + strconv.Itoa(expectedPos)
			if !strings.Contains(result.err.Error(), expectedLocation) {
				t.Fatalf("diagnostic location = %v, want call-expression full-start %s", result.err, expectedLocation)
			}
			if result.output != "" {
				t.Fatalf("rejected source was mutated/emitted:\n%s", result.output)
			}
		})
	}
}

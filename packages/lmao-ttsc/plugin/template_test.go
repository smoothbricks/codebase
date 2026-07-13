package main

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	shimast "github.com/microsoft/typescript-go/shim/ast"
	shimprinter "github.com/microsoft/typescript-go/shim/printer"
	"github.com/samchon/ttsc/packages/ttsc/driver"
)

const templateFixtureDeclarations = `
export interface OpCompileMetadata {
  readonly runtimeHint: number;
}
export class FluentLogEntry { line(value: number): this; }
export class SpanLogger {
  info(message: string): FluentLogEntry;
  debug(message: string): FluentLogEntry;
  warn(message: string): FluentLogEntry;
  error(message: string): FluentLogEntry;
  trace(message: string): FluentLogEntry;
  jobId(value: string): FluentLogEntry;
  elapsedMs(value: number): FluentLogEntry;
  attempt(value: number): FluentLogEntry;
  success(value: boolean): FluentLogEntry;
}
export class SpanContext {
  readonly _buffer: { constructor: unknown; _opMetadata: unknown };
  readonly log: SpanLogger;
  ok(value: unknown): unknown;
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
	if strings.Count(output, "writeLogEntry($$b") != 3 || strings.Count(output, "_logHeaders[$$i]") != 3 {
		t.Fatalf("factory-local static logs did not lower through the packed registered-header seam\n%s", output)
	}
	if strings.Count(output, binding+"[") != 7 {
		t.Fatalf("registered binding uses = %d, want two per static log and one static span\n%s", strings.Count(output, binding+"["), output)
	}
	if strings.Contains(output, "TemplateIds") {
		t.Fatalf("a separate template-ID lane survived global vocabulary lowering\n%s", output)
	}
	if strings.Count(output, "ctx.spanStatic0(") != 1 {
		t.Fatalf("stable child span rewrites = %d, want exactly one registered static span\n%s", strings.Count(output, "ctx.spanStatic0("), output)
	}
	for _, field := range []string{"child.SpanBufferClass", "child.metadata", "child.fn", "child.runtimeHint"} {
		if !strings.Contains(output, field) {
			t.Fatalf("stable child span output missing %s\n%s", field, output)
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

package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	shimast "github.com/microsoft/typescript-go/shim/ast"
	shimprinter "github.com/microsoft/typescript-go/shim/printer"
	"github.com/samchon/ttsc/packages/ttsc/driver"
)

const templateFixtureDeclarations = `
export interface OpCompileMetadata {
  readonly runtimeHint: number;
  readonly logTemplateIds: readonly string[];
}
export class FluentLogEntry { line(value: number): this; }
export class SpanLogger {
  info(message: string): FluentLogEntry;
  debug(message: string): FluentLogEntry;
  warn(message: string): FluentLogEntry;
  error(message: string): FluentLogEntry;
  trace(message: string): FluentLogEntry;
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

func transformTemplateFixture(t *testing.T, body string) string {
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
	transform := lmaoPluginTransform(program, root)
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
		return shimprinter.EmitSourceFile(printer, result)
	}
	t.Fatal("template fixture input source was not loaded")
	return ""
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

func emittedLogBlock(level string, templateID uint16) *shimast.Node {
	list := factory.NewNodeList([]*shimast.Node{factory.NewExpressionStatement(ident("placeholder"))})
	transformer := &fileTransformer{}
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

func TestCompileMetadataKeepsTemplateTableOrder(t *testing.T) {
	node := compileMetadataNode(opCompileAnalysis{
		runtimeHint:    123,
		logTemplateIds: []string{"first", "shared", "last"},
	})
	strings := collectNodeText(node, shimast.KindStringLiteral)
	if len(strings) != 3 || strings[0] != "first" || strings[1] != "shared" || strings[2] != "last" {
		t.Fatalf("compile metadata template order = %q, want [first shared last]", strings)
	}
	identifiers := collectNodeText(node, shimast.KindIdentifier)
	if !containsText(identifiers, "runtimeHint") || !containsText(identifiers, "logTemplateIds") {
		t.Fatalf("compile metadata fields = %q, want runtimeHint and logTemplateIds", identifiers)
	}
	numbers := collectNodeText(node, shimast.KindNumericLiteral)
	if !containsText(numbers, "123") {
		t.Fatalf("compile metadata numeric values = %q, want runtimeHint 123", numbers)
	}
}

func TestLiteralLogInlineUsesOnlyTemplateLaneForEveryLevel(t *testing.T) {
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
			if !containsText(identifiers, "_messageTemplateIds") {
				t.Fatalf("%s literal inline does not write _messageTemplateIds: %q", level, identifiers)
			}
			if containsText(identifiers, "message_values") || containsText(identifiers, "message_nulls") {
				t.Fatalf("%s literal inline writes dynamic message storage: %q", level, identifiers)
			}
			numbers := collectNodeText(block, shimast.KindNumericLiteral)
			if !containsText(numbers, "37") {
				t.Fatalf("%s literal inline numeric values = %q, want template ID 37", level, numbers)
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
	if containsText(identifiers, "_messageTemplateIds") {
		t.Fatalf("dynamic inline unexpectedly writes the template lane: %q", identifiers)
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

func TestFactoryLocalOpsReceiveMetadataTemplatesAndStableChildRewrite(t *testing.T) {
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

	for _, literal := range []string{"child literal", "group literal", "parent literal"} {
		if strings.Count(output, literal) != 1 {
			t.Fatalf("%q occurrences = %d, want exactly one metadata entry\n%s", literal, strings.Count(output, literal), output)
		}
	}
	if strings.Count(output, "logTemplateIds") != 3 {
		t.Fatalf("logTemplateIds occurrences = %d, want child, grouped, and parent metadata\n%s", strings.Count(output, "logTemplateIds"), output)
	}
	if !strings.Contains(output, "_messageTemplateIds[$$i] = 1") {
		t.Fatalf("factory-local literal logs did not use template IDs\n%s", output)
	}
	if strings.Count(output, "ctx.span0(") != 1 {
		t.Fatalf("stable child span rewrites = %d, want exactly one\n%s", strings.Count(output, "ctx.span0("), output)
	}
	for _, field := range []string{"child.SpanBufferClass", "child.metadata", "child.fn", "child.runtimeHint"} {
		if !strings.Contains(output, field) {
			t.Fatalf("stable child span output missing %s\n%s", field, output)
		}
	}
}

func TestLiteralLogsInValueContextsUseRecordedIDsWithoutDoubleRewrite(t *testing.T) {
	output := transformTemplateFixture(t, `
declare function dynamicMessage(): string;
declare function consume(value: unknown): void;
defineOp('value-contexts', (ctx) => {
  const initialized = ctx.log.info('initializer literal');
  const conditional = true ? ctx.log.warn('conditional literal') : ctx.log.warn(dynamicMessage());
  consume(ctx.log.error('argument literal'));
  return ctx.ok([initialized, conditional]);
});
`)

	if !strings.Contains(output, `logTemplateIds: ["initializer literal", "conditional literal", "argument literal"]`) {
		t.Fatalf("value-context metadata table missing or out of order\n%s", output)
	}
	for _, call := range []string{"ctx.log._infoTemplate(1)", "ctx.log._warnTemplate(2)", "ctx.log._errorTemplate(3)"} {
		if strings.Count(output, call) != 1 {
			t.Fatalf("%s occurrences = %d, want exactly one\n%s", call, strings.Count(output, call), output)
		}
	}
	if strings.Count(output, "ctx.log.warn(dynamicMessage())") != 1 {
		t.Fatalf("dynamic log evaluations = %d, want one\n%s", strings.Count(output, "ctx.log.warn(dynamicMessage())"), output)
	}
	if strings.Contains(output, "ctx.log.info('initializer literal')") || strings.Contains(output, "ctx.log.warn('conditional literal')") || strings.Contains(output, "ctx.log.error('argument literal')") {
		t.Fatalf("literal value-context call survived alongside its template rewrite\n%s", output)
	}
}

package lmao

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	shimast "github.com/microsoft/typescript-go/shim/ast"
	shimprinter "github.com/microsoft/typescript-go/shim/printer"
	"github.com/samchon/ttsc/packages/ttsc/driver"
)

// The corpus declaration set is deliberately SEPARATE from
// templateFixtureDeclarations: the 49 output-asserting tests are themselves a
// byte-level oracle over their own inputs, and perturbing their ambient types
// would move that oracle. This set is a superset — it adds the inline-closure
// and override span overloads, a destructurable `span` property, and the
// `op`/`task`/`defineModule` entry points — so a corpus input exercises a
// lowering decision rather than a type error.
//
// `span` is a callable PROPERTY, not a method, because that is what the runtime
// installs (`this.span = span` closing over `self`, spanContext.ts:895-1047) and
// it is the only form a first parameter can destructure.
const corpusDeclarations = `
export interface OpCompileMetadata {
  readonly runtimeHint: number;
  readonly eagerColumns?: readonly string[];
}
export interface UserAttributeFields {
  alpha: string;
  beta: number;
  jobId: string;
  elapsedMs: number;
  attempt: number;
  success: boolean;
  operation: 'READ' | 'WRITE';
  outcome: 'failure' | 'success';
  category: string;
  text: string;
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
}
export interface SpanOverrides {
  readonly [key: string]: unknown;
}
export type SpanFn = {
  (name: string, op: Op, ...args: unknown[]): Promise<unknown>;
  (name: string, fn: (ctx: SpanContext, ...args: unknown[]) => unknown, ...args: unknown[]): Promise<unknown>;
  (name: string, overrides: SpanOverrides, op: Op, ...args: unknown[]): Promise<unknown>;
};
export class SpanContext {
  readonly _buffer: { constructor: unknown; _opMetadata: unknown };
  readonly log: SpanLogger;
  readonly tag: GeneratedTagWriter;
  readonly span: SpanFn;
  readonly deps: Record<string, unknown>;
  readonly scope: Record<string, unknown>;
  ok(value: unknown): FluentResult;
  err(value: unknown): FluentResult;
  ff(name: string): boolean;
  setScope(values: Record<string, unknown>): void;
}
export class TaskContext extends SpanContext {}
export class Op {
  readonly SpanBufferClass: unknown;
  readonly remappedViewClass: unknown;
  readonly metadata: unknown;
  readonly runtimeHint: number;
  readonly callsitePlan: { newCtx0(parent: unknown): SpanContext };
  readonly fn: (ctx: SpanContext) => unknown;
}
export class OpGroup {}
export function defineOp(name: string, fn: (ctx: SpanContext, ...args: unknown[]) => unknown, metadata?: unknown, compileMetadata?: OpCompileMetadata): Op;
export function defineOps(definitions: Record<string, Op | ((ctx: SpanContext, ...args: unknown[]) => unknown)>, compileMetadataByKey?: Readonly<Record<string, OpCompileMetadata>>): OpGroup;
export function op(fn: (ctx: SpanContext, ...args: unknown[]) => unknown): Op;
export function task(name: string, fn: (ctx: TaskContext, ...args: unknown[]) => unknown): unknown;
export function defineModule(definition: Record<string, unknown>): unknown;
`

const corpusImportLine = "import { defineModule, defineOp, defineOps, op, task, Op, OpGroup, SpanContext, SpanFn, TaskContext } from '@smoothbricks/lmao';\n"

// transformCorpusEntry runs the whole-program collect + transform pipeline over
// one corpus source and returns the printed output. It shares the tsgo mutex
// with runTemplateFixture: printer/program emit is not concurrency-safe.
func transformCorpusEntry(t *testing.T, body string) string {
	t.Helper()
	tsgoTestMu.Lock()
	defer tsgoTestMu.Unlock()
	root := t.TempDir()
	inputPath := filepath.Join(root, "input.ts")
	declarationDir := filepath.Join(root, "node_modules", "@smoothbricks", "lmao")
	if err := os.MkdirAll(declarationDir, 0o755); err != nil {
		t.Fatal(err)
	}
	files := map[string]string{
		inputPath:                                     corpusImportLine + body,
		filepath.Join(root, "tsconfig.json"):          `{"compilerOptions":{"target":"esnext","module":"esnext","moduleResolution":"node"},"files":["input.ts"]}`,
		filepath.Join(declarationDir, "index.d.ts"):   corpusDeclarations,
		filepath.Join(declarationDir, "package.json"): `{"name":"@smoothbricks/lmao","types":"index.d.ts"}`,
	}
	for name, content := range files {
		if err := os.WriteFile(name, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	configPath := filepath.Join(root, "tsconfig.json")
	program, _, err := driver.LoadProgram(root, configPath, driver.LoadProgramOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer program.Close()
	var inputSource *shimast.SourceFile
	for _, sourceFile := range program.SourceFiles() {
		if sourceFile != nil && filepath.Clean(sourceFile.FileName()) == filepath.Clean(inputPath) {
			inputSource = sourceFile
			break
		}
	}
	if inputSource == nil {
		t.Fatal("corpus input source was not loaded")
	}

	transform, err := lmaoPluginTransform(program, compilerOptions{cwd: root, tsconfig: configPath})
	if err != nil {
		t.Fatalf("corpus transform construction failed: %v", err)
	}
	emitContext := shimprinter.NewEmitContext()
	result := transform(emitContext, inputSource)
	if result == nil {
		t.Fatal("corpus transform returned nil")
	}
	printer := shimprinter.NewPrinter(shimprinter.PrinterOptions{}, shimprinter.PrintHandlers{}, emitContext)
	return shimprinter.EmitSourceFile(printer, result)
}

// TestCorpusDump is the differential oracle for the span lowerings: it prints
// every corpus entry's transform output so a pre-change and post-change tree can
// be diffed byte for byte. Every call site the change does not claim to newly
// handle must survive identically, which is a property no single assertion can
// state.
//
// Gated on LMAO_CORPUS_OUT so the ordinary suite stays a pure assertion run;
// with the variable set the dump is the whole point of the test.
func TestCorpusDump(t *testing.T) {
	outDir := os.Getenv("LMAO_CORPUS_OUT")
	if outDir == "" {
		t.Skip("set LMAO_CORPUS_OUT=<dir> to dump the transform corpus")
	}
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		t.Fatal(err)
	}
	names := make([]string, 0, len(transformCorpus))
	for name := range transformCorpus {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		body := transformCorpus[name]
		output := transformCorpusEntry(t, body)
		record := "=== INPUT " + name + " ===\n" + strings.TrimRight(body, "\n") +
			"\n=== OUTPUT " + name + " ===\n" + output
		if err := os.WriteFile(filepath.Join(outDir, name+".txt"), []byte(record), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("dumped %d corpus outputs to %s", len(names), outDir)
}

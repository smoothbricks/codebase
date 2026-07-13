package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"testing"

	shimprinter "github.com/microsoft/typescript-go/shim/printer"
	"github.com/samchon/ttsc/packages/ttsc/driver"
)

type wholeProgramSource struct {
	name string
	body string
}

type wholeProgramFileResult struct {
	entries  []vocabularyCatalogEntry
	ordinals map[globalVocabularyID]int
	output   string
}

type wholeProgramResult struct {
	catalog vocabularyCatalog
	files   map[string]wholeProgramFileResult
}

func compileWholeProgramFixture(t *testing.T, sources []wholeProgramSource) wholeProgramResult {
	t.Helper()
	root := t.TempDir()
	declarationDir := filepath.Join(root, "node_modules", "@smoothbricks", "lmao")
	if err := os.MkdirAll(declarationDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(declarationDir, "index.d.ts"), []byte(templateFixtureDeclarations), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(declarationDir, "package.json"), []byte(`{"name":"@smoothbricks/lmao","types":"index.d.ts"}`), 0o644); err != nil {
		t.Fatal(err)
	}

	fileNames := make([]string, len(sources))
	for index, source := range sources {
		fileNames[index] = source.name
		text := "import { defineOp } from '@smoothbricks/lmao';\n" + source.body
		if err := os.WriteFile(filepath.Join(root, source.name), []byte(text), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	configBytes, err := json.Marshal(struct {
		CompilerOptions map[string]string `json:"compilerOptions"`
		Files           []string          `json:"files"`
	}{
		CompilerOptions: map[string]string{"target": "esnext", "module": "esnext", "moduleResolution": "node"},
		Files:           fileNames,
	})
	if err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(root, "tsconfig.json")
	if err := os.WriteFile(configPath, configBytes, 0o644); err != nil {
		t.Fatal(err)
	}

	program, _, err := driver.LoadProgram(root, configPath, driver.LoadProgramOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer program.Close()
	options := compilerOptions{cwd: root, tsconfig: configPath}
	compilation, err := collectProgramCompilation(program, options)
	if err != nil {
		t.Fatalf("whole-program collection failed: %v", err)
	}
	transform, err := lmaoPluginTransform(program, options)
	if err != nil {
		t.Fatalf("whole-program transform failed: %v", err)
	}

	result := wholeProgramResult{files: make(map[string]wholeProgramFileResult, len(sources))}
	catalogByKey := make(map[vocabularyKey]vocabularyCatalogEntry)
	for _, sourceFile := range program.SourceFiles() {
		if sourceFile == nil || sourceFile.IsDeclarationFile {
			continue
		}
		collected := compilation.files[sourceFile]
		if collected == nil {
			t.Fatalf("%s was absent from whole-program compilation", sourceFile.FileName())
		}
		entries := append([]vocabularyCatalogEntry(nil), collected.registrationEntries...)
		for _, entry := range entries {
			key, keyErr := keyFromCatalogEntry(entry)
			if keyErr != nil {
				t.Fatal(keyErr)
			}
			catalogByKey[key] = entry
		}
		emitContext := shimprinter.NewEmitContext()
		transformed := transform(emitContext, sourceFile)
		if transformed == nil {
			t.Fatalf("whole-program transform returned nil for %s", sourceFile.FileName())
		}
		printer := shimprinter.NewPrinter(shimprinter.PrinterOptions{}, shimprinter.PrintHandlers{}, emitContext)
		ordinals := make(map[globalVocabularyID]int, len(collected.transformer.vocabularyOrdinals))
		for id, ordinal := range collected.transformer.vocabularyOrdinals {
			ordinals[id] = ordinal
		}
		result.files[filepath.Base(sourceFile.FileName())] = wholeProgramFileResult{
			entries:  entries,
			ordinals: ordinals,
			output:   shimprinter.EmitSourceFile(printer, transformed),
		}
	}
	result.catalog.Entries = make([]vocabularyCatalogEntry, 0, len(catalogByKey))
	for _, entry := range catalogByKey {
		result.catalog.Entries = append(result.catalog.Entries, entry)
	}
	sortVocabularyEntries(result.catalog.Entries)
	return result
}

func decimalUint32(values []uint32) string {
	parts := make([]string, len(values))
	for index, value := range values {
		parts[index] = strconv.FormatUint(uint64(value), 10)
	}
	return strings.Join(parts, ", ")
}

func decimalInt32(values []int32) string {
	parts := make([]string, len(values))
	for index, value := range values {
		parts[index] = strconv.FormatInt(int64(value), 10)
	}
	return strings.Join(parts, ", ")
}

func assertFileLocalRegistration(t *testing.T, file wholeProgramFileResult, wantEntries []vocabularyCatalogEntry) {
	t.Helper()
	if !reflect.DeepEqual(file.entries, wantEntries) {
		t.Fatalf("file-local catalog entries = %#v, want %#v", file.entries, wantEntries)
	}
	wantOrdinals := vocabularyOrdinals(wantEntries)
	if !reflect.DeepEqual(file.ordinals, wantOrdinals) {
		t.Fatalf("file-local ordinals = %#v, want %#v", file.ordinals, wantOrdinals)
	}
	fragment, err := fragmentFromEntries(wantEntries)
	if err != nil {
		t.Fatal(err)
	}
	contracts := []string{
		`import "@smoothbricks/lmao/vocabulary/register/v1";`,
		`contentHash: "` + independentContentHash(wantEntries) + `"`,
		`ids: new Uint32Array([` + decimalUint32(fragment.IDs) + `])`,
		`kindTags: new Uint8Array([` + decimalBytes(fragment.KindTags) + `])`,
		`utf8: new Uint8Array([` + decimalBytes(fragment.UTF8) + `])`,
		`offsets: new Int32Array([` + decimalInt32(fragment.Offsets) + `])`,
	}
	for _, contract := range contracts {
		if !strings.Contains(file.output, contract) {
			t.Fatalf("file-local registration missing %q:\n%s", contract, file.output)
		}
	}
	registration := regexp.MustCompile(`const (\$\$lmaoVocabulary\w*) = \$\$registerLmaoVocabulary\w*\(\{`).FindAllStringSubmatch(file.output, -1)
	if len(registration) != 1 {
		t.Fatalf("file-local registration calls = %d, want exactly one:\n%s", len(registration), file.output)
	}
	binding := registration[0][1]
	for ordinal := range wantEntries {
		if !strings.Contains(file.output, fmt.Sprintf("%s[%d]", binding, ordinal)) {
			t.Fatalf("emitted callsites never consume file-local ordinal %d through %s:\n%s", ordinal, binding, file.output)
		}
	}
}

func TestWholeProgramVocabularyIsDeterministicAcrossReversedModulesTraversalAndDuplicates(t *testing.T) {
	baselineA := `
defineOp('a', (ctx) => {
  ctx.log.info('shared');
  ctx.log.warn('alpha {jobId}', { jobId: 'a' as string });
  ctx.log.info('shared');
  return ctx.ok(null);
});
`
	permutedA := `
defineOp('a', (ctx) => {
  ctx.log.info('shared');
  ctx.log.info('shared');
  ctx.log.warn('alpha {jobId}', { jobId: 'a' as string });
  return ctx.ok(null);
});
`
	baselineB := `
const child = defineOp('child', (ctx) => ctx.ok(null));
defineOp('b', async (ctx) => {
  await ctx.span('shared', child);
  ctx.log.error('beta');
  return ctx.ok(null);
});
`
	permutedB := `
const child = defineOp('child', (ctx) => ctx.ok(null));
defineOp('b', async (ctx) => {
  ctx.log.error('beta');
  await ctx.span('shared', child);
  return ctx.ok(null);
});
`

	baseline := compileWholeProgramFixture(t, []wholeProgramSource{{name: "a.ts", body: baselineA}, {name: "b.ts", body: baselineB}, {name: "empty.ts", body: "export const untouched = 1;"}})
	permuted := compileWholeProgramFixture(t, []wholeProgramSource{{name: "empty.ts", body: "export const untouched = 1;"}, {name: "b.ts", body: permutedB}, {name: "a.ts", body: permutedA}})

	wantA := independentCatalog(
		independentEntry(vocabularyLogTemplate, "shared"),
		independentEntry(vocabularyLogTemplate, "alpha {jobId}", vocabularyField{Name: "jobId", Column: "jobId"}),
	).Entries
	wantB := independentCatalog(
		independentEntry(vocabularySpanName, "shared"),
		independentEntry(vocabularyLogTemplate, "beta"),
	).Entries
	wantCatalog := independentCatalog(append(append([]vocabularyCatalogEntry{}, wantA...), wantB...)...)
	if !reflect.DeepEqual(baseline.catalog, wantCatalog) || !reflect.DeepEqual(permuted.catalog, wantCatalog) {
		t.Fatalf("whole-program catalog changed with module/traversal permutation or duplicate occurrences:\nbaseline=%#v\npermuted=%#v\nwant=%#v", baseline.catalog, permuted.catalog, wantCatalog)
	}
	for _, result := range []wholeProgramResult{baseline, permuted} {
		assertFileLocalRegistration(t, result.files["a.ts"], wantA)
		assertFileLocalRegistration(t, result.files["b.ts"], wantB)
		if !reflect.DeepEqual(result.files["a.ts"].entries, baseline.files["a.ts"].entries) ||
			!reflect.DeepEqual(result.files["b.ts"].entries, baseline.files["b.ts"].entries) {
			t.Fatal("reversed module collection changed file-local registration semantics")
		}
	}
}

func TestWholeProgramEmptySourceEmitsNoVocabularyRegistration(t *testing.T) {
	result := compileWholeProgramFixture(t, []wholeProgramSource{{name: "empty.ts", body: `"use client"; export const untouched = 1;`}})
	file := result.files["empty.ts"]
	if len(result.catalog.Entries) != 0 || len(file.entries) != 0 || len(file.ordinals) != 0 {
		t.Fatalf("empty source produced vocabulary state: catalog=%#v file=%#v ordinals=%#v", result.catalog, file.entries, file.ordinals)
	}
	for _, forbidden := range []string{
		"@smoothbricks/lmao/vocabulary/register/v1",
		"LMAO_VOCABULARY_ABI_UNAVAILABLE",
		"$$registerLmaoVocabulary",
		"$$lmaoVocabulary",
	} {
		if strings.Contains(file.output, forbidden) {
			t.Fatalf("empty source emitted vocabulary registration %q:\n%s", forbidden, file.output)
		}
	}
	if !strings.Contains(file.output, `"use client";`) || !strings.Contains(file.output, "export const untouched = 1;") {
		t.Fatalf("empty source lost its directive or module body:\n%s", file.output)
	}
}

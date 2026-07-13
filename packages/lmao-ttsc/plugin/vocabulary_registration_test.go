package main

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"testing"

	shimast "github.com/microsoft/typescript-go/shim/ast"
	shimprinter "github.com/microsoft/typescript-go/shim/printer"
	"github.com/samchon/ttsc/packages/ttsc/driver"
)

func loadRegistrationTestSource(t *testing.T, source string) *shimast.SourceFile {
	t.Helper()
	root := t.TempDir()
	inputPath := filepath.Join(root, "input.ts")
	if err := os.WriteFile(inputPath, []byte(source), 0o644); err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(root, "tsconfig.json")
	if err := os.WriteFile(configPath, []byte(`{"compilerOptions":{"target":"esnext","module":"esnext"},"files":["input.ts"]}`), 0o644); err != nil {
		t.Fatal(err)
	}
	program, _, err := driver.LoadProgram(root, configPath, driver.LoadProgramOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = program.Close() })
	for _, sourceFile := range program.SourceFiles() {
		if sourceFile != nil && filepath.Clean(sourceFile.FileName()) == filepath.Clean(inputPath) {
			return sourceFile
		}
	}
	t.Fatal("registration test input source was not loaded")
	return nil
}

func emitRegistrationTestSource(sourceFile *shimast.SourceFile, emitContext *shimprinter.EmitContext) string {
	printer := shimprinter.NewPrinter(shimprinter.PrinterOptions{}, shimprinter.PrintHandlers{}, emitContext)
	return shimprinter.EmitSourceFile(printer, sourceFile)
}

func decimalBytes(values []byte) string {
	parts := make([]string, len(values))
	for index, value := range values {
		parts[index] = strconv.Itoa(int(value))
	}
	return strings.Join(parts, ", ")
}

func TestVocabularyRegistrationEmitsExactSection6FragmentAndOrdinalOperands(t *testing.T) {
	entries := []vocabularyManifestEntry{
		{ID: 0x010203, Kind: vocabularyLogTemplate, Text: "A\x00💩", Fields: []vocabularyField{{Name: "user💩", Column: "user_id"}, {Name: "e\u0301", Column: "é"}}},
		{ID: 0x0a0b0c, Kind: vocabularySpanName, Text: "é", Fields: []vocabularyField{}},
	}
	wantUTF8 := []byte{
		0x06, 0x00, 0x00, 0x00, 'A', 0x00, 0xf0, 0x9f, 0x92, 0xa9,
		0x02, 0x00,
		0x08, 0x00, 'u', 's', 'e', 'r', 0xf0, 0x9f, 0x92, 0xa9,
		0x07, 0x00, 'u', 's', 'e', 'r', '_', 'i', 'd',
		0x03, 0x00, 'e', 0xcc, 0x81,
		0x02, 0x00, 0xc3, 0xa9,
		0x02, 0x00, 0x00, 0x00, 0xc3, 0xa9, 0x00, 0x00,
	}
	fragment, err := fragmentFromEntries(entries)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(fragment.IDs, []uint32{0x010203, 0x0a0b0c}) ||
		!bytes.Equal(fragment.KindTags, []byte{1, 2}) ||
		!bytes.Equal(fragment.UTF8, wantUTF8) ||
		!reflect.DeepEqual(fragment.Offsets, []int32{0, 40, 48}) {
		t.Fatalf("fragment = %#v, want exact §6 ids, kind tags, record bytes, and offsets", fragment)
	}

	emitContext := shimprinter.NewEmitContext()
	binding, registration := vocabularyRegistrationStatements(emitContext, entries)
	if binding == nil || len(registration) != 4 {
		t.Fatalf("registration returned binding %v and %d statements, want a binding and import/callback/guard/call", binding, len(registration))
	}
	ordinals := vocabularyOrdinals(entries)
	transformer := fileTransformer{vocabularyBinding: binding, vocabularyOrdinals: ordinals}
	sourceFile := loadRegistrationTestSource(t, "export const untouched = 1;\n")
	original := append([]*shimast.Node(nil), sourceFile.Statements.Nodes...)
	original = append(original,
		constDecl(ident("logLocalID"), transformer.staticVocabularyOperand(globalVocabularyID(entries[0].ID))),
		constDecl(ident("spanLocalID"), transformer.staticVocabularyOperand(globalVocabularyID(entries[1].ID))),
	)
	sourceFile.Statements = factory.NewNodeList(original)
	prependVocabularyRegistration(sourceFile, registration)
	shimast.SetParentInChildrenUnset(sourceFile.AsNode())
	output := emitRegistrationTestSource(sourceFile, emitContext)

	wantHash := independentContentHash(entries)
	contracts := []string{
		`import "@smoothbricks/lmao/vocabulary/register/v1";`,
		`Symbol.for("@smoothbricks/lmao/vocabulary/register/v1")`,
		`schemaVersion: 1`,
		`idAlgorithm: "sha256-24-v1"`,
		`contentHash: "` + wantHash + `"`,
		`ids: new Uint32Array([66051, 658188])`,
		`kindTags: new Uint8Array([1, 2])`,
		`utf8: new Uint8Array([` + decimalBytes(wantUTF8) + `])`,
		`offsets: new Int32Array([0, 40, 48])`,
		`throw new Error("LMAO_VOCABULARY_ABI_UNAVAILABLE")`,
	}
	for _, contract := range contracts {
		if !strings.Contains(output, contract) {
			t.Errorf("emitted registration missing exact contract %q:\n%s", contract, output)
		}
	}
	for _, obsolete := range []string{"values:", "denseIndices:"} {
		if strings.Contains(output, obsolete) {
			t.Errorf("emitted §6 fragment contains obsolete field %q:\n%s", obsolete, output)
		}
	}

	registrationPattern := regexp.MustCompile(`const (\$\$lmaoVocabulary\w*) = (\$\$registerLmaoVocabulary\w*)\(\{`)
	matches := registrationPattern.FindAllStringSubmatch(output, -1)
	if len(matches) != 1 {
		t.Fatalf("registration calls = %d, want exactly one:\n%s", len(matches), output)
	}
	bindingName, callbackName := matches[0][1], matches[0][2]
	guard := fmt.Sprintf(`if (typeof %s !== "function")`, callbackName)
	if !strings.Contains(output, guard) {
		t.Errorf("callback guard = missing %q:\n%s", guard, output)
	}
	if !strings.Contains(output, "const logLocalID = "+bindingName+"[0];") ||
		!strings.Contains(output, "const spanLocalID = "+bindingName+"[1];") {
		t.Errorf("callsites do not use the returned binding's exact local ordinals:\n%s", output)
	}
}

func TestVocabularyRegistrationEmptyFragmentEmitsNothing(t *testing.T) {
	emitContext := shimprinter.NewEmitContext()
	binding, statements := vocabularyRegistrationStatements(emitContext, nil)
	if binding != nil || statements != nil {
		t.Fatalf("empty fragment returned binding %v and statements %#v, want neither", binding, statements)
	}
	sourceFile := loadRegistrationTestSource(t, `"use strict"; export const value = 1;`)
	before := emitRegistrationTestSource(sourceFile, emitContext)
	prependVocabularyRegistration(sourceFile, statements)
	after := emitRegistrationTestSource(sourceFile, emitContext)
	if after != before {
		t.Fatalf("empty registration changed module emission:\nbefore:\n%s\nafter:\n%s", before, after)
	}
}

func TestPrependVocabularyRegistrationPreservesDirectivesAndImportPosition(t *testing.T) {
	entry := vocabularyManifestEntry{ID: 7, Kind: vocabularySpanName, Text: "work", Fields: []vocabularyField{}}
	emitContext := shimprinter.NewEmitContext()
	_, registration := vocabularyRegistrationStatements(emitContext, []vocabularyManifestEntry{entry})
	sourceFile := loadRegistrationTestSource(t, `"use client";
import { dependency } from "./dependency";
const body = dependency;
`)
	prependVocabularyRegistration(sourceFile, registration)
	shimast.SetParentInChildrenUnset(sourceFile.AsNode())
	output := emitRegistrationTestSource(sourceFile, emitContext)

	ordered := []string{
		`"use client";`,
		`import { dependency } from "./dependency";`,
		`import "@smoothbricks/lmao/vocabulary/register/v1";`,
		`Symbol.for("@smoothbricks/lmao/vocabulary/register/v1")`,
		`const body = dependency;`,
	}
	prior := -1
	for _, text := range ordered {
		index := strings.Index(output, text)
		if index < 0 || index <= prior {
			t.Fatalf("directive/import/registration/body order does not preserve %q after prior contract:\n%s", text, output)
		}
		prior = index
	}
}

func TestVocabularyRegistrationBindingAvoidsPreferredSourceName(t *testing.T) {
	entry := vocabularyManifestEntry{ID: 9, Kind: vocabularyLogTemplate, Text: "collision", Fields: []vocabularyField{}}
	emitContext := shimprinter.NewEmitContext()
	binding, registration := vocabularyRegistrationStatements(emitContext, []vocabularyManifestEntry{entry})
	sourceFile := loadRegistrationTestSource(t, "const $$lmaoVocabulary = 7;\n")
	statements := append([]*shimast.Node(nil), sourceFile.Statements.Nodes...)
	statements = append(statements, constDecl(ident("localID"), (&fileTransformer{
		vocabularyBinding: binding,
		vocabularyOrdinals: map[globalVocabularyID]int{9: 0},
	}).staticVocabularyOperand(9)))
	sourceFile.Statements = factory.NewNodeList(statements)
	prependVocabularyRegistration(sourceFile, registration)
	shimast.SetParentInChildrenUnset(sourceFile.AsNode())
	output := emitRegistrationTestSource(sourceFile, emitContext)

	match := regexp.MustCompile(`const (\$\$lmaoVocabulary\w*) = \$\$registerLmaoVocabulary\w*\(\{`).FindStringSubmatch(output)
	if len(match) != 2 {
		t.Fatalf("generated vocabulary binding not found:\n%s", output)
	}
	if match[1] == "$$lmaoVocabulary" {
		t.Fatalf("generated binding collided with the source declaration:\n%s", output)
	}
	if !strings.Contains(output, "const $$lmaoVocabulary = 7;") || !strings.Contains(output, "const localID = "+match[1]+"[0];") {
		t.Fatalf("source binding or generated-binding callsite was not preserved:\n%s", output)
	}
}

func TestManifestSupersetEmitsOnlyFileRecordsWithLocalOrdinalOperands(t *testing.T) {
	unrelated := vocabularyManifestEntry{ID: 11, Kind: vocabularyLogTemplate, Text: "other module", Fields: []vocabularyField{}}
	span := vocabularyManifestEntry{ID: 22, Kind: vocabularySpanName, Text: "spän💩", Fields: []vocabularyField{}}
	log := vocabularyManifestEntry{ID: 33, Kind: vocabularyLogTemplate, Text: "user é", Fields: []vocabularyField{{Name: "user", Column: "user_id"}}}
	manifest := vocabularyManifest{Entries: []vocabularyManifestEntry{unrelated, span, log}}
	spanKey, err := keyFromManifestEntry(span)
	if err != nil {
		t.Fatal(err)
	}
	logKey, err := keyFromManifestEntry(log)
	if err != nil {
		t.Fatal(err)
	}
	entries := manifestEntriesForFile(manifest, map[vocabularyKey]struct{}{logKey: {}, spanKey: {}})
	emitContext := shimprinter.NewEmitContext()
	binding, registration := vocabularyRegistrationStatements(emitContext, entries)
	transformer := fileTransformer{vocabularyBinding: binding, vocabularyOrdinals: vocabularyOrdinals(entries)}
	sourceFile := loadRegistrationTestSource(t, "export const untouched = 1;\n")
	statements := append([]*shimast.Node(nil), sourceFile.Statements.Nodes...)
	statements = append(statements,
		constDecl(ident("spanLocalID"), transformer.staticVocabularyOperand(globalVocabularyID(span.ID))),
		constDecl(ident("logLocalID"), transformer.staticVocabularyOperand(globalVocabularyID(log.ID))),
	)
	sourceFile.Statements = factory.NewNodeList(statements)
	prependVocabularyRegistration(sourceFile, registration)
	shimast.SetParentInChildrenUnset(sourceFile.AsNode())
	output := emitRegistrationTestSource(sourceFile, emitContext)

	match := regexp.MustCompile(`const (\$\$lmaoVocabulary\w*) = \$\$registerLmaoVocabulary\w*\(\{`).FindStringSubmatch(output)
	if len(match) != 2 {
		t.Fatalf("generated file-local vocabulary binding not found:\n%s", output)
	}
	if !strings.Contains(output, "ids: new Uint32Array([22, 33])") || strings.Contains(output, "ids: new Uint32Array([11") {
		t.Fatalf("file fragment did not exclude the unrelated app-wide manifest record:\n%s", output)
	}
	if !strings.Contains(output, "const spanLocalID = "+match[1]+"[0];") ||
		!strings.Contains(output, "const logLocalID = "+match[1]+"[1];") {
		t.Fatalf("file-local operands do not bind the selected records by ordinal:\n%s", output)
	}
}

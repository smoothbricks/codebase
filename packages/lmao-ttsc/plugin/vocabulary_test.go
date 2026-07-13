package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
)

func independentKindTag(kind vocabularyKind) byte {
	switch kind {
	case vocabularyLogTemplate:
		return 1
	case vocabularySpanName:
		return 2
	default:
		return 0xff
	}
}

func independentRecord(text string, fields []vocabularyField) []byte {
	var record bytes.Buffer
	_ = binary.Write(&record, binary.LittleEndian, uint32(len([]byte(text))))
	record.WriteString(text)
	_ = binary.Write(&record, binary.LittleEndian, uint16(len(fields)))
	for _, field := range fields {
		_ = binary.Write(&record, binary.LittleEndian, uint16(len([]byte(field.Name))))
		record.WriteString(field.Name)
		_ = binary.Write(&record, binary.LittleEndian, uint16(len([]byte(field.Column))))
		record.WriteString(field.Column)
	}
	return record.Bytes()
}

func independentVocabularyID(key vocabularyKey) globalVocabularyID {
	record := []byte(key.Record)
	if len(record) == 0 {
		record = independentRecord(key.Value, nil)
	}
	digest := sha256.Sum256(append([]byte{independentKindTag(key.Kind)}, record...))
	return globalVocabularyID(uint32(digest[0])<<16 | uint32(digest[1])<<8 | uint32(digest[2]))
}

func independentEntry(kind vocabularyKind, text string, fields ...vocabularyField) vocabularyManifestEntry {
	record := independentRecord(text, fields)
	key := vocabularyKey{Kind: kind, Value: text, Record: string(record)}
	return vocabularyManifestEntry{ID: uint32(independentVocabularyID(key)), Kind: kind, Text: text, Fields: append([]vocabularyField{}, fields...)}
}

func independentContentStream(entries []vocabularyManifestEntry) []byte {
	ids := make([]uint32, len(entries))
	kindTags := make([]byte, len(entries))
	offsets := make([]int32, len(entries)+1)
	var records bytes.Buffer
	for index, entry := range entries {
		ids[index] = entry.ID
		kindTags[index] = independentKindTag(entry.Kind)
		records.Write(independentRecord(entry.Text, entry.Fields))
		offsets[index+1] = int32(records.Len())
	}
	var stream bytes.Buffer
	stream.WriteByte(1)
	_ = binary.Write(&stream, binary.LittleEndian, uint16(len("sha256-24-v1")))
	stream.WriteString("sha256-24-v1")
	_ = binary.Write(&stream, binary.LittleEndian, uint32(len(ids)))
	for _, id := range ids {
		_ = binary.Write(&stream, binary.LittleEndian, id)
	}
	_ = binary.Write(&stream, binary.LittleEndian, uint32(len(kindTags)))
	stream.Write(kindTags)
	_ = binary.Write(&stream, binary.LittleEndian, uint32(records.Len()))
	stream.Write(records.Bytes())
	_ = binary.Write(&stream, binary.LittleEndian, uint32(len(offsets)))
	for _, offset := range offsets {
		_ = binary.Write(&stream, binary.LittleEndian, offset)
	}
	return stream.Bytes()
}

func independentContentHash(entries []vocabularyManifestEntry) string {
	digest := sha256.Sum256(independentContentStream(entries))
	return hex.EncodeToString(digest[:])
}

func independentManifest(entries ...vocabularyManifestEntry) vocabularyManifest {
	entries = append([]vocabularyManifestEntry{}, entries...)
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].ID != entries[j].ID {
			return entries[i].ID < entries[j].ID
		}
		if independentKindTag(entries[i].Kind) != independentKindTag(entries[j].Kind) {
			return independentKindTag(entries[i].Kind) < independentKindTag(entries[j].Kind)
		}
		return bytes.Compare(independentRecord(entries[i].Text, entries[i].Fields), independentRecord(entries[j].Text, entries[j].Fields)) < 0
	})
	return vocabularyManifest{SchemaVersion: 1, IDAlgorithm: "sha256-24-v1", ContentHash: independentContentHash(entries), Entries: entries}
}

func collectorFromEntries(entries ...vocabularyManifestEntry) *programVocabularyCollector {
	collector := newProgramVocabularyCollector()
	for _, entry := range entries {
		key, err := keyFromManifestEntry(entry)
		if err != nil {
			panic(err)
		}
		collector.records[key] = entry
		collector.occurrences = append(collector.occurrences, vocabularyOccurrence{Key: key})
	}
	return collector
}

func cloneManifest(manifest vocabularyManifest) vocabularyManifest {
	clone := manifest
	clone.Entries = append([]vocabularyManifestEntry(nil), manifest.Entries...)
	for index := range clone.Entries {
		clone.Entries[index].Fields = append([]vocabularyField(nil), clone.Entries[index].Fields...)
	}
	return clone
}

func TestVocabularyRecordEncodingAndIDsFreezeCanonicalBytes(t *testing.T) {
	fields := []vocabularyField{{Name: "user💩", Column: "user_id"}, {Name: "e\u0301", Column: "é"}}
	gotRecord, err := encodeVocabularyRecord("A\x00💩", fields)
	if err != nil {
		t.Fatal(err)
	}
	wantRecord := []byte{
		0x06, 0x00, 0x00, 0x00, 'A', 0x00, 0xf0, 0x9f, 0x92, 0xa9,
		0x02, 0x00,
		0x08, 0x00, 'u', 's', 'e', 'r', 0xf0, 0x9f, 0x92, 0xa9,
		0x07, 0x00, 'u', 's', 'e', 'r', '_', 'i', 'd',
		0x03, 0x00, 'e', 0xcc, 0x81,
		0x02, 0x00, 0xc3, 0xa9,
	}
	if !bytes.Equal(gotRecord, wantRecord) {
		t.Fatalf("record bytes = %x, want exact §6 bytes %x", gotRecord, wantRecord)
	}

	keys := []vocabularyKey{
		{Kind: vocabularyLogTemplate, Value: "A\x00💩", Record: string(gotRecord)},
		{Kind: vocabularySpanName, Value: "A\x00💩", Record: string(gotRecord)},
		{Kind: vocabularyLogTemplate, Value: "A\x00💩", Record: string(independentRecord("A\x00💩", nil))},
		{Kind: vocabularyLogTemplate, Value: "é", Record: string(independentRecord("é", nil))},
		{Kind: vocabularyLogTemplate, Value: "e\u0301", Record: string(independentRecord("e\u0301", nil))},
	}
	seen := map[globalVocabularyID]vocabularyKey{}
	for _, key := range keys {
		got := deriveVocabularyID(key)
		want := independentVocabularyID(key)
		if got != want || got == 0 || got > maxVocabularyID {
			t.Fatalf("deriveVocabularyID(%v) = %d, want record-derived nonzero u24 %d", key, got, want)
		}
		if prior, exists := seen[got]; exists {
			t.Fatalf("fixtures %v and %v unexpectedly share ID %d", prior, key, got)
		}
		seen[got] = key
	}
}

func TestVocabularyFragmentArraysAndHashFreezeExactBytes(t *testing.T) {
	entries := []vocabularyManifestEntry{
		{ID: 0x010203, Kind: vocabularyLogTemplate, Text: "A\x00💩e\u0301", Fields: []vocabularyField{{Name: "x", Column: "y"}}},
		{ID: 0x0a0b0c, Kind: vocabularySpanName, Text: "é", Fields: []vocabularyField{}},
	}
	fragment, err := fragmentFromEntries(entries)
	if err != nil {
		t.Fatal(err)
	}
	wantRecords := append(independentRecord(entries[0].Text, entries[0].Fields), independentRecord(entries[1].Text, entries[1].Fields)...)
	if !reflect.DeepEqual(fragment.IDs, []uint32{0x010203, 0x0a0b0c}) ||
		!bytes.Equal(fragment.KindTags, []byte{1, 2}) ||
		!bytes.Equal(fragment.UTF8, wantRecords) ||
		!reflect.DeepEqual(fragment.Offsets, []int32{0, 21, 29}) {
		t.Fatalf("fragment = %#v, want exact ids/kinds/records/offsets", fragment)
	}
	wantStream := independentContentStream(entries)
	wantDigest := sha256.Sum256(wantStream)
	wantHash := hex.EncodeToString(wantDigest[:])
	if got := vocabularyContentHash(entries); got != wantHash {
		t.Fatalf("contentHash = %q, want SHA-256(%x) = %q", got, wantStream, wantHash)
	}
}

func TestCompiledStaticVocabularyUsesCurrentLocalDictionaryOrdinalBindings(t *testing.T) {
	output := transformTemplateFixture(t, `
function createOrdinalOps() {
  const child = defineOp('ordinal-child', (ctx) => { return ctx.ok(null); });
  const parent = defineOp('ordinal-parent', async (ctx) => {
    ctx.log.info('ordinal log');
    await ctx.span('ordinal span', child);
    return ctx.ok(null);
  });
  return { child, parent };
}
`)
	manifest := independentManifest(
		independentEntry(vocabularyLogTemplate, "ordinal log"),
		independentEntry(vocabularySpanName, "ordinal span"),
	)
	ordinals := map[vocabularyKind]int{}
	for ordinal, entry := range manifest.Entries {
		ordinals[entry.Kind] = ordinal
	}
	logOrdinal := strconv.Itoa(ordinals[vocabularyLogTemplate])
	registration := regexp.MustCompile(`const (\$\$lmaoVocabulary\w*) = \$\$registerLmaoVocabulary\w*\(\{`).FindStringSubmatch(output)
	if len(registration) != 2 || !strings.Contains(output, "_state._appendWriterEntry(8)") {
		t.Fatalf("static log allocation did not use state-owned append with registered vocabulary metadata\n%s", output)
	}
	binding := registration[1]
	localDictionaryPattern := regexp.MustCompile(`localMessageDictionary:\s*Object\.freeze\(\[\s*` + regexp.QuoteMeta(binding) + `\[` + logOrdinal + `\]\s*\]\)`)
	if !localDictionaryPattern.MatchString(output) || !strings.Contains(output, "_messageIds[$$i] = 1") || strings.Contains(output, "message_nulls") {
		t.Fatalf("current static log did not bind local ID 1 without obsolete validity storage through %s\n%s", binding, output)
	}
	spanPattern := regexp.MustCompile(`ctx\.span0\([^,]+,\s*` + regexp.QuoteMeta(binding) + `\[` + strconv.Itoa(ordinals[vocabularySpanName]) + `\],\s*child\.callsitePlan\.newCtx0\(ctx\),\s*child\.callsitePlan,\s*child\.fn\)`)
	if !spanPattern.MatchString(output) {
		t.Fatalf("static span did not read its fragment ordinal through unified span0 CallsitePlan dispatch using %s\n%s", binding, output)
	}
	if strings.Contains(output, "spanStatic0(") || strings.Count(output, binding+"[") != 2 {
		t.Fatalf("static log and span did not each use exactly one registered vocabulary operand\n%s", output)
	}
}

func TestSortVocabularyEntriesUsesIDKindTagAndRecordBytes(t *testing.T) {
	entries := []vocabularyManifestEntry{
		{ID: 9, Kind: vocabularySpanName, Text: "z", Fields: []vocabularyField{}},
		{ID: 8, Kind: vocabularySpanName, Text: "first-id", Fields: []vocabularyField{}},
		{ID: 9, Kind: vocabularyLogTemplate, Text: "same", Fields: []vocabularyField{{Name: "z", Column: "column"}}},
		{ID: 9, Kind: vocabularyLogTemplate, Text: "same", Fields: []vocabularyField{}},
		{ID: 9, Kind: vocabularyLogTemplate, Text: "alpha", Fields: []vocabularyField{}},
	}
	want := []vocabularyManifestEntry{entries[1], entries[3], entries[2], entries[4], entries[0]}
	sortVocabularyEntries(entries)
	if !reflect.DeepEqual(entries, want) {
		t.Fatalf("canonical entries = %#v, want exact (id, kindTag, record bytes) order %#v", entries, want)
	}
}

func TestBuildVocabularyManifestIsIndependentOfAdditionOrderAndDuplicates(t *testing.T) {
	entries := []vocabularyManifestEntry{
		independentEntry(vocabularySpanName, "same"),
		independentEntry(vocabularyLogTemplate, "nul\x00byte", vocabularyField{Name: "request", Column: "request_id"}),
		independentEntry(vocabularyLogTemplate, "same"),
		independentEntry(vocabularySpanName, "astral-💩"),
	}
	first := collectorFromEntries(entries[0], entries[1], entries[2], entries[3], entries[1])
	second := collectorFromEntries(entries[3], entries[2], entries[1], entries[0])
	gotFirst, err := buildVocabularyManifest(first)
	if err != nil {
		t.Fatal(err)
	}
	gotSecond, err := buildVocabularyManifest(second)
	if err != nil {
		t.Fatal(err)
	}
	want := independentManifest(entries...)
	if !reflect.DeepEqual(gotFirst, want) || !reflect.DeepEqual(gotSecond, want) {
		t.Fatalf("manifests depend on traversal order or duplicates:\nfirst=%#v\nsecond=%#v\nwant=%#v", gotFirst, gotSecond, want)
	}
}

func TestValidateVocabularyManifestRejectsMalformedRecordContract(t *testing.T) {
	base := independentManifest(independentEntry(vocabularyLogTemplate, "same", vocabularyField{Name: "user", Column: "user_id"}))
	extra := independentEntry(vocabularySpanName, "extra")
	withExtra := independentManifest(base.Entries[0], extra)
	baseID := base.Entries[0].ID
	cases := []struct {
		name     string
		actual   vocabularyManifest
		expected vocabularyManifest
		want     string
	}{
		{name: "schema version", actual: func() vocabularyManifest { m := cloneManifest(base); m.SchemaVersion = 2; return m }(), expected: base, want: "LMAO1004 schemaVersion must be 1"},
		{name: "algorithm", actual: func() vocabularyManifest { m := cloneManifest(base); m.IDAlgorithm = "sha256-24-v2"; return m }(), expected: base, want: `LMAO1004 idAlgorithm must be "sha256-24-v1"`},
		{name: "uppercase hash", actual: func() vocabularyManifest { m := cloneManifest(base); m.ContentHash = strings.Repeat("A", 64); return m }(), expected: base, want: "LMAO1004 contentHash must be 64 lowercase hex characters\nLMAO1006 contentHash does not match canonical fragment"},
		{name: "entry order", actual: func() vocabularyManifest {
			m := cloneManifest(withExtra)
			m.Entries[0], m.Entries[1] = m.Entries[1], m.Entries[0]
			m.ContentHash = independentContentHash(m.Entries)
			return m
		}(), expected: withExtra, want: "LMAO1006 entries are not sorted by (id, kindTag, record bytes)"},
		{name: "record-derived ID", actual: func() vocabularyManifest {
			m := cloneManifest(base)
			m.Entries[0].Fields[0].Column = "wrong_column"
			m.ContentHash = independentContentHash(m.Entries)
			return m
		}(), expected: base, want: "LMAO1006 entry id mismatch for log_template \"same\"\nLMAO1007 missing manifest entry log_template \"same\"\nLMAO1007 stale manifest entry log_template \"same\""},
		{name: "duplicate record and ID", actual: func() vocabularyManifest {
			m := cloneManifest(base)
			m.Entries = append(m.Entries, m.Entries[0])
			m.ContentHash = independentContentHash(m.Entries)
			return m
		}(), expected: base, want: fmt.Sprintf("LMAO1005 duplicate vocabulary id %d\nLMAO1005 duplicate vocabulary record log_template %q", baseID, "same")},
		{name: "unknown kind", actual: func() vocabularyManifest {
			m := cloneManifest(base)
			m.Entries[0].Kind = vocabularyKind("mystery")
			return m
		}(), expected: base, want: "LMAO1004 entry 0 has unknown kind \"mystery\"\nLMAO1007 missing manifest entry log_template \"same\""},
		{name: "zero ID", actual: func() vocabularyManifest {
			m := cloneManifest(base)
			m.Entries[0].ID = 0
			m.ContentHash = independentContentHash(m.Entries)
			return m
		}(), expected: base, want: "LMAO1004 entry 0 id 0 is outside 1..16777215\nLMAO1006 entry id mismatch for log_template \"same\""},
		{name: "out-of-range ID", actual: func() vocabularyManifest {
			m := cloneManifest(base)
			m.Entries[0].ID = 0x01000000
			m.ContentHash = independentContentHash(m.Entries)
			return m
		}(), expected: base, want: "LMAO1004 entry 0 id 16777216 is outside 1..16777215\nLMAO1006 entry id mismatch for log_template \"same\""},
		{name: "missing entry", actual: base, expected: withExtra, want: `LMAO1007 missing manifest entry span_name "extra"`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateVocabularyManifest(tc.actual, tc.expected)
			if err == nil || err.Error() != tc.want {
				t.Fatalf("validation error =\n%v\nwant =\n%s", err, tc.want)
			}
		})
	}
}

func TestVocabularyManifestValidationDistinguishesExactAndProgramSets(t *testing.T) {
	current := independentManifest(independentEntry(vocabularyLogTemplate, "current"))
	appWide := independentManifest(current.Entries[0], independentEntry(vocabularySpanName, "app-wide-extra"))
	if err := validateVocabularyManifest(appWide, current); err == nil || err.Error() != `LMAO1007 stale manifest entry span_name "app-wide-extra"` {
		t.Fatalf("exact validation error = %v", err)
	}
	if err := validateVocabularyManifestForProgram(appWide, current); err != nil {
		t.Fatalf("program validation rejected app-wide superset: %v", err)
	}
	if err := validateVocabularyManifestForProgram(current, appWide); err == nil || err.Error() != `LMAO1007 missing manifest entry span_name "app-wide-extra"` {
		t.Fatalf("program validation error = %v", err)
	}
}

func TestLoadVocabularyManifestRejectsMalformedOrNoncanonicalJSON(t *testing.T) {
	root := t.TempDir()
	canonical, err := canonicalManifestBytes(independentManifest())
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name   string
		data   []byte
		prefix string
	}{
		{name: "invalid JSON", data: []byte(`{"schemaVersion":`), prefix: "LMAO1003 malformed vocabulary manifest:"},
		{name: "unknown field", data: []byte(`{"schemaVersion":1,"idAlgorithm":"sha256-24-v1","contentHash":"","entries":[],"extra":true}`), prefix: "LMAO1003 malformed vocabulary manifest:"},
		{name: "trailing value", data: append(append([]byte(nil), canonical...), []byte(" {}")...), prefix: "LMAO1003 malformed vocabulary manifest:"},
		{name: "missing fields", data: []byte("{\n  \"schemaVersion\": 1,\n  \"idAlgorithm\": \"sha256-24-v1\",\n  \"contentHash\": \"" + strings.Repeat("0", 64) + "\",\n  \"entries\": [{\"id\":1,\"kind\":\"log_template\",\"text\":\"x\"}]\n}\n"), prefix: "LMAO1003 malformed vocabulary manifest: entry 0 fields must be an array"},
		{name: "minified", data: bytes.TrimSpace(canonical), prefix: "LMAO1003 noncanonical vocabulary manifest bytes:"},
		{name: "CRLF", data: bytes.ReplaceAll(canonical, []byte("\n"), []byte("\r\n")), prefix: "LMAO1003 noncanonical vocabulary manifest bytes:"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(root, strings.ReplaceAll(tc.name, " ", "-")+".json")
			if err := os.WriteFile(path, tc.data, 0o644); err != nil {
				t.Fatal(err)
			}
			if _, err := loadVocabularyManifest(path); err == nil || !strings.HasPrefix(err.Error(), tc.prefix) {
				t.Fatalf("load error = %v, want prefix %q", err, tc.prefix)
			}
		})
	}
	missing := filepath.Join(root, "missing.json")
	if _, err := loadVocabularyManifest(missing); err == nil || err.Error() != "LMAO1001 vocabulary manifest missing: "+filepath.ToSlash(missing)+"; run vocabulary sync" {
		t.Fatalf("missing manifest error = %v", err)
	}
}

func TestCanonicalManifestBytesFreezesRecordShapeAndNewline(t *testing.T) {
	manifest := vocabularyManifest{SchemaVersion: 1, IDAlgorithm: "sha256-24-v1", ContentHash: strings.Repeat("0", 64), Entries: []vocabularyManifestEntry{{ID: 66051, Kind: vocabularyLogTemplate, Text: "nul\x00💩", Fields: []vocabularyField{{Name: "user", Column: "user_id"}}}}}
	got, err := canonicalManifestBytes(manifest)
	if err != nil {
		t.Fatal(err)
	}
	want := "{\n" +
		"  \"schemaVersion\": 1,\n" +
		"  \"idAlgorithm\": \"sha256-24-v1\",\n" +
		"  \"contentHash\": \"" + strings.Repeat("0", 64) + "\",\n" +
		"  \"entries\": [\n" +
		"    {\n" +
		"      \"id\": 66051,\n" +
		"      \"kind\": \"log_template\",\n" +
		"      \"text\": \"nul\\u0000💩\",\n" +
		"      \"fields\": [\n" +
		"        {\n" +
		"          \"name\": \"user\",\n" +
		"          \"column\": \"user_id\"\n" +
		"        }\n" +
		"      ]\n" +
		"    }\n" +
		"  ]\n" +
		"}\n"
	if string(got) != want {
		t.Fatalf("canonical bytes =\n%s\nwant =\n%s", got, want)
	}
	if bytes.Contains(got, []byte{'\r'}) || !bytes.HasSuffix(got, []byte("}\n")) || bytes.HasSuffix(got, []byte("\n\n")) {
		t.Fatalf("canonical manifest newline contract violated: %q", got)
	}
}

func TestWorkspaceVocabularyManifestFreezesCanonicalFortyFourRecords(t *testing.T) {
	manifest, err := loadVocabularyManifest(filepath.Join("..", "..", "..", "lmao.vocabulary.json"))
	if err != nil {
		t.Fatal(err)
	}
	const wantHash = "71704d4de103c86fc8ad67aae66e00da962909f1dfdfda0d2a05d4da1d91942a"
	if manifest.ContentHash != wantHash {
		t.Fatalf("workspace vocabulary content hash = %s, want %s", manifest.ContentHash, wantHash)
	}
	wantIDs := []uint32{
		273228, 377410, 710401, 2076023, 2386772, 2827682, 2859671, 2878065,
		3148344, 4591923, 5230921, 5261009, 6461409, 6878406, 7139468, 7140773,
		7297412, 7618545, 8012750, 8619290, 9141714, 9474871, 9909391, 10509935,
		11095986, 11413935, 11765229, 12761596, 12949665, 13304129, 13547163, 13676444,
		13700822, 14278264, 14696506, 15020928, 15317875, 15339825, 15419721, 15531707,
		15639725, 16094209, 16343363, 16369894,
	}
	gotIDs := make([]uint32, len(manifest.Entries))
	for index, entry := range manifest.Entries {
		gotIDs[index] = entry.ID
	}
	if !reflect.DeepEqual(gotIDs, wantIDs) {
		t.Fatalf("workspace vocabulary IDs = %v, want canonical 13 records %v", gotIDs, wantIDs)
	}
}

func TestWriteManifestAtomicRepeatedNoOpPreservesExactFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "lmao.vocabulary.json")
	data := []byte("canonical\x00💩\n")
	if err := writeManifestAtomic(path, data); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o400); err != nil {
		t.Fatal(err)
	}
	for attempt := 0; attempt < 2; attempt++ {
		if err := writeManifestAtomic(path, append([]byte(nil), data...)); err != nil {
			t.Fatalf("no-op write %d: %v", attempt+1, err)
		}
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) || info.Mode().Perm() != 0o400 {
		t.Fatalf("no-op write replaced existing file: bytes=%x mode=%o", got, info.Mode().Perm())
	}
	if err := writeManifestAtomic(filepath.Dir(path), []byte("cannot replace a directory")); err == nil {
		t.Fatal("writeManifestAtomic returned nil for a directory destination")
	}
}

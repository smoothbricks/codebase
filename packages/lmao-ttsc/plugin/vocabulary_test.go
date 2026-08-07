package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
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

func independentEntry(kind vocabularyKind, text string, fields ...vocabularyField) vocabularyCatalogEntry {
	record := independentRecord(text, fields)
	key := vocabularyKey{Kind: kind, Value: text, Record: string(record)}
	return vocabularyCatalogEntry{ID: uint32(independentVocabularyID(key)), Kind: kind, Text: text, Fields: append([]vocabularyField{}, fields...)}
}

func independentContentStream(entries []vocabularyCatalogEntry) []byte {
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

func independentContentHash(entries []vocabularyCatalogEntry) string {
	digest := sha256.Sum256(independentContentStream(entries))
	return hex.EncodeToString(digest[:])
}

func independentCatalog(entries ...vocabularyCatalogEntry) vocabularyCatalog {
	entries = append([]vocabularyCatalogEntry{}, entries...)
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].ID != entries[j].ID {
			return entries[i].ID < entries[j].ID
		}
		if independentKindTag(entries[i].Kind) != independentKindTag(entries[j].Kind) {
			return independentKindTag(entries[i].Kind) < independentKindTag(entries[j].Kind)
		}
		return bytes.Compare(independentRecord(entries[i].Text, entries[i].Fields), independentRecord(entries[j].Text, entries[j].Fields)) < 0
	})
	return vocabularyCatalog{Entries: entries}
}

func collectorFromEntries(entries ...vocabularyCatalogEntry) *programVocabularyCollector {
	collector := newProgramVocabularyCollector()
	for _, entry := range entries {
		key, err := keyFromCatalogEntry(entry)
		if err != nil {
			panic(err)
		}
		collector.records[key] = entry
		collector.occurrences = append(collector.occurrences, vocabularyOccurrence{Key: key})
	}
	return collector
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
		if got != want || got == 0 || got > 0x00ffffff {
			t.Fatalf("deriveVocabularyID(%v) = %d, want record-derived nonzero u24 %d", key, got, want)
		}
		if prior, exists := seen[got]; exists {
			t.Fatalf("fixtures %v and %v unexpectedly share ID %d", prior, key, got)
		}
		seen[got] = key
	}
}

func TestVocabularyFragmentArraysAndHashFreezeExactBytes(t *testing.T) {
	entries := []vocabularyCatalogEntry{
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
	catalog := independentCatalog(
		independentEntry(vocabularyLogTemplate, "ordinal log"),
		independentEntry(vocabularySpanName, "ordinal span"),
	)
	ordinals := map[vocabularyKind]int{}
	for ordinal, entry := range catalog.Entries {
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
	entries := []vocabularyCatalogEntry{
		{ID: 9, Kind: vocabularySpanName, Text: "z", Fields: []vocabularyField{}},
		{ID: 8, Kind: vocabularySpanName, Text: "first-id", Fields: []vocabularyField{}},
		{ID: 9, Kind: vocabularyLogTemplate, Text: "same", Fields: []vocabularyField{{Name: "z", Column: "column"}}},
		{ID: 9, Kind: vocabularyLogTemplate, Text: "same", Fields: []vocabularyField{}},
		{ID: 9, Kind: vocabularyLogTemplate, Text: "alpha", Fields: []vocabularyField{}},
	}
	want := []vocabularyCatalogEntry{entries[1], entries[3], entries[2], entries[4], entries[0]}
	sortVocabularyEntries(entries)
	if !reflect.DeepEqual(entries, want) {
		t.Fatalf("canonical entries = %#v, want exact (id, kindTag, record bytes) order %#v", entries, want)
	}
}

func TestBuildVocabularyCatalogIsIndependentOfAdditionOrderAndDuplicates(t *testing.T) {
	entries := []vocabularyCatalogEntry{
		independentEntry(vocabularySpanName, "same"),
		independentEntry(vocabularyLogTemplate, "nul\x00byte", vocabularyField{Name: "request", Column: "request_id"}),
		independentEntry(vocabularyLogTemplate, "same"),
		independentEntry(vocabularySpanName, "astral-💩"),
	}
	first := collectorFromEntries(entries[0], entries[1], entries[2], entries[3], entries[1])
	second := collectorFromEntries(entries[3], entries[2], entries[1], entries[0])
	gotFirst, err := buildVocabularyCatalog(first)
	if err != nil {
		t.Fatal(err)
	}
	gotSecond, err := buildVocabularyCatalog(second)
	if err != nil {
		t.Fatal(err)
	}
	want := independentCatalog(entries...)
	if !reflect.DeepEqual(gotFirst, want) || !reflect.DeepEqual(gotSecond, want) {
		t.Fatalf("catalogs depend on traversal order or duplicates:\nfirst=%#v\nsecond=%#v\nwant=%#v", gotFirst, gotSecond, want)
	}
}

func TestBuildVocabularyCatalogRejectsIDCollisionIndependentlyOfInsertionOrder(t *testing.T) {
	seen := make(map[globalVocabularyID]vocabularyCatalogEntry)
	var left, right vocabularyCatalogEntry
	for candidate := 0; candidate < 100_000; candidate++ {
		entry := independentEntry(vocabularyLogTemplate, fmt.Sprintf("collision-%06d", candidate))
		id := globalVocabularyID(entry.ID)
		if id == 0 {
			continue
		}
		if prior, exists := seen[id]; exists {
			left, right = prior, entry
			break
		}
		seen[id] = entry
	}
	if left.Text == "" {
		t.Fatal("deterministic collision fixture search found no sha256-24 collision")
	}
	if bytes.Compare(independentRecord(left.Text, left.Fields), independentRecord(right.Text, right.Fields)) > 0 {
		left, right = right, left
	}
	want := fmt.Sprintf("LMAO1009 vocabulary id collision %d: %s %q and %s %q", left.ID, left.Kind, left.Text, right.Kind, right.Text)
	for _, entries := range [][]vocabularyCatalogEntry{{left, right}, {right, left}} {
		_, err := buildVocabularyCatalog(collectorFromEntries(entries...))
		if err == nil || err.Error() != want {
			t.Fatalf("collision error = %v, want deterministic %q", err, want)
		}
	}
}

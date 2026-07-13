package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"sort"
	"strings"

	shimast "github.com/microsoft/typescript-go/shim/ast"
)

const (
	vocabularySchemaVersion = 1
	vocabularyIDAlgorithm   = "sha256-24-v1"
)

type vocabularyKind string

const (
	vocabularyLogTemplate vocabularyKind = "log_template"
	vocabularySpanName    vocabularyKind = "span_name"
)

func (kind vocabularyKind) tag() byte {
	switch kind {
	case vocabularyLogTemplate:
		return 1
	case vocabularySpanName:
		return 2
	default:
		panic("unknown vocabulary kind")
	}
}

type globalVocabularyID uint32

type vocabularyField struct {
	Name   string
	Column string
}

// Record is the canonical binary §6 record. Value remains separately available
// for deterministic diagnostics and for simple static/span callers.
type vocabularyKey struct {
	Kind   vocabularyKind
	Value  string
	Record string
}

type vocabularyOccurrence struct {
	Key  vocabularyKey
	File string
	Pos  int
}

type vocabularyCatalogEntry struct {
	ID     uint32
	Kind   vocabularyKind
	Text   string
	Fields []vocabularyField
}

type vocabularyCatalog struct {
	Entries []vocabularyCatalogEntry
}

type compilerDiagnostic struct {
	Code    string
	File    string
	Pos     int
	Message string
}

type programVocabularyCollector struct {
	occurrences []vocabularyOccurrence
	logCalls    map[*shimast.CallExpression]vocabularyKey
	spanCalls   map[*shimast.CallExpression]vocabularyKey
	records     map[vocabularyKey]vocabularyCatalogEntry
	fileKeys    map[string]map[vocabularyKey]struct{}
	diagnostics []compilerDiagnostic
}

func newProgramVocabularyCollector() *programVocabularyCollector {
	return &programVocabularyCollector{
		logCalls:  map[*shimast.CallExpression]vocabularyKey{},
		spanCalls: map[*shimast.CallExpression]vocabularyKey{},
		records:   map[vocabularyKey]vocabularyCatalogEntry{},
		fileKeys:  map[string]map[vocabularyKey]struct{}{},
	}
}

func literalVocabularyValue(node *shimast.Node) (string, bool) {
	if node == nil {
		return "", false
	}
	switch node.Kind {
	case shimast.KindStringLiteral, shimast.KindNoSubstitutionTemplateLiteral:
		return shimast.NodeText(node), true
	default:
		return "", false
	}
}

func encodeVocabularyRecord(text string, fields []vocabularyField) ([]byte, error) {
	textBytes := []byte(text)
	if uint64(len(textBytes)) > math.MaxUint32 {
		return nil, fmt.Errorf("template text exceeds u32 UTF-8 length")
	}
	if len(fields) > math.MaxUint16 {
		return nil, fmt.Errorf("template has more than 65535 fields")
	}
	size := 4 + len(textBytes) + 2
	for _, field := range fields {
		name, column := []byte(field.Name), []byte(field.Column)
		if len(name) > math.MaxUint16 || len(column) > math.MaxUint16 {
			return nil, fmt.Errorf("field descriptor %q exceeds u16 UTF-8 length", field.Name)
		}
		size += 2 + len(name) + 2 + len(column)
	}
	record := make([]byte, size)
	offset := 0
	binary.LittleEndian.PutUint32(record[offset:], uint32(len(textBytes)))
	offset += 4
	copy(record[offset:], textBytes)
	offset += len(textBytes)
	binary.LittleEndian.PutUint16(record[offset:], uint16(len(fields)))
	offset += 2
	for _, field := range fields {
		name, column := []byte(field.Name), []byte(field.Column)
		binary.LittleEndian.PutUint16(record[offset:], uint16(len(name)))
		offset += 2
		copy(record[offset:], name)
		offset += len(name)
		binary.LittleEndian.PutUint16(record[offset:], uint16(len(column)))
		offset += 2
		copy(record[offset:], column)
		offset += len(column)
	}
	return record, nil
}

func simpleVocabularyKey(kind vocabularyKind, value string) vocabularyKey {
	record, err := encodeVocabularyRecord(value, nil)
	if err != nil {
		panic(err)
	}
	return vocabularyKey{Kind: kind, Value: value, Record: string(record)}
}

func (collector *programVocabularyCollector) add(kind vocabularyKind, value string, call *shimast.CallExpression, file string) {
	collector.addRecord(kind, value, nil, call, file)
}

func (collector *programVocabularyCollector) addRecord(kind vocabularyKind, text string, fields []vocabularyField, call *shimast.CallExpression, file string) {
	record, err := encodeVocabularyRecord(text, fields)
	if err != nil {
		collector.addDiagnostic("LMAO1011", file, call.AsNode().Pos(), err.Error())
		return
	}
	key := vocabularyKey{Kind: kind, Value: text, Record: string(record)}
	copiedFields := append([]vocabularyField(nil), fields...)
	if copiedFields == nil {
		copiedFields = []vocabularyField{}
	}
	collector.records[key] = vocabularyCatalogEntry{Kind: kind, Text: text, Fields: copiedFields}
	collector.occurrences = append(collector.occurrences, vocabularyOccurrence{Key: key, File: file, Pos: call.AsNode().Pos()})
	keys := collector.fileKeys[file]
	if keys == nil {
		keys = map[vocabularyKey]struct{}{}
		collector.fileKeys[file] = keys
	}
	keys[key] = struct{}{}
	if kind == vocabularyLogTemplate {
		collector.logCalls[call] = key
	} else {
		collector.spanCalls[call] = key
	}
}

func (collector *programVocabularyCollector) addDiagnostic(code, file string, pos int, message string) {
	collector.diagnostics = append(collector.diagnostics, compilerDiagnostic{Code: code, File: filepath.ToSlash(file), Pos: pos, Message: message})
}

func (collector *programVocabularyCollector) diagnosticError() error {
	if len(collector.diagnostics) == 0 {
		return nil
	}
	diagnostics := append([]compilerDiagnostic(nil), collector.diagnostics...)
	sort.Slice(diagnostics, func(i, j int) bool {
		a, b := diagnostics[i], diagnostics[j]
		if a.Code != b.Code {
			return a.Code < b.Code
		}
		if a.File != b.File {
			return a.File < b.File
		}
		if a.Pos != b.Pos {
			return a.Pos < b.Pos
		}
		return a.Message < b.Message
	})
	lines := make([]string, len(diagnostics))
	for i, diagnostic := range diagnostics {
		lines[i] = fmt.Sprintf("%s %s:%d: %s", diagnostic.Code, diagnostic.File, diagnostic.Pos, diagnostic.Message)
	}
	return errors.New(strings.Join(lines, "\n"))
}

func deriveVocabularyID(key vocabularyKey) globalVocabularyID {
	record := []byte(key.Record)
	if len(record) == 0 {
		record, _ = encodeVocabularyRecord(key.Value, nil)
	}
	h := sha256.New()
	_, _ = h.Write([]byte{key.Kind.tag()})
	_, _ = h.Write(record)
	digest := h.Sum(nil)
	return globalVocabularyID(uint32(digest[0])<<16 | uint32(digest[1])<<8 | uint32(digest[2]))
}

func keyFromCatalogEntry(entry vocabularyCatalogEntry) (vocabularyKey, error) {
	record, err := encodeVocabularyRecord(entry.Text, entry.Fields)
	if err != nil {
		return vocabularyKey{}, err
	}
	return vocabularyKey{Kind: entry.Kind, Value: entry.Text, Record: string(record)}, nil
}

func canonicalVocabularyEntries(records map[vocabularyKey]vocabularyCatalogEntry) ([]vocabularyCatalogEntry, error) {
	entries := make([]vocabularyCatalogEntry, 0, len(records))
	byID := map[globalVocabularyID]vocabularyKey{}
	for key, source := range records {
		id := deriveVocabularyID(key)
		if id == 0 {
			return nil, fmt.Errorf("LMAO1008 reserved vocabulary id 0 for %s %q", key.Kind, key.Value)
		}
		if prior, exists := byID[id]; exists && prior != key {
			pair := []vocabularyKey{prior, key}
			sort.Slice(pair, func(i, j int) bool { return compareVocabularyKeys(pair[i], pair[j]) < 0 })
			return nil, fmt.Errorf("LMAO1009 vocabulary id collision %d: %s %q and %s %q", id, pair[0].Kind, pair[0].Value, pair[1].Kind, pair[1].Value)
		}
		byID[id] = key
		source.ID = uint32(id)
		if source.Fields == nil {
			source.Fields = []vocabularyField{}
		}
		entries = append(entries, source)
	}
	sortVocabularyEntries(entries)
	return entries, nil
}

func compareVocabularyKeys(a, b vocabularyKey) int {
	if a.Kind.tag() != b.Kind.tag() {
		return int(a.Kind.tag()) - int(b.Kind.tag())
	}
	return bytes.Compare([]byte(a.Record), []byte(b.Record))
}

func sortVocabularyEntries(entries []vocabularyCatalogEntry) {
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].ID != entries[j].ID {
			return entries[i].ID < entries[j].ID
		}
		if entries[i].Kind != entries[j].Kind {
			return entries[i].Kind.tag() < entries[j].Kind.tag()
		}
		a, _ := keyFromCatalogEntry(entries[i])
		b, _ := keyFromCatalogEntry(entries[j])
		return bytes.Compare([]byte(a.Record), []byte(b.Record)) < 0
	})
}

type vocabularyFragment struct {
	IDs      []uint32
	KindTags []byte
	UTF8     []byte
	Offsets  []int32
}

func fragmentFromEntries(entries []vocabularyCatalogEntry) (vocabularyFragment, error) {
	fragment := vocabularyFragment{
		IDs: make([]uint32, 0, len(entries)), KindTags: make([]byte, 0, len(entries)),
		Offsets: make([]int32, 1, len(entries)+1),
	}
	for _, entry := range entries {
		key, err := keyFromCatalogEntry(entry)
		if err != nil {
			return vocabularyFragment{}, err
		}
		if len(fragment.UTF8)+len(key.Record) > math.MaxInt32 {
			return vocabularyFragment{}, fmt.Errorf("vocabulary fragment exceeds i32 offsets")
		}
		fragment.IDs = append(fragment.IDs, entry.ID)
		fragment.KindTags = append(fragment.KindTags, entry.Kind.tag())
		fragment.UTF8 = append(fragment.UTF8, []byte(key.Record)...)
		fragment.Offsets = append(fragment.Offsets, int32(len(fragment.UTF8)))
	}
	return fragment, nil
}

func vocabularyContentHash(entries []vocabularyCatalogEntry) string {
	fragment, err := fragmentFromEntries(entries)
	if err != nil {
		panic(err)
	}
	stream := bytes.NewBuffer(nil)
	stream.WriteByte(vocabularySchemaVersion)
	_ = binary.Write(stream, binary.LittleEndian, uint16(len(vocabularyIDAlgorithm)))
	stream.WriteString(vocabularyIDAlgorithm)
	_ = binary.Write(stream, binary.LittleEndian, uint32(len(fragment.IDs)))
	for _, id := range fragment.IDs {
		_ = binary.Write(stream, binary.LittleEndian, id)
	}
	_ = binary.Write(stream, binary.LittleEndian, uint32(len(fragment.KindTags)))
	stream.Write(fragment.KindTags)
	_ = binary.Write(stream, binary.LittleEndian, uint32(len(fragment.UTF8)))
	stream.Write(fragment.UTF8)
	_ = binary.Write(stream, binary.LittleEndian, uint32(len(fragment.Offsets)))
	for _, offset := range fragment.Offsets {
		_ = binary.Write(stream, binary.LittleEndian, offset)
	}
	digest := sha256.Sum256(stream.Bytes())
	return hex.EncodeToString(digest[:])
}

func buildVocabularyCatalog(collector *programVocabularyCollector) (vocabularyCatalog, error) {
	entries, err := canonicalVocabularyEntries(collector.records)
	if err != nil {
		return vocabularyCatalog{}, err
	}
	return vocabularyCatalog{Entries: entries}, nil
}

func resolveVocabularyIDs(catalog vocabularyCatalog, collector *programVocabularyCollector) (map[*shimast.CallExpression]globalVocabularyID, map[*shimast.CallExpression]globalVocabularyID) {
	byKey := make(map[vocabularyKey]globalVocabularyID, len(catalog.Entries))
	for _, entry := range catalog.Entries {
		if key, err := keyFromCatalogEntry(entry); err == nil {
			byKey[key] = globalVocabularyID(entry.ID)
		}
	}
	logs := make(map[*shimast.CallExpression]globalVocabularyID, len(collector.logCalls))
	for call, key := range collector.logCalls {
		logs[call] = byKey[key]
	}
	spans := make(map[*shimast.CallExpression]globalVocabularyID, len(collector.spanCalls))
	for call, key := range collector.spanCalls {
		spans[call] = byKey[key]
	}
	return logs, spans
}

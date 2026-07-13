package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"

	shimast "github.com/microsoft/typescript-go/shim/ast"
)

const (
	vocabularySchemaVersion = 1
	vocabularyIDAlgorithm   = "sha256-24-v1"
	maxVocabularyID         = 0x00ffffff
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
	Name   string `json:"name"`
	Column string `json:"column"`
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

type vocabularyManifestEntry struct {
	ID     uint32            `json:"id"`
	Kind   vocabularyKind    `json:"kind"`
	Text   string            `json:"text"`
	Fields []vocabularyField `json:"fields"`
}

type vocabularyManifest struct {
	SchemaVersion int                       `json:"schemaVersion"`
	IDAlgorithm   string                    `json:"idAlgorithm"`
	ContentHash   string                    `json:"contentHash"`
	Entries       []vocabularyManifestEntry `json:"entries"`
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
	records     map[vocabularyKey]vocabularyManifestEntry
	fileKeys    map[string]map[vocabularyKey]struct{}
	diagnostics []compilerDiagnostic
}

func newProgramVocabularyCollector() *programVocabularyCollector {
	return &programVocabularyCollector{
		logCalls:  map[*shimast.CallExpression]vocabularyKey{},
		spanCalls: map[*shimast.CallExpression]vocabularyKey{},
		records:   map[vocabularyKey]vocabularyManifestEntry{},
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
	collector.records[key] = vocabularyManifestEntry{Kind: kind, Text: text, Fields: copiedFields}
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
		if a.Code != b.Code { return a.Code < b.Code }
		if a.File != b.File { return a.File < b.File }
		if a.Pos != b.Pos { return a.Pos < b.Pos }
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

func keyFromManifestEntry(entry vocabularyManifestEntry) (vocabularyKey, error) {
	record, err := encodeVocabularyRecord(entry.Text, entry.Fields)
	if err != nil {
		return vocabularyKey{}, err
	}
	return vocabularyKey{Kind: entry.Kind, Value: entry.Text, Record: string(record)}, nil
}

func canonicalVocabularyEntries(records map[vocabularyKey]vocabularyManifestEntry) ([]vocabularyManifestEntry, error) {
	entries := make([]vocabularyManifestEntry, 0, len(records))
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
		if source.Fields == nil { source.Fields = []vocabularyField{} }
		entries = append(entries, source)
	}
	sortVocabularyEntries(entries)
	return entries, nil
}

func compareVocabularyKeys(a, b vocabularyKey) int {
	if a.Kind.tag() != b.Kind.tag() { return int(a.Kind.tag()) - int(b.Kind.tag()) }
	return bytes.Compare([]byte(a.Record), []byte(b.Record))
}

func sortVocabularyEntries(entries []vocabularyManifestEntry) {
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].ID != entries[j].ID { return entries[i].ID < entries[j].ID }
		if entries[i].Kind != entries[j].Kind { return entries[i].Kind.tag() < entries[j].Kind.tag() }
		a, _ := keyFromManifestEntry(entries[i]); b, _ := keyFromManifestEntry(entries[j])
		return bytes.Compare([]byte(a.Record), []byte(b.Record)) < 0
	})
}

type vocabularyFragment struct {
	IDs      []uint32
	KindTags []byte
	UTF8     []byte
	Offsets  []int32
}

func fragmentFromEntries(entries []vocabularyManifestEntry) (vocabularyFragment, error) {
	fragment := vocabularyFragment{
		IDs: make([]uint32, 0, len(entries)), KindTags: make([]byte, 0, len(entries)),
		Offsets: make([]int32, 1, len(entries)+1),
	}
	for _, entry := range entries {
		key, err := keyFromManifestEntry(entry)
		if err != nil { return vocabularyFragment{}, err }
		if len(fragment.UTF8)+len(key.Record) > math.MaxInt32 { return vocabularyFragment{}, fmt.Errorf("vocabulary fragment exceeds i32 offsets") }
		fragment.IDs = append(fragment.IDs, entry.ID)
		fragment.KindTags = append(fragment.KindTags, entry.Kind.tag())
		fragment.UTF8 = append(fragment.UTF8, []byte(key.Record)...)
		fragment.Offsets = append(fragment.Offsets, int32(len(fragment.UTF8)))
	}
	return fragment, nil
}

func vocabularyContentHash(entries []vocabularyManifestEntry) string {
	fragment, err := fragmentFromEntries(entries)
	if err != nil { panic(err) }
	stream := bytes.NewBuffer(nil)
	stream.WriteByte(vocabularySchemaVersion)
	_ = binary.Write(stream, binary.LittleEndian, uint16(len(vocabularyIDAlgorithm)))
	stream.WriteString(vocabularyIDAlgorithm)
	_ = binary.Write(stream, binary.LittleEndian, uint32(len(fragment.IDs)))
	for _, id := range fragment.IDs { _ = binary.Write(stream, binary.LittleEndian, id) }
	_ = binary.Write(stream, binary.LittleEndian, uint32(len(fragment.KindTags)))
	stream.Write(fragment.KindTags)
	_ = binary.Write(stream, binary.LittleEndian, uint32(len(fragment.UTF8)))
	stream.Write(fragment.UTF8)
	_ = binary.Write(stream, binary.LittleEndian, uint32(len(fragment.Offsets)))
	for _, offset := range fragment.Offsets { _ = binary.Write(stream, binary.LittleEndian, offset) }
	digest := sha256.Sum256(stream.Bytes())
	return hex.EncodeToString(digest[:])
}

func buildVocabularyManifest(collector *programVocabularyCollector) (vocabularyManifest, error) {
	entries, err := canonicalVocabularyEntries(collector.records)
	if err != nil { return vocabularyManifest{}, err }
	return vocabularyManifest{SchemaVersion: vocabularySchemaVersion, IDAlgorithm: vocabularyIDAlgorithm, ContentHash: vocabularyContentHash(entries), Entries: entries}, nil
}

func canonicalManifestBytes(manifest vocabularyManifest) ([]byte, error) {
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil { return nil, err }
	return append(data, '\n'), nil
}

func loadVocabularyManifest(path string) (vocabularyManifest, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) { return vocabularyManifest{}, fmt.Errorf("LMAO1001 vocabulary manifest missing: %s; run vocabulary sync", filepath.ToSlash(path)) }
		return vocabularyManifest{}, fmt.Errorf("LMAO1002 read vocabulary manifest: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(data)); decoder.DisallowUnknownFields()
	var manifest vocabularyManifest
	if err := decoder.Decode(&manifest); err != nil { return vocabularyManifest{}, fmt.Errorf("LMAO1003 malformed vocabulary manifest: %w", err) }
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil { return vocabularyManifest{}, fmt.Errorf("LMAO1003 malformed vocabulary manifest: trailing JSON value") }
		return vocabularyManifest{}, fmt.Errorf("LMAO1003 malformed vocabulary manifest: %w", err)
	}
	if manifest.Entries == nil { return vocabularyManifest{}, fmt.Errorf("LMAO1003 malformed vocabulary manifest: entries must be an array") }
	for i := range manifest.Entries {
		if manifest.Entries[i].Fields == nil { return vocabularyManifest{}, fmt.Errorf("LMAO1003 malformed vocabulary manifest: entry %d fields must be an array", i) }
	}
	canonical, err := canonicalManifestBytes(manifest)
	if err != nil { return vocabularyManifest{}, fmt.Errorf("LMAO1003 malformed vocabulary manifest: %w", err) }
	if !bytes.Equal(data, canonical) { return vocabularyManifest{}, fmt.Errorf("LMAO1003 noncanonical vocabulary manifest bytes: expected two-space JSON, LF, and exactly one final newline") }
	return manifest, nil
}

func validateVocabularyManifest(actual, expected vocabularyManifest) error { return validateVocabularyManifestMode(actual, expected, true) }
func validateVocabularyManifestForProgram(actual, expected vocabularyManifest) error { return validateVocabularyManifestMode(actual, expected, false) }

func validateVocabularyManifestMode(actual, expected vocabularyManifest, requireExactSet bool) error {
	var diagnostics []string
	if actual.SchemaVersion != vocabularySchemaVersion { diagnostics = append(diagnostics, fmt.Sprintf("LMAO1004 schemaVersion must be %d", vocabularySchemaVersion)) }
	if actual.IDAlgorithm != vocabularyIDAlgorithm { diagnostics = append(diagnostics, fmt.Sprintf("LMAO1004 idAlgorithm must be %q", vocabularyIDAlgorithm)) }
	if len(actual.ContentHash) != 64 || strings.ToLower(actual.ContentHash) != actual.ContentHash { diagnostics = append(diagnostics, "LMAO1004 contentHash must be 64 lowercase hex characters")
	} else if _, err := hex.DecodeString(actual.ContentHash); err != nil { diagnostics = append(diagnostics, "LMAO1004 contentHash must be 64 lowercase hex characters") }
	seenKeys := map[vocabularyKey]struct{}{}; seenIDs := map[uint32]struct{}{}; allValid := true
	actualKeys := map[vocabularyKey]struct{}{}
	for index, entry := range actual.Entries {
		if entry.Kind != vocabularyLogTemplate && entry.Kind != vocabularySpanName { diagnostics = append(diagnostics, fmt.Sprintf("LMAO1004 entry %d has unknown kind %q", index, entry.Kind)); allValid = false; continue }
		key, err := keyFromManifestEntry(entry)
		if err != nil { diagnostics = append(diagnostics, fmt.Sprintf("LMAO1004 entry %d invalid record: %v", index, err)); allValid = false; continue }
		actualKeys[key] = struct{}{}
		if entry.ID == 0 || entry.ID > maxVocabularyID { diagnostics = append(diagnostics, fmt.Sprintf("LMAO1004 entry %d id %d is outside 1..16777215", index, entry.ID)) }
		if _, exists := seenKeys[key]; exists { diagnostics = append(diagnostics, fmt.Sprintf("LMAO1005 duplicate vocabulary record %s %q", key.Kind, key.Value)) }; seenKeys[key] = struct{}{}
		if _, exists := seenIDs[entry.ID]; exists { diagnostics = append(diagnostics, fmt.Sprintf("LMAO1005 duplicate vocabulary id %d", entry.ID)) }; seenIDs[entry.ID] = struct{}{}
		if uint32(deriveVocabularyID(key)) != entry.ID { diagnostics = append(diagnostics, fmt.Sprintf("LMAO1006 entry id mismatch for %s %q", key.Kind, key.Value)) }
	}
	canonicalOrder := append([]vocabularyManifestEntry(nil), actual.Entries...); sortVocabularyEntries(canonicalOrder)
	for i := range canonicalOrder { if !equalManifestEntry(canonicalOrder[i], actual.Entries[i]) { diagnostics = append(diagnostics, "LMAO1006 entries are not sorted by (id, kindTag, record bytes)"); break } }
	if allValid && actual.ContentHash != vocabularyContentHash(actual.Entries) { diagnostics = append(diagnostics, "LMAO1006 contentHash does not match canonical fragment") }
	expectedKeys := map[vocabularyKey]struct{}{}
	for _, entry := range expected.Entries { if key, err := keyFromManifestEntry(entry); err == nil { expectedKeys[key] = struct{}{} } }
	var missing, stale []vocabularyKey
	for key := range expectedKeys { if _, exists := actualKeys[key]; !exists { missing = append(missing, key) } }
	if requireExactSet { for key := range actualKeys { if _, exists := expectedKeys[key]; !exists { stale = append(stale, key) } } }
	sort.Slice(missing, func(i,j int) bool{return compareVocabularyKeys(missing[i],missing[j])<0}); sort.Slice(stale, func(i,j int) bool{return compareVocabularyKeys(stale[i],stale[j])<0})
	for _, key := range missing { diagnostics = append(diagnostics, fmt.Sprintf("LMAO1007 missing manifest entry %s %q", key.Kind, key.Value)) }
	for _, key := range stale { diagnostics = append(diagnostics, fmt.Sprintf("LMAO1007 stale manifest entry %s %q", key.Kind, key.Value)) }
	if len(diagnostics)>0 { sort.Strings(diagnostics); return errors.New(strings.Join(diagnostics,"\n")) }
	return nil
}

func equalManifestEntry(a,b vocabularyManifestEntry) bool {
	if a.ID!=b.ID || a.Kind!=b.Kind || a.Text!=b.Text || len(a.Fields)!=len(b.Fields) { return false }
	for i:=range a.Fields { if a.Fields[i]!=b.Fields[i] { return false } }
	return true
}

func resolveVocabularyIDs(manifest vocabularyManifest, collector *programVocabularyCollector) (map[*shimast.CallExpression]globalVocabularyID, map[*shimast.CallExpression]globalVocabularyID) {
	byKey := make(map[vocabularyKey]globalVocabularyID,len(manifest.Entries))
	for _, entry := range manifest.Entries { if key,err:=keyFromManifestEntry(entry); err==nil { byKey[key]=globalVocabularyID(entry.ID) } }
	logs:=make(map[*shimast.CallExpression]globalVocabularyID,len(collector.logCalls)); for call,key:=range collector.logCalls { logs[call]=byKey[key] }
	spans:=make(map[*shimast.CallExpression]globalVocabularyID,len(collector.spanCalls)); for call,key:=range collector.spanCalls { spans[call]=byKey[key] }
	return logs,spans
}

func writeManifestAtomic(path string,data []byte) error {
	if existing,err:=os.ReadFile(path); err==nil && bytes.Equal(existing,data) { return nil }
	dir:=filepath.Dir(path); if err:=os.MkdirAll(dir,0o755);err!=nil{return err}
	tmp,err:=os.CreateTemp(dir,".lmao.vocabulary-*.tmp"); if err!=nil{return err}; tmpName:=tmp.Name(); defer os.Remove(tmpName)
	if _,err=tmp.Write(data);err==nil{err=tmp.Sync()}; if closeErr:=tmp.Close();err==nil{err=closeErr}; if err!=nil{return err}
	if err=os.Chmod(tmpName,0o644);err!=nil{return err}; return os.Rename(tmpName,path)
}

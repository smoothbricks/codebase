package main

import (
	"strings"
	"testing"
)

func TestGenerateSpanBufferClassSourceOwnsHotStorageWithoutRuntimeCompilation(t *testing.T) {
	fields := append([]namedSchemaField{}, systemSpanBufferFields...)
	fields = append(fields,
		namedSchemaField{name: "category", field: schemaField{kind: fieldDirect, storage: storageArray}},
		namedSchemaField{name: "count", field: schemaField{kind: fieldDirect, storage: storageNumber}},
		namedSchemaField{name: "enabled", field: schemaField{kind: fieldBool, storage: storageBoolean}},
		namedSchemaField{name: "total", field: schemaField{kind: fieldDirect, storage: storageBigUint64}},
		namedSchemaField{name: "outcome", field: schemaField{kind: fieldEnum, storage: storageEnum, enumValues: []string{"failure", "success"}}},
	)

	source := generateSpanBufferClassSource(
		"$$LmaoSpanBuffer_test",
		fields,
		"mixed",
		"specialized",
		[]string{"count"},
	)
	for _, forbidden := range []string{"new Function", "eval("} {
		if strings.Contains(source, forbidden) {
			t.Fatalf("source-emitted SpanBuffer retained runtime compiler %q", forbidden)
		}
	}
	for _, required := range []string{
		"class $$LmaoSpanBuffer_test",
		"constructor(requestedCapacity,stats,parent,isChained,callsiteMetadata,opMetadata,traceRoot,vocabularyGeneration)",
		"this._writeIndex=0",
		"this.timestamp=timestampView",
		"this._logHeaders=logHeaderView",
		"this._count_values=new Float64Array",
		"get category_nulls()",
		"enabled(pos,val)",
		"this._total_values=new BigUint64Array",
		"this.outcome_enumValues=[\"failure\",\"success\"]",
		"getOrCreateOverflow()",
	} {
		if !strings.Contains(source, required) {
			t.Fatalf("source-emitted SpanBuffer missing %q", required)
		}
	}
}

func TestSpanBufferArtifactIdentityIncludesEveryLayoutDimension(t *testing.T) {
	fields := []namedSchemaField{{
		name: "outcome",
		field: schemaField{
			kind:       fieldEnum,
			storage:    storageEnum,
			enumValues: []string{"failure", "success"},
		},
	}}
	base := spanBufferArtifactName(fields, "static-only", "current", nil)
	variants := []string{
		spanBufferArtifactName(fields, "mixed", "current", nil),
		spanBufferArtifactName(fields, "static-only", "specialized", nil),
		spanBufferArtifactName(fields, "static-only", "current", []string{"outcome"}),
		spanBufferArtifactName([]namedSchemaField{{name: "count", field: schemaField{kind: fieldDirect, storage: storageNumber}}}, "static-only", "current", nil),
	}
	for _, variant := range variants {
		if variant == base {
			t.Fatalf("distinct SpanBuffer layout dimension reused artifact identity %q", base)
		}
	}
}

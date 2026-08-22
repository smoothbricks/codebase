package lmao

import (
	"strconv"

	shimast "github.com/microsoft/typescript-go/shim/ast"
)

const vocabularyRegistrationSpecifier = "@smoothbricks/lmao/vocabulary/register/v1"

func catalogEntriesForFile(catalog vocabularyCatalog, keys map[vocabularyKey]struct{}) []vocabularyCatalogEntry {
	if len(keys) == 0 {
		return nil
	}
	entries := make([]vocabularyCatalogEntry, 0, len(keys))
	for _, entry := range catalog.Entries {
		key, err := keyFromCatalogEntry(entry)
		if err != nil {
			panic(err)
		}
		if _, exists := keys[key]; exists {
			entries = append(entries, entry)
		}
	}
	if len(entries) != len(keys) {
		panic("canonical vocabulary catalog omitted a source-file record")
	}
	return entries
}

func vocabularyOrdinals(entries []vocabularyCatalogEntry) map[globalVocabularyID]int {
	ordinals := make(map[globalVocabularyID]int, len(entries))
	for ordinal, entry := range entries {
		ordinals[globalVocabularyID(entry.ID)] = ordinal
	}
	return ordinals
}

func numericArray(values []uint32) *shimast.Node {
	elements := make([]*shimast.Node, len(values))
	for index, value := range values {
		elements[index] = num(int(value))
	}
	return factory.NewArrayLiteralExpression(factory.NewNodeList(elements), false)
}

func byteArray(values []byte) *shimast.Node {
	elements := make([]*shimast.Node, len(values))
	for index, value := range values {
		elements[index] = num(int(value))
	}
	return factory.NewArrayLiteralExpression(factory.NewNodeList(elements), false)
}

func offsetArray(values []int32) *shimast.Node {
	elements := make([]*shimast.Node, len(values))
	for index, value := range values {
		elements[index] = num(int(value))
	}
	return factory.NewArrayLiteralExpression(factory.NewNodeList(elements), false)
}

func typedArray(constructor string, values *shimast.Node) *shimast.Node {
	return factory.NewNewExpression(ident(constructor), nil, factory.NewNodeList([]*shimast.Node{values}))
}

func vocabularyFragmentNode(entries []vocabularyCatalogEntry) *shimast.Node {
	fragment, err := fragmentFromEntries(entries)
	if err != nil {
		panic(err)
	}
	properties := []*shimast.Node{
		factory.NewPropertyAssignment(nil, ident("schemaVersion"), nil, nil, num(vocabularySchemaVersion)),
		factory.NewPropertyAssignment(nil, ident("idAlgorithm"), nil, nil, str(vocabularyIDAlgorithm)),
		factory.NewPropertyAssignment(nil, ident("contentHash"), nil, nil, str(vocabularyContentHash(entries))),
		factory.NewPropertyAssignment(nil, ident("ids"), nil, nil, typedArray("Uint32Array", numericArray(fragment.IDs))),
		factory.NewPropertyAssignment(nil, ident("kindTags"), nil, nil, typedArray("Uint8Array", byteArray(fragment.KindTags))),
		factory.NewPropertyAssignment(nil, ident("utf8"), nil, nil, typedArray("Uint8Array", byteArray(fragment.UTF8))),
		factory.NewPropertyAssignment(nil, ident("offsets"), nil, nil, typedArray("Int32Array", offsetArray(fragment.Offsets))),
	}
	return factory.NewObjectLiteralExpression(factory.NewNodeList(properties), true)
}

// generatedBindingName reproduces the non-optimistic unique-name generator the
// tsgo printer applies to EmitContext.NewUniqueName identifiers
// (NameGenerator.makeUniqueName): the base always gains a `_<n>` suffix, and n
// is the first index neither the source file nor an earlier call already binds.
//
// The name is resolved here, into a plain identifier, rather than deferred to
// the printer: the compiler host that prints this tree owns a different
// EmitContext than any this transform could register auto-generate info in, and
// an unresolved generated identifier falls back to printing its raw base text —
// silently shadowing a user binding of the same name.
func generatedBindingName(sf *shimast.SourceFile, base string, taken map[string]bool) string {
	for index := 1; ; index++ {
		candidate := base + "_" + strconv.Itoa(index)
		if taken[candidate] {
			continue
		}
		if _, declared := sf.Identifiers[candidate]; declared {
			continue
		}
		taken[candidate] = true
		return candidate
	}
}

func vocabularyRegistrationStatements(sf *shimast.SourceFile, entries []vocabularyCatalogEntry) (*shimast.Node, []*shimast.Node) {
	if len(entries) == 0 {
		return nil, nil
	}
	taken := map[string]bool{}
	binding := ident(generatedBindingName(sf, "$$lmaoVocabulary", taken))
	register := ident(generatedBindingName(sf, "$$registerLmaoVocabulary", taken))
	symbol := callExpr(propAccess(ident("Symbol"), "for"), []*shimast.Node{str(vocabularyRegistrationSpecifier)})
	callback := factory.NewElementAccessExpression(ident("globalThis"), nil, symbol, shimast.NodeFlagsNone)
	importDeclaration := factory.NewImportDeclaration(nil, nil, str(vocabularyRegistrationSpecifier), nil)
	registerDeclaration := constDecl(register, callback)
	typeofRegister := factory.NewTypeOfExpression(register)
	unavailable := factory.NewBinaryExpression(nil, typeofRegister, nil, factory.NewToken(shimast.KindExclamationEqualsEqualsToken), str("function"))
	throwUnavailable := factory.NewThrowStatement(factory.NewNewExpression(ident("Error"), nil,
		factory.NewNodeList([]*shimast.Node{str("LMAO_VOCABULARY_ABI_UNAVAILABLE")})))
	guard := factory.NewIfStatement(unavailable, factory.NewBlock(factory.NewNodeList([]*shimast.Node{throwUnavailable}), false), nil)
	registration := constDecl(binding, callExpr(register, []*shimast.Node{vocabularyFragmentNode(entries)}))
	return binding, []*shimast.Node{importDeclaration, registerDeclaration, guard, registration}
}

func prependVocabularyRegistration(sf *shimast.SourceFile, statements []*shimast.Node) {
	if len(statements) == 0 {
		return
	}
	// Preserve directive prologues. ESM imports are evaluated before the module
	// body regardless of their textual position, so the installer still runs
	// before the registration call.
	index := 0
	for index < len(sf.Statements.Nodes) {
		statement := sf.Statements.Nodes[index]
		if statement.Kind == shimast.KindImportDeclaration || statement.Kind == shimast.KindImportEqualsDeclaration {
			index++
			continue
		}
		if statement.Kind == shimast.KindExpressionStatement {
			expression := statement.AsExpressionStatement().Expression
			if expression.Kind == shimast.KindStringLiteral {
				index++
				continue
			}
		}
		break
	}
	combined := make([]*shimast.Node, 0, len(sf.Statements.Nodes)+len(statements))
	combined = append(combined, sf.Statements.Nodes[:index]...)
	combined = append(combined, statements...)
	combined = append(combined, sf.Statements.Nodes[index:]...)
	sf.Statements = factory.NewNodeList(combined)
}

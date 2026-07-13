package main

import (
	"strings"

	shimast "github.com/microsoft/typescript-go/shim/ast"
	shimchecker "github.com/microsoft/typescript-go/shim/checker"
)

func isPlaceholderStart(ch byte) bool {
	return ch == '_' || ch == '$' || ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z'
}

func isPlaceholderContinue(ch byte) bool {
	return isPlaceholderStart(ch) || ch >= '0' && ch <= '9'
}

// parseStructuredTemplate decodes brace escapes and returns placeholders in
// source order. A doubled brace is always literal, so the obsolete
// {{field}} spelling cannot be accepted as a placeholder alias.
func parseStructuredTemplate(text string) (string, []string, bool) {
	var cooked strings.Builder
	cooked.Grow(len(text))
	var placeholders []string
	for i := 0; i < len(text); {
		switch text[i] {
		case '{':
			if i+1 < len(text) && text[i+1] == '{' {
				cooked.WriteByte('{')
				i += 2
				continue
			}
			end := i + 1
			for end < len(text) && text[end] != '}' {
				if text[end] == '{' {
					return "", nil, false
				}
				end++
			}
			if end == len(text) || end == i+1 {
				return "", nil, false
			}
			name := text[i+1 : end]
			if !isPlaceholderStart(name[0]) {
				return "", nil, false
			}
			for j := 1; j < len(name); j++ {
				if !isPlaceholderContinue(name[j]) {
					return "", nil, false
				}
			}
			cooked.WriteByte('{')
			cooked.WriteString(name)
			cooked.WriteByte('}')
			placeholders = append(placeholders, name)
			i = end + 1
		case '}':
			if i+1 >= len(text) || text[i+1] != '}' {
				return "", nil, false
			}
			cooked.WriteByte('}')
			i += 2
		default:
			cooked.WriteByte(text[i])
			i++
		}
	}
	return cooked.String(), placeholders, true
}

func (t *fileTransformer) addLogDiagnostic(code string, call *shimast.CallExpression, message string) {
	if t.vocabulary != nil {
		t.vocabulary.addDiagnostic(code, t.file.FileName(), call.AsNode().Pos(), message)
	}
}

func logFieldType(chk *shimchecker.Checker, logType *shimchecker.Type, name string) string {
	if logType == nil {
		return ""
	}
	propType := shimchecker.Checker_getTypeOfPropertyOfType(chk, logType, name)
	if propType == nil {
		return ""
	}
	sigs := shimchecker.Checker_getSignaturesOfType(chk, propType, shimchecker.SignatureKindCall)
	if len(sigs) == 0 {
		return ""
	}
	params := shimchecker.Signature_parameters(sigs[0])
	if len(params) == 0 {
		return ""
	}
	return chk.TypeToString(shimchecker.Checker_getTypeOfSymbol(chk, params[0]))
}

func resolvedLogEntryType(chk *shimchecker.Checker, call *shimast.CallExpression) *shimchecker.Type {
	signature := chk.GetResolvedSignature(call.AsNode())
	if signature == nil {
		return nil
	}
	return shimchecker.Checker_getReturnTypeOfSignature(chk, signature)
}

func resolvedLogSchema(chk *shimchecker.Checker, receiverType *shimchecker.Type, call *shimast.CallExpression) map[string]schemaField {
	schema := extractLogSchema(chk, receiverType)
	for name, field := range extractLogSchema(chk, resolvedLogEntryType(chk, call)) {
		schema[name] = field
	}
	return schema
}

func fieldValueMatches(chk *shimchecker.Checker, value *shimast.Node, expected string) bool {
	actualType := chk.GetTypeAtLocation(value)
	if actualType == nil {
		return false
	}
	actual := chk.TypeToString(actualType)
	switch expected {
	case "string":
		return actual == "string" || len(actual) >= 2 && (actual[0] == '\'' || actual[0] == '"')
	case "number":
		return actual == "number" || value.Kind == shimast.KindNumericLiteral
	case "boolean":
		return actual == "boolean" || actual == "true" || actual == "false"
	default:
		if len(actual) >= 2 && (actual[0] == '\'' || actual[0] == '"') {
			needle := strings.Trim(actual, "\"'")
			for _, member := range strings.Split(expected, " | ") {
				if strings.Trim(member, "\"'") == needle {
					return true
				}
			}
		}
		return actual == expected
	}
}

// collectLogVocabulary validates and registers one direct logger call while the
// checker still sees the untouched source tree.
func (t *fileTransformer) collectLogVocabulary(call *shimast.CallExpression) {
	if t.checker == nil || t.vocabulary == nil || call.Expression.Kind != shimast.KindPropertyAccessExpression {
		return
	}
	member := call.Expression.AsPropertyAccessExpression()
	level := shimast.NodeText(member.Name())
	if !logMethods[level] {
		return
	}
	receiver := member.Expression
	receiverType := t.checker.GetTypeAtLocation(receiver)
	if receiverType == nil || !isSpanLoggerType(t.checker, receiverType) {
		return
	}
	if !hasLmaoCallProvenance(t.checker, call) {
		t.addLogDiagnostic("LMAO_LOGGER_PROOF_REQUIRED", call, "logger receiver is not proven to originate from @smoothbricks/lmao")
		return
	}
	args := call.Arguments.Nodes
	if len(args) == 0 {
		if level == "info" || level == "warn" || level == "error" {
			t.addLogDiagnostic("LMAO_DYNAMIC_OPERATIONAL_TEXT", call, "operational log text must be a static literal")
		} else {
			t.addLogDiagnostic("LMAO_AVOIDABLE_INTERPOLATION", call, "diagnostic log text must be a literal template or raw string expression")
		}
		return
	}
	text, literal := literalLogMessage(args[0])
	if !literal {
		if level == "info" || level == "warn" || level == "error" {
			t.addLogDiagnostic("LMAO_DYNAMIC_OPERATIONAL_TEXT", call, "operational log text must be a static literal")
		} else if args[0].Kind == shimast.KindTemplateExpression || args[0].Kind == shimast.KindBinaryExpression {
			t.addLogDiagnostic("LMAO_AVOIDABLE_INTERPOLATION", call, "use a literal template and structured fields instead of interpolation or concatenation")
		}
		return
	}
	if len(args) > 2 {
		t.addLogDiagnostic("LMAO_FIELDS_NOT_OBJECT_LITERAL", call, "structured log calls accept exactly one plain object field bag")
		return
	}
	cooked, placeholders, validTemplate := parseStructuredTemplate(text)
	if !validTemplate {
		t.addLogDiagnostic("LMAO_PLACEHOLDER_MISMATCH", call, "template contains malformed brace syntax")
		return
	}
	placeholderSet := make(map[string]bool, len(placeholders))
	for _, name := range placeholders {
		if placeholderSet[name] {
			t.addLogDiagnostic("LMAO_PLACEHOLDER_MISMATCH", call, "template placeholders must be duplicate-free")
			return
		}
		placeholderSet[name] = true
	}
	if len(args) == 1 {
		if len(placeholders) != 0 {
			t.addLogDiagnostic("LMAO_PLACEHOLDER_MISMATCH", call, "every placeholder requires a matching structured field")
			return
		}
		t.vocabulary.addRecord(vocabularyLogTemplate, cooked, nil, call, t.file.FileName())
		return
	}
	if args[1].Kind != shimast.KindObjectLiteralExpression {
		t.addLogDiagnostic("LMAO_FIELDS_NOT_OBJECT_LITERAL", call, "structured fields must be a plain object literal")
		return
	}
	object := args[1].AsObjectLiteralExpression()
	fields := make([]vocabularyField, 0, len(object.Properties.Nodes))
	fieldSet := make(map[string]bool, len(object.Properties.Nodes))
	for _, property := range object.Properties.Nodes {
		var name string
		var value *shimast.Node
		switch property.Kind {
		case shimast.KindPropertyAssignment:
			assignment := property.AsPropertyAssignment()
			nameNode := assignment.Name()
			if nameNode.Kind != shimast.KindIdentifier && nameNode.Kind != shimast.KindStringLiteral {
				t.addLogDiagnostic("LMAO_FIELDS_NOT_OBJECT_LITERAL", call, "structured fields cannot use computed keys")
				return
			}
			name, value = shimast.NodeText(nameNode), assignment.Initializer
		case shimast.KindShorthandPropertyAssignment:
			name = shimast.NodeText(property.Name())
			value = property.Name()
		default:
			t.addLogDiagnostic("LMAO_FIELDS_NOT_OBJECT_LITERAL", call, "structured fields cannot use spreads, accessors, or methods")
			return
		}
		if fieldSet[name] || !placeholderSet[name] {
			t.addLogDiagnostic("LMAO_PLACEHOLDER_MISMATCH", call, "placeholder and field keys must form the same duplicate-free set")
			return
		}
		fieldSchemaType := resolvedLogEntryType(t.checker, call)
		expected := logFieldType(t.checker, fieldSchemaType, name)
		if expected == "" {
			expected = logFieldType(t.checker, receiverType, name)
		}
		if expected == "" || !fieldValueMatches(t.checker, value, expected) {
			actual := "<unresolved>"
			if actualType := t.checker.GetTypeAtLocation(value); actualType != nil {
				actual = t.checker.TypeToString(actualType)
			}
			t.addLogDiagnostic("LMAO_PLACEHOLDER_MISMATCH", call, "structured field "+name+" has checker type "+actual+", incompatible with schema type "+expected)
			return
		}
		fieldSet[name] = true
		fields = append(fields, vocabularyField{Name: name, Column: name})
	}
	if len(fieldSet) != len(placeholderSet) {
		t.addLogDiagnostic("LMAO_PLACEHOLDER_MISMATCH", call, "placeholder and field keys must match exactly")
		return
	}
	t.vocabulary.addRecord(vocabularyLogTemplate, cooked, fields, call, t.file.FileName())
}

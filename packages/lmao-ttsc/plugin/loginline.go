// loginline.go — partial inlining of statement-level log chains.
//
// The SpanBuffer design makes this a PARTIAL inline by construction: row
// allocation stays runtime (`logger._checkOverflow()` +
// `buffer._traceRoot.writeLogEntry(buffer, ENTRY_TYPE)` — overflow chaining,
// capacity tuning, and timestamps live there), while every fluent dispatch
// after it becomes a direct write at the returned row index through the
// buffer's STABLE column getters (lazy getters allocate on first access, so
// the transformer needs no eager/lazy knowledge).
//
//	ctx.log.info('msg').userId('u1');
//
// becomes
//
//	{
//	  const $$l = ctx.log;
//	  $$l._checkOverflow();
//	  const $$b = $$l._buffer;
//	  const $$i = $$b._traceRoot.writeLogEntry($$b, 8);
//	  $$l._writeIndex = $$i;
//	  if ($$b.message_values) { $$b.message_values[$$i] = 'msg';
//	    if ($$b.message_nulls) $$b.message_nulls[$$i >>> 3] |= 1 << ($$i & 7); }
//	  $$b.constructor.stats.totalWrites++;
//	  if ($$b.line_values) { $$b.line_values[$$i] = 3; ... }
//	  if ($$b.userId_values) { $$b.userId_values[$$i] = 'u1'; ... }
//	}
//
// Parity contract is the GENERATED SpanLogger source
// (packages/lmao/src/lib/codegen/spanLoggerGenerator.ts): raw getter writes
// with the same if-guards for message/line/category/text/number fields;
// enum and boolean fields go through the buffer's positional method
// (`$$b.field($$i, v)`) exactly like the generated setters, with the
// string→index switch constant-folded for literal enum values.
//
// Conservative bails (chain left untouched, runtime path is always correct):
// receiver not Checker-proved SpanLogger/GeneratedSpanLogger, a second
// log-level call in the same chain, a chained method that is neither a
// schema field nor line/with, non-single-arg links, non-object-literal
// with(), or non-statement context.
package main

import (
	"sort"
	"strings"

	shimast "github.com/microsoft/typescript-go/shim/ast"
	shimchecker "github.com/microsoft/typescript-go/shim/checker"
)

var spanLoggerTypeNames = []string{"SpanLogger", "GeneratedSpanLogger", "FluentLogEntry"}

// Entry-type dictionary values, pinned to
// packages/lmao/src/lib/schema/systemSchema.ts (ENTRY_TYPE_*). The fixture
// parity test exercises these; a runtime renumbering fails it.
var logEntryTypes = map[string]int{
	"trace": 6,
	"debug": 7,
	"info":  8,
	"warn":  9,
	"error": 10,
}

type logInline struct {
	list       *shimast.NodeList
	index      int
	logExpr    *shimast.Node // the `ctx.log` expression
	level      string        // info/debug/...
	message    *shimast.Node
	templateID globalVocabularyID // nonzero only for checker-proved registered literals
	line       int           // source line of the log call (spec 01o §6, folded in)
	lineArg    *shimast.Node // explicit .line(N) argument if present in source
	writes     []tagWrite    // chained attribute writes in execution order
	schema     map[string]schemaField
}

func isSpanLoggerType(chk *shimchecker.Checker, t *shimchecker.Type) bool {
	name := chk.TypeToString(t)
	for _, w := range spanLoggerTypeNames {
		if name == w || strings.HasPrefix(name, w+"<") {
			return true
		}
	}
	if sym := shimchecker.Type_getTypeNameSymbol(t); sym != nil {
		for _, w := range spanLoggerTypeNames {
			if sym.Name == w {
				return true
			}
		}
	}
	return false
}

func schemaFieldFromSetterCall(chk *shimchecker.Checker, call *shimast.CallExpression) (schemaField, bool) {
	signature := chk.GetResolvedSignature(call.AsNode())
	if signature == nil {
		return schemaField{}, false
	}
	params := shimchecker.Signature_parameters(signature)
	if len(params) == 0 {
		return schemaField{}, false
	}
	paramType := shimchecker.Checker_getTypeOfSymbol(chk, params[0])
	if paramType == nil {
		return schemaField{}, false
	}
	if paramType.IsUnion() {
		values := make([]string, 0, len(paramType.Types()))
		for _, member := range paramType.Types() {
			if !member.IsStringLiteral() {
				memberText := chk.TypeToString(member)
				if memberText == "null" || memberText == "undefined" {
					continue
				}
				values = nil
				break
			}
			value, ok := member.AsLiteralType().Value().(string)
			if !ok {
				values = nil
				break
			}
			values = append(values, value)
		}
		if len(values) > 0 {
			sort.Strings(values)
			return schemaField{kind: fieldEnum, enumValues: values}, true
		}
	}
	switch chk.TypeToString(paramType) {
	case "string", "number":
		return schemaField{kind: fieldDirect}, true
	case "boolean":
		return schemaField{kind: fieldBool}, true
	default:
		return schemaField{}, false
	}
}

func finiteStringLiteralUnion(chk *shimchecker.Checker, typ *shimchecker.Type) []string {
	if typ == nil || !typ.IsUnion() {
		return nil
	}
	values := make([]string, 0, len(typ.Types()))
	for _, member := range typ.Types() {
		if member.IsStringLiteral() {
			if value, ok := member.AsLiteralType().Value().(string); ok {
				values = append(values, value)
				continue
			}
		}
		memberText := chk.TypeToString(member)
		if memberText != "null" && memberText != "undefined" {
			return nil
		}
	}
	if len(values) == 0 {
		return nil
	}
	sort.Strings(values)
	return values
}


// findLogInline analyzes a statement-level call chain. Checker queries only;
// no mutation (must run in the collect phase).
func (t *fileTransformer) findLogInline(call *shimast.CallExpression) (*logInline, bool) {
	if t.checker == nil {
		return nil, false
	}
	type link struct {
		name string
		args []*shimast.Node
		node *shimast.CallExpression
	}
	var links []link
	current := call
	for {
		expr := current.Expression
		if expr.Kind != shimast.KindPropertyAccessExpression {
			return nil, false
		}
		pa := expr.AsPropertyAccessExpression()
		links = append([]link{{name: shimast.NodeText(pa.Name()), args: current.Arguments.Nodes, node: current}}, links...)
		next := pa.Expression
		if next.Kind == shimast.KindCallExpression {
			current = next.AsCallExpression()
			continue
		}
		// Chain root: the receiver of the FIRST link. It must be the log
		// object, Checker-proved.
		if len(links) == 0 || !logMethods[links[0].name] {
			return nil, false
		}
		recvType := t.checker.GetTypeAtLocation(next)
		if recvType == nil || !isSpanLoggerType(t.checker, recvType) {
			return nil, false
		}
		if len(links[0].args) < 1 || len(links[0].args) > 2 {
			return nil, false
		}
		templateID := t.staticLogIDs[links[0].node]
		if templateID == 0 {
			return nil, false // preserve permitted raw debug/trace calls byte-for-byte
		}

		in := &logInline{
			logExpr:    next,
			level:      links[0].name,
			message:    links[0].args[0],
			templateID: templateID,
			line:       t.lineOf(links[0].node.AsNode()),
			schema:     resolvedLogSchema(t.checker, recvType, links[0].node),
		}
		if len(links[0].args) == 2 {
			if links[0].args[1].Kind != shimast.KindObjectLiteralExpression {
				return nil, false
			}
			fieldsType := t.checker.GetContextualType(links[0].args[1], 0)
			for _, prop := range links[0].args[1].AsObjectLiteralExpression().Properties.Nodes {
				var write tagWrite
				switch prop.Kind {
				case shimast.KindPropertyAssignment:
					assignment := prop.AsPropertyAssignment()
					nameNode := assignment.Name()
					if nameNode.Kind != shimast.KindIdentifier && nameNode.Kind != shimast.KindStringLiteral {
						return nil, false
					}
					write = tagWrite{field: shimast.NodeText(nameNode), arg: assignment.Initializer}
				case shimast.KindShorthandPropertyAssignment:
					write = tagWrite{field: shimast.NodeText(prop.Name()), arg: prop.Name()}
				default:
					return nil, false
				}
				if fieldsType != nil {
					if propertyType := shimchecker.Checker_getTypeOfPropertyOfType(t.checker, fieldsType, write.field); propertyType != nil {
						if values := finiteStringLiteralUnion(t.checker, propertyType); len(values) > 0 {
							in.schema[write.field] = schemaField{kind: fieldEnum, enumValues: values}
						}
					}
				}
				in.writes = append(in.writes, write)
			}
		}
		for _, l := range links[1:] {
			switch {
			case logMethods[l.name]:
				return nil, false // second log entry in one chain: bail
			case l.name == "line":
				if len(l.args) != 1 {
					return nil, false
				}
				in.lineArg = l.args[0]
			case l.name == "with":
				if len(l.args) != 1 || l.args[0].Kind != shimast.KindObjectLiteralExpression {
					return nil, false
				}
				for _, prop := range l.args[0].AsObjectLiteralExpression().Properties.Nodes {
					if prop.Kind != shimast.KindPropertyAssignment {
						return nil, false
					}
					p := prop.AsPropertyAssignment()
					nameNode := p.Name()
					if nameNode.Kind != shimast.KindIdentifier && nameNode.Kind != shimast.KindStringLiteral {
						return nil, false
					}
					field := shimast.NodeText(nameNode)
					if _, known := in.schema[field]; !known {
						return nil, false
					}
					in.writes = append(in.writes, tagWrite{field: field, arg: p.Initializer})
				}
			default:
				if len(l.args) != 1 {
					return nil, false
				}
				if _, known := in.schema[l.name]; !known {
					field, resolved := schemaFieldFromSetterCall(t.checker, l.node)
					if !resolved {
						return nil, false // not a schema field setter: bail, never miscompile
					}
					in.schema[l.name] = field
				}
				contextualType := t.checker.GetContextualType(l.args[0], 0)
				if values := finiteStringLiteralUnion(t.checker, contextualType); len(values) > 0 {
					// A finite contextual string union comes from the checker-proven
					// schema setter; broad category/text setters contextualize as string.
					in.schema[l.name] = schemaField{kind: fieldEnum, enumValues: values}
				}
				if in.schema[l.name].kind == fieldDirect {
					if values := finiteStringLiteralUnion(t.checker, t.checker.GetTypeAtLocation(l.args[0])); len(values) > 0 {
						in.schema[l.name] = schemaField{kind: fieldEnum, enumValues: values}
					}
				}
				in.writes = append(in.writes, tagWrite{field: l.name, arg: l.args[0]})
			}
		}
		return in, true
	}
}

// extractLogSchema reuses the tag schema extraction but excludes the log/system
// methods that would otherwise classify as string fields.
func extractLogSchema(chk *shimchecker.Checker, logType *shimchecker.Type) map[string]schemaField {
	schema := extractSchema(chk, logType)
	// Generated setters include null/undefined in their parameter union.
	// extractSchema intentionally recognizes only all-literal unions, so
	// recover the precise enum dictionary here while ignoring nullable members.
	for _, symbol := range shimchecker.Checker_getPropertiesOfType(chk, logType) {
		name := symbol.Name
		if name == "with" || strings.HasPrefix(name, "_") || logMethods[name] || name == "line" {
			continue
		}
		propertyType := shimchecker.Checker_getTypeOfSymbol(chk, symbol)
		signatures := shimchecker.Checker_getSignaturesOfType(chk, propertyType, shimchecker.SignatureKindCall)
		if len(signatures) == 0 {
			continue
		}
		parameters := shimchecker.Signature_parameters(signatures[0])
		if len(parameters) == 0 {
			continue
		}
		parameterType := shimchecker.Checker_getTypeOfSymbol(chk, parameters[0])
		if parameterType == nil || !parameterType.IsUnion() {
			continue
		}
		values := make([]string, 0, len(parameterType.Types()))
		for _, member := range parameterType.Types() {
			if member.IsStringLiteral() {
				if value, ok := member.AsLiteralType().Value().(string); ok {
					values = append(values, value)
					continue
				}
			}
			memberText := chk.TypeToString(member)
			if memberText != "null" && memberText != "undefined" {
				values = nil
				break
			}
		}
		if len(values) > 0 {
			sort.Strings(values)
			schema[name] = schemaField{kind: fieldEnum, enumValues: values}
		}
	}
	for name := range logMethods {
		delete(schema, name)
	}
	delete(schema, "line")
	return schema
}

// --- emission -------------------------------------------------------------------

// guardedRawWrite emits the generated-setter shape:
//
//	if (b.f_values) { b.f_values[i] = v; if (b.f_nulls) b.f_nulls[i>>>3] |= 1 << (i&7); }
func guardedRawWrite(buf, idx *shimast.Node, field string, value *shimast.Node) *shimast.Node {
	values := func() *shimast.Node { return propAccess(buf, field+"_values") }
	nulls := func() *shimast.Node { return propAccess(buf, field+"_nulls") }
	at := func(arr *shimast.Node) *shimast.Node {
		return factory.NewElementAccessExpression(arr, nil, idx, shimast.NodeFlagsNone)
	}
	// b.f_nulls[i >>> 3] |= 1 << (i & 7)
	nullSlot := factory.NewElementAccessExpression(nulls(), nil,
		factory.NewBinaryExpression(nil, idx, nil, factory.NewToken(shimast.KindGreaterThanGreaterThanGreaterThanToken), num(3)),
		shimast.NodeFlagsNone)
	nullBit := factory.NewBinaryExpression(nil, num(1), nil, factory.NewToken(shimast.KindLessThanLessThanToken),
		factory.NewParenthesizedExpression(
			factory.NewBinaryExpression(nil, idx, nil, factory.NewToken(shimast.KindAmpersandToken), num(7))))
	setNull := factory.NewIfStatement(nulls(),
		factory.NewBlock(factory.NewNodeList([]*shimast.Node{
			binaryStmt(nullSlot, shimast.KindBarEqualsToken, nullBit),
		}), false), nil)
	body := []*shimast.Node{
		binaryStmt(at(values()), shimast.KindEqualsToken, value),
		setNull,
	}
	return factory.NewIfStatement(values(), factory.NewBlock(factory.NewNodeList(body), true), nil)
}

// fixedSchemaWrite emits one unconditional fixed-column value store. Schema
// resolution proves the lane exists; the null bitmap remains optional.
func fixedSchemaWrite(buf, idx *shimast.Node, field string, value *shimast.Node) []*shimast.Node {
	values := propAccess(buf, field+"_values")
	nulls := func() *shimast.Node { return propAccess(buf, field+"_nulls") }
	valueSlot := factory.NewElementAccessExpression(values, nil, idx, shimast.NodeFlagsNone)
	nullSlot := factory.NewElementAccessExpression(nulls(), nil,
		factory.NewBinaryExpression(nil, idx, nil, factory.NewToken(shimast.KindGreaterThanGreaterThanGreaterThanToken), num(3)),
		shimast.NodeFlagsNone)
	nullBit := factory.NewBinaryExpression(nil, num(1), nil, factory.NewToken(shimast.KindLessThanLessThanToken),
		factory.NewParenthesizedExpression(factory.NewBinaryExpression(nil, idx, nil, factory.NewToken(shimast.KindAmpersandToken), num(7))))
	return []*shimast.Node{
		binaryStmt(valueSlot, shimast.KindEqualsToken, value),
		factory.NewIfStatement(nulls(), factory.NewBlock(factory.NewNodeList([]*shimast.Node{
			binaryStmt(nullSlot, shimast.KindBarEqualsToken, nullBit),
		}), false), nil),
	}
}

// applyLogInlines splices replacement blocks (phase B — no checker use).
func (t *fileTransformer) applyLogInlines(inlines []logInline) {
	for _, in := range inlines {
		logger := ident("$$l")
		buf := ident("$$b")
		idx := ident("$$i")
		vocabularyOperand := func() *shimast.Node {
			if in.templateID == 0 { return num(0) }
			return t.staticVocabularyOperand(in.templateID)
		}
		// Overflow happy-path inline: one compare instead of a method call;
		// the (rare) overflow path still runs the runtime's _checkOverflow,
		// which owns buffer switching, capacity tuning, and scope prefill.
		overflowCheck := factory.NewIfStatement(
			factory.NewBinaryExpression(nil,
				propAccess(propAccess(ident("$$l"), "_buffer"), "_writeIndex"), nil,
				factory.NewToken(shimast.KindGreaterThanEqualsToken),
				propAccess(propAccess(ident("$$l"), "_buffer"), "_capacity")),
			factory.NewBlock(factory.NewNodeList([]*shimast.Node{
				factory.NewExpressionStatement(callExpr(propAccess(ident("$$l"), "_checkOverflow"), nil)),
			}), false), nil)
		stmts := []*shimast.Node{constDecl(logger, in.logExpr)}
		// Evaluate arguments before entering the logger path. A throwing field
		// initializer therefore cannot leave a partially written row.
		capturedWrites := make([]tagWrite, len(in.writes))
		for index, write := range in.writes {
			value := ident("$$f" + itoa(index))
			stmts = append(stmts, constDecl(value, write.arg))
			capturedWrites[index] = tagWrite{field: write.field, arg: value}
		}
		stmts = append(stmts,
			overflowCheck,
			constDecl(buf, propAccess(logger, "_buffer")),
			constDecl(idx, callExpr(propAccess(propAccess(buf, "_traceRoot"), "writeLogEntry"),
				[]*shimast.Node{buf, num(logEntryTypes[in.level]), vocabularyOperand()})),
			binaryStmt(propAccess(logger, "_writeIndex"), shimast.KindEqualsToken, idx),
		)
		if in.templateID != 0 {
			packed := factory.NewBinaryExpression(nil,
				factory.NewParenthesizedExpression(factory.NewBinaryExpression(nil,
					factory.NewParenthesizedExpression(factory.NewBinaryExpression(nil, vocabularyOperand(), nil,
						factory.NewToken(shimast.KindLessThanLessThanToken), num(8))), nil,
					factory.NewToken(shimast.KindBarToken), num(logEntryTypes[in.level]))), nil,
				factory.NewToken(shimast.KindGreaterThanGreaterThanGreaterThanToken), num(0))
			stmts = append(stmts, binaryStmt(
				factory.NewElementAccessExpression(propAccess(buf, "_logHeaders"), nil, idx, shimast.NodeFlagsNone),
				shimast.KindEqualsToken, packed))
		} else {
			stmts = append(stmts, guardedRawWrite(buf, idx, "message", in.message))
		}
		stmts = append(stmts, factory.NewExpressionStatement(factory.NewPostfixUnaryExpression(
			propAccess(propAccess(propAccess(buf, "constructor"), "stats"), "totalWrites"),
			shimast.KindPlusPlusToken)))

		lineValue := in.lineArg
		if lineValue == nil {
			lineValue = num(in.line) // §6 semantics folded into the inline
		}
		stmts = append(stmts, guardedRawWrite(buf, idx, "line", lineValue))

		for _, w := range capturedWrites {
			info := in.schema[w.field]
			switch info.kind {
			case fieldEnum:
				// Positional buffer method, string→index folded when literal
				// (mirrors the generated enum override setter).
				var enumIdx *shimast.Node
				if isCompileTimeLiteral(w.arg) && w.arg.Kind == shimast.KindStringLiteral {
					v := 0
					for i, ev := range info.enumValues {
						if ev == shimast.NodeText(w.arg) {
							v = i
							break
						}
					}
					enumIdx = num(v)
				} else {
					enumIdx = enumSwitchIIFE(w.arg, info.enumValues)
				}
				stmts = append(stmts, factory.NewExpressionStatement(
					callExpr(propAccess(buf, w.field), []*shimast.Node{idx, enumIdx})))
			case fieldBool:
				// Bit-packed at row granularity — go through the buffer's
				// positional method like the generated setter does.
				stmts = append(stmts, factory.NewExpressionStatement(
					callExpr(propAccess(buf, w.field), []*shimast.Node{idx, w.arg})))
			default:
				stmts = append(stmts, fixedSchemaWrite(buf, idx, w.field, w.arg)...)
			}
		}
		in.list.Nodes[in.index] = factory.NewBlock(factory.NewNodeList(stmts), true)
	}
}

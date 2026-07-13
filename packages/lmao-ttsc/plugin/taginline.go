// taginline.go — spec 01o §4 tag-chain inlining (smoo/lmao!n/transformer-tag-chain-inline)
//
// Checker-backed tag-chain inliner: rewrites statement-level
// `ctx.tag.a(x).b(y).with({...})` chains into a Block of direct columnar
// buffer writes (null-bitmap |= 1, values[0] = v), with enum indices folded
// to constants for literal arguments and switch-IIFEs for dynamic ones.
//
// Conservative by construction — the chain is transformed ONLY when the tsgo
// Checker proves the receiver of `.tag` is a TagWriter/GeneratedTagWriter;
// anything unprovable is left untouched (the runtime fluent path is always
// correct, just slower). Column names are the library-local (unprefixed)
// names per spec 01e — remapping is cold-path-only via RemappedBufferView.
//
// Output-shape parity with the TS inliner is the invariant (spec 01o: "the
// running invariant is the transformed output the tests assert"): all plain
// tag calls emit first in execution order, then .with() calls unrolled, each
// via the same boolean/enum/direct write shapes.
package main

import (
	"sort"
	"strings"

	shimast "github.com/microsoft/typescript-go/shim/ast"
	shimchecker "github.com/microsoft/typescript-go/shim/checker"
)

var tagWriterTypeNames = []string{"TagWriter", "GeneratedTagWriter"}

type schemaFieldKind int

const (
	fieldDirect schemaFieldKind = iota // category/text/number: direct value write
	fieldBool
	fieldEnum
)

type schemaField struct {
	kind       schemaFieldKind
	enumValues []string // sorted, for fieldEnum
	eager      bool
}

type tagWrite struct {
	field string
	arg   *shimast.Node
	node  *shimast.CallExpression
}

// --- chain detection ---------------------------------------------------------

// findTagChain walks receiver-wards from the outermost call. Returns the ctx
// expression and the writes in execution order (plain tag calls first, then
// unrolled with() properties — matching the TS inliner), or ok=false.
func (t *fileTransformer) findTagChain(call *shimast.CallExpression) (ctxExpr *shimast.Node, writes []tagWrite, schema map[string]schemaField, ok bool) {
	if t.checker == nil {
		return nil, nil, nil, false
	}
	var tagCalls []tagWrite
	var withProps [][]tagWrite
	current := call
	for {
		expr := current.Expression
		if expr.Kind != shimast.KindPropertyAccessExpression {
			return nil, nil, nil, false
		}
		pa := expr.AsPropertyAccessExpression()
		name := shimast.NodeText(pa.Name())

		if name == "with" {
			if len(current.Arguments.Nodes) != 1 || current.Arguments.Nodes[0].Kind != shimast.KindObjectLiteralExpression {
				return nil, nil, nil, false // non-literal with(): can't unroll
			}
			var props []tagWrite
			for _, prop := range current.Arguments.Nodes[0].AsObjectLiteralExpression().Properties.Nodes {
				if prop.Kind != shimast.KindPropertyAssignment {
					continue // spreads/shorthand/computed: skip (TS parity)
				}
				p := prop.AsPropertyAssignment()
				nameNode := p.Name()
				if nameNode.Kind != shimast.KindIdentifier && nameNode.Kind != shimast.KindStringLiteral {
					continue
				}
				props = append(props, tagWrite{field: shimast.NodeText(nameNode), arg: p.Initializer})
			}
			withProps = append([][]tagWrite{props}, withProps...)
		} else {
			if len(current.Arguments.Nodes) != 1 {
				return nil, nil, nil, false
			}
			tagCalls = append([]tagWrite{{field: name, arg: current.Arguments.Nodes[0], node: current}}, tagCalls...)
		}

		next := pa.Expression
		if next.Kind == shimast.KindCallExpression {
			current = next.AsCallExpression()
			continue
		}
		// Chain root: must be `<ctx>.tag` and the Checker must prove TagWriter.
		if next.Kind != shimast.KindPropertyAccessExpression {
			return nil, nil, nil, false
		}
		rootPA := next.AsPropertyAccessExpression()
		if shimast.NodeText(rootPA.Name()) != "tag" {
			return nil, nil, nil, false
		}
		tagType := t.checker.GetTypeAtLocation(next)
		if tagType == nil || !isTagWriterType(t.checker, tagType) {
			return nil, nil, nil, false
		}
		schema = extractSchema(t.checker, tagType)
		writes = tagCalls
		for _, props := range withProps {
			writes = append(writes, props...)
		}
		for _, write := range writes {
			currentField := schema[write.field]
			if values := finiteStringLiteralUnion(t.checker, t.checker.GetContextualType(write.arg, 0)); len(values) > 0 {
				schema[write.field] = schemaField{kind: fieldEnum, enumValues: values, eager: currentField.eager}
				continue
			}
			if write.node != nil {
				if resolvedField, resolved := schemaFieldFromSetterCall(t.checker, write.node); resolved {
					resolvedField.eager = currentField.eager
					schema[write.field] = resolvedField
				}
			}
		}
		if len(writes) == 0 {
			return nil, nil, nil, false
		}
		return rootPA.Expression, writes, schema, true
	}
}

func isTagWriterType(chk *shimchecker.Checker, t *shimchecker.Type) bool {
	name := chk.TypeToString(t)
	for _, w := range tagWriterTypeNames {
		if name == w || strings.HasPrefix(name, w+"<") {
			return true
		}
	}
	if sym := shimchecker.Type_getTypeNameSymbol(t); sym != nil {
		for _, w := range tagWriterTypeNames {
			if sym.Name == w {
				return true
			}
		}
	}
	return false
}

// extractSchema mirrors extractSchemaFromTagType: per property, the first
// call-signature parameter's type decides the field kind; `_eager: true` on
// the property type marks eager columns.
func extractSchema(chk *shimchecker.Checker, tagType *shimchecker.Type) map[string]schemaField {
	schema := map[string]schemaField{}
	for _, sym := range shimchecker.Checker_getPropertiesOfType(chk, tagType) {
		name := sym.Name
		if name == "with" || strings.HasPrefix(name, "_") {
			continue
		}
		propType := shimchecker.Checker_getTypeOfSymbol(chk, sym)
		sigs := shimchecker.Checker_getSignaturesOfType(chk, propType, shimchecker.SignatureKindCall)
		if len(sigs) == 0 {
			continue
		}
		params := shimchecker.Signature_parameters(sigs[0])
		if len(params) == 0 {
			continue
		}
		paramType := shimchecker.Checker_getTypeOfSymbol(chk, params[0])
		eager := false
		if et := shimchecker.Checker_getTypeOfPropertyOfType(chk, propType, "_eager"); et != nil {
			eager = chk.TypeToString(et) == "true"
		}

		if paramType.IsUnion() {
			var values []string
			allLiterals := true
			for _, member := range paramType.Types() {
				if member.IsStringLiteral() {
					if v, isStr := member.AsLiteralType().Value().(string); isStr {
						values = append(values, v)
						continue
					}
				}
				allLiterals = false
				break
			}
			if allLiterals && len(values) > 0 {
				sort.Strings(values) // sorted-order indexing matches runtime dictionaries
				schema[name] = schemaField{kind: fieldEnum, enumValues: values, eager: eager}
				continue
			}
		}
		switch chk.TypeToString(paramType) {
		case "string":
			schema[name] = schemaField{kind: fieldDirect, eager: eager}
		case "number":
			schema[name] = schemaField{kind: fieldDirect, eager: eager}
		case "boolean":
			schema[name] = schemaField{kind: fieldBool, eager: eager}
		}
	}
	return schema
}

// --- code generation ----------------------------------------------------------

func isCompileTimeLiteral(n *shimast.Node) bool {
	switch n.Kind {
	case shimast.KindStringLiteral, shimast.KindNumericLiteral, shimast.KindTrueKeyword, shimast.KindFalseKeyword:
		return true
	}
	return false
}

type inlineEmitter struct {
	bufferExpr     *shimast.Node // ctx._buffer
	enumLookupExpr *shimast.Node // ctx._physicalLayoutPlan.enumLookup.byField
	varCounter     int
	stmts          []*shimast.Node
}

func (e *inlineEmitter) freshVar() *shimast.Node {
	name := "$$v" + itoa(e.varCounter)
	e.varCounter++
	return ident(name)
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [8]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}

// columnAccess builds `<buffer>.<field>_<suffix>[0]`.
func (e *inlineEmitter) columnAccess(field, suffix string) *shimast.Node {
	return factory.NewElementAccessExpression(
		propAccess(e.bufferExpr, field+"_"+suffix), nil, num(0), shimast.NodeFlagsNone)
}

func binaryStmt(left *shimast.Node, op shimast.Kind, right *shimast.Node) *shimast.Node {
	return factory.NewExpressionStatement(
		factory.NewBinaryExpression(nil, left, nil, factory.NewToken(op), right))
}

func (e *inlineEmitter) emitNullsSet(field string) {
	e.stmts = append(e.stmts, binaryStmt(e.columnAccess(field, "nulls"), shimast.KindBarEqualsToken, num(1)))
}

func (e *inlineEmitter) emitField(w tagWrite, schema map[string]schemaField) {
	info, known := schema[w.field]
	literal := isCompileTimeLiteral(w.arg)
	if known && info.kind == fieldBool {
		e.emitBool(w, info.eager, literal)
		return
	}
	if known && info.kind == fieldEnum {
		e.emitEnum(w, info)
		return
	}
	eager := known && info.eager
	e.emitDirect(w, eager, literal)
}

func (e *inlineEmitter) emitBool(w tagWrite, eager, literal bool) {
	values := func() *shimast.Node { return e.columnAccess(w.field, "values") }
	setTrue := func() *shimast.Node { return binaryStmt(values(), shimast.KindBarEqualsToken, num(1)) }
	setFalse := func() *shimast.Node {
		return binaryStmt(values(), shimast.KindAmpersandEqualsToken,
			factory.NewPrefixUnaryExpression(shimast.KindTildeToken, num(1)))
	}
	if literal {
		if !eager {
			e.emitNullsSet(w.field)
		}
		if w.arg.Kind == shimast.KindTrueKeyword {
			e.stmts = append(e.stmts, setTrue())
		} else {
			e.stmts = append(e.stmts, setFalse())
		}
		return
	}
	v := e.freshVar()
	e.stmts = append(e.stmts, constDecl(v, w.arg))
	var ifBody []*shimast.Node
	if !eager {
		ifBody = append(ifBody, binaryStmt(e.columnAccess(w.field, "nulls"), shimast.KindBarEqualsToken, num(1)))
	}
	ifBody = append(ifBody, factory.NewIfStatement(ident(identText(v)),
		factory.NewBlock(factory.NewNodeList([]*shimast.Node{setTrue()}), false),
		factory.NewBlock(factory.NewNodeList([]*shimast.Node{setFalse()}), false)))
	e.stmts = append(e.stmts, notNullGuard(v, ifBody))
}

func (e *inlineEmitter) emitEnum(w tagWrite, info schemaField) {
	if !info.eager {
		e.emitNullsSet(w.field)
	}
	e.stmts = append(e.stmts, binaryStmt(
		e.columnAccess(w.field, "values"),
		shimast.KindEqualsToken,
		enumEncodeCall(e.enumLookupExpr, w.field, w.arg),
	))
}

func (e *inlineEmitter) emitDirect(w tagWrite, eager, literal bool) {
	if literal {
		if !eager {
			e.emitNullsSet(w.field)
		}
		e.stmts = append(e.stmts, binaryStmt(e.columnAccess(w.field, "values"), shimast.KindEqualsToken, w.arg))
		return
	}
	v := e.freshVar()
	e.stmts = append(e.stmts, constDecl(v, w.arg))
	var ifBody []*shimast.Node
	if !eager {
		ifBody = append(ifBody, binaryStmt(e.columnAccess(w.field, "nulls"), shimast.KindBarEqualsToken, num(1)))
	}
	ifBody = append(ifBody, binaryStmt(e.columnAccess(w.field, "values"), shimast.KindEqualsToken, ident(identText(v))))
	e.stmts = append(e.stmts, notNullGuard(v, ifBody))
}

func identText(n *shimast.Node) string { return shimast.NodeText(n) }

func constDecl(name *shimast.Node, init *shimast.Node) *shimast.Node {
	decl := factory.NewVariableDeclaration(name, nil, nil, init)
	return factory.NewVariableStatement(nil,
		factory.NewVariableDeclarationList(factory.NewNodeList([]*shimast.Node{decl}), shimast.NodeFlagsConst))
}

// notNullGuard builds `if (<v> != null) { <body> }`.
func notNullGuard(v *shimast.Node, body []*shimast.Node) *shimast.Node {
	cond := factory.NewBinaryExpression(nil, ident(identText(v)), nil,
		factory.NewToken(shimast.KindExclamationEqualsToken),
		factory.NewKeywordExpression(shimast.KindNullKeyword))
	return factory.NewIfStatement(cond, factory.NewBlock(factory.NewNodeList(body), true), nil)
}

// enumEncodeCall reuses the schema-order encoder already bound to the callsite plan.
func enumEncodeCall(enumLookupExpr *shimast.Node, field string, arg *shimast.Node) *shimast.Node {
	descriptor := factory.NewElementAccessExpression(enumLookupExpr, nil, str(field), shimast.NodeFlagsNone)
	return callExpr(propAccess(descriptor, "encode"), []*shimast.Node{arg})
}

// --- two-phase statement-level entry ----------------------------------------------
//
// Phase A resolves chains with the Checker over the untouched tree; phase B
// splices synthesized blocks. Splitting matters: lazy type checking triggered
// by a phase-A query re-walks whole containing declarations, and would panic
// on any already-spliced synthesized node (pos -1 source reads).

type tagInline struct {
	list    *shimast.NodeList
	index   int
	ctxExpr *shimast.Node
	writes  []tagWrite
	schema  map[string]schemaField
}

// collectTagInlines finds every provable tag-chain and log-chain
// ExpressionStatement. Checker queries happen here and only here; the tree
// is not modified.
func (t *fileTransformer) collectTagInlines(root *shimast.Node) ([]tagInline, []logInline, []resultInline) {
	var found []tagInline
	var foundLogs []logInline
	var foundResults []resultInline
	var visit func(node *shimast.Node)
	scanList := func(list *shimast.NodeList) {
		if list == nil {
			return
		}
		for i, stmt := range list.Nodes {
			var inner *shimast.Node
			isReturn := false
			switch stmt.Kind {
			case shimast.KindExpressionStatement:
				inner = stmt.AsExpressionStatement().Expression
			case shimast.KindReturnStatement:
				inner = stmt.AsReturnStatement().Expression
				isReturn = true
			default:
				continue
			}
			if inner == nil || inner.Kind != shimast.KindCallExpression {
				continue
			}
			call := inner.AsCallExpression()
			if !isReturn {
				if ctxExpr, writes, schema, ok := t.findTagChain(call); ok {
					t.processed[call] = true
					found = append(found, tagInline{list: list, index: i, ctxExpr: ctxExpr, writes: writes, schema: schema})
					continue
				}
				if in, ok := t.findLogInline(call); ok {
					t.processed[call] = true
					in.list, in.index = list, i
					foundLogs = append(foundLogs, *in)
					continue
				}
			}
			if in, ok := t.findResultInline(call); ok {
				t.processed[call] = true
				if in.okCall != nil && in.okCall.Kind == shimast.KindCallExpression {
					t.processed[in.okCall.AsCallExpression()] = true
				}
				in.list, in.index, in.isReturn = list, i, isReturn
				foundResults = append(foundResults, *in)
			}
		}
	}
	visit = func(node *shimast.Node) {
		if node == nil {
			return
		}
		if node.Kind == shimast.KindSourceFile {
			scanList(node.AsSourceFile().Statements)
		} else if node.CanHaveStatements() {
			scanList(node.StatementList())
		}
		node.ForEachChild(func(child *shimast.Node) bool {
			visit(child)
			return false
		})
	}
	visit(root)
	return found, foundLogs, foundResults
}

// applyTagInlines splices the replacement blocks (phase B — no checker use).
func (t *fileTransformer) applyTagInlines(inlines []tagInline) {
	for _, in := range inlines {
		state := ident("$$t")
		e := &inlineEmitter{
			bufferExpr: propAccess(state, "_spanBuffer"),
			enumLookupExpr: propAccess(
				propAccess(propAccess(state, "_physicalLayoutPlan"), "enumLookup"),
				"byField",
			),
			stmts: []*shimast.Node{
				constDecl(state, propAccess(propAccess(in.ctxExpr, "tag"), "_state")),
			},
		}
		for _, w := range in.writes {
			e.emitField(w, in.schema)
		}
		in.list.Nodes[in.index] = factory.NewBlock(factory.NewNodeList(e.stmts), true)
	}
}

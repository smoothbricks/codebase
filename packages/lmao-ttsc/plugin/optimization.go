package main

import (
	"sort"
	"strings"

	shimast "github.com/microsoft/typescript-go/shim/ast"
	shimchecker "github.com/microsoft/typescript-go/shim/checker"
)

const (
	runtimeHintTag                        uint32 = 1 << 16
	runtimeHintLog                        uint32 = 1 << 17
	runtimeHintFF                         uint32 = 1 << 18
	runtimeHintSpan                       uint32 = 1 << 19
	runtimeHintResult                     uint32 = 1 << 20
	runtimeHintScope                      uint32 = 1 << 21
	runtimeHintDeps                       uint32 = 1 << 22
	runtimeHintAnalyzed                   uint32 = 1 << 23
	runtimeHintMessageStatic              uint32 = 1 << 24
	runtimeHintMessageDynamic             uint32 = 1 << 25
	runtimeHintMessageMixed               uint32 = runtimeHintMessageStatic | runtimeHintMessageDynamic
	runtimeHintMessagePhysicalPacked      uint32 = 1 << 26
	runtimeHintMessagePhysicalSpecialized uint32 = 1 << 27
	runtimeHintMessagePhysicalMask        uint32 = runtimeHintMessagePhysicalPacked | runtimeHintMessagePhysicalSpecialized
	maxPackedDenseIndex                   uint32 = 0x00fffffe
)

type callMessagePhysicalLayout uint8

const (
	callMessagePhysicalCurrent callMessagePhysicalLayout = iota
	callMessagePhysicalSpecialized
	callMessagePhysicalPacked
)

var contextCapability = map[string]uint32{
	"tag": runtimeHintTag, "log": runtimeHintLog, "ff": runtimeHintFF,
	"span": runtimeHintSpan, "spanSync": runtimeHintSpan,
	"ok": runtimeHintResult, "err": runtimeHintResult,
	"scope": runtimeHintScope, "setScope": runtimeHintScope, "deps": runtimeHintDeps,
}

type opCompileAnalysis struct {
	runtimeHint            uint32
	eagerColumns           []string
	localMessageDictionary []globalVocabularyID
}

type hintRewrite struct {
	call    *shimast.CallExpression
	hints   map[string]opCompileAnalysis
	single  opCompileAnalysis
	isGroup bool
}

func functionParts(node *shimast.Node) (params *shimast.ParameterList, body *shimast.Node, async bool, ok bool) {
	switch node.Kind {
	case shimast.KindArrowFunction:
		fn := node.AsArrowFunction()
		return fn.Parameters, fn.Body, node.ModifierFlags()&shimast.ModifierFlagsAsync != 0, true
	case shimast.KindFunctionExpression:
		fn := node.AsFunctionExpression()
		return fn.Parameters, fn.Body, node.ModifierFlags()&shimast.ModifierFlagsAsync != 0, true
	case shimast.KindMethodDeclaration:
		fn := node.AsMethodDeclaration()
		return fn.Parameters, fn.Body, node.ModifierFlags()&shimast.ModifierFlagsAsync != 0, true
	default:
		return nil, nil, false, false
	}
}

func isNestedFunction(node *shimast.Node) bool {
	switch node.Kind {
	case shimast.KindArrowFunction, shimast.KindFunctionExpression, shimast.KindFunctionDeclaration,
		shimast.KindMethodDeclaration, shimast.KindGetAccessor, shimast.KindSetAccessor, shimast.KindConstructor:
		return true
	default:
		return false
	}
}

func isLoop(node *shimast.Node) bool {
	switch node.Kind {
	case shimast.KindForStatement, shimast.KindForInStatement, shimast.KindForOfStatement,
		shimast.KindWhileStatement, shimast.KindDoStatement:
		return true
	default:
		return false
	}
}

func selectMessagePhysicalLayout(capacity, staticRows, dynamicRows uint32, vocabularySize int) uint32 {
	if vocabularySize < 0 || vocabularySize > int(maxPackedDenseIndex)+1 {
		return 0
	}
	switch capacity {
	case 8, 64, 1024:
	default:
		return 0
	}
	usableRows := capacity - 2
	if usableRows == 0 || staticRows > usableRows || dynamicRows > usableRows || staticRows != usableRows-dynamicRows {
		return 0
	}
	total := usableRows

	// Quantize the compile-time whole-program counts to the nearest measured
	// 25% cell. The +total/2 term gives integer round-to-nearest without a
	// runtime ratio or interpolation in the emitted program.
	bucket := (staticRows*4 + total/2) / total
	if capacity == 64 && bucket == 2 {
		return runtimeHintMessagePhysicalSpecialized
	}
	return 0
}

func callMessagePhysicalLayoutFromHint(physicalHint uint32) callMessagePhysicalLayout {
	switch physicalHint & runtimeHintMessagePhysicalMask {
	case runtimeHintMessagePhysicalPacked:
		return callMessagePhysicalPacked
	case runtimeHintMessagePhysicalSpecialized:
		return callMessagePhysicalSpecialized
	default:
		// Zero is current; both bits set is invalid and conservatively current.
		return callMessagePhysicalCurrent
	}
}

// analyzeOpFunction is intentionally closed-world. A hint is emitted only when
// every use of the context parameter is a direct access to a known capability.
func analyzeOpFunction(
	fn *shimast.Node,
	staticLogIDs map[*shimast.CallExpression]globalVocabularyID,
	vocabularySize int,
	physicalLogCalls map[*shimast.CallExpression]callMessagePhysicalLayout,
	currentLogLocalIDs map[*shimast.CallExpression]uint16,
) (uint32, []globalVocabularyID) {
	params, body, _, ok := functionParts(fn)
	if !ok || params == nil || len(params.Nodes) == 0 || body == nil {
		return 0, nil
	}
	first := params.Nodes[0]
	if first.Kind != shimast.KindParameter || first.Name() == nil || first.Name().Kind != shimast.KindIdentifier {
		return 0, nil
	}
	ctxName := shimast.NodeText(first.Name())
	if ctxName == "" {
		return 0, nil
	}

	valid := true
	capacityKnown := true
	capacity := uint32(2) // rows 0 (tags) and 1 (terminal result)
	caps := uint32(0)
	staticMessageRows := uint32(0)
	dynamicMessageRows := uint32(0)
	staticCalls := make([]*shimast.CallExpression, 0)
	var visit func(*shimast.Node, bool)
	visit = func(node *shimast.Node, root bool) {
		if node == nil || !valid {
			return
		}
		if !root && isNestedFunction(node) {
			valid = false
			return
		}
		if isLoop(node) {
			capacityKnown = false
		}
		if node.Kind == shimast.KindIdentifier && shimast.NodeText(node) == ctxName {
			parent := node.Parent
			if parent == nil || parent.Kind != shimast.KindPropertyAccessExpression {
				valid = false
				return
			}
			pa := parent.AsPropertyAccessExpression()
			if pa.Expression != node || pa.Name() == nil {
				valid = false
				return
			}
			name := shimast.NodeText(pa.Name())
			capability, recognized := contextCapability[name]
			if !recognized || !isClosedWorldCapabilityUse(parent, name) {
				valid = false
				return
			}
			caps |= capability
			if name == "ff" {
				dynamicMessageRows++
				if capacityKnown && capacity < 0xffff {
					capacity++
				} else {
					capacityKnown = false
				}
			}
		}
		if node.Kind == shimast.KindCallExpression {
			call := node.AsCallExpression()
			if call.Expression.Kind == shimast.KindPropertyAccessExpression {
				method := call.Expression.AsPropertyAccessExpression()
				if logMethods[shimast.NodeText(method.Name())] && method.Expression.Kind == shimast.KindPropertyAccessExpression {
					logAccess := method.Expression.AsPropertyAccessExpression()
					if shimast.NodeText(logAccess.Name()) == "log" && logAccess.Expression.Kind == shimast.KindIdentifier && shimast.NodeText(logAccess.Expression) == ctxName {
						if staticLogIDs[call] != 0 {
							staticMessageRows++
							staticCalls = append(staticCalls, call)
						} else {
							dynamicMessageRows++
						}
						if capacityKnown && capacity < 0xffff {
							capacity++
						} else {
							capacityKnown = false
						}
					}
				}
			}
		}
		node.ForEachChild(func(child *shimast.Node) bool {
			visit(child, false)
			return !valid
		})
	}
	visit(body, true)
	if !valid {
		return 0, nil
	}
	if !capacityKnown {
		capacity = 0
	}
	messageHint := runtimeHintMessageStatic
	if dynamicMessageRows != 0 {
		messageHint = runtimeHintMessageDynamic
		if staticMessageRows != 0 {
			messageHint = runtimeHintMessageMixed
		}
	}
	physicalHint := uint32(0)
	if capacityKnown {
		physicalHint = selectMessagePhysicalLayout(capacity, staticMessageRows, dynamicMessageRows, vocabularySize)
	}
	physicalLayout := callMessagePhysicalLayoutFromHint(physicalHint)
	var localMessageDictionary []globalVocabularyID
	var localIDsByGlobalID map[globalVocabularyID]uint16
	for _, call := range staticCalls {
		physicalLogCalls[call] = physicalLayout
		if physicalLayout != callMessagePhysicalCurrent {
			continue
		}
		if localIDsByGlobalID == nil {
			localMessageDictionary = make([]globalVocabularyID, 0, len(staticCalls))
			localIDsByGlobalID = make(map[globalVocabularyID]uint16, len(staticCalls))
		}
		globalID := staticLogIDs[call]
		localID := localIDsByGlobalID[globalID]
		if localID == 0 {
			localMessageDictionary = append(localMessageDictionary, globalID)
			localID = uint16(len(localMessageDictionary))
			localIDsByGlobalID[globalID] = localID
		}
		currentLogLocalIDs[call] = localID
	}
	return runtimeHintAnalyzed | messageHint | physicalHint | caps | capacity, localMessageDictionary
}

type eagerColumnSet map[string]struct{}

type eagerColumnFlow struct {
	fields         eagerColumnSet
	terminalFields eagerColumnSet
	fallsThrough   bool
	hasTerminal    bool
}

type eagerColumnAnalyzer struct {
	transformer *fileTransformer
	ctxName     string
	valid       bool
}

func cloneEagerColumns(columns eagerColumnSet) eagerColumnSet {
	cloned := make(eagerColumnSet, len(columns))
	for name := range columns {
		cloned[name] = struct{}{}
	}
	return cloned
}

func addEagerColumns(dst, src eagerColumnSet) {
	for name := range src {
		dst[name] = struct{}{}
	}
}

func intersectEagerColumns(left, right eagerColumnSet) eagerColumnSet {
	if len(left) > len(right) {
		left, right = right, left
	}
	intersection := make(eagerColumnSet, len(left))
	for name := range left {
		if _, present := right[name]; present {
			intersection[name] = struct{}{}
		}
	}
	return intersection
}

func mergeEagerTerminals(dst *eagerColumnFlow, src eagerColumnFlow) {
	if !src.hasTerminal {
		return
	}
	if !dst.hasTerminal {
		dst.terminalFields = cloneEagerColumns(src.terminalFields)
		dst.hasTerminal = true
		return
	}
	dst.terminalFields = intersectEagerColumns(dst.terminalFields, src.terminalFields)
}

func (a *eagerColumnAnalyzer) validateSyntax(node *shimast.Node, root bool) {
	if node == nil || !a.valid {
		return
	}
	if !root && isNestedFunction(node) {
		a.valid = false
		return
	}
	switch node.Kind {
	case shimast.KindForStatement, shimast.KindForInStatement, shimast.KindForOfStatement,
		shimast.KindWhileStatement, shimast.KindDoStatement, shimast.KindSwitchStatement,
		shimast.KindTryStatement, shimast.KindLabeledStatement, shimast.KindWithStatement:
		a.valid = false
		return
	}
	node.ForEachChild(func(child *shimast.Node) bool {
		a.validateSyntax(child, false)
		return !a.valid
	})
}

func isNamedContextAccess(node *shimast.Node, ctxName, name string) bool {
	if node == nil || node.Kind != shimast.KindPropertyAccessExpression {
		return false
	}
	access := node.AsPropertyAccessExpression()
	return shimast.NodeText(access.Name()) == name && access.Expression.Kind == shimast.KindIdentifier && shimast.NodeText(access.Expression) == ctxName
}

func contextWriterRoot(call *shimast.CallExpression, ctxName string) string {
	current := call
	for {
		if current.Expression.Kind != shimast.KindPropertyAccessExpression {
			return ""
		}
		access := current.Expression.AsPropertyAccessExpression()
		next := access.Expression
		if next.Kind == shimast.KindCallExpression {
			current = next.AsCallExpression()
			continue
		}
		if next.Kind == shimast.KindIdentifier && shimast.NodeText(next) == ctxName {
			switch shimast.NodeText(access.Name()) {
			case "ok", "err":
				return "result"
			}
			return ""
		}
		if isNamedContextAccess(next, ctxName, "tag") {
			return "tag"
		}
		if isNamedContextAccess(next, ctxName, "log") {
			return "log"
		}
		return ""
	}
}

func (a *eagerColumnAnalyzer) provenCallWrites(call *shimast.CallExpression) (eagerColumnSet, bool) {
	root := contextWriterRoot(call, a.ctxName)
	if root == "" {
		return nil, false
	}
	var writes []tagWrite
	var schema map[string]schemaField
	proved := false
	switch root {
	case "tag":
		ctxExpr, recovered, recoveredSchema, ok := a.transformer.findTagChain(call)
		if ok && ctxExpr.Kind == shimast.KindIdentifier && shimast.NodeText(ctxExpr) == a.ctxName {
			writes, schema, proved = recovered, recoveredSchema, true
		}
	case "log":
		if recovered, ok := a.transformer.findLogInline(call); ok && isNamedContextAccess(recovered.logExpr, a.ctxName, "log") {
			writes, schema, proved = recovered.writes, recovered.schema, true
		}
	case "result":
		if recovered, ok := a.transformer.findResultInline(call); ok && recovered.okCall != nil && recovered.okCall.Kind == shimast.KindCallExpression {
			rootCall := recovered.okCall.AsCallExpression()
			if rootCall.Expression.Kind == shimast.KindPropertyAccessExpression {
				rootAccess := rootCall.Expression.AsPropertyAccessExpression()
				if rootAccess.Expression.Kind == shimast.KindIdentifier && shimast.NodeText(rootAccess.Expression) == a.ctxName {
					writes, schema, proved = recovered.writes, recovered.schema, true
				}
			}
		}
		if !proved && call.Expression.Kind == shimast.KindPropertyAccessExpression {
			access := call.Expression.AsPropertyAccessExpression()
			if access.Expression.Kind == shimast.KindIdentifier && shimast.NodeText(access.Expression) == a.ctxName && resultMethods[shimast.NodeText(access.Name())] {
				return eagerColumnSet{}, true
			}
		}
	}
	if !proved {
		a.valid = false
		return nil, true
	}
	columns := make(eagerColumnSet, len(writes))
	for _, write := range writes {
		if root == "result" && (write.field == "line" || write.field == "message") {
			continue
		}
		if _, known := schema[write.field]; !known {
			a.valid = false
			return nil, true
		}
		columns[write.field] = struct{}{}
	}
	return columns, true
}

func (a *eagerColumnAnalyzer) expressionWrites(node *shimast.Node) eagerColumnSet {
	columns := eagerColumnSet{}
	if node == nil || !a.valid {
		return columns
	}
	switch node.Kind {
	case shimast.KindParenthesizedExpression:
		return a.expressionWrites(node.AsParenthesizedExpression().Expression)
	case shimast.KindConditionalExpression:
		expression := node.AsConditionalExpression()
		base := a.expressionWrites(expression.Condition)
		whenTrue := cloneEagerColumns(base)
		addEagerColumns(whenTrue, a.expressionWrites(expression.WhenTrue))
		whenFalse := cloneEagerColumns(base)
		addEagerColumns(whenFalse, a.expressionWrites(expression.WhenFalse))
		return intersectEagerColumns(whenTrue, whenFalse)
	case shimast.KindBinaryExpression:
		expression := node.AsBinaryExpression()
		left := a.expressionWrites(expression.Left)
		operator := expression.OperatorToken.Kind
		if operator == shimast.KindAmpersandAmpersandToken || operator == shimast.KindBarBarToken || operator == shimast.KindQuestionQuestionToken {
			a.expressionWrites(expression.Right)
			return left
		}
		addEagerColumns(left, a.expressionWrites(expression.Right))
		return left
	case shimast.KindCallExpression:
		call := node.AsCallExpression()
		if recovered, handled := a.provenCallWrites(call); handled {
			addEagerColumns(columns, recovered)
			for _, argument := range call.Arguments.Nodes {
				addEagerColumns(columns, a.expressionWrites(argument))
			}
			return columns
		}
	}
	node.ForEachChild(func(child *shimast.Node) bool {
		addEagerColumns(columns, a.expressionWrites(child))
		return !a.valid
	})
	return columns
}

func (a *eagerColumnAnalyzer) statements(nodes []*shimast.Node, incoming eagerColumnSet) eagerColumnFlow {
	flow := eagerColumnFlow{fields: cloneEagerColumns(incoming), fallsThrough: true}
	for _, statement := range nodes {
		if !flow.fallsThrough || !a.valid {
			break
		}
		next := a.statement(statement, flow.fields)
		mergeEagerTerminals(&flow, next)
		flow.fields = next.fields
		flow.fallsThrough = next.fallsThrough
	}
	return flow
}

func (a *eagerColumnAnalyzer) statement(node *shimast.Node, incoming eagerColumnSet) eagerColumnFlow {
	flow := eagerColumnFlow{fields: cloneEagerColumns(incoming), fallsThrough: true}
	if node == nil || !a.valid {
		return flow
	}
	switch node.Kind {
	case shimast.KindBlock:
		return a.statements(node.AsBlock().Statements.Nodes, incoming)
	case shimast.KindExpressionStatement:
		addEagerColumns(flow.fields, a.expressionWrites(node.AsExpressionStatement().Expression))
	case shimast.KindReturnStatement:
		addEagerColumns(flow.fields, a.expressionWrites(node.AsReturnStatement().Expression))
		flow.terminalFields, flow.hasTerminal, flow.fallsThrough = cloneEagerColumns(flow.fields), true, false
	case shimast.KindThrowStatement:
		addEagerColumns(flow.fields, a.expressionWrites(node.AsThrowStatement().Expression))
		flow.terminalFields, flow.hasTerminal, flow.fallsThrough = cloneEagerColumns(flow.fields), true, false
	case shimast.KindIfStatement:
		statement := node.AsIfStatement()
		base := cloneEagerColumns(incoming)
		addEagerColumns(base, a.expressionWrites(statement.Expression))
		whenTrue := a.statement(statement.ThenStatement, base)
		whenFalse := eagerColumnFlow{fields: cloneEagerColumns(base), fallsThrough: true}
		if statement.ElseStatement != nil {
			whenFalse = a.statement(statement.ElseStatement, base)
		}
		flow = eagerColumnFlow{}
		mergeEagerTerminals(&flow, whenTrue)
		mergeEagerTerminals(&flow, whenFalse)
		switch {
		case whenTrue.fallsThrough && whenFalse.fallsThrough:
			flow.fields = intersectEagerColumns(whenTrue.fields, whenFalse.fields)
			flow.fallsThrough = true
		case whenTrue.fallsThrough:
			flow.fields, flow.fallsThrough = cloneEagerColumns(whenTrue.fields), true
		case whenFalse.fallsThrough:
			flow.fields, flow.fallsThrough = cloneEagerColumns(whenFalse.fields), true
		}
	default:
		addEagerColumns(flow.fields, a.expressionWrites(node))
	}
	return flow
}

func (t *fileTransformer) analyzeEagerColumns(fn *shimast.Node) []string {
	params, body, _, ok := functionParts(fn)
	if !ok || params == nil || len(params.Nodes) == 0 || body == nil {
		return nil
	}
	parameter := params.Nodes[0]
	if parameter.Kind != shimast.KindParameter || parameter.Name() == nil || parameter.Name().Kind != shimast.KindIdentifier {
		return nil
	}
	analyzer := eagerColumnAnalyzer{transformer: t, ctxName: shimast.NodeText(parameter.Name()), valid: true}
	analyzer.validateSyntax(body, true)
	if !analyzer.valid {
		return nil
	}
	var outcomes eagerColumnSet
	if body.Kind == shimast.KindBlock {
		flow := analyzer.statements(body.AsBlock().Statements.Nodes, eagerColumnSet{})
		if !analyzer.valid {
			return nil
		}
		if flow.hasTerminal {
			outcomes = cloneEagerColumns(flow.terminalFields)
		}
		if flow.fallsThrough {
			if outcomes == nil {
				outcomes = cloneEagerColumns(flow.fields)
			} else {
				outcomes = intersectEagerColumns(outcomes, flow.fields)
			}
		}
	} else {
		outcomes = analyzer.expressionWrites(body)
	}
	if !analyzer.valid || len(outcomes) == 0 {
		return nil
	}
	columns := make([]string, 0, len(outcomes))
	for name := range outcomes {
		columns = append(columns, name)
	}
	sort.Strings(columns)
	return columns
}

func literalLogMessage(node *shimast.Node) (string, bool) {
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

// tagCapabilityFullyInlined proves that every direct access to the operation's
// context tag writer is the root of a checker-approved statement chain visited
// by collectTagInlines. Any mismatch retains the runtime capability.
func (t *fileTransformer) tagCapabilityFullyInlined(fn *shimast.Node) bool {
	if t.checker == nil {
		return false
	}
	params, body, _, ok := functionParts(fn)
	if !ok || params == nil || len(params.Nodes) == 0 || body == nil {
		return false
	}
	first := params.Nodes[0]
	if first.Kind != shimast.KindParameter || first.Name() == nil || first.Name().Kind != shimast.KindIdentifier {
		return false
	}
	ctxName := shimast.NodeText(first.Name())
	if ctxName == "" {
		return false
	}

	inlineableRoots := map[*shimast.Node]struct{}{}
	forEachStatementCall(body, func(_ *shimast.NodeList, _ int, call *shimast.CallExpression, isReturn bool) {
		if isReturn {
			return
		}
		ctxExpr, _, _, found := t.findTagChain(call)
		if found && ctxExpr.Kind == shimast.KindIdentifier && shimast.NodeText(ctxExpr) == ctxName {
			inlineableRoots[ctxExpr] = struct{}{}
		}
	})

	valid := true
	foundTag := false
	var visit func(node *shimast.Node)
	visit = func(node *shimast.Node) {
		if node == nil || !valid {
			return
		}
		if node.Kind == shimast.KindIdentifier && shimast.NodeText(node) == ctxName {
			access := node.Parent
			if access != nil && access.Kind == shimast.KindPropertyAccessExpression {
				property := access.AsPropertyAccessExpression()
				if property.Expression == node && shimast.NodeText(property.Name()) == "tag" {
					foundTag = true
					if _, inlineable := inlineableRoots[node]; !inlineable {
						valid = false
						return
					}
				}
			}
		}
		node.ForEachChild(func(child *shimast.Node) bool {
			visit(child)
			return !valid
		})
	}
	visit(body)
	if !valid || !foundTag {
		return false
	}
	if t.directTagStates == nil {
		t.directTagStates = map[*shimast.Node]bool{}
	}
	for root := range inlineableRoots {
		t.directTagStates[root] = true
	}
	return true
}

// analyzeOpCompileMetadata derives runtime execution hints and conservative
// definitely-written user columns. Static log vocabulary remains whole-program
// metadata collected independently of Op nesting.
func (t *fileTransformer) analyzeOpCompileMetadata(fn *shimast.Node) opCompileAnalysis {
	if t.physicalLogCalls == nil {
		t.physicalLogCalls = map[*shimast.CallExpression]callMessagePhysicalLayout{}
	}
	if t.currentLogLocalIDs == nil {
		t.currentLogLocalIDs = map[*shimast.CallExpression]uint16{}
	}
	runtimeHint, localMessageDictionary := analyzeOpFunction(
		fn, t.staticLogIDs, t.vocabularySize, t.physicalLogCalls, t.currentLogLocalIDs,
	)
	analysis := opCompileAnalysis{runtimeHint: runtimeHint, localMessageDictionary: localMessageDictionary}
	if runtimeHint&runtimeHintAnalyzed != 0 {
		analysis.eagerColumns = t.analyzeEagerColumns(fn)
		if runtimeHint&runtimeHintTag != 0 && t.tagCapabilityFullyInlined(fn) {
			analysis.runtimeHint &^= runtimeHintTag
		}
	}
	return analysis
}

func isClosedWorldCapabilityUse(access *shimast.Node, name string) bool {
	parent := access.Parent
	switch name {
	case "log", "tag":
		if parent == nil || parent.Kind != shimast.KindPropertyAccessExpression || parent.AsPropertyAccessExpression().Expression != access {
			return false
		}
		if name == "log" && !logMethods[shimast.NodeText(parent.AsPropertyAccessExpression().Name())] {
			return false
		}
		grandparent := parent.Parent
		return grandparent != nil && grandparent.Kind == shimast.KindCallExpression && grandparent.AsCallExpression().Expression == parent
	case "ff":
		if parent != nil && parent.Kind == shimast.KindCallExpression && parent.AsCallExpression().Expression == access {
			return true
		}
		if parent == nil || parent.Kind != shimast.KindPropertyAccessExpression || parent.AsPropertyAccessExpression().Expression != access {
			return false
		}
		grandparent := parent.Parent
		return grandparent != nil && grandparent.Kind == shimast.KindCallExpression && grandparent.AsCallExpression().Expression == parent
	case "span", "spanSync", "ok", "err", "setScope":
		return parent != nil && parent.Kind == shimast.KindCallExpression && parent.AsCallExpression().Expression == access
	case "deps", "scope":
		return parent != nil && parent.Kind == shimast.KindPropertyAccessExpression && parent.AsPropertyAccessExpression().Expression == access
	default:
		return false
	}
}

func literalPropertyName(node *shimast.Node) (string, bool) {
	if node == nil {
		return "", false
	}
	switch node.Kind {
	case shimast.KindIdentifier, shimast.KindStringLiteral, shimast.KindNumericLiteral:
		return shimast.NodeText(node), true
	default:
		return "", false
	}
}

func collectDefineOpsHints(t *fileTransformer, object *shimast.ObjectLiteralExpression) map[string]opCompileAnalysis {
	hints := map[string]opCompileAnalysis{}
	for _, prop := range object.Properties.Nodes {
		if prop.Name() == nil {
			continue
		}
		key, named := literalPropertyName(prop.Name())
		if !named {
			continue
		}
		switch prop.Kind {
		case shimast.KindPropertyAssignment:
			initializer := prop.AsPropertyAssignment().Initializer
			if _, _, _, ok := functionParts(initializer); ok {
				hints[key] = t.analyzeOpCompileMetadata(initializer)
			}
		case shimast.KindMethodDeclaration:
			hints[key] = t.analyzeOpCompileMetadata(prop)
		}
	}
	return hints
}

func isLmaoDeclarationNode(declaration *shimast.Node) bool {
	if declaration == nil {
		return false
	}
	if source := shimast.GetSourceFileOfNode(declaration); source != nil {
		fileName := strings.ReplaceAll(source.FileName(), "\\", "/")
		if strings.HasPrefix(fileName, "packages/lmao/") || strings.Contains(fileName, "/packages/lmao/") || strings.Contains(fileName, "/node_modules/@smoothbricks/lmao/") {
			return true
		}
	}
	for current := declaration; current != nil; current = current.Parent {
		if current.Kind == shimast.KindModuleDeclaration && current.Name() != nil && shimast.NodeText(current.Name()) == "@smoothbricks/lmao" {
			return true
		}
	}
	return false
}

func isNamedType(chk *shimchecker.Checker, node *shimast.Node, name string) bool {
	typ := chk.GetTypeAtLocation(node)
	if typ == nil {
		return false
	}
	if sym := shimchecker.Type_getTypeNameSymbol(typ); sym != nil && sym.Name == name {
		return true
	}
	return strings.HasPrefix(chk.TypeToString(typ), name+"<")
}

func isNamedLmaoType(chk *shimchecker.Checker, node *shimast.Node, name string) bool {
	typ := chk.GetTypeAtLocation(node)
	if typ == nil {
		return false
	}
	sym := shimchecker.Type_getTypeNameSymbol(typ)
	if sym == nil || sym.Name != name {
		return false
	}
	for _, declaration := range sym.Declarations {
		if isLmaoDeclarationNode(declaration) {
			return true
		}
	}
	return false
}

func hasLmaoCallProvenance(chk *shimchecker.Checker, call *shimast.CallExpression) bool {
	signature := chk.GetResolvedSignature(call.AsNode())
	return signature != nil && isLmaoDeclarationNode(signature.Declaration())
}

func (t *fileTransformer) collectOptimizations(root *shimast.Node, emitHints bool) []hintRewrite {
	if t.checker == nil {
		return nil
	}
	var hints []hintRewrite
	var visit func(*shimast.Node)
	visit = func(node *shimast.Node) {
		if node == nil {
			return
		}
		if node.Kind == shimast.KindCallExpression {
			call := node.AsCallExpression()
			if !emitHints {
				t.collectLogVocabulary(call)
			}
			_, name := calleeNames(call)
			provenLmaoCall := hasLmaoCallProvenance(t.checker, call)
			switch name {
			case "defineOp":
				if emitHints && provenLmaoCall && len(call.Arguments.Nodes) >= 2 && len(call.Arguments.Nodes) < 4 && isNamedType(t.checker, call.AsNode(), "Op") {
					hints = append(hints, hintRewrite{call: call, single: t.analyzeOpCompileMetadata(call.Arguments.Nodes[1])})
				}
			case "defineOps":
				if emitHints && provenLmaoCall && len(call.Arguments.Nodes) == 1 && call.Arguments.Nodes[0].Kind == shimast.KindObjectLiteralExpression && isNamedType(t.checker, call.AsNode(), "OpGroup") {
					group := collectDefineOpsHints(t, call.Arguments.Nodes[0].AsObjectLiteralExpression())
					if len(group) > 0 {
						hints = append(hints, hintRewrite{call: call, hints: group, isGroup: true})
					}
				}
			case "span":
				if emitHints {
					break
				}
				recv, _ := calleeNames(call)
				if recv != nil && len(call.Arguments.Nodes) >= 2 && len(call.Arguments.Nodes) <= 10 &&
					(recv.Kind == shimast.KindIdentifier || recv.Kind == shimast.KindThisKeyword) {
					opOrFn := call.Arguments.Nodes[1]
					plainFunction := opOrFn.Kind == shimast.KindArrowFunction || opOrFn.Kind == shimast.KindFunctionExpression
					provenOp := opOrFn.Kind == shimast.KindIdentifier && isNamedLmaoType(t.checker, opOrFn, "Op")
					recvType := t.checker.GetTypeAtLocation(recv)
					provenContext := recvType != nil && isLmaoContextType(t.checker, recvType)
					if provenOp && provenContext {
						t.opSpans[call] = true
					}
					if provenContext && (plainFunction || provenOp) && t.vocabulary != nil {
						if text, literal := literalVocabularyValue(call.Arguments.Nodes[0]); literal {
							t.vocabulary.add(vocabularySpanName, text, call, t.file.FileName())
						}
					}
				}
			}
		}
		node.ForEachChild(func(child *shimast.Node) bool {
			visit(child)
			return false
		})
	}
	visit(root)
	return hints
}

func isOpType(chk *shimchecker.Checker, typ *shimchecker.Type) bool {
	name := chk.TypeToString(typ)
	if strings.HasPrefix(name, "Op<") {
		return true
	}
	sym := shimchecker.Type_getTypeNameSymbol(typ)
	return sym != nil && sym.Name == "Op"
}

func (t *fileTransformer) spanContextApproved(node *shimast.Node) bool {
	parent := node.Parent
	if parent == nil {
		return false
	}
	if parent.Kind == shimast.KindAwaitExpression && parent.AsAwaitExpression().Expression == node {
		return true
	}
	if parent.Kind == shimast.KindReturnStatement && parent.AsReturnStatement().Expression == node {
		for current := parent.Parent; current != nil; current = current.Parent {
			if isNestedFunction(current) {
				return current.ModifierFlags()&shimast.ModifierFlagsAsync != 0
			}
		}
		return false
	}
	if parent.Kind == shimast.KindArrowFunction {
		fn := parent.AsArrowFunction()
		return fn.Body == node && parent.ModifierFlags()&shimast.ModifierFlagsAsync != 0
	}
	return false
}

func baseCompileMetadataProperties(analysis opCompileAnalysis) []*shimast.Node {
	properties := []*shimast.Node{
		factory.NewPropertyAssignment(nil, ident("runtimeHint"), nil, nil, num(int(analysis.runtimeHint))),
	}
	if len(analysis.eagerColumns) > 0 {
		elements := make([]*shimast.Node, len(analysis.eagerColumns))
		for index, name := range analysis.eagerColumns {
			elements[index] = str(name)
		}
		properties = append(properties, factory.NewPropertyAssignment(nil, ident("eagerColumns"), nil, nil,
			factory.NewArrayLiteralExpression(factory.NewNodeList(elements), false)))
	}
	return properties
}

func (t *fileTransformer) compileMetadataNode(analysis opCompileAnalysis) *shimast.Node {
	properties := baseCompileMetadataProperties(analysis)
	if len(analysis.localMessageDictionary) > 0 {
		elements := make([]*shimast.Node, len(analysis.localMessageDictionary))
		for index, globalID := range analysis.localMessageDictionary {
			elements[index] = t.staticVocabularyOperand(globalID)
		}
		frozenDictionary := callExpr(propAccess(ident("Object"), "freeze"), []*shimast.Node{
			factory.NewArrayLiteralExpression(factory.NewNodeList(elements), false),
		})
		properties = append(properties, factory.NewPropertyAssignment(
			nil, ident("localMessageDictionary"), nil, nil, frozenDictionary,
		))
	}
	return factory.NewObjectLiteralExpression(factory.NewNodeList(properties), true)
}

func (t *fileTransformer) applyHintRewrites(rewrites []hintRewrite) {
	for _, rewrite := range rewrites {
		args := append([]*shimast.Node{}, rewrite.call.Arguments.Nodes...)
		if rewrite.isGroup {
			names := make([]string, 0, len(rewrite.hints))
			for name := range rewrite.hints {
				names = append(names, name)
			}
			sort.Strings(names)
			props := make([]*shimast.Node, 0, len(names))
			for _, name := range names {
				props = append(props, factory.NewPropertyAssignment(nil, str(name), nil, nil, t.compileMetadataNode(rewrite.hints[name])))
			}
			args = append(args, factory.NewObjectLiteralExpression(factory.NewNodeList(props), true))
		} else {
			for len(args) < 3 {
				args = append(args, ident("undefined"))
			}
			args = append(args, t.compileMetadataNode(rewrite.single))
		}
		rewrite.call.Arguments = factory.NewNodeList(args)
	}
}

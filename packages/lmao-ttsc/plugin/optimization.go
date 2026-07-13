package main

import (
	"sort"
	"strings"

	shimast "github.com/microsoft/typescript-go/shim/ast"
	shimchecker "github.com/microsoft/typescript-go/shim/checker"
)

const (
	runtimeHintTag      uint32 = 1 << 16
	runtimeHintLog      uint32 = 1 << 17
	runtimeHintFF       uint32 = 1 << 18
	runtimeHintSpan     uint32 = 1 << 19
	runtimeHintResult   uint32 = 1 << 20
	runtimeHintScope    uint32 = 1 << 21
	runtimeHintDeps     uint32 = 1 << 22
	runtimeHintAnalyzed uint32 = 1 << 23
	runtimeHintMessageStatic  uint32 = 1 << 24
	runtimeHintMessageDynamic uint32 = 1 << 25
	runtimeHintMessageMixed   uint32 = runtimeHintMessageStatic | runtimeHintMessageDynamic
)

var contextCapability = map[string]uint32{
	"tag": runtimeHintTag, "log": runtimeHintLog, "ff": runtimeHintFF,
	"span": runtimeHintSpan, "spanSync": runtimeHintSpan,
	"ok": runtimeHintResult, "err": runtimeHintResult,
	"scope": runtimeHintScope, "setScope": runtimeHintScope, "deps": runtimeHintDeps,
}

type opCompileAnalysis struct {
	runtimeHint uint32
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

// analyzeOpFunction is intentionally closed-world. A hint is emitted only when
// every use of the context parameter is a direct access to a known capability.
func analyzeOpFunction(fn *shimast.Node, staticLogIDs map[*shimast.CallExpression]globalVocabularyID) uint32 {
	params, body, _, ok := functionParts(fn)
	if !ok || params == nil || len(params.Nodes) == 0 || body == nil {
		return 0
	}
	first := params.Nodes[0]
	if first.Kind != shimast.KindParameter || first.Name() == nil || first.Name().Kind != shimast.KindIdentifier {
		return 0
	}
	ctxName := shimast.NodeText(first.Name())
	if ctxName == "" {
		return 0
	}

	valid := true
	capacityKnown := true
	capacity := uint32(2) // rows 0 (tags) and 1 (terminal result)
	caps := uint32(0)
	hasStaticMessage := false
	hasDynamicMessage := false
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
				hasDynamicMessage = true
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
							hasStaticMessage = true
						} else {
							hasDynamicMessage = true
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
		return 0
	}
	if !capacityKnown {
		capacity = 0
	}
	messageHint := runtimeHintMessageStatic
	if hasDynamicMessage {
		messageHint = runtimeHintMessageDynamic
		if hasStaticMessage {
			messageHint = runtimeHintMessageMixed
		}
	}
	return runtimeHintAnalyzed | messageHint | caps | capacity
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

// analyzeOpCompileMetadata derives only runtime execution hints. Static log
// vocabulary is whole-program metadata collected independently of Op nesting.
func (t *fileTransformer) analyzeOpCompileMetadata(fn *shimast.Node) opCompileAnalysis {
	return opCompileAnalysis{runtimeHint: analyzeOpFunction(fn, t.staticLogIDs)}
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
					if provenOp && provenContext { t.opSpans[call] = true }
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

func compileMetadataNode(analysis opCompileAnalysis) *shimast.Node {
	return factory.NewObjectLiteralExpression(factory.NewNodeList([]*shimast.Node{
		factory.NewPropertyAssignment(nil, ident("runtimeHint"), nil, nil, num(int(analysis.runtimeHint))),
	}), true)
}

func applyHintRewrites(rewrites []hintRewrite) {
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
				props = append(props, factory.NewPropertyAssignment(nil, str(name), nil, nil, compileMetadataNode(rewrite.hints[name])))
			}
			args = append(args, factory.NewObjectLiteralExpression(factory.NewNodeList(props), true))
		} else {
			for len(args) < 3 {
				args = append(args, ident("undefined"))
			}
			args = append(args, compileMetadataNode(rewrite.single))
		}
		rewrite.call.Arguments = factory.NewNodeList(args)
	}
}

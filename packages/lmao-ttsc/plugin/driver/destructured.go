package lmao

import (
	shimast "github.com/microsoft/typescript-go/shim/ast"
)

// generatedContextName is the identifier a destructured context parameter is
// replaced by (spec 01o §3). It is emitted only after proving the name occurs
// nowhere in the function.
const generatedContextName = "__ctx"

// destructuredRewrite is one function literal whose first parameter is an object
// binding pattern binding `span`, proven safe to re-root onto a named context
// parameter.
//
// The pattern node is REUSED as the residual declaration's binding name rather
// than rebuilt: that is what preserves aliases, defaults, and nested bindings
// verbatim, with no reconstruction to get wrong. The parameter node itself is
// replaced, because a ParameterDeclaration's name is not assignable.
type destructuredRewrite struct {
	fn      *shimast.Node             // arrow function or function expression
	params  *shimast.ParameterList    // its parameter list; slot 0 is the pattern
	pattern *shimast.Node             // the object binding pattern being replaced
	body    *shimast.Node             // block or concise expression body
	spanEl  *shimast.Node             // the binding element that bound `span`
	calls   []*shimast.CallExpression // bare span(...) calls to re-root
}

// collectDestructuredRewrites finds every function literal whose destructured
// context can be re-rooted. Checker queries happen here, over the untouched
// parse tree, because the mutation pass runs after synthesized nodes exist and
// the tsgo checker panics on their -1 positions.
//
// The preflight is all-or-nothing per function: a mixed rewrite would change
// destructuring semantics, so any single unprovable use declines the whole
// literal and leaves every one of its span calls on the public dispatcher.
func (t *fileTransformer) collectDestructuredRewrites(root *shimast.Node) []destructuredRewrite {
	if t.checker == nil {
		return nil
	}
	var rewrites []destructuredRewrite
	var visit func(*shimast.Node)
	visit = func(node *shimast.Node) {
		if node == nil {
			return
		}
		if node.Kind == shimast.KindArrowFunction || node.Kind == shimast.KindFunctionExpression {
			if rewrite, ok := t.analyzeDestructuredContext(node); ok {
				rewrites = append(rewrites, rewrite)
			}
		}
		node.ForEachChild(func(child *shimast.Node) bool {
			visit(child)
			return false
		})
	}
	visit(root)
	return rewrites
}

// analyzeDestructuredContext proves one function literal rewritable.
//
// The context proof is the CHECKER TYPE of the destructured parameter, not the
// identity of the callee it is passed to: that is strictly stronger than a
// syntactic `op`/`defineOp`/`defineOps`/`task` whitelist, since it cannot admit
// a same-named foreign function, and it admits a proven context the whitelist
// would miss.
func (t *fileTransformer) analyzeDestructuredContext(fn *shimast.Node) (destructuredRewrite, bool) {
	var empty destructuredRewrite
	params, body, _, ok := functionParts(fn)
	if !ok || params == nil || body == nil || len(params.Nodes) == 0 {
		return empty, false
	}
	first := params.Nodes[0]
	if first.Kind != shimast.KindParameter {
		return empty, false
	}
	parameter := first.AsParameterDeclaration()
	// A defaulted or rest context parameter would have to keep that default or
	// spread on the replacement binding; neither is a context this rewrite can
	// name.
	if parameter.Initializer != nil || parameter.DotDotDotToken != nil {
		return empty, false
	}
	pattern := parameter.Name()
	if pattern == nil || pattern.Kind != shimast.KindObjectBindingPattern {
		return empty, false
	}
	paramType := t.checker.GetTypeAtLocation(first)
	if paramType == nil || !isLmaoContextType(t.checker, paramType) {
		return empty, false
	}

	spanElement, local, ok := spanBindingElement(pattern)
	if !ok {
		return empty, false
	}
	// `__ctx` must not already mean something here: the replacement binds it for
	// the whole function, so any existing occurrence would be captured.
	if subtreeBindsName(fn, generatedContextName) {
		return empty, false
	}

	// Escape analysis. The destructured local is provably still the context's
	// `span` member exactly when every occurrence of it in the body is the
	// CALLEE of a call expression. The runtime installs `span` as an own
	// property holding a closure over `self`, never over `this`
	// (packages/lmao/src/lib/spanContext.ts:895-1047), so a callee-position
	// `span(a, b)` is exactly `ctx.span(a, b)` and re-rooting it is an identity.
	// Any other occurrence — passed as a value, aliased, returned, or
	// re-declared in an inner scope — is an escape whose target this pass cannot
	// prove, so it declines.
	uses := &destructuredUses{local: local}
	uses.scan(body)
	if uses.escaped || len(uses.calls) == 0 {
		return empty, false
	}
	// Every bare call must satisfy the same §2 proof the member form does;
	// otherwise re-rooting would buy a longer expression and no lowering.
	for _, call := range uses.calls {
		if !t.provenBareSpanCall(call) {
			return empty, false
		}
	}
	for _, call := range uses.calls {
		opOrFn := call.Arguments.Nodes[1]
		if opOrFn.Kind == shimast.KindArrowFunction || opOrFn.Kind == shimast.KindFunctionExpression {
			t.fnSpans[call] = true
			continue
		}
		t.opSpans[call] = true
	}
	return destructuredRewrite{
		fn:      fn,
		params:  params,
		pattern: pattern,
		body:    body,
		spanEl:  spanElement,
		calls:   uses.calls,
	}, true
}

// spanBindingElement returns the element binding `span` and the local name it
// binds. A rest element anywhere declines the pattern: removing `span` from the
// pattern would silently move it INTO the rest object and change its contents.
func spanBindingElement(pattern *shimast.Node) (element *shimast.Node, local string, ok bool) {
	elements := pattern.AsBindingPattern().Elements
	if elements == nil {
		return nil, "", false
	}
	for _, node := range elements.Nodes {
		if node.Kind != shimast.KindBindingElement {
			return nil, "", false
		}
		binding := node.AsBindingElement()
		if binding.DotDotDotToken != nil {
			return nil, "", false
		}
		property := binding.Name()
		if binding.PropertyName != nil {
			property = binding.PropertyName
		}
		name, literal := literalPropertyName(property)
		if !literal {
			return nil, "", false
		}
		if name != "span" {
			continue
		}
		// A default would mean the local is only sometimes the context's member,
		// and a nested pattern does not bind a callable at all.
		if binding.Initializer != nil || binding.Name() == nil || binding.Name().Kind != shimast.KindIdentifier {
			return nil, "", false
		}
		if element != nil {
			return nil, "", false // duplicate span binding
		}
		element, local, ok = node, shimast.NodeText(binding.Name()), true
	}
	return element, local, ok
}

// destructuredUses accumulates the callee-position calls of one local name and
// records whether the name ever appeared anywhere else.
type destructuredUses struct {
	local   string
	calls   []*shimast.CallExpression
	escaped bool
}

func (u *destructuredUses) scan(node *shimast.Node) {
	if node == nil || u.escaped {
		return
	}
	if node.Kind == shimast.KindIdentifier {
		// Reached only when the identifier was NOT consumed as a callee below.
		if shimast.NodeText(node) == u.local {
			u.escaped = true
		}
		return
	}
	if node.Kind == shimast.KindCallExpression {
		call := node.AsCallExpression()
		if call.Expression.Kind == shimast.KindIdentifier && shimast.NodeText(call.Expression) == u.local {
			u.calls = append(u.calls, call)
			// The callee is accounted for; the arguments still are not, so an
			// argument that passes the local along still escapes.
			if call.Arguments != nil {
				for _, arg := range call.Arguments.Nodes {
					u.scan(arg)
				}
			}
			return
		}
	}
	node.ForEachChild(func(child *shimast.Node) bool {
		u.scan(child)
		return false
	})
}

// provenBareSpanCall applies the §2 proof to a bare `span(...)` call: the spanN
// ABI takes at most eight trailing arguments, and the op position must be a
// checker-proved Op identifier or an inline closure. The override form's object
// literal satisfies neither and so declines, exactly as it does on the member
// form.
func (t *fileTransformer) provenBareSpanCall(call *shimast.CallExpression) bool {
	if call.Arguments == nil || len(call.Arguments.Nodes) < 2 || len(call.Arguments.Nodes) > 10 {
		return false
	}
	opOrFn := call.Arguments.Nodes[1]
	if opOrFn.Kind == shimast.KindArrowFunction || opOrFn.Kind == shimast.KindFunctionExpression {
		return true
	}
	return opOrFn.Kind == shimast.KindIdentifier && isNamedLmaoType(t.checker, opOrFn, "Op")
}

// subtreeBindsName reports whether an identifier with the given text occurs
// anywhere in the subtree.
func subtreeBindsName(node *shimast.Node, name string) bool {
	found := false
	var visit func(*shimast.Node)
	visit = func(current *shimast.Node) {
		if current == nil || found {
			return
		}
		if current.Kind == shimast.KindIdentifier && shimast.NodeText(current) == name {
			found = true
			return
		}
		current.ForEachChild(func(child *shimast.Node) bool {
			visit(child)
			return false
		})
	}
	visit(node)
	return found
}

// applyDestructuredRewrites replaces each proven destructured context parameter
// with `__ctx`, rebinds the remaining properties as the first body statement,
// and re-roots the bare span calls onto that parameter. The §2 lowering itself
// is left to trySpanRewrite during the walk — there is exactly one span lowering
// in this transform, and this pass only decides its receiver.
//
// It must run AFTER the tag/log/result inline passes, which address statements
// by index into the original lists, and BEFORE the walk that lowers the calls.
func (t *fileTransformer) applyDestructuredRewrites() {
	for _, rewrite := range t.destructures {
		contextParameter := factory.NewParameterDeclaration(nil, nil, ident(generatedContextName), nil, nil, nil)
		rewrite.params.Nodes[0] = contextParameter

		var residual []*shimast.Node
		for _, element := range rewrite.pattern.AsBindingPattern().Elements.Nodes {
			if element != rewrite.spanEl {
				residual = append(residual, element)
			}
		}
		if len(residual) > 0 {
			// Reuse the pattern minus `span`, so aliases, defaults, and nested
			// bindings survive exactly as written.
			rewrite.pattern.AsBindingPattern().Elements = factory.NewNodeList(residual)
			declaration := factory.NewVariableStatement(nil, factory.NewVariableDeclarationList(
				factory.NewNodeList([]*shimast.Node{
					factory.NewVariableDeclaration(rewrite.pattern, nil, nil, ident(generatedContextName)),
				}),
				shimast.NodeFlagsConst,
			))
			t.prependStatement(rewrite, declaration)
		}

		for _, call := range rewrite.calls {
			call.Expression = propAccess(ident(generatedContextName), "span")
		}
	}
}

// prependStatement puts one statement at the top of a function body. A concise
// arrow body becomes a block with an explicit return, because there is nowhere
// else for the statement to go; a body that needs no statement is left concise
// rather than expanded for its own sake.
func (t *fileTransformer) prependStatement(rewrite destructuredRewrite, statement *shimast.Node) {
	if rewrite.body.Kind == shimast.KindBlock {
		block := rewrite.body.AsBlock()
		block.Statements = factory.NewNodeList(append([]*shimast.Node{statement}, block.Statements.Nodes...))
		return
	}
	block := factory.NewBlock(factory.NewNodeList([]*shimast.Node{
		statement,
		factory.NewReturnStatement(rewrite.body),
	}), true)
	switch rewrite.fn.Kind {
	case shimast.KindArrowFunction:
		rewrite.fn.AsArrowFunction().Body = block
	case shimast.KindFunctionExpression:
		rewrite.fn.AsFunctionExpression().Body = block
	}
}

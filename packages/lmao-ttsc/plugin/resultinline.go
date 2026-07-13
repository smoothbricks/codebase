// resultinline.go — partial inlining of result chains (ctx.ok/ctx.err).
//
// The runtime's Ok/Err fluent methods (.line/.message/.with/schema setters)
// delegate to a GeneratedResultWriter — a fixed-position writer at ROW 1
// (packages/lmao/src/lib/codegen/fixedPositionWriterGenerator.ts), the same
// family as the TagWriter at row 0. That makes result chains tag-shaped:
// every chained write lands at a compile-time-known index. The partial
// inline keeps the ok()/err() call itself (entry semantics, value capture)
// and replaces the fluent chain with direct writes at [1], guarded on the
// result's buffer being present (reproducing the runtime's
// `_resultWriter()?.` no-op when there is no buffer):
//
//	return ctx.ok(user).line(42).with({ userId: u });
//
// becomes
//
//	{
//	  const $$r = ctx.ok(user);
//	  const $$b = $$r._buffer;
//	  if ($$b) {
//	    if ($$b.line_values) { $$b.line_values[1] = 42; ...nulls bit 1... }
//	    if ($$b.userId_values) { $$b.userId_values[1] = u; ... }
//	  }
//	  return $$r;
//	}
//
// Fires in ExpressionStatement and ReturnStatement contexts. Conservative
// bails: receiver not Checker-proved LMAO context (TaskContext/SpanContext/
// ModuleContext/RequestContext), chained method that is not line/message/
// with/schema field, non-literal with(), non-single-arg links.
package main

import (
	"strings"

	shimast "github.com/microsoft/typescript-go/shim/ast"
	shimchecker "github.com/microsoft/typescript-go/shim/checker"
)

var lmaoContextTypeNames = []string{"TaskContext", "SpanContext", "ModuleContext", "RequestContext"}

type resultInline struct {
	list     *shimast.NodeList
	index    int
	isReturn bool
	okCall   *shimast.Node // the ctx.ok(...) / ctx.err(...) call, kept verbatim
	writes   []tagWrite    // line/message/schema writes in execution order
	schema   map[string]schemaField
}

func isLmaoContextType(chk *shimchecker.Checker, t *shimchecker.Type) bool {
	name := chk.TypeToString(t)
	for _, w := range lmaoContextTypeNames {
		if name == w || strings.HasPrefix(name, w+"<") {
			return true
		}
	}
	if sym := shimchecker.Type_getTypeNameSymbol(t); sym != nil {
		for _, w := range lmaoContextTypeNames {
			if sym.Name == w {
				return true
			}
		}
	}
	return false
}

// findResultInline analyzes an ok/err chain. Checker queries only; no mutation.
func (t *fileTransformer) findResultInline(call *shimast.CallExpression) (*resultInline, bool) {
	if t.checker == nil {
		return nil, false
	}
	type link struct {
		name string
		args []*shimast.Node
	}
	var links []link
	current := call
	for {
		expr := current.Expression
		if expr.Kind != shimast.KindPropertyAccessExpression {
			return nil, false
		}
		pa := expr.AsPropertyAccessExpression()
		name := shimast.NodeText(pa.Name())

		if resultMethods[name] {
			// Chain root: receiver must be a Checker-proved LMAO context.
			recvType := t.checker.GetTypeAtLocation(pa.Expression)
			if recvType == nil || !isLmaoContextType(t.checker, recvType) {
				return nil, false
			}
			if len(links) == 0 {
				return nil, false // bare ok()/err(): nothing to inline
			}
			// Schema comes from the Ok/Err type's own fluent surface.
			okType := t.checker.GetTypeAtLocation(current.AsNode())
			schema := map[string]schemaField{}
			if okType != nil {
				schema = extractSchema(t.checker, okType)
				for _, sys := range []string{"line", "message", "isOk", "isErr", "map", "match", "unwrapOr", "success"} {
					delete(schema, sys)
				}
			}
			in := &resultInline{okCall: current.AsNode(), schema: schema}
			hasLine := false
			for _, l := range links {
				if l.name == "line" {
					hasLine = true
				}
			}
			if !hasLine {
				// §6 semantics folded in: inject the call-site line.
				in.writes = append(in.writes, tagWrite{field: "line", arg: num(t.lineOf(current.AsNode()))})
			}
			for _, l := range links {
				switch {
				case l.name == "line" || l.name == "message":
					if len(l.args) != 1 {
						return nil, false
					}
					in.writes = append(in.writes, tagWrite{field: l.name, arg: l.args[0]})
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
						return nil, false
					}
					in.writes = append(in.writes, tagWrite{field: l.name, arg: l.args[0]})
				}
			}
			return in, true
		}

		links = append([]link{{name: name, args: current.Arguments.Nodes}}, links...)
		next := pa.Expression
		if next.Kind != shimast.KindCallExpression {
			return nil, false
		}
		current = next.AsCallExpression()
	}
}

// applyResultInlines splices replacement blocks (phase B — no checker use).
func (t *fileTransformer) applyResultInlines(inlines []resultInline) {
	for _, in := range inlines {
		res := ident("$$r")
		buf := ident("$$b")

		var writes []*shimast.Node
		for _, w := range in.writes {
			info, known := in.schema[w.field]
			switch {
			case known && info.kind == fieldEnum:
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
				writes = append(writes, factory.NewExpressionStatement(
					callExpr(propAccess(buf, w.field), []*shimast.Node{num(1), enumIdx})))
			case known && info.kind == fieldBool:
				writes = append(writes, factory.NewExpressionStatement(
					callExpr(propAccess(buf, w.field), []*shimast.Node{num(1), w.arg})))
			default:
				writes = append(writes, guardedRawWrite(buf, num(1), w.field, w.arg))
			}
		}

		stmts := []*shimast.Node{
			constDecl(res, in.okCall),
			constDecl(buf, propAccess(res, "_buffer")),
			factory.NewIfStatement(buf, factory.NewBlock(factory.NewNodeList(writes), true), nil),
		}
		if in.isReturn {
			stmts = append(stmts, factory.NewReturnStatement(res))
		}
		in.list.Nodes[in.index] = factory.NewBlock(factory.NewNodeList(stmts), true)
	}
}

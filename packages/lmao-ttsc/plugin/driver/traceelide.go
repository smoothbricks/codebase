// traceelide.go — the trace level's compile-away contract (spec 01o §6.3).
//
// A trace entry is a statement of its own: a trace call on a checker-proved
// LMAO logger, optionally followed by the entry's fluent setters.
//
//	ctx.log.trace('cache warm no-op').reason('already warm');
//
// A production compile removes every such statement whole, the call, its
// setters and every argument expression, so nothing of it runs. A test compile
// keeps it for the log lowering like any other level, so a trace-claim test can
// count it. A trace call anywhere else is LMAO_TRACE_NOT_STATEMENT in both
// modes: production cannot remove a value somebody consumes, and keeping one
// would ship trace cost that the test compile never flagged.
package lmao

import (
	"fmt"

	shimast "github.com/microsoft/typescript-go/shim/ast"
)

// traceEntries is what a compile does with trace statements. The zero value
// names no mode, so a compile that forgot to choose one cannot run.
type traceEntries uint8

const (
	// traceEntriesElided removes every trace statement: the production compile.
	traceEntriesElided traceEntries = iota + 1
	// traceEntriesKept lowers trace statements like any other log level: the
	// test compile.
	traceEntriesKept
)

// traceEntriesFor maps the compile's NODE_ENV to its trace mode. Test runners
// (bun test, vitest) set NODE_ENV=test before any transform runs, and ttsc hands
// its compiler the host's environment; every other compile, NODE_ENV unset
// included, is a production compile.
func traceEntriesFor(nodeEnv string) traceEntries {
	if nodeEnv == "test" {
		return traceEntriesKept
	}
	return traceEntriesElided
}

// traceStatement is one statement that holds a whole trace entry, and the
// trace call at its root.
type traceStatement struct {
	statement *shimast.Node
	call      *shimast.CallExpression
}

// collectTraceStatements returns every trace statement of the file and reports
// every proved trace call that is not one. Checker queries only; it must run
// on the untouched tree.
func (t *fileTransformer) collectTraceStatements(root *shimast.Node) []traceStatement {
	if t.checker == nil {
		return nil
	}
	var statements []traceStatement
	var visit func(*shimast.Node)
	visit = func(node *shimast.Node) {
		if node.Kind == shimast.KindPropertyAccessExpression {
			if statement, ok := t.traceStatementAt(node); ok {
				statements = append(statements, statement)
			}
		}
		node.ForEachChild(func(child *shimast.Node) bool {
			visit(child)
			return false
		})
	}
	visit(root)
	return statements
}

// traceStatementAt classifies one property access. It answers the statement a
// proved trace entry occupies; an access that is not an LMAO trace, or that it
// reported as LMAO_TRACE_NOT_STATEMENT, answers false.
func (t *fileTransformer) traceStatementAt(access *shimast.Node) (traceStatement, bool) {
	member := access.AsPropertyAccessExpression()
	if shimast.NodeText(member.Name()) != "trace" {
		return traceStatement{}, false
	}
	receiverType := t.checker.GetTypeAtLocation(member.Expression)
	if receiverType == nil || !isSpanLoggerType(t.checker, receiverType) {
		return traceStatement{}, false
	}
	parent := access.Parent
	if parent.Kind != shimast.KindCallExpression || parent.AsCallExpression().Expression != access {
		t.reportTraceNotStatement(access, "a trace method is only ever called, never read as a value")
		return traceStatement{}, false
	}
	call := parent.AsCallExpression()
	if !hasLmaoCallProvenance(t.checker, call) {
		return traceStatement{}, false // collectLogVocabulary reports LMAO_LOGGER_PROOF_REQUIRED
	}
	if isFluentLogEntryType(t.checker, receiverType) {
		t.reportTraceNotStatement(access, "a trace entry starts its own statement on the logger, not another entry's chain")
		return traceStatement{}, false
	}
	chain := parent
	for {
		for chain.Parent.Kind == shimast.KindParenthesizedExpression {
			chain = chain.Parent
		}
		next := chain.Parent
		if next.Kind != shimast.KindPropertyAccessExpression || next.AsPropertyAccessExpression().Expression != chain {
			break
		}
		link := next.Parent
		if link.Kind != shimast.KindCallExpression || link.AsCallExpression().Expression != next {
			break
		}
		if logMethods[shimast.NodeText(next.AsPropertyAccessExpression().Name())] {
			t.reportTraceNotStatement(next, "a trace entry's chain must not start another log entry")
			return traceStatement{}, false
		}
		if !hasLmaoCallProvenance(t.checker, link.AsCallExpression()) {
			t.reportTraceNotStatement(next, "a trace entry's chain continues only with the entry's own setters")
			return traceStatement{}, false
		}
		chain = link
	}
	if chain.Parent.Kind != shimast.KindExpressionStatement {
		t.reportTraceNotStatement(access, "a trace entry must be a statement of its own, never a value")
		return traceStatement{}, false
	}
	return traceStatement{statement: chain.Parent, call: call}, true
}

func (t *fileTransformer) reportTraceNotStatement(node *shimast.Node, message string) {
	if t.vocabulary != nil {
		t.vocabulary.addDiagnostic("LMAO_TRACE_NOT_STATEMENT", t.file.FileName(), node.Pos(), message)
	}
}

// elideTraceStatement removes one trace statement from the tree. No checker
// use. A statement list loses the statement; a single-statement slot (an if
// branch, a loop body, a label) takes an empty statement, so the user code
// around it, a condition included, still runs.
func elideTraceStatement(statement *shimast.Node) {
	parent := statement.Parent
	if parent.CanHaveStatements() {
		list := parent.StatementList()
		// A fresh slice: a node list's backing array may be shared with a
		// header held elsewhere, and an in-place shift would corrupt it.
		kept := make([]*shimast.Node, 0, len(list.Nodes)-1)
		for _, node := range list.Nodes {
			if node != statement {
				kept = append(kept, node)
			}
		}
		list.Nodes = kept
		return
	}
	empty := factory.NewEmptyStatement()
	switch parent.Kind {
	case shimast.KindIfStatement:
		branch := parent.AsIfStatement()
		if branch.ThenStatement == statement {
			branch.ThenStatement = empty
		} else {
			branch.ElseStatement = empty
		}
	case shimast.KindDoStatement:
		parent.AsDoStatement().Statement = empty
	case shimast.KindWhileStatement:
		parent.AsWhileStatement().Statement = empty
	case shimast.KindForStatement:
		parent.AsForStatement().Statement = empty
	case shimast.KindForInStatement, shimast.KindForOfStatement:
		parent.AsForInOrOfStatement().Statement = empty
	case shimast.KindLabeledStatement:
		parent.AsLabeledStatement().Statement = empty
	case shimast.KindWithStatement:
		parent.AsWithStatement().Statement = empty
	default:
		panic(fmt.Sprintf("trace statement under a %s, which holds no statement", parent.Kind))
	}
}

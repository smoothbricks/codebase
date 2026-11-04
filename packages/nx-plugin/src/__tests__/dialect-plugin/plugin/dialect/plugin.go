// Package dialect admits `for await (… of lowered)` in a generator, which TypeScript refuses outside an async function
// (TS1103), and lowers it to the sync `for (… of lowered)`, as a decide plugin lowers its part-stream loop: the
// nx-plugin's tests hold that a lane gating on the program's diagnostics compiles the dialect and still refuses every
// other `for await` the checker refuses.
package dialect

import (
	"github.com/microsoft/typescript-go/shim/ast"
	"github.com/samchon/ttsc/packages/ttsc/driver"
)

// forAwaitOutsideAsync is TypeScript's "'for await' loops are only allowed within async functions and at the top
// levels of modules."
const forAwaitOutsideAsync = 1103

// Iterated is the one identifier a dialect loop iterates.
const Iterated = "lowered"

func init() {
	driver.RegisterPlugin(plugin{})
}

type plugin struct{}

// loops are the dialect loops of file.
func loops(file *ast.SourceFile) []*ast.Node {
	var found []*ast.Node
	var visit func(node *ast.Node) bool
	visit = func(node *ast.Node) bool {
		if node.Kind == ast.KindForOfStatement {
			loop := node.AsForInOrOfStatement()
			if loop.AwaitModifier != nil && loop.Expression.Kind == ast.KindIdentifier && loop.Expression.Text() == Iterated {
				found = append(found, node)
			}
		}
		node.ForEachChild(visit)
		return false
	}
	file.AsNode().ForEachChild(visit)
	return found
}

// LoweredDiagnostics names each TS1103 that sits on a dialect loop's await.
func (plugin) LoweredDiagnostics(_ *driver.Program, _ driver.PluginContext, diagnostics []*ast.Diagnostic) []*ast.Diagnostic {
	var lowered []*ast.Diagnostic
	for _, diagnostic := range diagnostics {
		if diagnostic.Code() != forAwaitOutsideAsync || diagnostic.File() == nil {
			continue
		}
		for _, loop := range loops(diagnostic.File()) {
			await := loop.AsForInOrOfStatement().AwaitModifier
			if diagnostic.Pos() >= await.Pos() && diagnostic.Pos() < await.End() {
				lowered = append(lowered, diagnostic)
			}
		}
	}
	return lowered
}

// ApplyProgram lowers each dialect loop in place: without its await it is the sync loop.
func (plugin) ApplyProgram(prog *driver.Program, _ driver.PluginContext) error {
	for _, file := range prog.SourceFiles() {
		for _, loop := range loops(file) {
			loop.AsForInOrOfStatement().AwaitModifier = nil
		}
	}
	return nil
}

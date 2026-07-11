// ttsc transform plugin for LMAO — Go port of spec 01o (smoo/lmao!n/transformer).
//
// Implements the structural (TypeChecker-free) transformations:
//
//   §1/§2  span() line injection + monomorphic spanN rewrite (heuristic Op detection)
//   §5     defineModule() metadata injection (git_sha, package_name, package_file)
//   §6     .line(N) injection on log/ok/err chains
//   §7     task('name', fn) line injection
//
// NOT yet ported (staged, requires the tsgo Checker via driver shims):
//   §3     destructured-context rewriting (shipped in the TS transformer;
//          needs identifier-binding analysis parity before porting)
//   §4     tag-chain inlining with schema specialization (enum indices,
//          eager/lazy null-bitmap elision). The checker-free fallback of §4
//          is deliberately NOT ported either: emitting direct buffer writes
//          without the schema risks divergence from the TS inliner's output;
//          run the classic transformer for tag inlining until the Checker
//          port lands.
//
// Column-name contract (spec 01e): any future §4 port must write
// library-local (unprefixed) column names; prefix/mapColumns remapping is
// cold-path-only via RemappedBufferView and must never appear in emitted
// hot-path writes.
//
// VERIFICATION STATUS: authored without a local Go toolchain (none in this
// repo's devenv). Compiles-by-inspection against the ttsc shim API as
// documented in ttsc's authoring/getting-started and end-to-end walkthroughs;
// run `go vet ./...` + the fixture e2e in a Go-enabled environment before
// publishing (see plugin/main_test.go).
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	shimast "github.com/microsoft/typescript-go/shim/ast"
	shimprinter "github.com/microsoft/typescript-go/shim/printer"
	"github.com/samchon/ttsc/packages/ttsc/driver"
)

const pluginName = "@smoothbricks/lmao-transformer"
const pluginVersion = "0.1.6"

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "%s: command required\n", pluginName)
		os.Exit(2)
	}
	switch os.Args[1] {
	case "version", "-v", "--version":
		fmt.Printf("%s %s\n", pluginName, pluginVersion)
	case "check":
		// Transform-stage plugin: check is a no-op.
	case "transform":
		os.Exit(runTransform(os.Args[2:]))
	case "build":
		os.Exit(runBuild(os.Args[2:]))
	default:
		fmt.Fprintf(os.Stderr, "%s: unknown command %q\n", pluginName, os.Args[1])
		os.Exit(2)
	}
}

// ---------------------------------------------------------------------------
// Host plumbing (per ttsc authoring walkthrough)
// ---------------------------------------------------------------------------

func readFlags(args []string) (cwd, tsconfig string) {
	tsconfig = "tsconfig.json"
	for _, a := range args {
		switch {
		case strings.HasPrefix(a, "--cwd="):
			cwd = strings.TrimPrefix(a, "--cwd=")
		case strings.HasPrefix(a, "--tsconfig="):
			tsconfig = strings.TrimPrefix(a, "--tsconfig=")
		}
	}
	if cwd == "" {
		cwd, _ = os.Getwd()
	}
	if !filepath.IsAbs(tsconfig) {
		tsconfig = filepath.Join(cwd, tsconfig)
	}
	return cwd, tsconfig
}

type transformResult struct {
	TypeScript map[string]string `json:"typescript"`
}

func outputKey(cwd, fileName string) string {
	rel, err := filepath.Rel(cwd, fileName)
	if err != nil {
		return fileName
	}
	return filepath.ToSlash(rel)
}

func runTransform(args []string) int {
	cwd, tsconfig := readFlags(args)
	prog, _, err := driver.LoadProgram(cwd, tsconfig, driver.LoadProgramOptions{})
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: %v\n", pluginName, err)
		return 2
	}
	defer prog.Close()

	transformProgram(prog, cwd)

	printer := shimprinter.NewPrinter(shimprinter.PrinterOptions{}, shimprinter.PrintHandlers{}, nil)
	out := transformResult{TypeScript: map[string]string{}}
	for _, file := range prog.SourceFiles() {
		if file == nil || file.IsDeclarationFile {
			continue
		}
		out.TypeScript[outputKey(cwd, file.FileName())] = shimprinter.EmitSourceFile(printer, file)
	}
	data, err := json.Marshal(out)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: marshal failed: %v\n", pluginName, err)
		return 3
	}
	fmt.Println(string(data))
	return 0
}

func runBuild(args []string) int {
	cwd, tsconfig := readFlags(args)
	prog, _, err := driver.LoadProgram(cwd, tsconfig, driver.LoadProgramOptions{})
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: %v\n", pluginName, err)
		return 2
	}
	defer prog.Close()

	transformProgram(prog, cwd)

	if _, emitDiags, err := prog.EmitAllRaw(nil); err != nil {
		fmt.Fprintf(os.Stderr, "%s: emit failed: %v\n", pluginName, err)
		return 3
	} else if len(emitDiags) > 0 {
		for _, d := range emitDiags {
			fmt.Fprintln(os.Stderr, d.String())
		}
		return 2
	}
	return 0
}

// ---------------------------------------------------------------------------
// Transformations
// ---------------------------------------------------------------------------

var logMethods = map[string]bool{"info": true, "debug": true, "warn": true, "error": true, "trace": true}
var resultMethods = map[string]bool{"ok": true, "err": true}

func transformProgram(prog *driver.Program, cwd string) {
	for _, file := range prog.SourceFiles() {
		if file == nil || file.IsDeclarationFile {
			continue
		}
		t := &fileTransformer{file: file, cwd: cwd}
		t.walk(file.AsNode())
	}
}

type fileTransformer struct {
	file *shimast.SourceFile
	cwd  string
	// seenDefineModule guards the one-module-per-file invariant (spec 01o §5).
	seenDefineModule bool
}

func (t *fileTransformer) walk(node *shimast.Node) {
	if node == nil {
		return
	}
	if node.Kind == shimast.KindCallExpression {
		call := node.AsCallExpression()
		switch {
		case t.tryDefineModuleMetadata(call):
		case t.trySpanRewrite(call):
		case t.tryTaskLine(call):
		case t.tryChainLine(call, logMethods, isLogReceiver):
		case t.tryChainLine(call, resultMethods, nil):
		}
	}
	node.ForEachChild(func(child *shimast.Node) bool {
		t.walk(child)
		return false
	})
}

// lineOf returns the 1-based line of a node's start position.
func (t *fileTransformer) lineOf(node *shimast.Node) int {
	line, _ := shimast.GetLineAndCharacterOfPosition(t.file, node.Pos())
	return line + 1
}

// calleePropertyName returns the property name if the call target is
// `recv.name(...)`, or the identifier text for a bare `name(...)`.
func calleeNames(call *shimast.CallExpression) (recv *shimast.Node, name string) {
	expr := call.Expression
	if expr.Kind == shimast.KindPropertyAccessExpression {
		pa := expr.AsPropertyAccessExpression()
		return pa.Expression, pa.Name().Text()
	}
	if expr.Kind == shimast.KindIdentifier {
		return nil, expr.Text()
	}
	return nil, ""
}

// --- §5 defineModule metadata -------------------------------------------------

func (t *fileTransformer) tryDefineModuleMetadata(call *shimast.CallExpression) bool {
	_, name := calleeNames(call)
	if name != "defineModule" || len(call.Arguments.Nodes) == 0 {
		return false
	}
	arg := call.Arguments.Nodes[0]
	if arg.Kind != shimast.KindObjectLiteralExpression {
		return false
	}
	obj := arg.AsObjectLiteralExpression()
	if t.seenDefineModule {
		fmt.Fprintf(os.Stderr, "%s: invariant violation: %s contains multiple defineModule() declarations\n",
			pluginName, t.file.FileName())
		os.Exit(2)
	}
	t.seenDefineModule = true
	for _, prop := range obj.Properties.Nodes {
		if prop.Kind == shimast.KindPropertyAssignment && prop.Name() != nil && prop.Name().Text() == "metadata" {
			return false // already has metadata — leave alone
		}
	}

	gitSha := gitLastCommit(t.file.FileName(), t.cwd)
	pkgName, pkgFile := nearestPackage(t.file.FileName())

	// Prepend `metadata: { git_sha, package_name, package_file }` via a
	// synthesized property. Leaf-text mutations must set the synthesize flag
	// (ttsc synthesize-flag invariant for leaf-text mutations).
	meta := shimast.NewSynthesizedObjectLiteral(map[string]string{
		"git_sha":      gitSha,
		"package_name": pkgName,
		"package_file": pkgFile,
	})
	obj.Properties.Nodes = append(
		[]*shimast.Node{shimast.NewSynthesizedPropertyAssignment("metadata", meta)},
		obj.Properties.Nodes...,
	)
	return true
}

func gitLastCommit(filePath, cwd string) string {
	rel, err := filepath.Rel(cwd, filePath)
	if err != nil {
		return "unknown"
	}
	out, err := exec.Command("git", "-C", cwd, "rev-list", "-1", "HEAD", "--", filepath.ToSlash(rel)).Output()
	if err != nil {
		return "unknown"
	}
	sha := strings.TrimSpace(string(out))
	if sha == "" {
		return "unknown"
	}
	return sha
}

func nearestPackage(filePath string) (name, relFile string) {
	dir := filepath.Dir(filePath)
	root := filepath.VolumeName(dir) + string(filepath.Separator)
	for dir != root {
		data, err := os.ReadFile(filepath.Join(dir, "package.json"))
		if err == nil {
			var pkg struct {
				Name string `json:"name"`
			}
			if json.Unmarshal(data, &pkg) == nil && pkg.Name != "" {
				rel, err := filepath.Rel(dir, filePath)
				if err != nil {
					rel = filepath.Base(filePath)
				}
				return pkg.Name, filepath.ToSlash(rel)
			}
		}
		dir = filepath.Dir(dir)
	}
	return "unknown", filepath.Base(filePath)
}

// --- §1/§2 span rewrite -------------------------------------------------------

func (t *fileTransformer) trySpanRewrite(call *shimast.CallExpression) bool {
	recv, name := calleeNames(call)
	if name != "span" || recv == nil || len(call.Arguments.Nodes) < 2 {
		return false
	}
	args := call.Arguments.Nodes
	nameArg, opOrFn := args[0], args[1]
	rest := args[2:]

	// Heuristic Op detection (no Checker): non-function literal → Op path.
	isOp := opOrFn.Kind != shimast.KindArrowFunction && opOrFn.Kind != shimast.KindFunctionExpression

	line := t.lineOf(call.AsNode())
	methodName := fmt.Sprintf("span%d", len(rest))

	var bufferClass, remappedView, opMetadata, fn *shimast.Node
	if isOp {
		bufferClass = shimast.NewSynthesizedPropertyAccess(opOrFn, "SpanBufferClass")
		remappedView = shimast.NewSynthesizedPropertyAccess(opOrFn, "remappedViewClass")
		opMetadata = shimast.NewSynthesizedPropertyAccess(opOrFn, "metadata")
		fn = shimast.NewSynthesizedPropertyAccess(opOrFn, "fn")
	} else {
		buffer := shimast.NewSynthesizedPropertyAccess(recv, "_buffer")
		bufferClass = shimast.NewSynthesizedPropertyAccess(buffer, "constructor")
		remappedView = shimast.NewSynthesizedIdentifier("undefined")
		opMetadata = shimast.NewSynthesizedPropertyAccess(shimast.NewSynthesizedPropertyAccess(recv, "_buffer"), "_opMetadata")
		fn = opOrFn
	}

	newCtx := shimast.NewSynthesizedCall(shimast.NewSynthesizedPropertyAccess(recv, "_newCtx0"), nil)
	newArgs := append([]*shimast.Node{
		shimast.NewSynthesizedNumericLiteral(line),
		nameArg,
		newCtx,
		bufferClass,
		remappedView,
		opMetadata,
		fn,
	}, rest...)

	call.Expression = shimast.NewSynthesizedPropertyAccess(recv, methodName)
	call.Arguments.Nodes = newArgs
	return true
}

// --- §7 task line -------------------------------------------------------------

func (t *fileTransformer) tryTaskLine(call *shimast.CallExpression) bool {
	_, name := calleeNames(call)
	if name != "task" || len(call.Arguments.Nodes) != 2 {
		return false
	}
	if call.Arguments.Nodes[0].Kind != shimast.KindStringLiteral {
		return false
	}
	call.Arguments.Nodes = append(call.Arguments.Nodes,
		shimast.NewSynthesizedNumericLiteral(t.lineOf(call.AsNode())))
	return true
}

// --- §6 log / result .line(N) ---------------------------------------------------

// isLogReceiver checks the receiver of a log method is a `.log` property access.
func isLogReceiver(recv *shimast.Node) bool {
	return recv != nil &&
		recv.Kind == shimast.KindPropertyAccessExpression &&
		recv.AsPropertyAccessExpression().Name().Text() == "log"
}

// tryChainLine inserts `.line(N)` right after the matched method at the root
// of a fluent chain, preserving trailing calls, no-op when `.line` is present.
func (t *fileTransformer) tryChainLine(call *shimast.CallExpression, methods map[string]bool, receiverOK func(*shimast.Node) bool) bool {
	// Only fire at chain tops: parent must not be the expression of an outer
	// PropertyAccess+Call (walk() visits outermost first, so mark handled
	// chains by detecting `.line` presence instead of a WeakSet).
	target, trailing := findChainTarget(call, methods, receiverOK)
	if target == nil {
		return false
	}
	if chainHasLine(call) {
		return false
	}
	line := t.lineOf(target.AsNode())

	// Build: target.line(N), then re-hang the trailing links on top.
	lineCall := shimast.NewSynthesizedCall(
		shimast.NewSynthesizedPropertyAccess(target.AsNode(), "line"),
		[]*shimast.Node{shimast.NewSynthesizedNumericLiteral(line)},
	)
	rebuildChainOnto(call, lineCall, trailing)
	return true
}

type chainLink struct {
	name string
	args []*shimast.Node
}

// findChainTarget walks receiver-wards from `call` looking for the first
// method whose name is in `methods` (and whose receiver passes receiverOK),
// collecting the trailing links crossed on the way.
func findChainTarget(call *shimast.CallExpression, methods map[string]bool, receiverOK func(*shimast.Node) bool) (*shimast.CallExpression, []chainLink) {
	var trailing []chainLink
	current := call
	for {
		expr := current.Expression
		if expr.Kind != shimast.KindPropertyAccessExpression {
			return nil, nil
		}
		pa := expr.AsPropertyAccessExpression()
		name := pa.Name().Text()
		if methods[name] && (receiverOK == nil || receiverOK(pa.Expression)) {
			return current, trailing
		}
		trailing = append([]chainLink{{name: name, args: current.Arguments.Nodes}}, trailing...)
		if pa.Expression.Kind != shimast.KindCallExpression {
			return nil, nil
		}
		current = pa.Expression.AsCallExpression()
	}
}

func chainHasLine(call *shimast.CallExpression) bool {
	current := call
	for {
		expr := current.Expression
		if expr.Kind != shimast.KindPropertyAccessExpression {
			return false
		}
		pa := expr.AsPropertyAccessExpression()
		if pa.Name().Text() == "line" {
			return true
		}
		if pa.Expression.Kind != shimast.KindCallExpression {
			return false
		}
		current = pa.Expression.AsCallExpression()
	}
}

// rebuildChainOnto mutates `root` (the outermost chain call) so its chain
// becomes: base, then the trailing links in order.
func rebuildChainOnto(root *shimast.CallExpression, base *shimast.Node, trailing []chainLink) {
	current := base
	for _, link := range trailing {
		current = shimast.NewSynthesizedCall(
			shimast.NewSynthesizedPropertyAccess(current, link.name),
			link.args,
		)
	}
	// Replace root's content with the rebuilt chain (structural mutation on
	// the outermost node keeps the parent pointers intact).
	rebuilt := current.AsCallExpression()
	root.Expression = rebuilt.Expression
	root.Arguments = rebuilt.Arguments
}

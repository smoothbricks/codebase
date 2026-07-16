// ttsc transform plugin for LMAO — Go port of spec 01o (smoo/lmao!n/transformer).
//
// Implements the structural (TypeChecker-free) transformations:
//
//	§1/§2  span() line injection + monomorphic spanN rewrite (heuristic Op detection)
//	§5     defineModule() metadata injection (git_sha, package_name, package_file)
//	§6     .line(N) injection on log/ok/err chains
//	§7     task('name', fn) line injection
//
// NOT yet ported (staged, requires the tsgo Checker via driver shims):
//
//	§3     destructured-context rewriting (shipped in the TS transformer;
//	       needs identifier-binding analysis parity before porting)
//	§4     tag-chain inlining with schema specialization (enum indices,
//	       eager/lazy null-bitmap elision). The checker-free fallback of §4
//	       is deliberately NOT ported either: emitting direct buffer writes
//	       without the schema risks divergence from the TS inliner's output;
//	       run the classic transformer for tag inlining until the Checker
//	       port lands.
//
// Column-name contract (spec 01e): any future §4 port must write
// library-local (unprefixed) column names; prefix/mapColumns remapping is
// cold-path-only via RemappedBufferView and must never appear in emitted
// hot-path writes.
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"

	shimast "github.com/microsoft/typescript-go/shim/ast"
	shimchecker "github.com/microsoft/typescript-go/shim/checker"
	shimprinter "github.com/microsoft/typescript-go/shim/printer"
	shimscanner "github.com/microsoft/typescript-go/shim/scanner"
	"github.com/samchon/ttsc/packages/ttsc/driver"
)

const pluginName = "@smoothbricks/lmao-ttsc"
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

// lmaoPluginTransform builds the emit-phase transformer. Running inside
// tsgo's per-file emit chain (the AST-integration model, same as typia) is
// what makes synthesized pos:-1 nodes safe: the checker's per-file emit-time
// passes (grammar checks, MarkLinkedReferences) run BEFORE the script
// transformers, so nothing type-checks the mutated tree. Checker queries the
// transform itself makes (tag-chain detection) happen in a collect phase over
// the still-untouched file before any node is spliced.
func lmaoPluginTransform(prog *driver.Program, cwd string) driver.PluginTransform {
	return func(_ *shimprinter.EmitContext, sf *shimast.SourceFile) *shimast.SourceFile {
		if sf == nil || sf.IsDeclarationFile {
			return sf
		}
		t := &fileTransformer{
			file: sf, cwd: cwd, checker: prog.Checker,
			processed:      map[*shimast.CallExpression]bool{},
			opSpans:        map[*shimast.CallExpression]bool{},
			logTemplateIDs: map[*shimast.CallExpression]uint16{},
		}
		hintRewrites := t.collectOptimizations(sf.AsNode())
		tagInlines, logInlines, resultInlines := t.collectTagInlines(sf.AsNode())
		applyHintRewrites(hintRewrites)
		t.applyTagInlines(tagInlines)
		t.applyLogInlines(logInlines)
		t.applyResultInlines(resultInlines)
		t.walk(sf.AsNode())
		// Wire Parent on synthesized nodes (only where nil); printer passes
		// walk parents. See shim/ast/parent.go.
		shimast.SetParentInChildrenUnset(sf.AsNode())
		return sf
	}
}

func runTransform(args []string) int {
	cwd, tsconfig := readFlags(args)
	prog, _, err := driver.LoadProgram(cwd, tsconfig, driver.LoadProgramOptions{})
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: %v\n", pluginName, err)
		return 2
	}
	defer prog.Close()

	transform := lmaoPluginTransform(prog, cwd)
	out := transformResult{TypeScript: map[string]string{}}
	for _, file := range prog.SourceFiles() {
		if file == nil || file.IsDeclarationFile {
			continue
		}
		ec := shimprinter.NewEmitContext()
		result := file
		if next := transform(ec, result); next != nil {
			result = next
		}
		printer := shimprinter.NewPrinter(shimprinter.PrinterOptions{}, shimprinter.PrintHandlers{}, ec)
		out.TypeScript[outputKey(cwd, file.FileName())] = shimprinter.EmitSourceFile(printer, result)
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

	emitDiags, err := prog.EmitWithPluginTransformers(
		[]driver.PluginTransform{lmaoPluginTransform(prog, cwd)}, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: emit failed: %v\n", pluginName, err)
		return 3
	}
	if len(emitDiags) > 0 {
		for _, d := range emitDiags {
			fmt.Fprintln(os.Stderr, d.String())
		}
		return 2
	}
	return 0
}

// ---------------------------------------------------------------------------
// Node synthesis helpers (shared factory; synthesized nodes carry pos -1)
// ---------------------------------------------------------------------------

var factory = shimast.NewNodeFactory(shimast.NodeFactoryHooks{})

// Factory-built nodes keep their default undefined (-1) text range: that is
// what makes the printer emit .Text instead of a source span. The tsgo
// CHECKER, however, panics on pos -1 (checkGrammarNumericLiteral →
// GetTextOfNodeFromSourceText slices source text) — so the invariant here is
// ORDERING, not positions: every Checker query happens in a detection pass
// over the untouched parse tree, and synthesized nodes are only spliced in
// afterwards, where nothing type-checks them again.

func ident(text string) *shimast.Node {
	return factory.NewIdentifier(text)
}

func str(text string) *shimast.Node {
	return factory.NewStringLiteral(text, shimast.TokenFlagsNone)
}

func num(n int) *shimast.Node {
	return factory.NewNumericLiteral(strconv.Itoa(n), shimast.TokenFlagsNone)
}

func propAccess(expr *shimast.Node, name string) *shimast.Node {
	return factory.NewPropertyAccessExpression(expr, nil, ident(name), shimast.NodeFlagsNone)
}

func callExpr(expr *shimast.Node, args []*shimast.Node) *shimast.Node {
	return factory.NewCallExpression(expr, nil, nil, factory.NewNodeList(args), shimast.NodeFlagsNone)
}

// ---------------------------------------------------------------------------
// Transformations
// ---------------------------------------------------------------------------

var logMethods = map[string]bool{"info": true, "debug": true, "warn": true, "error": true, "trace": true}
var resultMethods = map[string]bool{"ok": true, "err": true}

type fileTransformer struct {
	file *shimast.SourceFile
	cwd  string
	// seenDefineModule guards the one-module-per-file invariant (spec 01o §5).
	seenDefineModule bool
	// processed marks call nodes produced by a chain rewrite so the walker
	// does not re-match them (the Go analog of the TS transformer's
	// processedCalls WeakSet). Without it, the cloned inner call re-matches
	// on descent and the rewrite regresses infinitely.
	processed map[*shimast.CallExpression]bool
	// opSpans is populated with Checker-proved, stable-expression Op calls in
	// the untouched-tree collect phase. The mutation walk never queries types.
	opSpans map[*shimast.CallExpression]bool
	// logTemplateIDs is populated by checker/provenance-gated per-Op analysis
	// and consumed only after all checker-backed collection has completed.
	logTemplateIDs map[*shimast.CallExpression]uint16
	// checker is the program's pinned type checker (nil-safe: tag-chain
	// inlining is skipped entirely without it).
	checker *shimchecker.Checker
}

func (t *fileTransformer) walk(node *shimast.Node) {
	if node == nil {
		return
	}
	if node.Kind == shimast.KindCallExpression {
		call := node.AsCallExpression()
		switch {
		case t.tryDefineModuleMetadata(call):
		case t.processed[call]:
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

// lineOf returns the 1-based line of a node's trivia-skipped start position
// (parity with the TS transformer's getStart()-based getLineNumber).
func (t *fileTransformer) lineOf(node *shimast.Node) int {
	pos := shimscanner.SkipTrivia(t.file.Text(), node.Pos())
	return shimscanner.GetECMALineOfPosition(t.file, pos) + 1
}

// calleeNames returns the receiver and method name for `recv.name(...)`,
// or (nil, text) for a bare `name(...)`.
func calleeNames(call *shimast.CallExpression) (recv *shimast.Node, name string) {
	expr := call.Expression
	if expr.Kind == shimast.KindPropertyAccessExpression {
		pa := expr.AsPropertyAccessExpression()
		return pa.Expression, shimast.NodeText(pa.Name())
	}
	if expr.Kind == shimast.KindIdentifier {
		return nil, shimast.NodeText(expr)
	}
	return nil, ""
}

// --- §5 defineModule metadata ------------------------------------------------

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
		if prop.Kind == shimast.KindPropertyAssignment && prop.Name() != nil &&
			shimast.NodeText(prop.Name()) == "metadata" {
			return false // already has metadata — leave alone
		}
	}

	gitSha := gitLastCommit(t.file.FileName(), t.cwd)
	pkgName, pkgFile := nearestPackage(t.file.FileName())

	metaProps := []*shimast.Node{
		factory.NewPropertyAssignment(nil, ident("git_sha"), nil, nil, str(gitSha)),
		factory.NewPropertyAssignment(nil, ident("package_name"), nil, nil, str(pkgName)),
		factory.NewPropertyAssignment(nil, ident("package_file"), nil, nil, str(pkgFile)),
	}
	metaObj := factory.NewObjectLiteralExpression(factory.NewNodeList(metaProps), true)
	metadata := factory.NewPropertyAssignment(nil, ident("metadata"), nil, nil, metaObj)

	obj.Properties = factory.NewNodeList(append([]*shimast.Node{metadata}, obj.Properties.Nodes...))
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

// --- §1/§2 span rewrite --------------------------------------------------------

func (t *fileTransformer) trySpanRewrite(call *shimast.CallExpression) bool {
	recv, name := calleeNames(call)
	if name != "span" || recv == nil || len(call.Arguments.Nodes) < 2 || len(call.Arguments.Nodes) > 10 {
		return false
	}
	args := call.Arguments.Nodes
	nameArg, opOrFn := args[0], args[1]
	rest := args[2:]

	if recv.Kind != shimast.KindIdentifier && recv.Kind != shimast.KindThisKeyword {
		return false
	}
	isPlainFunction := opOrFn.Kind == shimast.KindArrowFunction || opOrFn.Kind == shimast.KindFunctionExpression
	if !isPlainFunction && !t.opSpans[call] {
		return false
	}

	line := t.lineOf(call.AsNode())
	methodName := fmt.Sprintf("span%d", len(rest))
	childCtx := callExpr(propAccess(ident("Object"), "create"), []*shimast.Node{recv})
	var bufferClass, remappedView, opMetadata, fn, runtimeHint *shimast.Node
	if isPlainFunction {
		bufferClass = propAccess(propAccess(recv, "_buffer"), "constructor")
		remappedView = ident("undefined")
		opMetadata = propAccess(propAccess(recv, "_buffer"), "_opMetadata")
		fn = opOrFn
		runtimeHint = num(0)
	} else {
		bufferClass = propAccess(opOrFn, "SpanBufferClass")
		remappedView = propAccess(opOrFn, "remappedViewClass")
		opMetadata = propAccess(opOrFn, "metadata")
		fn = propAccess(opOrFn, "fn")
		runtimeHint = propAccess(opOrFn, "runtimeHint")
	}
	newArgs := append([]*shimast.Node{
		num(line), nameArg, childCtx, bufferClass, remappedView, opMetadata, fn,
	}, rest...)
	newArgs = append(newArgs, runtimeHint)

	call.Expression = propAccess(recv, methodName)
	call.Arguments = factory.NewNodeList(newArgs)
	return true
}

// --- §7 task line ---------------------------------------------------------------

func (t *fileTransformer) tryTaskLine(call *shimast.CallExpression) bool {
	_, name := calleeNames(call)
	if name != "task" || len(call.Arguments.Nodes) != 2 {
		return false
	}
	if call.Arguments.Nodes[0].Kind != shimast.KindStringLiteral {
		return false
	}
	call.Arguments = factory.NewNodeList(append(
		append([]*shimast.Node{}, call.Arguments.Nodes...),
		num(t.lineOf(call.AsNode())),
	))
	return true
}

// --- §6 log / result .line(N) -----------------------------------------------------

// isLogReceiver checks the receiver of a log method is a `.log` property access.
func isLogReceiver(recv *shimast.Node) bool {
	return recv != nil &&
		recv.Kind == shimast.KindPropertyAccessExpression &&
		shimast.NodeText(recv.AsPropertyAccessExpression().Name()) == "log"
}

// tryChainLine inserts `.line(N)` right after the matched method at the root
// of a fluent chain, preserving trailing calls; no-op when `.line` is present.
func (t *fileTransformer) tryChainLine(call *shimast.CallExpression, methods map[string]bool, receiverOK func(*shimast.Node) bool) bool {
	if t.processed[call] {
		return false
	}
	// Only fire at the TOP of a fluent chain: a call that is itself the
	// receiver of an enclosing method call belongs to a larger chain whose
	// top the walker handles (or already handled). Injecting mid-chain
	// duplicates .line() (the TS transformer prevents this with its
	// processedCalls sweep over allCallsInChain).
	if parent := call.AsNode().Parent; parent != nil && parent.Kind == shimast.KindPropertyAccessExpression {
		if gp := parent.Parent; gp != nil && gp.Kind == shimast.KindCallExpression &&
			gp.AsCallExpression().Expression == parent {
			return false
		}
	}
	target, trailing := findChainTarget(call, methods, receiverOK)
	templateID := uint16(0)
	if receiverOK != nil {
		if templateTarget, templateTrailing, id := findTemplateLogInChain(call, t.logTemplateIDs); templateTarget != nil {
			target, trailing, templateID = templateTarget, templateTrailing, id
		}
	}
	if target == nil {
		return false
	}
	hasLine := chainHasLine(call)
	if hasLine && templateID == 0 {
		return false
	}
	line := t.lineOf(target.AsNode())

	// Build a fresh inner call, substituting the private template entry point
	// when this exact call was assigned an Op-local ID during collection.
	innerExpression := target.Expression
	innerArguments := target.Arguments
	if templateID != 0 {
		pa := target.Expression.AsPropertyAccessExpression()
		innerExpression = propAccess(pa.Expression, "_"+shimast.NodeText(pa.Name())+"Template")
		innerArguments = factory.NewNodeList([]*shimast.Node{num(int(templateID))})
	}
	inner := factory.NewCallExpression(
		innerExpression, nil, target.TypeArguments, innerArguments, shimast.NodeFlagsNone,
	)
	t.processed[inner.AsCallExpression()] = true
	t.processed[call] = true
	var rebuilt *shimast.Node
	if hasLine {
		rebuilt = inner
	} else {
		rebuilt = callExpr(propAccess(inner, "line"), []*shimast.Node{num(line)})
	}
	for _, link := range trailing {
		rebuilt = callExpr(propAccess(rebuilt, link.name), factory.NewNodeList(link.args).Nodes)
	}
	rc := rebuilt.AsCallExpression()
	call.Expression = rc.Expression
	call.Arguments = rc.Arguments
	return true
}

// findTemplateLogInChain locates a previously analyzed literal call while
// preserving the same receiver-to-outer fluent link order as findChainTarget.
func findTemplateLogInChain(call *shimast.CallExpression, ids map[*shimast.CallExpression]uint16) (*shimast.CallExpression, []chainLink, uint16) {
	var trailing []chainLink
	current := call
	for {
		if id := ids[current]; id != 0 {
			return current, trailing, id
		}
		expr := current.Expression
		if expr.Kind != shimast.KindPropertyAccessExpression {
			return nil, nil, 0
		}
		pa := expr.AsPropertyAccessExpression()
		trailing = append([]chainLink{{name: shimast.NodeText(pa.Name()), args: current.Arguments.Nodes}}, trailing...)
		if pa.Expression.Kind != shimast.KindCallExpression {
			return nil, nil, 0
		}
		current = pa.Expression.AsCallExpression()
	}
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
		name := shimast.NodeText(pa.Name())
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
		if shimast.NodeText(pa.Name()) == "line" {
			return true
		}
		if pa.Expression.Kind != shimast.KindCallExpression {
			return false
		}
		current = pa.Expression.AsCallExpression()
	}
}

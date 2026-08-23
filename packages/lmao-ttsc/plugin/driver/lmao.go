// Package lmao is the ttsc transform plugin for LMAO — the Go implementation of
// spec 01o (smoo/lmao!n/transformer).
//
// The package is a library, not a command: init() registers the transform with
// driver.RegisterPlugin so ttsc can link it into a compiler host alongside
// sibling transform plugins. Two independent executable transform hosts cannot
// share one emit pass, so an executable of its own would make single-pass
// composition impossible. `../host` is the standalone sidecar that drives the
// same registration through ttsc's utility host.
//
// Implemented transformations:
//
//	§1/§2  span() line injection + monomorphic spanN rewrite (checker-proved Op detection)
//	§4     tag-chain inlining with schema specialization (enum indices,
//	       eager/lazy null-bitmap elision)
//	§5     defineModule() metadata injection (git_sha, package_name, package_file)
//	§6     .line(N) injection on log/ok/err chains, literal messages encoded as
//	       vocabulary IDs
//	§7     task('name', fn) line injection
//
// NOT yet ported:
//
//	§3     destructured-context rewriting (shipped in the TS transformer;
//	       needs identifier-binding analysis parity before porting)
//
// Column-name contract (spec 01e): emitted hot-path writes always use
// library-local (unprefixed) column names; prefix/mapColumns remapping is
// cold-path-only via RemappedBufferView and must never appear in emitted
// hot-path writes.
package lmao

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

// PluginName is the manifest name ttsc addresses this plugin by; it is also the
// prefix of every diagnostic the transform reports.
const PluginName = "@smoothbricks/lmao-ttsc"

// PluginVersion is reported by the standalone sidecar's `version` command.
const PluginVersion = "0.1.6"

func init() {
	driver.RegisterPlugin(plugin{})
}

// plugin implements driver.ProgramPlugin for @smoothbricks/lmao-ttsc.
//
// ProgramPlugin and not EmitTransformPlugin: the source-to-source lane that
// @ttsc/unplugin drives (utility.RunTransform) applies linked ProgramPlugins
// and then prints the parse tree, and never assembles the emit-phase transform
// chain. Rewriting the tree in ApplyProgram is the only hook every lane
// honours — the text lane, EmitAllRaw, and EmitWithPluginTransformers alike.
type plugin struct{}

// ApplyProgram rewrites every non-declaration source file of the program in
// place, after the whole-program vocabulary catalog has been resolved.
func (plugin) ApplyProgram(prog *driver.Program, ctx driver.PluginContext) error {
	if err := validateEntryConfig(ctx.Entry.Config); err != nil {
		return err
	}
	transform, err := lmaoPluginTransform(prog, compilerOptions{cwd: ctx.Cwd, tsconfig: ctx.Tsconfig})
	if err != nil {
		return err
	}
	for _, file := range prog.SourceFiles() {
		// The emit context is unused: every synthesized binding resolves to a
		// plain identifier (generatedBindingName) precisely so the rewrite
		// survives printing by a host that owns a different context.
		transform(nil, file)
	}
	return nil
}

// validateEntryConfig rejects any tsconfig plugin-entry key the transform does
// not own. "transform" and "enabled" are the transport keys ttsc puts on every
// entry. The lowest-sorting offender is named so the diagnostic is stable
// across Go's randomized map iteration.
func validateEntryConfig(config map[string]any) error {
	unsupported := ""
	for option := range config {
		if option == "transform" || option == "enabled" {
			continue
		}
		if unsupported == "" || option < unsupported {
			unsupported = option
		}
	}
	if unsupported != "" {
		return fmt.Errorf("LMAO1010 %s unsupported configuration option %q", PluginName, unsupported)
	}
	return nil
}

type compilerOptions struct{ cwd, tsconfig string }

type collectedFile struct {
	transformer         *fileTransformer
	hintRewrites        []hintRewrite
	tagInlines          []tagInline
	logInlines          []logInline
	resultInlines       []resultInline
	registrationEntries []vocabularyCatalogEntry
}
type programCompilation struct {
	files map[*shimast.SourceFile]*collectedFile
}

func collectProgramCompilation(prog *driver.Program, options compilerOptions) (*programCompilation, error) {
	collector := newProgramVocabularyCollector()
	compilation := &programCompilation{files: map[*shimast.SourceFile]*collectedFile{}}
	for _, sf := range prog.SourceFiles() {
		if sf == nil || sf.IsDeclarationFile {
			continue
		}
		t := &fileTransformer{file: sf, cwd: options.cwd, checker: prog.Checker, processed: map[*shimast.CallExpression]bool{}, opSpans: map[*shimast.CallExpression]bool{}, fnSpans: map[*shimast.CallExpression]bool{}, physicalLogCalls: map[*shimast.CallExpression]callMessagePhysicalLayout{}, currentLogLocalIDs: map[*shimast.CallExpression]uint16{}, vocabulary: collector}
		t.collectOptimizations(sf.AsNode(), false)
		t.destructures = t.collectDestructuredRewrites(sf.AsNode())
		compilation.files[sf] = &collectedFile{transformer: t}
	}
	if err := collector.diagnosticError(); err != nil {
		return nil, err
	}
	catalog, err := buildVocabularyCatalog(collector)
	if err != nil {
		return nil, err
	}
	staticLogs, staticSpans := resolveVocabularyIDs(catalog, collector)
	for sf, collected := range compilation.files {
		collected.transformer.staticLogIDs = staticLogs
		collected.transformer.staticSpanNameIDs = staticSpans
		collected.transformer.vocabularySize = len(catalog.Entries)
		collected.hintRewrites = collected.transformer.collectOptimizations(sf.AsNode(), true)
		collected.registrationEntries = catalogEntriesForFile(catalog, collector.fileKeys[sf.FileName()])
		collected.transformer.vocabularyOrdinals = vocabularyOrdinals(collected.registrationEntries)
		collected.tagInlines, collected.logInlines, collected.resultInlines = collected.transformer.collectTagInlines(sf.AsNode())
	}
	return compilation, nil
}

func lmaoPluginTransform(prog *driver.Program, options compilerOptions) (driver.PluginTransform, error) {
	compilation, err := collectProgramCompilation(prog, options)
	if err != nil {
		return nil, err
	}
	return func(_ *shimprinter.EmitContext, sf *shimast.SourceFile) *shimast.SourceFile {
		if sf == nil || sf.IsDeclarationFile {
			return sf
		}
		collected := compilation.files[sf]
		if collected == nil {
			panic("source file missing from finalized LMAO compilation")
		}
		t := collected.transformer
		binding, registration := vocabularyRegistrationStatements(sf, collected.registrationEntries)
		t.vocabularyBinding = binding
		t.applyHintRewrites(collected.hintRewrites)
		if t.spanBufferAotUsed {
			registration = append(spanBufferAotRuntimeStatements(), registration...)
		}
		t.applyTagInlines(collected.tagInlines)
		t.applyLogInlines(collected.logInlines)
		t.applyResultInlines(collected.resultInlines)
		// After the inline passes, which address statements by index into the
		// original lists, and before the walk that lowers the re-rooted calls.
		t.applyDestructuredRewrites()
		t.walk(sf.AsNode())
		prependVocabularyRegistration(sf, registration)
		shimast.SetParentInChildrenUnset(sf.AsNode())
		return sf
	}, nil
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
	// fnSpans is the same proof for an inline arrow/function-expression op
	// position: the receiver is a proven LMAO context, so the closure inherits
	// that receiver's callsite plan. A closure body is KNOWN at the call site,
	// which is why it needs no Op provenance proof of its own.
	fnSpans map[*shimast.CallExpression]bool
	// destructures holds the §3 function literals whose destructured context
	// parameter was proven safe to re-root onto `__ctx`.
	destructures       []destructuredRewrite
	staticLogIDs       map[*shimast.CallExpression]globalVocabularyID
	staticSpanNameIDs  map[*shimast.CallExpression]globalVocabularyID
	physicalLogCalls   map[*shimast.CallExpression]callMessagePhysicalLayout
	currentLogLocalIDs map[*shimast.CallExpression]uint16
	directTagStates    map[*shimast.Node]bool
	vocabulary         *programVocabularyCollector
	vocabularyOrdinals map[globalVocabularyID]int
	vocabularySize     int
	vocabularyBinding  *shimast.Node
	spanBufferAotUsed  bool
	checker            *shimchecker.Checker
}

func (t *fileTransformer) staticVocabularyOperand(id globalVocabularyID) *shimast.Node {
	if id == 0 || t.vocabularyBinding == nil {
		panic("static vocabulary operand requested before registration")
	}
	ordinal, exists := t.vocabularyOrdinals[id]
	if !exists {
		panic("static vocabulary id missing from source-file fragment")
	}
	return factory.NewElementAccessExpression(t.vocabularyBinding, nil, num(ordinal), shimast.NodeFlagsNone)
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
		// Panic, not os.Exit: this package is linked into a compiler host it
		// shares with sibling transform plugins, and an exit would kill that
		// host — and every sibling's emit — without a word on any channel the
		// host can report through.
		panic(fmt.Sprintf("%s: invariant violation: %s contains multiple defineModule() declarations",
			PluginName, t.file.FileName()))
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
	if isPlainFunction {
		if !t.fnSpans[call] {
			return false
		}
	} else if !t.opSpans[call] {
		return false
	}

	line := t.lineOf(call.AsNode())
	methodName := fmt.Sprintf("span%d", len(rest))
	nameOperand := nameArg
	if staticID := t.staticSpanNameIDs[call]; staticID != 0 {
		nameOperand = t.staticVocabularyOperand(staticID)
	}
	// An Op carries its own frozen callsite plan and function; an inline closure
	// has neither, so it inherits the RECEIVER's plan and is itself the
	// function. That is exactly what the runtime dispatcher does for a closure
	// target — resolveSpanTarget falls back to `self._physicalLayoutPlan`
	// (packages/lmao/src/lib/spanContext.ts:163-185, 936-940) — so the lowered
	// call and the variadic call construct the same child span. The plan node is
	// shared between the newCtx0 receiver and the plan argument, matching the Op
	// path: both positions print one pure property load.
	plan := propAccess(opOrFn, "callsitePlan")
	fnOperand := propAccess(opOrFn, "fn")
	if isPlainFunction {
		plan = propAccess(recv, "_physicalLayoutPlan")
		fnOperand = opOrFn
	}
	childCtx := callExpr(propAccess(plan, "newCtx0"), []*shimast.Node{recv})
	newArgs := []*shimast.Node{
		num(line),
		nameOperand,
		childCtx,
		plan,
		fnOperand,
	}
	newArgs = append(newArgs, rest...)

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
	templateID := globalVocabularyID(0)
	if receiverOK != nil {
		if templateTarget, templateTrailing, id := findTemplateLogInChain(call, t.staticLogIDs); templateTarget != nil {
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

	// Build a fresh inner call, substituting the private registered entry point.
	innerExpression := target.Expression
	innerArguments := target.Arguments
	if templateID != 0 {
		pa := target.Expression.AsPropertyAccessExpression()
		innerExpression = propAccess(pa.Expression, "_"+shimast.NodeText(pa.Name())+"Template")
		innerArguments = factory.NewNodeList([]*shimast.Node{t.staticVocabularyOperand(templateID)})
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
func findTemplateLogInChain(call *shimast.CallExpression, ids map[*shimast.CallExpression]globalVocabularyID) (*shimast.CallExpression, []chainLink, globalVocabularyID) {
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

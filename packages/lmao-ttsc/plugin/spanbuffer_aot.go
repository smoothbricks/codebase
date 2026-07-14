package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	shimast "github.com/microsoft/typescript-go/shim/ast"
	shimcore "github.com/microsoft/typescript-go/shim/core"
	shimparser "github.com/microsoft/typescript-go/shim/parser"
)

const spanBufferAotSpecifier = "@smoothbricks/lmao/span-buffer/aot/v1"
const spanBufferAotBindingName = "$$_lmaoSpanBufferAot"

type spanBufferAotArtifact struct {
	className             string
	classExpression       *shimast.Node
	messageLayoutFamily   string
	messagePhysicalLayout string
	eagerColumns          []string
}

func spanBufferAotRuntimeStatements() []*shimast.Node {
	symbol := callExpr(propAccess(ident("Symbol"), "for"), []*shimast.Node{str(spanBufferAotSpecifier)})
	runtime := factory.NewElementAccessExpression(ident("globalThis"), nil, symbol, shimast.NodeFlagsNone)
	binding := ident(spanBufferAotBindingName)
	declaration := constDecl(binding, runtime)
	typeofRuntime := factory.NewTypeOfExpression(binding)
	unavailable := factory.NewBinaryExpression(
		nil,
		typeofRuntime,
		nil,
		factory.NewToken(shimast.KindExclamationEqualsEqualsToken),
		str("object"),
	)
	throwUnavailable := factory.NewThrowStatement(factory.NewNewExpression(
		ident("Error"),
		nil,
		factory.NewNodeList([]*shimast.Node{str("LMAO_SPAN_BUFFER_AOT_ABI_UNAVAILABLE")}),
	))
	guard := factory.NewIfStatement(
		unavailable,
		factory.NewBlock(factory.NewNodeList([]*shimast.Node{throwUnavailable}), false),
		nil,
	)
	return []*shimast.Node{
		factory.NewImportDeclaration(nil, nil, str(spanBufferAotSpecifier), nil),
		declaration,
		guard,
	}
}

func spanBufferMessageLayout(runtimeHint uint32) string {
	if runtimeHint&runtimeHintAnalyzed == 0 {
		return "mixed"
	}
	switch runtimeHint & runtimeHintMessageMixed {
	case runtimeHintMessageStatic:
		return "static-only"
	case runtimeHintMessageDynamic:
		return "dynamic-only"
	default:
		return "mixed"
	}
}

func spanBufferMessagePhysicalLayout(runtimeHint uint32) string {
	if runtimeHint&runtimeHintAnalyzed == 0 {
		return "current"
	}
	switch runtimeHint & runtimeHintMessagePhysicalMask {
	case runtimeHintMessagePhysicalPacked:
		return "packed"
	case runtimeHintMessagePhysicalSpecialized:
		return "specialized"
	default:
		return "current"
	}
}

func spanBufferArtifactName(
	fields []namedSchemaField,
	family string,
	physical string,
	eagerColumns []string,
) string {
	var signature strings.Builder
	signature.WriteString(family)
	signature.WriteByte('|')
	signature.WriteString(physical)
	for _, eager := range eagerColumns {
		signature.WriteString("|e:")
		signature.WriteString(eager)
	}
	for _, named := range fields {
		signature.WriteString("|f:")
		signature.WriteString(named.name)
		signature.WriteByte(':')
		signature.WriteString(fmt.Sprint(named.field.storage))
		if named.field.eager {
			signature.WriteString(":eager")
		}
		for _, value := range named.field.enumValues {
			signature.WriteByte(':')
			signature.WriteString(value)
		}
	}
	digest := sha256.Sum256([]byte(signature.String()))
	return "$$LmaoSpanBuffer_" + hex.EncodeToString(digest[:8])
}

func parseSpanBufferClassExpression(source string) *shimast.Node {
	sourceFile := shimparser.ParseSourceFile(
		shimast.SourceFileParseOptions{FileName: "/lmao-span-buffer-aot.ts"},
		"("+source+");",
		shimcore.ScriptKindTS,
	)
	if sourceFile == nil || len(sourceFile.Statements.Nodes) != 1 {
		panic("failed to parse ttsc SpanBuffer class")
	}
	expression := sourceFile.Statements.Nodes[0].AsExpressionStatement().Expression
	if expression.Kind != shimast.KindParenthesizedExpression {
		panic("ttsc SpanBuffer class parser returned an unexpected expression")
	}
	classExpression := expression.AsParenthesizedExpression().Expression
	if classExpression.Kind != shimast.KindClassExpression {
		panic("ttsc SpanBuffer artifact is not a class expression")
	}
	return factory.DeepCloneNode(classExpression)
}

func newSpanBufferAotArtifact(analysis opCompileAnalysis) *spanBufferAotArtifact {
	if analysis.runtimeHint&runtimeHintAnalyzed == 0 || len(analysis.spanBufferFields) == 0 {
		return nil
	}
	family := spanBufferMessageLayout(analysis.runtimeHint)
	physical := spanBufferMessagePhysicalLayout(analysis.runtimeHint)
	className := spanBufferArtifactName(analysis.spanBufferFields, family, physical, analysis.eagerColumns)
	source := generateSpanBufferClassSource(className, analysis.spanBufferFields, family, physical, analysis.eagerColumns)
	return &spanBufferAotArtifact{
		className:             className,
		classExpression:       parseSpanBufferClassExpression(source),
		messageLayoutFamily:   family,
		messagePhysicalLayout: physical,
		eagerColumns:          analysis.eagerColumns,
	}
}

func (artifact *spanBufferAotArtifact) materializeCall(schemaExpression *shimast.Node) *shimast.Node {
	eager := make([]*shimast.Node, len(artifact.eagerColumns))
	for index, name := range artifact.eagerColumns {
		eager[index] = str(name)
	}
	factoryFunction := factory.NewArrowFunction(
		nil,
		nil,
		factory.NewNodeList([]*shimast.Node{}),
		nil,
		nil,
		factory.NewToken(shimast.KindEqualsGreaterThanToken),
		artifact.classExpression,
	)
	return callExpr(
		propAccess(ident(spanBufferAotBindingName), "materializeCompiledSpanBufferClass"),
		[]*shimast.Node{
			schemaExpression,
			str(artifact.messageLayoutFamily),
			str(artifact.messagePhysicalLayout),
			factory.NewArrayLiteralExpression(factory.NewNodeList(eager), false),
			factoryFunction,
		},
	)
}

func (artifact *spanBufferAotArtifact) materializerFunction() *shimast.Node {
	schema := ident("$$schema")
	parameter := factory.NewParameterDeclaration(nil, nil, schema, nil, nil, nil)
	return factory.NewArrowFunction(
		nil,
		nil,
		factory.NewNodeList([]*shimast.Node{parameter}),
		nil,
		nil,
		factory.NewToken(shimast.KindEqualsGreaterThanToken),
		artifact.materializeCall(schema),
	)
}

func fieldStorage(field schemaField) (constructor string, bytes int, bitPacked bool) {
	switch field.storage {
	case storageBoolean:
		return "Uint8Array", 1, true
	case storageNumber:
		return "Float64Array", 8, false
	case storageBigUint64:
		return "BigUint64Array", 8, false
	case storageEnum:
		switch {
		case len(field.enumValues) <= 256:
			return "Uint8Array", 1, false
		case len(field.enumValues) <= 65536:
			return "Uint16Array", 2, false
		default:
			return "Uint32Array", 4, false
		}
	default:
		return "Array", 0, false
	}
}

func writeColumnAllocation(body *strings.Builder, fieldName string, field schemaField, capacity string, nullable bool) {
	constructor, bytes, bitPacked := fieldStorage(field)
	if !nullable {
		switch {
		case constructor == "Array":
			fmt.Fprintf(body, "this._%s_values=new Array(%s);", fieldName, capacity)
		case bitPacked:
			fmt.Fprintf(body, "this._%s_values=new Uint8Array((%s+7)>>>3);", fieldName, capacity)
		default:
			fmt.Fprintf(body, "this._%s_values=new %s(%s);", fieldName, constructor, capacity)
		}
		return
	}
	if bitPacked {
		fmt.Fprintf(body, "{const n=(%s+7)>>>3;const b=new ArrayBuffer(n+n);this._%s_nulls=new Uint8Array(b,0,n);this._%s_values=new Uint8Array(b,n,n);}", capacity, fieldName, fieldName)
		return
	}
	if constructor == "Array" {
		fmt.Fprintf(body, "{const n=(%s+7)>>>3;this._%s_nulls=new Uint8Array(n);this._%s_values=new Array(%s);}", capacity, fieldName, fieldName, capacity)
		return
	}
	shift := 0
	for (1 << shift) < bytes {
		shift++
	}
	fmt.Fprintf(body, "{const n=(%s+7)>>>3;const o=((n+%d)>>>%d)<<%d;const b=new ArrayBuffer(o+%s*%d);this._%s_nulls=new Uint8Array(b,0,n);this._%s_values=new %s(b,o,%s);}", capacity, bytes-1, shift, shift, capacity, bytes, fieldName, fieldName, constructor, capacity)
}

func writeLazyAllocation(body *strings.Builder, fieldName string, field schemaField) {
	constructor, bytes, bitPacked := fieldStorage(field)
	if bitPacked {
		fmt.Fprintf(body, "const c=this._alignedCapacity,n=(c+7)>>>3,b=new ArrayBuffer(n+n);v=this._%s_nulls=new Uint8Array(b,0,n);this._%s_values=new Uint8Array(b,n,n);", fieldName, fieldName)
		return
	}
	if constructor == "Array" {
		fmt.Fprintf(body, "const c=this._alignedCapacity,n=(c+7)>>>3;v=this._%s_nulls=new Uint8Array(n);this._%s_values=new Array(c);", fieldName, fieldName)
		return
	}
	shift := 0
	for (1 << shift) < bytes {
		shift++
	}
	fmt.Fprintf(body, "const c=this._alignedCapacity,n=(c+7)>>>3,o=((n+%d)>>>%d)<<%d,b=new ArrayBuffer(o+c*%d);v=this._%s_nulls=new Uint8Array(b,0,n);this._%s_values=new %s(b,o,c);", bytes-1, shift, shift, bytes, fieldName, fieldName, constructor)
}

func writeValueAssignment(body *strings.Builder, access string, field schemaField) {
	if field.storage == storageBoolean {
		fmt.Fprintf(body, "const i=pos>>>3,m=1<<(pos&7);if(val){%s_values[i]|=m;}else{%s_values[i]&=~m;}", access, access)
		return
	}
	fmt.Fprintf(body, "%s_values[pos]=val;", access)
}

func eagerDefault(field schemaField) string {
	switch field.storage {
	case storageBoolean:
		return "false"
	case storageBigUint64:
		return "0n"
	case storageArray:
		return "''"
	default:
		return "0"
	}
}

func writeColumnMembers(body *strings.Builder, named namedSchemaField, preallocated bool) {
	name := named.name
	field := named.field
	if field.eager {
		fmt.Fprintf(body, "get %s_values(){return this._%s_values;}get %s(){return this._%s_values;}", name, name, name, name)
		fmt.Fprintf(body, "%s(pos,val){if(val==null)val=%s;", name, eagerDefault(field))
		writeValueAssignment(body, "this._"+name, field)
		body.WriteString("return this;}")
		return
	}
	if preallocated {
		fmt.Fprintf(body, "get %s_nulls(){return this._%s_nulls;}get %s_values(){return this._%s_values;}get %s(){return this._%s_values;}", name, name, name, name, name, name)
		fmt.Fprintf(body, "%s(pos,val){if(val==null){this._%s_nulls[pos>>>3]&=~(1<<(pos&7));}else{", name, name)
		writeValueAssignment(body, "this._"+name, field)
		fmt.Fprintf(body, "this._%s_nulls[pos>>>3]|=1<<(pos&7);}return this;}", name)
		return
	}
	fmt.Fprintf(body, "get %s_nulls(){let v=this._%s_nulls;if(v===undefined){", name, name)
	writeLazyAllocation(body, name, field)
	body.WriteString("}return v;}")
	fmt.Fprintf(body, "get %s_values(){if(this._%s_values===undefined)this.%s_nulls;return this._%s_values;}get %s(){return this.%s_values;}", name, name, name, name, name, name)
	fmt.Fprintf(body, "%s(pos,val){if(val==null){this.%s_nulls[pos>>>3]&=~(1<<(pos&7));}else{", name, name)
	writeValueAssignment(body, "this."+name, field)
	fmt.Fprintf(body, "this.%s_nulls[pos>>>3]|=1<<(pos&7);}return this;}", name)
}

func generateSpanBufferClassSource(
	className string,
	fields []namedSchemaField,
	family string,
	physical string,
	eagerColumns []string,
) string {
	preallocated := map[string]bool{}
	for _, name := range eagerColumns {
		preallocated[name] = true
	}
	var body strings.Builder
	fmt.Fprintf(&body, "class %s{constructor(requestedCapacity,stats,parent,isChained,callsiteMetadata,opMetadata,traceRoot,vocabularyGeneration){", className)
	body.WriteString("const a=" + spanBufferAotBindingName + ";if(typeof globalThis.globalSpanCounter==='undefined')globalThis.globalSpanCounter=0;const spanId=++globalThis.globalSpanCounter;const threadId=isChained?parent.thread_id:a.getThreadId();")
	switch {
	case physical == "packed":
		body.WriteString("const rowHeaderOffset=requestedCapacity*8,systemSize=(requestedCapacity*12+7)&~7;let systemBuffer,identityView;")
	case family == "dynamic-only":
		body.WriteString("const systemSize=(requestedCapacity*9+7)&~7;let systemBuffer,identityView;")
	case physical == "current":
		body.WriteString("const messageIdOffset=(requestedCapacity*9+1)&~1,systemSize=(messageIdOffset+requestedCapacity*2+7)&~7;let systemBuffer,identityView;")
	default:
		body.WriteString("const logHeaderOffset=(requestedCapacity*9+3)&~3,systemSize=(logHeaderOffset+requestedCapacity*4+7)&~7;let systemBuffer,identityView;")
	}
	body.WriteString("if(isChained){systemBuffer=new ArrayBuffer(systemSize);identityView=parent._identity;}else if(parent){const identitySize=12;systemBuffer=new ArrayBuffer(systemSize+identitySize);identityView=new Uint8Array(systemBuffer,systemSize,identitySize);a.copyThreadIdTo(identityView,0);identityView[8]=spanId;identityView[9]=spanId>>>8;identityView[10]=spanId>>>16;identityView[11]=spanId>>>24;}else{const traceIdBytes=traceRoot._traceIdBytes,identitySize=13+traceIdBytes.length;systemBuffer=new ArrayBuffer(systemSize+identitySize);identityView=new Uint8Array(systemBuffer,systemSize,identitySize);a.copyThreadIdTo(identityView,0);identityView[8]=spanId;identityView[9]=spanId>>>8;identityView[10]=spanId>>>16;identityView[11]=spanId>>>24;identityView[12]=traceIdBytes.length;identityView.set(traceIdBytes,13);}")
	body.WriteString("const timestampView=new BigInt64Array(systemBuffer,0,requestedCapacity);")
	if physical == "packed" {
		body.WriteString("const rowHeaderView=new Uint32Array(systemBuffer,rowHeaderOffset,requestedCapacity);")
	} else {
		body.WriteString("const entryTypeView=new Uint8Array(systemBuffer,requestedCapacity*8,requestedCapacity);")
		if family != "dynamic-only" {
			if physical == "current" {
				body.WriteString("const messageIdView=new Uint16Array(systemBuffer,messageIdOffset,requestedCapacity);")
			} else {
				body.WriteString("const logHeaderView=new Uint32Array(systemBuffer,logHeaderOffset,requestedCapacity);")
			}
		}
	}
	body.WriteString("this._writeIndex=0;this._capacity=requestedCapacity;this._overflow=undefined;this.timestamp=timestampView;")
	if physical == "packed" {
		body.WriteString("this._rowHeaders=rowHeaderView;")
	} else {
		body.WriteString("this.entry_type=entryTypeView;")
		if family != "dynamic-only" {
			if physical == "current" {
				body.WriteString("this._messageIds=messageIdView;")
			} else {
				body.WriteString("this._logHeaders=logHeaderView;")
			}
		}
	}
	body.WriteString("this._vocabularyGeneration=vocabularyGeneration;")
	if family == "static-only" {
		body.WriteString("this._spanName=undefined;this._terminalMessage=undefined;")
	} else if family == "dynamic-only" {
		body.WriteString("this._spanName=undefined;")
	}
	body.WriteString("this._nodeIndex=4294967295;this._topologyGeneration=0;this._parent=isChained?parent._parent:parent;this._traceRoot=traceRoot;this._scopeValues=parent?parent._scopeValues:a.EMPTY_SCOPE;this._threadId=threadId;this._identity=identityView;this._system=systemBuffer;this._callsiteMetadata=callsiteMetadata;this._opMetadata=opMetadata;this._statsSealed=false;this._statsReservedRows=isChained?0:2;")
	if family != "static-only" {
		body.WriteString("this.message_values=new Array(requestedCapacity);")
	}
	body.WriteString("const alignedCapacity=Math.ceil(requestedCapacity/8)*8;this._alignedCapacity=alignedCapacity;this._capacity=requestedCapacity;this._overflow=undefined;")
	for _, named := range fields {
		if named.name == "message" {
			continue
		}
		switch {
		case named.field.eager:
			writeColumnAllocation(&body, named.name, named.field, "alignedCapacity", false)
		case preallocated[named.name]:
			writeColumnAllocation(&body, named.name, named.field, "alignedCapacity", true)
		default:
			fmt.Fprintf(&body, "this._%s_nulls=undefined;this._%s_values=undefined;", named.name, named.name)
		}
		if named.field.storage == storageEnum {
			encoded, _ := json.Marshal(named.field.enumValues)
			fmt.Fprintf(&body, "this.%s_enumValues=%s;", named.name, encoded)
		}
	}
	body.WriteString("}")
	body.WriteString("getColumnIfAllocated(n){return this[`_${n}_values`];}getNullsIfAllocated(n){return this[`_${n}_nulls`];}")
	for _, named := range fields {
		if named.name != "message" {
			writeColumnMembers(&body, named, preallocated[named.name])
		}
	}
	body.WriteString("get span_id(){const b=this._identity;return b?(b[8]|(b[9]<<8)|(b[10]<<16)|(b[11]<<24))>>>0:0;}get thread_id(){return this._threadId;}get trace_id(){return this._traceRoot.trace_id;}get _spanStartTime(){return this.timestamp[0];}get _lastLoggedTime(){const chain=[];let current=this;while(current){chain.push(current);current=current._overflow;}for(let i=chain.length-1;i>=0;i--){const buffer=chain[i];for(let row=buffer._writeIndex-1;row>=0;row--){const ts=buffer.timestamp[row];if(ts!==0n)return ts;}}return null;}get _hasParent(){return this._parent!==undefined;}get parent_span_id(){return this._parent?.span_id??0;}get parent_thread_id(){return this._parent?.thread_id??0n;}isParentOf(other){return this===other._parent;}isChildOf(other){return this._parent===other;}copyThreadIdTo(dest,offset){const source=this._identity;if(source){dest[offset]=source[0];dest[offset+1]=source[1];dest[offset+2]=source[2];dest[offset+3]=source[3];dest[offset+4]=source[4];dest[offset+5]=source[5];dest[offset+6]=source[6];dest[offset+7]=source[7];}else dest.fill(0,offset,offset+8);}copyParentThreadIdTo(dest,offset){if(this._parent)this._parent.copyThreadIdTo(dest,offset);else dest.fill(0,offset,offset+8);}get _logSchema(){return this.constructor.schema;}get _messageLayoutFamily(){return this.constructor.messageLayoutFamily;}get _messagePhysicalLayout(){return this.constructor.messagePhysicalLayout;}get _columns(){return this.constructor.schema._columns;}get _stats(){return this.constructor.stats;}_sealStats(){if(this._statsSealed)return;const completedRows=this._writeIndex-this._statsReservedRows;if(completedRows>0)this.constructor.stats.totalWrites+=completedRows;this._statsSealed=true;}_sealStatsChain(){let current=this;while(current){current._sealStats();current=current._overflow;}}getOrCreateOverflow(){if(this._overflow)return this._overflow;this._sealStats();const tracer=this._traceRoot.tracer;tracer.onStatsWillResetFor(this);" + spanBufferAotBindingName + ".checkCapacityTuning(this.constructor.stats);return tracer.bufferStrategy.createOverflowBuffer(this);}")
	if family == "static-only" {
		body.WriteString("message(pos,val){if(pos===0)this._spanName=val;else if(pos===1)this._terminalMessage=val;else throw new RangeError('Static-only buffers only accept raw system messages at rows 0 and 1');return this;}")
	} else {
		body.WriteString("message(pos,val){this.message_values[pos]=val;return this;}")
	}
	body.WriteString("}")
	return body.String()
}

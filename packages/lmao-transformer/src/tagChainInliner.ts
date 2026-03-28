/**
 * Tag Chain Inliner - Transform ctx.tag fluent chains into direct buffer array writes
 *
 * Transforms: ctx.tag.operation('SELECT').userId('u123')
 * Into: Direct TypedArray writes for maximum performance
 *
 * This module provides compile-time optimization of tag attribute writes by:
 * 1. Extracting schema information from TypeScript types
 * 2. Generating optimized code for lazy vs eager columns
 * 3. Computing enum indices at compile time when possible
 * 4. Generating switch IIFEs for runtime enum values
 * 5. Unrolling .with() bulk setters for individual writes
 */

import ts from 'typescript';

// ============================================================================
// Schema Types & Extraction
// ============================================================================

/**
 * Information about a schema field extracted from the TypeScript type.
 */
export interface SchemaFieldInfo {
  /** The LMAO schema type */
  type: 'enum' | 'category' | 'text' | 'number' | 'boolean';
  /** For enum types: the known values in sorted order */
  enumValues?: readonly string[];
  /** Whether this column is eager (no null bitmap) */
  eager: boolean;
}

/**
 * Type names that indicate a LMAO TagWriter type.
 */
const LMAO_TAG_WRITER_TYPE_NAMES = new Set(['TagWriter', 'GeneratedTagWriter']);

/**
 * Check if a type is a TagWriter type.
 */
function isTagWriterType(type: ts.Type, typeChecker: ts.TypeChecker): boolean {
  const typeName = typeChecker.typeToString(type);

  for (const name of LMAO_TAG_WRITER_TYPE_NAMES) {
    if (typeName === name || typeName.startsWith(`${name}<`)) {
      return true;
    }
  }

  const symbol = type.getSymbol();
  if (symbol) {
    const symbolName = symbol.getName();
    if (LMAO_TAG_WRITER_TYPE_NAMES.has(symbolName)) {
      return true;
    }
  }

  return false;
}

/**
 * Extract schema field information from a TagWriter type using TypeChecker.
 *
 * Inspects the type's properties to determine:
 * - The schema type (enum/category/text/number/boolean)
 * - Enum values for enum types (from union of string literals)
 * - Whether the field is eager (from _eager property)
 *
 * @param tagType - The TypeScript type of ctx.tag
 * @param typeChecker - TypeScript type checker instance
 * @returns Map of field name to schema info, or null if extraction fails
 */
export function extractSchemaFromTagType(
  tagType: ts.Type,
  typeChecker: ts.TypeChecker,
): Map<string, SchemaFieldInfo> | null {
  const schemaInfo = new Map<string, SchemaFieldInfo>();

  for (const prop of tagType.getProperties()) {
    const propName = prop.getName();
    // Skip internal properties and known non-field methods
    if (propName === 'with' || propName.startsWith('_')) {
      continue;
    }

    const propType = typeChecker.getTypeOfSymbol(prop);
    const callSigs = propType.getCallSignatures();
    if (callSigs.length === 0) continue;

    const params = callSigs[0].getParameters();
    if (params.length === 0) continue;

    const paramType = typeChecker.getTypeOfSymbol(params[0]);

    // Check for _eager property to determine eagerness
    const eager = checkEagerProperty(propType, typeChecker);

    // Check for enum type (union of string literals)
    if (paramType.isUnion()) {
      const stringLiteralTypes: string[] = [];
      let allStringLiterals = true;

      for (const t of paramType.types) {
        if (t.isStringLiteral()) {
          stringLiteralTypes.push(t.value);
        } else {
          allStringLiterals = false;
        }
      }

      if (allStringLiterals && stringLiteralTypes.length > 0) {
        // Sort enum values for consistent indexing (matches runtime behavior)
        const sortedValues = [...stringLiteralTypes].sort();
        schemaInfo.set(propName, {
          type: 'enum',
          enumValues: sortedValues,
          eager,
        });
        continue;
      }
    }

    // Check primitive types
    const typeString = typeChecker.typeToString(paramType);
    if (typeString === 'string') {
      // Without more type info, default to category (most common)
      schemaInfo.set(propName, { type: 'category', eager });
    } else if (typeString === 'number') {
      schemaInfo.set(propName, { type: 'number', eager });
    } else if (typeString === 'boolean') {
      schemaInfo.set(propName, { type: 'boolean', eager });
    }
  }

  return schemaInfo.size > 0 ? schemaInfo : null;
}

/**
 * Check if a type has an _eager property with literal type true.
 */
function checkEagerProperty(type: ts.Type, typeChecker: ts.TypeChecker): boolean {
  const eagerSymbol = type.getProperty('_eager');
  if (!eagerSymbol) return false;

  const eagerType = typeChecker.getTypeOfSymbol(eagerSymbol);
  // Check if it's a literal type with value true
  // TypeScript represents boolean literals as intrinsic types
  // We check if the type string representation is 'true'
  const typeString = typeChecker.typeToString(eagerType);
  return typeString === 'true';
}

// ============================================================================
// Chain Detection
// ============================================================================

/**
 * Information about a single tag method call in a chain.
 */
interface TagCallInfo {
  /** The method name (e.g., 'operation', 'userId') */
  methodName: string;
  /** The argument expression */
  argument: ts.Expression;
  /** Reference to the original call expression */
  originalCall: ts.CallExpression;
}

/**
 * Information about a .with() bulk setter call.
 */
interface WithCallInfo {
  /** The object literal argument to with() */
  properties: ts.ObjectLiteralExpression;
  /** Reference to the original call expression */
  originalCall: ts.CallExpression;
}

/**
 * Information about a complete tag chain.
 */
export interface TagChainInfo {
  /** The context expression (e.g., `ctx`) */
  ctxExpression: ts.Expression;
  /** All tag method calls in execution order */
  tagCalls: TagCallInfo[];
  /** All .with() calls in the chain */
  withCalls: WithCallInfo[];
  /** All original call expressions in the chain */
  allOriginalCalls: ts.CallExpression[];
  /** Schema information extracted from the type (if available) */
  schemaInfo: Map<string, SchemaFieldInfo> | null;
}

/**
 * Find the root of a tag chain and collect all method calls.
 *
 * Given: ctx.tag.operation('SELECT').userId('123').with({...})
 * Returns: { ctxExpression: ctx, tagCalls: [...], withCalls: [...] }
 *
 * @param node - The call expression to analyze
 * @param typeChecker - Optional TypeChecker for type-aware analysis
 * @returns TagChainInfo if this is a valid tag chain, null otherwise
 */
export function findTagChainRoot(
  node: ts.CallExpression,
  typeChecker: ts.TypeChecker | undefined,
): TagChainInfo | null {
  const tagCalls: TagCallInfo[] = [];
  const withCalls: WithCallInfo[] = [];
  const allOriginalCalls: ts.CallExpression[] = [];
  let current: ts.Expression = node;

  // Walk up the chain collecting method calls
  while (ts.isCallExpression(current)) {
    const callExpr = current;
    const expr = callExpr.expression;

    if (!ts.isPropertyAccessExpression(expr)) {
      return null;
    }

    const methodName = expr.name.text;
    allOriginalCalls.push(callExpr);

    // Handle .with() bulk setter
    if (methodName === 'with') {
      if (callExpr.arguments.length !== 1) {
        return null;
      }
      const arg = callExpr.arguments[0];
      if (ts.isObjectLiteralExpression(arg)) {
        withCalls.push({
          properties: arg,
          originalCall: callExpr,
        });
      } else {
        // .with() with non-literal object - can't inline
        return null;
      }
      current = expr.expression;
      continue;
    }

    // Regular tag method call - must have single argument
    if (callExpr.arguments.length !== 1) {
      return null;
    }

    tagCalls.push({
      methodName,
      argument: callExpr.arguments[0],
      originalCall: callExpr,
    });

    current = expr.expression;
  }

  // Check if we've reached ctx.tag
  if (!ts.isPropertyAccessExpression(current)) {
    return null;
  }

  if (current.name.text !== 'tag') {
    return null;
  }

  const ctxExpression = current.expression;

  // Verify the type if we have a type checker
  let schemaInfo: Map<string, SchemaFieldInfo> | null = null;
  if (typeChecker) {
    const tagType = typeChecker.getTypeAtLocation(current);
    if (!isTagWriterType(tagType, typeChecker)) {
      return null;
    }
    schemaInfo = extractSchemaFromTagType(tagType, typeChecker);
  }

  return {
    ctxExpression,
    tagCalls: tagCalls.reverse(), // Reverse to get execution order
    withCalls: withCalls.reverse(),
    allOriginalCalls,
    schemaInfo,
  };
}

// ============================================================================
// Code Generation
// ============================================================================

/**
 * Counter for generating unique variable names.
 * Reset per transformation to avoid conflicts.
 */
let varCounter = 0;

/**
 * Clear comments and source position from generated nodes to prevent comment inheritance from source code.
 *
 * When creating new Statement nodes via factory.createExpressionStatement(),
 * they inherit comments from nearby source locations and may have positional
 * information that causes them to be associated with source comments during
 * printing. This function recursively clears comments from the node and all its children.
 *
 * @param node - The node to clear comments and position from
 */
function clearComments(node: ts.Node): void {
  // Clear synthetic leading comments
  ts.setSyntheticLeadingComments(node, undefined);

  // Clear synthetic trailing comments
  ts.setSyntheticTrailingComments(node, undefined);

  // Clear source map range to prevent source comment inheritance
  ts.setSourceMapRange(node, undefined);

  // Clear positional information to prevent source comment inheritance.
  // Use the public helper so we reset the text range without type-erasing casts.
  ts.setTextRange(node, { pos: -1, end: -1 });

  // Recursively clear comments from all child nodes
  ts.forEachChild(node, clearComments);
}

/**
 * Reset the variable counter. Call at the start of each transformation.
 */
function resetVarCounter(): void {
  varCounter = 0;
}

/**
 * Generate a unique variable name.
 */
function generateVarName(): string {
  return `$$v${varCounter++}`;
}

/**
 * Check if an expression is a literal that can be evaluated at compile time.
 */
function isCompileTimeLiteral(expr: ts.Expression): boolean {
  return (
    ts.isStringLiteral(expr) ||
    ts.isNumericLiteral(expr) ||
    expr.kind === ts.SyntaxKind.TrueKeyword ||
    expr.kind === ts.SyntaxKind.FalseKeyword
  );
}

/**
 * Get enum index for a value using sorted order (matching runtime behavior).
 * Returns null if value is not in the enum.
 */
function getEnumIndex(value: string, enumValues: readonly string[]): number | null {
  const index = enumValues.indexOf(value);
  return index >= 0 ? index : null;
}

/**
 * Generate a switch IIFE for runtime enum value mapping.
 *
 * Output: (($$v) => { switch($$v) { case 'A': return 0; case 'B': return 1; default: return 0; } })(valueExpr)
 */
function generateEnumSwitchIIFE(
  factory: ts.NodeFactory,
  valueExpr: ts.Expression,
  enumValues: readonly string[],
): ts.Expression {
  // Build switch cases
  const cases: ts.CaseOrDefaultClause[] = [];

  for (let i = 0; i < enumValues.length; i++) {
    cases.push(
      factory.createCaseClause(factory.createStringLiteral(enumValues[i]), [
        factory.createReturnStatement(factory.createNumericLiteral(i)),
      ]),
    );
  }

  // Default case returns 0
  cases.push(factory.createDefaultClause([factory.createReturnStatement(factory.createNumericLiteral(0))]));

  // Create the switch statement
  const switchParam = factory.createIdentifier('$$v');
  const switchStmt = factory.createSwitchStatement(switchParam, factory.createCaseBlock(cases));

  // Create arrow function: ($$v) => { switch($$v) { ... } }
  const arrowFn = factory.createArrowFunction(
    undefined,
    undefined,
    [factory.createParameterDeclaration(undefined, undefined, switchParam)],
    undefined,
    factory.createToken(ts.SyntaxKind.EqualsGreaterThanToken),
    factory.createBlock([switchStmt], true),
  );

  // Create IIFE: (($$v) => {...})(valueExpr)
  return factory.createCallExpression(factory.createParenthesizedExpression(arrowFn), undefined, [valueExpr]);
}

/**
 * Generate statements for writing a single tag value.
 *
 * @param factory - TypeScript node factory
 * @param bufferExpr - Expression for accessing the buffer (e.g., ctx._buffer)
 * @param fieldName - The column/field name
 * @param argument - The value expression
 * @param fieldInfo - Schema information for this field (if available)
 * @param statements - Array to append generated statements to
 */
function generateFieldWriteStatements(
  factory: ts.NodeFactory,
  bufferExpr: ts.Expression,
  fieldName: string,
  argument: ts.Expression,
  fieldInfo: SchemaFieldInfo | undefined,
  statements: ts.Statement[],
): void {
  const isLiteral = isCompileTimeLiteral(argument);
  const isEager = fieldInfo?.eager ?? false;

  // Column accessors
  const nullsAccess = factory.createElementAccessExpression(
    factory.createPropertyAccessExpression(bufferExpr, factory.createIdentifier(`${fieldName}_nulls`)),
    factory.createNumericLiteral(0),
  );
  const valuesAccess = factory.createElementAccessExpression(
    factory.createPropertyAccessExpression(bufferExpr, factory.createIdentifier(`${fieldName}_values`)),
    factory.createNumericLiteral(0),
  );

  // Handle different field types
  if (fieldInfo?.type === 'boolean') {
    generateBooleanWrite(factory, nullsAccess, valuesAccess, argument, isLiteral, isEager, statements);
    return;
  }

  if (fieldInfo?.type === 'enum' && fieldInfo.enumValues) {
    generateEnumWrite(
      factory,
      nullsAccess,
      valuesAccess,
      argument,
      fieldInfo.enumValues,
      isLiteral,
      isEager,
      statements,
    );
    return;
  }

  // Default: category/text/number - direct value write
  generateDirectWrite(factory, nullsAccess, valuesAccess, argument, isLiteral, isEager, statements);
}

/**
 * Generate write statements for boolean fields using bitmap operations.
 */
function generateBooleanWrite(
  factory: ts.NodeFactory,
  nullsAccess: ts.ElementAccessExpression,
  valuesAccess: ts.ElementAccessExpression,
  argument: ts.Expression,
  isLiteral: boolean,
  isEager: boolean,
  statements: ts.Statement[],
): void {
  if (isLiteral) {
    const isTrue = argument.kind === ts.SyntaxKind.TrueKeyword;

    // Set null bitmap (if not eager)
    if (!isEager) {
      // $$b.field_nulls[0] |= 1
      const stmt = factory.createExpressionStatement(
        factory.createBinaryExpression(nullsAccess, ts.SyntaxKind.BarEqualsToken, factory.createNumericLiteral(1)),
      );
      clearComments(stmt);
      statements.push(stmt);
    }

    // Set or clear value bit
    if (isTrue) {
      // $$b.field_values[0] |= 1
      const stmt = factory.createExpressionStatement(
        factory.createBinaryExpression(valuesAccess, ts.SyntaxKind.BarEqualsToken, factory.createNumericLiteral(1)),
      );
      clearComments(stmt);
      statements.push(stmt);
    } else {
      // $$b.field_values[0] &= ~1
      const stmt = factory.createExpressionStatement(
        factory.createBinaryExpression(
          valuesAccess,
          ts.SyntaxKind.AmpersandEqualsToken,
          factory.createPrefixUnaryExpression(ts.SyntaxKind.TildeToken, factory.createNumericLiteral(1)),
        ),
      );
      clearComments(stmt);
      statements.push(stmt);
    }
  } else {
    // Variable - wrap in null check
    const varName = generateVarName();
    const varIdent = factory.createIdentifier(varName);

    // const $$v0 = getValue();
    const varStmt = factory.createVariableStatement(
      undefined,
      factory.createVariableDeclarationList(
        [factory.createVariableDeclaration(varIdent, undefined, undefined, argument)],
        ts.NodeFlags.Const,
      ),
    );
    clearComments(varStmt);
    statements.push(varStmt);

    // Build the if body
    const ifBody: ts.Statement[] = [];

    // Set null bitmap (if not eager)
    if (!isEager) {
      const stmt = factory.createExpressionStatement(
        factory.createBinaryExpression(nullsAccess, ts.SyntaxKind.BarEqualsToken, factory.createNumericLiteral(1)),
      );
      clearComments(stmt);
      ifBody.push(stmt);
    }

    // Create the true and false statements
    const trueStmt = factory.createExpressionStatement(
      factory.createBinaryExpression(valuesAccess, ts.SyntaxKind.BarEqualsToken, factory.createNumericLiteral(1)),
    );
    clearComments(trueStmt);

    const falseStmt = factory.createExpressionStatement(
      factory.createBinaryExpression(
        valuesAccess,
        ts.SyntaxKind.AmpersandEqualsToken,
        factory.createPrefixUnaryExpression(ts.SyntaxKind.TildeToken, factory.createNumericLiteral(1)),
      ),
    );
    clearComments(falseStmt);

    // if ($$v0) { values |= 1 } else { values &= ~1 }
    const trueBlock = factory.createBlock([trueStmt]);
    clearComments(trueBlock);
    const falseBlock = factory.createBlock([falseStmt]);
    clearComments(falseBlock);
    const innerIfStmt = factory.createIfStatement(varIdent, trueBlock, falseBlock);
    clearComments(innerIfStmt);
    ifBody.push(innerIfStmt);

    // if ($$v0 != null) { ... }
    const ifBodyBlock = factory.createBlock(ifBody, true);
    clearComments(ifBodyBlock);
    const outerIfStmt = factory.createIfStatement(
      factory.createBinaryExpression(varIdent, ts.SyntaxKind.ExclamationEqualsToken, factory.createNull()),
      ifBodyBlock,
    );
    clearComments(outerIfStmt);
    statements.push(outerIfStmt);
  }
}

/**
 * Generate write statements for enum fields.
 */
function generateEnumWrite(
  factory: ts.NodeFactory,
  nullsAccess: ts.ElementAccessExpression,
  valuesAccess: ts.ElementAccessExpression,
  argument: ts.Expression,
  enumValues: readonly string[],
  isLiteral: boolean,
  isEager: boolean,
  statements: ts.Statement[],
): void {
  if (isLiteral && ts.isStringLiteral(argument)) {
    // Compute enum index at compile time
    const enumIndex = getEnumIndex(argument.text, enumValues);
    const indexValue = enumIndex ?? 0;

    // Set null bitmap (if not eager)
    if (!isEager) {
      const stmt = factory.createExpressionStatement(
        factory.createBinaryExpression(nullsAccess, ts.SyntaxKind.BarEqualsToken, factory.createNumericLiteral(1)),
      );
      clearComments(stmt);
      statements.push(stmt);
    }

    // $$b.field_values[0] = indexValue
    const stmt = factory.createExpressionStatement(
      factory.createBinaryExpression(valuesAccess, ts.SyntaxKind.EqualsToken, factory.createNumericLiteral(indexValue)),
    );
    clearComments(stmt);
    statements.push(stmt);
  } else {
    // Variable - generate switch IIFE
    // Set null bitmap (if not eager)
    if (!isEager) {
      const stmt = factory.createExpressionStatement(
        factory.createBinaryExpression(nullsAccess, ts.SyntaxKind.BarEqualsToken, factory.createNumericLiteral(1)),
      );
      clearComments(stmt);
      statements.push(stmt);
    }

    // $$b.field_values[0] = (($$v) => { switch($$v) {...} })(argument)
    const switchIIFE = generateEnumSwitchIIFE(factory, argument, enumValues);
    const stmt = factory.createExpressionStatement(
      factory.createBinaryExpression(valuesAccess, ts.SyntaxKind.EqualsToken, switchIIFE),
    );
    clearComments(stmt);
    statements.push(stmt);
  }
}

/**
 * Generate write statements for direct value types (category/text/number).
 */
function generateDirectWrite(
  factory: ts.NodeFactory,
  nullsAccess: ts.ElementAccessExpression,
  valuesAccess: ts.ElementAccessExpression,
  argument: ts.Expression,
  isLiteral: boolean,
  isEager: boolean,
  statements: ts.Statement[],
): void {
  if (isLiteral) {
    // Literal value - no null check needed
    if (!isEager) {
      // Set null bitmap
      const stmt = factory.createExpressionStatement(
        factory.createBinaryExpression(nullsAccess, ts.SyntaxKind.BarEqualsToken, factory.createNumericLiteral(1)),
      );
      clearComments(stmt);
      statements.push(stmt);
    }

    // Direct assignment
    const stmt = factory.createExpressionStatement(
      factory.createBinaryExpression(valuesAccess, ts.SyntaxKind.EqualsToken, argument),
    );
    clearComments(stmt);
    statements.push(stmt);
  } else {
    // Variable - wrap in null check
    const varName = generateVarName();
    const varIdent = factory.createIdentifier(varName);

    // const $$v0 = getValue();
    const varStmt = factory.createVariableStatement(
      undefined,
      factory.createVariableDeclarationList(
        [factory.createVariableDeclaration(varIdent, undefined, undefined, argument)],
        ts.NodeFlags.Const,
      ),
    );
    clearComments(varStmt);
    statements.push(varStmt);

    // Build if body
    const ifBody: ts.Statement[] = [];

    if (!isEager) {
      // Set null bitmap
      const stmt = factory.createExpressionStatement(
        factory.createBinaryExpression(nullsAccess, ts.SyntaxKind.BarEqualsToken, factory.createNumericLiteral(1)),
      );
      clearComments(stmt);
      ifBody.push(stmt);
    }

    // Assign value
    const stmt = factory.createExpressionStatement(
      factory.createBinaryExpression(valuesAccess, ts.SyntaxKind.EqualsToken, varIdent),
    );
    clearComments(stmt);
    ifBody.push(stmt);

    // if ($$v0 != null) { ... }
    const ifBodyBlock = factory.createBlock(ifBody, true);
    clearComments(ifBodyBlock);
    const ifStmt = factory.createIfStatement(
      factory.createBinaryExpression(varIdent, ts.SyntaxKind.ExclamationEqualsToken, factory.createNull()),
      ifBodyBlock,
    );
    clearComments(ifStmt);
    statements.push(ifStmt);
  }
}

/**
 * Unroll a .with() bulk setter into individual field writes.
 */
function unrollWithCall(
  factory: ts.NodeFactory,
  bufferExpr: ts.Expression,
  withCall: WithCallInfo,
  schemaInfo: Map<string, SchemaFieldInfo> | null,
  statements: ts.Statement[],
): void {
  for (const prop of withCall.properties.properties) {
    if (!ts.isPropertyAssignment(prop)) continue;

    // Get the property name
    let fieldName: string;
    if (ts.isIdentifier(prop.name)) {
      fieldName = prop.name.text;
    } else if (ts.isStringLiteral(prop.name)) {
      fieldName = prop.name.text;
    } else {
      continue; // Skip computed property names
    }

    const fieldInfo = schemaInfo?.get(fieldName);
    generateFieldWriteStatements(factory, bufferExpr, fieldName, prop.initializer, fieldInfo, statements);
  }
}

// ============================================================================
// Main Transformer Entry Point
// ============================================================================

/**
 * Try to transform a ctx.tag fluent chain into direct buffer writes.
 *
 * @param node - The call expression to potentially transform
 * @param sourceFile - The source file containing the node
 * @param factory - TypeScript node factory
 * @param processedCalls - WeakSet to track processed calls (prevents re-visiting)
 * @param typeChecker - Optional TypeChecker for type-aware transformation
 * @returns A block statement with inlined writes, or null if transformation not possible
 */
export function tryTransformTagChain(
  callExpr: ts.CallExpression,
  expressionStmt: ts.ExpressionStatement,
  _sourceFile: ts.SourceFile,
  factory: ts.NodeFactory,
  processedCalls: WeakSet<ts.CallExpression>,
  typeChecker: ts.TypeChecker | undefined,
): ts.Statement | null {
  // Find the tag chain root
  const chainInfo = findTagChainRoot(callExpr, typeChecker);
  if (!chainInfo) return null;

  // Must have at least one tag call or with call to transform
  if (chainInfo.tagCalls.length === 0 && chainInfo.withCalls.length === 0) {
    return null;
  }

  // Reset variable counter for this transformation
  resetVarCounter();

  // Mark all original calls as processed
  for (const call of chainInfo.allOriginalCalls) {
    processedCalls.add(call);
  }

  // Generate buffer expression: ctx._buffer
  const bufferExpr = factory.createPropertyAccessExpression(
    chainInfo.ctxExpression,
    factory.createIdentifier('_buffer'),
  );

  // Collect all generated statements
  const statements: ts.Statement[] = [];

  // Process regular tag calls
  for (const call of chainInfo.tagCalls) {
    const fieldInfo = chainInfo.schemaInfo?.get(call.methodName);
    generateFieldWriteStatements(factory, bufferExpr, call.methodName, call.argument, fieldInfo, statements);
  }

  // Process .with() calls (unroll each)
  for (const withCall of chainInfo.withCalls) {
    unrollWithCall(factory, bufferExpr, withCall, chainInfo.schemaInfo, statements);
  }

  // Clear comments from all statements to prevent inheritance/duplication
  for (const stmt of statements) {
    clearComments(stmt);
  }

  // Return a block statement containing all the writes
  const block = factory.createBlock(statements, true);

  // Extract and preserve the original leading comment from the ExpressionStatement
  const sourceFile = expressionStmt.getSourceFile();
  const sourceText = sourceFile.getFullText();

  // Use expressionStmt.pos (not getStart()) for proper comment range detection
  const commentRanges = ts.getLeadingCommentRanges(sourceText, expressionStmt.pos);

  if (commentRanges && commentRanges.length > 0) {
    // Get the last (closest) comment before the statement
    const lastComment = commentRanges[commentRanges.length - 1];

    // Only preserve single-line comments (// comments)
    if (lastComment.kind === ts.SyntaxKind.SingleLineCommentTrivia) {
      // Extract the comment text and clean it
      const commentText = sourceText.substring(lastComment.pos, lastComment.end);
      const commentContent = commentText.substring(2).trim(); // Remove '//' prefix

      // Add the cleaned comment as a synthetic leading comment on the block
      ts.addSyntheticLeadingComment(block, ts.SyntaxKind.SingleLineCommentTrivia, ` ${commentContent}`, true);
    }
  }

  return block;
}

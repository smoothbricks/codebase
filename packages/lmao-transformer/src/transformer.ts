import { execSync } from 'child_process';
import * as fs from 'fs';
import path from 'path';
import ts from 'typescript';

const LOG_METHODS = new Set(['info', 'debug', 'warn', 'error', 'trace']);
const RESULT_METHODS = new Set(['ok', 'err']);

/**
 * Type names that indicate a LMAO context type.
 * These are checked when a TypeChecker is available.
 */
const LMAO_CONTEXT_TYPE_NAMES = new Set(['TaskContext', 'SpanContext', 'ModuleContext', 'RequestContext']);

/**
 * Type names that indicate a LMAO SpanLogger type.
 * The `.log` property returns this type.
 */
const LMAO_SPAN_LOGGER_TYPE_NAMES = new Set(['SpanLogger', 'GeneratedSpanLogger']);

export interface LmaoTransformerOptions {
  /**
   * Optional TypeChecker for type-aware transformation.
   * When provided, only transforms calls on objects with LMAO context types.
   * When not provided, transforms based on structural patterns only.
   */
  typeChecker?: ts.TypeChecker;
  /**
   * Project root for computing relative file paths.
   * Defaults to the git repository root, or current working directory if not in a git repo.
   */
  projectRoot?: string;
}

/**
 * Get the git repository root directory.
 * Returns undefined if not in a git repository.
 */
function getGitRoot(): string | undefined {
  try {
    const result = execSync('git rev-parse --show-toplevel', {
      encoding: 'utf-8',
      stdio: ['pipe', 'pipe', 'pipe'],
    });
    return result.trim() || undefined;
  } catch {
    return undefined;
  }
}

// Cache the git root to avoid repeated execSync calls
let cachedGitRoot: string | undefined | null = null;

/**
 * Get the cached git root, computing it once on first access.
 */
function getCachedGitRoot(): string | undefined {
  if (cachedGitRoot === null) {
    cachedGitRoot = getGitRoot();
  }
  return cachedGitRoot;
}

/**
 * Get the last git commit SHA that modified the given file.
 * Returns 'unknown' if git is not available or the file has no history.
 */
function getLastGitCommit(filePath: string): string {
  try {
    const result = execSync(`git log -1 --format=%H -- "${filePath}"`, {
      encoding: 'utf-8',
      stdio: ['pipe', 'pipe', 'pipe'],
    });
    return result.trim() || 'unknown';
  } catch {
    return 'unknown';
  }
}

/**
 * Derive a PascalCase module name from a file path.
 * e.g., 'src/services/user-service.ts' -> 'UserService'
 */
function deriveModuleName(filePath: string): string {
  const fileName = path.basename(filePath, path.extname(filePath));
  // Convert kebab-case or snake_case to PascalCase
  return fileName
    .split(/[-_]/)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join('');
}

/**
 * Find the nearest package.json by walking up from a file path.
 * Returns { packageName, packageDir } or undefined if not found.
 */
function findNearestPackage(filePath: string): { packageName: string; packageDir: string } | undefined {
  let currentDir = path.dirname(filePath);
  const root = path.parse(currentDir).root;

  while (currentDir !== root) {
    const packageJsonPath = path.join(currentDir, 'package.json');
    try {
      const content = fs.readFileSync(packageJsonPath, 'utf-8');
      const pkg = JSON.parse(content);
      if (pkg.name) {
        return { packageName: pkg.name, packageDir: currentDir };
      }
    } catch {
      // package.json doesn't exist or is invalid, keep walking up
    }
    currentDir = path.dirname(currentDir);
  }
  return undefined;
}

/**
 * Derive the module identifier from a file path.
 * Format: @scope/package-name/path/to/file (without extension)
 *
 * Example:
 *   filePath: /project/packages/lmao-transformer/examples/demo-source.ts
 *   packageName: @smoothbricks/lmao-transformer
 *   packageDir: /project/packages/lmao-transformer
 *   Result: @smoothbricks/lmao-transformer/examples/demo-source
 */
function deriveModuleId(filePath: string): string {
  const packageInfo = findNearestPackage(filePath);
  if (!packageInfo) {
    // Fallback to just the filename without extension
    return path.basename(filePath, path.extname(filePath));
  }

  const { packageName, packageDir } = packageInfo;
  const relativePath = path.relative(packageDir, filePath);
  // Remove extension and normalize path separators
  const withoutExt = relativePath.replace(/\.[^.]+$/, '');
  const normalized = withoutExt.split(path.sep).join('/');

  return `${packageName}/${normalized}`;
}

/**
 * Check if the call expression is a createModuleContext() call.
 * Handles both direct calls and property access (e.g., lmao.createModuleContext()).
 */
function isCreateModuleContextCall(node: ts.CallExpression): boolean {
  const expr = node.expression;

  // Direct call: createModuleContext({...})
  if (ts.isIdentifier(expr) && expr.text === 'createModuleContext') {
    return true;
  }

  // Property access: lmao.createModuleContext({...})
  if (ts.isPropertyAccessExpression(expr) && expr.name.text === 'createModuleContext') {
    return true;
  }

  return false;
}

/**
 * Check if an object literal already has a moduleMetadata property.
 */
function hasModuleMetadataProperty(objectLiteral: ts.ObjectLiteralExpression): boolean {
  return objectLiteral.properties.some(
    (prop) => ts.isPropertyAssignment(prop) && ts.isIdentifier(prop.name) && prop.name.text === 'moduleMetadata',
  );
}

/**
 * Try to transform createModuleContext() to inject moduleMetadata.
 *
 * Before:
 *   createModuleContext({ tagAttributes: schema })
 *
 * After:
 *   createModuleContext({
 *     moduleMetadata: { gitSha: '...', filePath: '...', moduleName: '...' },
 *     tagAttributes: schema
 *   })
 */
function tryTransformCreateModuleContextCall(
  node: ts.CallExpression,
  sourceFile: ts.SourceFile,
  factory: ts.NodeFactory,
  projectRoot: string,
): ts.CallExpression | null {
  if (!isCreateModuleContextCall(node)) {
    return null;
  }

  // Must have at least one argument that is an object literal
  if (node.arguments.length === 0) {
    return null;
  }

  const firstArg = node.arguments[0];
  if (!ts.isObjectLiteralExpression(firstArg)) {
    return null;
  }

  // Don't transform if moduleMetadata already exists
  if (hasModuleMetadataProperty(firstArg)) {
    return null;
  }

  const absoluteFilePath = sourceFile.fileName;
  const moduleId = deriveModuleId(absoluteFilePath);
  const gitSha = getLastGitCommit(absoluteFilePath);
  const moduleName = deriveModuleName(absoluteFilePath);

  // Create the moduleMetadata object literal
  // filePath is the module identifier (package-name/path/to/file)
  const moduleMetadataObject = factory.createObjectLiteralExpression(
    [
      factory.createPropertyAssignment(factory.createIdentifier('gitSha'), factory.createStringLiteral(gitSha)),
      factory.createPropertyAssignment(factory.createIdentifier('filePath'), factory.createStringLiteral(moduleId)),
      factory.createPropertyAssignment(factory.createIdentifier('moduleName'), factory.createStringLiteral(moduleName)),
    ],
    true, // multiLine
  );

  // Create the moduleMetadata property assignment
  const moduleMetadataProperty = factory.createPropertyAssignment(
    factory.createIdentifier('moduleMetadata'),
    moduleMetadataObject,
  );

  // Create new object literal with moduleMetadata as first property
  const newObjectLiteral = factory.createObjectLiteralExpression(
    [moduleMetadataProperty, ...firstArg.properties],
    true, // multiLine
  );

  // Return updated call expression
  return factory.updateCallExpression(node, node.expression, node.typeArguments, [
    newObjectLiteral,
    ...node.arguments.slice(1),
  ]);
}

/**
 * Creates a LMAO transformer that injects line numbers into logging, span, and result calls.
 *
 * Transformations:
 * - ctx.span('name', fn) → ctx.span('name', fn, lineNumber)
 * - ctx.log.{info,debug,warn,error,trace}() → ctx.log.{method}().line(N)
 * - ctx.ok(value) → ctx.ok(value).line(N)
 * - ctx.err(code, error) → ctx.err(code, error).line(N)
 *
 * @param options - Optional configuration including TypeChecker for type-aware transformation
 */
export function createLmaoTransformer(options: LmaoTransformerOptions = {}): ts.TransformerFactory<ts.SourceFile> {
  const { typeChecker, projectRoot = getCachedGitRoot() ?? process.cwd() } = options;

  return (context: ts.TransformationContext): ts.Transformer<ts.SourceFile> => {
    const factory = context.factory;

    return (sourceFile: ts.SourceFile): ts.SourceFile => {
      // Track original nodes that are part of a chain we've already processed
      // This prevents re-transformation when we visit children of a transformed chain
      const processedCalls = new WeakSet<ts.CallExpression>();

      const visitor = (node: ts.Node): ts.Node => {
        // Only interested in call expressions
        if (!ts.isCallExpression(node)) {
          return ts.visitEachChild(node, visitor, context);
        }

        // Skip if already processed as part of a chain
        if (processedCalls.has(node)) {
          return ts.visitEachChild(node, visitor, context);
        }

        // Check for createModuleContext() pattern
        const moduleContextTransformed = tryTransformCreateModuleContextCall(node, sourceFile, factory, projectRoot);
        if (moduleContextTransformed) {
          return ts.visitEachChild(moduleContextTransformed, visitor, context);
        }

        // Check for ctx.span() pattern
        const spanTransformed = tryTransformSpanCall(node, sourceFile, factory, typeChecker);
        if (spanTransformed) {
          return ts.visitEachChild(spanTransformed, visitor, context);
        }

        // Check for task('name', fn) pattern (from module context)
        const taskTransformed = tryTransformTaskCall(node, sourceFile, factory);
        if (taskTransformed) {
          return ts.visitEachChild(taskTransformed, visitor, context);
        }

        // Check for ctx.log.{method}() pattern - only at the TOP of a chain
        const logTransformed = tryTransformLogChain(node, sourceFile, factory, processedCalls, typeChecker);
        if (logTransformed) {
          return ts.visitEachChild(logTransformed, visitor, context);
        }

        // Check for ctx.ok() or ctx.err() pattern - only at the TOP of a chain
        const resultTransformed = tryTransformResultChain(node, sourceFile, factory, processedCalls, typeChecker);
        if (resultTransformed) {
          return ts.visitEachChild(resultTransformed, visitor, context);
        }

        // Check for ctx.tag fluent chain pattern - inline to direct buffer writes
        const tagTransformed = tryTransformTagChain(node, factory, processedCalls, typeChecker);
        if (tagTransformed) {
          return ts.visitEachChild(tagTransformed, visitor, context);
        }

        return ts.visitEachChild(node, visitor, context);
      };

      const result = ts.visitNode(sourceFile, visitor);
      if (!result || !ts.isSourceFile(result)) {
        return sourceFile;
      }
      return result;
    };
  };
}

/**
 * Check if a type is a LMAO context type (TaskContext, SpanContext, etc.)
 */
function isLmaoContextType(type: ts.Type, typeChecker: ts.TypeChecker): boolean {
  const typeName = typeChecker.typeToString(type);

  if (LMAO_CONTEXT_TYPE_NAMES.has(typeName)) {
    return true;
  }

  for (const name of LMAO_CONTEXT_TYPE_NAMES) {
    if (typeName.startsWith(`${name}<`)) {
      return true;
    }
  }

  const baseTypes = type.getBaseTypes?.();
  if (baseTypes) {
    for (const baseType of baseTypes) {
      if (isLmaoContextType(baseType, typeChecker)) {
        return true;
      }
    }
  }

  const symbol = type.getSymbol();
  if (symbol) {
    const symbolName = symbol.getName();
    if (LMAO_CONTEXT_TYPE_NAMES.has(symbolName)) {
      return true;
    }
  }

  return false;
}

/**
 * Check if a type is a LMAO SpanLogger type
 */
function isLmaoSpanLoggerType(type: ts.Type, typeChecker: ts.TypeChecker): boolean {
  const typeName = typeChecker.typeToString(type);

  if (LMAO_SPAN_LOGGER_TYPE_NAMES.has(typeName)) {
    return true;
  }

  for (const name of LMAO_SPAN_LOGGER_TYPE_NAMES) {
    if (typeName.startsWith(`${name}<`)) {
      return true;
    }
  }

  const symbol = type.getSymbol();
  if (symbol) {
    const symbolName = symbol.getName();
    if (LMAO_SPAN_LOGGER_TYPE_NAMES.has(symbolName)) {
      return true;
    }
  }

  return false;
}

/**
 * Try to transform a ctx.span('name', fn) call to ctx.span('name', fn, lineNumber)
 */
function tryTransformSpanCall(
  node: ts.CallExpression,
  sourceFile: ts.SourceFile,
  factory: ts.NodeFactory,
  typeChecker: ts.TypeChecker | undefined,
): ts.CallExpression | null {
  const expr = node.expression;

  if (!ts.isPropertyAccessExpression(expr)) {
    return null;
  }

  if (expr.name.text !== 'span') {
    return null;
  }

  // Check we have exactly 2 arguments (name, fn) - not already transformed
  if (node.arguments.length !== 2) {
    return null;
  }

  // If we have a type checker, verify the receiver is a LMAO context type
  if (typeChecker) {
    const receiverType = typeChecker.getTypeAtLocation(expr.expression);
    if (!isLmaoContextType(receiverType, typeChecker)) {
      return null;
    }
  }

  const lineNumber = getLineNumber(node, sourceFile);

  return factory.updateCallExpression(node, node.expression, node.typeArguments, [
    ...node.arguments,
    factory.createNumericLiteral(lineNumber),
  ]);
}

/**
 * Try to transform a task('name', fn) call to task('name', fn, lineNumber).
 *
 * This handles both:
 * - module.task('name', fn) - property access pattern
 * - task('name', fn) - destructured pattern: const { task } = createModuleContext(...)
 *
 * The line number is the definition location of the task.
 */
function tryTransformTaskCall(
  node: ts.CallExpression,
  sourceFile: ts.SourceFile,
  factory: ts.NodeFactory,
): ts.CallExpression | null {
  const expr = node.expression;

  // Check for either:
  // 1. Property access: something.task(...)
  // 2. Direct identifier: task(...)
  let isTaskCall = false;

  if (ts.isPropertyAccessExpression(expr)) {
    isTaskCall = expr.name.text === 'task';
  } else if (ts.isIdentifier(expr)) {
    isTaskCall = expr.text === 'task';
  }

  if (!isTaskCall) {
    return null;
  }

  // Check we have exactly 2 arguments (name, fn) - not already transformed
  if (node.arguments.length !== 2) {
    return null;
  }

  // First argument should be a string literal (task name)
  const firstArg = node.arguments[0];
  if (!ts.isStringLiteral(firstArg)) {
    return null;
  }

  const lineNumber = getLineNumber(node, sourceFile);

  return factory.updateCallExpression(node, node.expression, node.typeArguments, [
    ...node.arguments,
    factory.createNumericLiteral(lineNumber),
  ]);
}

/**
 * Try to transform a log call chain.
 *
 * For example:
 *   ctx.log.info('msg') → ctx.log.info('msg').line(N)
 *   ctx.log.info('msg').userId('123') → ctx.log.info('msg').line(N).userId('123')
 */
function tryTransformLogChain(
  node: ts.CallExpression,
  sourceFile: ts.SourceFile,
  factory: ts.NodeFactory,
  processedCalls: WeakSet<ts.CallExpression>,
  typeChecker: ts.TypeChecker | undefined,
): ts.CallExpression | null {
  const logCallInfo = findLogCallInChain(node, typeChecker);
  if (!logCallInfo) {
    return null;
  }

  const { logCall, chainAfterLogCall, allCallsInChain } = logCallInfo;

  // Mark all calls in this chain as processed
  for (const call of allCallsInChain) {
    processedCalls.add(call);
  }

  // Check if .line() is already in the chain
  if (hasLineInChain(node)) {
    return null;
  }

  const lineNumber = getLineNumber(logCall, sourceFile);

  const lineCall = factory.createCallExpression(
    factory.createPropertyAccessExpression(logCall, factory.createIdentifier('line')),
    undefined,
    [factory.createNumericLiteral(lineNumber)],
  );

  if (chainAfterLogCall.length === 0) {
    return lineCall;
  }

  return rebuildChain(lineCall, chainAfterLogCall, factory);
}

/**
 * Try to transform a result call chain (ctx.ok() or ctx.err()).
 *
 * For example:
 *   ctx.ok(value) → ctx.ok(value).line(N)
 *   ctx.err('CODE', error) → ctx.err('CODE', error).line(N)
 *   ctx.ok(value).with({...}) → ctx.ok(value).line(N).with({...})
 */
function tryTransformResultChain(
  node: ts.CallExpression,
  sourceFile: ts.SourceFile,
  factory: ts.NodeFactory,
  processedCalls: WeakSet<ts.CallExpression>,
  typeChecker: ts.TypeChecker | undefined,
): ts.CallExpression | null {
  const resultCallInfo = findResultCallInChain(node, typeChecker);
  if (!resultCallInfo) {
    return null;
  }

  const { resultCall, chainAfterResultCall, allCallsInChain } = resultCallInfo;

  // Mark all calls in this chain as processed
  for (const call of allCallsInChain) {
    processedCalls.add(call);
  }

  // Check if .line() is already in the chain
  if (hasLineInChain(node)) {
    return null;
  }

  const lineNumber = getLineNumber(resultCall, sourceFile);

  const lineCall = factory.createCallExpression(
    factory.createPropertyAccessExpression(resultCall, factory.createIdentifier('line')),
    undefined,
    [factory.createNumericLiteral(lineNumber)],
  );

  if (chainAfterResultCall.length === 0) {
    return lineCall;
  }

  return rebuildChain(lineCall, chainAfterResultCall, factory);
}

interface ChainLink {
  methodName: string;
  typeArguments: ts.NodeArray<ts.TypeNode> | undefined;
  arguments: ts.NodeArray<ts.Expression>;
}

interface CallInChainResult {
  logCall: ts.CallExpression;
  chainAfterLogCall: ChainLink[];
  allCallsInChain: ts.CallExpression[];
}

interface ResultCallInChainResult {
  resultCall: ts.CallExpression;
  chainAfterResultCall: ChainLink[];
  allCallsInChain: ts.CallExpression[];
}

/**
 * Find the log method call in a chain
 */
function findLogCallInChain(
  node: ts.CallExpression,
  typeChecker: ts.TypeChecker | undefined,
): CallInChainResult | null {
  const chain: ChainLink[] = [];
  const allCalls: ts.CallExpression[] = [];
  let current: ts.Expression = node;

  while (ts.isCallExpression(current)) {
    const callExpr = current;
    allCalls.push(callExpr);
    const expr = callExpr.expression;

    if (ts.isPropertyAccessExpression(expr)) {
      const methodName = expr.name.text;

      if (LOG_METHODS.has(methodName) && isLogPropertyAccess(expr.expression, typeChecker)) {
        return {
          logCall: callExpr,
          chainAfterLogCall: chain.reverse(),
          allCallsInChain: allCalls,
        };
      }

      chain.push({
        methodName,
        typeArguments: callExpr.typeArguments,
        arguments: callExpr.arguments,
      });
      current = expr.expression;
    } else {
      break;
    }
  }

  return null;
}

/**
 * Find the result method call (ok/err) in a chain
 */
function findResultCallInChain(
  node: ts.CallExpression,
  typeChecker: ts.TypeChecker | undefined,
): ResultCallInChainResult | null {
  const chain: ChainLink[] = [];
  const allCalls: ts.CallExpression[] = [];
  let current: ts.Expression = node;

  while (ts.isCallExpression(current)) {
    const callExpr = current;
    allCalls.push(callExpr);
    const expr = callExpr.expression;

    if (ts.isPropertyAccessExpression(expr)) {
      const methodName = expr.name.text;

      if (RESULT_METHODS.has(methodName) && isContextExpression(expr.expression, typeChecker)) {
        return {
          resultCall: callExpr,
          chainAfterResultCall: chain.reverse(),
          allCallsInChain: allCalls,
        };
      }

      chain.push({
        methodName,
        typeArguments: callExpr.typeArguments,
        arguments: callExpr.arguments,
      });
      current = expr.expression;
    } else {
      break;
    }
  }

  return null;
}

/**
 * Check if expression is a property access ending in .log (e.g., ctx.log)
 */
function isLogPropertyAccess(expr: ts.Expression, typeChecker: ts.TypeChecker | undefined): boolean {
  if (!ts.isPropertyAccessExpression(expr)) {
    return false;
  }

  if (expr.name.text !== 'log') {
    return false;
  }

  if (typeChecker) {
    const logType = typeChecker.getTypeAtLocation(expr);
    if (!isLmaoSpanLoggerType(logType, typeChecker)) {
      return false;
    }
  }

  return true;
}

/**
 * Check if expression is a LMAO context (for ctx.ok/ctx.err)
 */
function isContextExpression(expr: ts.Expression, typeChecker: ts.TypeChecker | undefined): boolean {
  // Without type checker, we can't verify - accept any identifier
  if (!typeChecker) {
    return true;
  }

  const exprType = typeChecker.getTypeAtLocation(expr);
  return isLmaoContextType(exprType, typeChecker);
}

/**
 * Check if .line() is already called in the chain
 */
function hasLineInChain(node: ts.CallExpression): boolean {
  let current: ts.Expression = node;

  while (ts.isCallExpression(current)) {
    const expr = current.expression;
    if (ts.isPropertyAccessExpression(expr)) {
      if (expr.name.text === 'line') {
        return true;
      }
      current = expr.expression;
    } else {
      break;
    }
  }

  return false;
}

/**
 * Rebuild a method chain on top of a base expression
 */
function rebuildChain(base: ts.Expression, chain: ChainLink[], factory: ts.NodeFactory): ts.CallExpression {
  let current = base;

  for (const link of chain) {
    current = factory.createCallExpression(
      factory.createPropertyAccessExpression(current, factory.createIdentifier(link.methodName)),
      link.typeArguments,
      [...link.arguments],
    );
  }

  return current as ts.CallExpression;
}

/**
 * Get the 1-based line number for a node
 */
function getLineNumber(node: ts.Node, sourceFile: ts.SourceFile): number {
  const { line } = sourceFile.getLineAndCharacterOfPosition(node.getStart(sourceFile));
  return line + 1;
}

// ============================================================================
// Tag Chain Inlining
// ============================================================================

/**
 * Type names that indicate a LMAO TagWriter type.
 */
const LMAO_TAG_WRITER_TYPE_NAMES = new Set(['TagWriter', 'GeneratedTagWriter']);

/**
 * Information about a schema field extracted from the type.
 */
interface SchemaFieldInfo {
  type: 'enum' | 'category' | 'text' | 'number' | 'boolean';
  enumValues?: readonly string[];
}

/**
 * Information about a single tag method call in a chain.
 */
interface TagCallInfo {
  methodName: string;
  argument: ts.Expression;
  originalCall: ts.CallExpression;
}

/**
 * Information about a complete tag chain.
 */
interface TagChainInfo {
  /** The context expression (e.g., `ctx`) */
  ctxExpression: ts.Expression;
  /** All tag method calls in execution order */
  tagCalls: TagCallInfo[];
  /** Schema information extracted from the type (if available) */
  schemaInfo: Map<string, SchemaFieldInfo> | null;
}

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
 */
function extractSchemaFromTagType(tagType: ts.Type, typeChecker: ts.TypeChecker): Map<string, SchemaFieldInfo> | null {
  const schemaInfo = new Map<string, SchemaFieldInfo>();

  for (const prop of tagType.getProperties()) {
    const propName = prop.getName();
    // Skip internal properties
    if (propName === 'with' || propName.startsWith('_')) {
      continue;
    }

    const propType = typeChecker.getTypeOfSymbol(prop);
    const callSigs = propType.getCallSignatures();
    if (callSigs.length === 0) continue;

    const params = callSigs[0].getParameters();
    if (params.length === 0) continue;

    const paramType = typeChecker.getTypeOfSymbol(params[0]);

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
        schemaInfo.set(propName, {
          type: 'enum',
          enumValues: stringLiteralTypes,
        });
        continue;
      }
    }

    // Check primitive types
    const typeString = typeChecker.typeToString(paramType);
    if (typeString === 'string') {
      schemaInfo.set(propName, { type: 'category' }); // Default string type
    } else if (typeString === 'number') {
      schemaInfo.set(propName, { type: 'number' });
    } else if (typeString === 'boolean') {
      schemaInfo.set(propName, { type: 'boolean' });
    }
  }

  return schemaInfo.size > 0 ? schemaInfo : null;
}

/**
 * Find the root of a tag chain and collect all method calls.
 *
 * Given: ctx.tag.operation('SELECT').userId('123')
 * Returns: { ctxExpression: ctx, tagCalls: [{operation, 'SELECT'}, {userId, '123'}] }
 */
function findTagChainRoot(node: ts.CallExpression, typeChecker: ts.TypeChecker | undefined): TagChainInfo | null {
  const tagCalls: TagCallInfo[] = [];
  let current: ts.Expression = node;

  // Walk up the chain collecting method calls
  while (ts.isCallExpression(current)) {
    const callExpr = current;
    const expr = callExpr.expression;

    if (!ts.isPropertyAccessExpression(expr)) {
      return null;
    }

    const methodName = expr.name.text;

    // Skip 'with' method - it takes an object, not a single value
    if (methodName === 'with') {
      return null;
    }

    // Check if we have a single argument
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

  // Verify the type if we have a type checker
  if (typeChecker) {
    const tagType = typeChecker.getTypeAtLocation(current);
    if (!isTagWriterType(tagType, typeChecker)) {
      return null;
    }

    const ctxExpression = current.expression;
    const schemaInfo = extractSchemaFromTagType(tagType, typeChecker);

    return {
      ctxExpression,
      tagCalls: tagCalls.reverse(), // Reverse to get execution order
      schemaInfo,
    };
  }

  // Without type checker, accept based on structure
  return {
    ctxExpression: current.expression,
    tagCalls: tagCalls.reverse(),
    schemaInfo: null,
  };
}

/**
 * Check if an expression is a literal that can be inlined.
 */
function isInlineableLiteral(expr: ts.Expression): boolean {
  return (
    ts.isStringLiteral(expr) ||
    ts.isNumericLiteral(expr) ||
    expr.kind === ts.SyntaxKind.TrueKeyword ||
    expr.kind === ts.SyntaxKind.FalseKeyword
  );
}

/**
 * Get enum index for a value using sorted order (matching runtime behavior).
 * Enum values are sorted alphabetically for consistent indexing.
 */
function getEnumIndex(value: string, enumValues: readonly string[]): number | null {
  const sorted = [...enumValues].sort();
  const index = sorted.indexOf(value);
  return index >= 0 ? index : null;
}

/**
 * Generate inlined tag writes as a comma expression.
 *
 * Transforms: ctx.tag.operation('SELECT').userId('123')
 * Into: (ctx._buffer.operation(0, 2), ctx._buffer.userId(0, '123'), ctx.tag)
 *
 * The last element (ctx.tag) preserves the return value for chaining.
 */
function generateInlinedTagWrites(
  ctxExpression: ts.Expression,
  tagCalls: TagCallInfo[],
  schemaInfo: Map<string, SchemaFieldInfo> | null,
  factory: ts.NodeFactory,
): ts.Expression {
  const expressions: ts.Expression[] = [];

  // Generate buffer setter calls
  // ctx._buffer
  const bufferAccess = factory.createPropertyAccessExpression(ctxExpression, factory.createIdentifier('_buffer'));

  for (const call of tagCalls) {
    const { methodName, argument } = call;

    // Determine the value to write
    let valueExpr: ts.Expression;

    const fieldInfo = schemaInfo?.get(methodName);

    if (fieldInfo?.type === 'enum' && fieldInfo.enumValues && ts.isStringLiteral(argument)) {
      // Compute enum index at compile time
      const enumIndex = getEnumIndex(argument.text, fieldInfo.enumValues);
      if (enumIndex !== null) {
        valueExpr = factory.createNumericLiteral(enumIndex);
      } else {
        // Unknown enum value - keep as string (will error at runtime or be caught by TypeScript)
        valueExpr = argument;
      }
    } else {
      // Pass through the literal value
      valueExpr = argument;
    }

    // Generate: ctx._buffer.methodName(0, value)
    const setterCall = factory.createCallExpression(
      factory.createPropertyAccessExpression(bufferAccess, factory.createIdentifier(methodName)),
      undefined,
      [factory.createNumericLiteral(0), valueExpr],
    );

    expressions.push(setterCall);
  }

  // Add ctx.tag as the final value to preserve return value for chaining
  const tagAccess = factory.createPropertyAccessExpression(ctxExpression, factory.createIdentifier('tag'));
  expressions.push(tagAccess);

  // Create comma expression: (expr1, expr2, ..., exprN)
  let result: ts.Expression = expressions[0];
  for (let i = 1; i < expressions.length; i++) {
    result = factory.createBinaryExpression(result, ts.SyntaxKind.CommaToken, expressions[i]);
  }

  // Wrap in parentheses for clarity
  return factory.createParenthesizedExpression(result);
}

/**
 * Try to transform a ctx.tag fluent chain to inlined buffer writes.
 *
 * Before: ctx.tag.operation('SELECT').userId('123')
 * After:  (ctx._buffer.operation(0, 2), ctx._buffer.userId(0, '123'), ctx.tag)
 */
function tryTransformTagChain(
  node: ts.CallExpression,
  factory: ts.NodeFactory,
  processedCalls: WeakSet<ts.CallExpression>,
  typeChecker: ts.TypeChecker | undefined,
): ts.Expression | null {
  // Find the tag chain root
  const chainInfo = findTagChainRoot(node, typeChecker);
  if (!chainInfo) return null;

  // Check if we have any inlineable calls (all must have literal arguments)
  const allLiterals = chainInfo.tagCalls.every((call) => isInlineableLiteral(call.argument));
  if (!allLiterals) {
    return null; // Can't inline if any argument is not a literal
  }

  // Mark all original calls as processed
  for (const call of chainInfo.tagCalls) {
    processedCalls.add(call.originalCall);
  }

  // Generate inlined code
  return generateInlinedTagWrites(chainInfo.ctxExpression, chainInfo.tagCalls, chainInfo.schemaInfo, factory);
}

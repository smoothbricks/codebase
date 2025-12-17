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

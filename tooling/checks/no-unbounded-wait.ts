// `deadline/no-unbounded-wait` and `deadline/unbounded-wait-why`: no TypeScript wait on another process without a
// bound (specs/cowshed/05_gateway.md, "Deadlines", "Enforcement"; specs/lmao/01r_transient_errors_and_retry.md,
// "TypeScript deadlines").
//
// A wait on another process is bounded by `deadline` or `idle` from @smoothbricks/lmao/errors/Deadline, whose body
// ends its side of the wait on `wait.signal`. The rule refuses the spellings that wait without one: `fetch` without a
// `signal`; a socket connect (`net`/`tls` `connect`, `createConnection`, `Socket#connect`, `Bun.connect`); a Bun
// child's `exited`; `events.once` on a child, socket or stream without a `signal`; a promise settled only from a
// child's, socket's or stream's callback; and a child_process call that waits for the child's end (`spawnSync`,
// `execSync`, `execFileSync`, `execFile`, `exec`, `Bun.spawnSync`) without `timeout`. Inside a function passed to
// `deadline` or `idle`, every one but `fetch` and the synchronous calls is bounded by that wait: a `fetch` still takes
// the wait's signal, and a synchronous call blocks the timer that would end it.
//
// A kept wait carries `// eslint-disable-next-line deadline/no-unbounded-wait -- WHY unbounded: <what ends the wait>`
// on the line above it; `deadline/unbounded-wait-why` refuses any other directive that disables the rule. The walk is
// type-directed: each callee and receiver is resolved to its declaration in @types/node or bun-types, so a local
// function that happens to share a name is never refused.
import type { ParserServices } from '@typescript-eslint/parser';
import type { Rule } from 'eslint';
import ts from 'typescript';

const RULE = 'deadline/no-unbounded-wait';
const ESCAPE = `\`// eslint-disable-next-line ${RULE} -- WHY unbounded: <what ends the wait>\``;
const PRIMITIVE =
  'Wait inside `deadline(op, peer, bound, (wait) => …)` (a request, answering TIMEOUT past its class bound) or `idle(op, holder, idleMs, (wait) => …)` (a long wait whose holder shows progress, answering SILENT) from @smoothbricks/lmao/errors/Deadline, ending your side of the wait on `wait.signal`';

export type WaitFinding = 'fetch' | 'connect' | 'exited' | 'event' | 'promise' | 'syncChild';

const MESSAGES: Record<WaitFinding | 'untyped', string> = {
  fetch: `WAIT-FETCH: fetch without a \`signal\` waits as long as the server takes, and a server that never answers holds this process forever. Pass a bounded wait's signal: \`deadline(op, peer, bound, (wait) => fetch(url, { signal: wait.signal }))\` from @smoothbricks/lmao/errors/Deadline; or keep it with ${ESCAPE}.`,
  connect: `WAIT-CONNECT: \`{{name}}\` connects to another process with no bound on the connect or its answer. ${PRIMITIVE}; or keep it with ${ESCAPE}.`,
  exited: `WAIT-EXIT: a child's \`exited\` settles only when the child exits, and a child stuck in the kernel never does. ${PRIMITIVE}; or keep it with ${ESCAPE}.`,
  event: `WAIT-EVENT: \`once(…, '{{event}}')\` on a child, socket or stream settles only when that peer acts. ${PRIMITIVE}, passing \`{ signal: wait.signal }\`; or keep it with ${ESCAPE}.`,
  promise: `WAIT-PROMISE: this promise settles only from a child's, socket's or stream's callback, so a peer that never calls back holds it forever. ${PRIMITIVE}; or keep it with ${ESCAPE}.`,
  syncChild: `WAIT-SYNC-CHILD: \`{{name}}\` without \`timeout\` waits for the child's end with no bound{{blocks}}. Pass \`timeout\`, the bound of the child's latency class; or keep it with ${ESCAPE}.`,
  untyped: `WAIT-UNTYPED: ${RULE} is type-directed and this file was linted without a TypeScript program (parserOptions.project or projectService), so it can refuse nothing here.`,
};

/** One wait without a bound, at its 0-based TS span. */
export interface Finding {
  readonly kind: WaitFinding;
  readonly start: number;
  readonly end: number;
  readonly data: Readonly<Record<string, string>>;
}

type Origin = 'node' | 'bun' | 'lib' | 'deadline';

/** Where `symbol` is declared: @types/node, bun-types, a TypeScript lib, or lmao's Deadline module. */
function originOf(symbol: ts.Symbol): { readonly origin: Origin; readonly module: string } | null {
  for (const declaration of symbol.declarations ?? []) {
    const file = declaration.getSourceFile().fileName;
    const node = /[\\/]@types[\\/]node[\\/](?:.*[\\/])?([^\\/]+)\.d\.ts$/.exec(file);
    if (node !== null) return { origin: 'node', module: node[1] ?? '' };
    if (/[\\/]bun-types[\\/]/.test(file)) return { origin: 'bun', module: '' };
    if (/[\\/]lmao[\\/](?:src|dist[\\/]ts)[\\/]lib[\\/]errors[\\/]Deadline\.(?:d\.)?ts$/.test(file)) {
      return { origin: 'deadline', module: '' };
    }
    if (/[\\/]typescript[\\/]lib[\\/]lib\.[^\\/]+\.d\.ts$/.test(file) || /[\\/]undici-types[\\/]/.test(file)) {
      return { origin: 'lib', module: '' };
    }
  }
  return null;
}

function resolved(checker: ts.TypeChecker, symbol: ts.Symbol | undefined): ts.Symbol | undefined {
  return symbol !== undefined && (symbol.flags & ts.SymbolFlags.Alias) !== 0
    ? checker.getAliasedSymbol(symbol)
    : symbol;
}

/** The symbol a call's callee names, aliases followed. */
function calleeSymbol(checker: ts.TypeChecker, call: ts.CallExpression): ts.Symbol | undefined {
  const callee = ts.isPropertyAccessExpression(call.expression) ? call.expression.name : call.expression;
  return resolved(checker, checker.getSymbolAtLocation(callee));
}

/** A handle on another process's end of a wait: a child, a socket, a server, a stream, a Bun child. */
const PEER_HANDLES: Readonly<Record<string, true>> = {
  ChildProcess: true,
  Socket: true,
  Server: true,
  Readable: true,
  Writable: true,
  Duplex: true,
  Subprocess: true,
};

/** Whether `type`, one of its union members, or one of their bases is a peer handle declared in @types/node or bun-types. */
function isPeerHandle(checker: ts.TypeChecker, type: ts.Type, seen = new Set<ts.Symbol>()): boolean {
  if (type.isUnion()) return type.types.some((member) => isPeerHandle(checker, member, seen));
  const symbol = type.getSymbol();
  if (symbol === undefined || seen.has(symbol)) return false;
  seen.add(symbol);
  const origin = originOf(symbol)?.origin;
  if (PEER_HANDLES[symbol.name] === true && (origin === 'node' || origin === 'bun')) return true;
  for (const declaration of symbol.declarations ?? []) {
    if (!ts.isInterfaceDeclaration(declaration) && !ts.isClassDeclaration(declaration)) continue;
    for (const clause of declaration.heritageClauses ?? []) {
      for (const base of clause.types) {
        if (isPeerHandle(checker, checker.getTypeAtLocation(base), seen)) return true;
      }
    }
  }
  return false;
}

/** Whether `property` names `key` (`key: …`, `key`, `'key': …`, or a method). */
function names(property: ts.ObjectLiteralElementLike, key: string): boolean {
  const name = property.name;
  return name !== undefined && (ts.isIdentifier(name) || ts.isStringLiteral(name)) && name.text === key;
}

/** Whether an argument certainly carries `key`: a literal that names it, or a value whose type requires it. */
function carries(checker: ts.TypeChecker, argument: ts.Expression, key: string): boolean {
  if (ts.isObjectLiteralExpression(argument)) {
    return argument.properties.some(
      (property) =>
        names(property, key) || (ts.isSpreadAssignment(property) && carries(checker, property.expression, key)),
    );
  }
  const type = checker.getTypeAtLocation(argument);
  const members = type.isUnion() ? type.types : [type];
  return members.every((member) => {
    const property = checker.getPropertyOfType(member, key);
    return property !== undefined && (property.flags & ts.SymbolFlags.Optional) === 0;
  });
}

/** The child_process calls that wait for the child's end; `true` marks the synchronous ones. */
const CHILD_CALLS: Readonly<Record<string, boolean>> = {
  spawnSync: true,
  execSync: true,
  execFileSync: true,
  execFile: false,
  exec: false,
};
const CONNECTS: Readonly<Record<string, true>> = { connect: true, createConnection: true };

/** Whether `node` is a function, the shape a callback or a promise executor takes. */
function isFunction(node: ts.Node): node is ts.ArrowFunction | ts.FunctionExpression {
  return ts.isArrowFunction(node) || ts.isFunctionExpression(node);
}

/** Whether `call` is a method call on a peer handle (`child.on(…)`, `socket.write(…)`, `server.close(…)`). */
function isPeerCall(checker: ts.TypeChecker, call: ts.Node): call is ts.CallExpression {
  return (
    ts.isCallExpression(call) &&
    ts.isPropertyAccessExpression(call.expression) &&
    isPeerHandle(checker, checker.getTypeAtLocation(call.expression.expression))
  );
}

/** The name a function is declared under (`function onClose`, `const onClose = () => …`), if any. */
function declaredName(node: ts.Node): ts.Identifier | undefined {
  if (ts.isFunctionDeclaration(node)) return node.name;
  return isFunction(node) && ts.isVariableDeclaration(node.parent) && ts.isIdentifier(node.parent.name)
    ? node.parent.name
    : undefined;
}

/**
 * Whether `callback` (a function, or a settler handed over as is) runs only as a peer handle's callback: it is an
 * argument of a peer call, or it is a named function whose every reference in `scope` is one.
 */
function runsOnlyFromPeers(checker: ts.TypeChecker, scope: ts.Node, callback: ts.Node): boolean {
  if (isPeerCall(checker, callback.parent) && callback.parent.arguments.some((argument) => argument === callback)) {
    return true;
  }
  const name = declaredName(callback);
  const symbol = name === undefined ? undefined : checker.getSymbolAtLocation(name);
  if (name === undefined || symbol === undefined) return false;
  let references = 0;
  let fromPeers = 0;
  const visit = (node: ts.Node): void => {
    if (ts.isIdentifier(node) && node !== name && checker.getSymbolAtLocation(node) === symbol) {
      references += 1;
      if (isPeerCall(checker, node.parent) && node.parent.arguments.some((argument) => argument === node)) {
        fromPeers += 1;
      }
    }
    ts.forEachChild(node, visit);
  };
  ts.forEachChild(scope, visit);
  return references > 0 && references === fromPeers;
}

/**
 * Whether every settle of a promise (each reference to `settlers` inside `scope`) happens in a callback a peer handle
 * runs (`child.on('close', …)`, `socket.write(data, …)`, `server.close(…)`), and there is at least one. A settle
 * anywhere else (a timer, an abort listener, the executor's own body, a stored function) may end the wait without the
 * peer, so the promise is not refused.
 */
function settlesOnlyFromPeers(checker: ts.TypeChecker, scope: ts.Node, settlers: ReadonlySet<ts.Symbol>): boolean {
  let settles = 0;
  let fromPeers = 0;
  const visit = (node: ts.Node): void => {
    if (ts.isIdentifier(node) && !ts.isBindingElement(node.parent) && !ts.isParameter(node.parent)) {
      const symbol = ts.isShorthandPropertyAssignment(node.parent)
        ? checker.getShorthandAssignmentValueSymbol(node.parent)
        : checker.getSymbolAtLocation(node);
      if (symbol !== undefined && settlers.has(symbol)) {
        settles += 1;
        // The callback that settles: the settler itself when handed over (`child.once('close', resolve)`), else the
        // innermost function around it below the scope.
        let callback: ts.Node | undefined =
          ts.isCallExpression(node.parent) && node.parent.expression !== node ? node : undefined;
        for (let up: ts.Node = node.parent; callback === undefined && up !== scope; up = up.parent) {
          if (isFunction(up) || ts.isFunctionDeclaration(up)) callback = up;
        }
        if (callback !== undefined && runsOnlyFromPeers(checker, scope, callback)) fromPeers += 1;
      }
    }
    ts.forEachChild(node, visit);
  };
  ts.forEachChild(scope, visit);
  return settles > 0 && settles === fromPeers;
}

/** The symbols of `resolve`/`reject` destructured from `Promise.withResolvers()` in `declaration`. */
function withResolversSettlers(checker: ts.TypeChecker, declaration: ts.VariableDeclaration): Set<ts.Symbol> {
  const settlers = new Set<ts.Symbol>();
  if (!ts.isObjectBindingPattern(declaration.name)) return settlers;
  for (const element of declaration.name.elements) {
    const key = element.propertyName ?? element.name;
    if (!ts.isIdentifier(key) || (key.text !== 'resolve' && key.text !== 'reject') || !ts.isIdentifier(element.name)) {
      continue;
    }
    const symbol = checker.getSymbolAtLocation(element.name);
    if (symbol !== undefined) settlers.add(symbol);
  }
  return settlers;
}

/** Whether `expression` is the global `Promise` (its constructor or `Promise.withResolvers`). */
function isGlobalPromise(checker: ts.TypeChecker, expression: ts.Expression): boolean {
  if (!ts.isIdentifier(expression) || expression.text !== 'Promise') return false;
  const symbol = checker.getSymbolAtLocation(expression);
  return symbol !== undefined && originOf(symbol)?.origin === 'lib';
}

/** Every wait in `file` without a bound. */
export function walkWaits(checker: ts.TypeChecker, file: ts.SourceFile): Finding[] {
  const found: Finding[] = [];
  const report = (kind: WaitFinding, node: ts.Node, data: Record<string, string> = {}) => {
    found.push({ kind, start: node.getStart(file), end: node.getEnd(), data });
  };
  const visit = (node: ts.Node, bounded: boolean): void => {
    let boundsItsCallbacks = false;
    if (ts.isCallExpression(node)) {
      const symbol = calleeSymbol(checker, node);
      const where = symbol === undefined ? null : originOf(symbol);
      const name = symbol?.name ?? '';
      const declaration = symbol?.declarations?.[0];
      const isStatic =
        declaration !== undefined && (ts.getCombinedModifierFlags(declaration) & ts.ModifierFlags.Static) !== 0;
      const isMethod =
        declaration !== undefined && (ts.isMethodSignature(declaration) || ts.isMethodDeclaration(declaration));
      if (where?.origin === 'deadline' && (name === 'deadline' || name === 'idle')) {
        boundsItsCallbacks = true;
      } else if (name === 'fetch' && (where?.origin === 'lib' || where?.origin === 'node' || where?.origin === 'bun')) {
        const init = node.arguments[1];
        if (init === undefined || !carries(checker, init, 'signal')) report('fetch', node.expression);
      } else if (
        !bounded &&
        CONNECTS[name] === true &&
        ((where?.origin === 'node' && (where.module === 'net' || where.module === 'tls')) || where?.origin === 'bun')
      ) {
        report('connect', node.expression, { name: node.expression.getText(file) });
      } else if (
        (where?.origin === 'node' && where.module === 'child_process' && Object.hasOwn(CHILD_CALLS, name)) ||
        (where?.origin === 'bun' && name === 'spawnSync')
      ) {
        const synchronous = CHILD_CALLS[name] !== false;
        if ((synchronous || !bounded) && !node.arguments.some((argument) => carries(checker, argument, 'timeout'))) {
          report('syncChild', node.expression, {
            name: node.expression.getText(file),
            blocks: synchronous ? ', and blocks this whole process, its timers included, until then' : '',
          });
        }
      } else if (
        !bounded &&
        name === 'once' &&
        where?.origin === 'node' &&
        where.module === 'events' &&
        (!isMethod || isStatic)
      ) {
        const [emitter, event, options] = node.arguments;
        if (
          emitter !== undefined &&
          isPeerHandle(checker, checker.getTypeAtLocation(emitter)) &&
          (options === undefined || !carries(checker, options, 'signal'))
        ) {
          report('event', node.expression, {
            event: event !== undefined && ts.isStringLiteral(event) ? event.text : '…',
          });
        }
      }
    } else if (!bounded && ts.isPropertyAccessExpression(node) && node.name.text === 'exited') {
      const symbol = resolved(checker, checker.getSymbolAtLocation(node.name));
      if (symbol !== undefined && originOf(symbol)?.origin === 'bun') report('exited', node);
    } else if (!bounded && ts.isNewExpression(node) && isGlobalPromise(checker, node.expression)) {
      const executor = node.arguments?.[0];
      if (executor !== undefined && isFunction(executor)) {
        const settlers = new Set<ts.Symbol>();
        for (const parameter of executor.parameters.slice(0, 2)) {
          const symbol = ts.isIdentifier(parameter.name) ? checker.getSymbolAtLocation(parameter.name) : undefined;
          if (symbol !== undefined) settlers.add(symbol);
        }
        if (settlesOnlyFromPeers(checker, executor.body, settlers)) report('promise', node);
      }
    } else if (
      !bounded &&
      ts.isVariableDeclaration(node) &&
      node.initializer !== undefined &&
      ts.isCallExpression(node.initializer) &&
      ts.isPropertyAccessExpression(node.initializer.expression) &&
      node.initializer.expression.name.text === 'withResolvers' &&
      isGlobalPromise(checker, node.initializer.expression.expression)
    ) {
      let scope: ts.Node = node;
      while (!ts.isSourceFile(scope) && !ts.isBlock(scope)) scope = scope.parent;
      if (settlesOnlyFromPeers(checker, scope, withResolversSettlers(checker, node)))
        report('promise', node.initializer);
    }
    ts.forEachChild(node, (child) => visit(child, bounded || (boundsItsCallbacks && isFunction(child))));
  };
  visit(file, false);
  return found;
}

const noUnboundedWait: Rule.RuleModule = {
  meta: {
    type: 'problem',
    docs: { description: 'No wait on another process without a bound (specs/cowshed/05_gateway.md, "Deadlines").' },
    schema: [],
    messages: MESSAGES,
  },
  create(context) {
    return {
      Program(node) {
        const services: ParserServices | undefined = context.sourceCode.parserServices;
        const file = services?.program?.getSourceFile(context.filename);
        if (services?.program == null || file === undefined) {
          context.report({ node, messageId: 'untyped' });
          return;
        }
        const position = (offset: number) => {
          const { line, character } = file.getLineAndCharacterOfPosition(offset);
          return { line: line + 1, column: character };
        };
        for (const { kind, start, end, data } of walkWaits(services.program.getTypeChecker(), file)) {
          context.report({ loc: { start: position(start), end: position(end) }, messageId: kind, data });
        }
      },
    };
  },
};

/** An ESLint directive comment's kind, the rules it names and its ` -- ` description. */
const DIRECTIVE = /^\s*eslint-disable(?<scope>-next-line|-line)?(?=\s|$)(?<rest>[\s\S]*)$/;

const unboundedWaitWhy: Rule.RuleModule = {
  meta: {
    type: 'problem',
    docs: { description: `Every directive that disables ${RULE} states why the wait is unbounded.` },
    schema: [],
    messages: {
      unnamed: `WAIT-WHY: a directive that names no rule disables ${RULE} with every other rule; name the rules it disables, and for a kept wait write ${ESCAPE}.`,
      scope: `WAIT-WHY: ${RULE} is disabled for one statement only, on the line above it: ${ESCAPE}.`,
      why: `WAIT-WHY: a kept wait states what ends it: ${ESCAPE}. A peer that can stall the wait forever is no reason; bound the wait instead.`,
    },
  },
  create(context) {
    return {
      Program() {
        for (const comment of context.sourceCode.getAllComments()) {
          const directive = DIRECTIVE.exec(comment.value);
          if (directive === null || comment.loc == null) continue;
          const rest = directive.groups?.rest ?? '';
          const separator = rest.search(/\s-{2,}\s/);
          const rules = (separator < 0 ? rest : rest.slice(0, separator))
            .split(',')
            .map((rule) => rule.trim())
            .filter((rule) => rule !== '');
          const description =
            separator < 0
              ? ''
              : rest
                  .slice(separator)
                  .replace(/^\s-{2,}\s/, '')
                  .trim();
          const loc = comment.loc;
          if (rules.length === 0) context.report({ loc, messageId: 'unnamed' });
          else if (!rules.includes(RULE)) continue;
          else if (directive.groups?.scope !== '-next-line') context.report({ loc, messageId: 'scope' });
          else if (!/^WHY unbounded: \S/.test(description)) context.report({ loc, messageId: 'why' });
        }
      },
    };
  },
};

export const deadlinePlugin = {
  rules: { 'no-unbounded-wait': noUnboundedWait, 'unbounded-wait-why': unboundedWaitWhy },
};

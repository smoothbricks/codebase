import { describe, expect, it } from 'bun:test';
import { fileURLToPath } from 'node:url';
import ts from 'typescript';
import { createLmaoTransformer } from '../transformer.js';

function transform(source: string, options?: { fileName?: string; projectRoot?: string }): string {
  const result = ts.transpileModule(source, {
    fileName: options?.fileName,
    compilerOptions: {
      alwaysStrict: false,
      module: ts.ModuleKind.ESNext,
      target: ts.ScriptTarget.ESNext,
    },
    transformers: { before: [createLmaoTransformer({ projectRoot: options?.projectRoot })] },
  });
  return result.outputText;
}

const thisTestFile = fileURLToPath(import.meta.url);

/**
 * Normalize whitespace for comparison - removes extra spaces and normalizes quotes
 */
function normalize(code: string): string {
  return code.replace(/\s+/g, ' ').replace(/"/g, "'").trim();
}

describe('lmao-transformer', () => {
  describe('ctx.span() transformation', () => {
    it('should add line number and rewrite to span0 for closure', () => {
      const input = `ctx.span('test', async () => {});`;
      const output = transform(input);
      // New format: ctx.span0(line, name, ctx._newCtx0(), SpanBufferClass, remappedViewClass, opMetadata, fn)
      expect(normalize(output)).toContain("ctx.span0(1, 'test'");
      expect(normalize(output)).toContain('ctx._newCtx0()');
      expect(normalize(output)).toContain('ctx._buffer.constructor');
      expect(normalize(output)).toContain('async () => { }');
    });

    it('should prepend line number and rewrite to span0 for non-closures (assumed Op)', () => {
      const input = `ctx.span('test', myOp);`;
      const output = transform(input);
      // Without type checker, non-closure is assumed to be an Op
      expect(normalize(output)).toContain("ctx.span0(1, 'test'");
      expect(normalize(output)).toContain('myOp.SpanBufferClass');
      expect(normalize(output)).toContain('myOp.fn');
    });

    it('should handle multi-line spans and use correct line number', () => {
      const input = `const x = 1;
ctx.span('first', async () => {});
const y = 2;
ctx.span('second', async () => {});`;
      const output = transform(input);
      expect(normalize(output)).toContain("ctx.span0(2, 'first'");
      expect(normalize(output)).toContain("ctx.span0(4, 'second'");
    });

    it('should handle await ctx.span()', () => {
      const input = `await ctx.span('test', async (childCtx) => { return 42; });`;
      const output = transform(input);
      expect(normalize(output)).toContain("await ctx.span0(1, 'test'");
      expect(normalize(output)).toContain('async (childCtx) => { return 42; }');
    });

    it('should handle nested spans', () => {
      const input = `ctx.span('outer', async (outerCtx) => {
  outerCtx.span('inner', async () => {});
});`;
      const output = transform(input);
      expect(normalize(output)).toContain("ctx.span0(1, 'outer'");
      expect(normalize(output)).toContain("outerCtx.span0(2, 'inner'");
    });

    it('should handle span with more than 2 arguments', () => {
      const input = `ctx.span('name', op, 1, 2, 3);`;
      const output = transform(input);
      // With 3 extra args, method becomes span3
      expect(normalize(output)).toContain("ctx.span3(1, 'name'");
      expect(normalize(output)).toContain('op.fn, 1, 2, 3');
    });
  });

  describe('ctx.log.{method}() transformation', () => {
    it('should append .line() to ctx.log.info()', () => {
      const input = `ctx.log.info('Processing user');`;
      const output = transform(input);
      expect(normalize(output)).toContain("ctx.log.info('Processing user').line(1)");
    });

    it('should append .line() to ctx.log.debug()', () => {
      const input = `ctx.log.debug('Debug message');`;
      const output = transform(input);
      expect(normalize(output)).toContain("ctx.log.debug('Debug message').line(1)");
    });

    it('should append .line() to ctx.log.warn()', () => {
      const input = `ctx.log.warn('Warning');`;
      const output = transform(input);
      expect(normalize(output)).toContain("ctx.log.warn('Warning').line(1)");
    });

    it('should append .line() to ctx.log.error()', () => {
      const input = `ctx.log.error('Error occurred');`;
      const output = transform(input);
      expect(normalize(output)).toContain("ctx.log.error('Error occurred').line(1)");
    });

    it('should append .line() to ctx.log.trace()', () => {
      const input = `ctx.log.trace('Trace message');`;
      const output = transform(input);
      expect(normalize(output)).toContain("ctx.log.trace('Trace message').line(1)");
    });

    it('should not transform if .line() already exists', () => {
      const input = `ctx.log.info('msg').line(99);`;
      const output = transform(input);
      // Should keep existing .line(99), not add another
      expect(normalize(output)).toContain('.line(99)');
      expect(normalize(output)).not.toContain('.line(1)');
    });

    it('should handle chained calls - insert line after log method', () => {
      const input = `ctx.log.info('msg').userId('123');`;
      const output = transform(input);
      expect(normalize(output)).toContain("ctx.log.info('msg').line(1).userId('123')");
    });

    it('should handle multiple chained calls', () => {
      const input = `ctx.log.info('msg').userId('123').requestId('abc');`;
      const output = transform(input);
      expect(normalize(output)).toContain("ctx.log.info('msg').line(1).userId('123').requestId('abc')");
    });

    it('should not transform if .line() already exists in chain', () => {
      const input = `ctx.log.info('msg').line(50).userId('123');`;
      const output = transform(input);
      expect(normalize(output)).toContain('.line(50)');
      expect(normalize(output)).not.toContain('.line(1)');
    });

    it('should handle multi-line with correct line numbers', () => {
      const input = `const a = 1;
ctx.log.info('first');
const b = 2;
ctx.log.warn('second');`;
      const output = transform(input);
      expect(normalize(output)).toContain("ctx.log.info('first').line(2)");
      expect(normalize(output)).toContain("ctx.log.warn('second').line(4)");
    });

    it('should handle multiple arguments to log method', () => {
      const input = `ctx.log.info('User', userId);`;
      const output = transform(input);
      expect(normalize(output)).toContain("ctx.log.info('User', userId).line(1)");
    });
  });

  describe('combined transformations', () => {
    it('should transform both span and log in same file', () => {
      const input = `ctx.span('process', async (ctx) => {
  ctx.log.info('Starting');
});`;
      const output = transform(input);
      // New span format includes many more params
      expect(normalize(output)).toContain("ctx.span0(1, 'process'");
      expect(normalize(output)).toContain("ctx.log.info('Starting').line(2)");
    });

    it('should handle deeply nested structures', () => {
      const input = `ctx.span('outer', async (outerCtx) => {
  outerCtx.log.info('outer log');
  outerCtx.span('inner', async (innerCtx) => {
    innerCtx.log.debug('inner log');
  });
});`;
      const output = transform(input);
      expect(normalize(output)).toContain('.line(2)');
      expect(normalize(output)).toContain('.line(4)');
      expect(normalize(output)).toContain("ctx.span0(1, 'outer'");
      expect(normalize(output)).toContain("outerCtx.span0(3, 'inner'");
    });
  });

  describe('ctx.ok() and ctx.err() transformation', () => {
    it('should append .line() to ctx.ok()', () => {
      const input = 'return ctx.ok({ success: true });';
      const output = transform(input);
      expect(normalize(output)).toContain('ctx.ok({ success: true }).line(1)');
    });

    it('should append .line() to ctx.err()', () => {
      const input = `return ctx.err('VALIDATION_ERROR', { field: 'email' });`;
      const output = transform(input);
      expect(normalize(output)).toContain("ctx.err('VALIDATION_ERROR', { field: 'email' }).line(1)");
    });

    it('should handle ctx.ok() with chained .with()', () => {
      const input = `return ctx.ok(result).with({ userId: '123' });`;
      const output = transform(input);
      expect(normalize(output)).toContain("ctx.ok(result).line(1).with({ userId: '123' })");
    });

    it('should handle ctx.err() with chained .message()', () => {
      const input = `return ctx.err('ERROR', details).message('Failed');`;
      const output = transform(input);
      expect(normalize(output)).toContain("ctx.err('ERROR', details).line(1).message('Failed')");
    });

    it('should handle ctx.ok() with multiple chained calls', () => {
      const input = `return ctx.ok(user).with({ userId: user.id }).message('Created');`;
      const output = transform(input);
      expect(normalize(output)).toContain("ctx.ok(user).line(1).with({ userId: user.id }).message('Created')");
    });

    it('should not transform if .line() already exists on ok()', () => {
      const input = 'return ctx.ok(result).line(99);';
      const output = transform(input);
      expect(normalize(output)).toContain('.line(99)');
      expect(normalize(output)).not.toContain('.line(1)');
    });

    it('should not transform if .line() already exists on err()', () => {
      const input = `return ctx.err('ERROR', details).line(50).with({ foo: 'bar' });`;
      const output = transform(input);
      expect(normalize(output)).toContain('.line(50)');
      expect(normalize(output)).not.toContain('.line(1)');
    });

    it('should handle multi-line with correct line numbers', () => {
      const input = `const a = 1;
return ctx.ok({ done: true });`;
      const output = transform(input);
      expect(normalize(output)).toContain('.line(2)');
    });

    it('should handle ok/err inside spans', () => {
      const input = `ctx.span('process', async (ctx) => {
  return ctx.ok({ success: true });
});`;
      const output = transform(input);
      expect(normalize(output)).toContain('ctx.ok({ success: true }).line(2)');
      expect(normalize(output)).toContain('ctx.span0(1,'); // span line number is first arg
    });
  });

  describe('edge cases', () => {
    it('should not transform non-log property accesses', () => {
      const input = `ctx.other.info('test');`;
      const output = transform(input);
      // Should not have .line() added
      expect(normalize(output)).toBe("ctx.other.info('test');");
    });

    it('should not transform non-log method calls', () => {
      const input = `ctx.log.custom('test');`;
      const output = transform(input);
      // 'custom' is not in LOG_METHODS, should not transform
      expect(normalize(output)).toBe("ctx.log.custom('test');");
    });

    it('should handle empty source', () => {
      const input = '';
      const output = transform(input);
      expect(output.trim()).toBe('');
    });

    it('should handle code with no ctx patterns', () => {
      const input = `const x = 1; console.log('hello');`;
      const output = transform(input);
      expect(normalize(output)).toContain("console.log('hello')");
    });

    it('should handle chains with type arguments (type args erased at runtime)', () => {
      // Note: TypeScript erases type arguments during transpilation, so we can't preserve them
      // in the output. This test verifies the transformation still works correctly.
      const input = `ctx.log.info('msg').tag<MyType>({ foo: 'bar' });`;
      const output = transform(input);
      expect(output).toContain('.line(1)');
      // Type arguments are erased, but the method call should still be there
      expect(normalize(output)).toContain(".tag({ foo: 'bar' })");
    });
  });

  describe('defineModule() transformation', () => {
    it('should inject metadata into defineModule()', () => {
      const input = ['defineModule({', '  logSchema: schema,', '});'].join('\n');
      const output = transform(input);
      expect(output).toContain('metadata');
      expect(output).toContain('git_sha: "unknown"');
      expect(output).toContain('package_name: "unknown"');
      expect(output).toContain('package_file: "module.ts"');
    });

    it('should inject real file metadata when source file exists', () => {
      const input = ['defineModule({', '  logSchema: schema,', '});'].join('\n');
      const output = transform(input, { fileName: thisTestFile });
      expect(output).toMatch(/git_sha: "[0-9a-f]{40}"/);
      expect(output).toContain('package_name: "@smoothbricks/lmao-transformer"');
      expect(output).toContain('package_file: "src/__tests__/transformer.test.ts"');
    });

    it('should reject multiple defineModule declarations in one source file', () => {
      const input = [
        'defineModule({',
        '  logSchema: schema,',
        '});',
        'defineModule({',
        '  logSchema: schema,',
        '});',
      ].join('\n');
      expect(() => transform(input)).toThrow('contains multiple defineModule() declarations');
    });

    it('should not overwrite existing metadata', () => {
      const input = [
        'defineModule({',
        "  metadata: { gitSha: 'custom', packageName: 'custom', packagePath: 'custom' },",
        '  logSchema: schema,',
        '});',
      ].join('\n');
      const output = transform(input);
      expect(output).toContain("gitSha: 'custom'");
      // Should NOT have duplicate metadata
      expect(output.match(/metadata/g)?.length).toBe(1);
    });

    it('should handle lmao.defineModule() property access pattern', () => {
      const input = ['lmao.defineModule({', '  logSchema: schema,', '});'].join('\n');
      const output = transform(input);
      expect(output).toContain('metadata');
      expect(output).toContain('git_sha');
      expect(output).toContain('package_name');
      expect(output).toContain('package_file');
    });

    it('should preserve existing properties when injecting metadata', () => {
      const input = ['defineModule({', '  logSchema: schema,', '  ff: flags,', '});'].join('\n');
      const output = transform(input);
      expect(output).toContain('metadata');
      expect(output).toContain('logSchema');
    });

    it('should not transform defineModule without object argument', () => {
      const input = 'defineModule();';
      const output = transform(input);
      expect(output).toBe('defineModule();\n');
    });

    it('should not transform defineModule with non-object argument', () => {
      const input = 'defineModule(config);';
      const output = transform(input);
      expect(output).toBe('defineModule(config);\n');
    });

    it('should not transform defineModule with non-object argument', () => {
      const input = 'defineModule(config);';
      const output = transform(input);
      expect(normalize(output)).toBe('defineModule(config);');
    });

    it('should handle defineModule with empty object', () => {
      const input = 'defineModule({});';
      const output = transform(input);
      expect(output).toContain('metadata');
      expect(output).toContain('git_sha');
    });

    it('should not transform defineModule with logSchema property', () => {
      const input = 'defineModule({ logSchema: schema });';
      const output = transform(input);
      expect(output).toContain('metadata');
      expect(output).toContain('logSchema');
    });

    it('should not overwrite existing metadata', () => {
      const input = [
        'defineModule({',
        "  metadata: { gitSha: 'custom', packageName: 'custom', packagePath: 'custom' },",
        '  logSchema: schema,',
        '});',
      ].join('\n');
      const output = transform(input);
      expect(output).toContain("gitSha: 'custom'");
      // Should NOT have duplicate metadata
      expect(output.match(/metadata/g)?.length).toBe(1);
    });

    it('should handle lmao.defineModule() property access pattern', () => {
      const input = ['lmao.defineModule({', '  logSchema: schema,', '});'].join('\n');
      const output = transform(input);
      expect(output).toContain('metadata');
      expect(output).toContain('git_sha');
      expect(output).toContain('package_name');
      expect(output).toContain('package_file');
    });

    it('should preserve existing properties when injecting metadata', () => {
      const input = ['defineModule({', '  logSchema: schema,', '  ff: flags,', '});'].join('\n');
      const output = transform(input);
      expect(output).toContain('metadata');
      expect(output).toContain('logSchema');
    });

    it('should not transform defineModule without object argument', () => {
      const input = 'defineModule();';
      const output = transform(input);
      expect(output).toBe('defineModule();\n');
    });

    it('should not transform defineModule with non-object argument', () => {
      const input = 'defineModule(config);';
      const output = transform(input);
      expect(output).toBe('defineModule(config);\n');
    });

    it('should handle defineModule with empty object', () => {
      const input = 'defineModule({});';
      const output = transform(input);
      expect(output).toContain('metadata');
      expect(output).toContain('git_sha');
    });

    it('should not transform defineModule with logSchema property', () => {
      const input = 'defineModule({ logSchema: schema });';
      const output = transform(input);
      expect(output).toContain('metadata');
      expect(output).toContain('logSchema');
    });
  });

  describe('task() transformation', () => {
    it('should add line number to task() calls with 2 arguments', () => {
      const input = `module.task('processOrder', async (ctx) => {});`;
      const output = transform(input);
      expect(normalize(output)).toContain("module.task('processOrder', async (ctx) => { }, 1)");
    });

    it('should not transform if line argument already provided', () => {
      const input = `module.task('processOrder', async (ctx) => {}, 99);`;
      const output = transform(input);
      expect(normalize(output)).toContain("module.task('processOrder', async (ctx) => { }, 99)");
      expect(normalize(output)).not.toContain(', 1)');
    });

    it('should handle multi-line task definitions', () => {
      const input = `const x = 1;
module.task('first', async (ctx) => {});
const y = 2;
module.task('second', async (ctx) => {});`;
      const output = transform(input);
      expect(normalize(output)).toContain("module.task('first', async (ctx) => { }, 2)");
      expect(normalize(output)).toContain("module.task('second', async (ctx) => { }, 4)");
    });

    it('should handle exported task definitions', () => {
      const input = `export const processOrder = module.task('processOrder', async (ctx) => {});`;
      const output = transform(input);
      expect(normalize(output)).toContain("module.task('processOrder', async (ctx) => { }, 1)");
    });

    it('should not transform non-task property accesses', () => {
      const input = `module.notTask('test', async () => {});`;
      const output = transform(input);
      expect(normalize(output)).toContain("module.notTask('test', async () => { })");
      expect(normalize(output)).not.toContain(', 1)');
    });

    it('should not transform task calls with non-string first argument', () => {
      const input = 'module.task(getName(), async () => {});';
      const output = transform(input);
      // Should not add line number since first arg is not a string literal
      expect(normalize(output)).toContain('module.task(getName(), async () => { })');
    });

    it('should handle destructured task() direct calls', () => {
      // const { task } = createModuleContext(...); task('name', fn)
      const input = `const processData = task('process-data', async (ctx) => {});`;
      const output = transform(input);
      expect(normalize(output)).toContain("task('process-data', async (ctx) => { }, 1)");
    });

    it('should handle destructured task() with correct line number', () => {
      const input = `const x = 1;
const y = 2;
const myTask = task('my-task', async (ctx) => {});`;
      const output = transform(input);
      expect(normalize(output)).toContain("task('my-task', async (ctx) => { }, 3)");
    });
  });

  describe('ctx.tag chain inlining', () => {
    it('should inline single tag call to direct array writes', () => {
      const input = "ctx.tag.operation('SELECT');";
      const output = transform(input);
      // Should transform to a block with direct array writes
      expect(normalize(output)).toContain('ctx._buffer.operation_nulls[0] |= 1');
      expect(normalize(output)).toContain('ctx._buffer.operation_values[0]');
    });

    it('should inline multiple tag calls in a chain', () => {
      const input = "ctx.tag.operation('SELECT').userId('user-123');";
      const output = transform(input);
      // Should have both buffer writes with null bitmaps
      expect(normalize(output)).toContain('operation_nulls[0] |= 1');
      expect(normalize(output)).toContain('operation_values[0]');
      expect(normalize(output)).toContain('userId_nulls[0] |= 1');
      expect(normalize(output)).toContain("userId_values[0] = 'user-123'");
    });

    it('should inline number values directly', () => {
      const input = 'ctx.tag.count(42);';
      const output = transform(input);
      expect(normalize(output)).toContain('count_nulls[0] |= 1');
      expect(normalize(output)).toContain('count_values[0] = 42');
    });

    it('should inline boolean true', () => {
      const input = 'ctx.tag.enabled(true);';
      const output = transform(input);
      // Without TypeChecker, boolean is treated as regular value write
      // With TypeChecker, it would use bit-packed |= 1
      expect(normalize(output)).toContain('enabled_nulls[0] |= 1');
      expect(normalize(output)).toContain('enabled_values[0]');
      expect(normalize(output)).toContain('true');
    });

    it('should inline boolean false', () => {
      const input = 'ctx.tag.disabled(false);';
      const output = transform(input);
      // Without TypeChecker, boolean is treated as regular value write
      // With TypeChecker, it would use bit-packed &= ~1
      expect(normalize(output)).toContain('disabled_nulls[0] |= 1');
      expect(normalize(output)).toContain('disabled_values[0]');
      expect(normalize(output)).toContain('false');
    });

    it('should inline non-literal arguments with null checks', () => {
      const input = 'ctx.tag.userId(getUserId());';
      const output = transform(input);
      // Non-literals are wrapped in null check
      expect(normalize(output)).toContain('const $$v0 = getUserId()');
      expect(normalize(output)).toContain('if ($$v0 != null)');
      expect(normalize(output)).toContain('userId_nulls[0] |= 1');
      expect(normalize(output)).toContain('userId_values[0] = $$v0');
    });

    it('should inline literals and keep non-literals as method calls', () => {
      const input = "ctx.tag.operation('SELECT').userId(userId);";
      const output = transform(input);
      // When there's a non-literal argument, the new inliner handles it with null checks
      // The null check wraps the variable write
      expect(normalize(output)).toContain('operation_nulls[0] |= 1');
      expect(normalize(output)).toContain('operation_values[0]');
      expect(normalize(output)).toContain('userId');
    });

    it('should inline ctx.tag.with() calls', () => {
      const input = "ctx.tag.with({ operation: 'SELECT', userId: '123' });";
      const output = transform(input);
      // with() is now inlined - each property becomes a direct write
      expect(normalize(output)).toContain('operation_nulls[0] |= 1');
      expect(normalize(output)).toContain('userId_nulls[0] |= 1');
    });

    it('should NOT inline tag chains assigned to variables', () => {
      const input = "const t = ctx.tag.operation('SELECT');";
      const output = transform(input);
      // Expression context (variable assignment) is NOT transformed
      // We only transform statement context
      expect(normalize(output)).toContain('ctx.tag.operation');
    });

    it('should handle deeply nested context expression', () => {
      const input = "this.ctx.tag.userId('123');";
      const output = transform(input);
      // New format uses direct array access with null bitmap
      expect(normalize(output)).toContain('this.ctx._buffer.userId_nulls[0] |= 1');
      expect(normalize(output)).toContain('this.ctx._buffer.userId_values[0]');
    });

    it('should handle tag chain with many calls', () => {
      const input = "ctx.tag.a('1').b('2').c('3').d('4');";
      const output = transform(input);
      // New format uses direct array access with null bitmaps
      expect(normalize(output)).toContain('a_nulls[0] |= 1');
      expect(normalize(output)).toContain("a_values[0] = '1'");
      expect(normalize(output)).toContain('b_nulls[0] |= 1');
      expect(normalize(output)).toContain("b_values[0] = '2'");
      expect(normalize(output)).toContain('c_nulls[0] |= 1');
      expect(normalize(output)).toContain("c_values[0] = '3'");
      expect(normalize(output)).toContain('d_nulls[0] |= 1');
      expect(normalize(output)).toContain("d_values[0] = '4'");
    });
  });
});

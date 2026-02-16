# LMAO Specification TODO

- [ ] Update transformer to generate branchless null-check assignment code
- [ ] Implement Logger classes AOT compilation
- [ ] Implement Logger class codegen deduplication, caching
- [ ] Transformer: span() → span_op/span_fn rewriting
- [ ] Transformer: with() bulk setter unrolling
- [ ] Transformer: Result type tag unrolling into single applyTags closure?
- [ ] Transformer: Destructured ctx { span, log, tag } parameter rewriting
- [ ] Configure log-level by module! Property on module itself.
- [ ] Transformer: Inline if (level) checks for log.{info,warn,error,debug,trace} calls.

---

Hyper ideas:

## Update AGENTS.md on package install

- Add details not already in package.json
  - Link to package documentation
  - Link to prompt with rules about this dependency

## AGENTS.md

This one is a gem: https://github.com/dew-labs/wolfy-interface/blob/dev/AGENTS.MD

> MOST IMPORTANTLY: BE EXTREMELY CONCISE AND SUCCINCT, SACRIFICE GRAMMAR FOR THE SAKE OF CONCISION!!!. MOST IMPORTANTLY:
> BE EXTREMELY CONCISE AND SUCCINCT, SACRIFICE GRAMMAR FOR THE SAKE OF CONCISION!!!. MOST IMPORTANTLY: BE EXTREMELY
> CONCISE AND SUCCINCT, SACRIFICE GRAMMAR FOR THE SAKE OF CONCISION!!!.

- 99 Agents, not super great but maybe there's some gems
  - https://github.com/wshobson/agents/blob/main/docs/agents.md

- This one even says "It is December 2025"
  - https://github.com/trevor-nichols/agentrules-architect/blob/main/AGENTS.md

- Make sure the root describes each package in the monorepo
- Make sure it tells to read nearest AGENTS.md file to the package

- https://github.com/ivawzh/agents-md Compose canonical AGENTS.md from sustainable and elegant file structures. Keep
  agent context current, composable, and shareable with your human docs. Abstract-context-as-code is what we aim to
  achieve.

- TAMAGUI example and more
  - https://github.com/gakeez/agents_md_collection/blob/main/examples/typescript-react-native-expo-development.md

## Prompt optimization

- Define with <poml> syntax, VS code extension: https://github.com/microsoft/poml
  - Docs: https://microsoft.github.io/poml/stable/language/components/

- https://github.com/guyaluk/contextor/tree/main/agent/prompts
  > "Your mission is to identify missing context that causes inefficient patterns" A GitHub Action that automatically
  > analyzes your codebase and generates focused AI agent context documentation recommendations

## Function Overloads

- https://github.com/noshiro-pf/ts-data-forge/blob/main/src/functional/optional/impl/optional-or-else.mts

```ts
export function orElse<O extends UnknownOptional, const O2 extends UnknownOptional>(
  optional: O,
  alternative: O2
): O | O2;

// Curried version
export function orElse<S, S2>(alternative: Optional<S2>): (optional: Optional<S>) => Optional<S> | Optional<S2>;

export function orElse<O extends UnknownOptional, const O2 extends UnknownOptional>(
  ...args: readonly [optional: O, alternative: O2] | readonly [alternative: O2]
): (O | O2) | ((optional: Optional<Unwrap<O>>) => Optional<Unwrap<O>> | O2) {
  switch (args.length) {
    case 2: {
      const [optional, alternative] = args;

      return orElseImpl(optional, alternative);
    }

    case 1: {
      // Curried version
      const [alternative] = args;

      return (optional: Optional<Unwrap<O>>) => orElseImpl(optional, alternative);
    }
  }
}
```

# TypeScript framework collection of libraries

- https://github.com/vitaly-t/iter-ops-extras

- https://github.com/sindresorhus/type-fest
  - [ ] Compare to: https://github.com/noshiro-pf/ts-type-forge

- https://github.com/noshiro-pf/ts-data-forge
  ```ts
  expectType<Admin, User>('<='); // Admin is a subtype of User
  ```

  - Extensible Result type via simple `$$tag` (put on prototype!)
    https://github.com/noshiro-pf/ts-data-forge/blob/main/src/functional/result/impl/result-ok.mts

---

Random ideas:

- [ ] Declarative diagram coding: https://d2lang.com/ (also animated) and Markdown

---

AI Coding Agent ideas:

- Port this OpenCode agents
  - https://github.com/trevor-nichols/agentrules-architect/tree/main
  - BUT ALSO process the deep-dives for nested AGENTS.md files.

- Planning agent needs to decide what packages can be reused to implement the task.
  - We need a curated list of NPM packages that have a prompt. A directory of prompts just for packages?

- Can tests include a reference to a spec file and Agents get instruction to read the spec on test failure (or workflow
  enforce in context) to decide if the test itself is wrong or it found an implementation inconsistency.

- Review agents should flag things to humans via async workflow signals. Multiple choice or Other

- TDD cycle with Test -> Review -> Develop -> Review -> Refactor -> Test -> Review

- Audit PR after TDD cycles finishes

---

- [ ] Symlink to git submodule shared AGENTS.md: https://github.com/noshiro-pf/common-agent-config/

https://github.com/jupid-tax/ai/blob/main/pitch.md

> 6.  Self-Improving AI Systems Implementing DSPy-inspired architecture where:
>
> - Prompts evolve based on performance metrics, not manual tweaking
> - Each customer interaction improves the system
> - A/B testing happens automatically at the prompt level
> - Performance data from PostHog directly influences prompt evolution

https://jupid.com/

---

Open questions:

- [ ] Decide if ctx.{ok, err, invalid, exception} methods should be removed. Result closure!!!
      `ts     import { ok, err, invalid, exception } from 'lmao/result';     return ok(result).tag(value); // will it infer the type?     `
      What about the Result type generation. Can't import from lmao... Unless it's application global..? Prefix rewrite
      happens on buffer itself!

- [ ] Can transformer apply to library code? Or should libraries pre-transform? What about Bun as transformer?

---

How is Nanosecond anchoring implemented. Can it be improved? Anchor per-thread at module init time?

What if there's hours passing between the module init and the request being handled. Can we anchor once per root or that
already happens? Are the anchors correct for concurrent promises to track duration (performance.now() delta)?

---

Can we return other ops directly and still have Result type log to the correct place?

- Requires the Result type to keep the tags in its own instance
- span() then takes result tags and applies them to the spanBuffer

The nice thing about closures is, we don't need a buffer reference in Result anymore!

Closure explicit vs implicit:

- `return ctx.ok(result, _ => _.tag(value));`
  - But that's practically the same thing
  - Also still has to go through the remapper, or doesn't matter?
- `return ctx.ok().tag()` is definitely shorter
  - Same as a closure, the variable has to be SOMEWHERE in memory
  - BUT the code is also there to write to SpanBuffer directly...
  - V8 can better optimize closure probably

```ts
class Result<ResultType extends 'ok' | 'err' | 'invalid' | 'exception', R, E, I, X> {
  public applyTags?: (ctx: Context) => void;

  constructor(
    public readonly type: ResultType,
    public readonly data: ResultData<ResultType, R, E, I, X>;
  ) {}

  get ok(): R?        { return this.type === 'ok'        ? this.data as R : undefined; }
  get err(): E?       { return this.type === 'err'       ? this.data as E : undefined; }
  get invalid(): I?   { return this.type === 'invalid'   ? this.data as I : undefined; }
  get exception(): X? { return this.type === 'exception' ? this.data as X : undefined; }

  tag() { return this; }

  with(tags: Tags) {
    const prev = this.applyTags;
    this.applyTags = (ctx) => {
      if (prev) prev(ctx);
      for (const [key, value] of Object.entries(tags)) { ctx.tag[key](value); }
    }
    return this;
  }

  userId(id: string) {
    const prev = this.applyTags;
    this.applyTags = !this.prev
      ? (ctx) => {            ctx.tag.userId(id); }
      : (ctx) => { prev(ctx); ctx.tag.userId(id); }
    return this;
  }
}
```

---

Can we class drill?

- Generate code once for the application module
  - Ok/Err/Invalid/Exception classes
  - LogSchema

- Have the class constructors be part of ctx prototype

Have app module be part of ctx.

- Then modules using it can just get their instances from that.
- Only if appModule != this, generate runtime classes.
- Prefix remap classes still need to be generated per module.
- Avoid bundle bloat: fallback to runtime codegen on incompatible schema?

We need validation and preconditions.

- if (!valid) return invalid();
- Specialized version of ctx.err(): ctx.invalid(data, message).tag(value);

---

## Lambda Ops library

```ts
const ops = defineModule().ctx<Lambda & { user: 'type' }>()
.op({
  forSQS: (ctx, event) => {
    // Try if conditional type can auto-cast event when NOT throwing exception.
    Lambda.requireSQS(event); // Will this type event?

    // Alternative
    if (!Lambda.SQS(event)) return ctx.invalid(event, 'Invalid SNS event');
    // Now typed correctly
  },

  hybridHandler: (ctx, event) => {
    switch (Labmda.eventType(event)) {
      case Lambda.SQSEvent: return ctx.forSQS(event);
      case Lambda.SNSEvent: return ctx.forSNS(event);
      case Lambda.HTTPRequest: return ctx.forHTTP(event);
      default: return ctx.invalid(event, 'Invalid event');
    }
  },
})

export forr = Lambda(ops.module, {
  SQS: 'forSQS',
  SNS: 'forSNS',
  HTTP: 'forHTTP',
});
```

---

ClickHouse example groups by package-name but there can be multiple ops in a package, and even multiple files. It would
return multiple metrics, can these be aggregated to package-wide metrics? Single op metrics would group by "message"
column.

---

The specs used to show metadata on the defineModule call, but this seems to have been removed. Add it back with note
about transformer can inject this at compile time.

And suddenly the module got a `name`. Why? We already defined packageName packagePath gitSha.

And why isn't ctx<...>(): this just returning itself with the refined Extra type?

---

What if traceContext already creates a span-start entry?

---

I just noticed your example: await span('compute', async () => { return heavyComputation(); });

What if `heavyComputation` returns a Result type, but this result type is actually bound to a deeper child buffer? The
span code would still compile...

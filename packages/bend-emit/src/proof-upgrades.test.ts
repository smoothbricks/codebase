import { expect, test } from 'bun:test';
import { spawnSync } from 'node:child_process';
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import ts from 'typescript';
import { blake3Hex } from './blake3.js';
import { run as runCli } from './cli.js';
import { checkBendFile } from './emit.js';

function withTempDir<T>(fn: (dir: string) => Promise<T> | T): Promise<T> {
  const dir = mkdtempSync(join(tmpdir(), 'bend-emit-test-'));
  return Promise.resolve(fn(dir)).finally(() => rmSync(dir, { recursive: true, force: true }));
}

function sh(cmd: string, args: string[], cwd: string, env?: NodeJS.ProcessEnv) {
  const r = spawnSync(cmd, args, { cwd, encoding: 'utf8', maxBuffer: 1 << 26, ...(env !== undefined ? { env } : {}) });
  return { code: r.status ?? -1, out: r.stdout ?? '', err: r.error?.message ?? r.stderr ?? '' };
}

/** One line of rustc's `--error-format=json` stderr. */
type RustcDiagnostic = { level: string; spans: unknown[]; message: string; rendered: string | null };

function isRustcDiagnostic(v: unknown): v is RustcDiagnostic {
  return (
    typeof v === 'object' &&
    v !== null &&
    '$message_type' in v &&
    v.$message_type === 'diagnostic' &&
    'level' in v &&
    typeof v.level === 'string' &&
    'spans' in v &&
    Array.isArray(v.spans) &&
    'message' in v &&
    typeof v.message === 'string'
  );
}

/**
 * What rustc said about the source it compiled, from its JSON stderr: every error, and every warning that points into a
 * file. A warning with no span is about the toolchain (xcrun's note while rustc finds the SDK, the "N warnings
 * emitted" tally), never the compiled Rust. A line that is no rustc diagnostic is kept, so unknown output fails closed.
 */
function rustcDiagnostics(stderr: string): string[] {
  return stderr
    .split('\n')
    .filter((line) => line !== '')
    .flatMap((line) => {
      let parsed: unknown;
      try {
        parsed = JSON.parse(line);
      } catch {
        return [line];
      }
      if (!isRustcDiagnostic(parsed)) return [line];
      const counts = parsed.level.startsWith('error') || (parsed.level === 'warning' && parsed.spans.length > 0);
      return counts ? [parsed.rendered ?? parsed.message] : [];
    });
}

/** Compiles with rustc; the claim is its exit status and its diagnostics of the compiled source. */
function rustc(args: string[], cwd: string) {
  const r = sh('rustc', ['--error-format=json', ...args], cwd);
  return { code: r.code, diagnostics: rustcDiagnostics(r.err) };
}

test('BLAKE3 matches official test vectors including multi-chunk input', () => {
  // Official BLAKE3 vectors: empty, "abc", 2048-byte (2 chunks) and 3500-byte (4 chunks) sequential bytes (i % 251)
  expect(blake3Hex('')).toBe('af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262');
  expect(blake3Hex('abc')).toBe('6437b3ac38465133ffb63b75273a8db548c558465d79db03fd359c6cd5bd9d85');
  const multi2 = new Uint8Array(2048);
  for (let i = 0; i < multi2.length; i++) multi2[i] = i % 251;
  expect(blake3Hex(multi2)).toBe('e776b6028c7cd22a4d0ba182a8bf62205d2ef576467e838ed6f2529b85fba24a');
  const multi4 = new Uint8Array(3500);
  for (let i = 0; i < multi4.length; i++) multi4[i] = i % 251;
  expect(blake3Hex(multi4)).toBe('d8176fd15bec06755b8843ceec85924544f36b42fa0468ee4723bfd1d3aa80e8');
});

// Deliverable 1: Nullary-Law Rust Corpus Bug
test('D1: nullary law in Rust module mode emits () and compiles/runs with rustc', async () => {
  await withTempDir(async (dir) => {
    const modelPath = join(dir, 'nullary.bend');
    writeFileSync(
      modelPath,
      `import Base

def one() -> Bool:
  True{}

law one_is_true:
  {one() == True{} : Bool}

def one_is_true():
  {==}
`,
    );
    const book = await checkBendFile(modelPath);
    const em = book.emit({
      target: { kind: 'rust-module', modelName: 'nullary.bend', flags: [] },
      functions: ['one'],
      laws: ['one_is_true'],
      properties: 4,
    });
    if (em.kind !== 'module') throw new Error('expected module');
    expect(em.source).not.toContain('(,)');
    expect(em.source).toContain('pub fn corpus_one_is_true() -> Vec<()>');

    writeFileSync(join(dir, 'model.rs'), em.source);
    writeFileSync(
      join(dir, 'main.rs'),
      `mod model;
fn main() {
  let corpus = model::corpus_one_is_true();
  assert_eq!(corpus.len(), 1);
  for () in corpus {
    assert!(model::law_one_is_true().0);
  }
  let stats = model::check_one_is_true_definedness();
  assert!(stats.passed());
  assert_eq!(stats.vacuous, 0);
}
`,
    );
    expect(rustc(['--edition', '2021', 'main.rs', '-o', 'main_bin'], dir)).toEqual({ code: 0, diagnostics: [] });
    const exec = sh('./main_bin', [], dir);
    expect({ code: exec.code, err: exec.err }).toEqual({ code: 0, err: '' });
  });
});

// Deliverable 2: IsTrue(p) Predicate Laws & specs/bitmosaic/laws/seek.bend
test('D2: IsTrue(p) hypotheses and claims emit and pass across Rust, Go, TS, including seek.bend', async () => {
  await withTempDir(async (dir) => {
    const seekPath = join(import.meta.dir, '../../../specs/bitmosaic/laws/seek.bend');
    const book = await checkBendFile(seekPath);
    const laws = [
      'the_member_at_the_rank_is_not_below',
      'members_before_the_rank_are_below',
      'a_rank_is_inside_or_at_the_end',
      'the_example',
    ];
    const em = book.emit({
      target: { kind: 'all', stem: 'seek' },
      laws,
      cap: 30,
      listLength: 2,
      properties: 25,
      propertyLength: 6,
    });
    if (em.kind !== 'all') throw new Error('expected all');

    writeFileSync(join(dir, 'seek.rs'), em.rust);
    writeFileSync(join(dir, 'seek.go'), em.go);
    writeFileSync(join(dir, 'seek.ts'), em.typescript);

    expect(rustc(['--edition', '2021', 'seek.rs', '-o', 'seek_rs'], dir)).toEqual({ code: 0, diagnostics: [] });
    const goBuild = sh('go', ['build', '-o', 'seek_go', 'seek.go'], dir);
    expect({ code: goBuild.code, err: goBuild.err }).toEqual({ code: 0, err: '' });

    const rsRun = sh('./seek_rs', [], dir);
    const goRun = sh('./seek_go', [], dir);
    const tsRun = sh('bun', ['seek.ts'], dir);
    expect({ rs: rsRun.code, go: goRun.code, ts: tsRun.code }).toEqual({ rs: 0, go: 0, ts: 0 });
    expect(tsRun.out).toContain('definedness the_member_at_the_rank_is_not_below');
    expect(tsRun.out).toContain('definedness the_example');
  });
});

// Deliverable 3: Bounded Arithmetic & Bitwise Intrinsics
test('D3: Nat.sub/mul/div/mod and bitwise ops verify against Bend defs and run on Rust, Go, TS', async () => {
  await withTempDir(async (dir) => {
    const modelPath = join(dir, 'arith_bits.bend');
    writeFileSync(
      modelPath,
      `import Base

def sub_s(+a: Nat, +b: Nat) -> Nat:
  match a b:
    case 0n _:
      0n
    case 1n+ap 0n:
      1n+ap
    case 1n+ap 1n+bp:
      sub_s(ap, bp)

def mul_s(+a: Nat, +b: Nat) -> Nat:
  match a:
    case 0n:
      0n
    case 1n+p:
      Nat.add(b, mul_s(p, b))

def odd(+n: Nat) -> Bool:
  match n:
    case 0n:
      False{}
    case 1n:
      True{}
    case 2n+p:
      odd(p)

def half(+n: Nat) -> Nat:
  match n:
    case 0n:
      0n
    case 1n:
      0n
    case 2n+p:
      1n+half(p)

def bit_of(+b: Bool) -> Nat:
  match b:
    case False{}:
      0n
    case True{}:
      1n

def bit_and_go(+k: Nat, +a: Nat, +b: Nat) -> Nat:
  match k:
    case 0n:
      0n
    case 1n+kp:
      Nat.add(bit_of(Bool.and(odd(a), odd(b))), Nat.mul(2n, bit_and_go(kp, half(a), half(b))))

def bit_and(+a: Nat, +b: Nat) -> Nat:
  bit_and_go(a, a, b)

def bit_or_go(+k: Nat, +a: Nat, +b: Nat) -> Nat:
  match k:
    case 0n:
      0n
    case 1n+kp:
      Nat.add(bit_of(Bool.or(odd(a), odd(b))), Nat.mul(2n, bit_or_go(kp, half(a), half(b))))

def bit_or(+a: Nat, +b: Nat) -> Nat:
  bit_or_go(Nat.add(a, b), a, b)

def bit_xor_go(+k: Nat, +a: Nat, +b: Nat) -> Nat:
  match k:
    case 0n:
      0n
    case 1n+kp:
      Nat.add(bit_of(Bool.xor(odd(a), odd(b))), Nat.mul(2n, bit_xor_go(kp, half(a), half(b))))

def bit_xor(+a: Nat, +b: Nat) -> Nat:
  bit_xor_go(Nat.add(a, b), a, b)

def shl(+a: Nat, +b: Nat) -> Nat:
  match b:
    case 0n:
      a
    case 1n+bp:
      Nat.mul(2n, shl(a, bp))

def shr(+a: Nat, +b: Nat) -> Nat:
  match b:
    case 0n:
      a
    case 1n+bp:
      half(shr(a, bp))

def popcount_go(+k: Nat, +a: Nat) -> Nat:
  match k:
    case 0n:
      0n
    case 1n+kp:
      Nat.add(bit_of(odd(a)), popcount_go(kp, half(a)))

def popcount(+a: Nat) -> Nat:
  popcount_go(a, a)

def ctz_step(+b: Bool, +rest: Nat) -> Nat:
  match b:
    case True{}:
      0n
    case False{}:
      1n+rest

def ctz_go(+k: Nat, +a: Nat) -> Nat:
  match k:
    case 0n:
      0n
    case 1n+kp:
      ctz_step(odd(a), ctz_go(kp, half(a)))

def ctz(+a: Nat) -> Nat:
  ctz_go(a, a)

law arith_identities:
  for +a: Nat
  for +b: Nat
  {Nat.sub(Nat.add(a, b), b) == a : Nat}

law sub_agrees:
  for +a: Nat
  for +b: Nat
  {sub_s(a, b) == Nat.sub(a, b) : Nat}

law mul_agrees:
  for +a: Nat
  for +b: Nat
  {mul_s(a, b) == Nat.mul(a, b) : Nat}

law divmod_zero_safe:
  for +a: Nat
  {Nat.add(Nat.div(a, 0n), Nat.mod(a, 0n)) == a : Nat}

law divmod_reconstruct:
  for +a: Nat
  for +b: Nat
  for h: {Nat.is_eq(b, 0n) == False{} : Bool}
  {Nat.add(Nat.mul(Nat.div(a, b), b), Nat.mod(a, b)) == a : Nat}

law bit_xor_self:
  for +a: Nat
  {bit_xor(a, a) == 0n : Nat}

law bit_or_and:
  for +a: Nat
  for +b: Nat
  {Nat.add(bit_and(a, b), bit_or(a, b)) == Nat.add(a, b) : Nat}

law shift_roundtrip:
  for +a: Nat
  for +b: Nat
  {shr(shl(a, b), b) == a : Nat}

law popcount_shl:
  for +a: Nat
  for +b: Nat
  {popcount(shl(a, b)) == popcount(a) : Nat}

law ctz_shl_nonzero:
  for +a: Nat
  for +b: Nat
  for h: {Nat.is_eq(a, 0n) == False{} : Bool}
  {ctz(shl(a, b)) == Nat.add(ctz(a), b) : Nat}
`,
    );
    const book = await checkBendFile(modelPath);
    const laws = [
      'arith_identities',
      'sub_agrees',
      'mul_agrees',
      'divmod_zero_safe',
      'divmod_reconstruct',
      'bit_xor_self',
      'bit_or_and',
      'shift_roundtrip',
      'popcount_shl',
      'ctz_shl_nonzero',
    ];
    const em = book.emit({
      target: { kind: 'all', stem: 'arith_bits' },
      functions: ['bit_and', 'bit_or', 'bit_xor', 'shl', 'shr', 'popcount', 'ctz', 'sub_s', 'mul_s'],
      laws,
      cap: 30,
      properties: 25,
    });
    if (em.kind !== 'all') throw new Error('expected all');

    // Verify native lowerings happened in Rust
    expect(em.rust).toContain('saturating_sub');
    expect(em.rust).toContain('checked_mul');
    expect(em.rust).toContain('count_ones');
    expect(em.rust).toContain('trailing_zeros');

    writeFileSync(join(dir, 'arith_bits.rs'), em.rust);
    writeFileSync(join(dir, 'arith_bits.go'), em.go);
    writeFileSync(join(dir, 'arith_bits.ts'), em.typescript);

    expect(rustc(['--edition', '2021', 'arith_bits.rs', '-o', 'arith_rs'], dir)).toEqual({ code: 0, diagnostics: [] });
    const goBuild = sh('go', ['build', '-o', 'arith_go', 'arith_bits.go'], dir);
    expect({ code: goBuild.code, err: goBuild.err }).toEqual({ code: 0, err: '' });

    expect(sh('./arith_rs', [], dir).code).toBe(0);
    expect(sh('./arith_go', [], dir).code).toBe(0);
    expect(sh('bun', ['arith_bits.ts'], dir).code).toBe(0);

    // Also test >32-bit bitwise accuracy in TS module mode!
    const tsMod = book.emit({
      target: { kind: 'ts-module', modelName: 'arith_bits.bend', flags: [] },
      functions: ['bit_and', 'bit_or', 'bit_xor', 'shl', 'shr', 'popcount', 'ctz'],
      laws: [],
    });
    if (tsMod.kind !== 'module') throw new Error('expected module');
    writeFileSync(join(dir, 'mod.ts'), tsMod.source);
    writeFileSync(
      join(dir, 'check48.ts'),
      `import { bit_and, bit_or, bit_xor, shl, shr, popcount, ctz } from './mod.ts';
const hi = 2 ** 40;
if (bit_or(hi, 5) !== hi + 5) process.exit(10);
if (bit_and(hi + 5, hi + 1) !== hi + 1) process.exit(11);
if (bit_xor(hi + 5, hi + 1) !== 4) process.exit(12);
if (shl(1, 40) !== hi) process.exit(13);
if (shr(hi, 40) !== 1) process.exit(14);
if (popcount(hi + 7) !== 4) process.exit(15);
if (ctz(hi) !== 40) process.exit(16);
if (ctz(0) !== 0) process.exit(17);
`,
    );
    const check48 = sh('bun', ['check48.ts'], dir);
    expect({ code: check48.code, err: check48.err }).toEqual({ code: 0, err: '' });
  });
});

// Deliverable 4: Vacuity-Rate Tracking & Definedness Companion Gate
test('D4: contradictory-hypothesis law fails closed as vacuous unless --allow-vacuous is passed', async () => {
  await withTempDir(async (dir) => {
    const modelPath = join(dir, 'vacuous.bend');
    writeFileSync(
      modelPath,
      `import Base

law impossible_precondition:
  for +x: Nat
  for h1: {x == 0n : Nat}
  for h2: {x == 1n : Nat}
  {x == 2n : Nat}
`,
    );
    const book = await checkBendFile(modelPath);
    const em = book.emit({
      target: { kind: 'all', stem: 'vacuous' },
      laws: ['impossible_precondition'],
      cap: 10,
      properties: 10,
    });
    if (em.kind !== 'all') throw new Error('expected all');

    writeFileSync(join(dir, 'vacuous.rs'), em.rust);
    writeFileSync(join(dir, 'vacuous.go'), em.go);
    writeFileSync(join(dir, 'vacuous.ts'), em.typescript);

    expect(sh('rustc', ['--edition', '2021', 'vacuous.rs', '-o', 'vac_rs'], dir).code).toBe(0);
    expect(sh('go', ['build', '-o', 'vac_go', 'vacuous.go'], dir).code).toBe(0);

    // Without --allow-vacuous: ALL targets MUST fail closed (exit 1)
    const rsFail = sh('./vac_rs', [], dir);
    const goFail = sh('./vac_go', [], dir);
    const tsFail = sh('bun', ['vacuous.ts'], dir);
    expect({ rs: rsFail.code, go: goFail.code, ts: tsFail.code }).toEqual({ rs: 1, go: 1, ts: 1 });
    expect(rsFail.out).toContain('tested=0');
    expect(goFail.out).toContain('tested=0');
    expect(tsFail.out).toContain('tested=0');

    // With --allow-vacuous: ALL targets exit 0
    expect(sh('./vac_rs', ['--allow-vacuous'], dir).code).toBe(0);
    expect(sh('./vac_go', ['--allow-vacuous'], dir).code).toBe(0);
    expect(sh('bun', ['vacuous.ts', '--allow-vacuous'], dir).code).toBe(0);

    // Module mode companion helper reports isVacuous = true
    const tsMod = book.emit({
      target: { kind: 'ts-module', modelName: 'vacuous.bend', flags: [] },
      laws: ['impossible_precondition'],
      cap: 10,
      properties: 10,
    });
    if (tsMod.kind !== 'module') throw new Error('expected module');
    writeFileSync(join(dir, 'vac_mod.ts'), tsMod.source);
    writeFileSync(
      join(dir, 'check_vac.ts'),
      `import { check_impossible_precondition_definedness } from './vac_mod.ts';
const s = check_impossible_precondition_definedness();
if (!s.isVacuous || s.passed || s.tested !== 0 || s.vacuous !== s.total || s.total <= 0) {
  console.error(s);
  process.exit(1);
}
`,
    );
    expect(sh('bun', ['check_vac.ts'], dir).code).toBe(0);
  });
});

// Deliverable 5: Generic ADT Monomorphization
test('D5: generic ADTs (Maybe<Nat>, Maybe<Bool>, Result, custom Pair2) monomorphize across Rust, Go, TS', async () => {
  await withTempDir(async (dir) => {
    const modelPath = join(dir, 'generic_adt.bend');
    writeFileSync(
      modelPath,
      `import Base

type Pair2<-A: Data, -B: Data> is Data:
  MkPair2{fst: A, snd: B}

def swap_pair(p: Pair2<Nat, Bool>) -> Pair2<Bool, Nat>:
  match p:
    case MkPair2{a, b}:
      MkPair2{b, a}

def unwrap_nat(m: +Maybe<Nat>, d: Nat) -> Nat:
  Maybe.default(&2, Nat, m, d)

def is_some_bool(m: +Maybe<Bool>) -> Bool:
  match m:
    case None{}:
      False{}
    case Some{v}:
      True{}

def result_ok_or(r: +Result<Nat, Bool>, fallback: Bool) -> Bool:
  match r:
    case Fail{e}:
      fallback
    case Done{v}:
      v

law pair_swap_roundtrip:
  for +a: Nat
  for +b: Bool
  {swap_pair(MkPair2{a, b}) == MkPair2{b, a} : Pair2<Bool, Nat>}

law maybe_nat_some:
  for +x: Nat
  {unwrap_nat(Some{x}, 0n) == x : Nat}

law maybe_bool_some:
  for +b: Bool
  {is_some_bool(Some{b}) == True{} : Bool}

law result_done_returns_value:
  for +b: Bool
  for +fb: Bool
  {result_ok_or(Done{b}, fb) == b : Bool}
`,
    );
    const book = await checkBendFile(modelPath);
    const laws = ['pair_swap_roundtrip', 'maybe_nat_some', 'maybe_bool_some', 'result_done_returns_value'];
    const em = book.emit({
      target: { kind: 'all', stem: 'generic_adt' },
      functions: ['swap_pair', 'unwrap_nat', 'is_some_bool', 'result_ok_or'],
      laws,
      cap: 20,
      properties: 20,
    });
    if (em.kind !== 'all') throw new Error('expected all');

    writeFileSync(join(dir, 'generic_adt.rs'), em.rust);
    writeFileSync(join(dir, 'generic_adt.go'), em.go);
    writeFileSync(join(dir, 'generic_adt.ts'), em.typescript);

    expect(rustc(['--edition', '2021', 'generic_adt.rs', '-o', 'gen_rs'], dir)).toEqual({ code: 0, diagnostics: [] });
    const goBuild = sh('go', ['build', '-o', 'gen_go', 'generic_adt.go'], dir);
    expect({ code: goBuild.code, err: goBuild.err }).toEqual({ code: 0, err: '' });

    expect(sh('./gen_rs', [], dir).code).toBe(0);
    expect(sh('./gen_go', [], dir).code).toBe(0);
    expect(sh('bun', ['generic_adt.ts'], dir).code).toBe(0);
  });
});

// Deliverable 6: Provenance Manifest (--manifest <path>) & De-Bruijn Law Digest
test('D6: --manifest emits deterministic JSON and alpha-renamed laws have identical de-Bruijn BLAKE3 digests', async () => {
  await withTempDir(async (dir) => {
    const modelPath = join(dir, 'alpha.bend');
    writeFileSync(
      modelPath,
      `import Base

def double(+n: Nat) -> Nat:
  Nat.add(n, n)

law alpha_one:
  for +x: Nat
  for +y: Bool
  for h: {y == True{} : Bool}
  {double(x) == Nat.add(x, x) : Nat}

def alpha_one(x, y, h):
  {==}

law alpha_two:
  for +renamed_a: Nat
  for +renamed_b: Bool
  for proof_witness: {renamed_b == True{} : Bool}
  {double(renamed_a) == Nat.add(renamed_a, renamed_a) : Nat}

law different_law:
  for +x: Nat
  {double(x) == x : Nat}
`,
    );
    const outDir = join(dir, 'out');
    const manifestPath = join(dir, 'manifest.json');
    await runCli([
      modelPath,
      outDir,
      '--laws',
      'alpha_one,alpha_two,different_law',
      '--cap',
      '10',
      '--props',
      '8',
      '--manifest',
      manifestPath,
    ]);
    const manifest = JSON.parse(readFileSync(manifestPath, 'utf8'));
    expect(manifest.modelName).toBe('alpha.bend');
    expect(typeof manifest.bendRevision).toBe('string');
    expect(manifest.bendRevision.length).toBe(40);
    expect(manifest.laws).toHaveLength(3);

    const [l1, l2, l3] = manifest.laws;
    expect(l1.name).toBe('alpha_one');
    expect(l1.proven).toBe(true);
    expect(l2.name).toBe('alpha_two');
    expect(l2.proven).toBe(false);
    // Alpha-equivalent laws MUST have the exact same 64-hex BLAKE3 digest!
    expect(l1.digest).toMatch(/^[0-9a-f]{64}$/);
    expect(l1.digest).toBe(l2.digest);
    // Different law MUST have a different digest!
    expect(l3.digest).not.toBe(l1.digest);
    expect(l1.hypsCount).toBe(1);
    expect(l1.propsCount).toBe(8);
    expect(l1.reachedFns).toContain('double');
    expect(l1.params).toEqual([
      { name: 'x', type: 'Nat' },
      { name: 'y', type: 'Bool' },
    ]);
  });
});

// Deliverable 7: Iterative Tail-Call Lowering (Rust/TS/Go)
test('D7: self-tail-recursive def lowers to loop and runs at depth 100000 without stack overflow', async () => {
  await withTempDir(async (dir) => {
    const modelPath = join(dir, 'tco.bend');
    writeFileSync(
      modelPath,
      `import Base

def sum_acc(+n: Nat, +acc: Nat) -> Nat:
  match n:
    case 0n:
      acc
    case 1n+p:
      sum_acc(p, Nat.add(acc, 1n))

law sum_acc_is_add:
  for +n: Nat
  for +acc: Nat
  {sum_acc(n, acc) == Nat.add(n, acc) : Nat}
`,
    );
    const book = await checkBendFile(modelPath);
    const rsMod = book.emit({
      target: { kind: 'rust-module', modelName: 'tco.bend', flags: [] },
      functions: ['sum_acc'],
      laws: ['sum_acc_is_add'],
    });
    const tsMod = book.emit({
      target: { kind: 'ts-module', modelName: 'tco.bend', flags: [] },
      functions: ['sum_acc'],
      laws: ['sum_acc_is_add'],
    });
    const goMod = book.emit({
      target: { kind: 'go-module', modelName: 'tco.bend', flags: [] },
      functions: ['sum_acc'],
      laws: ['sum_acc_is_add'],
    });
    if (rsMod.kind !== 'module' || tsMod.kind !== 'module' || goMod.kind !== 'module') {
      throw new Error('expected modules');
    }

    expect(rsMod.source).toContain('loop {');
    expect(tsMod.source).toContain('for (;;)');
    expect(goMod.source).toContain('for {');

    // Run at depth 100_000 in Rust, TS, and Go!
    writeFileSync(join(dir, 'model.rs'), rsMod.source);
    writeFileSync(
      join(dir, 'tco_main.rs'),
      `mod model;
fn main() {
  assert_eq!(model::sum_acc(100_000, 7), 100_007);
}
`,
    );
    expect(rustc(['--edition', '2021', 'tco_main.rs', '-o', 'tco_rs'], dir)).toEqual({ code: 0, diagnostics: [] });
    expect(sh('./tco_rs', [], dir).code).toBe(0);

    writeFileSync(join(dir, 'model.ts'), tsMod.source);
    writeFileSync(
      join(dir, 'tco_main.ts'),
      `import { sum_acc } from './model.ts';
if (sum_acc(100_000, 7) !== 100_007) process.exit(1);
`,
    );
    expect(sh('bun', ['tco_main.ts'], dir).code).toBe(0);

    writeFileSync(join(dir, 'model.go'), goMod.source);
    writeFileSync(
      join(dir, 'model_test.go'),
      `package bendmodel
import "testing"
func TestDeepTCO(t *testing.T) {
  if got := Fn_sum_acc(100000, 7); got != 100007 {
    t.Fatalf("got %d", got)
  }
}
`,
    );
    const goTest = sh('go', ['test', 'model.go', 'model_test.go'], dir);
    expect({ code: goTest.code, out: goTest.out, err: goTest.err }).toEqual({
      code: 0,
      out: expect.stringContaining('ok'),
      err: '',
    });
  });
});

// Conformance fixture regression check (Rust, Go, TS + Go callable binding_test.go)
test('Conformance fixture (conformance.bend) agrees across Rust, Go, TS and passes binding_test.go', async () => {
  await withTempDir(async (dir) => {
    const conformancePath = join(import.meta.dir, '../test/conformance.bend');
    const bindingGoPath = join(import.meta.dir, '../test/testdata/binding_test.go');
    const book = await checkBendFile(conformancePath);
    const laws = ['packets_agree', 'guarded_lists_agree', 'all_packets_are_zero'];

    // Standalone targets: Rust, Go, TS agree on every row and exit 1 on open law
    const em = book.emit({
      target: { kind: 'all', stem: 'conformance' },
      laws,
      cap: 40,
      listLength: 2,
      properties: 16,
    });
    if (em.kind !== 'all') throw new Error('expected all');
    writeFileSync(join(dir, 'conformance.rs'), em.rust);
    writeFileSync(join(dir, 'conformance.go'), em.go);
    writeFileSync(join(dir, 'conformance.ts'), em.typescript);

    expect(rustc(['--edition', '2021', 'conformance.rs', '-o', 'conf_rs'], dir)).toEqual({ code: 0, diagnostics: [] });
    expect(sh('go', ['build', '-o', 'conf_go', 'conformance.go'], dir)).toEqual({ code: 0, out: '', err: '' });

    const rsRun = sh('./conf_rs', [], dir);
    const goRun = sh('./conf_go', [], dir);
    const tsRun = sh('bun', ['conformance.ts'], dir);
    expect({ rs: rsRun.code, go: goRun.code, ts: tsRun.code }).toEqual({ rs: 1, go: 1, ts: 1 });
    const rows = (out: string) =>
      out
        .split('\n')
        .filter((l) => l.startsWith('law ') || l.startsWith('prop '))
        .map((l) => l.split(' | args ')[0]);
    expect(rows(rsRun.out)).toEqual(rows(tsRun.out));
    expect(rows(goRun.out)).toEqual(rows(tsRun.out));
    expect(tsRun.out).toContain('Packed(0,false,[])');

    // Go callable module binding test
    const goMod = book.emit({
      target: { kind: 'go-module', modelName: 'conformance.bend', flags: [] },
      laws,
      cap: 40,
      listLength: 2,
      properties: 16,
    });
    if (goMod.kind !== 'module') throw new Error('expected module');
    writeFileSync(join(dir, 'model.go'), goMod.source);
    writeFileSync(join(dir, 'binding_test.go'), readFileSync(bindingGoPath, 'utf8'));
    const goTest = sh('go', ['test', '-v', 'model.go', 'binding_test.go'], dir);
    expect({ code: goTest.code, err: goTest.err }).toEqual({ code: 0, err: '' });
    expect(goTest.out).toContain('PASS: TestBoundInputs');
    expect(goTest.out).toContain('PASS: TestNatLimit');
  });
});

// Recursive user ADT boxing (Rust Rc<T>) & depth-bounded enumeration/generation across Rust, Go, TS
test('Recursive user ADTs (Tree + Ancestor) box via Rc<T> in Rust, terminate enumeration, and pass on Rust, Go, TS', async () => {
  await withTempDir(async (dir) => {
    const modelPath = join(dir, 'rec_tree.bend');
    writeFileSync(
      modelPath,
      `import Base

type Tree is Data:
  Leaf{v: Nat}
  Branch{l: Tree, r: Tree}

type Anc is Data:
  Root{}
  Step{side: Tree, parent: Anc}

def tree_sum(+t: Tree) -> Nat:
  match t:
    case Leaf{v}:
      v
    case Branch{l, r}:
      Nat.add(tree_sum(l), tree_sum(r))

def tree_mirror(+t: Tree) -> Tree:
  match t:
    case Leaf{v}:
      Leaf{v}
    case Branch{l, r}:
      Branch{tree_mirror(r), tree_mirror(l)}

def anc_depth(+a: Anc) -> Nat:
  match a:
    case Root{}:
      0n
    case Step{side, parent}:
      1n+anc_depth(parent)

law mirror_preserves_sum:
  for +t: Tree
  {tree_sum(tree_mirror(t)) == tree_sum(t) : Nat}

law mirror_involutive:
  for +t: Tree
  {tree_mirror(tree_mirror(t)) == t : Tree}

law anc_depth_nonneg:
  for +a: Anc
  {Nat.is_le(0n, anc_depth(a)) == True{} : Bool}
`,
    );
    const book = await checkBendFile(modelPath);
    const laws = ['mirror_preserves_sum', 'mirror_involutive', 'anc_depth_nonneg'];
    const em = book.emit({
      target: { kind: 'all', stem: 'rec_tree' },
      laws,
      cap: 30,
      adtDepth: 2,
      properties: 25,
    });
    if (em.kind !== 'all') throw new Error('expected all');
    expect(em.rust).toContain('Rc<Tree>');
    expect(em.rust).toContain('Rc<Anc>');

    writeFileSync(join(dir, 'rec_tree.rs'), em.rust);
    writeFileSync(join(dir, 'rec_tree.go'), em.go);
    writeFileSync(join(dir, 'rec_tree.ts'), em.typescript);

    expect(rustc(['--edition', '2021', 'rec_tree.rs', '-o', 'rec_rs'], dir)).toEqual({ code: 0, diagnostics: [] });
    const goBuild = sh('go', ['build', '-o', 'rec_go', 'rec_tree.go'], dir);
    expect({ code: goBuild.code, err: goBuild.err }).toEqual({ code: 0, err: '' });

    const rsRun = sh('./rec_rs', [], dir);
    const goRun = sh('./rec_go', [], dir);
    const tsRun = sh('bun', ['rec_tree.ts'], dir);
    expect({ rs: rsRun.code, go: goRun.code, ts: tsRun.code }).toEqual({ rs: 0, go: 0, ts: 0 });
  });
});

/** The compiler's diagnostics for one emitted module, checked strictly in-process with nothing else in the program. */
function typeErrors(file: string): string[] {
  const program = ts.createProgram([file], {
    strict: true,
    noEmit: true,
    target: ts.ScriptTarget.ES2022,
    module: ts.ModuleKind.ESNext,
    lib: ['lib.es2022.d.ts'],
    types: [],
  });
  return ts
    .getPreEmitDiagnostics(program)
    .map((d) => `${d.start ?? 0}: ${ts.flattenDiagnosticMessageText(d.messageText, '\n')}`);
}

// A tail call lowered to a loop reassigns its parameters, so each value it passes keeps the parameter's type: a
// rebuilt `Con` and a user ADT constructor alike keep their literal tags.
test('a tail call that passes a constructed List or ADT value emits a TS module that typechecks', async () => {
  await withTempDir(async (dir) => {
    const modelPath = join(dir, 'tco_ctor.bend');
    writeFileSync(
      modelPath,
      `import Base

type Path is Data:
  Root{}
  Step{depth: Nat, parent: Path}

def last(l: +List<Nat>) -> Nat:
  match l:
    case Nil{}:
      0n
    case Con{h, t}:
      match t:
        case Nil{}:
          h
        case Con{g, u}:
          last(Con{g, u})

def climb(+n: Nat, p: Path) -> Path:
  match n:
    case 0n:
      p
    case 1n+k:
      climb(k, Step{k, p})

law last_of_one:
  for n: Nat
  {last(Con{n, Nil{}}) == n : Nat}

law climb_none:
  for p: Path
  {climb(0n, p) == p : Path}
`,
    );
    const book = await checkBendFile(modelPath);
    const em = book.emit({
      target: { kind: 'ts-module', modelName: 'tco_ctor.bend', flags: [] },
      functions: ['last', 'climb'],
      laws: ['last_of_one', 'climb_none'],
    });
    if (em.kind !== 'module') throw new Error('expected module');
    expect(em.source.match(/continue tail;/g)?.length).toBe(2);
    const file = join(dir, 'model.ts');
    writeFileSync(file, em.source);
    expect(typeErrors(file)).toEqual([]);
  });
});

// rustc warns when xcrun writes to stderr while it finds the SDK. In the sandbox the first cold xcrun call prints
// `DVTDeveloperPaths: Failed to get length of DARWIN_USER_CACHE_DIR`, which rustc reports as a warning with no source
// span: it says nothing about the emitted Rust. Both fixtures are rustc's --error-format=json stderr as captured.
test('a toolchain warning without a source span is no diagnostic of the compiled Rust', () => {
  const stderr = readFileSync(join(import.meta.dir, '../test/testdata/rustc-xcrun-cold-cache.jsonl'), 'utf8');
  expect(rustcDiagnostics(stderr)).toEqual([]);
});

test('a warning that points into the compiled Rust is a diagnostic', () => {
  const stderr = readFileSync(join(import.meta.dir, '../test/testdata/rustc-unused-variable.jsonl'), 'utf8');
  expect(rustcDiagnostics(stderr)).toEqual([expect.stringContaining('unused variable: `x`')]);
});

test('cross-directory import emits valid identifiers and compiles/passes in TS, Rust and Go', async () => {
  await withTempDir(async (dir) => {
    const lawsDir = join(dir, 'laws');
    const aLawsDir = join(dir, 'a', 'laws');
    const outDir = join(dir, 'out');
    mkdirSync(lawsDir, { recursive: true });
    mkdirSync(aLawsDir, { recursive: true });
    mkdirSync(outDir, { recursive: true });

    writeFileSync(
      join(lawsDir, 'lemmas.bend'),
      `import Base

type Found is Data:
  Hit{v: Nat}
  Miss{}

def found_if(+ok: Bool, +x: Nat) -> Found:
  match ok:
    case True{}:
      Hit{x}
    case False{}:
      Miss{}

def is_hit(+f: Found) -> Bool:
  match f:
    case Hit{v}:
      True{}
    case Miss{}:
      False{}

def mem(+x: Nat, +xs: +List<Nat>) -> Bool:
  match xs:
    case Nil{}:
      False{}
    case Con{+h, +t}:
      Bool.or(Nat.is_eq(x, h), mem(x, t))

def lookup(+x: Nat, +xs: +List<Nat>) -> Found:
  found_if(mem(x, xs), x)
`,
    );

    const rootPath = join(aLawsDir, 'root.bend');
    writeFileSync(
      rootPath,
      `import Base
import ../../laws/lemmas.bend as L

law mem_head:
  for +x: Nat
  for +xs: +List<Nat>
  {L.mem(x, Con{x, xs}) == True{} : Bool}

law lookup_head_is_hit:
  for +x: Nat
  for +xs: +List<Nat>
  {L.is_hit(L.lookup(x, Con{x, xs})) == True{} : Bool}
`,
    );

    const book = await checkBendFile(rootPath);
    const laws = ['mem_head', 'lookup_head_is_hit'];
    const em = book.emit({
      target: { kind: 'all', stem: 'cross_dir' },
      laws,
      cap: 20,
      listLength: 2,
      properties: 12,
    });
    if (em.kind !== 'all') throw new Error('expected all');

    expect(em.rust).toContain('pub fn ______laws_lemmas_mem(');
    expect(em.rust).toContain('pub enum ______laws_lemmas_Found');
    expect(em.rust).toContain('______laws_lemmas_Hit(u64)');
    expect(em.typescript).toContain('export function ______laws_lemmas_mem(');
    expect(em.goose).toContain('fn ______laws_lemmas_mem(');
    expect(em.goose).toContain('enum ______laws_lemmas_Found');
    expect(em.goose).toContain('______laws_lemmas_Hit { v: i64 }');

    writeFileSync(join(outDir, 'cross_dir.rs'), em.rust);
    writeFileSync(join(outDir, 'cross_dir.go'), em.go);
    writeFileSync(join(outDir, 'cross_dir.ts'), em.typescript);

    // TypeScript module strict typecheck + standalone driver run
    const tsMod = book.emit({
      target: { kind: 'ts-module', modelName: 'root.bend', flags: [] },
      laws,
      cap: 20,
      listLength: 2,
      properties: 12,
    });
    if (tsMod.kind !== 'module') throw new Error('expected module');
    const tsModPath = join(outDir, 'cross_dir_mod.ts');
    writeFileSync(tsModPath, tsMod.source);
    expect(typeErrors(tsModPath)).toEqual([]);

    expect(rustc(['--edition', '2021', 'cross_dir.rs', '-o', 'cross_dir_rs'], outDir)).toEqual({
      code: 0,
      diagnostics: [],
    });
    const goBuild = sh('go', ['build', '-o', 'cross_dir_go', 'cross_dir.go'], outDir);
    expect({ code: goBuild.code, err: goBuild.err }).toEqual({ code: 0, err: '' });

    const rsRun = sh('./cross_dir_rs', [], outDir);
    const goRun = sh('./cross_dir_go', [], outDir);
    const tsRun = sh('bun', ['cross_dir.ts'], outDir);
    expect({ rs: rsRun.code, go: goRun.code, ts: tsRun.code }).toEqual({ rs: 0, go: 0, ts: 0 });
  });
});

test('distinct Bend keys colliding after identifier mapping are refused for defs and ADTs', async () => {
  await withTempDir(async (dir) => {
    // 1. Def collision: `a/b:f` and `a_b:f` both map to `a_b_f`
    const fnDir = join(dir, 'fn_collision');
    mkdirSync(join(fnDir, 'a'), { recursive: true });
    writeFileSync(
      join(fnDir, 'a', 'b.bend'),
      `import Base

def f(+x: Nat) -> Nat:
  Nat.add(x, 1n)
`,
    );
    writeFileSync(
      join(fnDir, 'a_b.bend'),
      `import Base

def f(+x: Nat) -> Nat:
  Nat.add(x, 2n)
`,
    );
    const fnRoot = join(fnDir, 'root.bend');
    writeFileSync(
      fnRoot,
      `import Base
import ./a/b.bend as Slash
import ./a_b.bend as Under

law both_f:
  for +x: Nat
  {Nat.add(Slash.f(x), Under.f(x)) == Nat.add(Under.f(x), Slash.f(x)) : Nat}
`,
    );
    const fnBook = await checkBendFile(fnRoot);
    for (const target of [
      { kind: 'all', stem: 'collide_fn' } as const,
      { kind: 'ts-module', modelName: 'root.bend', flags: [] } as const,
      { kind: 'rust-module', modelName: 'root.bend', flags: [] } as const,
      { kind: 'go-module', modelName: 'root.bend', flags: [] } as const,
    ]) {
      expect(() => fnBook.emit({ target, laws: ['both_f'], cap: 10, properties: 4 })).toThrow(
        /bend-emit refuses:.*(a\/b:f.*a_b:f|a_b:f.*a\/b:f).*a_b_f/,
      );
    }

    // 2. ADT collision: `a/b:Tag` and `a_b:Tag` both map to `a_b_Tag`
    const adtDir = join(dir, 'adt_collision');
    mkdirSync(join(adtDir, 'a'), { recursive: true });
    writeFileSync(
      join(adtDir, 'a', 'b.bend'),
      `import Base

type Tag is Data:
  One{}

def tag_one(+t: Tag) -> Nat:
  match t:
    case One{}:
      1n
`,
    );
    writeFileSync(
      join(adtDir, 'a_b.bend'),
      `import Base

type Tag is Data:
  Two{}

def tag_two(+t: Tag) -> Nat:
  match t:
    case Two{}:
      2n
`,
    );
    const adtRoot = join(adtDir, 'root.bend');
    writeFileSync(
      adtRoot,
      `import Base
import ./a/b.bend as Slash
import ./a_b.bend as Under

law both_tags:
  for +s: Slash.Tag
  for +u: Under.Tag
  {Nat.add(Slash.tag_one(s), Under.tag_two(u)) == 3n : Nat}
`,
    );
    const adtBook = await checkBendFile(adtRoot);
    expect(() =>
      adtBook.emit({
        target: { kind: 'all', stem: 'collide_adt' },
        laws: ['both_tags'],
        cap: 10,
        properties: 4,
      }),
    ).toThrow(/bend-emit refuses:.*(a\/b:Tag.*a_b:Tag|a_b:Tag.*a\/b:Tag).*a_b_Tag/);
  });
});

test('hub 0x-prefixed namespace whose mapped identifier starts with a digit is refused by name', async () => {
  await withTempDir(async (dir) => {
    const libDir = join(dir, 'lib');
    const hubPkgDir = join(libDir, '0xdeadbeef');
    const outDir = join(dir, 'out');
    mkdirSync(hubPkgDir, { recursive: true });
    mkdirSync(outDir, { recursive: true });

    writeFileSync(
      join(hubPkgDir, 'mod.bend'),
      `import Base

def id_nat(+x: Nat) -> Nat:
  x
`,
    );
    const rootPath = join(dir, 'root.bend');
    writeFileSync(
      rootPath,
      `import Base
import 0xdeadbeef/mod.bend as H

law hub_id:
  for +x: Nat
  {H.id_nat(x) == x : Nat}
`,
    );

    const cli = join(import.meta.dir, '../bin/bend-emit.ts');
    const r = sh('bun', [cli, rootPath, outDir, '--laws', 'hub_id', '--cap', '4', '--props', '2'], dir, {
      ...process.env,
      BEND_LIB: libDir,
    });
    expect(r.code).not.toBe(0);
    expect(r.err).toMatch(/bend-emit refuses:.*0xdeadbeef\/mod:id_nat.*digit/);
  });
});

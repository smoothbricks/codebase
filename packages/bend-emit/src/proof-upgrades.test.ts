import { expect, test } from 'bun:test';
import { spawnSync } from 'node:child_process';
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { blake3Hex } from './blake3.js';
import { run as runCli } from './cli.js';
import { checkBendFile } from './emit.js';

function withTempDir<T>(fn: (dir: string) => Promise<T> | T): Promise<T> {
  const dir = mkdtempSync(join(tmpdir(), 'bend-emit-test-'));
  return Promise.resolve(fn(dir)).finally(() => rmSync(dir, { recursive: true, force: true }));
}

function sh(cmd: string, args: string[], cwd: string) {
  const r = spawnSync(cmd, args, { cwd, encoding: 'utf8', maxBuffer: 1 << 26 });
  return { code: r.status ?? -1, out: r.stdout ?? '', err: r.error?.message ?? r.stderr ?? '' };
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
    const compile = sh('rustc', ['--edition', '2021', 'main.rs', '-o', 'main_bin'], dir);
    expect({ code: compile.code, err: compile.err }).toEqual({ code: 0, err: '' });
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

    const rsBuild = sh('rustc', ['--edition', '2021', 'seek.rs', '-o', 'seek_rs'], dir);
    expect({ code: rsBuild.code, err: rsBuild.err }).toEqual({ code: 0, err: '' });
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

    const rsBuild = sh('rustc', ['--edition', '2021', 'arith_bits.rs', '-o', 'arith_rs'], dir);
    expect({ code: rsBuild.code, err: rsBuild.err }).toEqual({ code: 0, err: '' });
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

    const rsBuild = sh('rustc', ['--edition', '2021', 'generic_adt.rs', '-o', 'gen_rs'], dir);
    expect({ code: rsBuild.code, err: rsBuild.err }).toEqual({ code: 0, err: '' });
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
    const rsBuild = sh('rustc', ['--edition', '2021', 'tco_main.rs', '-o', 'tco_rs'], dir);
    expect({ code: rsBuild.code, err: rsBuild.err }).toEqual({ code: 0, err: '' });
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

    expect(sh('rustc', ['--edition', '2021', 'conformance.rs', '-o', 'conf_rs'], dir)).toEqual({
      code: 0,
      out: '',
      err: '',
    });
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

    const rsBuild = sh('rustc', ['--edition', '2021', 'rec_tree.rs', '-o', 'rec_rs'], dir);
    expect({ code: rsBuild.code, err: rsBuild.err }).toEqual({ code: 0, err: '' });
    const goBuild = sh('go', ['build', '-o', 'rec_go', 'rec_tree.go'], dir);
    expect({ code: goBuild.code, err: goBuild.err }).toEqual({ code: 0, err: '' });

    const rsRun = sh('./rec_rs', [], dir);
    const goRun = sh('./rec_go', [], dir);
    const tsRun = sh('bun', ['rec_tree.ts'], dir);
    expect({ rs: rsRun.code, go: goRun.code, ts: tsRun.code }).toEqual({ rs: 0, go: 0, ts: 0 });
  });
});

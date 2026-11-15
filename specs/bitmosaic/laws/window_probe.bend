# A small image's windows (Window, Window at, Window wide: packages/bitmosaic/crates/bitmosaic/README.md "Native v2",
# small forms) are bitmaps: bit `i` is the member `base + i`. The reader takes the bitmap a 64-bit word at a time.
# These laws say what a word-at-a-time reader may skip.
#
# The model: a bitmap is its bits as `Bool`s, lowest position first; a word is any prefix `w` of it, `app(w, rest)`
# the bitmap that starts with that word. `below(p, bs)` is the rank (set bits at positions under `p`), `pop` a
# population count, `sel(bs, k)` the position of the `k`-th set bit, `mem` a bit test, `band` the AND of two bitmaps
# and `drop(d, bs)` the bitmap from position `d` on.
#
# Proven here, for every bitmap and every word length:
# - whole_words_count_whole: a rank past a word is that word's population count plus the rank in the rest.
# - a_mask_counts_inside_its_word: a rank inside a word reads that word alone (its bits under the position: a mask).
#   Both LICENSE `small::count_below` (src/small.rs): whole words by population count, then one masked word, and
#   never a bit loop.
# - select_skips_whole_words, select_stays_in_its_word: the `k`-th set bit lies in the first word whose running
#   population count passes `k`, at the rank `k` less the counts before it.
#   LICENSE `small::select_bit`: a word is skipped by its count, and one word is searched.
# - rank_of_select, select_is_a_member: the position `sel` answers is a member with exactly `k` members under it, so
#   any routine that returns a set bit with `k` set bits under it is `select`.
#   LICENSE `small::select_in_word`: it is held to this pair (the test below), not to a bit loop.
# - and_holds_what_both_hold, and_goes_word_by_word, a_later_start_shifts_positions: the AND of two bitmaps holds
#   the members both hold, word by word, and a bitmap cut at a later start holds the same members at shifted
#   positions. LICENSE `small::window_and`: two windows are cut to the bytes both cover and ANDed a word at a time,
#   with no shift, because both are byte-aligned at value zero.
#
# Not modelled, and why: `select_in_word`'s byte-wise prefix counts (a 64-bit multiply by 0x0101…01 and a
# subtract-and-mask compare) and `word_at`'s overlapping loads. Bend2's `U32` is 32 `Bool`s with add, mul, shifts and
# masks, but it has no 64-bit word, no induction principle over a word's bits beyond case analysis, and `{==}`
# cannot enumerate 2^64 words; a proof of the multiply would be a proof of a ripple adder per step. The reduced
# statement proven is the pair rank_of_select / select_is_a_member over bit lists, and the Rust test holds
# `select` to it on every word the small universe produces.
#
# Check: bend2 specs/bitmosaic/laws/window_probe.bend --verdict
import Base

def add(+a: Nat, b: Nat) -> Nat:
  match a:
    case 0n:
      b
    case 1n+p:
      1n+add(p, b)

def le(+a: Nat, +b: Nat) -> Bool:
  match a b:
    case 0n b0:
      True{}
    case 1n+p 0n:
      False{}
    case 1n+p 1n+q:
      le(p, q)

def IsTrue(b: Bool) -> Data:
  match b:
    case True{}:
      Unit
    case False{}:
      Empty

def both(+a: Bool, b: Bool) -> Bool:
  match a:
    case True{}:
      b
    case False{}:
      False{}

def tally(+b: Bool, n: Nat) -> Nat:
  match b:
    case True{}:
      1n+n
    case False{}:
      n

def len(+bs: +List<Bool>) -> Nat:
  match bs:
    case Nil{}:
      0n
    case Con{b, t}:
      1n+len(t)

def app(+a: +List<Bool>, b: +List<Bool>) -> +List<Bool>:
  match a:
    case Nil{}:
      b
    case Con{h, t}:
      h <> app(t, b)

def pop(+bs: +List<Bool>) -> Nat:
  match bs:
    case Nil{}:
      0n
    case Con{b, t}:
      tally(b, pop(t))

def below(+p: Nat, +bs: +List<Bool>) -> Nat:
  match p bs:
    case 0n b0:
      0n
    case 1n+q Nil{}:
      0n
    case 1n+q Con{b, t}:
      tally(b, below(q, t))

def mem(+bs: +List<Bool>, +i: Nat) -> Bool:
  match bs i:
    case Nil{} i0:
      False{}
    case Con{b, t} 0n:
      b
    case Con{b, t} 1n+j:
      mem(t, j)

def sel(+bs: +List<Bool>, +k: Nat) -> Nat:
  match bs k:
    case Nil{} k0:
      0n
    case Con{False{}, t} k0:
      1n+sel(t, k0)
    case Con{True{}, t} 0n:
      0n
    case Con{True{}, t} 1n+j:
      1n+sel(t, j)

def tally_add(+b: Bool, +a: Nat, +c: Nat, +r: Nat) -> {a == add(c, r) : Nat} -> {tally(b, a) == add(tally(b, c), r) : Nat}:
  match b:
    case True{}:
      e =>
        %e : {1n+a == 1n+_ : Nat}
        {==}
    case False{}:
      e => e

def under(+n: Nat, +k: Nat) -> Bool:
  match n k:
    case 0n k0:
      False{}
    case 1n+m 0n:
      True{}
    case 1n+m 1n+j:
      under(m, j)

def band(+a: +List<Bool>, +b: +List<Bool>) -> +List<Bool>:
  match a b:
    case Nil{} b0:
      []
    case Con{x, t} Nil{}:
      []
    case Con{x, t} Con{y, u}:
      both(x, y) <> band(t, u)

def drop(+d: Nat, +bs: +List<Bool>) -> +List<Bool>:
  match d bs:
    case 0n b0:
      b0
    case 1n+e Nil{}:
      []
    case 1n+e Con{b, t}:
      drop(e, t)

def eqlen(+a: +List<Bool>, +b: +List<Bool>) -> Bool:
  match a b:
    case Nil{} Nil{}:
      True{}
    case Nil{} Con{y, u}:
      False{}
    case Con{x, t} Nil{}:
      False{}
    case Con{x, t} Con{y, u}:
      eqlen(t, u)

law whole_words_count_whole:
  for +w: +List<Bool>
  for +rest: +List<Bool>
  for +q: Nat
  {below(add(len(w), q), app(w, rest)) == add(pop(w), below(q, rest)) : Nat}

def whole_words_count_whole(w, rest, q):
  match w:
    case Nil{}:
      {==}
    case Con{b, u}:
      tally_add(b, below(add(len(u), q), app(u, rest)), pop(u), below(q, rest), whole_words_count_whole(u, rest, q))

def tally_same(+b: Bool, +a: Nat, +c: Nat) -> {a == c : Nat} -> {tally(b, a) == tally(b, c) : Nat}:
  match b:
    case True{}:
      e =>
        %e : {1n+a == 1n+_ : Nat}
        {==}
    case False{}:
      e => e

law a_mask_counts_inside_its_word:
  for +q: Nat
  for +w: +List<Bool>
  for +rest: +List<Bool>
  for +i: IsTrue(le(q, len(w)))
  {below(q, app(w, rest)) == below(q, w) : Nat}

def a_mask_counts_inside_its_word(q, w, rest, i):
  match q w:
    case 0n w0:
      {==}
    case 1n+p Nil{}:
      Empty.absurd({below(1n+p, app(Nil{}, rest)) == below(1n+p, Nil{}) : Nat}, i)
    case 1n+p Con{b, u}:
      tally_same(b, below(p, app(u, rest)), below(p, u), a_mask_counts_inside_its_word(p, u, rest, i))

law select_skips_whole_words:
  for +w: +List<Bool>
  for +rest: +List<Bool>
  for +j: Nat
  {sel(app(w, rest), add(pop(w), j)) == add(len(w), sel(rest, j)) : Nat}

def select_skips_whole_words(w, rest, j):
  match w:
    case Nil{}:
      {==}
    case Con{False{}, u}:
      %select_skips_whole_words(u, rest, j) : {1n+sel(app(u, rest), add(pop(u), j)) == 1n+_ : Nat}
      {==}
    case Con{True{}, u}:
      %select_skips_whole_words(u, rest, j) : {1n+sel(app(u, rest), add(pop(u), j)) == 1n+_ : Nat}
      {==}

law select_stays_in_its_word:
  for +w: +List<Bool>
  for +k: Nat
  for +rest: +List<Bool>
  for +i: IsTrue(under(pop(w), k))
  {sel(app(w, rest), k) == sel(w, k) : Nat}

def select_stays_in_its_word(w, k, rest, i):
  match w k:
    case Nil{} k0:
      Empty.absurd({sel(app(Nil{}, rest), k0) == sel(Nil{}, k0) : Nat}, i)
    case Con{False{}, u} k0:
      %select_stays_in_its_word(u, k0, rest, i) : {1n+sel(app(u, rest), k0) == 1n+_ : Nat}
      {==}
    case Con{True{}, u} 0n:
      {==}
    case Con{True{}, u} 1n+j:
      %select_stays_in_its_word(u, j, rest, i) : {1n+sel(app(u, rest), j) == 1n+_ : Nat}
      {==}

law rank_of_select:
  for +bs: +List<Bool>
  for +k: Nat
  for +i: IsTrue(under(pop(bs), k))
  {below(sel(bs, k), bs) == k : Nat}

def rank_of_select(bs, k, i):
  match bs k:
    case Nil{} k0:
      Empty.absurd({below(sel(Nil{}, k0), Nil{}) == k0 : Nat}, i)
    case Con{False{}, u} k0:
      rank_of_select(u, k0, i)
    case Con{True{}, u} 0n:
      {==}
    case Con{True{}, u} 1n+j:
      %rank_of_select(u, j, i) : {1n+below(sel(u, j), u) == 1n+_ : Nat}
      {==}

law select_is_a_member:
  for +bs: +List<Bool>
  for +k: Nat
  for +i: IsTrue(under(pop(bs), k))
  {mem(bs, sel(bs, k)) == True{} : Bool}

def select_is_a_member(bs, k, i):
  match bs k:
    case Nil{} k0:
      Empty.absurd({mem(Nil{}, sel(Nil{}, k0)) == True{} : Bool}, i)
    case Con{False{}, u} k0:
      select_is_a_member(u, k0, i)
    case Con{True{}, u} 0n:
      {==}
    case Con{True{}, u} 1n+j:
      select_is_a_member(u, j, i)

def both_false(+a: Bool) -> {False{} == both(a, False{}) : Bool}:
  match a:
    case True{}:
      {==}
    case False{}:
      {==}

law and_holds_what_both_hold:
  for +a: +List<Bool>
  for +b: +List<Bool>
  for +i: Nat
  {mem(band(a, b), i) == both(mem(a, i), mem(b, i)) : Bool}

def and_holds_what_both_hold(a, b, i):
  match a b i:
    case Nil{} b0 i0:
      {==}
    case Con{x, t} Nil{} 0n:
      both_false(x)
    case Con{x, t} Nil{} 1n+j:
      both_false(mem(t, j))
    case Con{x, t} Con{y, u} 0n:
      {==}
    case Con{x, t} Con{y, u} 1n+j:
      and_holds_what_both_hold(t, u, j)

law a_later_start_shifts_positions:
  for +d: Nat
  for +a: +List<Bool>
  for +i: Nat
  {mem(drop(d, a), i) == mem(a, add(d, i)) : Bool}

def a_later_start_shifts_positions(d, a, i):
  match d a:
    case 0n a0:
      {==}
    case 1n+e Nil{}:
      {==}
    case 1n+e Con{b, t}:
      a_later_start_shifts_positions(e, t, i)

law and_goes_word_by_word:
  for +w: +List<Bool>
  for +x: +List<Bool>
  for +r: +List<Bool>
  for +s: +List<Bool>
  for +i: IsTrue(eqlen(w, x))
  {band(app(w, r), app(x, s)) == app(band(w, x), band(r, s)) : +List<Bool>}

def and_goes_word_by_word(w, x, r, s, i):
  match w x:
    case Nil{} Nil{}:
      {==}
    case Nil{} Con{y, u}:
      Empty.absurd({band(app(Nil{}, r), app(Con{y, u}, s)) == app(band(Nil{}, Con{y, u}), band(r, s)) : +List<Bool>}, i)
    case Con{b, t} Nil{}:
      Empty.absurd({band(app(Con{b, t}, r), app(Nil{}, s)) == app(band(Con{b, t}, Nil{}), band(r, s)) : +List<Bool>}, i)
    case Con{b, t} Con{y, u}:
      %and_goes_word_by_word(t, u, r, s, i) : {both(b, y) <> band(app(t, r), app(u, s)) == both(b, y) <> _ : +List<Bool>}
      {==}

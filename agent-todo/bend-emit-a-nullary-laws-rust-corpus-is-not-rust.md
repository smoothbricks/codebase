# bend-emit writes a nullary law's Rust corpus as `(,)`

`packages/bend-emit`, Rust module mode (`--rust-module`, `kind: 'rust-module'`): for a law with no `for` binders,
`corpus_<law>()` is emitted with one case written `(,)`, which is not Rust. Standalone mode compiles; only the module
binding a production test includes is broken, so any law book with a closed law cannot be bound to Rust.

Minimal input (`nullary.bend`):

```bend
import Base

def one() -> Bool:
  True{}

law one_is_true:
  {one() == True{} : Bool}

def one_is_true():
  {==}
```

`bend-emit nullary.bend out --fns one --laws one_is_true --rust-module out/model.rs` writes:

```rust
pub fn corpus_one_is_true() -> Vec<()> {
  vec![
    (,),
  ]
}
```

and a binding `mod model; fn main() { for () in model::corpus_one_is_true() { assert!(model::law_one_is_true().0); } }`
fails: `error: expected expression, found ','` at `(,)`.

Change: emit a nullary case as `()`, the unit tuple `Vec<()>` already declares; a one-binder case keeps `(x,)`.

Red first: a bend-emit test that emits the book above in Rust module mode and compiles it with the binding above, which
then runs one case and passes. Found 2026-11-21 binding `specs/cowshed/laws/re_adopt_resume.bend`, which works around it
by stating its closed law over one fact.

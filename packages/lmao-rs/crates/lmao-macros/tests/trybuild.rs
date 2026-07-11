//! Macro error-message contract tests (trybuild). Each `compile_fail/*.rs`
//! pins the diagnostic a user sees for a schema-DSL mistake.

#[test]
fn compile_fail_cases() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/compile_fail/*.rs");
    t.pass("tests/pass/*.rs");
}

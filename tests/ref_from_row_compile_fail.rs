//! Compile-fail tests for RefFromRow derive macro.

#[test]
fn compile_fail() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/compile_fail/ref_from_row/*.rs");
}

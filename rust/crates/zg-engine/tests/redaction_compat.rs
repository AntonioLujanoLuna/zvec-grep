//! Differential redaction cases captured from the TypeScript oracle.
//!
//! `compat/redaction/cases.json` records the input/limit/output triples produced
//! by `redactErrorText` in `src/engine/errors.ts`, which stays the behavioral
//! reference during the rewrite. Keep the fixture in sync with the oracle rather
//! than with this implementation: a failing case here is a parity finding, not a
//! test to adjust.

use serde_json::Value;
use zg_engine::redaction::redact_text;

const CASES: &str = include_str!("../../../compat/redaction/cases.json");

#[test]
fn matches_recorded_oracle_cases() {
    let fixture: Value = serde_json::from_str(CASES).expect("redaction fixture must be valid JSON");
    let cases = fixture["cases"]
        .as_array()
        .expect("redaction fixture must list cases");
    assert!(!cases.is_empty(), "redaction fixture must not be empty");

    for case in cases {
        let input = case["input"].as_str().expect("case must set input");
        let max_chars = case["maxChars"]
            .as_u64()
            .expect("case must set maxChars")
            .try_into()
            .expect("maxChars must fit in usize");
        let expected = case["expected"].as_str().expect("case must set expected");
        assert_eq!(
            redact_text(input, max_chars),
            expected,
            "redact_text({input:?}, {max_chars}) must match the TypeScript oracle"
        );
    }
}

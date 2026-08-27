//! No-Docker conformance gate. It drives the compatibility engine directly
//! against the golden cp-schema-registry verdicts in
//! `tests/fixtures/compat/*_matrix.json`. There are 21 Avro cases, 88 Protobuf
//! cases, and 92 JSON cases, all captured from real cp 7.4.0. cp is the
//! authority, and this gate fails if our engine diverges from a single verdict.

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
};

use crabka_schema_registry::{compat, format::SchemaType, store::StoreState};

#[derive(serde::Deserialize)]
struct Case {
    #[serde(rename = "case")]
    name: String,
    level: String,
    writer: String,
    reader: String,
    is_compatible: bool,
}

/// Where one golden matrix lives.
///
/// Every fixture-backed suite in this crate anchors on `CARGO_MANIFEST_DIR`
/// and reads the file directly. Cargo sets that variable to an absolute
/// package path; this crate's `crate_tests` call sets it to the package's
/// runfiles-relative path, which is what a Bazel test runs from. Naming each
/// matrix also makes a missing fixture a failure rather than a silently
/// smaller gate.
fn matrix_path(file: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("compat")
        .join(file)
}

/// Load a golden matrix fixture (`avro_matrix.json` / `protobuf_matrix.json`).
fn load_matrix(path: &Path) -> Vec<Case> {
    serde_json::from_slice(
        &std::fs::read(path).unwrap_or_else(|e| panic!("read {}: {e}", path.display())),
    )
    .expect("valid matrix")
}

/// Cases where this engine deliberately answers something other than cp,
/// keyed by `(case, level)`. Empty today. An entry added here needs a comment
/// that says why cp's verdict is the wrong one to copy.
fn known_divergences() -> HashMap<(&'static str, &'static str), bool> {
    HashMap::new()
}

/// Drive every `ty` case in `file` through the engine and assert that each
/// verdict matches cp, except for any documented divergence.
fn assert_matrix_matches_cp(file: &str, ty: SchemaType) {
    let path = matrix_path(file);
    let divergences = known_divergences();
    let mut mismatches = Vec::new();
    for c in load_matrix(&path) {
        let mut snap = StoreState::default();
        snap.set_subject_compat("s", c.level.clone());
        snap.register("s", ty, &c.writer, &[], None)
            .expect("writer registers");
        let got = compat::check_against_version(&snap, "s", ty, &c.reader, &[], None)
            .expect("verdict")
            .is_compatible;
        let expected = *divergences
            .get(&(c.name.as_str(), c.level.as_str()))
            .unwrap_or(&c.is_compatible);
        if got != expected {
            mismatches.push(format!(
                "{}/{}: ours={got} cp={} (expected {expected})",
                c.name, c.level, c.is_compatible
            ));
        }
    }
    assert2::assert!(mismatches.is_empty(), "{mismatches:#?}");
}

#[test]
fn avro_engine_matches_cp_verdicts() {
    assert_matrix_matches_cp("avro_matrix.json", SchemaType::Avro);
}

#[test]
fn protobuf_engine_matches_cp_verdicts() {
    assert_matrix_matches_cp("protobuf_matrix.json", SchemaType::Protobuf);
}

#[test]
fn json_engine_matches_cp_verdicts() {
    assert_matrix_matches_cp("json_matrix.json", SchemaType::Json);
}

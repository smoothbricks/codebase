use std::collections::BTreeSet;
use std::env;
use std::fmt::Write as _;
use std::fs;
use std::path::{Path, PathBuf};

fn main() {
    let manifest_dir =
        PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR"));
    // The TypeScript SSOT now lives in the same npm package as these crates, so
    // the walk stops at the package root rather than at `packages/`. That is what
    // makes the published tarball self-sufficient: `node_modules/@smoothbricks/lmao`
    // is the package root there too, and `src/` ships alongside `crates/`.
    let package_root = manifest_dir
        .ancestors()
        .nth(2)
        .expect("lmao-core must live under <package>/crates/lmao-core");
    let schema_path = package_root.join("src/lib/schema/systemSchema.ts");
    let tuning_path = package_root.join("src/lib/capacityTuning.ts");

    println!("cargo:rerun-if-changed={}", schema_path.display());
    println!("cargo:rerun-if-changed={}", tuning_path.display());

    generate_entry_types(&schema_path);
    generate_thread_schema(&schema_path);
    generate_thread_kinds(&schema_path);
    generate_tuning_constants(&tuning_path);
}

fn generate_entry_types(path: &Path) {
    let source = fs::read_to_string(path).expect("read TypeScript entry-type SSOT");
    let table = source
        .split_once("export const ENTRY_TYPE_NAMES = [")
        .expect("ENTRY_TYPE_NAMES declaration")
        .1
        .split_once("] as const;")
        .expect("ENTRY_TYPE_NAMES terminator")
        .0;
    let names: Vec<&str> = table
        .lines()
        .filter_map(|line| line.trim().strip_prefix('\''))
        .filter_map(|line| line.split_once('\'').map(|(name, _)| name))
        .collect();
    assert_eq!(
        names.first(),
        Some(&""),
        "entry-type slot 0 must stay unused"
    );
    assert!(names.len() > 1, "entry-type table must not be empty");

    let mut output = String::from(
        "// Generated from packages/lmao/src/lib/schema/systemSchema.ts by build.rs.\n\
         #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]\n\
         #[repr(u8)]\n\
         pub enum EntryType {\n",
    );
    for (discriminant, name) in names.iter().enumerate().skip(1) {
        writeln!(output, "    {} = {discriminant},", rust_variant(name)).unwrap();
    }
    output.push_str("}\n\nimpl EntryType {\n");
    writeln!(output, "    pub const COUNT: usize = {};", names.len() - 1).unwrap();
    output.push_str("    pub const ALL: [Self; Self::COUNT] = [\n");
    for name in names.iter().skip(1) {
        writeln!(output, "        Self::{},", rust_variant(name)).unwrap();
    }
    output.push_str("    ];\n\n    pub const NAMES: [&'static str; Self::COUNT + 1] = [\n");
    for name in &names {
        writeln!(output, "        {name:?},").unwrap();
    }
    output.push_str(
        "    ];\n\n\
         \t#[inline]\n\
         \tpub const fn as_u8(self) -> u8 { self as u8 }\n\n\
         \t#[inline]\n\
         \tpub const fn name(self) -> &'static str { Self::NAMES[self as usize] }\n\n\
         \tpub const fn from_u8(value: u8) -> Option<Self> {\n\
         \t    if value == 0 || value as usize > Self::COUNT {\n\
         \t        None\n\
         \t    } else {\n\
         \t        Some(Self::ALL[value as usize - 1])\n\
         \t    }\n\
         \t}\n\
         }\n",
    );

    write_generated("entry_type.rs", output);
}

fn generate_thread_schema(path: &Path) {
    let source = fs::read_to_string(path).expect("read TypeScript system-column SSOT");
    let table = source
        .split_once("export const THREAD_SYSTEM_COLUMNS = [")
        .expect("THREAD_SYSTEM_COLUMNS declaration")
        .1
        .split_once("] as const;")
        .expect("THREAD_SYSTEM_COLUMNS terminator")
        .0;
    let mut columns = Vec::new();
    for line in table
        .lines()
        .map(str::trim)
        .filter(|line| line.starts_with("{ name:"))
    {
        let name = line
            .split_once("name: '")
            .and_then(|(_, tail)| tail.split_once('\''))
            .map(|(name, _)| name)
            .expect("system column name");
        let kind = line
            .split_once("kind: '")
            .and_then(|(_, tail)| tail.split_once('\''))
            .map(|(kind, _)| kind)
            .expect("system column kind");
        let nullable = line.contains("nullable: true");
        columns.push((name, kind, nullable));
    }
    assert!(!columns.is_empty(), "system-column table must not be empty");

    let mut output = String::from(
        "// Generated from packages/lmao/src/lib/schema/systemSchema.ts by build.rs.\n\
         #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]\n\
         pub enum SystemColumnKind {\n",
    );
    let kinds: BTreeSet<_> = columns.iter().map(|(_, kind, _)| *kind).collect();
    for kind in kinds {
        writeln!(output, "    {},", rust_system_column_kind(kind)).unwrap();
    }
    output.push_str(
        "}\n\n\
         #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]\n\
         pub struct SystemColumnMeta {\n\
             pub name: &'static str,\n\
             pub kind: SystemColumnKind,\n\
             pub nullable: bool,\n\
         }\n\n\
         pub const SYSTEM_COLUMNS: &[SystemColumnMeta] = &[\n",
    );
    for (name, kind, nullable) in &columns {
        writeln!(
            output,
            "    SystemColumnMeta {{ name: {name:?}, kind: SystemColumnKind::{}, nullable: {nullable} }},",
            rust_system_column_kind(kind)
        )
        .unwrap();
    }
    output.push_str("];\n\npub const SYSTEM_COLUMN_COUNT: usize = SYSTEM_COLUMNS.len();\n");
    write_generated("thread_schema.rs", output);
}

fn rust_system_column_kind(kind: &str) -> &'static str {
    match kind {
        "timestamp_ns" => "TimestampNanosecond",
        "dictionary_u32" => "DictionaryU32",
        "dictionary_u8" => "DictionaryU8",
        "u64" => "U64",
        "u32" => "U32",
        other => panic!("unknown system column kind {other}"),
    }
}
fn generate_thread_kinds(path: &Path) {
    let source = fs::read_to_string(path).expect("read ThreadSpanBuffer ABI kind SSOT");
    let table = source
        .split_once("export const THREAD_ATTRIBUTE_KINDS = [")
        .expect("THREAD_ATTRIBUTE_KINDS declaration")
        .1
        .split_once("] as const;")
        .expect("THREAD_ATTRIBUTE_KINDS terminator")
        .0;
    let mut kinds = Vec::new();
    for line in table
        .lines()
        .map(str::trim)
        .filter(|line| line.starts_with("{ name:"))
    {
        let name = line
            .split_once("name: '")
            .and_then(|(_, tail)| tail.split_once('\''))
            .map(|(name, _)| name)
            .expect("attribute ABI kind name");
        let discriminant = line
            .split_once("discriminant: ")
            .and_then(|(_, tail)| tail.split_once('}'))
            .and_then(|(value, _)| value.trim().parse::<u8>().ok())
            .expect("attribute ABI kind discriminant");
        kinds.push((name, discriminant));
    }
    assert!(
        !kinds.is_empty(),
        "attribute ABI kind table must not be empty"
    );

    let mut output = String::from(
        "// Generated from packages/lmao/src/lib/schema/systemSchema.ts by build.rs.\n\
         #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]\n\
         #[repr(u8)]\n\
         pub enum AttributeKind {\n",
    );
    for (name, discriminant) in &kinds {
        writeln!(output, "    {} = {discriminant},", rust_variant(name)).unwrap();
    }
    output.push_str("}\n\n");
    for (name, discriminant) in kinds {
        writeln!(
            output,
            "pub const ATTRIBUTE_KIND_{}: u8 = {discriminant};",
            name.to_uppercase()
        )
        .unwrap();
    }
    write_generated("thread_kinds.rs", output);
}

fn generate_tuning_constants(path: &Path) {
    let source = fs::read_to_string(path).expect("read TypeScript capacity-tuning SSOT");
    let min_capacity = constant(&source, "MIN_CAPACITY");
    let max_capacity = constant(&source, "MAX_CAPACITY");
    let grow_threshold = constant(&source, "GROW_THRESHOLD");
    let shrink_threshold = constant(&source, "SHRINK_THRESHOLD");
    let min_spans = constant(&source, "MIN_SPANS_FOR_TUNING");
    let output = format!(
        "// Generated from packages/lmao/src/lib/capacityTuning.ts by build.rs.\n\
         pub const MIN_CAPACITY: usize = {min_capacity};\n\
         pub const MAX_CAPACITY: usize = {max_capacity};\n\
         const GROW_THRESHOLD: f64 = {grow_threshold};\n\
         const SHRINK_THRESHOLD: f64 = {shrink_threshold};\n\
         const MIN_SPANS_SAMPLE: u64 = {min_spans};\n",
    );
    write_generated("tuning.rs", output);
}

fn constant<'a>(source: &'a str, name: &str) -> &'a str {
    let prefix = format!("const {name} = ");
    source
        .lines()
        .find_map(|line| line.trim().strip_prefix(&prefix))
        .and_then(|value| value.strip_suffix(';'))
        .unwrap_or_else(|| panic!("missing TypeScript constant {name}"))
}

fn rust_variant(name: &str) -> String {
    let mut variant = String::new();
    for word in name.split('-') {
        let mut chars = word.chars();
        if let Some(first) = chars.next() {
            variant.extend(first.to_uppercase());
            variant.extend(chars);
        }
    }
    variant
}

fn write_generated(name: &str, contents: String) {
    let out = PathBuf::from(env::var_os("OUT_DIR").expect("OUT_DIR"));
    fs::write(out.join(name), contents).expect("write generated Rust binding");
}

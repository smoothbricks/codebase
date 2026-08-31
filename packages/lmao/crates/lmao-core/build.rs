use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fmt::Write as _;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

const REVISION_ENV: &str = "LMAO_GIT_REVISION";

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
    generate_source_git(package_root, &manifest_dir);
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

fn generate_source_git(package_root: &Path, manifest_dir: &Path) {
    println!("cargo:rerun-if-env-changed={REVISION_ENV}");
    let Some(repo_dir) =
        git_output(manifest_dir, &["rev-parse", "--show-toplevel"]).map(PathBuf::from)
    else {
        write_source_git(&BTreeMap::new(), None);
        return;
    };
    track_git_state(&repo_dir);

    let requested = env::var(REVISION_ENV)
        .ok()
        .filter(|value| !value.is_empty());
    let revision = match requested {
        Some(requested) => Some(
            git_output(
                &repo_dir,
                &["rev-parse", "--verify", &format!("{requested}^{{commit}}")],
            )
            .unwrap_or_else(|| {
                panic!(
                    "{REVISION_ENV} does not name a commit in {}: {requested}",
                    repo_dir.display()
                )
            }),
        ),
        None => git_output(&repo_dir, &["rev-parse", "HEAD"]),
    };

    let mut rust_files = Vec::new();
    collect_rust_files(&package_root.join("crates"), &mut rust_files);
    rust_files.sort();
    let mut source_shas = BTreeMap::new();
    if let Some(revision) = revision.as_deref() {
        for source in rust_files {
            println!("cargo:rerun-if-changed={}", source.display());
            let Some(repo_relative) = relative_utf8(&source, &repo_dir) else {
                continue;
            };
            let Some(sha) = git_output(
                &repo_dir,
                &["rev-list", "-1", revision, "--", repo_relative],
            ) else {
                continue;
            };
            if sha.is_empty() {
                continue;
            }
            add_source_alias(&mut source_shas, &source, &repo_dir, &sha);
            add_source_alias(&mut source_shas, &source, package_root, &sha);
            add_source_alias(&mut source_shas, &source, manifest_dir, &sha);
        }
    }
    write_source_git(&source_shas, revision.as_deref());
}

fn collect_rust_files(dir: &Path, files: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(dir).unwrap_or_else(|error| panic!("read {}: {error}", dir.display()))
    {
        let path = entry
            .unwrap_or_else(|error| panic!("read entry under {}: {error}", dir.display()))
            .path();
        if path.is_dir() {
            if path.file_name().and_then(|name| name.to_str()) == Some("target") {
                continue;
            }
            collect_rust_files(&path, files);
        } else if path.extension().and_then(|extension| extension.to_str()) == Some("rs") {
            files.push(path);
        }
    }
}

fn add_source_alias(entries: &mut BTreeMap<String, String>, source: &Path, base: &Path, sha: &str) {
    if let Some(alias) = relative_utf8(source, base) {
        entries.insert(alias.to_owned(), sha.to_owned());
    }
}

fn relative_utf8<'a>(path: &'a Path, base: &Path) -> Option<&'a str> {
    path.strip_prefix(base).ok()?.to_str()
}

fn git_output(cwd: &Path, args: &[&str]) -> Option<String> {
    let output = Command::new("git")
        .current_dir(cwd)
        .args(args)
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    String::from_utf8(output.stdout)
        .ok()
        .map(|value| value.trim().to_owned())
}

fn track_git_state(repo_dir: &Path) {
    let Some(head_path) = git_path(repo_dir, "HEAD") else {
        return;
    };
    println!("cargo:rerun-if-changed={}", head_path.display());
    if let Ok(head) = fs::read_to_string(&head_path)
        && let Some(reference) = head.trim().strip_prefix("ref: ")
        && let Some(reference_path) = git_path(repo_dir, reference)
    {
        println!("cargo:rerun-if-changed={}", reference_path.display());
    }
    if let Some(packed_refs) = git_path(repo_dir, "packed-refs") {
        println!("cargo:rerun-if-changed={}", packed_refs.display());
    }
}

fn git_path(repo_dir: &Path, name: &str) -> Option<PathBuf> {
    let path = PathBuf::from(git_output(repo_dir, &["rev-parse", "--git-path", name])?);
    Some(if path.is_absolute() {
        path
    } else {
        repo_dir.join(path)
    })
}

fn write_source_git(entries: &BTreeMap<String, String>, revision: Option<&str>) {
    println!(
        "cargo:rustc-env=LMAO_GIT_REVISION={}",
        revision.unwrap_or_default()
    );
    let mut generated = String::from(
        "#[doc(hidden)]\n#[inline(always)]\npub fn source_git_sha(file: &str) -> Option<&'static str> {\n    match file {\n",
    );
    for (file, sha) in entries {
        writeln!(generated, "        {file:?} => Some({sha:?}),")
            .expect("writing to String cannot fail");
    }
    generated.push_str("        _ => None,\n    }\n}\n");
    write_generated("source_git.rs", generated);
}

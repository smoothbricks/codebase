//! `collect` binds every owner declaration to the Rust module it is written in. An API source takes
//! the module its declarations give it, following the Rust reference: out-of-line `mod`s resolve in
//! the declaring module's directory, a `#[path]` attribute names the file it loads, and an inline
//! module extends the path. A declaration's visibility narrows everything beneath it. One scratch
//! copy of the real owner sources carries each boundary, checked as it is added. The same copy then
//! gains a second source that declares a name the first already declares, which `collect` refuses by
//! name.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

use cowshed_api_gen::collect;
use cowshed_api_gen::ir::Api;
use cowshed_api_gen::ir::DeclarationVisibility::{self, Crate, Public, Super};
use cowshed_api_gen::records;

/// Distinguishes the scratch trees of tests running at once in one process.
static SCRATCHES: AtomicUsize = AtomicUsize::new(0);

const ROOT_FILE: &str = "crates/cowshed-core/src/api/mod.rs";
const OPERATIONS_FILE: &str = "crates/cowshed-core/src/api/operations.rs";
const SERVED_FILE: &str = "crates/cowshed-core/src/api/served.generated.rs";
const FIXTURE_FILE: &str = "crates/cowshed-core/src/api/collect_fixture.rs";
/// Adjacent to the fixture but declared nowhere, so the module graph does not reach it.
const UNDECLARED_FILE: &str = "crates/cowshed-core/src/api/collect_undeclared.rs";
/// Loaded by the `#[path]` declaration appended to `operations.rs`: a non-mod-rs file, so its
/// children live in `api/collect_included/`.
const INCLUDED_FILE: &str = "crates/cowshed-core/src/api/collect_included.rs";
const INCLUDED_DEEP_FILE: &str = "crates/cowshed-core/src/api/collect_included/deep.rs";
/// Privately declared by the root, with a public child under it.
const PRIVATE_FILE: &str = "crates/cowshed-core/src/api/collect_private.rs";
const PRIVATE_INNER_FILE: &str = "crates/cowshed-core/src/api/collect_private/collect_inner.rs";
/// Loaded by a `#[path]` naming a mod-rs file, so its children live in `api/collect_mod_dir/`.
const MOD_DIR_FILE: &str = "crates/cowshed-core/src/api/collect_mod_dir/mod.rs";
const LEAF_FILE: &str = "crates/cowshed-core/src/api/collect_mod_dir/leaf.rs";
/// Declared under `#[cfg(test)]`, so it is not API and must never be read into the model.
const TEST_ONLY_FILE: &str = "crates/cowshed-core/src/api/collect_test_only.rs";
const DUPLICATE_FILE: &str = "crates/cowshed-core/src/api/collect_duplicate.rs";

/// Declared by the API root: the public fixture, a private module, a test-only module, a
/// `#[path]` module naming a mod-rs file, and a root record.
const ROOT_DECLARATIONS: &str = r#"
pub mod collect_fixture;
mod collect_private;
#[cfg(test)]
mod collect_test_only;
#[path = "collect_mod_dir/mod.rs"]
pub mod collect_mod;

#[derive(Serialize)]
pub struct CollectRoot {
    pub value: u8,
}
"#;

/// A `#[path]` declaration in a non-mod-rs source, named the way `served` is declared.
const INCLUDED_DECLARATION: &str = r#"
#[path = "collect_included.rs"]
pub(super) mod collect_included;
"#;

/// A record appended to the real `served` source, which `operations.rs` path-includes.
const SERVED_DECLARATION: &str = r#"
#[derive(Serialize)]
pub struct CollectServed {
    pub value: u8,
}
"#;

const DUPLICATE_DECLARATION: &str = "\nmod collect_duplicate;\n";

/// Declarations in a nested inline module at each visibility the source can write, plus a public
/// record under a private inline module: a global export needs `pub` all the way down.
const FIXTURE: &str = r#"
#[derive(Serialize)]
pub struct CollectTop {
    pub value: u8,
}

#[derive(Serialize)]
pub(crate) struct CollectCrate {
    pub value: u8,
}

#[derive(Serialize)]
pub(in crate::api) struct CollectScoped {
    pub value: u8,
}

pub mod nested {
    #[derive(Serialize)]
    pub struct CollectNested {
        pub value: u8,
    }

    #[derive(Serialize)]
    pub(super) struct CollectSuper {
        pub value: u8,
    }

    pub mod deeper {
        #[derive(Serialize)]
        pub struct CollectDeep {
            pub value: u8,
        }
    }
}

pub(crate) mod sealed_by_crate {
    #[derive(Serialize)]
    pub struct CollectSealed {
        pub value: u8,
    }
}

mod hidden {
    #[derive(Serialize)]
    pub struct CollectHidden {
        pub value: u8,
    }
}
"#;

/// Reached through the `#[path]` declaration; its record sits under a `pub(super)` module, and it
/// declares one child that inherits that.
const INCLUDED: &str = r#"
#[derive(Serialize)]
pub struct CollectIncluded {
    pub value: u8,
}

pub mod deep;
"#;

const INCLUDED_DEEP: &str = r#"
#[derive(Serialize)]
pub struct CollectTrailing {
    pub value: u8,
}
"#;

/// Privately declared; its public child must not be a global export either.
const PRIVATE: &str = r#"
#[derive(Serialize)]
pub struct CollectPrivate {
    pub value: u8,
}

pub mod collect_inner;
"#;

const PRIVATE_INNER: &str = r#"
#[derive(Serialize)]
pub struct CollectInner {
    pub value: u8,
}
"#;

/// Loaded through a mod-rs `#[path]`; its public child is exported through public ancestors.
const MOD_DIR: &str = r#"
#[derive(Serialize)]
pub struct CollectModDir {
    pub value: u8,
}

pub mod leaf;
"#;

const LEAF: &str = r#"
#[derive(Serialize)]
pub struct CollectLeaf {
    pub value: u8,
}
"#;

const TEST_ONLY: &str = r#"
#[derive(Serialize)]
pub struct CollectTestOnly {
    pub value: u8,
}
"#;

/// Declared nowhere: it must never be read, so its record must never appear.
const UNDECLARED: &str = r#"
#[derive(Serialize)]
pub struct CollectUndeclared {
    pub value: u8,
}
"#;

/// The same declared name in the top level and in a nested module of one source: refused.
const DUPLICATE: &str = r#"
#[derive(Serialize)]
pub struct CollectShared {
    pub value: u8,
}

pub mod nested {
    #[derive(Serialize)]
    pub struct CollectShared {
        pub value: u8,
    }
}
"#;

/// A scratch copy of the owner sources `collect` reads, removed on every exit path.
struct Scratch(PathBuf);

impl Scratch {
    fn copy_of(project: &Path) -> Self {
        let root = std::env::temp_dir().join(format!(
            "cowshed-api-gen-collect-{}-{}",
            std::process::id(),
            SCRATCHES.fetch_add(1, Ordering::Relaxed)
        ));
        for crate_source in [
            "crates/cowshed-core/src",
            "crates/cowshed-gateway-types/src",
        ] {
            copy_tree(&project.join(crate_source), &root.join(crate_source));
        }
        Self(root)
    }

    /// Adds the scratch source `relative` with `contents`, creating its directories.
    fn add(&self, relative: &str, contents: &str) {
        let path = self.0.join(relative);
        fs::create_dir_all(path.parent().expect("scratch source directory"))
            .expect("scratch directory");
        fs::write(path, contents).expect("scratch source");
    }

    /// Appends `text` to the existing scratch source `relative`.
    fn append(&self, relative: &str, text: &str) {
        let path = self.0.join(relative);
        let mut source = fs::read_to_string(&path).expect("scratch source");
        source.push_str(text);
        fs::write(&path, source).expect("scratch source");
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        if let Err(error) = fs::remove_dir_all(&self.0) {
            eprintln!(
                "scratch project {} was not removed: {error}",
                self.0.display()
            );
        }
    }
}

fn copy_tree(source: &Path, destination: &Path) {
    fs::create_dir_all(destination).expect("scratch directory");
    for entry in fs::read_dir(source).expect("source directory") {
        let entry = entry.expect("source entry");
        let path = entry.path();
        let target = destination.join(entry.file_name());
        if entry.file_type().expect("entry type").is_dir() {
            copy_tree(&path, &target);
        } else {
            fs::copy(&path, &target).expect("copy source");
        }
    }
}

/// The cowshed package root of the checkout running this test.
fn project() -> PathBuf {
    let manifest = std::env::var_os("CARGO_MANIFEST_DIR")
        .expect("cargo and nextest export CARGO_MANIFEST_DIR to the test process");
    Path::new(&manifest).join("../..")
}

#[test]
fn collect_binds_each_declaration_to_its_module_and_refuses_a_duplicate_name() {
    let project = project();
    let scratch = Scratch::copy_of(&project);
    scratch.add(FIXTURE_FILE, FIXTURE);
    scratch.add(INCLUDED_FILE, INCLUDED);
    scratch.add(INCLUDED_DEEP_FILE, INCLUDED_DEEP);
    scratch.add(PRIVATE_FILE, PRIVATE);
    scratch.add(PRIVATE_INNER_FILE, PRIVATE_INNER);
    scratch.add(MOD_DIR_FILE, MOD_DIR);
    scratch.add(LEAF_FILE, LEAF);
    scratch.add(TEST_ONLY_FILE, TEST_ONLY);
    scratch.add(UNDECLARED_FILE, UNDECLARED);
    scratch.append(ROOT_FILE, ROOT_DECLARATIONS);
    scratch.append(OPERATIONS_FILE, INCLUDED_DECLARATION);
    scratch.append(SERVED_FILE, SERVED_DECLARATION);

    let model = collect(&scratch.0).expect("the owner sources with the fixture collect");

    let root = "cowshed_core::api";
    let fixture = "cowshed_core::api::collect_fixture";
    let nested = "cowshed_core::api::collect_fixture::nested";
    let deeper = "cowshed_core::api::collect_fixture::nested::deeper";
    let sealed = "cowshed_core::api::collect_fixture::sealed_by_crate";
    let hidden = "cowshed_core::api::collect_fixture::hidden";
    let included = "cowshed_core::api::operations::collect_included";
    let included_deep = "cowshed_core::api::operations::collect_included::deep";
    let served = "cowshed_core::api::operations::served";
    let private = "cowshed_core::api::collect_private";
    let private_inner = "cowshed_core::api::collect_private::collect_inner";
    let mod_dir = "cowshed_core::api::collect_mod";
    let leaf = "cowshed_core::api::collect_mod::leaf";
    let scoped = DeclarationVisibility::Restricted {
        path: "crate::api".to_owned(),
    };
    for (name, module, visibility, exported) in [
        ("CollectRoot", root, Public, true),
        ("CollectTop", fixture, Public, true),
        ("CollectCrate", fixture, Crate, false),
        ("CollectScoped", fixture, scoped, false),
        ("CollectNested", nested, Public, true),
        ("CollectSuper", nested, Super, false),
        ("CollectDeep", deeper, Public, true),
        ("CollectSealed", sealed, Public, false),
        ("CollectHidden", hidden, Public, false),
        ("CollectIncluded", included, Public, false),
        ("CollectTrailing", included_deep, Public, false),
        ("CollectServed", served, Public, false),
        ("CollectPrivate", private, Public, false),
        ("CollectInner", private_inner, Public, false),
        ("CollectModDir", mod_dir, Public, true),
        ("CollectLeaf", leaf, Public, true),
    ] {
        let record = &model.api.records[name];
        assert_eq!(record.module, module, "{name}");
        assert_eq!(record.visibility, visibility, "{name}");
        assert_eq!(record.exported, exported, "{name}");
    }
    // A source no declaration reaches is not part of the crate, and a test-only declaration is not
    // API, even though both sit beside declared sources.
    for name in ["CollectUndeclared", "CollectTestOnly"] {
        assert!(!model.api.records.contains_key(name), "{name}");
        assert!(!model.modules.contains_key(name), "{name}");
    }
    // Sources outside the API module keep the module their file defines: `api/mod.rs` is its
    // directory. `GatewayStatus` and `DaemonStatus` are different declarations, one name each.
    assert_eq!(model.modules["TraceId"], "cowshed_core::api::dto");
    assert_eq!(model.modules["ErrorCode"], "cowshed_core::error");
    assert_eq!(model.modules["GatewayStatus"], "cowshed_core::api::dto");
    assert_eq!(
        model.modules["DaemonStatus"],
        "cowshed_gateway_types::status"
    );
    // The class an exec names and the budget a refusal reports are the gateway's own
    // declarations: the exec API references them instead of restating them.
    assert_eq!(
        model.modules["JobserverClass"],
        "cowshed_gateway_types::cpu_budget"
    );
    assert_eq!(
        model.modules["CpuBudgetStatus"],
        "cowshed_gateway_types::cpu_budget"
    );
    // The modules map is derived from the records, so the two cannot disagree.
    assert_eq!(model.modules.len(), model.api.records.len());
    for (name, module) in &model.modules {
        assert_eq!(&model.api.records[name].module, module, "{name}");
    }

    scratch.add(DUPLICATE_FILE, DUPLICATE);
    scratch.append(ROOT_FILE, DUPLICATE_DECLARATION);
    let error = collect(&scratch.0)
        .err()
        .expect("a duplicate declared name is refused, not resolved first- or last-wins");
    assert!(error.contains("collect_duplicate.rs"), "{error}");
    assert!(error.contains("CollectShared"), "{error}");
}

/// A bare name declared by two sources is refused by the real parser in either order. The first
/// declaration stays in place, and a declaration after the refused one is never reached.
#[test]
fn a_bare_name_declared_by_two_sources_is_refused_by_the_real_parser() {
    let api_source = "#[derive(Serialize)] pub struct Shared { pub value: u8 }";
    let support_source = r#"
#[derive(Serialize)]
pub struct Shared { pub other: String }
#[derive(Serialize)]
pub struct Fresh { pub value: u8 }
"#;

    // API then support: the support declaration is refused and the API record retained.
    let mut api = Api::default();
    records::parse(api_source, &mut api, "cowshed_core::api::dto")
        .expect("the API declaration parses");
    let error = records::parse_support(support_source, &mut api, "cowshed_gateway_types::status")
        .expect_err("a support declaration cannot share a bare name with an API declaration");
    assert!(error.to_string().contains("Shared"), "{error}");
    assert_eq!(api.records["Shared"].module, "cowshed_core::api::dto");
    assert!(!api.records.contains_key("Fresh"));

    // Support then API: the API declaration is refused and the support record retained.
    let mut api = Api::default();
    records::parse_support(support_source, &mut api, "cowshed_gateway_types::status")
        .expect("the support declarations parse");
    let error = records::parse(api_source, &mut api, "cowshed_core::api::dto")
        .expect_err("an API declaration cannot share a bare name with a support declaration");
    assert!(error.to_string().contains("Shared"), "{error}");
    assert_eq!(
        api.records["Shared"].module,
        "cowshed_gateway_types::status"
    );

    // Support then support, distinct modules: the second source is refused and the first retained.
    let mut api = Api::default();
    records::parse_support(api_source, &mut api, "cowshed_gateway_types::status")
        .expect("the first support declaration parses");
    let error = records::parse_support(support_source, &mut api, "cowshed_core::metadata")
        .expect_err("a second support declaration cannot share a bare name with the first");
    assert!(error.to_string().contains("Shared"), "{error}");
    assert_eq!(
        api.records["Shared"].module,
        "cowshed_gateway_types::status"
    );
    assert!(!api.records.contains_key("Fresh"));
}

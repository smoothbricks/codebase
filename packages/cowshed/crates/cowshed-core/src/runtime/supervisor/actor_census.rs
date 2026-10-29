//! The census that keeps file-system calls off the supervisor's actor (specs/cowshed/
//! 11_shell.md, "Supervisor"). It parses this crate's source as the target it runs on compiles
//! it and walks every function the actor's turns reach: the methods of `SupervisorActor`, and
//! those of the production sinks whose futures the actor awaits on its own thread, through each
//! call the source names (`self.method`, a function or a type's associated function by name or
//! path). A call into `std::fs`, a `libc` call that waits on a file system, or a `Path` method
//! that stats is refused with the chain of calls that reaches it. Work handed to the blocking
//! pool (`off_actor::run`, `off_actor::drive`, the artifact store's `run`, any `spawn_blocking`)
//! is not the actor's and is not walked. A call through a trait object or on a value of another
//! type is beyond a census of names; such a value blocks only through
//! [`super::off_actor::OffActor`], whose type lends it to the blocking pool alone.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt;
use std::path::{Path, PathBuf};

use syn::punctuated::Punctuated;
use syn::visit::{self, Visit};

/// The `libc` calls that wait on a file system.
const LIBC_FILE_CALLS: &[&str] = &[
    "access",
    "chmod",
    "fchmod",
    "fchmodat",
    "fdatasync",
    "fgetattrlist",
    "flock",
    "fstat",
    "fstatat",
    "fstatfs",
    "fsync",
    "ftruncate",
    "getattrlist",
    "link",
    "linkat",
    "lstat",
    "mkdir",
    "mkdirat",
    "mkfifo",
    "mkfifoat",
    "open",
    "openat",
    "opendir",
    "pread",
    "pwrite",
    "read",
    "readdir",
    "readlink",
    "readlinkat",
    "realpath",
    "rename",
    "renameat",
    "rmdir",
    "stat",
    "statfs",
    "symlink",
    "symlinkat",
    "truncate",
    "unlink",
    "unlinkat",
    "write",
];

/// The `Path` methods that stat or read the file system.
const PATH_METHODS: &[&str] = &[
    "canonicalize",
    "exists",
    "is_dir",
    "is_file",
    "is_symlink",
    "metadata",
    "read_dir",
    "read_link",
    "symlink_metadata",
    "try_exists",
];

/// The crates a path may start with that are not this one.
const EXTERNAL: &[&str] = &[
    "alloc",
    "arc_swap",
    "async_trait",
    "bytes",
    "core",
    "cowshed_gateway_types",
    "cowshed_os",
    "libc",
    "serde",
    "serde_json",
    "sha2",
    "std",
    "tokio",
    "uuid",
];

/// The types in `runtime::supervisor` whose methods run on the actor: the actor's own, and the
/// production sinks whose futures it awaits on its thread.
const ROOT_OWNERS: &[&str] = &[
    "SupervisorActor",
    "SystemSpawnSink",
    "CommitmentPublisherHandle",
];

/// Where a function is: its module under the crate root, the type whose impl holds it, its name.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct Site {
    module: Vec<String>,
    owner: Option<String>,
    name: String,
}

impl fmt::Display for Site {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for segment in &self.module {
            write!(formatter, "{segment}::")?;
        }
        if let Some(owner) = &self.owner {
            write!(formatter, "{owner}::")?;
        }
        formatter.write_str(&self.name)
    }
}

/// One file-system call the actor reaches: the calls from a root that lead to it, and the call.
#[derive(Debug)]
struct Finding {
    chain: Vec<Site>,
    call: String,
}

impl fmt::Display for Finding {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (at, site) in self.chain.iter().enumerate() {
            if at > 0 {
                formatter.write_str(" -> ")?;
            }
            write!(formatter, "{site}")?;
        }
        write!(formatter, " calls {}", self.call)
    }
}

/// One module: the names it imports, as paths from the crate root, and its functions' bodies by
/// owner and name (several under one key when a trait impl and an inherent one share it).
#[derive(Default)]
struct Module {
    imports: BTreeMap<String, Vec<String>>,
    functions: BTreeMap<(Option<String>, String), Vec<syn::Block>>,
}

/// This crate's source, parsed a file at a time as the walk reaches it.
struct Source {
    root: PathBuf,
    /// Text that stands in for a file's, by its path under `root`.
    replaced: BTreeMap<PathBuf, String>,
    read: BTreeSet<PathBuf>,
    modules: BTreeMap<Vec<String>, Module>,
}

impl Source {
    fn new(root: PathBuf) -> Self {
        Self {
            root,
            replaced: BTreeMap::new(),
            read: BTreeSet::new(),
            modules: BTreeMap::new(),
        }
    }

    /// The same source with `file`'s text replaced by `text`.
    fn replacing(mut self, file: &Path, text: String) -> Self {
        self.replaced.insert(file.to_path_buf(), text);
        self
    }

    /// The module at `path`, parsing the file that holds it the first time it is asked for.
    fn module(&mut self, path: &[String]) -> Option<&Module> {
        if !self.modules.contains_key(path) {
            // A module inline in a file is indexed with the nearest file that holds it.
            for held in (1..=path.len()).rev() {
                let base = self.root.join(path[..held].join("/"));
                let file = [base.with_extension("rs"), base.join("mod.rs")]
                    .into_iter()
                    .find(|file| self.replaced.contains_key(self.relative(file)) || file.is_file());
                if let Some(file) = file {
                    self.load(path[..held].to_vec(), &file);
                    break;
                }
            }
        }
        self.modules.get(path)
    }

    fn relative<'a>(&self, file: &'a Path) -> &'a Path {
        file.strip_prefix(&self.root).unwrap_or(file)
    }

    fn load(&mut self, path: Vec<String>, file: &Path) {
        if !self.read.insert(file.to_path_buf()) {
            return;
        }
        let text = match self.replaced.get(self.relative(file)) {
            Some(text) => text.clone(),
            None => std::fs::read_to_string(file)
                .unwrap_or_else(|error| panic!("read {}: {error}", file.display())),
        };
        let parsed = syn::parse_file(&text)
            .unwrap_or_else(|error| panic!("parse {}: {error}", file.display()));
        self.index(path, &parsed.items);
    }

    fn index(&mut self, path: Vec<String>, items: &[syn::Item]) {
        let mut module = Module::default();
        for item in items {
            match item {
                syn::Item::Use(used) => flatten(&path, &used.tree, Vec::new(), &mut module.imports),
                syn::Item::Fn(function) if compiled(&function.attrs) => module
                    .functions
                    .entry((None, function.sig.ident.to_string()))
                    .or_default()
                    .push((*function.block).clone()),
                syn::Item::Impl(implementation) if compiled(&implementation.attrs) => {
                    let syn::Type::Path(owner) = &*implementation.self_ty else {
                        continue;
                    };
                    let Some(owner) = owner.path.segments.last() else {
                        continue;
                    };
                    for member in &implementation.items {
                        if let syn::ImplItem::Fn(function) = member
                            && compiled(&function.attrs)
                        {
                            module
                                .functions
                                .entry((
                                    Some(owner.ident.to_string()),
                                    function.sig.ident.to_string(),
                                ))
                                .or_default()
                                .push(function.block.clone());
                        }
                    }
                }
                syn::Item::Mod(inner) if compiled(&inner.attrs) => {
                    if let Some((_, items)) = &inner.content {
                        let mut inner_path = path.clone();
                        inner_path.push(inner.ident.to_string());
                        self.index(inner_path, items);
                    }
                }
                _ => {}
            }
        }
        self.modules.insert(path, module);
    }
}

/// Whether the target this runs on compiles an item with `attrs`, outside a test build.
fn compiled(attrs: &[syn::Attribute]) -> bool {
    attrs
        .iter()
        .filter(|attr| attr.path().is_ident("cfg"))
        .all(|attr| {
            attr.parse_args::<syn::Meta>()
                .is_ok_and(|meta| holds(&meta))
        })
}

/// Whether `predicate` holds for this target. A predicate the census does not know (a feature)
/// holds: it keeps the item in the census.
fn holds(predicate: &syn::Meta) -> bool {
    match predicate {
        syn::Meta::Path(flag) => !flag.is_ident("test"),
        syn::Meta::NameValue(pair) => match &pair.value {
            syn::Expr::Lit(syn::ExprLit {
                lit: syn::Lit::Str(os),
                ..
            }) if pair.path.is_ident("target_os") => os.value() == std::env::consts::OS,
            _ => true,
        },
        syn::Meta::List(list) => {
            let inner = list
                .parse_args_with(Punctuated::<syn::Meta, syn::Token![,]>::parse_terminated)
                .map(|inner| inner.into_iter().collect::<Vec<_>>())
                .unwrap_or_default();
            if list.path.is_ident("not") {
                !inner.iter().all(holds)
            } else if list.path.is_ident("any") {
                inner.iter().any(holds)
            } else {
                inner.iter().all(holds)
            }
        }
    }
}

/// Bind each name a `use` tree imports into `module` to the path it stands for.
fn flatten(
    module: &[String],
    tree: &syn::UseTree,
    prefix: Vec<String>,
    imports: &mut BTreeMap<String, Vec<String>>,
) {
    let mut bind = |local: String, mut full: Vec<String>, name: String| {
        if name != "self" {
            full.push(name);
        }
        if local != "_" {
            imports.insert(local, absolute(module, &BTreeMap::new(), &full));
        }
    };
    match tree {
        syn::UseTree::Path(path) => {
            let mut prefix = prefix;
            prefix.push(path.ident.to_string());
            flatten(module, &path.tree, prefix, imports);
        }
        syn::UseTree::Name(name) => {
            let name = name.ident.to_string();
            let local = if name == "self" {
                prefix.last().cloned().unwrap_or_default()
            } else {
                name.clone()
            };
            bind(local, prefix, name);
        }
        syn::UseTree::Rename(rename) => {
            bind(rename.rename.to_string(), prefix, rename.ident.to_string());
        }
        syn::UseTree::Glob(_) => {}
        syn::UseTree::Group(group) => {
            for tree in &group.items {
                flatten(module, tree, prefix.clone(), imports);
            }
        }
    }
}

/// `path`, named in `module`, as a path from the crate root (`crate::…`) or from another crate.
fn absolute(
    module: &[String],
    imports: &BTreeMap<String, Vec<String>>,
    path: &[String],
) -> Vec<String> {
    let here = || {
        let mut here = vec!["crate".to_owned()];
        here.extend(module.iter().cloned());
        here
    };
    let Some((head, rest)) = path.split_first() else {
        return Vec::new();
    };
    let (mut resolved, mut rest) = match head.as_str() {
        "crate" => (vec!["crate".to_owned()], rest),
        "self" => (here(), rest),
        "super" => {
            let mut base = here();
            base.pop();
            (base, rest)
        }
        name if EXTERNAL.contains(&name) => (vec![name.to_owned()], rest),
        name => match imports.get(name) {
            Some(imported) => (imported.clone(), rest),
            None => (here(), path),
        },
    };
    while let Some((head, tail)) = rest.split_first()
        && head == "super"
    {
        resolved.pop();
        rest = tail;
    }
    resolved.extend(rest.iter().cloned());
    resolved
}

/// Whether `path`, from the crate root or another crate, names a call that waits on a file
/// system.
fn waits_on_files(path: &[String]) -> bool {
    let path: Vec<&str> = path.iter().map(String::as_str).collect();
    path.starts_with(&["std", "fs"])
        || path.starts_with(&["std", "os", "unix", "fs"])
        || matches!(path.as_slice(), ["libc", call] if LIBC_FILE_CALLS.contains(call))
}

/// Whether a call of `path` hands its arguments to the blocking pool.
fn off_actor(path: &[String]) -> bool {
    let path: Vec<&str> = path.iter().map(String::as_str).collect();
    path.ends_with(&["off_actor", "run"])
        || path.ends_with(&["off_actor", "drive"])
        || path.ends_with(&["thread", "spawn"])
        || path.last() == Some(&"spawn_blocking")
}

fn is_self(expression: &syn::Expr) -> bool {
    matches!(expression, syn::Expr::Path(path) if path.path.is_ident("self"))
}

/// What one function body names: the file-system calls it makes, and the functions it calls,
/// each as the sites it may be.
struct Calls<'a> {
    module: &'a [String],
    owner: Option<&'a str>,
    imports: &'a BTreeMap<String, Vec<String>>,
    waits: Vec<String>,
    callees: Vec<Vec<Site>>,
}

impl Calls<'_> {
    fn site(&self, owner: Option<String>, name: String) -> Site {
        Site {
            module: self.module.to_vec(),
            owner,
            name,
        }
    }

    fn named(&mut self, segments: &[String]) {
        if let [head, name] = segments
            && head == "Self"
        {
            let site = self.site(self.owner.map(str::to_owned), name.clone());
            self.callees.push(vec![site]);
            return;
        }
        let path = absolute(self.module, self.imports, segments);
        if waits_on_files(&path) {
            self.waits.push(path.join("::"));
            return;
        }
        let Some((root, inner)) = path.split_first() else {
            return;
        };
        if root != "crate" {
            return;
        }
        let Some((name, module)) = inner.split_last() else {
            return;
        };
        let mut sites = vec![Site {
            module: module.to_vec(),
            owner: None,
            name: name.clone(),
        }];
        if let Some((owner, module)) = module.split_last() {
            sites.push(Site {
                module: module.to_vec(),
                owner: Some(owner.clone()),
                name: name.clone(),
            });
        }
        self.callees.push(sites);
    }
}

impl<'ast> Visit<'ast> for Calls<'_> {
    fn visit_expr_call(&mut self, call: &'ast syn::ExprCall) {
        if let syn::Expr::Path(function) = &*call.func {
            let segments: Vec<String> = function
                .path
                .segments
                .iter()
                .map(|segment| segment.ident.to_string())
                .collect();
            if off_actor(&segments) {
                return;
            }
            self.named(&segments);
        }
        visit::visit_expr_call(self, call);
    }

    fn visit_expr_method_call(&mut self, call: &'ast syn::ExprMethodCall) {
        let method = call.method.to_string();
        // The actor's artifact store is lent to the blocking pool by its `run`.
        let lent = method == "run"
            && matches!(&*call.receiver, syn::Expr::Field(field)
                if is_self(&field.base)
                    && matches!(&field.member, syn::Member::Named(name) if name == "artifacts"));
        if method == "spawn_blocking" || lent {
            self.visit_expr(&call.receiver);
            return;
        }
        if PATH_METHODS.contains(&method.as_str()) {
            self.waits.push(format!(".{method}()"));
        }
        if is_self(&call.receiver) {
            let site = self.site(self.owner.map(str::to_owned), method);
            self.callees.push(vec![site]);
        }
        visit::visit_expr_method_call(self, call);
    }
}

/// Every file-system call the actor's turns reach in `source`.
fn census(source: &mut Source) -> Vec<Finding> {
    let actor = vec!["runtime".to_owned(), "supervisor".to_owned()];
    let roots: Vec<Site> = source
        .module(&actor)
        .expect("the supervisor's module")
        .functions
        .keys()
        .filter(|(owner, name)| {
            owner.as_deref().is_some_and(|owner| ROOT_OWNERS.contains(&owner))
                // The actor's drop is its runtime's teardown, which hands every job to a
                // retirement thread and is bounded by its own join.
                && name != "drop"
        })
        .map(|(owner, name)| Site {
            module: actor.clone(),
            owner: owner.clone(),
            name: name.clone(),
        })
        .collect();
    let mut reached: BTreeMap<Site, Option<Site>> =
        roots.iter().map(|root| (root.clone(), None)).collect();
    let mut queue: VecDeque<Site> = roots.into_iter().collect();
    let mut findings = Vec::new();
    while let Some(site) = queue.pop_front() {
        let Some(module) = source.module(&site.module) else {
            continue;
        };
        let Some(bodies) = module
            .functions
            .get(&(site.owner.clone(), site.name.clone()))
        else {
            continue;
        };
        let mut calls = Calls {
            module: &site.module,
            owner: site.owner.as_deref(),
            imports: &module.imports,
            waits: Vec::new(),
            callees: Vec::new(),
        };
        for body in bodies {
            calls.visit_block(body);
        }
        let (waits, callees) = (calls.waits, calls.callees);
        if !waits.is_empty() {
            let mut chain = vec![site.clone()];
            while let Some(Some(parent)) = chain.last().and_then(|at| reached.get(at)) {
                chain.push(parent.clone());
            }
            chain.reverse();
            for call in waits {
                findings.push(Finding {
                    chain: chain.clone(),
                    call,
                });
            }
        }
        for sites in callees {
            for callee in sites {
                if reached.contains_key(&callee) {
                    continue;
                }
                let known = source.module(&callee.module).is_some_and(|module| {
                    module
                        .functions
                        .contains_key(&(callee.owner.clone(), callee.name.clone()))
                });
                if known {
                    reached.insert(callee.clone(), Some(site.clone()));
                    queue.push_back(callee);
                }
            }
        }
    }
    findings
}

#[cfg(test)]
mod tests {
    use super::*;

    /// This crate's `src`, as the test process finds it.
    fn src() -> PathBuf {
        let manifest = std::env::var_os("CARGO_MANIFEST_DIR")
            .expect("cargo names the crate's manifest directory to its tests");
        PathBuf::from(manifest).join("src")
    }

    /// The actor reaches no file-system call, and one put back where the observed stalls were
    /// (a build volume's `stat` in `admit_exec`) is named with the chain that reaches it.
    #[test]
    fn the_supervisor_actor_reaches_no_file_system_call() {
        let findings = census(&mut Source::new(src()));
        assert!(
            findings.is_empty(),
            "the supervisor's actor reaches file-system calls; run them on the blocking pool \
             (runtime/supervisor/off_actor.rs):\n{}",
            findings
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join("\n")
        );

        let supervisor = Path::new("runtime/supervisor.rs");
        let text = std::fs::read_to_string(src().join(supervisor)).expect("the supervisor");
        let signature = "    ) -> Result<ExecAdmission> {\n";
        assert_eq!(
            text.matches(signature).count(),
            1,
            "admit_exec's signature line"
        );
        let restat =
            format!("{signature}        let _ = std::fs::metadata(&self.workspace_root);\n");
        let findings = census(
            &mut Source::new(src()).replacing(supervisor, text.replacen(signature, &restat, 1)),
        );
        let named: Vec<String> = findings.iter().map(ToString::to_string).collect();
        assert!(
            named.iter().any(|finding| finding.ends_with(
                "runtime::supervisor::SupervisorActor::admit_exec calls std::fs::metadata"
            )),
            "{named:#?}"
        );
    }
}

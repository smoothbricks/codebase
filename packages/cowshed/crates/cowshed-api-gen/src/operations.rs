//! The controller's operation table, read from the one `operations!` invocation that declares it
//! (`cowshed-core/src/api/operations.rs`). Each row parses exactly as the macro matches it:
//! `scope lane "method" Marker(Request) -> Result;`, `lane` being `json`, `upload` or
//! `download(offset_field)`.

use syn::parse::{Parse, ParseStream};
use syn::{Attribute, Ident, Item, LitStr, Token, Type, parenthesized};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Scope {
    Coordinator,
    Worker,
    Internal,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Lane {
    Json,
    Upload,
    /// The request field the raw-byte frame starts at.
    Download(String),
}

pub struct Operation {
    pub docs: Vec<String>,
    pub scope: Scope,
    pub lane: Lane,
    pub method: String,
    pub marker: String,
    pub request: Type,
    pub result: Type,
}

impl Operation {
    /// The method's namespace: `job` for `job.logs`.
    pub fn prefix(&self) -> &str {
        self.method
            .split_once('.')
            .map_or(self.method.as_str(), |(prefix, _)| prefix)
    }

    /// The method's name within its namespace: `logs` for `job.logs`.
    pub fn name(&self) -> &str {
        self.method
            .split_once('.')
            .map_or(self.method.as_str(), |(_, name)| name)
    }
}

/// Every row of the file's top-level `operations!` invocation, in declaration order. A file that
/// invokes it at top level other than exactly once declares no table.
pub fn parse(source: &str) -> syn::Result<Vec<Operation>> {
    let file = syn::parse_file(source)?;
    let mut tables = file.items.into_iter().filter_map(|item| match item {
        Item::Macro(item) if item.mac.path.is_ident("operations") => Some(item),
        _ => None,
    });
    let table = tables.next().ok_or_else(|| {
        syn::Error::new(
            proc_macro2::Span::call_site(),
            "no top-level operations! table",
        )
    })?;
    if let Some(second) = tables.next() {
        return Err(syn::Error::new_spanned(
            second.mac.path,
            "a second top-level operations! table",
        ));
    }
    Ok(table.mac.parse_body::<Table>()?.0)
}

struct Table(Vec<Operation>);

impl Parse for Table {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let mut operations = Vec::new();
        while !input.is_empty() {
            operations.push(row(input)?);
        }
        Ok(Self(operations))
    }
}

fn row(input: ParseStream<'_>) -> syn::Result<Operation> {
    let attributes = input.call(Attribute::parse_outer)?;
    let docs = attributes
        .iter()
        .map(|attribute| match &attribute.meta {
            syn::Meta::NameValue(doc) if doc.path.is_ident("doc") => match &doc.value {
                syn::Expr::Lit(syn::ExprLit {
                    lit: syn::Lit::Str(text),
                    ..
                }) => Ok(text.value().trim().to_owned()),
                value => Err(syn::Error::new_spanned(value, "a doc row is a string")),
            },
            _ => Err(syn::Error::new_spanned(
                attribute,
                "an operation row carries doc comments only",
            )),
        })
        .collect::<syn::Result<Vec<_>>>()?;
    if docs.is_empty() {
        return Err(input.error("every operation row is documented"));
    }
    let scope: Ident = input.parse()?;
    let scope = match scope.to_string().as_str() {
        "coordinator" => Scope::Coordinator,
        "worker" => Scope::Worker,
        "internal" => Scope::Internal,
        _ => return Err(syn::Error::new_spanned(scope, "unknown operation scope")),
    };
    let lane: Ident = input.parse()?;
    let lane = match lane.to_string().as_str() {
        "json" => Lane::Json,
        "upload" => Lane::Upload,
        "download" => {
            let offset;
            parenthesized!(offset in input);
            let field: Ident = offset.parse()?;
            Lane::Download(field.to_string())
        }
        _ => return Err(syn::Error::new_spanned(lane, "unknown operation lane")),
    };
    let method: LitStr = input.parse()?;
    let marker: Ident = input.parse()?;
    let request;
    parenthesized!(request in input);
    let request: Type = request.parse()?;
    input.parse::<Token![->]>()?;
    let result: Type = input.parse()?;
    input.parse::<Token![;]>()?;
    Ok(Operation {
        docs,
        scope,
        lane,
        method: method.value(),
        marker: marker.to_string(),
        request,
        result,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rows_parse_as_the_macro_matches_them() {
        let operations = parse(
            r#"
            operations! {
                /// Reads one stream's bytes from an offset.
                worker download(offset) "job.logs" JobLogs(LogsRequest) -> LogsChunk;
                /// Lists jobs.
                worker json "worker.listJobs" WorkerListJobs(WorkerScope) -> Vec<JobInfo>;
            }
            "#,
        )
        .expect("table");
        assert_eq!(operations.len(), 2);
        assert_eq!(operations[0].lane, Lane::Download("offset".to_owned()));
        assert_eq!(operations[0].prefix(), "job");
        assert_eq!(operations[0].name(), "logs");
        assert_eq!(operations[0].marker, "JobLogs");
        assert_eq!(operations[1].scope, Scope::Worker);
        assert_eq!(operations[1].docs, ["Lists jobs."]);
    }

    #[test]
    fn an_undocumented_or_unknown_row_is_refused() {
        assert!(parse(r#"operations! { worker json "a.b" A(B) -> C; }"#).is_err());
        assert!(
            parse(
                r#"operations! { /// d
            anyone json "a.b" A(B) -> C; }"#
            )
            .is_err()
        );
    }
}

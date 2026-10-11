use quote::ToTokens;
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};

/// The wire-name segment of one already-normalized qualified Rust path.
pub fn last_segment(path: &str) -> &str {
    path.rsplit_once("::").map_or(path, |(_, last)| last)
}

#[derive(Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Api {
    pub records: BTreeMap<String, Record>,
    /// Canonical records named as Option roots by public operations.
    pub nullable_results: BTreeSet<String>,
}

/// Actual declarations plus their source module bindings, without a mirrored record schema.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DeclarationModel {
    pub api: Api,
    pub modules: BTreeMap<String, String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Record {
    pub name: String,
    /// Lexical module that owns the declaration, including inline module components.
    pub module: String,
    pub visibility: DeclarationVisibility,
    pub docs: Vec<String>,
    pub serde: Serde,
    pub projection: Projection,
    pub shape: Shape,
    pub exported: bool,
}

#[derive(Serialize, Debug, Clone, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum DeclarationVisibility {
    Public,
    Crate,
    Super,
    Private,
    Restricted { path: String },
}

#[derive(Serialize)]
#[serde(tag = "kind", content = "data", rename_all = "camelCase")]
pub enum Shape {
    Struct(Fields),
    Enum(Vec<Variant>),
    Alias(RustType),
}

#[derive(Serialize)]
#[serde(tag = "kind", content = "data", rename_all = "camelCase")]
pub enum Fields {
    Unit,
    Named(Vec<Field>),
    Tuple(Vec<Field>),
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Field {
    pub name: Option<String>,
    pub ty: RustType,
    pub docs: Vec<String>,
    pub serde: Serde,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Variant {
    pub name: String,
    pub docs: Vec<String>,
    pub serde: Serde,
    pub fields: Fields,
}

/// Facts from serde attributes, without guessing whether a record is an input or an output.
#[derive(Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Serde {
    pub rename: Option<String>,
    pub rename_all: Option<String>,
    pub rename_all_fields: Option<String>,
    pub tag: Option<String>,
    pub content: Option<String>,
    pub untagged: bool,
    pub transparent: bool,
    pub default: bool,
    /// The function a `default = "…"` names; `None` for a bare `default`, which is `Default`.
    pub default_with: Option<String>,
    pub skip_serializing_if: Option<String>,
    pub skip: bool,
    pub flatten: bool,
    pub deny_unknown_fields: bool,
    pub into: Option<String>,
    pub from: Option<String>,
    pub deserialize_with: Option<String>,
}

/// Metadata for custom serializers. Kept on the canonical Rust declaration, never in an adapter.
#[derive(Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Projection {
    pub wire: Option<String>,
    pub scalar: Option<String>,
    pub exclusive: Vec<String>,
    pub name: Option<String>,
    pub unit_value: Option<String>,
    pub output_only: bool,
}

/// The source-owned normalized Rust type graph used by every declaration projection.
///
/// Names retain their path and arguments retain type, lifetime and const information.
/// Constant expressions retain their normalized token spelling; consumers that need a
/// literal bound must refuse a symbolic expression rather than invent its value.
#[derive(Serialize, Debug, Clone, PartialEq, Eq)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum RustType {
    Named {
        name: String,
        arguments: Vec<TypeArgument>,
    },
    Array {
        element: Box<RustType>,
        length: String,
    },
    Tuple {
        elements: Vec<RustType>,
    },
    Reference {
        element: Box<RustType>,
        lifetime: Option<String>,
        mutable: bool,
    },
    Slice {
        element: Box<RustType>,
    },
}

/// Arguments preserve their source category; no source fact is erased by serialization.
#[derive(Serialize, Debug, Clone, PartialEq, Eq)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum TypeArgument {
    Type { value: RustType },
    Lifetime { name: String },
    Const { expression: String },
}

impl RustType {
    /// Parse and normalize one declared Rust type, naming unsupported source syntax.
    pub fn parse(source: &str) -> Result<Self, String> {
        Self::from_syn(syn::parse_str(source).map_err(|error| format!("{source}: {error}"))?)
    }

    /// Normalize once at the parser boundary; generators and serializers share this graph.
    pub fn from_syn(value: syn::Type) -> Result<Self, String> {
        match value {
            syn::Type::Path(path) if path.qself.is_none() => {
                let mut name = if path.path.leading_colon.is_some() {
                    "::".to_owned()
                } else {
                    String::new()
                };
                let count = path.path.segments.len();
                let mut arguments = Vec::new();
                for (index, segment) in path.path.segments.into_iter().enumerate() {
                    if index != 0 {
                        name.push_str("::");
                    }
                    name.push_str(&segment.ident.to_string());
                    if index + 1 != count && !matches!(segment.arguments, syn::PathArguments::None)
                    {
                        return Err(format!(
                            "{name}: generic arguments on an intermediate path segment are unsupported"
                        ));
                    }
                    match segment.arguments {
                        syn::PathArguments::None => {}
                        syn::PathArguments::AngleBracketed(items) => {
                            for argument in items.args {
                                arguments.push(match argument {
                                    syn::GenericArgument::Type(value) => TypeArgument::Type {
                                        value: Self::from_syn(value)?,
                                    },
                                    syn::GenericArgument::Lifetime(value) => {
                                        TypeArgument::Lifetime {
                                            name: value.ident.to_string(),
                                        }
                                    }
                                    syn::GenericArgument::Const(value) => TypeArgument::Const {
                                        expression: value.to_token_stream().to_string(),
                                    },
                                    value => {
                                        return Err(format!(
                                            "{name}: unsupported generic argument {}",
                                            value.to_token_stream()
                                        ));
                                    }
                                });
                            }
                        }
                        syn::PathArguments::Parenthesized(_) => {
                            return Err(format!("{name}: function type arguments are unsupported"));
                        }
                    }
                }
                if name.is_empty() {
                    return Err("a named Rust type has an empty path".into());
                }
                Ok(Self::Named { name, arguments })
            }
            syn::Type::Array(value) => Ok(Self::Array {
                element: Box::new(Self::from_syn(*value.elem)?),
                length: value.len.to_token_stream().to_string(),
            }),
            syn::Type::Tuple(value) => Ok(Self::Tuple {
                elements: value
                    .elems
                    .into_iter()
                    .map(Self::from_syn)
                    .collect::<Result<_, _>>()?,
            }),
            syn::Type::Reference(value) => Ok(Self::Reference {
                element: Box::new(Self::from_syn(*value.elem)?),
                lifetime: value.lifetime.map(|value| value.ident.to_string()),
                mutable: value.mutability.is_some(),
            }),
            syn::Type::Slice(value) => Ok(Self::Slice {
                element: Box::new(Self::from_syn(*value.elem)?),
            }),
            syn::Type::Paren(value) => Self::from_syn(*value.elem),
            syn::Type::Group(value) => Self::from_syn(*value.elem),
            value => Err(format!(
                "unsupported Rust declaration type {}",
                value.to_token_stream()
            )),
        }
    }
}

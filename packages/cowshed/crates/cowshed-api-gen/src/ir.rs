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

impl Api {
    /// Publish static native sizes after collection, including forward and cross-file references.
    pub fn refresh_layouts(&mut self) {
        let sizes: Vec<_> = self
            .records
            .values()
            .map(|record| record_layout(record, self, 0).map(|layout| layout.size))
            .collect();
        for (record, size) in self.records.values_mut().zip(sizes) {
            record.static_size = size;
        }
    }
}

#[derive(Clone, Copy)]
struct Layout {
    size: u32,
    align: u32,
    spare_tags: u64,
}

fn aggregate_layout<'a>(
    types: impl Iterator<Item = &'a RustType>,
    api: &Api,
    depth: usize,
) -> Option<Layout> {
    let mut size = 0_u32;
    let mut align = 1_u32;
    let mut single = None;
    let mut count = 0_u32;
    for ty in types {
        let layout = type_layout(ty, api, depth)?;
        size = size.checked_add(layout.size)?;
        align = align.max(layout.align);
        count += 1;
        single = Some(layout);
    }
    // Natural Rust records group fields by alignment. Sizes are multiples of each field's
    // power-of-two alignment, so only trailing padding remains after that grouping.
    let size = size.checked_add(align - 1)? & !(align - 1);
    Some(Layout {
        size,
        align,
        spare_tags: if count == 1 { single?.spare_tags } else { 0 },
    })
}

fn fields_layout(fields: &Fields, api: &Api, depth: usize) -> Option<Layout> {
    match fields {
        Fields::Unit => Some(Layout {
            size: 0,
            align: 1,
            spare_tags: 0,
        }),
        Fields::Named(fields) | Fields::Tuple(fields) => {
            aggregate_layout(fields.iter().map(|field| &field.ty), api, depth)
        }
    }
}

fn enum_layout(variants: &[Variant], api: &Api, depth: usize) -> Option<Layout> {
    let count = u64::try_from(variants.len()).ok()?;
    let tag: u32 = match count {
        0 => return None,
        1 => return fields_layout(&variants.first()?.fields, api, depth),
        2..=256 => 1,
        257..=65_536 => 2,
        65_537..=4_294_967_296 => 4,
        _ => return None,
    };
    let mut size = 0;
    let mut align = tag;
    let mut data_arms = 0;
    let mut empty_arms = 0;
    let mut payload = None;
    for variant in variants {
        let layout = fields_layout(&variant.fields, api, depth)?;
        if layout.size == 0 {
            empty_arms += 1;
        } else {
            data_arms += 1;
            payload = Some(layout);
        }
        size = size.max(layout.size);
        align = align.max(layout.align);
    }
    if data_arms == 1 {
        let payload = payload?;
        if payload.spare_tags >= empty_arms {
            return Some(Layout {
                spare_tags: payload.spare_tags - empty_arms,
                ..payload
            });
        }
    }
    // A sequential tag precedes the largest payload, padded to its alignment. The unused tag
    // values are niches a containing one-data-arm enum can use without adding another tag.
    let payload_at = tag.checked_add(align - 1)? & !(align - 1);
    let size = payload_at.checked_add(size)?.checked_add(align - 1)? & !(align - 1);
    Some(Layout {
        size,
        align,
        spare_tags: (1_u64 << (tag * 8)) - count,
    })
}

fn record_layout(record: &Record, api: &Api, depth: usize) -> Option<Layout> {
    if !record.layout_known || depth > api.records.len() {
        return None;
    }
    match &record.shape {
        Shape::Struct(fields) => fields_layout(fields, api, depth),
        Shape::Alias(ty) => type_layout(ty, api, depth),
        Shape::Enum(variants) => enum_layout(variants, api, depth),
    }
}

fn type_layout(ty: &RustType, api: &Api, depth: usize) -> Option<Layout> {
    match ty {
        RustType::Named { name, arguments } if arguments.is_empty() => {
            let width = match name.as_str() {
                "bool" | "u8" | "i8" => Some(1),
                "u16" | "i16" => Some(2),
                "u32" | "i32" | "f32" | "char" => Some(4),
                "u64" | "i64" | "f64" => Some(8),
                "u128" | "i128" => Some(16),
                _ => None,
            };
            if let Some(size) = width {
                return Some(Layout {
                    size,
                    align: size,
                    spare_tags: 0,
                });
            }
            let record = api.records.get(last_segment(name))?;
            record_layout(record, api, depth + 1)
        }
        RustType::Array { element, length } => {
            let count = syn::parse_str::<syn::LitInt>(length)
                .ok()?
                .base10_parse::<u32>()
                .ok()?;
            let element = type_layout(element, api, depth)?;
            Some(Layout {
                size: element.size.checked_mul(count)?,
                align: element.align,
                spare_tags: if count == 0 { 0 } else { element.spare_tags },
            })
        }
        RustType::Tuple { elements } => aggregate_layout(elements.iter(), api, depth),
        RustType::Named { .. } | RustType::Reference { .. } | RustType::Slice { .. } => None,
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Record {
    pub name: String,
    /// Lexical module that owns the declaration, including inline module components.
    pub module: String,
    pub visibility: DeclarationVisibility,
    pub docs: Vec<String>,
    /// Whether the declaration's Rust AST derives Copy.
    pub copy: bool,
    /// Static native byte size; unknown layouts are never classified as small.
    pub static_size: Option<u32>,
    #[serde(skip)]
    pub layout_known: bool,
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
    /// The source-owned native scalar projection API, never guessed by a consumer.
    pub accessor: Option<ScalarAccessor>,
    pub exclusive: Vec<String>,
    pub name: Option<String>,
    pub unit_value: Option<String>,
    pub output_only: bool,
}

#[derive(Serialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum ScalarAccessor {
    Get,
    AsStr,
    Display,
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

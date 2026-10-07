use std::collections::BTreeMap;

#[derive(Default)]
pub struct Api {
    pub records: BTreeMap<String, Record>,
}

pub struct Record {
    pub name: String,
    pub docs: Vec<String>,
    pub serde: Serde,
    pub projection: Projection,
    pub shape: Shape,
    pub exported: bool,
}

pub enum Shape {
    Struct(Fields),
    Enum(Vec<Variant>),
    Alias(syn::Type),
}

pub enum Fields {
    Unit,
    Named(Vec<Field>),
    Tuple(Vec<Field>),
}

pub struct Field {
    pub name: Option<String>,
    pub ty: syn::Type,
    pub docs: Vec<String>,
    pub serde: Serde,
}

pub struct Variant {
    pub name: String,
    pub docs: Vec<String>,
    pub serde: Serde,
    pub fields: Fields,
}

/// Facts from serde attributes, without guessing whether a record is an input or an output.
#[derive(Default)]
pub struct Serde {
    pub rename: Option<String>,
    pub rename_all: Option<String>,
    pub rename_all_fields: Option<String>,
    pub tag: Option<String>,
    pub content: Option<String>,
    pub untagged: bool,
    pub transparent: bool,
    pub default: bool,
    pub skip_serializing_if: Option<String>,
    pub skip: bool,
    pub flatten: bool,
    pub deny_unknown_fields: bool,
    pub into: Option<String>,
    pub from: Option<String>,
    pub deserialize_with: Option<String>,
}

/// Metadata for custom serializers. Kept on the canonical Rust declaration, never in an adapter.
#[derive(Default)]
pub struct Projection {
    pub wire: Option<String>,
    pub scalar: Option<String>,
    pub exclusive: Vec<String>,
    pub name: Option<String>,
    pub unit_value: Option<String>,
    pub output_only: bool,
}

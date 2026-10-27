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
    /// Publish static native sizes once all declarations, including cross-file references, are collected.
    pub fn refresh_layouts(&mut self) {
        #[cfg(test)]
        layout_counts::finalization();
        let mut pass = LayoutPass {
            api: self,
            states: BTreeMap::new(),
        };
        let sizes: Vec<_> = self
            .records
            .values()
            .map(|record| record_layout(record, &mut pass).map(|layout| layout.size))
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

#[derive(Clone, Copy)]
enum LayoutState {
    Active,
    Finished(Option<Layout>),
}

struct LayoutPass<'a> {
    api: &'a Api,
    states: BTreeMap<&'a str, LayoutState>,
}

fn aggregate_layout<'a>(
    types: impl Iterator<Item = &'a RustType>,
    pass: &mut LayoutPass<'a>,
) -> Option<Layout> {
    let mut size = 0_u32;
    let mut align = 1_u32;
    let mut single = None;
    let mut count = 0_u32;
    for ty in types {
        let layout = type_layout(ty, pass)?;
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

fn fields_layout<'a>(fields: &'a Fields, pass: &mut LayoutPass<'a>) -> Option<Layout> {
    match fields {
        Fields::Unit => Some(Layout {
            size: 0,
            align: 1,
            spare_tags: 0,
        }),
        Fields::Named(fields) | Fields::Tuple(fields) => {
            aggregate_layout(fields.iter().map(|field| &field.ty), pass)
        }
    }
}

fn enum_layout<'a>(variants: &'a [Variant], pass: &mut LayoutPass<'a>) -> Option<Layout> {
    let count = u64::try_from(variants.len()).ok()?;
    let tag: u32 = match count {
        0 => return None,
        1 => return fields_layout(&variants.first()?.fields, pass),
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
        let layout = fields_layout(&variant.fields, pass)?;
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

fn record_layout<'a>(record: &'a Record, pass: &mut LayoutPass<'a>) -> Option<Layout> {
    match pass.states.get(record.name.as_str()) {
        Some(LayoutState::Active) => return None,
        Some(LayoutState::Finished(layout)) => return *layout,
        None => {}
    }
    #[cfg(test)]
    layout_counts::record_evaluation(&record.name);
    pass.states.insert(&record.name, LayoutState::Active);
    let layout = if record.layout_known {
        match &record.shape {
            Shape::Struct(fields) => fields_layout(fields, pass),
            Shape::Alias(ty) => type_layout(ty, pass),
            Shape::Enum(variants) => enum_layout(variants, pass),
        }
    } else {
        None
    };
    pass.states
        .insert(&record.name, LayoutState::Finished(layout));
    layout
}

fn type_layout<'a>(ty: &'a RustType, pass: &mut LayoutPass<'a>) -> Option<Layout> {
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
            let record = pass.api.records.get(last_segment(name))?;
            record_layout(record, pass)
        }
        RustType::Array { element, length } => {
            let count = syn::parse_str::<syn::LitInt>(length)
                .ok()?
                .base10_parse::<u32>()
                .ok()?;
            let element = type_layout(element, pass)?;
            Some(Layout {
                size: element.size.checked_mul(count)?,
                align: element.align,
                spare_tags: if count == 0 { 0 } else { element.spare_tags },
            })
        }
        RustType::Tuple { elements } => aggregate_layout(elements.iter(), pass),
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

#[cfg(test)]
pub(crate) mod layout_counts {
    use std::cell::RefCell;
    use std::collections::BTreeMap;

    #[derive(Default, Debug)]
    pub(crate) struct Counts {
        pub finalizations: usize,
        pub evaluations: BTreeMap<String, usize>,
    }

    thread_local! {
        static COUNTS: RefCell<Counts> = RefCell::new(Counts::default());
    }

    pub(crate) fn finalization() {
        COUNTS.with_borrow_mut(|counts| counts.finalizations += 1);
    }

    pub(crate) fn record_evaluation(name: &str) {
        COUNTS.with_borrow_mut(|counts| {
            *counts.evaluations.entry(name.to_owned()).or_default() += 1;
        });
    }

    pub(crate) fn take() -> Counts {
        COUNTS.with_borrow_mut(std::mem::take)
    }

    pub(crate) fn assert_one_pass(api: &super::Api, counts: &Counts, context: &str) {
        assert_eq!(counts.finalizations, 1, "{context}: {counts:?}");
        assert_eq!(
            counts.evaluations.len(),
            api.records.len(),
            "{context}: {counts:?}"
        );
        for name in api.records.keys() {
            assert_eq!(counts.evaluations.get(name), Some(&1), "{context}: {name}");
        }
    }
}

#[cfg(test)]
mod layout_tests {
    use super::*;
    use crate::records;
    use std::fmt::Write;

    #[test]
    fn native_layout_tables_preserve_sizes_alignment_and_niches() {
        let mut api = Api::default();
        records::parse(
            r#"
            type Byte = u8;
            type Signed = i16;
            type Word = u32;
            type Wide = u64;
            type Huge = u128;
            type Boolean = bool;
            type Character = char;
            type Float = f64;
            type Unit = ();
            type EmptyArray = [u16; 0];
            type Array = [u16; 3];
            type Tuple = (u8, u32, u8);
            type ArrayTuple = ([u16; 3], u8);
            type Qualified = ::unrelated::Word;
            #[derive(Serialize)] struct Padded { a: u8, b: u32, c: u8 }
            #[derive(Serialize)] struct TupleRecord(u8, u32, u8);
            #[derive(Serialize)] struct Empty;
            #[derive(Serialize)] enum Tags { Off, On }
            type TagAlias = Tags;
            #[derive(Serialize)] enum Niche { Empty, Data(TagAlias) }
            #[derive(Serialize)] enum NestedNiche { Empty, Data(Niche) }
            #[derive(Serialize)] enum ArrayNiche { Empty, Data([Tags; 3]) }
            #[derive(Serialize)] enum NoArrayNiche { Empty, Data([Tags; 0]) }
            #[derive(Serialize)] enum Payload { Empty, Data(u64) }
            #[derive(Serialize)] enum TwoPayloads { First(u8), Second(u64) }
            #[derive(Serialize)] enum One { Only(u16) }
            #[derive(Serialize)] enum PaddedPayload { Empty, Data((u8, u32, u8)) }
            "#,
            &mut api,
            "fixture::layouts",
        )
        .expect("native layout declarations");
        api.refresh_layouts();
        for (name, size) in [
            ("Byte", 1),
            ("Signed", 2),
            ("Word", 4),
            ("Wide", 8),
            ("Huge", 16),
            ("Boolean", 1),
            ("Character", 4),
            ("Float", 8),
            ("Unit", 0),
            ("EmptyArray", 0),
            ("Array", 6),
            ("Tuple", 8),
            ("ArrayTuple", 8),
            ("Qualified", 4),
            ("Padded", 8),
            ("TupleRecord", 8),
            ("Empty", 0),
            ("Tags", 1),
            ("TagAlias", 1),
            ("Niche", 1),
            ("NestedNiche", 1),
            ("ArrayNiche", 3),
            ("NoArrayNiche", 1),
            ("Payload", 16),
            ("TwoPayloads", 16),
            ("One", 2),
            ("PaddedPayload", 12),
        ] {
            assert_eq!(api.records[name].static_size, Some(size), "{name}");
        }
    }

    #[test]
    fn native_layout_unknown_tables_preserve_refusals_and_overflow() {
        let mut api = Api::default();
        records::parse(
            r#"
            type Missing = Absent;
            type QualifiedPrimitive = core::primitive::u32;
            type PointerWidth = usize;
            type Symbolic = [u8; LIMIT];
            type Expression = [u8; 2 + 2];
            type ArrayOverflow = [u64; 536870912];
            type LengthOverflow = [u8; 4294967296];
            type Largest = [u8; 4294967295];
            type SumOverflow = (Largest, u8);
            type PaddingOverflow = ([u8; 4294967293], u16);
            type Borrowed = &'static u32;
            type Slice = [u8];
            type Generic = Option<u32>;
            type SelfAlias = SelfAlias;
            type MutualA = MutualB;
            type MutualB = MutualA;
            type ZeroRecursive = [ZeroRecursive; 0];
            #[derive(Serialize)] struct SelfRecord { next: SelfRecord }
            #[derive(Serialize)] enum RecursiveEnum { Empty, Next(RecursiveEnum) }
            #[derive(Serialize)] enum Void {}
            #[repr(C)] #[derive(Serialize)] struct Explicit { value: u32 }
            #[repr(u8)] #[derive(Serialize)] enum ReprEnum { First, Second }
            #[derive(Serialize)] enum Discriminant { First = 0, Second }
            "#,
            &mut api,
            "fixture::unknown",
        )
        .expect("unknown layout declarations");
        layout_counts::take();
        api.refresh_layouts();
        let counts = layout_counts::take();
        layout_counts::assert_one_pass(&api, &counts, "unknowns and cycles");
        assert_eq!(api.records["Largest"].static_size, Some(u32::MAX));
        for (name, record) in &api.records {
            if name != "Largest" {
                assert_eq!(record.static_size, None, "{name}");
            }
        }
    }

    #[test]
    fn complete_source_collection_finalizes_once_with_forward_and_cross_file_references() {
        for n in [1, 8, 64, 256] {
            layout_counts::take();
            let mut api = Api::default();
            for index in 0..n {
                let source = format!(
                    "pub mod nested {{ pub type Forward{index} = external::Later{index}; }}"
                );
                records::parse(&source, &mut api, "fixture::api").expect("forward source");
            }
            for index in 0..n {
                let source = format!("pub type Later{index} = (u8, u32);");
                records::parse_support(&source, &mut api, "fixture::support")
                    .expect("cross-file target");
            }
            api.refresh_layouts();
            let counts = layout_counts::take();
            layout_counts::assert_one_pass(&api, &counts, &format!("collection N={n}"));
            for record in api.records.values() {
                assert_eq!(record.static_size, Some(8), "N={n}: {}", record.name);
            }
        }
    }

    #[test]
    fn native_layout_graphs_evaluate_each_declaration_once_per_pass() {
        for n in [1, 8, 64, 256] {
            for graph in ["chain", "diamond", "unknown", "cycle"] {
                let mut source = String::new();
                match graph {
                    "chain" => {
                        source.push_str("type Node0 = u32;\n");
                        for index in 1..n {
                            writeln!(source, "type Node{index} = Node{};", index - 1)
                                .expect("write chain declaration");
                        }
                    }
                    "diamond" => {
                        source.push_str(
                            "type Shared = u32; type Left = Shared; type Right = Shared;\n",
                        );
                        for index in 0..n {
                            writeln!(source, "type Node{index} = (Left, Right);")
                                .expect("write diamond declaration");
                        }
                    }
                    "unknown" => {
                        source.push_str(
                            "type Shared = Absent; type Left = Shared; type Right = Shared;\n",
                        );
                        for index in 0..n {
                            writeln!(source, "type Node{index} = (Left, Right);")
                                .expect("write unknown declaration");
                        }
                    }
                    "cycle" => {
                        for index in 0..n {
                            writeln!(source, "type Node{index} = Node{};", (index + 1) % n)
                                .expect("write cycle declaration");
                        }
                    }
                    _ => unreachable!("the graph table is closed"),
                }
                let mut api = Api::default();
                records::parse(&source, &mut api, "fixture::graphs").expect("graph declarations");
                layout_counts::take();
                api.refresh_layouts();
                let counts = layout_counts::take();
                layout_counts::assert_one_pass(&api, &counts, &format!("{graph} N={n}"));
                for (name, record) in &api.records {
                    let expected = match graph {
                        "chain" => Some(4),
                        "diamond" if name.starts_with("Node") => Some(8),
                        "diamond" => Some(4),
                        "unknown" | "cycle" => None,
                        _ => unreachable!("the graph table is closed"),
                    };
                    assert_eq!(record.static_size, expected, "{graph} N={n}: {name}");
                }
            }
        }
    }

    #[test]
    fn native_layout_memo_is_discarded_between_passes() {
        let mut api = Api::default();
        records::parse(
            "type Alias = Value; type Value = u32;",
            &mut api,
            "fixture::passes",
        )
        .expect("pass declarations");
        for known in [true, false, true] {
            api.records
                .get_mut("Value")
                .expect("Value declaration")
                .layout_known = known;
            layout_counts::take();
            api.refresh_layouts();
            let counts = layout_counts::take();
            layout_counts::assert_one_pass(&api, &counts, "fresh pass");
            for record in api.records.values() {
                assert_eq!(record.static_size, known.then_some(4), "{}", record.name);
            }
        }
    }
}

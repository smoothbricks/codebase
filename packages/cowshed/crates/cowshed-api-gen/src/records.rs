use crate::ir::{Api, Field, Fields, Projection, Record, RustType, Serde, Shape, Variant};
use syn::spanned::Spanned;
use syn::{Attribute, Expr, Item, Lit, Meta, Visibility};

pub fn parse(source: &str, api: &mut Api) -> syn::Result<()> {
    parse_items(syn::parse_file(source)?.items, api, true)
}

/// API declarations have lexical priority over equally named implementation/support records.
pub fn parse_support(source: &str, api: &mut Api) -> syn::Result<()> {
    let mut support = Api::default();
    parse_items(syn::parse_file(source)?.items, &mut support, false)?;
    for (name, record) in support.records {
        api.records.entry(name).or_insert(record);
    }
    Ok(())
}

fn parse_items(items: Vec<Item>, api: &mut Api, exported: bool) -> syn::Result<()> {
    let mut custom_serializers: std::collections::BTreeSet<String> = items
        .iter()
        .filter_map(|item| {
            let Item::Impl(item) = item else {
                return None;
            };
            let (_, trait_path, _) = item.trait_.as_ref()?;
            if !trait_path.segments.last().is_some_and(|segment| {
                segment.ident == "Serialize" || segment.ident == "Deserialize"
            }) {
                return None;
            }
            let syn::Type::Path(ty) = item.self_ty.as_ref() else {
                return None;
            };
            ty.path
                .segments
                .last()
                .map(|segment| segment.ident.to_string())
        })
        .collect();
    // `string_domain_serde!(Name)` writes both serde impls for `Name`, so its argument is a
    // serialized declaration even though no `impl` written here names it.
    for item in &items {
        match item {
            Item::Macro(item) if item.mac.path.is_ident("string_domain_serde") => {
                custom_serializers
                    .insert(syn::parse2::<syn::Ident>(item.mac.tokens.clone())?.to_string());
            }
            _ => {}
        }
    }
    let needed = needed_declarations(&items, &custom_serializers)?;
    for item in items {
        let record = match item {
            Item::Struct(item) => {
                let projection = projection(&item.attrs)?;
                let serialized = is_serialized(&item.attrs)
                    || custom_serializers.contains(&item.ident.to_string());
                // Runtime and internal state that no declaration reaches has no wire or projection
                // shape, so its field types are never normalized.
                if !serialized
                    && !is_public_projection(&projection)
                    && !needed.contains(&item.ident.to_string())
                {
                    None
                } else {
                    let shape = Shape::Struct(fields(item.fields, projection.output_only)?);
                    Some(Record {
                        exported: exported
                            && is_public(&item.vis)
                            && item.generics.type_params().next().is_none()
                            && serialized,
                        name: item.ident.to_string(),
                        docs: docs(&item.attrs),
                        serde: serde(&item.attrs)?,
                        projection,
                        shape,
                    })
                }
            }
            Item::Enum(item) => {
                let projection = projection(&item.attrs)?;
                let serialized = is_serialized(&item.attrs)
                    || custom_serializers.contains(&item.ident.to_string());
                if !serialized
                    && !is_public_projection(&projection)
                    && !needed.contains(&item.ident.to_string())
                {
                    None
                } else {
                    Some(Record {
                        exported: exported
                            && is_public(&item.vis)
                            && item.generics.type_params().next().is_none()
                            && serialized,
                        name: item.ident.to_string(),
                        docs: docs(&item.attrs),
                        serde: serde(&item.attrs)?,
                        projection,
                        shape: Shape::Enum(
                            item.variants
                                .into_iter()
                                .map(|variant| {
                                    Ok(Variant {
                                        name: variant.ident.to_string(),
                                        docs: docs(&variant.attrs),
                                        serde: serde(&variant.attrs)?,
                                        fields: fields(variant.fields, false)?,
                                    })
                                })
                                .collect::<syn::Result<_>>()?,
                        ),
                    })
                }
            }
            // Aliases always normalize: each names a real type a declaration may reference, so
            // dropping one would leave that reference dangling.
            Item::Type(item) => Some(Record {
                exported: false,
                name: item.ident.to_string(),
                docs: docs(&item.attrs),
                serde: serde(&item.attrs)?,
                projection: projection(&item.attrs)?,
                shape: Shape::Alias(normalized_type(*item.ty)?),
            }),
            Item::Macro(item) if item.mac.path.is_ident("hex_identifier") => {
                let identifier = syn::parse2::<HexIdentifier>(item.mac.tokens)?;
                let digits = identifier.bytes.checked_mul(2).ok_or_else(|| {
                    syn::Error::new(
                        proc_macro2::Span::call_site(),
                        "hex identifier width overflow",
                    )
                })?;
                Some(Record {
                    name: identifier.name.to_string(),
                    docs: docs(&item.attrs),
                    serde: Serde::default(),
                    projection: Projection {
                        scalar: Some(format!(
                            "string & tags.Pattern<'^(?!0{{{digits}}}$)[0-9a-f]{{{digits}}}$'>"
                        )),
                        ..Projection::default()
                    },
                    shape: Shape::Alias(normalized_type(syn::parse_quote!(String))?),
                    exported,
                })
            }
            Item::Mod(item) if !is_test_only(&item.attrs)? => {
                if let Some((_, items)) = item.content {
                    parse_items(items, api, exported)?;
                }
                None
            }
            _ => None,
        };
        if let Some(mut record) = record {
            record.exported |= exported && is_public_projection(&record.projection);
            let name = record.name.clone();
            if api.records.insert(name.clone(), record).is_some() {
                return Err(syn::Error::new(
                    proc_macro2::Span::call_site(),
                    format!("duplicate API record {name}"),
                ));
            }
        }
    }
    Ok(())
}

/// Whether a struct or enum is a declaration in its own right: serialized, or carrying a public
/// projection.
fn is_declaration(
    attrs: &[Attribute],
    name: &syn::Ident,
    custom: &std::collections::BTreeSet<String>,
) -> syn::Result<bool> {
    Ok(is_serialized(attrs)
        || custom.contains(&name.to_string())
        || is_public_projection(&projection(attrs)?))
}

/// The structs and enums of one source list that normalize because a declaration reaches them.
/// Every serialized or projected struct or enum, and every alias, seeds the walk, since aliases
/// always normalize. A struct or enum reached through a typed reference is needed, transitively.
/// Only type positions are references (paths and their generic type arguments, references, arrays,
/// slices, tuples and parentheses): identifiers in variants, constants and expressions name no
/// type. A position this walk does not enter, such as a trait object or function pointer, is not
/// a reference here; a declaration that keeps it refuses it by name when it normalizes. Nested
/// modules are separate lists.
fn needed_declarations(
    items: &[Item],
    custom: &std::collections::BTreeSet<String>,
) -> syn::Result<std::collections::BTreeSet<String>> {
    let mut declared: std::collections::BTreeMap<String, Vec<&syn::Type>> = Default::default();
    let mut seeds: Vec<&syn::Type> = Vec::new();
    for item in items {
        match item {
            Item::Struct(item) => {
                let types: Vec<&syn::Type> = item.fields.iter().map(|field| &field.ty).collect();
                if is_declaration(&item.attrs, &item.ident, custom)? {
                    seeds.extend(types.iter().copied());
                }
                declared.insert(item.ident.to_string(), types);
            }
            Item::Enum(item) => {
                let types: Vec<&syn::Type> = item
                    .variants
                    .iter()
                    .flat_map(|variant| variant.fields.iter().map(|field| &field.ty))
                    .collect();
                if is_declaration(&item.attrs, &item.ident, custom)? {
                    seeds.extend(types.iter().copied());
                }
                declared.insert(item.ident.to_string(), types);
            }
            Item::Type(item) => seeds.push(&*item.ty),
            _ => {}
        }
    }
    let mut names = Vec::new();
    for ty in seeds {
        type_names(ty, &mut names);
    }
    let mut needed = std::collections::BTreeSet::new();
    while let Some(name) = names.pop() {
        let Some(types) = declared.get(&name) else {
            continue;
        };
        if needed.insert(name) {
            for ty in types {
                type_names(ty, &mut names);
            }
        }
    }
    Ok(needed)
}

/// Appends the name of each path segment a type reaches, through its generic type arguments and
/// the reference, array, slice, tuple and parenthesized types around it.
fn type_names(ty: &syn::Type, names: &mut Vec<String>) {
    match ty {
        syn::Type::Path(path) => {
            if let Some(qself) = &path.qself {
                type_names(&qself.ty, names);
            }
            for segment in &path.path.segments {
                names.push(segment.ident.to_string());
                if let syn::PathArguments::AngleBracketed(arguments) = &segment.arguments {
                    for argument in &arguments.args {
                        if let syn::GenericArgument::Type(value) = argument {
                            type_names(value, names);
                        }
                    }
                }
            }
        }
        syn::Type::Reference(value) => type_names(&value.elem, names),
        syn::Type::Array(value) => type_names(&value.elem, names),
        syn::Type::Slice(value) => type_names(&value.elem, names),
        syn::Type::Paren(value) => type_names(&value.elem, names),
        syn::Type::Group(value) => type_names(&value.elem, names),
        syn::Type::Tuple(value) => {
            for element in &value.elems {
                type_names(element, names);
            }
        }
        _ => {}
    }
}

struct HexIdentifier {
    name: syn::Ident,
    bytes: usize,
}

impl syn::parse::Parse for HexIdentifier {
    fn parse(input: syn::parse::ParseStream<'_>) -> syn::Result<Self> {
        let name = input.parse()?;
        input.parse::<syn::Token![,]>()?;
        let bytes = input.parse::<syn::LitInt>()?.base10_parse()?;
        input.parse::<syn::Token![,]>()?;
        let _: syn::Ident = input.parse()?;
        Ok(Self { name, bytes })
    }
}

fn is_public(visibility: &Visibility) -> bool {
    matches!(visibility, Visibility::Public(_))
}

fn is_public_projection(projection: &Projection) -> bool {
    projection.wire.is_some() || projection.scalar.is_some() || projection.name.is_some()
}

fn is_serialized(attrs: &[Attribute]) -> bool {
    attrs.iter().any(|attr| {
        if !attr.path().is_ident("derive") {
            return false;
        }
        let paths = attr.parse_args_with(
            syn::punctuated::Punctuated::<syn::Path, syn::Token![,]>::parse_terminated,
        );
        paths.is_ok_and(|paths| {
            paths.iter().any(|path| {
                path.segments.last().is_some_and(|segment| {
                    segment.ident == "Serialize" || segment.ident == "Deserialize"
                })
            })
        })
    })
}

fn fields(fields: syn::Fields, output_only: bool) -> syn::Result<Fields> {
    let parse_field = |field: syn::Field| -> syn::Result<Field> {
        Ok(Field {
            name: field.ident.map(|ident| ident.to_string()),
            ty: normalized_type(field.ty)?,
            docs: docs(&field.attrs),
            serde: serde_for(&field.attrs, output_only)?,
        })
    };
    match fields {
        syn::Fields::Unit => Ok(Fields::Unit),
        syn::Fields::Named(fields) => Ok(Fields::Named(
            fields
                .named
                .into_iter()
                .map(parse_field)
                .collect::<syn::Result<_>>()?,
        )),
        syn::Fields::Unnamed(fields) => Ok(Fields::Tuple(
            fields
                .unnamed
                .into_iter()
                .map(parse_field)
                .collect::<syn::Result<_>>()?,
        )),
    }
}

/// Normalizes a declared type once, at the parser boundary. A refused type keeps the span of the
/// whole declaration and names its unsupported syntax as the cause.
fn normalized_type(ty: syn::Type) -> syn::Result<RustType> {
    let span = ty.span();
    RustType::from_syn(ty).map_err(|cause| syn::Error::new(span, cause))
}

fn docs(attrs: &[Attribute]) -> Vec<String> {
    attrs
        .iter()
        .filter_map(|attr| match &attr.meta {
            Meta::NameValue(meta) if meta.path.is_ident("doc") => match &meta.value {
                Expr::Lit(expr) => match &expr.lit {
                    Lit::Str(value) => Some(value.value().trim().to_owned()),
                    _ => None,
                },
                _ => None,
            },
            _ => None,
        })
        .collect()
}

pub fn serde(attrs: &[Attribute]) -> syn::Result<Serde> {
    serde_for(attrs, false)
}

fn serde_for(attrs: &[Attribute], output_only: bool) -> syn::Result<Serde> {
    let mut result = Serde::default();
    for attr in attrs {
        let mut apply = |meta: syn::meta::ParseNestedMeta<'_>| {
            if meta.path.is_ident("rename") {
                result.rename = Some(meta.value()?.parse::<syn::LitStr>()?.value());
            } else if meta.path.is_ident("rename_all") {
                result.rename_all = Some(meta.value()?.parse::<syn::LitStr>()?.value());
            } else if meta.path.is_ident("rename_all_fields") {
                result.rename_all_fields = Some(meta.value()?.parse::<syn::LitStr>()?.value());
            } else if meta.path.is_ident("tag") {
                result.tag = Some(meta.value()?.parse::<syn::LitStr>()?.value());
            } else if meta.path.is_ident("content") {
                result.content = Some(meta.value()?.parse::<syn::LitStr>()?.value());
            } else if meta.path.is_ident("skip_serializing_if") {
                result.skip_serializing_if = Some(meta.value()?.parse::<syn::LitStr>()?.value());
            } else if meta.path.is_ident("into") {
                result.into = Some(meta.value()?.parse::<syn::LitStr>()?.value());
            } else if meta.path.is_ident("from") || meta.path.is_ident("try_from") {
                result.from = Some(meta.value()?.parse::<syn::LitStr>()?.value());
            } else if meta.path.is_ident("default") {
                result.default = true;
                if meta.input.peek(syn::Token![=]) {
                    result.default_with = Some(meta.value()?.parse::<syn::LitStr>()?.value());
                }
            } else if meta.path.is_ident("untagged") {
                result.untagged = true;
            } else if meta.path.is_ident("transparent") {
                result.transparent = true;
            } else if meta.path.is_ident("flatten") {
                result.flatten = true;
            } else if meta.path.is_ident("deny_unknown_fields") {
                result.deny_unknown_fields = true;
            } else if meta.path.is_ident("skip") || meta.path.is_ident("skip_serializing") {
                result.skip = true;
            } else if meta.path.is_ident("bound")
                || meta.path.is_ident("borrow")
                || meta.path.is_ident("expecting")
                || meta.path.is_ident("crate")
            {
                consume_value(&meta)?;
            } else if meta.path.is_ident("deserialize_with") && output_only {
                result.deserialize_with = Some(meta.value()?.parse::<syn::LitStr>()?.value());
            } else {
                let name = meta
                    .path
                    .get_ident()
                    .map(ToString::to_string)
                    .unwrap_or_else(|| "qualified attribute".to_owned());
                return Err(meta.error(format!("unsupported serde attribute {name}")));
            }
            Ok(())
        };
        if attr.path().is_ident("serde") {
            attr.parse_nested_meta(&mut apply)?;
        } else if attr.path().is_ident("cfg_attr") {
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("serde") {
                    meta.parse_nested_meta(&mut apply)
                } else {
                    consume_value(&meta)
                }
            })?;
        }
    }
    Ok(result)
}

fn consume_value(meta: &syn::meta::ParseNestedMeta<'_>) -> syn::Result<()> {
    if meta.input.peek(syn::Token![=]) {
        let _: Expr = meta.value()?.parse()?;
    } else if meta.input.peek(syn::token::Paren) {
        let content;
        syn::parenthesized!(content in meta.input);
        let _: proc_macro2::TokenStream = content.parse()?;
    }
    Ok(())
}

fn projection(attrs: &[Attribute]) -> syn::Result<Projection> {
    let mut result = Projection::default();
    for attr in attrs.iter().filter(|attr| attr.path().is_ident("cfg_attr")) {
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("cowshed_api") {
                meta.parse_nested_meta(|meta| {
                    let value = meta.value()?.parse::<syn::LitStr>()?.value();
                    if meta.path.is_ident("wire") {
                        result.wire = Some(value);
                    } else if meta.path.is_ident("scalar") {
                        result.scalar = Some(value);
                    } else if meta.path.is_ident("exclusive") {
                        result.exclusive = value
                            .split(',')
                            .map(|name| name.trim().to_owned())
                            .collect();
                    } else if meta.path.is_ident("name") {
                        result.name = Some(value);
                    } else if meta.path.is_ident("unit_value") {
                        result.unit_value = Some(value);
                    } else if meta.path.is_ident("output_only") {
                        result.output_only = value
                            .parse()
                            .map_err(|_| meta.error("output_only must be true or false"))?;
                    } else {
                        return Err(meta.error("unknown cowshed_api projection attribute"));
                    }
                    Ok(())
                })?;
            } else {
                consume_value(&meta)?;
            }
            Ok(())
        })?;
    }
    Ok(result)
}

fn is_test_only(attrs: &[Attribute]) -> syn::Result<bool> {
    for attr in attrs.iter().filter(|attr| attr.path().is_ident("cfg")) {
        if requires_test(&attr.parse_args::<Meta>()?)? {
            return Ok(true);
        }
    }
    Ok(false)
}

fn requires_test(predicate: &Meta) -> syn::Result<bool> {
    match predicate {
        Meta::Path(path) => Ok(path.is_ident("test")),
        Meta::List(list) if list.path.is_ident("all") || list.path.is_ident("any") => {
            let predicates = list.parse_args_with(
                syn::punctuated::Punctuated::<Meta, syn::Token![,]>::parse_terminated,
            )?;
            let all = list.path.is_ident("all");
            let mut required = !all && !predicates.is_empty();
            for predicate in predicates {
                let test_only = requires_test(&predicate)?;
                required = if all {
                    required || test_only
                } else {
                    required && test_only
                };
            }
            Ok(required)
        }
        _ => Ok(false),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::TypeArgument;

    #[test]
    fn unsupported_serde_shapes_are_refused_by_attribute_name() {
        for attribute in [
            "with = \"custom\"",
            "serialize_with = \"custom\"",
            "deserialize_with = \"custom\"",
            "getter = \"custom\"",
            "remote = \"Other\"",
            "alias = \"old\"",
            "other",
            "variant_identifier",
            "field_identifier",
            "skip_deserializing",
        ] {
            let source = format!(
                "#[derive(Serialize)] #[serde({attribute})] pub struct Record {{ pub value: String }}"
            );
            let error =
                parse(&source, &mut Api::default()).expect_err("unsupported shape must fail");
            assert!(
                error
                    .to_string()
                    .contains(attribute.split([' ', '=']).next().unwrap())
            );
        }
    }

    #[test]
    fn test_only_modules_are_excluded_by_predicate_not_spelling() {
        let source = r#"
            #[cfg(all(test, unix))] mod oracle { #[derive(Serialize)] pub struct Fixture { pub value: String } }
            mod tests { #[derive(Serialize)] pub struct Production { pub value: String } }
        "#;
        let mut api = Api::default();
        parse(source, &mut api).unwrap();
        assert!(!api.records.contains_key("Fixture"));
        assert!(api.records["Production"].exported);
    }

    #[test]
    fn explicit_output_projection_does_not_weaken_input_attribute_checks() {
        let source = r#"
            #[derive(Serialize, Deserialize)]
            #[cfg_attr(any(), cowshed_api(output_only = "true"))]
            pub struct ErrorReceipt {
                #[serde(default, deserialize_with = "custom_input", skip_serializing_if = "Option::is_none")]
                pub reason: Option<String>,
            }
        "#;
        let mut api = Api::default();
        parse(source, &mut api).unwrap();
        let Shape::Struct(Fields::Named(fields)) = &api.records["ErrorReceipt"].shape else {
            panic!("receipt has named fields");
        };
        assert_eq!(
            fields[0].serde.deserialize_with.as_deref(),
            Some("custom_input")
        );
        assert!(
            parse(
                &source.replace("deserialize_with", "serialize_with"),
                &mut Api::default()
            )
            .is_err()
        );
    }

    fn named(name: &str, arguments: Vec<TypeArgument>) -> RustType {
        RustType::Named {
            name: name.to_owned(),
            arguments,
        }
    }

    fn argument(value: RustType) -> TypeArgument {
        TypeArgument::Type { value }
    }

    #[test]
    fn field_and_alias_types_are_normalized_once_at_the_parser_boundary() {
        let source = r#"
            #[derive(Serialize)]
            pub struct Shaped {
                pub borrowed: &'a mut std::collections::BTreeMap<String, Vec<u8>>,
                pub path: ::std::path::PathBuf,
                pub digits: [u8; N + 1],
                pub pair: (u8, Option<String>),
                pub holder: Holder<'static, u8>,
                pub cells: Grid<3>,
            }
            pub type Alias = Vec<::std::string::String>;
            hex_identifier!(TraceId, 16, InvalidTraceId);
        "#;
        let mut api = Api::default();
        parse(source, &mut api).expect("declarations");
        let Shape::Struct(Fields::Named(fields)) = &api.records["Shaped"].shape else {
            panic!("Shaped has named fields");
        };
        assert_eq!(
            fields[0].ty,
            RustType::Reference {
                element: Box::new(named(
                    "std::collections::BTreeMap",
                    vec![
                        argument(named("String", Vec::new())),
                        argument(named("Vec", vec![argument(named("u8", Vec::new()))])),
                    ],
                )),
                lifetime: Some("a".to_owned()),
                mutable: true,
            }
        );
        assert_eq!(fields[1].ty, named("::std::path::PathBuf", Vec::new()));
        assert_eq!(
            fields[2].ty,
            RustType::Array {
                element: Box::new(named("u8", Vec::new())),
                length: "N + 1".to_owned(),
            }
        );
        assert_eq!(
            fields[3].ty,
            RustType::Tuple {
                elements: vec![
                    named("u8", Vec::new()),
                    named("Option", vec![argument(named("String", Vec::new()))]),
                ],
            }
        );
        assert_eq!(
            fields[4].ty,
            named(
                "Holder",
                vec![
                    TypeArgument::Lifetime {
                        name: "static".to_owned(),
                    },
                    argument(named("u8", Vec::new())),
                ],
            )
        );
        assert_eq!(
            fields[5].ty,
            named(
                "Grid",
                vec![TypeArgument::Const {
                    expression: "3".to_owned(),
                }],
            )
        );
        let Shape::Alias(alias) = &api.records["Alias"].shape else {
            panic!("Alias is a type alias");
        };
        assert_eq!(
            *alias,
            named(
                "Vec",
                vec![argument(named("::std::string::String", Vec::new()))]
            )
        );
        let Shape::Alias(digest) = &api.records["TraceId"].shape else {
            panic!("TraceId is a hex identifier alias");
        };
        assert_eq!(*digest, named("String", Vec::new()));
    }

    #[test]
    fn unsupported_type_syntax_is_refused_by_name_at_the_parser_boundary() {
        for (ty, cause) in [
            (
                "Box<dyn std::fmt::Debug>",
                "unsupported Rust declaration type",
            ),
            ("fn(u8) -> u8", "unsupported Rust declaration type"),
            ("*const u8", "unsupported Rust declaration type"),
            (
                "<Owner as Trait>::Assoc",
                "unsupported Rust declaration type",
            ),
            (
                "Outer<u8>::Inner",
                "generic arguments on an intermediate path segment are unsupported",
            ),
            ("Box<Iterator<Item = u8>>", "unsupported generic argument"),
        ] {
            let source = format!("#[derive(Serialize)] pub struct Record {{ pub value: {ty} }}");
            let error =
                parse(&source, &mut Api::default()).expect_err("unsupported syntax is refused");
            assert!(error.to_string().contains(cause), "{ty}: {error}");
        }
        let error = parse("pub type Callback = fn(u8) -> u8;", &mut Api::default())
            .expect_err("an unsupported alias is refused");
        assert!(
            error
                .to_string()
                .contains("unsupported Rust declaration type"),
            "{error}"
        );
    }

    #[test]
    fn only_declarations_are_normalized() {
        let source = r#"
            pub struct RuntimeHandle {
                runtime: Arc<dyn Runtime>,
                completion: std::marker::PhantomData<fn() -> u8>,
            }
            pub enum Outcome {
                Pending(Box<dyn Fn()>),
            }
        "#;
        let mut api = Api::default();
        parse(source, &mut api).expect("runtime state is not a declaration");
        assert!(!api.records.contains_key("RuntimeHandle"));
        assert!(!api.records.contains_key("Outcome"));
    }

    #[test]
    fn a_projected_declaration_is_normalized_and_refuses_unsupported_syntax() {
        let source = r#"
            #[cfg_attr(any(), cowshed_api(name = "Projected"))]
            pub struct Handle {
                runtime: Arc<dyn Runtime>,
            }
        "#;
        let error =
            parse(source, &mut Api::default()).expect_err("a projected declaration is normalized");
        assert!(
            error
                .to_string()
                .contains("unsupported Rust declaration type"),
            "{error}"
        );
    }

    #[test]
    fn a_string_domain_declared_by_macro_is_a_serialized_declaration() {
        let source = r#"
            pub struct BranchName(String);
            string_domain_serde!(BranchName);
            pub struct Unrelated(String);
        "#;
        let mut api = Api::default();
        parse(source, &mut api).expect("declarations");
        assert!(api.records["BranchName"].exported);
        assert!(!api.records.contains_key("Unrelated"));
    }

    #[test]
    fn a_serialized_declaration_keeps_the_support_types_it_reaches() {
        let source = r#"
            #[derive(Serialize)]
            pub struct Outer { pub inner: Inner }
            pub struct Inner { pub id: Ident }
            pub type Ident = Chain;
            pub type Chain = Deep<u8>;
            pub struct Deep<T>(pub T);
        "#;
        let mut api = Api::default();
        parse(source, &mut api).expect("reached support types normalize");
        assert!(api.records["Outer"].exported);
        assert!(!api.records["Inner"].exported);
        assert!(api.records.contains_key("Ident"));
        assert!(api.records.contains_key("Chain"));
        assert!(api.records.contains_key("Deep"));
    }

    #[test]
    fn an_unreached_runtime_holder_is_skipped_and_a_reached_one_is_refused() {
        let mut api = Api::default();
        parse(
            "pub struct RuntimeHolder { runtime: Arc<dyn Runtime> }",
            &mut api,
        )
        .expect("an unreached runtime holder is not normalized");
        assert!(!api.records.contains_key("RuntimeHolder"));
        for source in [
            "#[derive(Serialize)] pub struct Reaches { pub holder: RuntimeHolder } pub struct RuntimeHolder { runtime: Arc<dyn Runtime> }",
            "pub type Handle = RuntimeHolder; pub struct RuntimeHolder { runtime: Arc<dyn Runtime> }",
        ] {
            let error = parse(source, &mut Api::default())
                .expect_err("a holder a declaration or alias reaches is refused");
            assert!(
                error
                    .to_string()
                    .contains("unsupported Rust declaration type"),
                "{error}"
            );
        }
    }
}

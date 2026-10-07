use crate::ir::{Api, Field, Fields, Projection, Record, Serde, Shape, Variant};
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
    let custom_serializers: std::collections::BTreeSet<_> = items
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
    for item in items {
        let record = match item {
            Item::Struct(item) => {
                let projection = projection(&item.attrs)?;
                let shape = Shape::Struct(fields(item.fields, projection.output_only)?);
                Some(Record {
                    exported: exported
                        && is_public(&item.vis)
                        && item.generics.type_params().next().is_none()
                        && (is_serialized(&item.attrs)
                            || custom_serializers.contains(&item.ident.to_string())),
                    name: item.ident.to_string(),
                    docs: docs(&item.attrs),
                    serde: serde(&item.attrs)?,
                    projection,
                    shape,
                })
            }
            Item::Enum(item) => Some(Record {
                exported: exported
                    && is_public(&item.vis)
                    && item.generics.type_params().next().is_none()
                    && (is_serialized(&item.attrs)
                        || custom_serializers.contains(&item.ident.to_string())),
                name: item.ident.to_string(),
                docs: docs(&item.attrs),
                serde: serde(&item.attrs)?,
                projection: projection(&item.attrs)?,
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
            }),
            Item::Type(item) => Some(Record {
                exported: false,
                name: item.ident.to_string(),
                docs: docs(&item.attrs),
                serde: serde(&item.attrs)?,
                projection: projection(&item.attrs)?,
                shape: Shape::Alias(*item.ty),
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
                    shape: Shape::Alias(syn::parse_quote!(String)),
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
    let parse_field = |field: syn::Field| {
        Ok(Field {
            name: field.ident.map(|ident| ident.to_string()),
            ty: field.ty,
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
                consume_value(&meta)?;
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
}

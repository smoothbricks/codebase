use crate::ir::{Api, Field, Fields, Projection, Record, Serde, Shape, Variant};
use syn::{Attribute, Expr, Item, Lit, Meta, Visibility};

pub fn parse(source: &str, api: &mut Api) -> syn::Result<()> {
    parse_items(syn::parse_file(source)?.items, api, true)
}

pub fn parse_support(source: &str, api: &mut Api) -> syn::Result<()> {
    parse_items(syn::parse_file(source)?.items, api, false)
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
            Item::Struct(item) => Some(Record {
                exported: exported
                    && is_public(&item.vis)
                    && (is_serialized(&item.attrs)
                        || custom_serializers.contains(&item.ident.to_string())),
                name: item.ident.to_string(),
                docs: docs(&item.attrs),
                serde: serde(&item.attrs)?,
                projection: projection(&item.attrs)?,
                shape: Shape::Struct(fields(item.fields)?),
            }),
            Item::Enum(item) => Some(Record {
                exported: exported
                    && is_public(&item.vis)
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
                                fields: fields(variant.fields)?,
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
            Item::Mod(item) if item.ident != "tests" => {
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

fn is_public(visibility: &Visibility) -> bool {
    matches!(visibility, Visibility::Public(_))
}

fn is_public_projection(projection: &Projection) -> bool {
    projection.wire.is_some() || projection.scalar.is_some()
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

fn fields(fields: syn::Fields) -> syn::Result<Fields> {
    let parse_field = |field: syn::Field| {
        Ok(Field {
            name: field.ident.map(|ident| ident.to_string()),
            ty: field.ty,
            docs: docs(&field.attrs),
            serde: serde(&field.attrs)?,
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
            } else {
                consume_value(&meta)?;
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

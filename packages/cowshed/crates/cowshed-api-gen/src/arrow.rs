//! Arrow columns for one annotated Rust record.
//!
//! The record is the declaration. This emitter writes the field list, the
//! encode, the decode, and the sparse merge. It does not emit a TypeScript or
//! N-API projection, so the wire surface is unchanged.

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArrowColumn {
    pub name: String,
    pub kind: ArrowKind,
    /// Dictionary names for a `dict8` column. Index 0 is the unused key.
    pub values: Option<Vec<String>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ArrowKind {
    UInt16,
    UInt32,
    UInt64,
    Int32,
    Int64,
    Utf8,
    Dict32,
    Dict8,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArrowRecord {
    pub prefix: String,
    pub fields: String,
    pub error: String,
    pub merge: bool,
    pub struct_name: String,
    pub columns: Vec<ArrowColumn>,
}

/// The `arrow_columns` record in `source`. A file with none is an error: the
/// process span has one declaration, not a silent empty schema.
pub fn parse_arrow_record(source: &str) -> Result<ArrowRecord, String> {
    let file = syn::parse_file(source).map_err(|error| error.to_string())?;
    let mut found = None;
    for item in file.items {
        let syn::Item::Struct(item) = item else {
            continue;
        };
        if !has_arrow_columns(&item.attrs) {
            continue;
        }
        if found.is_some() {
            return Err("more than one arrow_columns record".to_owned());
        }
        found = Some(record_from_struct(&item)?);
    }
    found.ok_or_else(|| "no arrow_columns record".to_owned())
}

fn has_arrow_columns(attrs: &[syn::Attribute]) -> bool {
    attrs.iter().any(|attr| {
        let mut marked = false;
        let _ = attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("cowshed_api") {
                meta.parse_nested_meta(|meta| {
                    if meta.path.is_ident("arrow_columns") {
                        marked = true;
                    }
                    let _: syn::LitStr = meta.value()?.parse()?;
                    Ok(())
                })?;
            } else {
                consume_value(&meta)?;
            }
            Ok(())
        });
        marked
    })
}

fn consume_value(meta: &syn::meta::ParseNestedMeta<'_>) -> syn::Result<()> {
    if meta.input.peek(syn::Token![=]) {
        let _: syn::Expr = meta.value()?.parse()?;
    } else if meta.input.peek(syn::token::Paren) {
        let content;
        syn::parenthesized!(content in meta.input);
        let _: proc_macro2::TokenStream = content.parse()?;
    }
    Ok(())
}
struct OwnerNames {
    prefix: String,
    fields: String,
    error: String,
    merge: bool,
}

fn owner_names(attrs: &[syn::Attribute]) -> Result<OwnerNames, String> {
    let mut prefix = None;
    let mut fields = None;
    let mut error = None;
    let mut merge = None;
    for attr in attrs {
        if !matches!(&attr.meta, syn::Meta::List(_)) {
            continue;
        }
        attr.parse_nested_meta(|meta| {
            if !meta.path.is_ident("cowshed_api") {
                consume_value(&meta)?;
                return Ok(());
            }
            meta.parse_nested_meta(|meta| {
                let value = meta.value()?.parse::<syn::LitStr>()?.value();
                if meta.path.is_ident("arrow_columns") {
                    prefix = Some(value);
                } else if meta.path.is_ident("fields") {
                    fields = Some(value);
                } else if meta.path.is_ident("error") {
                    error = Some(value);
                } else if meta.path.is_ident("merge") {
                    merge = Some(match value.as_str() {
                        "true" => true,
                        "false" => false,
                        _ => return Err(meta.error("merge must be true or false")),
                    });
                } else {
                    return Err(meta.error("unknown arrow record attribute"));
                }
                Ok(())
            })
        })
        .map_err(|error| error.to_string())?;
    }
    let prefix = prefix.ok_or_else(|| "missing arrow_columns".to_owned())?;
    Ok(OwnerNames {
        fields: fields.unwrap_or_else(|| format!("{prefix}_fields")),
        error: error.unwrap_or_else(|| {
            let mut chars = prefix.chars();
            match chars.next() {
                Some(first) => format!("{}{}SpanError", first.to_uppercase(), chars.as_str()),
                None => "SpanError".to_owned(),
            }
        }),
        merge: merge.unwrap_or(false),
        prefix,
    })
}
fn record_from_struct(item: &syn::ItemStruct) -> Result<ArrowRecord, String> {
    let syn::Fields::Named(fields) = &item.fields else {
        return Err(format!("{} must name its columns", item.ident));
    };
    let mut columns = Vec::new();
    for field in &fields.named {
        let name = field
            .ident
            .as_ref()
            .ok_or_else(|| "an arrow column needs a name".to_owned())?
            .to_string();
        let (kind, values) = arrow_of(&field.attrs).map_err(|error| format!("{name}: {error}"))?;
        if kind == ArrowKind::Dict8 && values.is_none() {
            return Err(format!("{name}: a dict8 column names its values"));
        }
        if kind != ArrowKind::Dict8 && values.is_some() {
            return Err(format!("{name}: only a dict8 column names values"));
        }
        columns.push(ArrowColumn { name, kind, values });
    }
    if columns.is_empty() {
        return Err(format!("{} declares no columns", item.ident));
    }
    let owner = owner_names(&item.attrs)?;
    Ok(ArrowRecord {
        prefix: owner.prefix,
        fields: owner.fields,
        error: owner.error,
        merge: owner.merge,
        struct_name: item.ident.to_string(),
        columns,
    })
}

fn arrow_of(attrs: &[syn::Attribute]) -> Result<(ArrowKind, Option<Vec<String>>), String> {
    let mut kind = None;
    let mut values = None;
    for attr in attrs {
        if !matches!(&attr.meta, syn::Meta::List(_)) {
            continue;
        }
        attr.parse_nested_meta(|meta| {
            if !meta.path.is_ident("cowshed_api") {
                consume_value(&meta)?;
                return Ok(());
            }
            meta.parse_nested_meta(|meta| {
                let value = meta
                    .value()?
                    .parse::<syn::LitStr>()
                    .map_err(|error| syn::Error::new(error.span(), error))?
                    .value();
                if meta.path.is_ident("arrow") {
                    kind = Some(match value.as_str() {
                        "uint16" => ArrowKind::UInt16,
                        "uint32" => ArrowKind::UInt32,
                        "uint64" => ArrowKind::UInt64,
                        "int32" => ArrowKind::Int32,
                        "int64" => ArrowKind::Int64,
                        "utf8" => ArrowKind::Utf8,
                        "dict32" => ArrowKind::Dict32,
                        "dict8" => ArrowKind::Dict8,
                        _ => {
                            return Err(meta.error(
                                "arrow must be uint16, uint32, uint64, int32, int64, utf8, dict32 or dict8",
                            ));
                        }
                    });
                } else if meta.path.is_ident("values") {
                    values = Some(value.split('|').map(str::to_owned).collect::<Vec<_>>());
                } else if meta.path.is_ident("arrow_columns") {
                    // The record marker, not a column.
                } else {
                    return Err(meta.error("unknown arrow column attribute"));
                }
                Ok(())
            })
        })
        .map_err(|error| error.to_string())?;
    }
    Ok((kind.ok_or_else(|| "missing arrow type".to_owned())?, values))
}

/// Rust included by the process-span module. It calls that module's array
/// helpers; it does not restate a second schema.
pub fn emit_columns(record: &ArrowRecord) -> String {
    let count = format!("{}_COLUMN_COUNT", record.prefix.to_uppercase());
    let encode = format!("encode_{}_columns", record.prefix);
    let decode = format!("decode_{}_columns", record.prefix);
    let mut out =
        String::from("// Generated by cowshed-api-gen from the column record. Do not edit.\n\n");
    out.push_str(&format!(
        "const {count}: usize = {};\n\n",
        record.columns.len()
    ));
    for column in &record.columns {
        let Some(values) = &column.values else {
            continue;
        };
        let const_name = format!("{}_NAMES", column.name.to_uppercase());
        out.push_str(&format!(
            "pub const {const_name}: [&str; {}] = [{}];\n",
            values.len(),
            values
                .iter()
                .map(|value| format!("{value:?}"))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    if record.columns.iter().any(|column| column.values.is_some()) {
        out.push('\n');
    }
    out.push_str(&format!(
        "pub fn {}() -> Vec<Field> {{\n    vec![\n",
        record.fields
    ));
    for column in &record.columns {
        out.push_str(&format!(
            "        Field::new({:?}, {}, true),\n",
            column.name,
            data_type(column.kind)
        ));
    }
    out.push_str("    ]\n}\n\n");
    out.push_str(&format!(
        "fn {encode}(columns: &{}) -> Result<Vec<ArrayRef>, TraceSegmentError> {{\n    Ok(vec![\n",
        record.struct_name
    ));
    for column in &record.columns {
        out.push_str(&format!("        {},\n", encode_expr(column)));
    }
    out.push_str("    ])\n}\n\n");
    out.push_str(&format!(
        "fn {decode}(batch: &RecordBatch, start: usize) -> Result<{}, {}> {{\n    Ok({} {{\n",
        record.struct_name, record.error, record.struct_name
    ));
    for column in &record.columns {
        out.push_str(&format!(
            "        {name}: {decoded}?,\n",
            name = column.name,
            decoded = decode_expr(column)
        ));
    }
    out.push_str("    })\n}\n");
    if record.merge {
        let merge = format!("merge_{}_columns", record.prefix);
        out.push('\n');
        out.push_str(&format!(
            "fn {merge}(into: &mut {}, other: {}) {{\n",
            record.struct_name, record.struct_name
        ));
        for column in &record.columns {
            out.push_str(&format!(
                "    if other.{name}.is_some() {{\n        into.{name} = other.{name};\n    }}\n",
                name = column.name
            ));
        }
        out.push_str("}\n");
    }
    out
}

fn data_type(kind: ArrowKind) -> &'static str {
    match kind {
        ArrowKind::UInt16 => "DataType::UInt16",
        ArrowKind::UInt32 => "DataType::UInt32",
        ArrowKind::UInt64 => "DataType::UInt64",
        ArrowKind::Int32 => "DataType::Int32",
        ArrowKind::Int64 => "DataType::Int64",
        ArrowKind::Utf8 => "DataType::Utf8",
        ArrowKind::Dict32 => "dict_type(DataType::UInt32)",
        ArrowKind::Dict8 => "dict_type(DataType::UInt8)",
    }
}

fn encode_expr(column: &ArrowColumn) -> String {
    match column.kind {
        ArrowKind::UInt16
        | ArrowKind::UInt32
        | ArrowKind::UInt64
        | ArrowKind::Int32
        | ArrowKind::Int64 => {
            format!(
                "{kind}(columns.{name})",
                kind = encode_fn(column.kind),
                name = column.name
            )
        }
        ArrowKind::Utf8 => format!("text(columns.{name}.as_deref())", name = column.name),
        ArrowKind::Dict32 => format!("category(columns.{name}.as_deref())?", name = column.name),
        ArrowKind::Dict8 => format!(
            "enumeration(columns.{name}, &{const_name}_NAMES)?",
            name = column.name,
            const_name = column.name.to_uppercase()
        ),
    }
}

fn encode_fn(kind: ArrowKind) -> &'static str {
    match kind {
        ArrowKind::UInt16 => "uint16",
        ArrowKind::UInt32 => "uint32",
        ArrowKind::UInt64 => "uint64",
        ArrowKind::Int32 => "int32",
        ArrowKind::Int64 => "int64",
        ArrowKind::Utf8 => "text",
        ArrowKind::Dict32 => "category",
        ArrowKind::Dict8 => "enumeration",
    }
}

fn decode_expr(column: &ArrowColumn) -> String {
    let reader = match column.kind {
        ArrowKind::UInt16 => "optional_u16",
        ArrowKind::UInt32 => "optional_u32",
        ArrowKind::UInt64 => "optional_u64",
        ArrowKind::Int32 => "optional_i32",
        ArrowKind::Int64 => "optional_i64",
        ArrowKind::Utf8 => "optional_text",
        ArrowKind::Dict32 => "optional_dict",
        ArrowKind::Dict8 => {
            let names = format!("{}_NAMES", column.name.to_uppercase());
            return format!("optional_key(batch, start, {:?}, &{names})", column.name);
        }
    };
    format!("{reader}(batch, start, {:?})", column.name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn one_record_emits_the_field_list_and_both_directions() {
        let source = r#"
            #[cfg_attr(any(), cowshed_api(arrow_columns = "process", fields = "process_custom_fields", merge = "true"))]
            struct Sparse {
                #[cfg_attr(any(), cowshed_api(arrow = "uint32"))]
                proc_pid: Option<u32>,
                #[cfg_attr(any(), cowshed_api(arrow = "dict8", values = "|none|lock"))]
                blocked_on: Option<u8>,
            }
        "#;
        let record = parse_arrow_record(source).expect("the record");
        assert_eq!(record.columns.len(), 2);
        let emitted = emit_columns(&record);
        assert!(emitted.contains("const PROCESS_COLUMN_COUNT: usize = 2;"));
        assert!(emitted.contains(r#"Field::new("proc_pid", DataType::UInt32, true)"#));
        assert!(emitted.contains(r#"Field::new("blocked_on", dict_type(DataType::UInt8), true)"#));
        assert!(
            emitted.contains(r#"pub const BLOCKED_ON_NAMES: [&str; 3] = ["", "none", "lock"];"#)
        );
        assert!(emitted.contains("uint32(columns.proc_pid)"));
        assert!(!emitted.contains(".1"));
        assert!(emitted.contains("enumeration(columns.blocked_on, &BLOCKED_ON_NAMES)?"));
        assert!(
            emitted.contains(r#"optional_key(batch, start, "blocked_on", &BLOCKED_ON_NAMES)?"#)
        );
        assert!(emitted.contains("ProcessSpanError"));
        assert!(emitted.contains("fn process_custom_fields()"));
        assert!(emitted.contains("if other.proc_pid.is_some()"));
        assert!(!emitted.contains("api.generated"));
    }

    #[test]
    fn a_dict8_column_without_values_is_refused() {
        let source = r#"
            #[cfg_attr(any(), cowshed_api(arrow_columns = "process"))]
            struct Sparse {
                #[cfg_attr(any(), cowshed_api(arrow = "dict8"))]
                blocked_on: Option<u8>,
            }
        "#;
        let error = parse_arrow_record(source).expect_err("values are required");
        assert!(error.contains("names its values"), "{error}");
    }

    #[test]
    fn an_unattributed_record_uses_its_prefix_and_error() {
        let source = r#"
            #[cfg_attr(any(), cowshed_api(arrow_columns = "unattributed", fields = "unattributed_fields", error = "UnattributedSpanError"))]
            struct SparseReconciliation {
                #[cfg_attr(any(), cowshed_api(arrow = "int64"))]
                unattributed_cpu_us: Option<i64>,
            }
        "#;
        let record = parse_arrow_record(source).expect("the record");
        let emitted = emit_columns(&record);
        assert!(emitted.contains("const UNATTRIBUTED_COLUMN_COUNT: usize = 1;"));
        assert!(emitted.contains("pub fn unattributed_fields()"));
        assert!(emitted.contains("fn encode_unattributed_columns"));
        assert!(emitted.contains("fn decode_unattributed_columns"));
        assert!(emitted.contains("UnattributedSpanError"));
        assert!(emitted.contains("int64(columns.unattributed_cpu_us)"));
        assert!(emitted.contains(r#"optional_i64(batch, start, "unattributed_cpu_us")?"#));
        assert!(emitted.contains("DataType::Int64"));
        assert!(!emitted.contains("ProcessSpanError"));
        assert!(!emitted.contains(".1"));
        assert!(!emitted.contains("merge_unattributed_columns"));
    }

    #[test]
    fn a_job_boundary_preserves_unsigned_sixteen_bit_cores() {
        let record = parse_arrow_record(
            r#"
                /// A real column owner may be documented.
                #[cfg_attr(any(), cowshed_api(arrow_columns = "job_boundary"))]
                struct BoundaryColumns {
                    /// The checked number of online host cores.
                    #[cfg_attr(any(), cowshed_api(arrow = "uint16"))]
                    cores: Option<u16>,
                }
            "#,
        )
        .expect("a checked core count has an exact Arrow column");
        let emitted = emit_columns(&record);
        assert!(emitted.contains("DataType::UInt16"));
        assert!(emitted.contains("uint16(columns.cores)"));
        assert!(emitted.contains(r#"optional_u16(batch, start, "cores")?"#));
    }
}

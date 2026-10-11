//! The public declaration IR: its serialized wire shape, its normalized Rust type graph, and the
//! TypeScript model the owner emitter derives from the same declarations. Expected values are
//! committed goldens or literals written in full; nothing is checked by containment.

use cowshed_api_gen::ir::{
    Api, DeclarationModel, DeclarationVisibility, Field, Fields, Projection, Record, RustType,
    ScalarAccessor, Serde, Shape, TypeArgument, Variant,
};
use cowshed_api_gen::{records, typescript};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};

/// Independent expected wire corpus for `fixture()`, written from the serde contract.
const WIRE_GOLDEN: &str = include_str!("goldens/ir-serde-fixture.json");
/// Raw owner bytes: `cowshed-api-gen ir-types ./declaration-model.js`.
const TYPES_GOLDEN: &str = include_str!("goldens/ir-types.ts.txt");
/// Raw owner bytes: `cowshed-api-gen ir-validators ./declaration-model.js`.
const VALIDATORS_GOLDEN: &str = include_str!("goldens/ir-validators.ts.txt");

const FIXTURE_MODULE: &str = "cowshed_core::api::fixture";

#[test]
fn serialized_declarations_carry_the_actual_owner_metadata_and_type_graph() {
    let source = r#"
        /// A source-owned declaration.
        #[derive(Serialize, Deserialize)]
        #[serde(rename_all = "camelCase", deny_unknown_fields)]
        pub struct Example<'a> {
            #[serde(rename = "borrowedValue", default, skip_serializing_if = "Option::is_none")]
            pub borrowed: Option<&'a mut qualified::Token<'static, 32>>,
            pub bytes: [u8; LIMIT + 1],
        }
    "#;
    let mut api = Api::default();
    records::parse(source, &mut api, FIXTURE_MODULE)
        .expect("the actual owner parser reads the declaration");
    let serialized = serde_json::to_value(&api).expect("the public IR serializes itself");
    let record = &serialized["records"]["Example"];
    assert_eq!(record["module"], FIXTURE_MODULE);
    assert_eq!(record["visibility"], json!({"kind": "public"}));
    assert_eq!(record["serde"]["renameAll"], "camelCase");
    assert_eq!(record["serde"]["denyUnknownFields"], true);
    let fields = &record["shape"]["data"]["data"];
    assert_eq!(fields[0]["serde"]["rename"], "borrowedValue");
    assert_eq!(fields[0]["serde"]["default"], true);
    assert_eq!(fields[0]["serde"]["skipSerializingIf"], "Option::is_none");
    assert_eq!(
        fields[1]["ty"],
        json!({
            "kind": "array", "element": {"kind":"named","name":"u8","arguments":[]},
            "length": "LIMIT + 1"
        })
    );
    let reference = &fields[0]["ty"]["arguments"][0]["value"];
    assert_eq!(reference["kind"], "reference");
    assert_eq!(reference["mutable"], true);
    assert_eq!(reference["lifetime"], "a");
    assert_eq!(reference["element"]["name"], "qualified::Token");
    assert_eq!(
        reference["element"]["arguments"],
        json!([
            {"kind":"lifetime","name":"static"}, {"kind":"const","expression":"32"}
        ])
    );
}

#[test]
fn public_declaration_types_and_validators_are_the_owner_emitter_goldens() {
    let output = cowshed_api_gen::ir_types("./declaration-model.js")
        .expect("the actual public IR is a valid source for the owner emitter");
    assert_eq!(
        output.types, TYPES_GOLDEN,
        "tests/goldens/ir-types.ts.txt is stale; regenerate with `cowshed-api-gen ir-types ./declaration-model.js`"
    );
    assert_eq!(
        output.validators, VALIDATORS_GOLDEN,
        "tests/goldens/ir-validators.ts.txt is stale; regenerate with `cowshed-api-gen ir-validators ./declaration-model.js`"
    );
}

#[test]
fn numeric_refinements_import_tags_from_the_owner_emitter() {
    let mut api = Api::default();
    records::parse(
        "#[derive(Serialize)] pub struct Tagged { pub count: u32 }",
        &mut api,
        FIXTURE_MODULE,
    )
    .expect("the source declares a numeric refinement");
    let tagged = typescript::emit(&api, "numeric source", "./tagged.js")
        .expect("the actual owner emits its numeric refinement");
    assert!(tagged.types.contains("import type { tags }"));
}

#[test]
fn serialized_public_ir_matches_the_committed_wire_golden() {
    let actual = serde_json::to_value(fixture()).expect("the public IR serializes itself");
    let golden: Value = serde_json::from_str(WIRE_GOLDEN).expect("the wire golden is JSON");
    assert_eq!(actual, golden);
}

#[test]
fn every_serialized_member_and_leaf_is_pinned_by_the_golden() {
    // Controls on the comparison itself: each object member and each scalar leaf of the golden is
    // load-bearing. A serializer that drops it, or an expectation that lost it or got a different
    // value, is a mismatch rather than a silent pass.
    let actual = serde_json::to_value(fixture()).expect("the fixture serializes");
    let golden: Value = serde_json::from_str(WIRE_GOLDEN).expect("the wire golden is JSON");
    let (mut members, mut leaves) = (Vec::new(), Vec::new());
    collect_pointers(&golden, "", &mut members, &mut leaves);
    assert!(
        !members.is_empty() && !leaves.is_empty(),
        "the golden walk reaches the fixture"
    );
    for pointer in &members {
        let mut dropped = actual.clone();
        remove_member(&mut dropped, pointer);
        assert_ne!(
            dropped, golden,
            "a serializer that drops {pointer} must be refused"
        );
        let mut omitted = golden.clone();
        remove_member(&mut omitted, pointer);
        assert_ne!(
            actual, omitted,
            "an expectation missing {pointer} must be refused"
        );
    }
    for pointer in &leaves {
        let mut wrong = golden.clone();
        *wrong
            .pointer_mut(pointer)
            .expect("each leaf pointer names a value") = json!("planted wrong expectation");
        assert_ne!(
            actual, wrong,
            "a wrong expected value at {pointer} must be refused"
        );
    }
}

/// Records the JSON pointer of every object member and every scalar leaf under `at`.
fn collect_pointers(value: &Value, at: &str, members: &mut Vec<String>, leaves: &mut Vec<String>) {
    match value {
        Value::Object(map) => {
            for (key, child) in map {
                assert!(
                    !key.contains(['/', '~']),
                    "{key} is a plain JSON pointer segment"
                );
                let pointer = format!("{at}/{key}");
                members.push(pointer.clone());
                collect_pointers(child, &pointer, members, leaves);
            }
        }
        Value::Array(items) => {
            for (index, child) in items.iter().enumerate() {
                collect_pointers(child, &format!("{at}/{index}"), members, leaves);
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {
            leaves.push(at.to_owned());
        }
    }
}

fn remove_member(value: &mut Value, pointer: &str) {
    let (parent, key) = pointer
        .rsplit_once('/')
        .expect("a member pointer names its parent");
    value
        .pointer_mut(parent)
        .and_then(Value::as_object_mut)
        .expect("a member's parent is an object")
        .remove(key)
        .expect("the member is present");
}

/// The declaration model that exercises every serde and projection fact, every variant of the
/// normalized type graph, every type argument, every field shape and every declaration visibility.
///
/// The struct literals name every field of every IR type, so a field added to the IR fails to
/// compile here until the wire golden pins its spelling.
fn fixture() -> DeclarationModel {
    let records = BTreeMap::from([
        ("Borrowed".to_owned(), borrowed()),
        ("Choice".to_owned(), choice()),
        ("Coordinates".to_owned(), coordinates()),
        ("Marker".to_owned(), marker()),
        ("Wire".to_owned(), wire()),
    ]);
    let modules = records
        .values()
        .map(|record| (record.name.clone(), record.module.clone()))
        .collect();
    DeclarationModel {
        api: Api {
            records,
            nullable_results: BTreeSet::from(["Choice".to_owned(), "Wire".to_owned()]),
        },
        modules,
    }
}

fn wire() -> Record {
    Record {
        name: "Wire".to_owned(),
        module: FIXTURE_MODULE.to_owned(),
        visibility: DeclarationVisibility::Public,
        docs: strings(&["A source-owned declaration.", "Second docs line."]),
        serde: every_serde_fact(),
        projection: every_projection_fact(),
        shape: Shape::Struct(Fields::Named(vec![
            field(
                Some("answer"),
                named(
                    "Option",
                    vec![type_argument(named(
                        "qualified::Token",
                        vec![
                            TypeArgument::Lifetime {
                                name: "static".to_owned(),
                            },
                            TypeArgument::Const {
                                expression: "32".to_owned(),
                            },
                        ],
                    ))],
                ),
                &["First field."],
                every_serde_fact(),
            ),
            field(
                Some("bytes"),
                RustType::Array {
                    element: Box::new(named("u8", Vec::new())),
                    length: "LIMIT + 1".to_owned(),
                },
                &[],
                Serde::default(),
            ),
            field(
                Some("borrowed"),
                RustType::Reference {
                    element: Box::new(named("Token", Vec::new())),
                    lifetime: Some("a".to_owned()),
                    mutable: true,
                },
                &[],
                Serde::default(),
            ),
        ])),
        exported: true,
    }
}

fn choice() -> Record {
    Record {
        name: "Choice".to_owned(),
        module: FIXTURE_MODULE.to_owned(),
        visibility: DeclarationVisibility::Restricted {
            path: "crate::api::fixture".to_owned(),
        },
        docs: strings(&["An externally tagged enum."]),
        serde: Serde {
            tag: Some("kind".to_owned()),
            content: Some("data".to_owned()),
            rename_all_fields: Some("kebab-case".to_owned()),
            ..Serde::default()
        },
        projection: Projection {
            unit_value: Some("false".to_owned()),
            ..Projection::default()
        },
        shape: Shape::Enum(vec![
            Variant {
                name: "Off".to_owned(),
                docs: strings(&["Disabled."]),
                serde: every_serde_fact(),
                fields: Fields::Unit,
            },
            Variant {
                name: "On".to_owned(),
                docs: Vec::new(),
                serde: Serde::default(),
                fields: Fields::Named(vec![field(
                    Some("level"),
                    named("u8", Vec::new()),
                    &[],
                    every_serde_fact(),
                )]),
            },
            Variant {
                name: "Pair".to_owned(),
                docs: strings(&["Tuple payload."]),
                serde: Serde::default(),
                fields: Fields::Tuple(vec![field(
                    None,
                    named("u16", Vec::new()),
                    &[],
                    Serde::default(),
                )]),
            },
        ]),
        exported: true,
    }
}

fn coordinates() -> Record {
    Record {
        name: "Coordinates".to_owned(),
        module: format!("{FIXTURE_MODULE}::inner"),
        visibility: DeclarationVisibility::Crate,
        docs: Vec::new(),
        serde: Serde::default(),
        projection: Projection::default(),
        shape: Shape::Struct(Fields::Tuple(vec![
            field(
                None,
                RustType::Tuple {
                    elements: vec![
                        named("String", Vec::new()),
                        RustType::Slice {
                            element: Box::new(named("u8", Vec::new())),
                        },
                    ],
                },
                &["Tuple field."],
                Serde {
                    skip: true,
                    ..Serde::default()
                },
            ),
            field(
                None,
                RustType::Tuple {
                    elements: Vec::new(),
                },
                &[],
                Serde::default(),
            ),
        ])),
        exported: false,
    }
}

fn marker() -> Record {
    Record {
        name: "Marker".to_owned(),
        module: FIXTURE_MODULE.to_owned(),
        visibility: DeclarationVisibility::Private,
        docs: Vec::new(),
        serde: Serde::default(),
        projection: Projection::default(),
        shape: Shape::Struct(Fields::Unit),
        exported: false,
    }
}

fn borrowed() -> Record {
    Record {
        name: "Borrowed".to_owned(),
        module: FIXTURE_MODULE.to_owned(),
        visibility: DeclarationVisibility::Super,
        docs: Vec::new(),
        serde: Serde::default(),
        projection: Projection::default(),
        shape: Shape::Alias(RustType::Reference {
            element: Box::new(named("str", Vec::new())),
            lifetime: None,
            mutable: false,
        }),
        exported: false,
    }
}

/// Every serde fact set to a non-default value, so each wire spelling is pinned by a value that
/// changes if the fact is dropped or renamed. The literal names all sixteen facts.
fn every_serde_fact() -> Serde {
    Serde {
        rename: Some("wireName".to_owned()),
        rename_all: Some("camelCase".to_owned()),
        rename_all_fields: Some("kebab-case".to_owned()),
        tag: Some("kind".to_owned()),
        content: Some("data".to_owned()),
        untagged: true,
        transparent: true,
        default: true,
        default_with: Some("make_default".to_owned()),
        skip_serializing_if: Some("Option::is_none".to_owned()),
        skip: true,
        flatten: true,
        deny_unknown_fields: true,
        into: Some("Target".to_owned()),
        from: Some("Source".to_owned()),
        deserialize_with: Some("decode".to_owned()),
    }
}

/// Every projection fact set to a non-default value. The literal names every actual owner field.
fn every_projection_fact() -> Projection {
    Projection {
        wire: Some("WireForm".to_owned()),
        scalar: Some("number & tags.Minimum<1>".to_owned()),
        accessor: Some(ScalarAccessor::Get),
        exclusive: vec!["a".to_owned(), "b".to_owned()],
        name: Some("PublicName".to_owned()),
        unit_value: Some("true".to_owned()),
        output_only: true,
    }
}

fn named(name: &str, arguments: Vec<TypeArgument>) -> RustType {
    RustType::Named {
        name: name.to_owned(),
        arguments,
    }
}

fn type_argument(value: RustType) -> TypeArgument {
    TypeArgument::Type { value }
}

fn strings(lines: &[&str]) -> Vec<String> {
    lines.iter().map(|line| (*line).to_owned()).collect()
}

fn field(name: Option<&str>, ty: RustType, docs: &[&str], serde: Serde) -> Field {
    Field {
        name: name.map(str::to_owned),
        ty,
        docs: strings(docs),
        serde,
    }
}

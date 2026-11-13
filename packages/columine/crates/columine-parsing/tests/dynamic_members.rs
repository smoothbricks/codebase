//! The dynamic-member rule as a consumer outside the crate reaches it: the
//! members whose explicit null the extractors keep as data.

use std::convert::Infallible;

use columine_parsing::validate::{SemanticSchema, collect_dynamic_members, parse_schema_envelope};

/// Every `(event type, member)` the envelope's schemas declare dynamic.
fn dynamic_members(envelope: &[u8]) -> Vec<(String, String)> {
    let schemas = parse_schema_envelope(envelope).unwrap();
    let mut members = Vec::new();
    for (event_type, schema) in schemas.iter() {
        collect_dynamic_members(schema, &mut |member| {
            members.push((event_type.clone(), member.to_owned()));
            Ok::<(), Infallible>(())
        })
        .unwrap();
    }
    members
}

#[test]
fn an_object_root_names_its_dynamic_members_only() {
    assert_eq!(
        dynamic_members(
            br#"{"hi":{"kind":"object","fields":{"data":{"kind":"optional","value":{"kind":"unknown"}},"raw":{"kind":"unknown"},"note":{"kind":"optional","value":{"kind":"string"}},"shape":{"kind":"optional","value":{"kind":"array","item":{"kind":"unknown"}}}}}}"#
        ),
        [("hi".to_owned(), "data".to_owned()), ("hi".to_owned(), "raw".to_owned())]
    );
}

#[test]
fn a_union_root_names_a_member_any_arm_declares_dynamic() {
    assert_eq!(
        dynamic_members(
            br#"{"done":{"kind":"union","variants":[{"kind":"object","fields":{"kind":{"kind":"enum","variants":["verdict"]},"returnValue":{"kind":"optional","value":{"kind":"unknown"}}}},{"kind":"object","fields":{"kind":{"kind":"enum","variants":["notify"]},"returnValue":{"kind":"optional","value":{"kind":"string"}},"extra":{"kind":"unknown"}}}]},"plain":{"kind":"string"}}"#
        ),
        [
            ("done".to_owned(), "returnValue".to_owned()),
            ("done".to_owned(), "extra".to_owned()),
        ]
    );
}

#[test]
fn a_dynamic_schema_is_unknown_beneath_any_optional() {
    let dynamic = |tree: &[u8]| SemanticSchema::from_tree_bytes(tree).unwrap().is_dynamic();
    assert!(dynamic(br#"{"kind":"unknown"}"#));
    assert!(dynamic(
        br#"{"kind":"optional","value":{"kind":"optional","value":{"kind":"unknown"}}}"#
    ));
    assert!(!dynamic(
        br#"{"kind":"optional","value":{"kind":"object","fields":{}}}"#
    ));
    assert!(!dynamic(br#"{"kind":"binary"}"#));
}

use cowshed_api_gen::{ir::ScalarAccessor, records};
use serde_json::json;

#[test]
fn source_accessors_are_closed_metadata_and_never_guessed() {
    let source = r#"
        #[cfg_attr(any(), cowshed_api(accessor = "get", scalar = "number & tags.Type<'uint32'>"))]
        pub struct Count(u16);
        #[cfg_attr(any(), cowshed_api(scalar = "string", accessor = "as-str"))]
        pub struct Text(String);
        #[cfg_attr(any(), cowshed_api(accessor = "display", scalar = "string & tags.Pattern<'^[0-9a-f]{16}$'>"))]
        pub struct Birth(u64);
        hex_identifier!(Trace, 16, InvalidTrace);
    "#;
    let mut api = cowshed_api_gen::ir::Api::default();
    records::parse(source, &mut api, "fixture::units").expect("real source metadata parses");
    api.refresh_layouts();
    for (name, accessor, wire) in [
        ("Count", ScalarAccessor::Get, "get"),
        ("Text", ScalarAccessor::AsStr, "as-str"),
        ("Birth", ScalarAccessor::Display, "display"),
        ("Trace", ScalarAccessor::AsStr, "as-str"),
    ] {
        let record = api
            .records
            .get(name)
            .expect("the owner parser retained the declaration");
        assert_eq!(record.projection.accessor, Some(accessor));
        assert_eq!(
            serde_json::to_value(&record.projection).expect("owner metadata serializes")["accessor"],
            json!(wire)
        );
    }

    // Each refusal names the metadata key it refuses; the wording around it is the parser's.
    for (annotation, key) in [
        (
            "accessor = \"private-hex\", scalar = \"string\"",
            "accessor",
        ),
        ("accessor = \"get\"", "scalar"),
    ] {
        let mut invalid = cowshed_api_gen::ir::Api::default();
        let source =
            format!("#[cfg_attr(any(), cowshed_api({annotation}))] pub struct Wrong(u64);");
        let refusal = records::parse(&source, &mut invalid, "fixture::units")
            .expect_err("invalid source accessor metadata is refused");
        assert!(refusal.to_string().contains(key), "{refusal}");
        assert!(!invalid.records.contains_key("Wrong"), "{annotation}");
    }
}

//! Native error properties are a projection of the canonical CowshedError declaration, not
//! another list of causes in the addon. Serde builds the JS values directly; no JSON text or
//! handwritten tagged-union encoder stands between a core refusal and its native cause.

use crate::ir::{Api, Fields, Shape};
use std::fmt::Write;

pub fn emit(api: &Api) -> Result<String, String> {
    let record = api
        .records
        .get("CowshedError")
        .ok_or("CowshedError has no canonical declaration")?;
    let Shape::Struct(Fields::Named(fields)) = &record.shape else {
        return Err("CowshedError is not a record of named fields".to_owned());
    };
    let mut output = String::from(
        "\n/// The canonical core error, with every serialized detail retained on the JS Error.\n\
         pub(super) fn cowshed_error(env: Env, source: cowshed_core::CowshedError) -> napi::Result<napi::Error> {\n\
         \x20   let details = env.to_js_value(&source)?.coerce_to_object()?;\n\
         \x20   let mut error = napi::JsError::from(napi::Error::new(source.code.as_str(), source.message))\n\
         \x20       .into_unknown(env).coerce_to_object()?;\n",
    );
    for field in fields.iter().filter(|field| !field.serde.skip) {
        if field.serde.flatten {
            return Err("CowshedError cannot flatten native error properties".to_owned());
        }
        let name = field
            .name
            .as_deref()
            .ok_or("CowshedError has an unnamed field")?;
        let wire = crate::typescript::renamed(
            name,
            field.serde.rename.as_deref(),
            record.serde.rename_all.as_deref(),
        )?;
        if wire == "code" || wire == "message" {
            continue;
        }
        writeln!(output,
            "    if details.has_named_property({wire:?})? {{\n        error.set_named_property({wire:?}, details.get_named_property::<napi::JsUnknown>({wire:?})?)?;\n    }}"
        ).unwrap();
    }
    output.push_str("    Ok(napi::Error::from(error.into_unknown()))\n}\n");
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn declaration(source: &str) -> Api {
        let mut api = Api::default();
        crate::records::parse(source, &mut api).expect("canonical error");
        api
    }

    #[test]
    fn native_error_details_follow_canonical_field_spellings() {
        let source = r#"
            #[derive(Serialize, Deserialize)]
            #[serde(rename_all = "camelCase")]
            pub struct CowshedError {
                pub code: ErrorCode,
                pub message: String,
                pub hint: String,
                #[serde(default, skip_serializing_if = "Option::is_none")]
                stdin: Option<Box<StdinRefusal>>,
                #[serde(rename = "otherBuild", default, skip_serializing_if = "Option::is_none")]
                other_build: Option<Box<OtherBuild>>,
                #[serde(skip)]
                private_state: Option<PrivateState>,
            }
        "#;
        let output = emit(&declaration(source)).expect("native projection");
        assert!(output.contains("env.to_js_value(&source)?"), "{output}");
        for property in ["hint", "stdin", "otherBuild"] {
            assert!(
                output.contains(&format!("error.set_named_property({property:?},")),
                "{output}"
            );
        }
        assert!(!output.contains("private_state"), "{output}");
        assert!(!output.contains("serde_json"), "{output}");
        assert!(!output.contains(".clone()"), "{output}");
        let changed = emit(&declaration(
            &source.replace("stdin: Option", "admission: Option"),
        ))
        .expect("changed projection");
        assert!(
            changed.contains("error.set_named_property(\"admission\","),
            "{changed}"
        );
        assert!(!changed.contains("\"stdin\""), "{changed}");
    }
}

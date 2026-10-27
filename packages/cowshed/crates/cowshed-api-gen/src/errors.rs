//! Native error properties are a projection of the canonical CowshedError declaration, not
//! another list of causes in the addon. Serde builds the JS values directly; no JSON text or
//! handwritten tagged-union encoder stands between a core refusal and its native cause.

use crate::ir::Api;

pub fn emit(api: &Api) -> Result<String, String> {
    if !api.records.contains_key("CowshedError") {
        return Err("CowshedError has no canonical declaration".to_owned());
    }
    // Serde owns field names, omission and flattening. Enumerate its actual own properties;
    // deriving a second field list here rejected flattened causes and drifted from the wire.
    Ok(String::from(
        r#"
/// The canonical core error, with every serialized detail retained on the JS Error.
pub(super) fn cowshed_error(env: Env, source: cowshed_core::CowshedError) -> napi::Result<napi::Error> {
    let details = env.to_js_value(&source)?.coerce_to_object()?;
    let mut error = napi::JsError::from(napi::Error::new(source.code.as_str(), source.message))
        .into_unknown(env).coerce_to_object()?;
    let names = details.get_all_property_names(
        napi::KeyCollectionMode::OwnOnly,
        napi::KeyFilter::AllProperties,
        napi::KeyConversion::NumbersToStrings,
    )?;
    for index in 0..names.get_array_length()? {
        let name = names.get_element::<napi::JsString>(index)?.into_utf8()?;
        let name = name.as_str()?;
        if name != "code" && name != "message" {
            error.set_named_property(name, details.get_named_property::<napi::JsUnknown>(name)?)?;
        }
    }
    Ok(napi::Error::from(error.into_unknown()))
}
"#,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn declaration(source: &str) -> Api {
        let mut api = Api::default();
        crate::records::parse(source, &mut api, "cowshed_core::error").expect("canonical error");
        api.refresh_layouts();
        api
    }

    #[test]
    fn native_error_details_follow_canonical_field_spellings() {
        let source = r#"
    #[derive(Serialize, Deserialize)]
    pub struct CowshedError {
        pub code: ErrorCode,
        pub message: String,
        pub hint: String,
        #[serde(flatten)]
        execution: Option<Box<ExecutionRefusal>>,
        #[serde(skip)]
        private_state: Option<PrivateState>,
    }
"#;
        let output = emit(&declaration(source)).expect("native flattened projection");
        for required in [
            "env.to_js_value(&source)?",
            "get_all_property_names(",
            "napi::KeyCollectionMode::OwnOnly",
            "error.set_named_property(name,",
        ] {
            assert!(output.contains(required), "{output}");
        }
        assert!(!output.contains("private_state"), "{output}");
        assert!(!output.contains("serde_json"), "{output}");
        assert!(!output.contains(".clone()"), "{output}");
        let changed = emit(&declaration(
            &source.replace("execution:", "another_cause:"),
        ))
        .expect("renamed private storage");
        assert_eq!(
            output, changed,
            "native properties follow Serde, not private storage names"
        );
    }
}

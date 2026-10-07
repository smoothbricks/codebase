//! Handle-local methods that are not controller operations. Their canonical declarations are
//! the annotated inherent methods in cowshed-core: the addon calls those methods, so state such
//! as JobStdin's delivered-byte cursor is never implemented again at the projection boundary.

use crate::ir::Api;
use std::collections::BTreeMap;
use std::fmt::Write;
use syn::{FnArg, GenericArgument, ImplItem, Item, LitStr, Pat, PathArguments, ReturnType, Type};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Receiver {
    Job,
    Stdin,
}

impl Receiver {
    pub fn field(self) -> &'static str {
        match self {
            Self::Job => "inner",
            Self::Stdin => "stdin",
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum Input {
    None,
    Bytes(String),
    OptionalRecord { name: String, record: String },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Answer {
    Unit,
    Attachment,
}

#[derive(Debug, Eq, PartialEq)]
pub struct Method {
    pub class: String,
    pub name: String,
    pub core: String,
    pub receiver: Receiver,
    pub input: Input,
    pub answer: Answer,
}

/// A method's `cowshed_api(napi = "Class.method")` declares its native home and spelling;
/// its actual Rust signature declares its arguments, receiver and answer. Unsupported shapes
/// are refused by the producer instead of falling back to an untyped operation adapter.
pub fn parse(source: &str) -> syn::Result<Vec<Method>> {
    let mut methods = Vec::new();
    for item in syn::parse_file(source)?.items {
        let Item::Impl(item) = item else { continue };
        if item.trait_.is_some() {
            continue;
        }
        for member in item.items {
            let ImplItem::Fn(method) = member else {
                continue;
            };
            let mut projection = None;
            for attr in method
                .attrs
                .iter()
                .filter(|attr| attr.path().is_ident("cfg_attr"))
            {
                attr.parse_nested_meta(|meta| {
                    if meta.path.is_ident("cowshed_api") {
                        meta.parse_nested_meta(|meta| {
                            if !meta.path.is_ident("napi") {
                                return Err(meta.error("unknown capability projection attribute"));
                            }
                            let value = meta.value()?.parse::<LitStr>()?;
                            if projection.replace(value).is_some() {
                                return Err(
                                    meta.error("a capability method has one native projection")
                                );
                            }
                            Ok(())
                        })
                    } else {
                        if meta.input.peek(syn::token::Paren) {
                            let content;
                            syn::parenthesized!(content in meta.input);
                            let _ = content.parse::<proc_macro2::TokenStream>()?;
                        }
                        Ok(())
                    }
                })?;
            }
            let Some(projection) = projection else {
                continue;
            };
            let fail = |message: &str| syn::Error::new_spanned(&method.sig, message);
            let value = projection.value();
            let (class, name) = value
                .split_once('.')
                .ok_or_else(|| syn::Error::new_spanned(&projection, "napi names Class.method"))?;
            syn::parse_str::<syn::Ident>(name)?;
            let receiver = match (simple_name(&item.self_ty).as_deref(), class) {
                (Some("JobHandle"), "JobHandle" | "JobAttachment") => Receiver::Job,
                (Some("JobStdin"), "JobAttachment") => Receiver::Stdin,
                _ => {
                    return Err(fail(
                        "the native class must hold the declared core receiver",
                    ));
                }
            };
            if method.sig.asyncness.is_none() || !method.sig.generics.params.is_empty() {
                return Err(fail(
                    "a projected capability method is async and nongeneric",
                ));
            }
            let mut arguments = method.sig.inputs.iter();
            match arguments.next() {
                Some(FnArg::Receiver(receiver))
                    if receiver.reference.is_some() && receiver.mutability.is_none() => {}
                _ => return Err(fail("a projected capability method borrows &self")),
            }
            let input = match arguments.next() {
                None => Input::None,
                Some(FnArg::Typed(argument)) => {
                    let Pat::Ident(name) = &*argument.pat else {
                        return Err(fail("a projected argument has one identifier"));
                    };
                    let name = name.ident.to_string();
                    if simple_name(&argument.ty).as_deref() == Some("Bytes") {
                        Input::Bytes(name)
                    } else if let Some(record) =
                        type_argument(&argument.ty, "Option").and_then(simple_name)
                    {
                        Input::OptionalRecord { name, record }
                    } else {
                        return Err(fail(
                            "a projected argument is Bytes or an optional API record",
                        ));
                    }
                }
                Some(FnArg::Receiver(_)) => {
                    return Err(fail("a projected method has one receiver"));
                }
            };
            if arguments.next().is_some() {
                return Err(fail("a projected method takes at most one argument"));
            }
            let ReturnType::Type(_, result) = &method.sig.output else {
                return Err(fail("a projected capability method returns Result"));
            };
            let answer = match type_argument(result, "Result") {
                Some(Type::Tuple(tuple)) if tuple.elems.is_empty() => Answer::Unit,
                Some(ty) if simple_name(ty).as_deref() == Some("JobAttachment") => {
                    Answer::Attachment
                }
                _ => return Err(fail("a projected capability answers unit or JobAttachment")),
            };
            if answer == Answer::Attachment && (class != "JobHandle" || receiver != Receiver::Job) {
                return Err(fail("a JobHandle mints its attachment"));
            }
            if methods
                .iter()
                .any(|method: &Method| method.class == class && method.name == name)
            {
                return Err(fail("a native capability method is declared once"));
            }
            methods.push(Method {
                class: class.to_owned(),
                name: name.to_owned(),
                core: method.sig.ident.to_string(),
                receiver,
                input,
                answer,
            });
        }
    }
    Ok(methods)
}

fn simple_name(ty: &Type) -> Option<String> {
    let Type::Path(ty) = ty else { return None };
    let segment = ty.path.segments.last()?;
    matches!(segment.arguments, PathArguments::None).then(|| segment.ident.to_string())
}

/// Emits direct calls to the annotated core methods. Only argument/result shapes vary here;
/// there is no controller operation name, offset calculation or per-operation dispatch.
pub fn emit(methods: &[Method], api: &Api) -> Result<(String, String), String> {
    let mut rust = String::new();
    let mut typescript = String::new();
    let mut classes: BTreeMap<&str, Vec<&Method>> = BTreeMap::new();
    for method in methods {
        classes.entry(&method.class).or_default().push(method);
    }
    // napi-derive emits references to the receiver's last path identifier, so the class must
    // be imported into the generated module, not merely named as `super::JobAttachment`.
    if classes.contains_key("JobAttachment") {
        rust.push_str("\nuse super::JobAttachment;\n");
    }
    for (class, methods) in classes {
        writeln!(rust, "\n#[napi]\nimpl {class} {{").unwrap();
        writeln!(typescript, "\n/** Handle-local methods declared by the core capability. */\nexport interface Native{class}Capabilities {{").unwrap();
        let mut wrappers = String::new();
        for method in methods {
            let field = method.receiver.field();
            let native_answer = match method.answer {
                Answer::Unit => "void",
                Answer::Attachment => "NativeJobAttachmentHandle",
            };
            let (
                rust_parameter,
                preparation,
                core_argument,
                ts_parameter,
                native_parameter,
                native_argument,
            ) = match &method.input {
                Input::None => (
                    String::new(),
                    String::new(),
                    String::new(),
                    String::new(),
                    String::new(),
                    String::new(),
                ),
                Input::Bytes(name) => (
                    format!(", {name}: Buffer"),
                    format!("        let {name} = super::owned_bytes({name});\n"),
                    name.clone(),
                    format!(", {name}: Uint8Array"),
                    format!("{name}: Buffer"),
                    format!("Buffer.from({name}.buffer, {name}.byteOffset, {name}.byteLength)"),
                ),
                Input::OptionalRecord { name, record } => {
                    let declaration = api.records.get(record).ok_or_else(|| {
                        format!(
                            "{class}.{}: API type {record} has no declaration",
                            method.name
                        )
                    })?;
                    let public_name = declaration.projection.name.as_deref().unwrap_or(record);
                    (
                        format!(", {name}: Option<String>"),
                        String::new(),
                        format!(
                            "super::optional_argument::<cowshed_core::api::{record}>({:?}, {name}.as_deref())?",
                            format!("{class}.{}", method.name)
                        ),
                        format!(", {name}?: Api.{public_name}"),
                        format!("{name}Json?: string"),
                        format!("{name} === undefined ? undefined : JSON.stringify({name})"),
                    )
                }
            };
            let result = match method.answer {
                Answer::Unit => "Ok(())".to_owned(),
                Answer::Attachment => "Ok(super::attachment(inner, result))".to_owned(),
            };
            let binding = if method.answer == Answer::Attachment {
                "let result = "
            } else {
                ""
            };
            writeln!(
                rust,
                "    #[napi(js_name = {:?})]\n    pub fn {}(&self, env: Env{rust_parameter}) -> napi::Result<JsObject> {{\n        let {field} = Arc::clone(&self.{field});\n{preparation}        super::spawn_promise(env, async move {{\n            {binding}{field}.{}({core_argument}).await?;\n            {result}\n        }})\n    }}",
                method.name, method.name, method.core,
            ).unwrap();
            writeln!(
                typescript,
                "  {}({native_parameter}): Promise<{native_answer}>;",
                method.name
            )
            .unwrap();
            let mut prefix = class.to_owned();
            prefix[..1].make_ascii_lowercase();
            let mut suffix = method.name.clone();
            suffix[..1].make_ascii_uppercase();
            writeln!(
                wrappers,
                "\nexport async function {prefix}{suffix}(handle: Native{class}Capabilities{ts_parameter}): Promise<{native_answer}> {{\n  return handle.{}({native_argument});\n}}",
                method.name,
            ).unwrap();
        }
        rust.push_str("}\n");
        typescript.push_str("}\n");
        typescript.push_str(&wrappers);
    }
    Ok((rust, typescript))
}

fn type_argument<'a>(ty: &'a Type, name: &str) -> Option<&'a Type> {
    let Type::Path(ty) = ty else { return None };
    let segment = ty.path.segments.last()?;
    if segment.ident != name {
        return None;
    }
    let PathArguments::AngleBracketed(arguments) = &segment.arguments else {
        return None;
    };
    if arguments.args.len() != 1 {
        return None;
    }
    let GenericArgument::Type(ty) = arguments.args.first()? else {
        return None;
    };
    Some(ty)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_methods_use_core_state_and_preserve_byte_views() {
        let source = r#"
            impl JobStdin {
                #[cfg_attr(any(), cowshed_api(napi = "JobAttachment.write"))]
                pub async fn write(&self, payload: Bytes) -> Result<()> {}
                #[cfg_attr(any(), cowshed_api(napi = "JobAttachment.end"))]
                pub async fn finish_input(&self) -> Result<()> {}
            }
            impl JobHandle {
                #[cfg_attr(any(), cowshed_api(napi = "JobHandle.attach"))]
                pub async fn attach(&self, resume: Option<JobJournalCursor>) -> Result<JobAttachment> {}
                #[cfg_attr(any(), cowshed_api(napi = "JobAttachment.detach"))]
                pub async fn detach(&self) -> Result<()> {}
            }
        "#;
        let mut api = Api::default();
        crate::records::parse(
            "#[derive(Serialize, Deserialize)] pub struct JobJournalCursor { pub stdout: u64, pub stderr: u64 }",
            &mut api,
        ).expect("cursor declaration");
        let methods = parse(source).expect("capability declarations");
        let (rust, typescript) = emit(&methods, &api).expect("projection");
        assert!(
            rust.contains("let payload = super::owned_bytes(payload);"),
            "{rust}"
        );
        assert!(rust.contains("stdin.write(payload).await?;"), "{rust}");
        assert!(rust.contains("stdin.finish_input().await?;"), "{rust}");
        assert!(rust.contains("inner.detach().await?;"), "{rust}");
        assert!(
            rust.contains("optional_argument::<cowshed_core::api::JobJournalCursor>"),
            "{rust}"
        );
        assert!(
            rust.contains("Ok(super::attachment(inner, result))"),
            "{rust}"
        );
        assert!(!rust.contains("operations::"), "{rust}");
        assert!(!rust.contains("offset"), "{rust}");
        assert!(
            typescript
                .contains("Buffer.from(payload.buffer, payload.byteOffset, payload.byteLength)"),
            "{typescript}"
        );
        assert!(
            typescript.contains("resume?: Api.JobJournalCursor"),
            "{typescript}"
        );
        assert!(
            typescript.contains("Promise<NativeJobAttachmentHandle>"),
            "{typescript}"
        );
        assert!(!typescript.contains("offset"), "{typescript}");
        let mutated =
            parse(&source.replace("finish_input", "finish_again")).expect("mutated declaration");
        let (mutated, _) = emit(&mutated, &api).expect("mutated projection");
        assert!(
            mutated.contains("stdin.finish_again().await?;"),
            "{mutated}"
        );
        assert!(!mutated.contains("finish_input"), "{mutated}");
    }

    #[test]
    fn methods_keep_the_core_signature_and_native_spelling() {
        let methods = parse(r#"
            impl JobStdin {
                #[cfg_attr(any(), cowshed_api(napi = "JobAttachment.end"))]
                pub async fn close(&self) -> Result<()> { core_close().await }
            }
            impl JobHandle {
                #[cfg_attr(any(), cowshed_api(napi = "JobHandle.attach"))]
                pub async fn attach(&self, cursor: Option<JobJournalCursor>) -> Result<JobAttachment> { core_attach(cursor).await }
            }
        "#).expect("capabilities");
        assert_eq!(methods[0].name, "end");
        assert_eq!(methods[0].core, "close");
        assert_eq!(methods[0].receiver, Receiver::Stdin);
        assert_eq!(
            methods[1].input,
            Input::OptionalRecord {
                name: "cursor".to_owned(),
                record: "JobJournalCursor".to_owned()
            }
        );
        assert_eq!(methods[1].answer, Answer::Attachment);
    }

    #[test]
    fn unannotated_methods_are_not_projected() {
        assert!(
            parse("impl JobHandle { pub async fn status(&self) -> Result<JobInfo> {} }")
                .expect("source")
                .is_empty()
        );
    }

    #[test]
    fn an_owned_or_wrong_receiver_is_refused() {
        for source in [
            r#"impl JobStdin { #[cfg_attr(any(), cowshed_api(napi = "JobAttachment.end"))] pub async fn close(self) -> Result<()> {} }"#,
            r#"impl Coordinator { #[cfg_attr(any(), cowshed_api(napi = "JobAttachment.end"))] pub async fn close(&self) -> Result<()> {} }"#,
        ] {
            assert!(parse(source).is_err(), "{source}");
        }
    }
}

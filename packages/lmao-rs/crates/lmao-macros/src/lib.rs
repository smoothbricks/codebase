//! # lmao-macros
//!
//! Compile-time replacements for what the TS implementation does with RUNTIME
//! codegen. Mapping to the specs / TS machinery each macro replaces:
//!
//! | Rust macro            | Replaces (TS)                                        | Spec |
//! |-----------------------|------------------------------------------------------|------|
//! | `define_log_schema!`  | `defineLogSchema` + `new Function()` buffer-class    | `01a_trace_schema_system.md`, `01b6_buffer_codegen_extension.md` |
//! |                       | codegen (`fixedPositionWriterGenerator.ts`,          | `01g_trace_context_api_codegen.md` |
//! |                       | `spanLoggerGenerator.ts`)                            | `01j_module_context_and_spanlogger_generation.md` |
//! | `span!`               | TypeScript AST transformer: line-number injection,   | `01o_typescript_transformer.md` |
//! |                       | monomorphic `spanN` arity rewriting — here           | |
//! |                       | `line!()`/`file!()` capture and direct field writes  | |
//!
//! Rust monomorphization gives the "hidden class stability" the V8 tricks aimed
//! at, for free: the generated buffer is a concrete struct, every writer is a
//! direct field store, no string-keyed lookup anywhere.
//!
//! ## Field DSL (from `01a`)
//!
//! ```ignore
//! define_log_schema!(pub HttpSchema {
//!     status: number,               // f64 column
//!     retries: uint64,              // u64 column
//!     cache_hit: boolean,           // bool column
//!     route: category,              // Arc<str> slots, dictionary at flush
//!     detail: text,                 // Arc<str> slots, 2-pass encode at flush
//!     method: enum["GET", "POST"],  // u16 index, dictionary fixed at compile time
//! });
//! ```
//!
//! Generates `HttpSchema` (buffer wrapper: core `SpanBuffer` + one lazy column per
//! field), `tag_*` writers (row 0, last-write-wins per `01b`), `set_*(row, v)`
//! row-targeted writers for log values, a `*_VALUES` const dictionary per enum
//! field, and a per-schema `ratchet()` (`OnceLock<Mutex<CapacityRatchet>>` — all
//! buffers of one schema share capacity learning, `01b2`).
//!
//! Not yet implemented from `01a` (deliberate, documented): `binary`/`unknown`
//! (msgpack columns) and `.mask(preset)` — both are flush-side concerns blocked
//! on `lmao-arrow`'s msgpack column support.

use proc_macro::TokenStream;
use proc_macro2::Span;
use quote::{format_ident, quote};
use syn::parse::{Parse, ParseStream};
use syn::punctuated::Punctuated;
use syn::{Ident, LitStr, Token, Visibility, braced, bracketed};

enum FieldKind {
    Number,
    Uint64,
    Boolean,
    Category,
    Text,
    Enum(Vec<String>),
}

struct Field {
    name: Ident,
    kind: FieldKind,
}

struct SchemaDef {
    vis: Visibility,
    name: Ident,
    fields: Vec<Field>,
}

impl Parse for Field {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let name: Ident = input.parse()?;
        input.parse::<Token![:]>()?;
        // `enum` is a Rust keyword, so accept either an ident or the kw token.
        let (kind_name, kind_span) = if input.peek(Token![enum]) {
            let kw: Token![enum] = input.parse()?;
            ("enum".to_string(), kw.span)
        } else {
            let ident: Ident = input.parse()?;
            (ident.to_string(), ident.span())
        };
        let kind = match kind_name.as_str() {
            "number" => FieldKind::Number,
            "uint64" => FieldKind::Uint64,
            "boolean" => FieldKind::Boolean,
            "category" => FieldKind::Category,
            "text" => FieldKind::Text,
            "enum" => {
                let content;
                bracketed!(content in input);
                let values: Punctuated<LitStr, Token![,]> = content.parse_terminated(
                    <LitStr as syn::parse::Parse>::parse as fn(ParseStream) -> syn::Result<LitStr>,
                    Token![,],
                )?;
                if values.is_empty() {
                    return Err(syn::Error::new(
                        kind_span,
                        "enum field needs at least one value: `enum[\"A\", ...]`",
                    ));
                }
                if values.len() > u16::MAX as usize {
                    return Err(syn::Error::new(kind_span, "enum dictionary too large"));
                }
                FieldKind::Enum(values.iter().map(|v| v.value()).collect())
            }
            other => {
                return Err(syn::Error::new(
                    kind_span,
                    format!(
                        "unknown field kind `{other}`; expected one of: number, uint64, \
                         boolean, category, text, enum[..] (binary/unknown are not \
                         supported yet — see lmao-macros docs)"
                    ),
                ));
            }
        };
        Ok(Field { name, kind })
    }
}

impl Parse for SchemaDef {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let vis: Visibility = input.parse()?;
        let name: Ident = input.parse()?;
        let content;
        braced!(content in input);
        let fields: Punctuated<Field, Token![,]> =
            content.parse_terminated(Field::parse, Token![,])?;
        if fields.is_empty() {
            return Err(syn::Error::new(
                name.span(),
                "schema needs at least one field",
            ));
        }
        Ok(SchemaDef {
            vis,
            name,
            fields: fields.into_iter().collect(),
        })
    }
}

/// Generates a schema-specific buffer wrapper + typed writer API. See the crate
/// docs for the DSL and what is generated.
#[proc_macro]
pub fn define_log_schema(input: TokenStream) -> TokenStream {
    let SchemaDef { vis, name, fields } = match syn::parse(input) {
        Ok(d) => d,
        Err(e) => return e.to_compile_error().into(),
    };

    let mut col_fields = Vec::new();
    let mut col_inits = Vec::new();
    let mut writers = Vec::new();
    let mut dict_consts = Vec::new();
    let mut bytes_terms = Vec::new();

    for f in &fields {
        let fname = &f.name;
        let tag_fn = format_ident!("tag_{}", fname);
        let set_fn = format_ident!("set_{}", fname);
        let get_fn = format_ident!("get_{}", fname);

        if let FieldKind::Enum(values) = &f.kind {
            let dict_name = format_ident!(
                "{}_VALUES",
                fname.to_string().to_uppercase(),
                span = Span::call_site()
            );
            let lits = values.iter();
            let n = values.len() as u16;
            dict_consts.push(quote! {
                /// Compile-time enum dictionary (`01a`: zero flush work).
                #vis const #dict_name: &[&str] = &[#(#lits),*];
            });
            writers.push(quote! {
                #[doc = concat!("Row-0 tag write for enum field `", stringify!(#fname), "` (index into the const dictionary).")]
                #[inline]
                #vis fn #tag_fn(&mut self, index: u16) -> &mut Self {
                    self.#set_fn(0, index)
                }
                #[inline]
                #vis fn #set_fn(&mut self, row: usize, index: u16) -> &mut Self {
                    debug_assert!(index < #n);
                    let cap = self.span.capacity();
                    self.#fname.set(row, cap, index);
                    self
                }
                #[inline]
                #vis fn #get_fn(&self, row: usize) -> Option<&'static str> {
                    self.#fname.get(row).map(|i| #dict_name[i as usize])
                }
            });
            col_fields.push(quote! { #fname: ::lmao_core::EnumColumn });
            col_inits.push(quote! { #fname: ::lmao_core::EnumColumn::new() });
            bytes_terms.push(quote! { self.#fname.allocated_bytes() });
            continue;
        }

        let (col_ty, val_ty, doc): (proc_macro2::TokenStream, proc_macro2::TokenStream, &str) =
            match &f.kind {
                FieldKind::Number => (
                    quote!(::lmao_core::F64Column),
                    quote!(f64),
                    "number (f64) column",
                ),
                FieldKind::Uint64 => (
                    quote!(::lmao_core::U64Column),
                    quote!(u64),
                    "uint64 column (shared metrics/user values, `01f`)",
                ),
                FieldKind::Boolean => (
                    quote!(::lmao_core::BoolColumn),
                    quote!(bool),
                    "boolean column",
                ),
                FieldKind::Category => (
                    quote!(::lmao_core::StrColumn),
                    quote!(impl Into<::lmao_core::SharedStr>),
                    "category string column — raw slot writes, dictionary at flush (`01a`)",
                ),
                FieldKind::Text => (
                    quote!(::lmao_core::StrColumn),
                    quote!(impl Into<::lmao_core::SharedStr>),
                    "text string column — raw slot writes, 2-pass encode at flush (`01a`)",
                ),
                FieldKind::Enum(_) => unreachable!(),
            };

        writers.push(quote! {
            #[doc = concat!("Row-0 tag write (last-write-wins, `01b`) — ", #doc, ".")]
            #[inline]
            #vis fn #tag_fn(&mut self, value: #val_ty) -> &mut Self {
                self.#set_fn(0, value)
            }
            #[doc = concat!("Row-targeted write — ", #doc, ".")]
            #[inline]
            #vis fn #set_fn(&mut self, row: usize, value: #val_ty) -> &mut Self {
                let cap = self.span.capacity();
                self.#fname.set(row, cap, value);
                self
            }
        });
        match &f.kind {
            FieldKind::Category | FieldKind::Text => writers.push(quote! {
                #[inline]
                #vis fn #get_fn(&self, row: usize) -> Option<&str> {
                    self.#fname.get(row)
                }
            }),
            _ => writers.push(quote! {
                #[inline]
                #vis fn #get_fn(&self, row: usize) -> Option<#val_ty> {
                    self.#fname.get(row)
                }
            }),
        }
        col_fields.push(quote! { #fname: #col_ty });
        col_inits.push(quote! { #fname: <#col_ty>::new() });
        bytes_terms.push(quote! { self.#fname.allocated_bytes() });
    }

    let expanded = quote! {
        #(#dict_consts)*

        /// Schema-generated span buffer: core system columns + one lazy column per
        /// schema field. Generated by `lmao_macros::define_log_schema!`.
        #vis struct #name {
            /// The underlying system-column buffer.
            #vis span: ::lmao_core::SpanBuffer,
            #(#col_fields,)*
        }

        impl #name {
            /// Per-schema capacity ratchet: ALL buffers of this schema share
            /// capacity learning (`01b2`).
            #vis fn ratchet() -> &'static ::std::sync::Mutex<::lmao_core::CapacityRatchet> {
                static RATCHET: ::std::sync::OnceLock<::std::sync::Mutex<::lmao_core::CapacityRatchet>> =
                    ::std::sync::OnceLock::new();
                RATCHET.get_or_init(|| {
                    ::std::sync::Mutex::new(::lmao_core::CapacityRatchet::new(64))
                })
            }

            /// Start a span buffer at the ratchet-recommended capacity.
            #vis fn start(
                identity: ::std::sync::Arc<::lmao_core::SpanIdentity>,
                anchor: &::lmao_core::TraceAnchor,
                clock: &dyn ::lmao_core::Clock,
            ) -> Self {
                let capacity = Self::ratchet().lock().unwrap().capacity();
                Self {
                    span: ::lmao_core::SpanBuffer::start(identity, capacity, anchor, clock),
                    #(#col_inits,)*
                }
            }

            /// Complete the span and feed the ratchet (`01b2`: stats recorded per
            /// finished span).
            #vis fn finish_ok(
                mut self,
                anchor: &::lmao_core::TraceAnchor,
                clock: &dyn ::lmao_core::Clock,
            ) -> Self {
                self.span.end_ok(anchor, clock);
                Self::ratchet()
                    .lock()
                    .unwrap()
                    .record_span(self.span.write_index().saturating_sub(2) as u64);
                self
            }

            /// Total heap bytes held by lazy attribute columns (0 when untouched).
            #vis fn attribute_bytes(&self) -> usize {
                0 #(+ #bytes_terms)*
            }

            #(#writers)*
        }
    };
    expanded.into()
}

/// Span invocation with callsite capture — the Rust equivalent of the TS AST
/// transformer's line-number injection (`01o`).
///
/// ```ignore
/// let (out, buf) = span!(trace, "fetch-user", |ctx| -> Result<_, ()> {
///     ctx.log(EntryType::Info, "looking up {id}", line!());
///     Ok(42)
/// });
/// ```
///
/// Expands to `trace.span(name, parent, 64, ...)` with `set_callsite(file!(),
/// line!())` applied before the body runs. Use `span!(trace, parent_expr,
/// "name", |ctx| ...)` to nest under a parent identity.
#[proc_macro]
pub fn span(input: TokenStream) -> TokenStream {
    struct SpanCall {
        trace: syn::Expr,
        parent: Option<syn::Expr>,
        name: LitStr,
        body: syn::Expr,
    }
    impl Parse for SpanCall {
        fn parse(input: ParseStream) -> syn::Result<Self> {
            let trace: syn::Expr = input.parse()?;
            input.parse::<Token![,]>()?;
            let (parent, name) = if input.peek(LitStr) {
                (None, input.parse()?)
            } else {
                let parent: syn::Expr = input.parse()?;
                input.parse::<Token![,]>()?;
                (Some(parent), input.parse()?)
            };
            input.parse::<Token![,]>()?;
            let body: syn::Expr = input.parse()?;
            if !input.is_empty() {
                input.parse::<Token![,]>()?;
            }
            Ok(SpanCall {
                trace,
                parent,
                name,
                body,
            })
        }
    }

    let SpanCall {
        trace,
        parent,
        name,
        body,
    } = match syn::parse(input) {
        Ok(c) => c,
        Err(e) => return e.to_compile_error().into(),
    };
    let parent_expr = match parent {
        Some(p) => quote!(::core::option::Option::Some(#p)),
        None => quote!(::core::option::Option::None),
    };
    quote! {
        (#trace).span(#name, #parent_expr, 64, |__lmao_ctx| {
            __lmao_ctx.set_callsite(file!(), line!());
            (#body)(__lmao_ctx)
        })
    }
    .into()
}

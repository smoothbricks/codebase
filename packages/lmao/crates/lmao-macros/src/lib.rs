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
//! ```text
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
//! Generates a typed wrapper over one core `SpanBuffer`, `tag_*` writers (row 0,
//! last-write-wins per `01b`), `set_*(row, v)` row-targeted writers, scoped enum
//! dictionaries, and `FIELD_META` retaining every DSL strategy for Arrow flush.
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

macro_rules! field_kinds {
    ($(($ident:ident, $variant:ident)),+ $(,)?) => {
        enum FieldKind {
            $($variant,)+
            Enum(Vec<String>),
        }

        fn parse_scalar_kind(name: &str) -> Option<FieldKind> {
            match name {
                $(stringify!($ident) => Some(FieldKind::$variant),)+
                _ => None,
            }
        }

        fn supported_field_kinds() -> &'static str {
            concat!($(stringify!($ident), ", ",)+ "enum[..]")
        }
    };
}

field_kinds! {
    (number, Number),
    (uint64, Uint64),
    (boolean, Boolean),
    (category, Category),
    (text, Text),
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
        let kind = if kind_name == "enum" {
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
        } else if let Some(kind) = parse_scalar_kind(&kind_name) {
            kind
        } else {
            return Err(syn::Error::new(
                kind_span,
                format!(
                    "unknown field kind `{kind_name}`; expected one of: {} \
                     (binary/unknown are not supported yet — see lmao-macros docs)",
                    supported_field_kinds()
                ),
            ));
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
    let mut field_meta = Vec::new();
    let mut scope_fills = Vec::new();

    for f in &fields {
        let fname = &f.name;
        let tag_fn = format_ident!("tag_{}", fname);
        let set_fn = format_ident!("set_{}", fname);
        let get_fn = format_ident!("get_{}", fname);
        let scope_fn = format_ident!("scope_{}", fname);

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
                #vis const #dict_name: &'static [&'static str] = &[#(#lits),*];
            });
            field_meta.push(quote! {
                ::lmao_core::FieldMeta::new(
                    stringify!(#fname),
                    ::lmao_core::FieldStrategy::Enum(Self::#dict_name),
                )
            });
            writers.push(quote! {
                #[doc = concat!("Row-0 tag write for enum field `", stringify!(#fname), "` (index into the const dictionary).")]
                #[inline]
                #vis fn #tag_fn(
                    &mut self,
                    index: u16,
                ) -> ::core::result::Result<&mut Self, ::lmao_core::EnumIndexError> {
                    self.#set_fn(0, index)
                }
                #[inline]
                #vis fn #set_fn(
                    &mut self,
                    row: usize,
                    index: u16,
                ) -> ::core::result::Result<&mut Self, ::lmao_core::EnumIndexError> {
                    if index >= #n {
                        return ::core::result::Result::Err(::lmao_core::EnumIndexError {
                            field: stringify!(#fname),
                            index,
                            variants: #n,
                        });
                    }
                    let cap = self.span.capacity();
                    self.#fname.set(row, cap, index);
                    ::core::result::Result::Ok(self)
                }
                #[inline]
                #vis fn #get_fn(&self, row: usize) -> Option<&'static str> {
                    self.#fname
                        .get(row)
                        .and_then(|index| Self::#dict_name.get(index as usize).copied())
                }
                #[doc = concat!("Scope entry for enum field `", stringify!(#fname), "`: `Some(index)` sets it for every row of the span and its later children, `None` clears it (`01i`).")]
                #[inline]
                #vis fn #scope_fn(
                    index: Option<u16>,
                ) -> ::core::result::Result<::lmao_core::ScopeEntry, ::lmao_core::EnumIndexError> {
                    if let Some(index) = index {
                        if index >= #n {
                            return ::core::result::Result::Err(::lmao_core::EnumIndexError {
                                field: stringify!(#fname),
                                index,
                                variants: #n,
                            });
                        }
                    }
                    ::core::result::Result::Ok((
                        stringify!(#fname),
                        index.map(::lmao_core::ScopeValue::EnumIndex),
                    ))
                }
            });
            col_fields.push(quote! { #fname: ::lmao_core::EnumColumn });
            col_inits.push(quote! { #fname: ::lmao_core::EnumColumn::new() });
            bytes_terms.push(quote! { self.#fname.allocated_bytes() });
            scope_fills.push(quote! {
                match scope.get(stringify!(#fname)) {
                    Some(value @ ::lmao_core::ScopeValue::EnumIndex(index)) => {
                        // An out-of-range index would be written straight into a
                        // column the Arrow flush indexes against a fixed-size
                        // dictionary, so it is refused here rather than corrupting
                        // the batch. `scope_*` validates, but a raw `set_scope` can
                        // bypass it.
                        if *index < #n {
                            filled += self.#fname.fill_unset(rows, capacity, *index);
                        } else {
                            ::lmao_core::report_scope_mismatch(
                                stringify!(#fname),
                                "an in-range enum index",
                                value,
                            );
                        }
                    }
                    Some(mismatched) => ::lmao_core::report_scope_mismatch(
                        stringify!(#fname),
                        "ScopeValue::EnumIndex",
                        mismatched,
                    ),
                    None => {}
                }
            });
            continue;
        }

        let (col_ty, val_ty, doc, write_prelude): (
            proc_macro2::TokenStream,
            proc_macro2::TokenStream,
            &str,
            proc_macro2::TokenStream,
        ) = match &f.kind {
            FieldKind::Number => (
                quote!(::lmao_core::F64Column),
                quote!(f64),
                "number (f64) column",
                quote!(),
            ),
            FieldKind::Uint64 => (
                quote!(::lmao_core::U64Column),
                quote!(u64),
                "uint64 column (shared metrics/user values, `01f`)",
                quote!(),
            ),
            FieldKind::Boolean => (
                quote!(::lmao_core::BoolColumn),
                quote!(bool),
                "boolean column",
                quote!(),
            ),
            FieldKind::Category => (
                quote!(::lmao_core::StrColumn),
                quote!(::lmao_core::TextInput<'_>),
                "category string column — raw slot writes, dictionary at flush (`01a`)",
                quote!(let value = self.span.intern_text(value);),
            ),
            FieldKind::Text => (
                quote!(::lmao_core::StrColumn),
                quote!(::lmao_core::TextInput<'_>),
                "text string column — raw slot writes, 2-pass encode at flush (`01a`)",
                quote!(let value = self.span.intern_text(value);),
            ),
            FieldKind::Enum(_) => unreachable!(),
        };
        let strategy = match &f.kind {
            FieldKind::Number => quote!(::lmao_core::FieldStrategy::Number),
            FieldKind::Uint64 => quote!(::lmao_core::FieldStrategy::Uint64),
            FieldKind::Boolean => quote!(::lmao_core::FieldStrategy::Boolean),
            FieldKind::Category => quote!(::lmao_core::FieldStrategy::Category),
            FieldKind::Text => quote!(::lmao_core::FieldStrategy::Text),
            FieldKind::Enum(_) => unreachable!(),
        };
        // `category` and `text` share `ScopeValue::Text` because both are backed by
        // `StrColumn` and differ only in flush strategy, which `FIELD_META` retains.
        let (scope_ty, scope_variant, scope_prelude, scope_fill_arg) = match &f.kind {
            FieldKind::Number => (quote!(f64), quote!(Number), quote!(), quote!(*value)),
            FieldKind::Uint64 => (quote!(u64), quote!(Uint64), quote!(), quote!(*value)),
            FieldKind::Boolean => (quote!(bool), quote!(Boolean), quote!(), quote!(*value)),
            FieldKind::Category | FieldKind::Text => (
                quote!(::lmao_core::ScopeText),
                quote!(Text),
                quote!(let value = self.span.intern_scope_text(value);),
                quote!(value),
            ),
            FieldKind::Enum(_) => unreachable!(),
        };
        field_meta.push(quote! {
            ::lmao_core::FieldMeta::new(stringify!(#fname), #strategy)
        });

        writers.push(quote! {
            #[doc = concat!("Row-0 tag write (last-write-wins, `01b`) — ", #doc, ".")]
            #[inline]
            #vis fn #tag_fn(&mut self, value: #val_ty) -> &mut Self {
                self.#set_fn(0, value)
            }
            #[doc = concat!("Row-targeted write — ", #doc, ".")]
            #[inline]
            #vis fn #set_fn(&mut self, row: usize, value: #val_ty) -> &mut Self {
                #write_prelude
                let cap = self.span.capacity();
                self.#fname.set(row, cap, value);
                self
            }
            #[doc = concat!("Scope entry for `", stringify!(#fname), "`: `Some(value)` makes it the default on every row of this span and every child created afterwards, `None` clears it (`01i`). Direct `tag_`/`set_` writes always win on the rows they touch.")]
            #[inline]
            #vis fn #scope_fn(value: Option<#scope_ty>) -> ::lmao_core::ScopeEntry {
                (
                    stringify!(#fname),
                    value.map(::lmao_core::ScopeValue::#scope_variant),
                )
            }
        });
        match &f.kind {
            FieldKind::Category | FieldKind::Text => writers.push(quote! {
                #[inline]
                #vis fn #get_fn(&self, row: usize) -> Option<&str> {
                    self.#fname.get(row, self.span.arena())
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
        scope_fills.push(quote! {
            match scope.get(stringify!(#fname)) {
                Some(::lmao_core::ScopeValue::#scope_variant(value)) => {
                    #scope_prelude
                    filled += self.#fname.fill_unset(rows, capacity, #scope_fill_arg);
                }
                Some(mismatched) => ::lmao_core::report_scope_mismatch(
                    stringify!(#fname),
                    concat!("ScopeValue::", stringify!(#scope_variant)),
                    mismatched,
                ),
                None => {}
            }
        });
    }

    let expanded = quote! {
        /// Schema-generated typed columns over one already-started core span.
        #vis struct #name {
            /// The single system-column buffer owned by the tracing lifecycle.
            #vis span: ::lmao_core::SpanBuffer,
            #(#col_fields,)*
        }

        impl #name {
            #(#dict_consts)*

            /// Field strategies retained from the DSL for the Arrow flush planner.
            #vis const FIELD_META: &'static [::lmao_core::FieldMeta] = &[
                #(#field_meta,)*
            ];

            /// Attach typed columns to the span buffer created by `TraceContext`.
            #vis fn from_span(span: ::lmao_core::SpanBuffer) -> Self {
                Self {
                    span,
                    #(#col_inits,)*
                }
            }

            #vis fn into_span(self) -> ::lmao_core::SpanBuffer {
                self.span
            }

            /// Total heap bytes held by lazy attribute columns (0 when untouched).
            #vis fn attribute_bytes(&self) -> usize {
                0 #(+ #bytes_terms)*
            }

            /// Materialize this span's scope (`01i`) into the attribute columns and
            /// return how many cells were filled.
            ///
            /// Cold path: call once before handing the buffer to the flush pipeline.
            /// Every cell a direct `tag_*`/`set_*` write already touched keeps its
            /// value — scope only fills the nulls those writes left behind, which is
            /// `01i`'s "direct writes always win", enforced per row through the
            /// column's own validity bitmap rather than by remembering what was
            /// written.
            ///
            /// Fills rows `0..write_index` of THIS buffer. A scope field naming no
            /// schema column is ignored, exactly as in TypeScript, where a
            /// `_scopeValues` key with no matching column has nothing to fill.
            #vis fn fill_scope(&mut self) -> usize {
                // Take the shared handle first: one refcount bump, and the rest of
                // the method then needs no borrow of `self.span` while it writes
                // columns.
                let Some(scope) = self.span.scope_handle() else {
                    return 0;
                };
                let capacity = self.span.capacity();
                let rows = self.span.write_index();
                let mut filled = 0usize;
                #(#scope_fills)*
                filled
            }

            #(#writers)*
        }
    };
    expanded.into()
}

/// Span invocation with callsite capture — the Rust equivalent of the TS AST
/// transformer's line-number injection (`01o`).
///
/// ```text
/// let (out, buf) = span!(trace, "fetch-user", |ctx| -> Result<_, ()> {
///     ctx.log(EntryType::Info, "looking up {id}", line!());
///     Ok(42)
/// });
/// ```
///
/// Expands to `trace.span(name, parent, DEFAULT_CAPACITY, ...)` with compile-time
/// package/file/git attribution applied before the body runs. Use
/// `span!(trace, parent_expr, "name", |ctx| ...)` to nest under a parent identity.
#[proc_macro]
pub fn span(input: TokenStream) -> TokenStream {
    // span! only forwards these fragments (`#trace` / `#p` / `#body`); it never
    // matches on the tree. Parsing `syn::Expr` would pull in syn's `full` AST
    // just to round-trip tokens. Groups keep nested commas inside one fragment.
    fn opaque_expr(input: ParseStream) -> syn::Result<proc_macro2::TokenStream> {
        let mut tokens = proc_macro2::TokenStream::new();
        while !input.is_empty() && !input.peek(Token![,]) {
            let tt: proc_macro2::TokenTree = input.parse()?;
            tokens.extend(std::iter::once(tt));
        }
        if tokens.is_empty() {
            return Err(input.error("expected expression"));
        }
        Ok(tokens)
    }

    struct SpanCall {
        trace: proc_macro2::TokenStream,
        parent: Option<proc_macro2::TokenStream>,
        name: LitStr,
        body: proc_macro2::TokenStream,
    }
    impl Parse for SpanCall {
        fn parse(input: ParseStream) -> syn::Result<Self> {
            let trace = opaque_expr(input)?;
            input.parse::<Token![,]>()?;
            let (parent, name) = if input.peek(LitStr) {
                (None, input.parse()?)
            } else {
                let parent = opaque_expr(input)?;
                input.parse::<Token![,]>()?;
                (Some(parent), input.parse()?)
            };
            input.parse::<Token![,]>()?;
            let body = opaque_expr(input)?;
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
        (#trace).span(::lmao_core::TextInput::Static(#name), #parent_expr, ::lmao_core::DEFAULT_CAPACITY, |__lmao_ctx| {
            __lmao_ctx.set_source(::lmao_core::SourceMetadata {
                package_name: env!("CARGO_PKG_NAME"),
                package_file: file!(),
                git_sha: option_env!("GIT_SHA").or(option_env!("GITHUB_SHA")),
                line: line!(),
            });
            (#body)(__lmao_ctx)
        })
    }
    .into()
}

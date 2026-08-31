use lmao_core::{TextInput, TraceContext};

fn omit_provenance(trace: &TraceContext) {
    let _ = trace.span(TextInput::Static("missing"), None, 8, |_| Ok::<_, ()>(()));
}

fn main() {}

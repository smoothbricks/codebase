use lmao_macros::define_log_schema;

define_log_schema!(pub BadEnum {
    outcome: enum[],
});

fn main() {}

use lmao_macros::define_log_schema;

define_log_schema!(pub BadSchema {
    status: floatish,
});

fn main() {}

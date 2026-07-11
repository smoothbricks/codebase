use lmao_macros::define_log_schema;

define_log_schema!(pub FullSchema {
    latency: number,
    count: uint64,
    hit: boolean,
    route: category,
    detail: text,
    method: enum["GET", "POST"],
});

fn main() {
    // Dictionary is a compile-time const.
    assert_eq!(METHOD_VALUES, &["GET", "POST"]);
}

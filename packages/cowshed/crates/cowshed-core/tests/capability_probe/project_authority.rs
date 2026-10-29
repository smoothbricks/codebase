//! A `Project` calls attach, exec, grant and gc.
//! must name: attach
//! must name: exec
//! must name: grant
//! must name: gc
use cowshed_core::Project;
fn deny(value: &Project) {
    value.attach();
    value.exec();
    value.grant();
    value.gc();
}

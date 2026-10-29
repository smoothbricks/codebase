//! An operation claiming a coordinator method is declared outside cowshed-core and served to a `JobHandle`.
//! must name: `Forged: operations::sealed::Sealed` is not satisfied
use cowshed_core::JobHandle;
use cowshed_core::api::call::Serves;
use cowshed_core::api::operations::{JobRequest, Lane, Operation, OperationRequest, Scope};
pub enum Forged {}
impl Operation for Forged {
    const METHOD: &'static str = "coordinator.destroy";
    const SCOPE: Scope = Scope::Worker;
    const LANE: Lane = Lane::Json;
    type Request = JobRequest;
    type Result = ();
    fn request(request: JobRequest) -> OperationRequest {
        OperationRequest::JobKill(request)
    }
}
impl Serves<Forged> for JobHandle {
    const BOUND: &'static [&'static str] = &[];
}

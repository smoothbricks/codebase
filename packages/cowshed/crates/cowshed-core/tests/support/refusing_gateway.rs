use async_trait::async_trait;
use cowshed_core::Result;
use cowshed_core::gateway_sessions::{ControlRefusal, GatewayControl, GatewayStatusError};
use cowshed_core::storage::bootstrap::ValidatedHostStorage;
use cowshed_gateway_types::{DaemonStatus, WorkspaceSession};

struct RefusingGateway;

/// Lifecycle-only fixtures have no gateway capability. An unexpected job preflight must fail.
pub fn connect(_: &ValidatedHostStorage) -> Result<Box<dyn GatewayControl>> {
    Ok(Box::new(RefusingGateway))
}

#[async_trait]
impl GatewayControl for RefusingGateway {
    async fn gates_in_flight(
        &self,
    ) -> std::result::Result<
        std::collections::BTreeSet<cowshed_core::api::dto::GateJobRef>,
        GatewayStatusError,
    > {
        Err(GatewayStatusError::Control(
            "lifecycle-only fixture unexpectedly requested the gates in flight".to_owned(),
        ))
    }
    async fn status(&self) -> std::result::Result<DaemonStatus, GatewayStatusError> {
        Err(GatewayStatusError::Control(
            "lifecycle-only fixture unexpectedly requested gateway status".to_owned(),
        ))
    }

    async fn install(&self, _: &WorkspaceSession) -> std::result::Result<(), ControlRefusal> {
        Err(ControlRefusal::Failed(
            "lifecycle-only fixture unexpectedly installed a gateway session".to_owned(),
        ))
    }

    async fn remove(&self, _: &str, _: u64) -> std::result::Result<(), ControlRefusal> {
        Err(ControlRefusal::Failed(
            "lifecycle-only fixture unexpectedly removed a gateway session".to_owned(),
        ))
    }
}

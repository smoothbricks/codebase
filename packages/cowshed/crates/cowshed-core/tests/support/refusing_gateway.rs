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
    /// A test gateway never says a session is current: no exec it serves is resident.
    async fn session_current(&self, _: &str, _: u64) -> cowshed_core::Result<bool> {
        Ok(false)
    }

    async fn land_turns(
        &self,
    ) -> std::result::Result<Vec<cowshed_core::api::dto::LandTurnQueue>, GatewayStatusError> {
        Err(GatewayStatusError::Control(
            "lifecycle-only fixture unexpectedly requested the land turns".to_owned(),
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

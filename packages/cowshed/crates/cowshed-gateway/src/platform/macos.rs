//! macOS keeps scoped gateway credentials as Keychain generic-password items.

use crate::interfaces::{
    CredentialError, CredentialProtocol, CredentialProvider, CredentialQuery, CredentialRecord,
};

use super::{
    CredentialPresence, ScopedCredential, account_for, account_key, decode_record, validate_origin,
    validate_query, validate_scope,
};

/// Reads scoped gateway credentials from macOS generic-password items.
///
/// Items use service `dev.cowshed.gateway` and account
/// `v1|<repo_id>|<protocol>|<base64url-exact-origin>`. The password is a
/// versioned JSON record containing the same binding, methods, normalized path
/// prefixes, header name, and secret header value. Lookup occurs only after the
/// gateway actor has admitted the exact request.
#[derive(Clone, Debug, Default)]
pub struct KeychainCredentialProvider;

impl KeychainCredentialProvider {
    pub const SERVICE: &'static str = "dev.cowshed.gateway";

    pub fn new() -> Self {
        Self
    }

    pub fn account_for(query: &CredentialQuery) -> String {
        account_for(query)
    }
}

#[async_trait::async_trait]
impl CredentialProvider for KeychainCredentialProvider {
    async fn lookup(
        &self,
        query: &CredentialQuery,
    ) -> Result<Option<CredentialRecord>, CredentialError> {
        validate_query(query)?;
        lookup_keychain(Self::account_for(query)).await
    }
}

async fn lookup_keychain(account: String) -> Result<Option<CredentialRecord>, CredentialError> {
    tokio::task::spawn_blocking(move || {
        match security_framework::passwords::get_generic_password(
            KeychainCredentialProvider::SERVICE,
            &account,
        ) {
            Ok(bytes) => decode_record(bytes).map(Some),
            Err(error) if error.code() == security_framework_sys::base::errSecItemNotFound => {
                Ok(None)
            }
            Err(error) => Err(CredentialError::Unavailable(format!(
                "macOS Keychain lookup failed with OSStatus {}",
                error.code()
            ))),
        }
    })
    .await
    .map_err(|error| CredentialError::Unavailable(format!("Keychain task failed: {error}")))?
}

/// Install or replace one scoped credential in the host's platform store.
///
/// Enrolment is a trusted-host operation: the secret arrives from the operator's environment,
/// travels in a `Zeroizing` buffer, and is handed to the platform store without passing through
/// argv, a temporary file, or any workspace-reachable path. Replacing an existing binding is
/// deliberate — rotation is the common case and a second command to delete first would only
/// create a window with no credential at all.
pub async fn store_scoped_credential(credential: ScopedCredential) -> Result<(), CredentialError> {
    validate_scope(&credential)?;
    let account = credential.account();
    let payload = credential.to_json()?;
    tokio::task::spawn_blocking(move || {
        security_framework::passwords::set_generic_password(
            KeychainCredentialProvider::SERVICE,
            &account,
            payload.as_bytes(),
        )
        .map_err(|error| {
            CredentialError::Unavailable(format!(
                "macOS Keychain write failed with OSStatus {}",
                error.code()
            ))
        })
    })
    .await
    .map_err(|error| CredentialError::Unavailable(format!("Keychain task failed: {error}")))?
}

/// Remove one scoped credential. An absent binding is already the requested state.
pub async fn remove_scoped_credential(
    repo_id: &str,
    protocol: CredentialProtocol,
    origin: &str,
) -> Result<CredentialPresence, CredentialError> {
    validate_origin(origin)?;
    let account = account_key(repo_id, protocol, origin);
    tokio::task::spawn_blocking(
        move || match security_framework::passwords::delete_generic_password(
            KeychainCredentialProvider::SERVICE,
            &account,
        ) {
            Ok(()) => Ok(CredentialPresence::Installed),
            Err(error) if error.code() == security_framework_sys::base::errSecItemNotFound => {
                Ok(CredentialPresence::Absent)
            }
            Err(error) => Err(CredentialError::Unavailable(format!(
                "macOS Keychain delete failed with OSStatus {}",
                error.code()
            ))),
        },
    )
    .await
    .map_err(|error| CredentialError::Unavailable(format!("Keychain task failed: {error}")))?
}

/// The scope of the installed binding for this origin, or `None` when nothing is enrolled.
///
/// `status` reports what an operator enrolled, and the path prefixes are that answer: they are
/// the non-secret half of the record, and the half that decides which requests the credential
/// can ever ride. The decoded record's secret is dropped — and zeroed — right here rather than
/// travelling into a reporting path.
pub async fn scoped_credential_scope(
    repo_id: &str,
    protocol: CredentialProtocol,
    origin: &str,
) -> Result<Option<Vec<String>>, CredentialError> {
    validate_origin(origin)?;
    let account = account_key(repo_id, protocol, origin);
    Ok(lookup_keychain(account)
        .await?
        .map(|record| record.path_prefixes))
}

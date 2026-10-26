use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
    time::{Duration, Instant},
};

use rcgen::{
    Certificate, CertificateParams, ExtendedKeyUsagePurpose, Issuer, KeyPair, KeyUsagePurpose,
};
use rustls::{
    ServerConfig,
    pki_types::{CertificateDer, PrivatePkcs8KeyDer},
};
use sha2::{Digest, Sha256};
use thiserror::Error;
use time::OffsetDateTime;

use cowshed_gateway_types::WorkspaceCa;

use crate::{
    config::{GatewayLimits, GatewayTimeouts},
    interfaces::alpn_protocols,
};

pub(crate) struct CaSigner {
    issuer: Issuer<'static, KeyPair>,
    certificate_der: CertificateDer<'static>,
    pub fingerprint: [u8; 32],
}

impl CaSigner {
    pub fn parse(material: &WorkspaceCa) -> Result<Self, TlsError> {
        let key = KeyPair::from_pem(material.private_key_pem.as_str())
            .map_err(|error| TlsError::InvalidCa(error.to_string()))?;
        let issuer = Issuer::from_ca_cert_pem(&material.certificate_pem, key)
            .map_err(|error| TlsError::InvalidCa(error.to_string()))?;
        let mut reader = material.certificate_pem.as_bytes();
        let certificate_der = rustls_pemfile::certs(&mut reader)
            .next()
            .transpose()
            .map_err(|error| TlsError::InvalidCa(error.to_string()))?
            .ok_or_else(|| TlsError::InvalidCa("missing certificate".to_owned()))?;
        let fingerprint: [u8; 32] = Sha256::digest(certificate_der.as_ref()).into();
        Ok(Self {
            issuer,
            certificate_der,
            fingerprint,
        })
    }

    fn mint(&self, host: &str, lifetime: Duration) -> Result<Arc<ServerConfig>, TlsError> {
        let key = KeyPair::generate().map_err(|error| TlsError::Leaf(error.to_string()))?;
        let certificate = leaf_params(host, lifetime)?
            .signed_by(&key, &self.issuer)
            .map_err(|error| TlsError::Leaf(error.to_string()))?;
        self.serve(&certificate, &key)
    }

    fn serve(
        &self,
        certificate: &Certificate,
        key: &KeyPair,
    ) -> Result<Arc<ServerConfig>, TlsError> {
        let chain = vec![
            CertificateDer::from(certificate.der().to_vec()),
            self.certificate_der.clone(),
        ];
        let key_der = PrivatePkcs8KeyDer::from(key.serialize_der()).into();
        let mut config = ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(chain, key_der)
            .map_err(|error| TlsError::Leaf(error.to_string()))?;
        config.alpn_protocols = alpn_protocols();
        Ok(Arc::new(config))
    }
}

/// The RFC 5280 end-entity profile for an intercepted `host`. Every certificate a CA issues must
/// name it through the Authority Key Identifier (§4.2.1.1), and OpenSSL's strict chain check —
/// `X509_V_FLAG_X509_STRICT`, on in Python 3.13+'s default context — refuses a leaf without one
/// ("Missing Authority Key Identifier"). rcgen writes it only when asked, from the issuer's own
/// Subject Key Identifier, so the leaf names exactly the workspace CA. The key is P-256 ECDSA, so
/// it signs (digitalSignature) and never enciphers; serverAuth is the only purpose it serves.
fn leaf_params(host: &str, lifetime: Duration) -> Result<CertificateParams, TlsError> {
    let mut params = CertificateParams::new(vec![host.to_owned()])
        .map_err(|error| TlsError::Leaf(error.to_string()))?;
    let now = OffsetDateTime::now_utc();
    params.not_before = now - time::Duration::minutes(1);
    params.not_after = now
        + time::Duration::try_from(lifetime)
            .map_err(|_| TlsError::Leaf("invalid leaf lifetime".to_owned()))?;
    params.use_authority_key_identifier_extension = true;
    params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
    params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
    Ok(params)
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct CacheKey {
    workspace_id: String,
    host: String,
    fingerprint: [u8; 32],
}

struct CacheEntry {
    config: Arc<ServerConfig>,
    expires: Instant,
}

pub(crate) struct LeafCache {
    limits: GatewayLimits,
    lifetime: Duration,
    entries: HashMap<CacheKey, CacheEntry>,
    lru: VecDeque<CacheKey>,
}

impl LeafCache {
    pub fn new(limits: GatewayLimits, timeouts: GatewayTimeouts) -> Self {
        Self {
            limits,
            lifetime: timeouts.leaf_lifetime,
            entries: HashMap::new(),
            lru: VecDeque::new(),
        }
    }

    pub fn get_or_mint(
        &mut self,
        workspace_id: &str,
        host: &str,
        signer: &CaSigner,
    ) -> Result<Arc<ServerConfig>, TlsError> {
        let key = CacheKey {
            workspace_id: workspace_id.to_owned(),
            host: host.to_owned(),
            fingerprint: signer.fingerprint,
        };
        let now = Instant::now();
        if let Some(entry) = self.entries.get(&key)
            && entry.expires.saturating_duration_since(now) >= Duration::from_secs(5 * 60)
        {
            let config = Arc::clone(&entry.config);
            self.touch(&key);
            return Ok(config);
        }
        self.entries.remove(&key);
        self.remove_lru(&key);
        while self.entries.len() >= self.limits.leaf_cache_global
            || self.workspace_len(workspace_id) >= self.limits.leaf_cache_workspace
        {
            self.evict_one(workspace_id);
        }
        let config = signer.mint(host, self.lifetime)?;
        self.entries.insert(
            key.clone(),
            CacheEntry {
                config: Arc::clone(&config),
                expires: now + self.lifetime,
            },
        );
        self.lru.push_back(key);
        Ok(config)
    }

    pub fn drop_workspace(&mut self, workspace_id: &str) {
        self.entries
            .retain(|key, _| key.workspace_id != workspace_id);
        self.lru.retain(|key| key.workspace_id != workspace_id);
    }

    fn workspace_len(&self, workspace_id: &str) -> usize {
        self.entries
            .keys()
            .filter(|key| key.workspace_id == workspace_id)
            .count()
    }

    fn evict_one(&mut self, preferred_workspace: &str) {
        let position = self
            .lru
            .iter()
            .position(|key| key.workspace_id == preferred_workspace)
            .unwrap_or(0);
        if let Some(key) = self.lru.remove(position) {
            self.entries.remove(&key);
        }
    }

    fn touch(&mut self, key: &CacheKey) {
        self.remove_lru(key);
        self.lru.push_back(key.clone());
    }

    fn remove_lru(&mut self, key: &CacheKey) {
        if let Some(position) = self.lru.iter().position(|candidate| candidate == key) {
            self.lru.remove(position);
        }
    }
}

#[derive(Debug, Error)]
pub enum TlsError {
    #[error("workspace CA cannot be loaded: {0}")]
    InvalidCa(String),
    #[error("workspace leaf certificate cannot be minted: {0}")]
    Leaf(String),
}

#[cfg(test)]
#[path = "../tests/support/fixture_ports.rs"]
mod test_fixture_ports;
#[cfg(test)]
mod tests {
    use cowshed_os::test_backstop::ChildBackstop as _;
    use std::{io::Write as _, process::Stdio};

    use tokio_rustls::TlsAcceptor;
    use x509_parser::{
        extensions::{GeneralName, ParsedExtension},
        prelude::{FromDer, X509Certificate},
    };

    use super::*;

    const HOST: &str = "secure.test";
    const LIFETIME: Duration = Duration::from_secs(3600);

    /// The production workspace CA, so the chain under test is the one a sandbox is served.
    fn signer() -> (CaSigner, String) {
        let ca = cowshed_core::workspace_credentials::mint_workspace_ca(
            "cowshed:acme/widget:raven:00112233445566778899aabbccddeeff".to_owned(),
        )
        .expect("workspace CA");
        let certificate_pem = ca.certificate_pem.clone();
        (CaSigner::parse(&ca).expect("CA signer"), certificate_pem)
    }

    #[test]
    fn a_minted_leaf_names_its_ca_and_serves_only_tls_servers() {
        let (signer, _) = signer();
        let key = KeyPair::generate().expect("leaf key");
        let leaf = leaf_params(HOST, LIFETIME)
            .expect("leaf params")
            .signed_by(&key, &signer.issuer)
            .expect("leaf");
        let (_, ca) = X509Certificate::from_der(signer.certificate_der.as_ref()).expect("CA DER");
        let (_, leaf) = X509Certificate::from_der(leaf.der()).expect("leaf DER");

        let subject_key_identifier = ca
            .iter_extensions()
            .find_map(|extension| match extension.parsed_extension() {
                ParsedExtension::SubjectKeyIdentifier(identifier) => Some(identifier.0),
                _ => None,
            })
            .expect("CA subjectKeyIdentifier");
        let authority_key_identifier = leaf
            .iter_extensions()
            .find_map(|extension| match extension.parsed_extension() {
                ParsedExtension::AuthorityKeyIdentifier(identifier) => {
                    identifier.key_identifier.as_ref().map(|key| key.0)
                }
                _ => None,
            })
            .expect("leaf authorityKeyIdentifier");
        assert_eq!(authority_key_identifier, subject_key_identifier);
        assert_eq!(leaf.issuer(), ca.subject());

        let usage = leaf.key_usage().expect("one keyUsage").expect("keyUsage");
        assert!(usage.critical);
        assert!(usage.value.digital_signature());
        assert!(!usage.value.key_cert_sign());
        assert!(!usage.value.crl_sign());
        let purposes = leaf
            .extended_key_usage()
            .expect("one extendedKeyUsage")
            .expect("extendedKeyUsage");
        assert!(purposes.value.server_auth);
        assert!(!purposes.value.client_auth);
        let names = leaf
            .subject_alternative_name()
            .expect("one subjectAltName")
            .expect("subjectAltName");
        assert_eq!(names.value.general_names, [GeneralName::DNSName(HOST)]);
    }

    /// Python 3.13+ verifies through OpenSSL with `X509_V_FLAG_X509_STRICT`, which is how a
    /// sandbox's devenv Python failed to reach the npm registry. The client sets the flag itself, so
    /// the verdict does not depend on which Python runs; the AKI-less control leaf — the profile the
    /// gateway served before — proves this client is strict rather than merely permissive.
    #[tokio::test]
    async fn a_strict_x509_client_accepts_the_minted_chain_and_refuses_one_without_an_aki() {
        let (signer, ca_pem) = signer();

        let accepted =
            strict_handshake(signer.mint(HOST, LIFETIME).expect("minted leaf"), &ca_pem).await;
        assert!(
            accepted.status.success(),
            "strict client refused the minted chain: {}",
            String::from_utf8_lossy(&accepted.stderr)
        );

        let key = KeyPair::generate().expect("control key");
        let control = CertificateParams::new(vec![HOST.to_owned()])
            .expect("control params")
            .signed_by(&key, &signer.issuer)
            .expect("control leaf");
        let refused = strict_handshake(
            signer.serve(&control, &key).expect("control config"),
            &ca_pem,
        )
        .await;
        let reported = String::from_utf8_lossy(&refused.stderr);
        assert!(
            !refused.status.success(),
            "strict client accepted an AKI-less leaf: {reported}"
        );
        assert!(
            reported.contains("Missing Authority Key Identifier"),
            "{reported}"
        );
    }

    async fn strict_handshake(config: Arc<ServerConfig>, ca_pem: &str) -> std::process::Output {
        const CLIENT: &str = "import socket, ssl, sys
print(ssl.OPENSSL_VERSION, file=sys.stderr)
context = ssl.create_default_context(cadata=sys.stdin.read())
context.verify_flags |= ssl.VERIFY_X509_STRICT
with socket.create_connection(('127.0.0.1', int(sys.argv[1]))) as raw:
    with context.wrap_socket(raw, server_hostname=sys.argv[2]) as tls:
        print(tls.version())
";
        // A sandboxed client may connect only to its workspace's block ports (04_sandbox.md).
        let listener = super::test_fixture_ports::bind_fixture_listener().await;
        let port = listener.local_addr().expect("local address").port();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            // The client's verdict is the one under test; a refused handshake ends here with its alert.
            let _ = TlsAcceptor::from(config).accept(stream).await;
        });
        let ca_pem = ca_pem.to_owned();
        let output = tokio::task::spawn_blocking(move || {
            let mut child = std::process::Command::new("python3")
                .args(["-c", CLIENT, &port.to_string(), HOST])
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .expect("run python3");
            child
                .stdin
                .take()
                .expect("python3 stdin")
                .write_all(ca_pem.as_bytes())
                .expect("write CA");
            child.wait_with_output_bounded().expect("python3 output")
        })
        .await
        .expect("join python3");
        // The client's verdict is the result; a client that never connected leaves accept pending.
        server.abort();
        output
    }
}

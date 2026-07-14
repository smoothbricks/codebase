use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
    time::{Duration, Instant},
};

use rcgen::{CertificateParams, Issuer, KeyPair};
use rustls::{
    ServerConfig,
    pki_types::{CertificateDer, PrivatePkcs8KeyDer},
};
use sha2::{Digest, Sha256};
use thiserror::Error;
use time::OffsetDateTime;

use crate::config::{GatewayLimits, GatewayTimeouts, WorkspaceCa};

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
        let mut params = CertificateParams::new(vec![host.to_owned()])
            .map_err(|error| TlsError::Leaf(error.to_string()))?;
        let now = OffsetDateTime::now_utc();
        params.not_before = now - time::Duration::minutes(1);
        params.not_after = now
            + time::Duration::try_from(lifetime)
                .map_err(|_| TlsError::Leaf("invalid leaf lifetime".to_owned()))?;
        let certificate = params
            .signed_by(&key, &self.issuer)
            .map_err(|error| TlsError::Leaf(error.to_string()))?;
        let chain = vec![
            CertificateDer::from(certificate.der().to_vec()),
            self.certificate_der.clone(),
        ];
        let key_der = PrivatePkcs8KeyDer::from(key.serialize_der()).into();
        let mut config = ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(chain, key_der)
            .map_err(|error| TlsError::Leaf(error.to_string()))?;
        config.alpn_protocols = vec![b"http/1.1".to_vec()];
        Ok(Arc::new(config))
    }
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

use std::{
    convert::Infallible,
    future::Future,
    io::Cursor,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use async_trait::async_trait;
use bytes::Bytes;
use http::{
    HeaderMap, HeaderName, HeaderValue, Method, Request, Response, StatusCode, Uri, Version, header,
};
use http_body::{Body, Frame, SizeHint};
use http_body_util::{BodyExt as _, Empty, Full, Limited, combinators::BoxBody};
use hyper::{
    body::Incoming,
    client::conn::{http1 as client_http1, http2 as client_http2},
    server::conn::{http1 as server_http1, http2 as server_http2},
    service::service_fn,
};
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    sync::{mpsc, oneshot, watch},
    time::{Instant, Sleep, timeout},
};
use tokio_rustls::TlsAcceptor;

use crate::{
    actor::{
        Admission, AdmissionError, AuditAttempt, AuditDraft, Authentication, BoundListener,
        Command, CompletionLease, RequestIntent, RequestTarget,
    },
    cache::CacheBodyError,
    config::GatewayTimeouts,
    interfaces::{
        AuditKind, AuditStatus, AuthorizedTarget, BoxError, CredentialProtocol, CredentialProvider,
        CredentialQuery, NegotiatedTransport, UpstreamConnection, UpstreamConnector,
        UpstreamHealth, UpstreamPurpose,
    },
    mirror::{
        MirrorBody, MirrorCacheScope, MirrorCacheStatus, MirrorError, MirrorFetchRequest,
        MirrorOutcome, MirrorRequest, MirrorService, MirrorUpstream,
    },
    policy::{CanonicalHost, CanonicalTarget, EgressMode, TargetScheme, normalize_path},
};

const MAX_TARGET: usize = 8 * 1024;
const MAX_HEADERS: usize = 100;
const MAX_HEADER_FIELD: usize = 16 * 1024;
const MAX_HEADER_BYTES: usize = 64 * 1024;
const MAX_REQUEST_BODY: usize = 64 * 1024 * 1024;
const MAX_CLIENT_HELLO: usize = 64 * 1024;
const MAX_H2_CONCURRENT_STREAMS: u32 = 32;
const MAX_H2_SEND_BUFFER: usize = 64 * 1024;
const MAX_H2_RESET_STREAMS: usize = 32;

type ResponseBody = ProxyBody;
type ServiceResult = Result<Response<ResponseBody>, Infallible>;

#[derive(Clone)]
pub(crate) struct AcceptContext {
    pub workspace_id: String,
    pub commands: mpsc::Sender<Command>,
    pub credentials: Arc<dyn CredentialProvider>,
    pub connector: Arc<dyn UpstreamConnector>,
    pub mirror_service: MirrorService,
    pub timeouts: GatewayTimeouts,
    pub connection_stop: watch::Receiver<bool>,
    pub audit_stop: watch::Receiver<bool>,
}

pub(crate) async fn accept_loop(
    listener: BoundListener,
    context: AcceptContext,
    mut accept_stop: watch::Receiver<bool>,
    connection_stop: watch::Receiver<bool>,
    audit_stop: watch::Receiver<bool>,
) {
    match listener {
        BoundListener::Tcp(listener) => loop {
            tokio::select! {
                result = listener.accept() => {
                    let Ok((stream, _)) = result else { break };
                    let _ = stream.set_nodelay(true);
                    spawn_connection(stream, context.clone(), connection_stop.clone(), audit_stop.clone());
                }
                changed = accept_stop.changed() => {
                    if changed.is_err() || *accept_stop.borrow() { break; }
                }
            }
        },
        BoundListener::Unix(listener) => loop {
            tokio::select! {
                result = listener.accept() => {
                    let Ok((stream, _)) = result else { break };
                    spawn_connection(stream, context.clone(), connection_stop.clone(), audit_stop.clone());
                }
                changed = accept_stop.changed() => {
                    if changed.is_err() || *accept_stop.borrow() { break; }
                }
            }
        },
    }
}

fn spawn_connection<I>(
    stream: I,
    context: AcceptContext,
    mut connection_stop: watch::Receiver<bool>,
    mut audit_stop: watch::Receiver<bool>,
) where
    I: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Unpin + 'static,
{
    tokio::spawn(async move {
        let service_context = context.clone();
        let service = service_fn(move |request| {
            handle_request(
                request,
                service_context.clone(),
                Authentication::Bearer(None),
                None,
            )
        });
        let connection = server_http1::Builder::new()
            .timer(TokioTimer::new())
            .header_read_timeout(context.timeouts.request_headers)
            .max_headers(MAX_HEADERS)
            .serve_connection(TokioIo::new(stream), service)
            .with_upgrades();
        tokio::pin!(connection);
        tokio::select! {
            _ = &mut connection => {}
            changed = connection_stop.changed() => {
                if changed.is_ok() && *connection_stop.borrow() {
                    connection.as_mut().graceful_shutdown();
                    let _ = timeout(context.timeouts.request_total, &mut connection).await;
                }
            }
            changed = audit_stop.changed() => {
                if changed.is_ok() && *audit_stop.borrow() {
                    // Dropping the Hyper connection cancels the in-flight service future,
                    // including connector, credential, request-body, and response-body work.
                }
            }
        }
    });
}

async fn handle_request(
    request: Request<Incoming>,
    context: AcceptContext,
    inherited_authentication: Authentication,
    fixed_target: Option<CanonicalTarget>,
) -> ServiceResult {
    if let Err(error) = validate_request(&request) {
        return Ok(audited_problem(
            &context,
            &request,
            error.status,
            error.message,
            "malformed-request",
        )
        .await);
    }
    let authentication = match inherited_authentication {
        Authentication::Generation(generation) => Authentication::Generation(generation),
        Authentication::Bearer(_) => Authentication::Bearer(proxy_token(request.headers())),
    };
    if request.method() == Method::CONNECT {
        if fixed_target.is_some() {
            return Ok(audited_problem(
                &context,
                &request,
                StatusCode::METHOD_NOT_ALLOWED,
                "nested CONNECT is forbidden",
                "nested-connect",
            )
            .await);
        }
        return Ok(handle_connect(request, context, authentication).await);
    }
    let (target, path, audit_kind) = match request_target(&request, fixed_target.as_ref()) {
        Ok(value) => value,
        Err(error) => {
            return Ok(audited_problem(
                &context,
                &request,
                error.status,
                error.message,
                "invalid-target",
            )
            .await);
        }
    };
    let (path, trace_id) = extract_mirror_trace(&path, audit_kind).unwrap_or((path, None));
    let intent = RequestIntent {
        target,
        method: request.method().clone(),
        path: path.clone(),
        audit_kind,
        trace_id,
    };
    let admission = match admit(&context, authentication, intent).await {
        Ok(admission) => admission,
        Err(error) => return Ok(problem(error.status, error.message, error.hint.as_deref())),
    };
    if let Some(fixed) = fixed_target
        && admission.target != fixed
    {
        complete_now(
            &context,
            admission,
            AuditStatus::Denied,
            Some(StatusCode::FORBIDDEN),
            Some("authority-mismatch"),
            0,
            audit_kind,
        )
        .await;
        return Ok(problem(
            StatusCode::FORBIDDEN,
            "request authority differs from CONNECT authority",
            None,
        ));
    }
    let health = upstream_health(&context, &admission.target).await;
    if admission.protocol.is_some() {
        return Ok(handle_mirror_request(request, context, admission, health, audit_kind).await);
    }
    if health == UpstreamHealth::Offline {
        complete_now(
            &context,
            admission,
            AuditStatus::Offline,
            Some(StatusCode::SERVICE_UNAVAILABLE),
            Some("upstream-offline"),
            0,
            audit_kind,
        )
        .await;
        return Ok(problem(
            StatusCode::SERVICE_UNAVAILABLE,
            "upstream is offline",
            None,
        ));
    }
    let purpose = match admission.target.scheme {
        TargetScheme::Http => UpstreamPurpose::PlainHttp,
        TargetScheme::Https => UpstreamPurpose::TlsHttp,
    };
    let authorized = AuthorizedTarget {
        target: admission.target.clone(),
        purpose,
        private_network_authorized: admission.private_network_authorized,
    };
    let upstream = match timeout(
        context.timeouts.connect,
        context.connector.connect(&authorized),
    )
    .await
    {
        Ok(Ok(connection)) => connection,
        Ok(Err(_)) => {
            complete_now(
                &context,
                admission,
                AuditStatus::Failed,
                Some(StatusCode::BAD_GATEWAY),
                Some("connect-failed"),
                0,
                audit_kind,
            )
            .await;
            return Ok(problem(
                StatusCode::BAD_GATEWAY,
                "upstream connection failed",
                None,
            ));
        }
        Err(_) => {
            complete_now(
                &context,
                admission,
                AuditStatus::TimedOut,
                Some(StatusCode::GATEWAY_TIMEOUT),
                Some("connect-timeout"),
                0,
                audit_kind,
            )
            .await;
            return Ok(problem(
                StatusCode::GATEWAY_TIMEOUT,
                "upstream connect timed out",
                None,
            ));
        }
    };
    if upstream.transport == NegotiatedTransport::Raw {
        complete_now(
            &context,
            admission,
            AuditStatus::Failed,
            Some(StatusCode::BAD_GATEWAY),
            Some("upstream-protocol"),
            0,
            audit_kind,
        )
        .await;
        return Ok(problem(
            StatusCode::BAD_GATEWAY,
            "upstream connector returned raw transport for HTTP",
            None,
        ));
    }
    let request = match prepare_upstream_request(
        request,
        &admission,
        &admission.upstream_path,
        &context,
        upstream.transport,
    )
    .await
    {
        Ok(request) => request,
        Err(status) => {
            complete_now(
                &context,
                admission,
                AuditStatus::Failed,
                Some(status),
                Some("credential-failed"),
                0,
                audit_kind,
            )
            .await;
            return Ok(problem(
                status,
                "credential policy rejected the request",
                None,
            ));
        }
    };
    let mut sender = match timeout(
        context.timeouts.response_headers,
        handshake_upstream(upstream),
    )
    .await
    {
        Ok(Ok(sender)) => sender,
        Ok(Err(_)) => {
            complete_now(
                &context,
                admission,
                AuditStatus::Failed,
                Some(StatusCode::BAD_GATEWAY),
                Some("upstream-protocol"),
                0,
                audit_kind,
            )
            .await;
            return Ok(problem(
                StatusCode::BAD_GATEWAY,
                "upstream HTTP handshake failed",
                None,
            ));
        }
        Err(_) => {
            complete_now(
                &context,
                admission,
                AuditStatus::TimedOut,
                Some(StatusCode::GATEWAY_TIMEOUT),
                Some("upstream-handshake-timeout"),
                0,
                audit_kind,
            )
            .await;
            return Ok(problem(
                StatusCode::GATEWAY_TIMEOUT,
                "upstream HTTP handshake timed out",
                None,
            ));
        }
    };
    let response = match timeout(
        context.timeouts.response_headers,
        sender.send_request(request),
    )
    .await
    {
        Ok(Ok(response)) => response,
        Ok(Err(_)) => {
            complete_now(
                &context,
                admission,
                AuditStatus::Failed,
                Some(StatusCode::BAD_GATEWAY),
                Some("upstream-response"),
                0,
                audit_kind,
            )
            .await;
            return Ok(problem(
                StatusCode::BAD_GATEWAY,
                "upstream response failed",
                None,
            ));
        }
        Err(_) => {
            complete_now(
                &context,
                admission,
                AuditStatus::TimedOut,
                Some(StatusCode::GATEWAY_TIMEOUT),
                Some("response-headers-timeout"),
                0,
                audit_kind,
            )
            .await;
            return Ok(problem(
                StatusCode::GATEWAY_TIMEOUT,
                "upstream response headers timed out",
                None,
            ));
        }
    };
    let status = response.status();
    let (mut parts, body) = response.into_parts();
    strip_response_secrets(&mut parts.headers);
    strip_hop_headers(&mut parts.headers);
    let completion = Completion::new(context.commands.clone(), admission, status, None);
    let body = ProxyBody::stream(
        body.map_err(|error| -> BoxError { Box::new(error) })
            .boxed(),
        completion,
        context.timeouts.body_idle,
        context.timeouts.request_total,
    );
    Ok(Response::from_parts(parts, body))
}

enum UpstreamSender {
    Http1(client_http1::SendRequest<BoxBody<Bytes, BoxError>>),
    Http2(client_http2::SendRequest<BoxBody<Bytes, BoxError>>),
}

impl UpstreamSender {
    async fn send_request(
        &mut self,
        request: Request<BoxBody<Bytes, BoxError>>,
    ) -> Result<Response<Incoming>, hyper::Error> {
        match self {
            Self::Http1(sender) => sender.send_request(request).await,
            Self::Http2(sender) => sender.send_request(request).await,
        }
    }
}

async fn handshake_upstream(connection: UpstreamConnection) -> Result<UpstreamSender, ()> {
    match connection.transport {
        NegotiatedTransport::Http1 => {
            let (sender, connection) = client_http1::handshake(TokioIo::new(connection.io))
                .await
                .map_err(|_| ())?;
            tokio::spawn(async move {
                let _ = connection.await;
            });
            Ok(UpstreamSender::Http1(sender))
        }
        NegotiatedTransport::Http2 => {
            let mut builder = client_http2::Builder::new(TokioExecutor::new());
            builder
                .timer(TokioTimer::new())
                .initial_max_send_streams(MAX_H2_CONCURRENT_STREAMS as usize)
                .max_concurrent_streams(MAX_H2_CONCURRENT_STREAMS)
                .max_header_list_size(MAX_HEADER_BYTES as u32)
                .max_send_buf_size(MAX_H2_SEND_BUFFER)
                .max_concurrent_reset_streams(MAX_H2_RESET_STREAMS)
                .max_pending_accept_reset_streams(MAX_H2_RESET_STREAMS);
            let (sender, connection) = builder
                .handshake(TokioIo::new(connection.io))
                .await
                .map_err(|_| ())?;
            tokio::spawn(async move {
                let _ = connection.await;
            });
            Ok(UpstreamSender::Http2(sender))
        }
        NegotiatedTransport::Raw => Err(()),
    }
}

async fn handle_mirror_request(
    request: Request<Incoming>,
    context: AcceptContext,
    mut admission: Admission,
    health: UpstreamHealth,
    audit_kind: AuditKind,
) -> Response<ResponseBody> {
    let protocol = admission
        .protocol
        .expect("mirror admission must carry a protocol");
    let mut headers = request.headers().clone();
    strip_client_secrets(&mut headers);
    strip_hop_headers(&mut headers);
    let cache_scope = if admission.credential_allowed {
        MirrorCacheScope::Project(admission.repo_id.clone())
    } else {
        MirrorCacheScope::Anonymous
    };
    let mut mirror_request = match MirrorRequest::new(
        protocol,
        admission.target.clone(),
        request.method().clone(),
        admission.upstream_path.clone(),
        headers,
        cache_scope,
        admission.credential_allowed,
        None,
    ) {
        Ok(request) => request,
        Err(error) => {
            return mirror_failure(&context, admission, error, audit_kind).await;
        }
    };
    let mut initial_health = Some(health);
    loop {
        let hop_health = match initial_health.take() {
            Some(health) => health,
            None => upstream_health(&context, &admission.target).await,
        };
        let upstream = ProxyMirrorUpstream {
            context: &context,
            workspace_id: &admission.workspace_id,
            repo_id: &admission.repo_id,
            credential_allowed: admission.credential_allowed,
            impersonate: admission.impersonate,
            private_network_authorized: admission.private_network_authorized,
            trace_id: admission.trace_id.as_deref(),
            permit_id: admission.permit_id,
        };
        match context
            .mirror_service
            .execute(mirror_request, hop_health, &upstream)
            .await
        {
            Ok(MirrorOutcome::Response(response)) => {
                let status = response.response.status();
                let cache_status = response.cache_status;
                let (mut parts, body) = response.response.into_parts();
                strip_response_secrets(&mut parts.headers);
                strip_hop_headers(&mut parts.headers);
                let completion = Completion::new(
                    context.commands.clone(),
                    admission,
                    status,
                    Some(cache_status),
                );
                let body = ProxyBody::stream(
                    body,
                    completion,
                    context.timeouts.body_idle,
                    context.timeouts.request_total,
                );
                return Response::from_parts(parts, body);
            }
            Ok(MirrorOutcome::Redirect(redirect)) => {
                let generation = admission.generation;
                let request_path = admission.request_path.clone();
                let trace_id = admission.trace_id.clone();
                let redirected = redirect.request;
                let target = redirected.target.clone();
                let upstream_path = redirected.path.clone();
                complete_now(
                    &context,
                    admission,
                    AuditStatus::Completed,
                    Some(StatusCode::TEMPORARY_REDIRECT),
                    Some("mirror-redirect-hop"),
                    0,
                    audit_kind,
                )
                .await;
                let intent = RequestIntent {
                    target: RequestTarget::MirrorRedirect {
                        protocol,
                        target,
                        upstream_path,
                    },
                    method: redirected.method.clone(),
                    path: request_path,
                    audit_kind,
                    trace_id,
                };
                admission =
                    match admit(&context, Authentication::Generation(generation), intent).await {
                        Ok(admission) => admission,
                        Err(error) => {
                            return problem(error.status, error.message, error.hint.as_deref());
                        }
                    };
                let cache_scope = if admission.credential_allowed {
                    MirrorCacheScope::Project(admission.repo_id.clone())
                } else {
                    MirrorCacheScope::Anonymous
                };
                mirror_request = match MirrorRequest::from_redirect(
                    redirected,
                    cache_scope,
                    admission.credential_allowed,
                ) {
                    Ok(request) => request,
                    Err(error) => {
                        return mirror_failure(&context, admission, error, audit_kind).await;
                    }
                };
            }
            Err(error) => {
                return mirror_failure(&context, admission, error, audit_kind).await;
            }
        }
    }
}

async fn mirror_failure(
    context: &AcceptContext,
    admission: Admission,
    error: MirrorError,
    audit_kind: AuditKind,
) -> Response<ResponseBody> {
    let (status, audit_status, classification, message) = match error {
        MirrorError::OfflineMiss => (
            StatusCode::SERVICE_UNAVAILABLE,
            AuditStatus::Offline,
            "mirror-offline-miss",
            "mirror object is not available offline",
        ),
        MirrorError::MethodNotAllowed => (
            StatusCode::METHOD_NOT_ALLOWED,
            AuditStatus::Denied,
            "mirror-method",
            "mirror only accepts GET and HEAD",
        ),
        MirrorError::MissingIntegrity
        | MirrorError::ObjectTooLarge
        | MirrorError::InvalidProtocolPath
        | MirrorError::UnscopedCredential => (
            StatusCode::BAD_REQUEST,
            AuditStatus::Denied,
            "mirror-request",
            "mirror request is invalid",
        ),
        MirrorError::UnsafeRedirect
        | MirrorError::InvalidRedirect
        | MirrorError::TooManyRedirects => (
            StatusCode::BAD_GATEWAY,
            AuditStatus::Failed,
            "mirror-redirect",
            "mirror redirect was rejected",
        ),
        _ => (
            StatusCode::BAD_GATEWAY,
            AuditStatus::Failed,
            "mirror-service",
            "mirror service failed",
        ),
    };
    complete_now(
        context,
        admission,
        audit_status,
        Some(status),
        Some(classification),
        0,
        audit_kind,
    )
    .await;
    problem(status, message, None)
}

struct ProxyMirrorUpstream<'a> {
    context: &'a AcceptContext,
    workspace_id: &'a str,
    repo_id: &'a str,
    credential_allowed: bool,
    impersonate: bool,
    private_network_authorized: bool,
    trace_id: Option<&'a str>,
    permit_id: u64,
}

#[async_trait]
impl MirrorUpstream for ProxyMirrorUpstream<'_> {
    async fn fetch(
        &self,
        request: MirrorFetchRequest,
    ) -> Result<Response<MirrorBody>, CacheBodyError> {
        let purpose = match request.target.scheme {
            TargetScheme::Http => UpstreamPurpose::PlainHttp,
            TargetScheme::Https => UpstreamPurpose::TlsHttp,
        };
        let authorized = AuthorizedTarget {
            target: request.target.clone(),
            purpose,
            private_network_authorized: self.private_network_authorized,
        };
        let upstream = timeout(
            self.context.timeouts.connect,
            self.context.connector.connect(&authorized),
        )
        .await
        .map_err(|_| -> CacheBodyError { "mirror upstream connect timed out".into() })?
        .map_err(|error| -> CacheBodyError { Box::new(error) })?;
        if upstream.transport == NegotiatedTransport::Raw {
            return Err("mirror connector returned raw transport for HTTP".into());
        }

        let mut outbound = Request::builder()
            .method(request.method.clone())
            .body(
                Empty::<Bytes>::new()
                    .map_err(|never| -> CacheBodyError { match never {} })
                    .boxed(),
            )
            .map_err(|error| -> CacheBodyError { Box::new(error) })?;
        *outbound.headers_mut() = request.headers;
        strip_client_secrets(outbound.headers_mut());
        strip_hop_headers(outbound.headers_mut());
        outbound.headers_mut().insert(
            header::HOST,
            HeaderValue::from_str(&request.target.authority())
                .map_err(|error| -> CacheBodyError { Box::new(error) })?,
        );
        if !self.impersonate {
            let trace_id = self
                .trace_id
                .map(str::to_owned)
                .unwrap_or_else(|| format!("{:032x}", self.permit_id));
            let trace = format!("00-{trace_id}-{:016x}-01", self.permit_id);
            outbound.headers_mut().insert(
                HeaderName::from_static("traceparent"),
                HeaderValue::from_str(&trace)
                    .map_err(|error| -> CacheBodyError { Box::new(error) })?,
            );
        }
        if self.credential_allowed {
            let query = CredentialQuery {
                workspace_id: self.workspace_id.to_owned(),
                repo_id: self.repo_id.to_owned(),
                protocol: CredentialProtocol::from(request.protocol),
                origin: request.target.origin(),
                method: request.method,
                path: request.path.clone(),
            };
            if let Some(record) = timeout(
                self.context.timeouts.response_headers,
                self.context.credentials.lookup(&query),
            )
            .await
            .map_err(|_| -> CacheBodyError { "mirror credential lookup timed out".into() })?
            .map_err(|error| -> CacheBodyError { Box::new(error) })?
            {
                if !record.validate_for(&query) {
                    return Err("mirror credential scope mismatch".into());
                }
                outbound.headers_mut().insert(
                    record.header_name.clone(),
                    HeaderValue::from_str(record.header_value.as_str())
                        .map_err(|error| -> CacheBodyError { Box::new(error) })?,
                );
            }
        }
        match upstream.transport {
            NegotiatedTransport::Http1 => {
                *outbound.version_mut() = Version::HTTP_11;
                *outbound.uri_mut() = Uri::builder()
                    .path_and_query(request.path)
                    .build()
                    .map_err(|error| -> CacheBodyError { Box::new(error) })?;
            }
            NegotiatedTransport::Http2 => {
                *outbound.version_mut() = Version::HTTP_2;
                *outbound.uri_mut() = Uri::builder()
                    .scheme(request.target.scheme.as_str())
                    .authority(request.target.authority())
                    .path_and_query(request.path)
                    .build()
                    .map_err(|error| -> CacheBodyError { Box::new(error) })?;
            }
            NegotiatedTransport::Raw => {
                return Err("mirror connector returned raw transport for HTTP".into());
            }
        }
        let mut sender = timeout(
            self.context.timeouts.response_headers,
            handshake_upstream(upstream),
        )
        .await
        .map_err(|_| -> CacheBodyError { "mirror HTTP handshake timed out".into() })?
        .map_err(|_| -> CacheBodyError { "mirror HTTP handshake failed".into() })?;
        let response = timeout(
            self.context.timeouts.response_headers,
            sender.send_request(outbound),
        )
        .await
        .map_err(|_| -> CacheBodyError { "mirror response headers timed out".into() })?
        .map_err(|error| -> CacheBodyError { Box::new(error) })?;
        let (mut parts, body) = response.into_parts();
        strip_response_secrets(&mut parts.headers);
        strip_hop_headers(&mut parts.headers);
        let body = body
            .map_err(|error| -> CacheBodyError { Box::new(error) })
            .boxed();
        Ok(Response::from_parts(parts, body))
    }
}

async fn handle_connect(
    mut request: Request<Incoming>,
    context: AcceptContext,
    authentication: Authentication,
) -> Response<ResponseBody> {
    let authority = match request.uri().authority() {
        Some(authority) => authority.as_str(),
        None => {
            return audited_problem(
                &context,
                &request,
                StatusCode::BAD_REQUEST,
                "CONNECT requires host and explicit port",
                "connect-authority-missing",
            )
            .await;
        }
    };
    let target = match CanonicalTarget::from_authority(authority, TargetScheme::Https) {
        Ok(target) => target,
        Err(_) => {
            return audited_problem(
                &context,
                &request,
                StatusCode::BAD_REQUEST,
                "CONNECT authority is invalid",
                "connect-authority-invalid",
            )
            .await;
        }
    };
    if !host_matches(request.headers(), &target) {
        return audited_problem(
            &context,
            &request,
            StatusCode::BAD_REQUEST,
            "Host differs from CONNECT authority",
            "connect-host-mismatch",
        )
        .await;
    }
    let intent = RequestIntent {
        target: RequestTarget::Generic(target.clone()),
        method: Method::CONNECT,
        path: "/".to_owned(),
        audit_kind: AuditKind::Connect,
        trace_id: None,
    };
    let admission = match admit(&context, authentication, intent).await {
        Ok(admission) => admission,
        Err(error) => return problem(error.status, error.message, error.hint.as_deref()),
    };
    if !matches!(admission.mode, EgressMode::Opaque)
        && upstream_health(&context, &target).await == UpstreamHealth::Offline
    {
        complete_now(
            &context,
            admission,
            AuditStatus::Offline,
            Some(StatusCode::SERVICE_UNAVAILABLE),
            Some("upstream-offline"),
            0,
            AuditKind::Connect,
        )
        .await;
        return problem(StatusCode::SERVICE_UNAVAILABLE, "upstream is offline", None);
    }
    let upgraded = hyper::upgrade::on(&mut request);
    match admission.mode {
        EgressMode::Opaque => {
            let authorized = AuthorizedTarget {
                target,
                purpose: UpstreamPurpose::OpaqueTcp,
                private_network_authorized: admission.private_network_authorized,
            };
            spawn_opaque(upgraded, authorized, context, admission);
            empty(StatusCode::OK)
        }
        EgressMode::Intercept => {
            let host = admission.target.host.as_str();
            let (reply, receive) = oneshot::channel();
            if context
                .commands
                .send(Command::MintLeaf {
                    workspace_id: admission.workspace_id.clone(),
                    generation: admission.generation,
                    host,
                    reply,
                })
                .await
                .is_err()
            {
                complete_now(
                    &context,
                    admission,
                    AuditStatus::Failed,
                    Some(StatusCode::SERVICE_UNAVAILABLE),
                    Some("actor-stopped"),
                    0,
                    AuditKind::Connect,
                )
                .await;
                return problem(StatusCode::SERVICE_UNAVAILABLE, "gateway stopped", None);
            }
            let leaf = match receive.await {
                Ok(Ok(leaf)) => leaf,
                _ => {
                    complete_now(
                        &context,
                        admission,
                        AuditStatus::Failed,
                        Some(StatusCode::BAD_GATEWAY),
                        Some("leaf-signing"),
                        0,
                        AuditKind::Connect,
                    )
                    .await;
                    return problem(
                        StatusCode::BAD_GATEWAY,
                        "workspace certificate signing failed",
                        None,
                    );
                }
            };
            spawn_intercept(upgraded, leaf, context, admission);
            empty(StatusCode::OK)
        }
    }
}

fn spawn_opaque(
    upgraded: hyper::upgrade::OnUpgrade,
    authorized: AuthorizedTarget,
    context: AcceptContext,
    admission: Admission,
) {
    tokio::spawn(async move {
        let mut audit_stop = watch_for_audit_stop(&context);
        let session_stop = watch_for_session_stop(&context);
        let audit_grace = context.timeouts.connect;
        let transport = async move {
            let mut connection_stop = watch_for_session_stop(&context);
            let upgraded = match upgraded.await {
                Ok(upgraded) => upgraded,
                Err(_) => {
                    complete_now(
                        &context,
                        admission,
                        AuditStatus::Failed,
                        None,
                        Some("upgrade-failed"),
                        0,
                        AuditKind::Opaque,
                    )
                    .await;
                    return;
                }
            };
            let mut client = TokioIo::new(upgraded);
            let captured = tokio::select! {
                result = timeout(
                    context.timeouts.tls_handshake,
                    capture_client_hello(&mut client, &admission.target.host),
                ) => {
                    match result {
                        Ok(Ok(captured)) => captured,
                        Ok(Err(error)) => {
                            complete_now(
                                &context,
                                admission,
                                AuditStatus::Denied,
                                Some(StatusCode::FORBIDDEN),
                                Some(error.classification()),
                                0,
                                AuditKind::Opaque,
                            ).await;
                            return;
                        }
                        Err(_) => {
                            complete_now(
                                &context,
                                admission,
                                AuditStatus::TimedOut,
                                Some(StatusCode::GATEWAY_TIMEOUT),
                                Some("client-hello-timeout"),
                                0,
                                AuditKind::Opaque,
                            ).await;
                            return;
                        }
                    }
                }
                _ = connection_stop.changed() => {
                    complete_now(
                        &context,
                        admission,
                        AuditStatus::Cancelled,
                        None,
                        Some("session-rotated"),
                        0,
                        AuditKind::Opaque,
                    ).await;
                    return;
                }
            };

            if upstream_health(&context, &authorized.target).await == UpstreamHealth::Offline {
                complete_now(
                    &context,
                    admission,
                    AuditStatus::Offline,
                    Some(StatusCode::SERVICE_UNAVAILABLE),
                    Some("upstream-offline"),
                    0,
                    AuditKind::Opaque,
                )
                .await;
                return;
            }
            let upstream = match timeout(
                context.timeouts.connect,
                context.connector.connect(&authorized),
            )
            .await
            {
                Ok(Ok(connection)) => connection,
                Ok(Err(_)) => {
                    complete_now(
                        &context,
                        admission,
                        AuditStatus::Failed,
                        Some(StatusCode::BAD_GATEWAY),
                        Some("connect-failed"),
                        0,
                        AuditKind::Opaque,
                    )
                    .await;
                    return;
                }
                Err(_) => {
                    complete_now(
                        &context,
                        admission,
                        AuditStatus::TimedOut,
                        Some(StatusCode::GATEWAY_TIMEOUT),
                        Some("connect-timeout"),
                        0,
                        AuditKind::Opaque,
                    )
                    .await;
                    return;
                }
            };
            if upstream.transport != NegotiatedTransport::Raw {
                complete_now(
                    &context,
                    admission,
                    AuditStatus::Failed,
                    Some(StatusCode::BAD_GATEWAY),
                    Some("upstream-protocol"),
                    0,
                    AuditKind::Opaque,
                )
                .await;
                return;
            }
            let mut upstream = upstream.io;
            match timeout(context.timeouts.connect, upstream.write_all(&captured)).await {
                Ok(Ok(())) => {}
                Ok(Err(_)) => {
                    complete_now(
                        &context,
                        admission,
                        AuditStatus::Failed,
                        Some(StatusCode::BAD_GATEWAY),
                        Some("client-hello-replay"),
                        0,
                        AuditKind::Opaque,
                    )
                    .await;
                    return;
                }
                Err(_) => {
                    complete_now(
                        &context,
                        admission,
                        AuditStatus::TimedOut,
                        Some(StatusCode::GATEWAY_TIMEOUT),
                        Some("client-hello-replay-timeout"),
                        0,
                        AuditKind::Opaque,
                    )
                    .await;
                    return;
                }
            }
            let replayed = captured.len() as u64;
            let result = tokio::select! {
                result = tokio::io::copy_bidirectional(&mut client, &mut upstream) => {
                    result.map(|(from_client, from_upstream)| {
                        replayed
                            .saturating_add(from_client)
                            .saturating_add(from_upstream)
                    })
                }
                _ = tokio::time::sleep(context.timeouts.tunnel_total) => {
                    complete_now(&context, admission, AuditStatus::TimedOut, Some(StatusCode::GATEWAY_TIMEOUT), Some("tunnel-total-timeout"), replayed, AuditKind::Opaque).await;
                    return;
                }
                _ = connection_stop.changed() => {
                    complete_now(&context, admission, AuditStatus::Cancelled, None, Some("session-rotated"), replayed, AuditKind::Opaque).await;
                    return;
                }
            };
            match result {
                Ok(bytes) => {
                    complete_now(
                        &context,
                        admission,
                        AuditStatus::Completed,
                        Some(StatusCode::OK),
                        None,
                        bytes,
                        AuditKind::Opaque,
                    )
                    .await
                }
                Err(_) => {
                    complete_now(
                        &context,
                        admission,
                        AuditStatus::Failed,
                        None,
                        Some("tunnel-io"),
                        replayed,
                        AuditKind::Opaque,
                    )
                    .await
                }
            }
        };
        tokio::pin!(transport);
        tokio::select! {
            biased;
            _ = &mut transport => {}
            _ = audit_stop.changed() => {
                if *session_stop.borrow() {
                    let _ = timeout(audit_grace, &mut transport).await;
                }
            }
        }
    });
}

#[derive(Clone, Copy, Debug)]
enum ClientHelloError {
    Io,
    NotTls,
    TooLarge,
    MissingSni,
    SniMismatch,
}

impl ClientHelloError {
    const fn classification(self) -> &'static str {
        match self {
            Self::Io => "client-hello-io",
            Self::NotTls => "client-hello-invalid",
            Self::TooLarge => "client-hello-too-large",
            Self::MissingSni => "sni-missing",
            Self::SniMismatch => "sni-mismatch",
        }
    }
}

async fn capture_client_hello<I>(
    client: &mut I,
    expected_host: &CanonicalHost,
) -> Result<Vec<u8>, ClientHelloError>
where
    I: AsyncRead + Unpin,
{
    let mut acceptor = rustls::server::Acceptor::default();
    let mut captured = Vec::with_capacity(4 * 1024);
    let mut chunk = [0_u8; 4 * 1024];
    loop {
        let remaining = MAX_CLIENT_HELLO.saturating_sub(captured.len());
        if remaining == 0 {
            return Err(ClientHelloError::TooLarge);
        }
        let limit = remaining.min(chunk.len());
        let read = client
            .read(&mut chunk[..limit])
            .await
            .map_err(|_| ClientHelloError::Io)?;
        if read == 0 {
            return Err(ClientHelloError::NotTls);
        }
        captured.extend_from_slice(&chunk[..read]);
        let mut cursor = Cursor::new(&chunk[..read]);
        let consumed = acceptor
            .read_tls(&mut cursor)
            .map_err(|_| ClientHelloError::NotTls)?;
        if consumed != read {
            return Err(ClientHelloError::NotTls);
        }
        let Some(accepted) = acceptor.accept().map_err(|_| ClientHelloError::NotTls)? else {
            continue;
        };
        let client_hello = accepted.client_hello();
        match expected_host {
            CanonicalHost::Dns(_) => {
                let sni = client_hello
                    .server_name()
                    .ok_or(ClientHelloError::MissingSni)?;
                if !sni_matches(Some(sni), expected_host) {
                    return Err(ClientHelloError::SniMismatch);
                }
            }
            CanonicalHost::Ip(_) if client_hello.server_name().is_some() => {
                return Err(ClientHelloError::SniMismatch);
            }
            CanonicalHost::Ip(_) => {}
        }
        return Ok(captured);
    }
}

fn spawn_intercept(
    upgraded: hyper::upgrade::OnUpgrade,
    leaf: Arc<rustls::ServerConfig>,
    context: AcceptContext,
    admission: Admission,
) {
    tokio::spawn(async move {
        let mut audit_stop = watch_for_audit_stop(&context);
        let session_stop = watch_for_session_stop(&context);
        let audit_grace = context.timeouts.connect;
        let transport = async move {
            let upgraded = match upgraded.await {
                Ok(upgraded) => upgraded,
                Err(_) => {
                    complete_now(
                        &context,
                        admission,
                        AuditStatus::Failed,
                        None,
                        Some("upgrade-failed"),
                        0,
                        AuditKind::Connect,
                    )
                    .await;
                    return;
                }
            };
            let tls = match timeout(
                context.timeouts.tls_handshake,
                TlsAcceptor::from(leaf).accept(TokioIo::new(upgraded)),
            )
            .await
            {
                Ok(Ok(tls)) => tls,
                Ok(Err(_)) => {
                    complete_now(
                        &context,
                        admission,
                        AuditStatus::Denied,
                        None,
                        Some("client-tls"),
                        0,
                        AuditKind::Connect,
                    )
                    .await;
                    return;
                }
                Err(_) => {
                    complete_now(
                        &context,
                        admission,
                        AuditStatus::TimedOut,
                        None,
                        Some("client-tls-timeout"),
                        0,
                        AuditKind::Connect,
                    )
                    .await;
                    return;
                }
            };
            if !sni_matches(tls.get_ref().1.server_name(), &admission.target.host) {
                complete_now(
                    &context,
                    admission,
                    AuditStatus::Denied,
                    Some(StatusCode::FORBIDDEN),
                    Some("sni-mismatch"),
                    0,
                    AuditKind::Connect,
                )
                .await;
                return;
            }
            let negotiated = match tls.get_ref().1.alpn_protocol() {
                Some(b"h2") => NegotiatedTransport::Http2,
                Some(b"http/1.1") => NegotiatedTransport::Http1,
                Some(_) | None => {
                    complete_now(
                        &context,
                        admission,
                        AuditStatus::Denied,
                        Some(StatusCode::BAD_REQUEST),
                        Some("client-alpn"),
                        0,
                        AuditKind::Connect,
                    )
                    .await;
                    return;
                }
            };
            match negotiated {
                NegotiatedTransport::Http1 => {
                    let nested_context = context.clone();
                    let fixed_target = admission.target.clone();
                    let generation = admission.generation;
                    let service = service_fn(move |request| {
                        handle_request(
                            request,
                            nested_context.clone(),
                            Authentication::Generation(generation),
                            Some(fixed_target.clone()),
                        )
                    });
                    let connection = server_http1::Builder::new()
                        .timer(TokioTimer::new())
                        .header_read_timeout(context.timeouts.request_headers)
                        .max_headers(MAX_HEADERS)
                        .serve_connection(TokioIo::new(tls), service);
                    drive_intercept(connection, &context, admission).await;
                }
                NegotiatedTransport::Http2 => {
                    let nested_context = context.clone();
                    let fixed_target = admission.target.clone();
                    let generation = admission.generation;
                    let service = service_fn(move |request| {
                        handle_request(
                            request,
                            nested_context.clone(),
                            Authentication::Generation(generation),
                            Some(fixed_target.clone()),
                        )
                    });
                    let mut builder = server_http2::Builder::new(TokioExecutor::new());
                    builder
                        .timer(TokioTimer::new())
                        .max_concurrent_streams(MAX_H2_CONCURRENT_STREAMS)
                        .max_header_list_size(MAX_HEADER_BYTES as u32)
                        .max_send_buf_size(MAX_H2_SEND_BUFFER)
                        .max_pending_accept_reset_streams(MAX_H2_RESET_STREAMS)
                        .max_local_error_reset_streams(MAX_H2_RESET_STREAMS);
                    let connection = builder.serve_connection(TokioIo::new(tls), service);
                    drive_intercept(connection, &context, admission).await;
                }
                NegotiatedTransport::Raw => unreachable!("TLS ALPN cannot select raw transport"),
            }
        };
        tokio::pin!(transport);
        tokio::select! {
            biased;
            _ = &mut transport => {}
            _ = audit_stop.changed() => {
                if *session_stop.borrow() {
                    let _ = timeout(audit_grace, &mut transport).await;
                }
            }
        }
    });
}

async fn drive_intercept<F>(connection: F, context: &AcceptContext, admission: Admission)
where
    F: Future<Output = Result<(), hyper::Error>>,
{
    let mut stopped = watch_for_session_stop(context);
    tokio::pin!(connection);
    tokio::select! {
        result = &mut connection => {
            let (status, classification) = if result.is_ok() {
                (AuditStatus::Completed, None)
            } else {
                (AuditStatus::Failed, Some("intercept-io"))
            };
            complete_now(context, admission, status, Some(StatusCode::OK), classification, 0, AuditKind::Connect).await;
        }
        _ = stopped.changed() => {
            complete_now(context, admission, AuditStatus::Cancelled, None, Some("session-rotated"), 0, AuditKind::Connect).await;
        }
        _ = tokio::time::sleep(context.timeouts.tunnel_total) => {
            complete_now(context, admission, AuditStatus::TimedOut, None, Some("tunnel-total-timeout"), 0, AuditKind::Connect).await;
        }
    }
}

fn watch_for_session_stop(context: &AcceptContext) -> watch::Receiver<bool> {
    context.connection_stop.clone()
}

fn watch_for_audit_stop(context: &AcceptContext) -> watch::Receiver<bool> {
    context.audit_stop.clone()
}

async fn upstream_health(context: &AcceptContext, target: &CanonicalTarget) -> UpstreamHealth {
    timeout(context.timeouts.connect, context.connector.health(target))
        .await
        .unwrap_or(UpstreamHealth::Unknown)
}

async fn prepare_upstream_request(
    request: Request<Incoming>,
    admission: &Admission,
    path: &str,
    context: &AcceptContext,
    transport: NegotiatedTransport,
) -> Result<Request<BoxBody<Bytes, BoxError>>, StatusCode> {
    let (mut parts, body) = request.into_parts();
    strip_client_secrets(&mut parts.headers);
    strip_hop_headers(&mut parts.headers);
    parts.headers.insert(
        header::HOST,
        HeaderValue::from_str(&admission.target.authority())
            .map_err(|_| StatusCode::BAD_REQUEST)?,
    );
    if !admission.impersonate {
        let trace_id = admission
            .trace_id
            .clone()
            .unwrap_or_else(|| format!("{:032x}", admission.permit_id));
        let trace = format!("00-{trace_id}-{:016x}-01", admission.permit_id);
        parts.headers.insert(
            HeaderName::from_static("traceparent"),
            HeaderValue::from_str(&trace).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?,
        );
    }
    if admission.credential_allowed {
        let protocol = admission
            .protocol
            .map(CredentialProtocol::from)
            .unwrap_or(CredentialProtocol::Generic);
        let query = CredentialQuery {
            workspace_id: admission.workspace_id.clone(),
            repo_id: admission.repo_id.clone(),
            protocol,
            origin: admission.target.origin(),
            method: parts.method.clone(),
            path: path.to_owned(),
        };
        if let Some(record) = timeout(
            context.timeouts.response_headers,
            context.credentials.lookup(&query),
        )
        .await
        .map_err(|_| StatusCode::GATEWAY_TIMEOUT)?
        .map_err(|_| StatusCode::BAD_GATEWAY)?
        {
            if !record.validate_for(&query) {
                return Err(StatusCode::BAD_GATEWAY);
            }
            let value = HeaderValue::from_str(record.header_value.as_str())
                .map_err(|_| StatusCode::BAD_GATEWAY)?;
            parts.headers.insert(record.header_name.clone(), value);
        }
    }
    parts.uri = match transport {
        NegotiatedTransport::Http1 => {
            parts.version = Version::HTTP_11;
            Uri::builder().path_and_query(path).build()
        }
        NegotiatedTransport::Http2 => {
            parts.version = Version::HTTP_2;
            Uri::builder()
                .scheme(admission.target.scheme.as_str())
                .authority(admission.target.authority())
                .path_and_query(path)
                .build()
        }
        NegotiatedTransport::Raw => return Err(StatusCode::BAD_GATEWAY),
    }
    .map_err(|_| StatusCode::BAD_REQUEST)?;
    let body = Limited::new(body, MAX_REQUEST_BODY).boxed();
    Ok(Request::from_parts(parts, body))
}

fn request_target(
    request: &Request<Incoming>,
    fixed: Option<&CanonicalTarget>,
) -> Result<(RequestTarget, String, AuditKind), RequestError> {
    let path = request
        .uri()
        .path_and_query()
        .map(|value| value.as_str())
        .unwrap_or("/")
        .to_owned();
    let local_mirror_kind = if fixed.is_none()
        && request.uri().scheme().is_none()
        && request.uri().authority().is_none()
    {
        local_mirror_kind(request.uri().path())
    } else {
        None
    };
    if local_mirror_kind.is_none() {
        normalize_path(request.uri().path())
            .map_err(|_| RequestError::bad("request path is ambiguous"))?;
    }
    if let Some(target) = fixed {
        if !request_authority_matches(request, target) {
            return Err(RequestError::bad(
                "request authority differs from CONNECT authority",
            ));
        }
        return Ok((
            RequestTarget::Generic(target.clone()),
            path,
            AuditKind::Intercept,
        ));
    }
    if let (Some(scheme), Some(authority)) = (request.uri().scheme_str(), request.uri().authority())
    {
        let scheme = TargetScheme::parse(scheme)
            .map_err(|_| RequestError::bad("unsupported proxy URI scheme"))?;
        let port = authority
            .port_u16()
            .or(match scheme {
                TargetScheme::Http => Some(80),
                TargetScheme::Https => Some(443),
            })
            .ok_or_else(|| RequestError::bad("target port is missing"))?;
        let canonical_authority = match CanonicalHost::parse(authority.host()) {
            Ok(CanonicalHost::Ip(std::net::IpAddr::V6(ip))) => format!("[{ip}]:{port}"),
            Ok(host) => format!("{}:{port}", host.as_str()),
            Err(_) => return Err(RequestError::bad("target host is invalid")),
        };
        let target = CanonicalTarget::from_authority(&canonical_authority, scheme)
            .map_err(|_| RequestError::bad("target authority is invalid"))?;
        if !host_matches(request.headers(), &target) {
            return Err(RequestError::bad("absolute URI and Host disagree"));
        }
        return Ok((RequestTarget::Generic(target), path, AuditKind::Http));
    }
    if let Some(kind) = local_mirror_kind {
        return Ok((RequestTarget::LocalMirror, path, kind));
    }
    Err(RequestError::bad(
        "generic proxy requests require absolute-form URI",
    ))
}
fn extract_mirror_trace(path: &str, kind: AuditKind) -> Option<(String, Option<String>)> {
    let protocol = match kind {
        AuditKind::Npm => "npm",
        AuditKind::Go => "go",
        _ => return None,
    };
    let prefix = format!("/{protocol}/t/");
    let remainder = path.strip_prefix(&prefix)?;
    let (traceparent, suffix) = remainder.split_once('/')?;
    let bytes = traceparent.as_bytes();
    if bytes.len() != 55
        || bytes[2] != b'-'
        || bytes[35] != b'-'
        || bytes[52] != b'-'
        || !bytes
            .iter()
            .enumerate()
            .all(|(index, byte)| matches!(index, 2 | 35 | 52) || byte.is_ascii_hexdigit())
        || &traceparent[..2] == "ff"
        || traceparent[3..35].bytes().all(|byte| byte == b'0')
        || traceparent[36..52].bytes().all(|byte| byte == b'0')
    {
        return None;
    }
    Some((
        format!("/{protocol}/{suffix}"),
        Some(traceparent[3..35].to_ascii_lowercase()),
    ))
}

fn local_mirror_kind(path: &str) -> Option<AuditKind> {
    if path == "/npm" || path.starts_with("/npm/") {
        Some(AuditKind::Npm)
    } else if path == "/cargo" || path.starts_with("/cargo/") {
        Some(AuditKind::Cargo)
    } else if path == "/go" || path.starts_with("/go/") {
        Some(AuditKind::Go)
    } else {
        None
    }
}

fn validate_request<B>(request: &Request<B>) -> Result<(), RequestError>
where
    B: Body,
{
    if request.uri().to_string().len() > MAX_TARGET {
        return Err(RequestError::new(
            StatusCode::URI_TOO_LONG,
            "request target exceeds 8 KiB",
        ));
    }
    let headers = request.headers();
    if headers.len() > MAX_HEADERS {
        return Err(RequestError::new(
            StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE,
            "too many header fields",
        ));
    }
    let mut aggregate = 0usize;
    for (name, value) in headers {
        if name.as_str().len() > MAX_HEADER_FIELD || value.as_bytes().len() > MAX_HEADER_FIELD {
            return Err(RequestError::new(
                StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE,
                "header field exceeds 16 KiB",
            ));
        }
        aggregate = aggregate
            .saturating_add(name.as_str().len())
            .saturating_add(value.as_bytes().len());
    }
    if aggregate > MAX_HEADER_BYTES {
        return Err(RequestError::new(
            StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE,
            "aggregate headers exceed 64 KiB",
        ));
    }
    if headers.get_all(header::HOST).iter().count() > 1 {
        return Err(RequestError::bad("duplicate Host header"));
    }
    if request.version() == Version::HTTP_2 {
        if request.uri().scheme().is_none() || request.uri().authority().is_none() {
            return Err(RequestError::bad(
                "HTTP/2 requires valid :scheme and :authority projections",
            ));
        }
        for forbidden in [
            header::CONNECTION,
            header::TRANSFER_ENCODING,
            header::UPGRADE,
            HeaderName::from_static("keep-alive"),
            HeaderName::from_static("proxy-connection"),
        ] {
            if headers.contains_key(forbidden) {
                return Err(RequestError::bad(
                    "HTTP/2 forbids connection-specific headers",
                ));
            }
        }
        if let Some(te) = headers.get(HeaderName::from_static("te"))
            && !te.as_bytes().eq_ignore_ascii_case(b"trailers")
        {
            return Err(RequestError::bad(
                "HTTP/2 TE header may contain only trailers",
            ));
        }
    }
    let content_lengths: Vec<_> = headers.get_all(header::CONTENT_LENGTH).iter().collect();
    if content_lengths.len() > 1 {
        let first = content_lengths[0].as_bytes();
        if content_lengths
            .iter()
            .any(|value| value.as_bytes() != first)
        {
            return Err(RequestError::bad("conflicting Content-Length headers"));
        }
        return Err(RequestError::bad("repeated Content-Length is forbidden"));
    }
    if headers.contains_key(header::TRANSFER_ENCODING)
        && headers.contains_key(header::CONTENT_LENGTH)
    {
        return Err(RequestError::bad(
            "Transfer-Encoding with Content-Length is forbidden",
        ));
    }
    if let Some(transfer) = headers.get(header::TRANSFER_ENCODING)
        && !transfer.as_bytes().eq_ignore_ascii_case(b"chunked")
    {
        return Err(RequestError::bad("unsupported transfer coding"));
    }
    if request.method() == Method::CONNECT {
        if headers.contains_key(header::CONTENT_LENGTH)
            || headers.contains_key(header::TRANSFER_ENCODING)
        {
            return Err(RequestError::bad("CONNECT cannot have a body"));
        }
        if request.body().size_hint().upper().unwrap_or(0) != 0 {
            return Err(RequestError::bad("CONNECT cannot have a body"));
        }
    }
    Ok(())
}

fn proxy_token(headers: &HeaderMap) -> Option<String> {
    let values: Vec<_> = headers
        .get_all(header::PROXY_AUTHORIZATION)
        .iter()
        .collect();
    if values.len() != 1 {
        return None;
    }
    let value = values[0].to_str().ok()?;
    let token = value.strip_prefix("Bearer ")?;
    if token.is_empty() || token.bytes().any(|byte| byte.is_ascii_whitespace()) {
        return None;
    }
    Some(token.to_owned())
}
fn request_authority_matches<B>(request: &Request<B>, target: &CanonicalTarget) -> bool {
    if request.version() != Version::HTTP_2 {
        return host_matches(request.headers(), target);
    }
    if request.uri().scheme_str() != Some(target.scheme.as_str()) {
        return false;
    }
    let Some(authority) = request.uri().authority() else {
        return false;
    };
    if !authority_value_matches(authority.as_str(), target) {
        return false;
    }
    let hosts: Vec<_> = request.headers().get_all(header::HOST).iter().collect();
    hosts.is_empty()
        || (hosts.len() == 1
            && hosts[0]
                .to_str()
                .is_ok_and(|host| authority_value_matches(host, target)))
}

fn authority_value_matches(value: &str, target: &CanonicalTarget) -> bool {
    let with_default_port = if value
        .parse::<http::uri::Authority>()
        .ok()
        .and_then(|authority| authority.port_u16())
        .is_none()
    {
        match &target.host {
            CanonicalHost::Ip(std::net::IpAddr::V6(ip)) => format!("[{ip}]:{}", target.port),
            _ => format!("{value}:{}", target.port),
        }
    } else {
        value.to_owned()
    };
    CanonicalTarget::from_authority(&with_default_port, target.scheme)
        .is_ok_and(|candidate| candidate == *target)
}

fn host_matches(headers: &HeaderMap, target: &CanonicalTarget) -> bool {
    let values: Vec<_> = headers.get_all(header::HOST).iter().collect();
    if values.len() != 1 {
        return false;
    }
    values[0]
        .to_str()
        .is_ok_and(|value| authority_value_matches(value, target))
}

fn sni_matches(sni: Option<&str>, host: &CanonicalHost) -> bool {
    match host {
        CanonicalHost::Dns(expected) => sni
            .and_then(|actual| CanonicalHost::parse(actual).ok())
            .is_some_and(|actual| actual == CanonicalHost::Dns(expected.clone())),
        CanonicalHost::Ip(_) => sni.is_none(),
    }
}

fn strip_client_secrets(headers: &mut HeaderMap) {
    const SENSITIVE: &[&str] = &[
        "authorization",
        "proxy-authorization",
        "cookie",
        "set-cookie",
        "npm-auth-token",
        "x-npm-token",
        "npm-otp",
        "x-goog-api-key",
        "traceparent",
        "tracestate",
    ];
    for name in SENSITIVE {
        headers.remove(*name);
    }
}

fn strip_response_secrets(headers: &mut HeaderMap) {
    headers.remove(header::SET_COOKIE);
    headers.remove(header::PROXY_AUTHENTICATE);
}

fn strip_hop_headers(headers: &mut HeaderMap) {
    let named_by_connection: Vec<HeaderName> = headers
        .get_all(header::CONNECTION)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .filter_map(|name| HeaderName::from_bytes(name.trim().as_bytes()).ok())
        .collect();
    for name in named_by_connection {
        headers.remove(name);
    }
    for name in [
        header::CONNECTION,
        header::UPGRADE,
        header::TRAILER,
        header::TRANSFER_ENCODING,
        HeaderName::from_static("te"),
        HeaderName::from_static("keep-alive"),
        HeaderName::from_static("proxy-connection"),
    ] {
        headers.remove(name);
    }
}

async fn audited_problem(
    context: &AcceptContext,
    request: &Request<Incoming>,
    status: StatusCode,
    message: &'static str,
    classification: &'static str,
) -> Response<ResponseBody> {
    let (reply, receive) = oneshot::channel();
    let attempt = AuditAttempt {
        kind: if request.method() == Method::CONNECT {
            AuditKind::Connect
        } else {
            AuditKind::Http
        },
        host: request
            .uri()
            .authority()
            .map(|authority| authority.to_string()),
        method: request.method().to_string(),
        path: request
            .uri()
            .path_and_query()
            .map_or_else(|| "/".to_owned(), ToString::to_string),
        status: AuditStatus::Denied,
        http_status: status,
        classification,
    };
    if context
        .commands
        .send(Command::AuditDenial {
            workspace_id: context.workspace_id.clone(),
            attempt,
            reply,
        })
        .await
        .is_err()
        || !matches!(receive.await, Ok(true))
    {
        return problem(
            StatusCode::SERVICE_UNAVAILABLE,
            "audit service is unavailable",
            None,
        );
    }
    problem(status, message, None)
}

async fn admit(
    context: &AcceptContext,
    authentication: Authentication,
    intent: RequestIntent,
) -> Result<Admission, AdmissionError> {
    let (reply, receive) = oneshot::channel();
    let (cancel, cancelled) = oneshot::channel();
    let _cancellation = AdmissionCancellation(Some(cancel));
    context
        .commands
        .send(Command::Admit {
            workspace_id: context.workspace_id.clone(),
            authentication,
            intent,
            reply,
            cancelled,
        })
        .await
        .map_err(|_| AdmissionError {
            status: StatusCode::SERVICE_UNAVAILABLE,
            message: "gateway stopped",
            hint: None,
            audit_status: AuditStatus::Failed,
        })?;
    receive.await.map_err(|_| AdmissionError {
        status: StatusCode::SERVICE_UNAVAILABLE,
        message: "gateway stopped",
        hint: None,
        audit_status: AuditStatus::Failed,
    })?
}

struct AdmissionCancellation(Option<oneshot::Sender<()>>);

impl Drop for AdmissionCancellation {
    fn drop(&mut self) {
        if let Some(cancel) = self.0.take() {
            let _ = cancel.send(());
        }
    }
}

async fn complete_now(
    _context: &AcceptContext,
    admission: Admission,
    status: AuditStatus,
    http_status: Option<StatusCode>,
    classification: Option<&str>,
    bytes: u64,
    kind: AuditKind,
) {
    let draft = completion_draft(&admission, status, http_status, classification, bytes, kind);
    admission.finish(draft);
}

fn completion_draft(
    admission: &Admission,
    status: AuditStatus,
    http_status: Option<StatusCode>,
    classification: Option<&str>,
    bytes: u64,
    kind: AuditKind,
) -> AuditDraft {
    AuditDraft {
        workspace_id: admission.workspace_id.clone(),
        revision: admission.revision,
        endpoint: admission.endpoint.clone(),
        kind,
        host: Some(admission.target.authority()),
        method: Some(admission.method.to_string()),
        path: Some(admission.request_path.clone()),
        status,
        http_status: http_status.map(|status| status.as_u16()),
        bytes,
        trace_id: admission
            .trace_id
            .clone()
            .or_else(|| Some(format!("{:032x}", admission.permit_id))),
        grant_hint: None,
        classification: classification.map(str::to_owned),
        mirror_cache_status: None,
    }
}

struct Completion {
    lease: Option<CompletionLease>,
    draft: Option<AuditDraft>,
}

impl Completion {
    fn new(
        _commands: mpsc::Sender<Command>,
        mut admission: Admission,
        status: StatusCode,
        mirror_cache_status: Option<MirrorCacheStatus>,
    ) -> Self {
        let mut draft = completion_draft(
            &admission,
            AuditStatus::Completed,
            Some(status),
            None,
            0,
            admission.audit_kind,
        );
        draft.mirror_cache_status = mirror_cache_status;
        Self {
            lease: Some(admission.take_completion()),
            draft: Some(draft),
        }
    }

    fn send(&mut self, status: AuditStatus, classification: Option<&str>, bytes: u64) {
        let (Some(lease), Some(mut draft)) = (self.lease.take(), self.draft.take()) else {
            return;
        };
        draft.status = status;
        draft.classification = classification.map(str::to_owned);
        draft.bytes = bytes;
        lease.finish(draft);
    }
}

impl Drop for Completion {
    fn drop(&mut self) {
        let bytes = self.draft.as_ref().map_or(0, |draft| draft.bytes);
        self.send(AuditStatus::Cancelled, Some("response-dropped"), bytes);
    }
}

pin_project_lite::pin_project! {
    pub(crate) struct ProxyBody {
        #[pin]
        inner: BoxBody<Bytes, BoxError>,
        completion: Option<Completion>,
        bytes: u64,
        #[pin]
        idle: Sleep,
        idle_duration: Duration,
        #[pin]
        total: Sleep,
    }
}

impl ProxyBody {
    fn stream(
        inner: BoxBody<Bytes, BoxError>,
        completion: Completion,
        idle: Duration,
        total: Duration,
    ) -> Self {
        Self {
            inner,
            completion: Some(completion),
            bytes: 0,
            idle: tokio::time::sleep(idle),
            idle_duration: idle,
            total: tokio::time::sleep(total),
        }
    }

    fn boxed(inner: BoxBody<Bytes, BoxError>) -> Self {
        let long = Duration::from_secs(365 * 24 * 60 * 60);
        Self {
            inner,
            completion: None,
            bytes: 0,
            idle: tokio::time::sleep(long),
            idle_duration: long,
            total: tokio::time::sleep(long),
        }
    }
}

impl Body for ProxyBody {
    type Data = Bytes;
    type Error = BoxError;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let mut this = self.project();
        if this.total.as_mut().poll(cx).is_ready() {
            if let Some(completion) = this.completion.as_mut() {
                completion.send(
                    AuditStatus::TimedOut,
                    Some("request-total-timeout"),
                    *this.bytes,
                );
            }
            *this.completion = None;
            return Poll::Ready(Some(Err("request total timeout".into())));
        }
        if this.idle.as_mut().poll(cx).is_ready() {
            if let Some(completion) = this.completion.as_mut() {
                completion.send(
                    AuditStatus::TimedOut,
                    Some("body-idle-timeout"),
                    *this.bytes,
                );
            }
            *this.completion = None;
            return Poll::Ready(Some(Err("body idle timeout".into())));
        }
        match this.inner.as_mut().poll_frame(cx) {
            Poll::Ready(Some(Ok(frame))) => {
                if let Some(data) = frame.data_ref() {
                    *this.bytes = this.bytes.saturating_add(data.len() as u64);
                    this.idle
                        .as_mut()
                        .reset(Instant::now() + *this.idle_duration);
                }
                if let Some(completion) = this.completion.as_mut() {
                    if let Some(draft) = completion.draft.as_mut() {
                        draft.bytes = *this.bytes;
                    }
                    if this.inner.as_ref().is_end_stream() {
                        completion.send(AuditStatus::Completed, None, *this.bytes);
                        *this.completion = None;
                    }
                }
                Poll::Ready(Some(Ok(frame)))
            }
            Poll::Ready(Some(Err(error))) => {
                if let Some(completion) = this.completion.as_mut() {
                    completion.send(AuditStatus::Failed, Some("response-body"), *this.bytes);
                }
                *this.completion = None;
                Poll::Ready(Some(Err(error)))
            }
            Poll::Ready(None) => {
                if let Some(completion) = this.completion.as_mut() {
                    completion.send(AuditStatus::Completed, None, *this.bytes);
                }
                *this.completion = None;
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

fn empty(status: StatusCode) -> Response<ResponseBody> {
    let body = Empty::<Bytes>::new()
        .map_err(|never| -> BoxError { match never {} })
        .boxed();
    Response::builder()
        .status(status)
        .body(ProxyBody::boxed(body))
        .expect("static response is valid")
}

fn problem(status: StatusCode, message: &str, hint: Option<&str>) -> Response<ResponseBody> {
    let value = serde_json::json!({
        "error": message,
        "grantHint": hint,
    });
    let bytes = Bytes::from(value.to_string());
    let body = Full::new(bytes)
        .map_err(|never| -> BoxError { match never {} })
        .boxed();
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "application/json")
        .body(ProxyBody::boxed(body))
        .expect("static response is valid")
}

struct RequestError {
    status: StatusCode,
    message: &'static str,
}

impl RequestError {
    const fn new(status: StatusCode, message: &'static str) -> Self {
        Self { status, message }
    }

    const fn bad(message: &'static str) -> Self {
        Self::new(StatusCode::BAD_REQUEST, message)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn h2_request(uri: &str) -> Request<Empty<Bytes>> {
        Request::builder()
            .version(Version::HTTP_2)
            .uri(uri)
            .body(Empty::new())
            .expect("valid test request")
    }

    #[test]
    fn h2_validation_rejects_connection_specific_headers() {
        for (name, value) in [
            ("connection", "keep-alive"),
            ("keep-alive", "timeout=5"),
            ("proxy-connection", "keep-alive"),
            ("transfer-encoding", "chunked"),
            ("upgrade", "websocket"),
            ("te", "gzip"),
        ] {
            let mut request = h2_request("https://secure.test/allowed");
            request.headers_mut().insert(
                HeaderName::from_bytes(name.as_bytes()).expect("valid header"),
                HeaderValue::from_static(value),
            );
            assert!(validate_request(&request).is_err(), "{name} was accepted");
        }
    }

    #[test]
    fn h2_validation_accepts_only_well_formed_pseudo_header_projection() {
        let missing_authority = Request::builder()
            .version(Version::HTTP_2)
            .uri("/allowed")
            .body(Empty::<Bytes>::new())
            .expect("valid test request");
        assert!(validate_request(&missing_authority).is_err());

        let mut trailers = h2_request("https://secure.test/allowed");
        trailers.headers_mut().insert(
            HeaderName::from_static("te"),
            HeaderValue::from_static("trailers"),
        );
        assert!(validate_request(&trailers).is_ok());
    }

    #[test]
    fn h2_authority_host_and_connect_target_must_be_equivalent() {
        let target =
            CanonicalTarget::from_authority("secure.test:443", TargetScheme::Https).unwrap();
        let matching = h2_request("https://secure.test/allowed");
        assert!(request_authority_matches(&matching, &target));

        let mismatched = h2_request("https://other.test/allowed");
        assert!(!request_authority_matches(&mismatched, &target));

        let mut conflicting_host = h2_request("https://secure.test/allowed");
        conflicting_host
            .headers_mut()
            .insert(header::HOST, HeaderValue::from_static("other.test"));
        assert!(!request_authority_matches(&conflicting_host, &target));
    }

    #[test]
    fn npm_and_go_trace_paths_are_stripped_and_adopted() {
        let traceparent = "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01";
        let (npm_path, npm_trace) = extract_mirror_trace(
            &format!("/npm/t/{traceparent}/@scope%2fpkg"),
            AuditKind::Npm,
        )
        .expect("npm trace prefix");
        assert_eq!(npm_path, "/npm/@scope%2fpkg");
        assert_eq!(
            npm_trace.as_deref(),
            Some("0123456789abcdef0123456789abcdef")
        );

        let (go_path, go_trace) = extract_mirror_trace(
            &format!("/go/t/{traceparent}/example.com/mod/@v/list"),
            AuditKind::Go,
        )
        .expect("Go trace prefix");
        assert_eq!(go_path, "/go/example.com/mod/@v/list");
        assert_eq!(go_trace, npm_trace);
        assert!(extract_mirror_trace("/npm/t/invalid/react", AuditKind::Npm).is_none());
    }
}

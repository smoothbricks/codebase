use std::{
    convert::Infallible,
    future::Future,
    io::{self, Cursor},
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
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
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf},
    sync::{mpsc, oneshot, watch},
    time::{Instant, Sleep, timeout, timeout_at},
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
    repo_mirror::RepoMirrorHandle,
    sim_broker::{SimBrokerError, SimBrokerHandle, SimRequest},
};

const MAX_TARGET: usize = 8 * 1024;
const MAX_HEADERS: usize = 100;
const MAX_HEADER_FIELD: usize = 16 * 1024;
const MAX_HEADER_BYTES: usize = 64 * 1024;
const MAX_REQUEST_BODY: usize = 64 * 1024 * 1024;
const MAX_CLIENT_HELLO: usize = 64 * 1024;
const MAX_SIM_REQUEST: usize = 64 * 1024;
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
    // Intentionally carried only as a structural fence: data-plane routing must
    // never expose the control-plane repository mirror.
    #[allow(dead_code)]
    pub repo_mirror: RepoMirrorHandle,
    pub sim_broker: SimBrokerHandle,
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
    let admitted_at = Instant::now();
    let request_deadline = admitted_at + context.timeouts.request_total;
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
        return Ok(handle_connect(request, context, authentication, admitted_at).await);
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
    let mut admission = match admit(&context, authentication, intent).await {
        Ok(admission) => admission,
        Err(error) => return Ok(problem(error.status, error.message, error.hint.as_deref())),
    };
    apply_trace_context(request.headers(), &mut admission);
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
    if audit_kind == AuditKind::Sim {
        return Ok(handle_sim_request(request, context, admission, request_deadline).await);
    }
    let health = upstream_health(&context, &admission.target).await;
    if admission.protocol.is_some() {
        return Ok(handle_mirror_request(
            request,
            context,
            admission,
            health,
            audit_kind,
            admitted_at,
        )
        .await);
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
    let connector_timeout = context.timeouts.connect
        + if purpose == UpstreamPurpose::TlsHttp {
            context.timeouts.tls_handshake
        } else {
            Duration::ZERO
        };
    let upstream = match timeout_at(
        stage_deadline(request_deadline, connector_timeout),
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
            let classification = if Instant::now() >= request_deadline {
                "request-total-timeout"
            } else {
                "connect-timeout"
            };
            complete_now(
                &context,
                admission,
                AuditStatus::TimedOut,
                Some(StatusCode::GATEWAY_TIMEOUT),
                Some(classification),
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
    let (request, mut request_timeout) = match prepare_upstream_request(
        request,
        &admission,
        &admission.upstream_path,
        &context,
        upstream.transport,
        request_deadline,
    )
    .await
    {
        Ok(prepared) => prepared,
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
    let mut sender = match timeout_at(
        stage_deadline(request_deadline, context.timeouts.response_headers),
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
            let classification = if Instant::now() >= request_deadline {
                "request-total-timeout"
            } else {
                "upstream-handshake-timeout"
            };
            complete_now(
                &context,
                admission,
                AuditStatus::TimedOut,
                Some(StatusCode::GATEWAY_TIMEOUT),
                Some(classification),
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
    let response = match timeout_at(
        stage_deadline(request_deadline, context.timeouts.response_headers),
        sender.send_request(request),
    )
    .await
    {
        Ok(Ok(response)) => response,
        Ok(Err(_)) => {
            let request_timeout = request_timeout.try_recv().ok();
            let (audit_status, http_status, classification, message) = request_timeout.map_or(
                (
                    AuditStatus::Failed,
                    StatusCode::BAD_GATEWAY,
                    "upstream-response",
                    "upstream response failed",
                ),
                |timed_out| {
                    (
                        AuditStatus::TimedOut,
                        StatusCode::GATEWAY_TIMEOUT,
                        timed_out.classification(),
                        "request body timed out",
                    )
                },
            );
            complete_now(
                &context,
                admission,
                audit_status,
                Some(http_status),
                Some(classification),
                0,
                audit_kind,
            )
            .await;
            return Ok(problem(http_status, message, None));
        }
        Err(_) => {
            let body_timeout = request_timeout.try_recv().ok();
            let classification = body_timeout.map_or_else(
                || {
                    if Instant::now() >= request_deadline {
                        "request-total-timeout"
                    } else {
                        "response-headers-timeout"
                    }
                },
                RequestBodyTimeout::classification,
            );
            complete_now(
                &context,
                admission,
                AuditStatus::TimedOut,
                Some(StatusCode::GATEWAY_TIMEOUT),
                Some(classification),
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
    let total_deadline = admitted_at + response_total_duration(&parts.headers, context.timeouts);
    let completion = Completion::new(context.commands.clone(), admission, status, None);
    let body = ProxyBody::stream(
        body.map_err(|error| -> BoxError { Box::new(error) })
            .boxed(),
        completion,
        Some(request_timeout),
        context.timeouts.body_idle,
        total_deadline,
    );
    Ok(Response::from_parts(parts, body))
}

async fn handle_sim_request(
    request: Request<Incoming>,
    context: AcceptContext,
    admission: Admission,
    request_deadline: Instant,
) -> Response<ResponseBody> {
    let content_type = request
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .map(str::trim);
    if content_type != Some("application/json") {
        complete_now(
            &context,
            admission,
            AuditStatus::Denied,
            Some(StatusCode::UNSUPPORTED_MEDIA_TYPE),
            Some("sim-content-type"),
            0,
            AuditKind::Sim,
        )
        .await;
        return problem(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "simulator requests require application/json",
            None,
        );
    }

    let body = Limited::new(request.into_body(), MAX_SIM_REQUEST).boxed();
    let (signal, mut request_timeout) = oneshot::channel();
    let body = TimedRequestBody::new(body, signal, context.timeouts.body_idle, request_deadline);
    let bytes = match body.collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(_) => {
            let timed_out = request_timeout.try_recv().ok();
            let (status, audit_status, classification, message) = timed_out.map_or(
                (
                    StatusCode::BAD_REQUEST,
                    AuditStatus::Denied,
                    "sim-request-body",
                    "simulator request body is invalid",
                ),
                |timed_out| {
                    (
                        StatusCode::GATEWAY_TIMEOUT,
                        AuditStatus::TimedOut,
                        timed_out.classification(),
                        "simulator request body timed out",
                    )
                },
            );
            complete_now(
                &context,
                admission,
                audit_status,
                Some(status),
                Some(classification),
                0,
                AuditKind::Sim,
            )
            .await;
            return problem(status, message, None);
        }
    };
    let sim_request = match serde_json::from_slice::<SimRequest>(&bytes) {
        Ok(request) => request,
        Err(_) => {
            complete_now(
                &context,
                admission,
                AuditStatus::Denied,
                Some(StatusCode::BAD_REQUEST),
                Some("sim-request-json"),
                0,
                AuditKind::Sim,
            )
            .await;
            return problem(
                StatusCode::BAD_REQUEST,
                "simulator request JSON is invalid",
                None,
            );
        }
    };
    let result = match timeout_at(
        request_deadline,
        context
            .sim_broker
            .request(admission.workspace_id.clone(), sim_request),
    )
    .await
    {
        Ok(Ok(result)) => result,
        Ok(Err(error)) => {
            let (status, audit_status, classification) = sim_error(&error);
            complete_now(
                &context,
                admission,
                audit_status,
                Some(status),
                Some(classification),
                0,
                AuditKind::Sim,
            )
            .await;
            return problem(status, "simulator operation was rejected", None);
        }
        Err(_) => {
            complete_now(
                &context,
                admission,
                AuditStatus::TimedOut,
                Some(StatusCode::GATEWAY_TIMEOUT),
                Some("request-total-timeout"),
                0,
                AuditKind::Sim,
            )
            .await;
            return problem(
                StatusCode::GATEWAY_TIMEOUT,
                "simulator operation timed out",
                None,
            );
        }
    };
    let payload = match serde_json::to_vec(&result) {
        Ok(payload) => payload,
        Err(_) => {
            complete_now(
                &context,
                admission,
                AuditStatus::Failed,
                Some(StatusCode::INTERNAL_SERVER_ERROR),
                Some("sim-response-json"),
                0,
                AuditKind::Sim,
            )
            .await;
            return problem(
                StatusCode::INTERNAL_SERVER_ERROR,
                "simulator response encoding failed",
                None,
            );
        }
    };
    complete_now(
        &context,
        admission,
        AuditStatus::Completed,
        Some(StatusCode::OK),
        None,
        payload.len() as u64,
        AuditKind::Sim,
    )
    .await;
    let body = Full::new(Bytes::from(payload))
        .map_err(|never| -> BoxError { match never {} })
        .boxed();
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/json")
        .body(ProxyBody::boxed(body))
        .expect("static simulator response is valid")
}

fn sim_error(error: &SimBrokerError) -> (StatusCode, AuditStatus, &'static str) {
    match error {
        SimBrokerError::NotGranted => (
            StatusCode::FORBIDDEN,
            AuditStatus::Denied,
            "sim-not-granted",
        ),
        SimBrokerError::InvalidScheme => (
            StatusCode::FORBIDDEN,
            AuditStatus::Denied,
            "sim-invalid-scheme",
        ),
        SimBrokerError::ApprovalRequired => (
            StatusCode::FORBIDDEN,
            AuditStatus::Denied,
            "sim-approval-required",
        ),
        SimBrokerError::InstallDisabled => (
            StatusCode::FORBIDDEN,
            AuditStatus::Denied,
            "sim-install-disabled",
        ),
        SimBrokerError::ProjectNotConfigured | SimBrokerError::UnknownWorkspace => {
            (StatusCode::FORBIDDEN, AuditStatus::Denied, "sim-project")
        }
        SimBrokerError::Capacity => (
            StatusCode::TOO_MANY_REQUESTS,
            AuditStatus::Limited,
            "sim-capacity",
        ),
        SimBrokerError::RunnerTimeout => (
            StatusCode::GATEWAY_TIMEOUT,
            AuditStatus::TimedOut,
            "sim-runner-timeout",
        ),
        SimBrokerError::AuditUnavailable | SimBrokerError::Stopped => (
            StatusCode::SERVICE_UNAVAILABLE,
            AuditStatus::Failed,
            "sim-unavailable",
        ),
        SimBrokerError::InvalidUrl
        | SimBrokerError::InvalidReceipt
        | SimBrokerError::InvalidApp
        | SimBrokerError::DigestMismatch
        | SimBrokerError::InvalidDigest
        | SimBrokerError::InvalidIdentifier
        | SimBrokerError::ReceiptReplay => (
            StatusCode::BAD_REQUEST,
            AuditStatus::Denied,
            "sim-invalid-request",
        ),
        SimBrokerError::InsecureDropRoot
        | SimBrokerError::RunnerFailed
        | SimBrokerError::InvalidRunnerOutput => {
            (StatusCode::BAD_GATEWAY, AuditStatus::Failed, "sim-failed")
        }
    }
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
    admitted_at: Instant,
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
            upstream_span_id: admission.upstream_span_id,
            trace_flags: admission.trace_flags,
            tracestate: admission.tracestate.as_deref(),
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
                let total_deadline =
                    admitted_at + response_total_duration(&parts.headers, context.timeouts);
                let completion = Completion::new(
                    context.commands.clone(),
                    admission,
                    status,
                    Some(cache_status),
                );
                let body = ProxyBody::stream(
                    body,
                    completion,
                    None,
                    context.timeouts.body_idle,
                    total_deadline,
                );
                return Response::from_parts(parts, body);
            }
            Ok(MirrorOutcome::Redirect(redirect)) => {
                let generation = admission.generation;
                let request_path = admission.request_path.clone();
                let trace_id = admission.trace_id.clone();
                let parent_span_id = Some(admission.span_id);
                let trace_flags = admission.trace_flags;
                let tracestate = admission.tracestate.clone();
                let trace_classification = admission.trace_classification.clone();
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
                admission.parent_span_id = parent_span_id;
                admission.trace_flags = trace_flags;
                admission.tracestate = tracestate;
                admission.trace_classification = trace_classification;
                admission.upstream_span_id = Some(admission.span_id.wrapping_add(1).max(1));
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
    upstream_span_id: Option<u64>,
    trace_flags: u8,
    tracestate: Option<&'a str>,
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
                .ok_or_else(|| -> CacheBodyError { "mirror trace id is missing".into() })?;
            let upstream_span_id = self
                .upstream_span_id
                .ok_or_else(|| -> CacheBodyError { "mirror upstream span id is missing".into() })?;
            let trace = serialize_traceparent(trace_id, upstream_span_id, self.trace_flags);
            outbound.headers_mut().insert(
                HeaderName::from_static("traceparent"),
                HeaderValue::from_str(&trace)
                    .map_err(|error| -> CacheBodyError { Box::new(error) })?,
            );
            if let Some(tracestate) = self.tracestate {
                outbound.headers_mut().insert(
                    HeaderName::from_static("tracestate"),
                    HeaderValue::from_str(tracestate)
                        .map_err(|error| -> CacheBodyError { Box::new(error) })?,
                );
            }
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
    admitted_at: Instant,
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
    let mut admission = match admit(&context, authentication, intent).await {
        Ok(admission) => admission,
        Err(error) => return problem(error.status, error.message, error.hint.as_deref()),
    };
    admission.trace_id = Some(format!("{:032x}", admission.permit_id.max(1)));
    admission.upstream_span_id = Some(admission.span_id.wrapping_add(1).max(1));
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
            spawn_opaque(upgraded, authorized, context, admission, admitted_at);
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
            spawn_intercept(upgraded, leaf, context, admission, admitted_at);
            empty(StatusCode::OK)
        }
    }
}

struct ActivityIo<T> {
    inner: T,
    started: Instant,
    activity_ns: Arc<AtomicU64>,
}

impl<T> ActivityIo<T> {
    fn new(inner: T, started: Instant, activity_ns: Arc<AtomicU64>) -> Self {
        Self {
            inner,
            started,
            activity_ns,
        }
    }

    fn touch(&self) {
        let elapsed = Instant::now().saturating_duration_since(self.started);
        self.activity_ns.store(
            u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX),
            Ordering::Release,
        );
    }
}

impl<T: AsyncRead + Unpin> AsyncRead for ActivityIo<T> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let before = buffer.filled().len();
        let result = Pin::new(&mut this.inner).poll_read(cx, buffer);
        if matches!(result, Poll::Ready(Ok(()))) && buffer.filled().len() > before {
            this.touch();
        }
        result
    }
}

impl<T: AsyncWrite + Unpin> AsyncWrite for ActivityIo<T> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let result = Pin::new(&mut this.inner).poll_write(cx, buffer);
        if matches!(result, Poll::Ready(Ok(written)) if written > 0) {
            this.touch();
        }
        result
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}

async fn wait_for_opaque_idle(started: Instant, activity_ns: &AtomicU64, idle: Duration) {
    loop {
        let observed = activity_ns.load(Ordering::Acquire);
        tokio::time::sleep_until(started + Duration::from_nanos(observed) + idle).await;
        if activity_ns.load(Ordering::Acquire) == observed {
            return;
        }
    }
}

fn spawn_opaque(
    upgraded: hyper::upgrade::OnUpgrade,
    authorized: AuthorizedTarget,
    context: AcceptContext,
    admission: Admission,
    admitted_at: Instant,
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
            let activity_started = Instant::now();
            let activity_ns = Arc::new(AtomicU64::new(0));
            let mut client = ActivityIo::new(client, activity_started, Arc::clone(&activity_ns));
            let mut upstream =
                ActivityIo::new(upstream, activity_started, Arc::clone(&activity_ns));
            let result = tokio::select! {
                result = tokio::io::copy_bidirectional(&mut client, &mut upstream) => {
                    result.map(|(from_client, from_upstream)| {
                        replayed
                            .saturating_add(from_client)
                            .saturating_add(from_upstream)
                    })
                }
                () = wait_for_opaque_idle(
                    activity_started,
                    activity_ns.as_ref(),
                    context.timeouts.body_idle,
                ) => {
                    complete_now(&context, admission, AuditStatus::TimedOut, Some(StatusCode::GATEWAY_TIMEOUT), Some("opaque-idle-timeout"), replayed, AuditKind::Opaque).await;
                    return;
                }
                _ = tokio::time::sleep_until(admitted_at + context.timeouts.tunnel_total) => {
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
    admitted_at: Instant,
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
                    drive_intercept(
                        connection,
                        &context,
                        admission,
                        admitted_at + context.timeouts.tunnel_total,
                    )
                    .await;
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
                    drive_intercept(
                        connection,
                        &context,
                        admission,
                        admitted_at + context.timeouts.tunnel_total,
                    )
                    .await;
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

async fn drive_intercept<F>(
    connection: F,
    context: &AcceptContext,
    admission: Admission,
    total_deadline: Instant,
) where
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
        _ = tokio::time::sleep_until(total_deadline) => {
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
    total_deadline: Instant,
) -> Result<
    (
        Request<BoxBody<Bytes, BoxError>>,
        oneshot::Receiver<RequestBodyTimeout>,
    ),
    StatusCode,
> {
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
            .as_deref()
            .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;
        let upstream_span_id = admission
            .upstream_span_id
            .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;
        let trace = serialize_traceparent(trace_id, upstream_span_id, admission.trace_flags);
        parts.headers.insert(
            HeaderName::from_static("traceparent"),
            HeaderValue::from_str(&trace).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?,
        );
        if let Some(tracestate) = admission.tracestate.as_deref() {
            parts.headers.insert(
                HeaderName::from_static("tracestate"),
                HeaderValue::from_str(tracestate).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?,
            );
        }
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
    let (signal, request_timeout) = oneshot::channel();
    let body =
        TimedRequestBody::new(body, signal, context.timeouts.body_idle, total_deadline).boxed();
    Ok((Request::from_parts(parts, body), request_timeout))
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct IncomingTraceContext {
    trace_id: String,
    parent_span_id: u64,
    flags: u8,
    tracestate: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum TraceContextAdmission {
    Absent,
    Valid(IncomingTraceContext),
    Invalid,
}

fn parse_trace_context(headers: &HeaderMap) -> TraceContextAdmission {
    let mut parents = headers.get_all("traceparent").iter();
    let Some(parent) = parents.next() else {
        return if headers.contains_key("tracestate") {
            TraceContextAdmission::Invalid
        } else {
            TraceContextAdmission::Absent
        };
    };
    if parents.next().is_some() {
        return TraceContextAdmission::Invalid;
    }
    let Ok(parent) = parent.to_str() else {
        return TraceContextAdmission::Invalid;
    };
    let Some((trace_id, parent_span_id, flags)) = parse_traceparent(parent) else {
        return TraceContextAdmission::Invalid;
    };
    let tracestate = match parse_tracestate(headers) {
        Ok(value) => value,
        Err(()) => return TraceContextAdmission::Invalid,
    };
    TraceContextAdmission::Valid(IncomingTraceContext {
        trace_id,
        parent_span_id,
        flags,
        tracestate,
    })
}

fn parse_traceparent(value: &str) -> Option<(String, u64, u8)> {
    let bytes = value.as_bytes();
    if bytes.len() < 55
        || bytes[2] != b'-'
        || bytes[35] != b'-'
        || bytes[52] != b'-'
        || !bytes[..55]
            .iter()
            .enumerate()
            .all(|(index, byte)| matches!(index, 2 | 35 | 52) || byte.is_ascii_hexdigit())
    {
        return None;
    }
    let version = u8::from_str_radix(&value[..2], 16).ok()?;
    if version == u8::MAX
        || (version == 0 && bytes.len() != 55)
        || (version != 0 && bytes.len() > 55 && (bytes[55] != b'-' || value.ends_with('-')))
    {
        return None;
    }
    let trace_id = &value[3..35];
    let parent = &value[36..52];
    if trace_id.bytes().all(|byte| byte == b'0') || parent.bytes().all(|byte| byte == b'0') {
        return None;
    }
    let parent_span_id = u64::from_str_radix(parent, 16).ok()?;
    let flags = u8::from_str_radix(&value[53..55], 16).ok()?;
    Some((trace_id.to_ascii_lowercase(), parent_span_id, flags))
}

fn parse_tracestate(headers: &HeaderMap) -> Result<Option<String>, ()> {
    let values = headers.get_all("tracestate");
    let mut members = Vec::new();
    let mut total = 0_usize;
    for value in values.iter() {
        let value = value.to_str().map_err(|_| ())?;
        total = total
            .checked_add(value.len())
            .and_then(|length| length.checked_add(usize::from(!members.is_empty())))
            .ok_or(())?;
        if total > 512 {
            return Err(());
        }
        for member in value.split(',') {
            let member = member.trim_matches([' ', '\t']);
            let (key, value) = member.split_once('=').ok_or(())?;
            if !valid_tracestate_key(key)
                || value.is_empty()
                || value.len() > 256
                || value.starts_with([' ', '\t'])
                || value.ends_with([' ', '\t'])
                || !value
                    .bytes()
                    .all(|byte| (0x20..=0x7e).contains(&byte) && byte != b',' && byte != b'=')
                || members
                    .iter()
                    .any(|existing: &String| existing.starts_with(&format!("{key}=")))
            {
                return Err(());
            }
            members.push(format!("{key}={value}"));
            if members.len() > 32 {
                return Err(());
            }
        }
    }
    Ok((!members.is_empty()).then(|| members.join(",")))
}

fn valid_tracestate_key(key: &str) -> bool {
    let (tenant, system) = key
        .split_once('@')
        .map_or((None, key), |(tenant, system)| (Some(tenant), system));
    if let Some(tenant) = tenant
        && (tenant.is_empty()
            || tenant.len() > 241
            || !tenant
                .bytes()
                .enumerate()
                .all(|(index, byte)| valid_key_byte(byte, index == 0, true)))
    {
        return false;
    }
    !system.is_empty()
        && system.len() <= 14
        && system
            .bytes()
            .enumerate()
            .all(|(index, byte)| valid_key_byte(byte, index == 0, false))
}

fn valid_key_byte(byte: u8, first: bool, tenant: bool) -> bool {
    if first {
        return byte.is_ascii_lowercase() || (tenant && byte.is_ascii_digit());
    }
    byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'_' | b'-' | b'*' | b'/')
}

fn serialize_traceparent(trace_id: &str, span_id: u64, flags: u8) -> String {
    format!("00-{trace_id}-{span_id:016x}-{flags:02x}")
}

fn apply_trace_context(headers: &HeaderMap, admission: &mut Admission) {
    admission.upstream_span_id = Some(admission.span_id.wrapping_add(1).max(1));
    match parse_trace_context(headers) {
        TraceContextAdmission::Valid(context) => {
            admission.trace_id = Some(context.trace_id);
            admission.parent_span_id = Some(context.parent_span_id);
            admission.trace_flags = context.flags;
            admission.tracestate = context.tracestate;
            admission.trace_classification = None;
        }
        TraceContextAdmission::Absent => {
            admission
                .trace_id
                .get_or_insert_with(|| format!("{:032x}", admission.permit_id.max(1)));
            admission.parent_span_id = None;
            admission.tracestate = None;
            admission.trace_classification = None;
        }
        TraceContextAdmission::Invalid => {
            admission
                .trace_id
                .get_or_insert_with(|| format!("{:032x}", admission.permit_id.max(1)));
            admission.trace_flags = 1;
            admission.parent_span_id = None;
            admission.tracestate = None;
            admission.trace_classification = Some("invalid-trace-context".to_owned());
        }
    }
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
    let local_kind = if fixed.is_none()
        && request.uri().scheme().is_none()
        && request.uri().authority().is_none()
    {
        local_request_kind(request.uri().path())
    } else {
        None
    };
    if local_kind.is_none() {
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
    if let Some(kind) = local_kind {
        let target = if kind == AuditKind::Sim {
            RequestTarget::LocalSim
        } else {
            RequestTarget::LocalMirror
        };
        return Ok((target, path, kind));
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

fn local_request_kind(path: &str) -> Option<AuditKind> {
    if path == "/sim" {
        return Some(AuditKind::Sim);
    }
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
        repo_id: admission.repo_id.clone(),
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
        span_id: admission.span_id,
        parent_span_id: admission.parent_span_id,
        upstream_span_id: admission.upstream_span_id,
        tracestate: admission.tracestate.clone(),
        grant_hint: None,
        classification: classification
            .map(str::to_owned)
            .or_else(|| admission.trace_classification.clone()),
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
        if let Some(classification) = classification {
            draft.classification = Some(classification.to_owned());
        }
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

fn stage_deadline(total_deadline: Instant, duration: Duration) -> Instant {
    std::cmp::min(total_deadline, Instant::now() + duration)
}

fn response_total_duration(headers: &HeaderMap, timeouts: GatewayTimeouts) -> Duration {
    let streaming = headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .is_some_and(|value| value.trim().eq_ignore_ascii_case("text/event-stream"));
    if streaming {
        timeouts.tunnel_total
    } else {
        timeouts.request_total
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RequestBodyTimeout {
    Idle,
    Total,
}

impl RequestBodyTimeout {
    const fn classification(self) -> &'static str {
        match self {
            Self::Idle => "request-body-idle-timeout",
            Self::Total => "request-total-timeout",
        }
    }
}

pin_project_lite::pin_project! {
    struct TimedRequestBody {
        #[pin]
        inner: BoxBody<Bytes, BoxError>,
        signal: Option<oneshot::Sender<RequestBodyTimeout>>,
        #[pin]
        idle: Sleep,
        idle_duration: Duration,
        #[pin]
        total: Sleep,
    }
}

impl TimedRequestBody {
    fn new(
        inner: BoxBody<Bytes, BoxError>,
        signal: oneshot::Sender<RequestBodyTimeout>,
        idle: Duration,
        total_deadline: Instant,
    ) -> Self {
        Self {
            inner,
            signal: Some(signal),
            idle: tokio::time::sleep(idle),
            idle_duration: idle,
            total: tokio::time::sleep_until(total_deadline),
        }
    }
}

impl Body for TimedRequestBody {
    type Data = Bytes;
    type Error = BoxError;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let mut this = self.project();
        let timed_out = if this.total.as_mut().poll(cx).is_ready() {
            Some(RequestBodyTimeout::Total)
        } else if this.idle.as_mut().poll(cx).is_ready() {
            Some(RequestBodyTimeout::Idle)
        } else {
            None
        };
        if let Some(timed_out) = timed_out {
            if let Some(signal) = this.signal.take() {
                let _ = signal.send(timed_out);
            }
            return Poll::Ready(Some(Err(timed_out.classification().into())));
        }
        match this.inner.as_mut().poll_frame(cx) {
            Poll::Ready(Some(Ok(frame))) => {
                if frame.data_ref().is_some_and(|data| !data.is_empty()) {
                    this.idle
                        .as_mut()
                        .reset(Instant::now() + *this.idle_duration);
                }
                Poll::Ready(Some(Ok(frame)))
            }
            Poll::Ready(Some(Err(error))) => {
                *this.signal = None;
                Poll::Ready(Some(Err(error)))
            }
            Poll::Ready(None) => {
                *this.signal = None;
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

pin_project_lite::pin_project! {
    pub(crate) struct ProxyBody {
        #[pin]
        inner: BoxBody<Bytes, BoxError>,
        completion: Option<Completion>,
        request_timeout: Option<oneshot::Receiver<RequestBodyTimeout>>,
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
        request_timeout: Option<oneshot::Receiver<RequestBodyTimeout>>,
        idle: Duration,
        total_deadline: Instant,
    ) -> Self {
        Self {
            inner,
            completion: Some(completion),
            request_timeout,
            bytes: 0,
            idle: tokio::time::sleep(idle),
            idle_duration: idle,
            total: tokio::time::sleep_until(total_deadline),
        }
    }

    fn boxed(inner: BoxBody<Bytes, BoxError>) -> Self {
        let long = Duration::from_secs(365 * 24 * 60 * 60);
        Self {
            inner,
            completion: None,
            request_timeout: None,
            bytes: 0,
            idle: tokio::time::sleep(long),
            idle_duration: long,
            total: tokio::time::sleep_until(Instant::now() + long),
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
        if let Some(receiver) = this.request_timeout.as_mut() {
            match Pin::new(receiver).poll(cx) {
                Poll::Ready(Ok(timed_out)) => {
                    if let Some(completion) = this.completion.as_mut() {
                        completion.send(
                            AuditStatus::TimedOut,
                            Some(timed_out.classification()),
                            *this.bytes,
                        );
                    }
                    *this.completion = None;
                    *this.request_timeout = None;
                    return Poll::Ready(Some(Err(timed_out.classification().into())));
                }
                Poll::Ready(Err(_)) => *this.request_timeout = None,
                Poll::Pending => {}
            }
        }
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
    #[test]
    fn generic_w3c_context_is_strictly_parsed_and_canonically_serialized() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "traceparent",
            HeaderValue::from_static("00-4BF92F3577B34DA6A3CE929D0E0E4736-00F067AA0BA902B7-03"),
        );
        headers.append(
            "tracestate",
            HeaderValue::from_static("vendor=value, tenant@system=opaque"),
        );
        let TraceContextAdmission::Valid(context) = parse_trace_context(&headers) else {
            panic!("valid generic W3C context was rejected");
        };
        assert_eq!(context.trace_id, "4bf92f3577b34da6a3ce929d0e0e4736");
        assert_eq!(context.parent_span_id, 0x00f0_67aa_0ba9_02b7);
        assert_eq!(context.flags, 3);
        assert_eq!(
            context.tracestate.as_deref(),
            Some("vendor=value,tenant@system=opaque")
        );
        assert_eq!(
            serialize_traceparent(&context.trace_id, 0x1122_3344_5566_7788, context.flags),
            "00-4bf92f3577b34da6a3ce929d0e0e4736-1122334455667788-03"
        );
    }

    #[test]
    fn invalid_w3c_context_is_rejected_as_one_unit() {
        for value in [
            "00-00000000000000000000000000000000-00f067aa0ba902b7-01",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01",
            "ff-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-extra",
            "not-a-traceparent",
        ] {
            let mut headers = HeaderMap::new();
            headers.insert(
                "traceparent",
                HeaderValue::from_str(value).expect("ASCII test header"),
            );
            assert_eq!(
                parse_trace_context(&headers),
                TraceContextAdmission::Invalid,
                "{value}"
            );
        }

        let mut duplicate_state = HeaderMap::new();
        duplicate_state.insert(
            "traceparent",
            HeaderValue::from_static("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"),
        );
        duplicate_state.insert(
            "tracestate",
            HeaderValue::from_static("vendor=one,vendor=two"),
        );
        assert_eq!(
            parse_trace_context(&duplicate_state),
            TraceContextAdmission::Invalid
        );

        let mut orphan_state = HeaderMap::new();
        orphan_state.insert("tracestate", HeaderValue::from_static("vendor=one"));
        assert_eq!(
            parse_trace_context(&orphan_state),
            TraceContextAdmission::Invalid
        );
    }
    #[derive(Debug)]
    struct PendingBody;

    impl Body for PendingBody {
        type Data = Bytes;
        type Error = BoxError;

        fn poll_frame(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
            Poll::Pending
        }
    }

    fn pending_body() -> BoxBody<Bytes, BoxError> {
        PendingBody.boxed()
    }

    #[test]
    fn sse_uses_streaming_total_while_ordinary_responses_do_not() {
        let timeouts = GatewayTimeouts {
            request_total: Duration::from_secs(15 * 60),
            tunnel_total: Duration::from_secs(60 * 60),
            ..GatewayTimeouts::default()
        };
        let mut headers = HeaderMap::new();
        assert_eq!(
            response_total_duration(&headers, timeouts),
            Duration::from_secs(15 * 60)
        );
        headers.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("text/event-stream; charset=utf-8"),
        );
        assert_eq!(
            response_total_duration(&headers, timeouts),
            Duration::from_secs(60 * 60)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn request_body_idle_and_total_timers_classify_independently() {
        let (idle_signal, idle_receive) = oneshot::channel();
        let idle_body = TimedRequestBody::new(
            pending_body(),
            idle_signal,
            Duration::from_secs(5),
            Instant::now() + Duration::from_secs(10),
        );
        let idle_task = tokio::spawn(async move {
            let mut idle_body = Box::pin(idle_body);
            std::future::poll_fn(|cx| idle_body.as_mut().poll_frame(cx))
                .await
                .expect("timeout frame")
                .expect_err("idle timeout is an error")
                .to_string()
        });
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(5)).await;
        assert_eq!(
            idle_receive.await.expect("idle timeout signal"),
            RequestBodyTimeout::Idle
        );
        assert_eq!(
            idle_task.await.expect("idle task"),
            "request-body-idle-timeout"
        );

        let (total_signal, total_receive) = oneshot::channel();
        let total_body = TimedRequestBody::new(
            pending_body(),
            total_signal,
            Duration::from_secs(10),
            Instant::now() + Duration::from_secs(5),
        );
        let total_task = tokio::spawn(async move {
            let mut total_body = Box::pin(total_body);
            std::future::poll_fn(|cx| total_body.as_mut().poll_frame(cx))
                .await
                .expect("timeout frame")
                .expect_err("total timeout is an error")
                .to_string()
        });
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(5)).await;
        assert_eq!(
            total_receive.await.expect("total timeout signal"),
            RequestBodyTimeout::Total
        );
        assert_eq!(
            total_task.await.expect("total task"),
            "request-total-timeout"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn response_idle_and_admission_total_timers_cancel_the_body() {
        let now = Instant::now();
        let idle_body = ProxyBody {
            inner: pending_body(),
            completion: None,
            request_timeout: None,
            bytes: 0,
            idle: tokio::time::sleep(Duration::from_secs(5)),
            idle_duration: Duration::from_secs(5),
            total: tokio::time::sleep_until(now + Duration::from_secs(10)),
        };
        let idle_task = tokio::spawn(async move {
            let mut idle_body = Box::pin(idle_body);
            std::future::poll_fn(|cx| idle_body.as_mut().poll_frame(cx))
                .await
                .expect("timeout frame")
                .expect_err("response idle timeout is an error")
                .to_string()
        });
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(5)).await;
        assert_eq!(idle_task.await.expect("idle task"), "body idle timeout");

        let now = Instant::now();
        let total_body = ProxyBody {
            inner: pending_body(),
            completion: None,
            request_timeout: None,
            bytes: 0,
            idle: tokio::time::sleep(Duration::from_secs(10)),
            idle_duration: Duration::from_secs(10),
            total: tokio::time::sleep_until(now + Duration::from_secs(5)),
        };
        let total_task = tokio::spawn(async move {
            let mut total_body = Box::pin(total_body);
            std::future::poll_fn(|cx| total_body.as_mut().poll_frame(cx))
                .await
                .expect("timeout frame")
                .expect_err("response total timeout is an error")
                .to_string()
        });
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(5)).await;
        assert_eq!(total_task.await.expect("total task"), "request total timeout");
    }

    #[tokio::test(start_paused = true)]
    async fn opaque_idle_deadline_moves_only_when_bytes_are_observed() {
        let started = Instant::now();
        let activity = Arc::new(AtomicU64::new(0));
        let watched = Arc::clone(&activity);
        let idle = tokio::spawn(async move {
            wait_for_opaque_idle(started, watched.as_ref(), Duration::from_secs(5)).await;
        });
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(4)).await;
        activity.store(4_000_000_000, Ordering::Release);
        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(!idle.is_finished(), "activity extended the opaque idle deadline");
        tokio::time::advance(Duration::from_secs(4)).await;
        idle.await.expect("opaque idle watcher");
    }
}

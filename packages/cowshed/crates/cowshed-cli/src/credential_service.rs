//! Trusted-host enrolment of scoped gateway registry credentials.
//!
//! A workspace reaches a private registry through the gateway, which attaches a credential the
//! host holds; nothing inside a sandbox ever sees the secret. What was missing was the operator's
//! side of that arrangement: a way to put the credential into the platform store, to see which
//! routes exist, and to remove one.
//!
//! This is deliberately a host command. The secret is read from the operator's own environment,
//! or from a command the operator names, and is handed to the platform store in a `Zeroizing`
//! buffer — never through argv, never through a file, never through anything a workspace can
//! reach. A workspace manifest cannot ask for a credential: authority is the operator running
//! this command plus the record the gateway independently validates.

use std::path::Path;

use cowshed_core::api::{CredentialReport, CredentialRoute as CredentialRouteReport};
use cowshed_core::repository::RepoId;
use cowshed_core::runtime::project::ProjectRuntime;
use cowshed_core::storage::host_config::{CredentialRoute, HostConfig};
use cowshed_core::{CowshedError, Result};
use cowshed_gateway::{
    CanonicalTarget, CredentialPresence, CredentialProtocol, ScopedCredential,
    remove_scoped_credential, scoped_credential_scope, store_scoped_credential, validate_scope,
};
use zeroize::Zeroizing;

use crate::args::{CredentialAddArgs, CredentialCommand};
use crate::output::Output;

/// Registry credentials ride the ordinary intercepted-egress path, whose credential lookup is
/// keyed `Generic`; the protocol tags exist for the mirror lane and are not an operator choice.
const PROTOCOL: CredentialProtocol = CredentialProtocol::Generic;

pub async fn dispatch<W: std::io::Write + Send, E: std::io::Write + Send>(
    command: CredentialCommand,
    project_root: &Path,
    json: bool,
    output: &mut Output<W, E>,
) -> Result<i32> {
    let runtime = ProjectRuntime::open_existing(project_root).await?;
    let repo_id = runtime.descriptor().repo_id.clone();
    let store_root = runtime.descriptor().store_root.clone();
    match command {
        CredentialCommand::Add(args) => add(&repo_id, &store_root, args, json, output).await,
        CredentialCommand::Status => report(&repo_id, &store_root, json, output).await,
        CredentialCommand::Remove { origin } => {
            remove(&repo_id, &store_root, &origin, json, output).await
        }
    }
}

async fn add<W: std::io::Write + Send, E: std::io::Write + Send>(
    repo_id: &RepoId,
    store_root: &Path,
    args: CredentialAddArgs,
    json: bool,
    output: &mut Output<W, E>,
) -> Result<i32> {
    let origin = canonical_origin(&args.origin)?;
    let secret = resolve_secret(&args).await?;
    let credential = ScopedCredential {
        repo_id: repo_id.as_str().to_owned(),
        protocol: PROTOCOL,
        origin: origin.clone(),
        methods: args.methods.clone(),
        path_prefixes: args.path_prefixes.clone(),
        header_name: "authorization".to_owned(),
        header_value: Zeroizing::new(format!("Bearer {}", *secret)),
    };
    // Refuse before touching the store, so a rejected enrolment leaves the previous credential
    // in place rather than a half-applied one.
    validate_scope(&credential).map_err(scope_error)?;
    let route = CredentialRoute {
        repo_id: repo_id.as_str().to_owned(),
        origin: origin.clone(),
        secret_env_names: args.secret_env.iter().cloned().collect(),
    };
    let mut host = HostConfig::load_for_store(store_root).map_err(host_error)?;
    host.upsert_credential_route(route).map_err(host_error)?;
    store_scoped_credential(credential)
        .await
        .map_err(scope_error)?;
    // The route record is what makes the withheld environment name effective, so it is written
    // after the credential exists: a route naming a credential that is not there would withhold
    // a token from every child while nothing supplied one.
    host.save(store_root).map_err(host_error)?;
    if !json {
        output
            .announce(&format!(
                "enrolled {origin} for {repo_id} with scope {}",
                args.path_prefixes.join(", ")
            ))
            .map_err(write_error)?;
        if let Some(name) = args.secret_env.as_deref() {
            output
                .note(&format!(
                    "{name} is now withheld from every child of this project",
                ))
                .map_err(write_error)?;
        }
        output
            .hint(&format!(
                "cowshed grant <workspace> --egress {}",
                host_of(&origin)
            ))
            .map_err(write_error)?;
    }
    report(repo_id, store_root, json, output).await
}

async fn remove<W: std::io::Write + Send, E: std::io::Write + Send>(
    repo_id: &RepoId,
    store_root: &Path,
    origin: &str,
    json: bool,
    output: &mut Output<W, E>,
) -> Result<i32> {
    let origin = canonical_origin(origin)?;
    let presence = remove_scoped_credential(repo_id.as_str(), PROTOCOL, &origin)
        .await
        .map_err(scope_error)?;
    let mut host = HostConfig::load_for_store(store_root).map_err(host_error)?;
    let recorded = host.remove_credential_route(repo_id.as_str(), &origin);
    if recorded {
        host.save(store_root).map_err(host_error)?;
    }
    if !json {
        let state = match (presence, recorded) {
            (CredentialPresence::Absent, false) => "was not enrolled",
            _ => "removed",
        };
        output
            .announce(&format!("{origin} {state} for {repo_id}"))
            .map_err(write_error)?;
    }
    report(repo_id, store_root, json, output).await
}

async fn report<W: std::io::Write + Send, E: std::io::Write + Send>(
    repo_id: &RepoId,
    store_root: &Path,
    json: bool,
    output: &mut Output<W, E>,
) -> Result<i32> {
    let host = HostConfig::load_for_store(store_root).map_err(host_error)?;
    let mut routes = Vec::new();
    for route in host
        .credential_routes()
        .iter()
        .filter(|route| route.repo_id == repo_id.as_str())
    {
        // The record answers with its scope and nothing else: the secret it also carries is
        // dropped, and zeroed, before this returns.
        let scope = scoped_credential_scope(repo_id.as_str(), PROTOCOL, &route.origin)
            .await
            .map_err(scope_error)?;
        routes.push(CredentialRouteReport {
            repo_id: repo_id.clone(),
            origin: route.origin.clone(),
            withheld_env_names: route.secret_env_names.clone(),
            installed: scope.is_some(),
            path_prefixes: scope.unwrap_or_default(),
        });
    }
    if json {
        output
            .success(CredentialReport { routes })
            .map_err(write_error)?;
        return Ok(0);
    }
    if routes.is_empty() {
        output
            .note("no gateway credential routes are enrolled for this project")
            .map_err(write_error)?;
        return Ok(0);
    }
    for route in &routes {
        let state = if route.installed {
            "credential installed"
        } else {
            "credential MISSING from the host store"
        };
        output
            .bare_line(format!("{}  {state}", route.origin).as_bytes())
            .map_err(write_error)?;
        if !route.path_prefixes.is_empty() {
            output
                .note(&format!("scope: {}", route.path_prefixes.join(", ")))
                .map_err(write_error)?;
        }
        if !route.withheld_env_names.is_empty() {
            output
                .note(&format!(
                    "withheld from every child: {}",
                    route.withheld_env_names.join(", ")
                ))
                .map_err(write_error)?;
        }
        if !route.installed {
            output
                .hint(&format!(
                    "cowshed credential add --origin {} --path-prefix <path> --secret-env <NAME>",
                    route.origin
                ))
                .map_err(write_error)?;
        }
    }
    Ok(0)
}

/// The exact origin text the gateway will look up.
///
/// A record matches by string equality against `CanonicalTarget::origin()`, which always carries
/// an explicit port, so the operator may write `https://registry.test` and this is what turns it
/// into the one spelling that can match.
fn canonical_origin(origin: &str) -> Result<String> {
    let url = url::Url::parse(origin).map_err(|error| {
        CowshedError::usage(
            format!("`{origin}` is not a URL: {error}"),
            "pass --origin https://<host>",
        )
    })?;
    if url.scheme() != "https" {
        return Err(CowshedError::usage(
            format!("`{origin}` is not an HTTPS origin"),
            "pass --origin https://<host>",
        ));
    }
    let target = CanonicalTarget::from_url(&url).map_err(|error| {
        CowshedError::usage(
            format!("`{origin}` is not a usable origin: {error}"),
            "pass --origin https://<host>",
        )
    })?;
    if !url.username().is_empty() || url.password().is_some() {
        return Err(CowshedError::usage(
            "an origin must not carry credentials",
            "pass --origin https://<host> and supply the secret with --secret-env",
        ));
    }
    if url.path() != "/" || url.query().is_some() || url.fragment().is_some() {
        return Err(CowshedError::usage(
            "an origin is host and port only; the registry path belongs in --path-prefix",
            "pass --origin https://<host> --path-prefix <path>",
        ));
    }
    Ok(target.origin())
}

fn host_of(origin: &str) -> String {
    url::Url::parse(origin)
        .ok()
        .and_then(|url| url.host_str().map(str::to_owned))
        .unwrap_or_else(|| origin.to_owned())
}

/// Read the secret the operator named, and nothing else.
///
/// A non-empty named environment variable wins outright and the command is not run: an operator
/// who exported the token is not asking for a vault round trip. The command form exists for a
/// host that keeps the secret in a credential helper; its argv comes from the flag, is spawned
/// directly with no shell, and is never assembled from anything a workspace wrote.
async fn resolve_secret(args: &CredentialAddArgs) -> Result<Zeroizing<String>> {
    if let Some(name) = args.secret_env.as_deref() {
        match std::env::var(name) {
            Ok(value) if !value.trim().is_empty() => {
                return Ok(Zeroizing::new(value.trim().to_owned()));
            }
            _ if args.secret_command.is_none() => {
                return Err(CowshedError::usage(
                    format!("{name} is unset or empty in this environment"),
                    "export the credential, or pass --secret-command",
                ));
            }
            _ => {}
        }
    }
    if let Some(argv) = args.secret_command.as_deref() {
        return run_secret_command(argv).await;
    }
    if args.secret_stdin {
        return read_secret_stdin().await;
    }
    Err(CowshedError::usage(
        "no credential source was named",
        "pass --secret-env <NAME>, --secret-command <json-argv>, or --secret-stdin",
    ))
}

async fn run_secret_command(argv: &[String]) -> Result<Zeroizing<String>> {
    let (program, arguments) = argv.split_first().ok_or_else(|| {
        CowshedError::usage(
            "--secret-command needs a non-empty argv array",
            "pass --secret-command '[\"op\",\"read\",\"op://vault/item/field\"]'",
        )
    })?;
    let output = tokio::process::Command::new(program)
        .args(arguments)
        .stdin(std::process::Stdio::null())
        .output()
        .await
        .map_err(|error| {
            CowshedError::environment_missing(
                format!("could not run `{program}`: {error}"),
                "install the credential helper named by --secret-command",
            )
        })?;
    if !output.status.success() {
        return Err(CowshedError::environment_missing(
            format!(
                "`{program}` exited without a credential (status {})",
                output.status
            ),
            "check the credential helper invocation",
        ));
    }
    let value = Zeroizing::new(String::from_utf8(output.stdout).map_err(|_| {
        CowshedError::integrity(
            format!("`{program}` produced a credential that is not UTF-8"),
            "check the credential helper invocation",
        )
    })?);
    let trimmed = Zeroizing::new(value.trim().to_owned());
    if trimmed.is_empty() {
        return Err(CowshedError::environment_missing(
            format!("`{program}` produced an empty credential"),
            "check the credential helper invocation",
        ));
    }
    Ok(trimmed)
}

async fn read_secret_stdin() -> Result<Zeroizing<String>> {
    use tokio::io::AsyncReadExt as _;

    let mut value = Zeroizing::new(String::new());
    tokio::io::stdin()
        .read_to_string(&mut value)
        .await
        .map_err(|error| {
            CowshedError::usage(
                format!("could not read the credential from stdin: {error}"),
                "pipe the credential, or pass --secret-env <NAME>",
            )
        })?;
    let trimmed = Zeroizing::new(value.trim().to_owned());
    if trimmed.is_empty() {
        return Err(CowshedError::usage(
            "stdin carried no credential",
            "pipe the credential, or pass --secret-env <NAME>",
        ));
    }
    Ok(trimmed)
}

fn scope_error(error: cowshed_gateway::CredentialError) -> CowshedError {
    CowshedError::usage(
        format!("the credential was refused: {error}"),
        "cowshed credential status",
    )
}

fn host_error(error: cowshed_core::storage::host_config::HostConfigError) -> CowshedError {
    CowshedError::integrity(
        format!("host credential configuration is unusable: {error}"),
        "cowshed doctor --json",
    )
}

fn write_error(error: std::io::Error) -> CowshedError {
    CowshedError::internal(format!("failed to write command result: {error}"))
}

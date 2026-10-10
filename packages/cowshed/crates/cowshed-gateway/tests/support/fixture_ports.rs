use std::net::Ipv4Addr;

use tokio::net::TcpListener;

/// The macOS port block this test runs inside, from the workspace environment; `None` outside a
/// workspace. A block exported without its size, or the reverse, is a misconfigured workspace, so
/// it panics by name rather than guessing a fallback.
pub fn enclosing_cow_port_block() -> Option<(u16, u16)> {
    let base = std::env::var_os("COWSHED_PORT_BASE");
    let size = std::env::var_os("COWSHED_PORT_BLOCK_SIZE");
    let (base, size) = match (base, size) {
        (None, None) => return None,
        (Some(base), Some(size)) => (base, size),
        (Some(_), None) => {
            panic!("COWSHED_PORT_BASE is set but COWSHED_PORT_BLOCK_SIZE is missing")
        }
        (None, Some(_)) => {
            panic!("COWSHED_PORT_BLOCK_SIZE is set but COWSHED_PORT_BASE is missing")
        }
    };
    let base = strict_port_env("COWSHED_PORT_BASE", &base);
    let size = strict_port_env("COWSHED_PORT_BLOCK_SIZE", &size);
    assert!(
        cowshed_gateway_types::is_macos_port_block(base, size),
        "COWSHED_PORT_BASE={base} COWSHED_PORT_BLOCK_SIZE={size} is not a macOS workspace port block"
    );
    Some((base, size))
}

fn strict_port_env(name: &str, value: &std::ffi::OsStr) -> u16 {
    let text = value
        .to_str()
        .unwrap_or_else(|| panic!("{name} is not UTF-8"));
    assert!(
        !text.is_empty() && text.bytes().all(|byte| byte.is_ascii_digit()),
        "{name}={text:?} is not a decimal port number"
    );
    text.parse()
        .unwrap_or_else(|error| panic!("{name}={text:?}: {error}"))
}

/// A loopback listener for a fixture's upstream server. Inside a workspace it takes the first free
/// service port of the workspace's block: the gateway's own port `base` stays reserved, and the
/// sandbox reaches no other port. Outside one it takes any free port.
pub async fn bind_fixture_listener() -> TcpListener {
    let Some((base, size)) = enclosing_cow_port_block() else {
        return TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .expect("bind upstream fixture");
    };
    // Service ports are base+1 ..= base+size-1, and the validated block keeps them in the port space.
    for port in (u32::from(base) + 1)..(u32::from(base) + u32::from(size)) {
        let port = u16::try_from(port).expect("service port fits the port space");
        match TcpListener::bind((Ipv4Addr::LOCALHOST, port)).await {
            Ok(listener) => return listener,
            Err(error) if error.kind() == std::io::ErrorKind::AddrInUse => {}
            Err(error) => panic!("bind fixture listener on port {port}: {error}"),
        }
    }
    panic!("no free service port in workspace port block {base}+{size}: the block is exhausted");
}

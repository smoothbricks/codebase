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

fn service_ports(base: u16, size: u16) -> impl Iterator<Item = u32> {
    ((u32::from(base) + 1)..(u32::from(base) + u32::from(size))).step_by(2)
}

/// A loopback listener for a fixture's upstream server. Inside a workspace it takes the first free
/// odd-offset service port of the workspace's block: the gateway's own port `base` stays reserved,
/// and even offsets are session ports protected by the adjacent service claim. Outside one it takes any free port.
pub async fn bind_fixture_listener() -> TcpListener {
    let Some((base, size)) = enclosing_cow_port_block() else {
        return TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .expect("bind upstream fixture");
    };
    // Each upstream listener is itself the P+1 claim; never take an even session port P.
    for port in service_ports(base, size) {
        let port = u16::try_from(port).expect("service port fits the port space");
        match TcpListener::bind((Ipv4Addr::LOCALHOST, port)).await {
            Ok(listener) => return listener,
            Err(error) if error.kind() == std::io::ErrorKind::AddrInUse => {}
            Err(error) => panic!("bind fixture listener on port {port}: {error}"),
        }
    }
    panic!("no free service port in workspace port block {base}+{size}: the block is exhausted");
}

#[cfg(test)]
mod tests {
    use super::service_ports;

    #[test]
    fn upstream_service_ports_never_take_even_session_offsets() {
        let base = cowshed_gateway_types::MACOS_PORT_MIN;
        for size in [2_u16, 4, 32, 64] {
            let ports = service_ports(base, size).collect::<Vec<_>>();
            assert_eq!(ports.len(), usize::from(size / 2));
            assert_eq!(ports.first(), Some(&(u32::from(base) + 1)));
            assert_eq!(ports.last(), Some(&(u32::from(base) + u32::from(size) - 1)));
            assert!(ports.iter().all(|port| (port - u32::from(base)) % 2 == 1));
        }
    }
}

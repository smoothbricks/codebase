#[test]
fn early_failure() {
    panic!("closed failure evidence é");
}

#[test]
fn later_test() -> Result<(), Box<dyn std::error::Error>> {
    use std::io::Read;

    let mut gate = std::net::TcpStream::connect(std::env::var("STREAM_GATE")?)?;
    let mut release = [0];
    gate.read_exact(&mut release)?;
    Ok(())
}

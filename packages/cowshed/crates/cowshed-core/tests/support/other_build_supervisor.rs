//! A supervisor of another cowshed build standing at a workspace socket, as one still draining
//! its jobs after an install does (11_shell.md "Draining a supervisor of another build"): it
//! answers every hello with the wire value it was given, a build this one is not, and counts the
//! hellos it was asked.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

pub struct OtherBuild {
    hellos: Arc<AtomicUsize>,
    server: tokio::task::JoinHandle<()>,
}

impl OtherBuild {
    /// Serve `answer`, the whole answer frame, to every connection at `socket`.
    pub fn serve(socket: &Path, answer: &'static [u8]) -> Self {
        let listener = tokio::net::UnixListener::bind(socket).expect("stand at the socket");
        let hellos = Arc::new(AtomicUsize::new(0));
        let asked = Arc::clone(&hellos);
        let server = tokio::spawn(async move {
            loop {
                let (mut stream, _) = listener.accept().await.expect("a hello");
                asked.fetch_add(1, Ordering::SeqCst);
                let mut length = [0_u8; 4];
                stream
                    .read_exact(&mut length)
                    .await
                    .expect("request length");
                let mut request = vec![0_u8; u32::from_be_bytes(length) as usize];
                stream.read_exact(&mut request).await.expect("request");
                stream
                    .write_all(
                        &u32::try_from(answer.len())
                            .expect("answer length")
                            .to_be_bytes(),
                    )
                    .await
                    .expect("answer length");
                stream.write_all(answer).await.expect("answer");
            }
        });
        Self { hellos, server }
    }

    /// How many connections asked it who it is.
    pub fn hellos(&self) -> usize {
        self.hellos.load(Ordering::SeqCst)
    }
}

impl Drop for OtherBuild {
    fn drop(&mut self) {
        self.server.abort();
    }
}

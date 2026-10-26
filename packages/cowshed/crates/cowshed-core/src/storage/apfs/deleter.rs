//! Nudging the gateway's cleanup (05_gateway.md, "Deleting released images").
//!
//! A verb releases an image by renaming it into a `.deleting` directory ([`super::release`]):
//! that rename is the commit, and the `.deleting` directories are the cleanup's queue. The verb
//! then nudges the gateway of its host namespace ([`nudge`]), which lists the directories
//! ([`leftovers`]) at its start and at every nudge, and frees one entry at a time off every
//! verb's path. No releasing process frees bytes, holds a lock for a free, or starts anything to
//! free them: measured 2026-10-25, deletion children a land started held its repository's land
//! turn after the land exited, and nine at once saturated the store's container.

use std::io::{self, Read as _, Write as _};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::time::Duration;

use cowshed_gateway_types::RELEASED_OP;

use super::release::{DELETING_DIRECTORY, Left, Listing, Why, released_in};

/// `<store>/.deleting`: where a project's final unbinding moves the released images its tree still
/// holds, so the tree goes at once and the gateway's cleanup frees them. Entries are named
/// `<fresh id>.dmg` ([`crate::build_volume::BuildVolumeId`]).
pub fn store_deleting(store: &Path) -> PathBuf {
    store.join(DELETING_DIRECTORY)
}

/// How long a nudge waits for the gateway to answer: the acknowledgement bound every gateway
/// admission has. It decides nothing: an entry no gateway was nudged for is found by its next
/// listing.
const ANSWER_WITHIN: Duration = Duration::from_secs(2);

/// Nudge the cleanup of the gateway at `socket` after this process committed the release at
/// `entry` and made it durable: the gateway lists its store again. `entry` is named only in what
/// this says. Nothing is waited for but the answer: a gateway that is absent, silent or refusing
/// finds the entry at its next listing, which this says.
pub fn nudge(socket: &Path, entry: &Path) {
    if let Err(error) = send(socket) {
        cowshed_os::eprintln!(
            "cowshed: released image {} waits in .deleting for the gateway's next listing: the gateway at {} took no nudge: {error}",
            entry.display(),
            socket.display()
        );
    }
}

/// Say, once per process, that this store names no gateway cleanup (a fixture store no test gave
/// a gateway): a release committed with no gateway to nudge waits in `.deleting` for a gateway's
/// start, which lists it (`leftovers`).
pub fn say_no_cleanup() {
    static SAID: std::sync::Once = std::sync::Once::new();
    SAID.call_once(|| {
        cowshed_os::eprintln!(
            "cowshed: this store names no gateway cleanup; its released images wait in .deleting for a gateway's start"
        )
    });
}
fn send(socket: &Path) -> io::Result<()> {
    let mut line =
        serde_json::to_vec(&serde_json::json!({ "op": RELEASED_OP })).map_err(io::Error::other)?;
    line.push(b'\n');
    // WHY unbounded: Darwin answers a unix connect in the call, refused when nothing listens or the
    // queue is full, never parked (APFS images are released on macOS only); the exchange after it
    // is bounded by ANSWER_WITHIN.
    #[allow(clippy::disallowed_methods)]
    let stream = cowshed_os::fork_lock::Fenced::create(|| UnixStream::connect(socket))?;
    let mut connection: &UnixStream = &stream;
    connection.set_read_timeout(Some(ANSWER_WITHIN))?;
    connection.set_write_timeout(Some(ANSWER_WITHIN))?;
    connection.write_all(&line)?;
    connection.shutdown(std::net::Shutdown::Write)?;
    let mut answer = String::new();
    connection.read_to_string(&mut answer)?;
    match serde_json::from_str::<serde_json::Value>(answer.trim_end()) {
        Ok(value) if value["ok"] == true => Ok(()),
        _ => Err(io::Error::other(format!("it answered {answer:?}"))),
    }
}

/// Every released image an earlier gateway left in the store at `store`: each project's
/// `build/.deleting` and `sessions/.deleting`, the same two of every archived project
/// (`.retired-projects/<operation-id>/`), and the store's own `.deleting`, in the order listed,
/// with each stray and unreadable directory left and named.
pub fn leftovers(store: &Path) -> Listing {
    let mut listing = crate::build_volume::released_build_volumes(store);
    listing.add(super::released_sessions(store));
    let deleting = store_deleting(store);
    match released_in(&deleting, crate::build_volume::is_image_name) {
        Ok(store_level) => listing.add(store_level),
        Err(error) => listing.left.push(Left {
            path: deleting,
            why: Why::Failed(error.to_string()),
        }),
    }
    listing
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn store(label: &str) -> PathBuf {
        let root = PathBuf::from(format!(
            "/private/tmp/csdel-{label}-{}",
            &uuid::Uuid::new_v4().simple().to_string()[..8]
        ));
        fs::create_dir_all(&root).expect("store");
        root
    }

    fn released(path: &Path) {
        fs::create_dir_all(path.parent().expect("deleting directory")).expect("directory");
        fs::write(path, vec![7_u8; 64 * 1024]).expect("released image");
    }

    fn volume(store: &Path) -> PathBuf {
        store.join(format!(
            "acme/widget/build/.deleting/{}.dmg",
            crate::build_volume::BuildVolumeId::mint()
        ))
    }

    /// A nudge is one request line, half-closed, and the gateway's answer: the line names nothing
    /// the listing does not.
    #[test]
    fn a_nudge_writes_one_released_line_and_reads_the_answer() {
        use std::os::unix::net::UnixListener;
        let store = store("hand");
        let socket = store.join("gateway.sock");
        let listener = UnixListener::bind(&socket).expect("the gateway");
        let entry = volume(&store);
        let gateway = std::thread::spawn(move || {
            let (mut client, _) = listener.accept().expect("a nudge");
            let mut line = String::new();
            client
                .read_to_string(&mut line)
                .expect("the request, then end of file");
            client.write_all(b"{\"ok\":true}\n").expect("the answer");
            line
        });
        nudge(&socket, &entry);
        let line = gateway.join().expect("the gateway thread");
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&line).expect("one JSON line"),
            serde_json::json!({"op": "released"})
        );
        fs::remove_dir_all(&store).expect("cleanup");
    }

    /// The listing at a gateway's start names every release left from before it, in every
    /// project's, every archived project's and the store's own deleting directory, names a stray,
    /// and never looks in a legacy `.releasing`; an owner marker is no project. It frees nothing.
    #[test]
    fn the_leftovers_are_every_release_an_earlier_gateway_left() {
        let store = store("leftovers");
        let left = [
            volume(&store),
            store.join(format!(
                ".deleting/{}.dmg",
                crate::build_volume::BuildVolumeId::mint()
            )),
            store.join(format!(
                "acme/widget/sessions/.deleting/topic-{}.dmg",
                "1".repeat(32)
            )),
            store.join(format!(
                ".retired-projects/0123/build/.deleting/{}.dmg",
                crate::build_volume::BuildVolumeId::mint()
            )),
        ];
        for entry in &left {
            released(entry);
        }
        let stray = store.join("acme/widget/sessions/.deleting/notes.txt");
        fs::write(&stray, b"not an image").expect("stray");
        fs::write(store.join("acme/.cowshed-owner"), b"owner").expect("owner marker");
        fs::write(store.join(".retired-projects/0123.json"), b"{}").expect("archive record");
        let legacy = store.join(format!(
            "acme/widget/build/.releasing/{}.dmg",
            crate::build_volume::BuildVolumeId::mint()
        ));
        released(&legacy);

        let listing = leftovers(&store);
        let mut found = listing.entries.clone();
        found.sort();
        let mut expected = left.to_vec();
        expected.sort();
        assert_eq!(found, expected);
        assert_eq!(
            listing.left,
            [Left {
                path: stray,
                why: Why::NotImage
            }]
        );
        assert!(
            left.iter().all(|entry| entry.exists()),
            "a listing frees nothing"
        );
        fs::remove_dir_all(&store).expect("cleanup");
    }
}

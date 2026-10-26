//! Handing released images to the gateway's cleanup (05_gateway.md, "Deleting released images").
//!
//! A verb releases an image by renaming it into a `.deleting` directory ([`super::release`]) and
//! hands the entry to the gateway of its host namespace ([`hand_over`]), which frees one entry at
//! a time off every verb's path, yielding to gate work. At its start the gateway's cleanup lists
//! what an earlier gateway left ([`leftovers`]). No releasing process frees bytes, holds a lock for
//! a free, or starts anything to free them: measured 2026-10-25, deletion children a land started
//! held its repository's land turn after the land exited, and nine at once saturated the store's
//! container.

use std::io::{self, Read as _, Write as _};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::time::Duration;

use cowshed_gateway_types::{ReleaseRecord, ReleasedRequest};

use super::release::{DELETING_DIRECTORY, Left, Listing, Why, released_in};

/// `<store>/.deleting`: where a project's final unbinding moves the released images its tree still
/// holds, so the tree goes at once and the gateway's cleanup frees them. Entries are named
/// `<fresh id>.dmg` ([`crate::build_volume::BuildVolumeId`]).
pub fn store_deleting(store: &Path) -> PathBuf {
    store.join(DELETING_DIRECTORY)
}

/// How long a hand-over waits for the gateway to answer: the acknowledgement bound every
/// gateway admission has. It decides nothing: an entry not handed over waits for the gateway's
/// next start.
const ANSWER_WITHIN: Duration = Duration::from_secs(2);

/// Hand `entry`, a release this process just committed and made durable, to the cleanup of the
/// gateway at `socket`, with what the releaser knew of it. Nothing is waited for but the answer: a
/// gateway that is absent, silent or refusing leaves the entry in `.deleting` for its next start,
/// which this says.
pub fn hand_over(socket: &Path, entry: &Path, record: ReleaseRecord) {
    if let Err(error) = send(socket, entry, record) {
        cowshed_os::eprintln!(
            "cowshed: released image {} waits in .deleting for the gateway's next start: the gateway at {} took no hand-over: {error}",
            entry.display(),
            socket.display()
        );
    }
}

/// Say, once per process, that this store names no gateway cleanup (a fixture store no test gave
/// a gateway): a release committed without a hand-over waits in `.deleting` for a gateway's
/// start, which lists it (`leftovers`).
pub fn say_no_cleanup() {
    static SAID: std::sync::Once = std::sync::Once::new();
    SAID.call_once(|| {
        cowshed_os::eprintln!(
            "cowshed: this store names no gateway cleanup; its released images wait in .deleting for a gateway's start"
        )
    });
}
fn send(socket: &Path, entry: &Path, record: ReleaseRecord) -> io::Result<()> {
    let mut line =
        serde_json::to_vec(&ReleasedRequest::new(entry, record)).map_err(io::Error::other)?;
    line.push(b'\n');
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

    /// A hand-over is one request line, half-closed, and the gateway's answer: the entry and what
    /// the releaser knew of it, the unknown left out.
    #[test]
    fn a_hand_over_writes_one_released_line_and_reads_the_answer() {
        use std::os::unix::net::UnixListener;
        let store = store("hand");
        let socket = store.join("gateway.sock");
        let listener = UnixListener::bind(&socket).expect("the gateway");
        let entry = volume(&store);
        let gateway = std::thread::spawn(move || {
            let (mut client, _) = listener.accept().expect("a hand-over");
            let mut line = String::new();
            client
                .read_to_string(&mut line)
                .expect("the request, then end of file");
            client.write_all(b"{\"ok\":true}\n").expect("the answer");
            line
        });
        hand_over(
            &socket,
            &entry,
            ReleaseRecord {
                unique_bytes: None,
                extents: std::num::NonZeroU64::new(12),
            },
        );
        let line = gateway.join().expect("the gateway thread");
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&line).expect("one JSON line"),
            serde_json::json!({"op": "released", "entry": entry, "extents": 12})
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

//! macOS's tree copy that keeps hard links: `ditto`, since its `cp -a` splits them.

pub(super) const COPY_TREE: (&str, &[&str]) = ("/usr/bin/ditto", &[]);

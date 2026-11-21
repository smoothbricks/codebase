//! Linux's tree copy that keeps hard links: GNU `cp -a`, found on PATH since NixOS keeps no
//! `/bin/cp`.

pub(super) const COPY_TREE: (&str, &[&str]) = ("cp", &["-a"]);

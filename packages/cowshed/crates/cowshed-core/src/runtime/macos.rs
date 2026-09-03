use std::collections::HashMap;

/// The environment for a non-interactive acceptance check run before a workspace lands.
///
/// Nobody is waiting at a keyboard for these builds and their output is worth storing:
/// `CARGO_INCREMENTAL=0` keeps every unit non-incremental, which is the shape the sccache
/// wrapper can cache and hand to the next landing. Interactive workspace commands leave
/// Cargo's incremental policy untouched.
pub(super) fn acceptance_check_environment() -> HashMap<String, String> {
    HashMap::from([("CARGO_INCREMENTAL".to_owned(), "0".to_owned())])
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::runtime::supervisor::build_environment;

    #[test]
    fn an_acceptance_check_carries_its_own_non_incremental_policy() {
        let requested: Vec<(String, String)> = acceptance_check_environment().into_iter().collect();
        let caller = requested
            .iter()
            .map(|(name, value)| (name.to_owned(), value.to_owned()))
            .collect();
        assert_eq!(
            build_environment(&caller).collect::<BTreeMap<_, _>>(),
            BTreeMap::from([
                ("CARGO_INCREMENTAL", "0"),
                ("RUSTC_WRAPPER", "sccache"),
                ("SCCACHE_BASEDIR_CWD", "1"),
            ])
        );
    }
}

use std::fmt;

const MAX_COMPONENT_BYTES: usize = 128;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InvalidRepoId;

impl fmt::Display for InvalidRepoId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(
            "repository identity must be two lowercase identity components joined by '/'",
        )
    }
}

impl std::error::Error for InvalidRepoId {}

pub fn validate_repo_id(value: &str) -> Result<(), InvalidRepoId> {
    let (owner, repo) = value.split_once('/').ok_or(InvalidRepoId)?;
    if repo.contains('/') || !valid_component(owner) || !valid_component(repo) {
        return Err(InvalidRepoId);
    }
    Ok(())
}

fn valid_component(value: &str) -> bool {
    let bytes = value.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= MAX_COMPONENT_BYTES
        && bytes
            .first()
            .is_some_and(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
        && bytes
            .iter()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || b"._-".contains(byte))
}

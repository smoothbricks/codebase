//! Dataset and snapshot names, and the one cowshed root every argument must lie below.

use std::fmt;

/// The last component of every cowshed root dataset.
pub const ROOT_COMPONENT: &str = "cowshed";

/// ZFS bounds a full dataset or snapshot name at 255 bytes.
const NAME_LIMIT: usize = 255;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NameError {
    Empty,
    TooLong(usize),
    Component(String),
    NotASnapshot,
    NotARoot,
}

impl fmt::Display for NameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => f.write_str("the name is empty"),
            Self::TooLong(length) => write!(f, "the name is {length} bytes, over {NAME_LIMIT}"),
            Self::Component(component) => write!(f, "{component:?} is not a name component"),
            Self::NotASnapshot => f.write_str("the name is not <dataset>@<snapshot>"),
            Self::NotARoot => write!(f, "a cowshed root is named <parent>/{ROOT_COMPONENT}"),
        }
    }
}

/// One component of a name: ASCII letters, digits and `._:-`, and neither `.` nor `..`. That is
/// narrower than ZFS accepts (no space), and it excludes every character with a meaning to `zfs`
/// itself: `/`, `@`, `#`, `%`, `,`, `=` and a leading `-` can never arrive inside a component.
fn component(text: &str) -> Result<(), NameError> {
    let legal = !text.is_empty()
        && text != "."
        && text != ".."
        && !text.starts_with('-')
        && text
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"._:-".contains(&byte));
    if legal {
        Ok(())
    } else {
        Err(NameError::Component(text.to_owned()))
    }
}

/// A filesystem dataset's full name, `<pool>/<component>/…`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Dataset(String);

impl Dataset {
    pub fn parse(text: &str) -> Result<Self, NameError> {
        if text.is_empty() {
            return Err(NameError::Empty);
        }
        if text.len() > NAME_LIMIT {
            return Err(NameError::TooLong(text.len()));
        }
        text.split('/').try_for_each(component)?;
        Ok(Self(text.to_owned()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The components of this name below `ancestor`, when it lies strictly below it.
    fn below<'a>(&'a self, ancestor: &Dataset) -> Option<impl Iterator<Item = &'a str> + use<'a>> {
        let rest = self.0.strip_prefix(ancestor.as_str())?.strip_prefix('/')?;
        Some(rest.split('/'))
    }
}

impl fmt::Display for Dataset {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// A snapshot's full name, `<dataset>@<snapshot>`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Snapshot {
    dataset: Dataset,
    full: String,
}

impl Snapshot {
    pub fn parse(text: &str) -> Result<Self, NameError> {
        let (dataset, name) = text.split_once('@').ok_or(NameError::NotASnapshot)?;
        if text.len() > NAME_LIMIT {
            return Err(NameError::TooLong(text.len()));
        }
        component(name)?;
        Ok(Self {
            dataset: Dataset::parse(dataset)?,
            full: text.to_owned(),
        })
    }

    pub fn dataset(&self) -> &Dataset {
        &self.dataset
    }

    pub fn as_str(&self) -> &str {
        &self.full
    }
}

/// The cowshed root dataset, `<parent>/cowshed`. Every dataset a verb names lies strictly below
/// it: the root itself, its parents and its siblings are never an argument.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Root(Dataset);

impl Root {
    pub fn parse(text: &str) -> Result<Self, NameError> {
        let dataset = Dataset::parse(text)?;
        match text.rsplit_once('/') {
            Some((_, ROOT_COMPONENT)) => Ok(Self(dataset)),
            _ => Err(NameError::NotARoot),
        }
    }

    pub fn dataset(&self) -> &Dataset {
        &self.0
    }

    pub fn holds(&self, dataset: &Dataset) -> bool {
        dataset.below(&self.0).is_some()
    }

    /// Whether `dataset` is a build dataset, `<root>/projects/<owner>/<repo>/build/<id>`: the
    /// only kind `promote` accepts (09_substrates.md, "The clone-origin dependency").
    pub fn holds_build_volume(&self, dataset: &Dataset) -> bool {
        let Some(below) = dataset.below(&self.0) else {
            return false;
        };
        let parts: Vec<&str> = below.collect();
        matches!(parts.as_slice(), ["projects", _, _, "build", id] if !id.starts_with('.'))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_are_accepted_or_refused_by_component() {
        for legal in [
            "z",
            "z/services/dev/zoned/cowshed",
            "tank/cowshed/projects/acme/widget/ws/.trash",
            "tank/a.b_c:d-e",
        ] {
            assert!(Dataset::parse(legal).is_ok(), "{legal}");
        }
        for illegal in [
            "",
            "/tank",
            "tank/",
            "tank//a",
            "tank/.",
            "tank/../other",
            "tank/a b",
            "tank/a@b",
            "tank/a#b",
            "tank/a%b",
            "tank/-r",
            "tank/a,b",
            "tank/a=b",
            "tank/a\nb",
        ] {
            assert!(Dataset::parse(illegal).is_err(), "{illegal:?}");
        }
        assert_eq!(
            Dataset::parse(&"a".repeat(256)),
            Err(NameError::TooLong(256))
        );
    }

    #[test]
    fn a_snapshot_names_one_dataset_and_one_snapshot() {
        let snapshot = Snapshot::parse("tank/cowshed/a@cowshed:fork-b").unwrap();
        assert_eq!(snapshot.dataset().as_str(), "tank/cowshed/a");
        assert_eq!(snapshot.as_str(), "tank/cowshed/a@cowshed:fork-b");
        for illegal in ["tank/a", "tank/a@", "tank/a@b@c", "tank/a@b/c", "@b"] {
            assert!(Snapshot::parse(illegal).is_err(), "{illegal:?}");
        }
    }

    #[test]
    fn a_root_holds_only_what_lies_strictly_below_it() {
        let root = Root::parse("tank/dev/cowshed").unwrap();
        let holds = |name: &str| root.holds(&Dataset::parse(name).unwrap());
        assert!(holds("tank/dev/cowshed/store"));
        assert!(holds("tank/dev/cowshed/projects/a/b/main"));
        assert!(!holds("tank/dev/cowshed"));
        assert!(!holds("tank/dev"));
        assert!(!holds("tank/dev/cowshed-other/x"));
        assert!(!holds("tank/dev/cowshedx"));
        assert!(!holds("other/dev/cowshed/store"));
        assert_eq!(Root::parse("tank/dev"), Err(NameError::NotARoot));
        assert_eq!(Root::parse("cowshed"), Err(NameError::NotARoot));
    }

    #[test]
    fn only_a_projects_build_dataset_is_a_build_volume() {
        let root = Root::parse("tank/cowshed").unwrap();
        let build = |name: &str| root.holds_build_volume(&Dataset::parse(name).unwrap());
        assert!(build("tank/cowshed/projects/acme/widget/build/7"));
        assert!(!build("tank/cowshed/projects/acme/widget/build"));
        assert!(!build("tank/cowshed/projects/acme/widget/build/.trash"));
        assert!(!build("tank/cowshed/projects/acme/widget/build/.trash/7"));
        assert!(!build("tank/cowshed/projects/acme/widget/ws/raven"));
        assert!(!build("tank/cowshed/projects/acme/widget/main"));
        assert!(!build("tank/cowshed/store/build/7"));
        assert!(!build("tank/other/projects/acme/widget/build/7"));
    }
}

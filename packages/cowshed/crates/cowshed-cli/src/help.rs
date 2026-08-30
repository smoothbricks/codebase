//! The shape of a command's help, and the three renderings of it.
//!
//! A [`CommandSpec`] lives beside its parser in [`crate::args`], so a flag's spelling and the one
//! line that explains it are written where the parser reads them. Everything a user sees is
//! rendered from that one value: the usage line a parse error hints, the command map, and the page
//! `cowshed <command> --help` prints. The usage line is therefore not a string anybody maintains —
//! it is the option table printed — which is why a flag cannot reach the parser and stay invisible.

use std::sync::LazyLock;

use crate::args::COMMANDS;

/// One flag, as typed, and the one line that says what it does.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Opt {
    /// The flag with its value placeholder, exactly as it is typed: `--slot <n>`.
    pub spelling: &'static str,
    pub meaning: &'static str,
}

impl Opt {
    /// The flag without its value, for the compact grammar the command map prints.
    fn flag(&self) -> &'static str {
        match self.spelling.split_once(' ') {
            Some((flag, _)) => flag,
            None => self.spelling,
        }
    }
}

/// One command: its grammar, and every word of help about it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CommandSpec {
    pub name: &'static str,
    /// Sentence clap's missing-argument path and `require_*` share.
    pub missing: &'static str,
    /// Positional grammar between the verb and its options; empty when the verb takes none.
    pub args: &'static str,
    /// Positional grammar that has to follow the options, like `exec`'s child argv.
    pub trailing: &'static str,
    /// The one line the command map prints.
    pub summary: &'static str,
    /// Paragraphs answering what the command is for and what is not obvious about it.
    pub about: &'static [&'static str],
    pub options: &'static [Opt],
}

impl CommandSpec {
    /// The full grammar: `new <name> [--ref <rev>] [--from <ws>] …`.
    #[must_use]
    pub fn usage(&self) -> String {
        let mut usage = String::from(self.name);
        if !self.args.is_empty() {
            usage.push(' ');
            usage.push_str(self.args);
        }
        for option in self.options {
            usage.push_str(" [");
            usage.push_str(option.spelling);
            usage.push(']');
        }
        if !self.trailing.is_empty() {
            usage.push(' ');
            usage.push_str(self.trailing);
        }
        usage
    }

    /// The runnable command a usage error points at.
    #[must_use]
    pub fn hint(&self) -> String {
        format!("cowshed {}", self.usage())
    }

    /// The page `cowshed <command> --help` prints.
    #[must_use]
    pub fn page(&self) -> String {
        let mut page = format!("usage: cowshed {}\n\n{}\n", self.usage(), self.summary);
        for paragraph in self.about {
            page.push('\n');
            wrap(&mut page, paragraph, 0, 0);
        }
        if !self.options.is_empty() {
            page.push_str("\noptions:\n");
            render_options(&mut page, self.options);
        }
        page.push_str("\nglobal options:\n");
        render_options(&mut page, GLOBALS);
        page
    }

    /// The grammar the command map prints: positionals, then flag names while they stay scannable.
    fn map_grammar(&self) -> String {
        let mut grammar = String::from(self.name);
        if !self.args.is_empty() {
            grammar.push(' ');
            grammar.push_str(self.args);
        }
        if !self.options.is_empty() {
            grammar.push(' ');
            grammar.push_str(&self.map_flags());
        }
        if !self.trailing.is_empty() {
            grammar.push(' ');
            grammar.push_str(self.trailing);
        }
        grammar
    }

    fn map_flags(&self) -> String {
        let mut flags = String::from("[");
        for (position, option) in self.options.iter().enumerate() {
            if position > 0 {
                flags.push('|');
            }
            flags.push_str(option.flag());
        }
        flags.push(']');
        if flags.len() > MAP_FLAG_BUDGET {
            return String::from("[options]");
        }
        flags
    }
}

/// Options every command accepts, wherever they appear in its argument list.
pub const GLOBALS: &[Opt] = &[
    Opt {
        spelling: "--json",
        meaning: "one JSON envelope on stdout instead of bare values and tables",
    },
    Opt {
        spelling: "-q, --quiet",
        meaning: "drop guidance from stderr; errors and `next:` hints still print",
    },
    Opt {
        spelling: "--project <git-root>",
        meaning: "name the adopted repository instead of discovering it from the cwd",
    },
];

/// Coordinator preconditions shared by `push`, `rebase`, and `land`.
pub const EXPECTED_WORKSPACE_INCARNATION: Opt = Opt {
    spelling: "--expected-workspace-incarnation <id>",
    meaning: "refuse unless the workspace is still this incarnation; read workspaceIncarnation from `cowshed ls --json`",
};

pub const EXPECTED_SOURCE_HEAD: Opt = Opt {
    spelling: "--expected-source-head <oid>",
    meaning: "refuse unless the workspace HEAD is still this commit",
};

/// A command's flags are named in the map while their bracket group stays this narrow; past it the
/// map says `[options]`, because a group wider than the grammar column beside it stops being an
/// index and becomes the page `cowshed <command> --help` already prints.
const MAP_FLAG_BUDGET: usize = 56;

/// Where prose stops. Wide enough for a full usage line, narrow enough to stay readable unwrapped.
const WIDTH: usize = 96;

/// The command index a bare `cowshed`, `cowshed --help`, and `cowshed help` all print.
pub fn command_map() -> &'static str {
    static MAP: LazyLock<String> = LazyLock::new(render_command_map);
    MAP.as_str()
}

/// The one onboarding sentence a first-time caller sees, period included.
const ONBOARDING_SENTENCE: &str =
    "first time here? run cowshed setup, then cowshed adopt in your checkout.";

/// Two-line onboarding a first-time caller sees above the command map.
pub fn onboarding_preamble() -> &'static str {
    static PREAMBLE: LazyLock<String> = LazyLock::new(|| {
        format!(
            "warm git workspaces — a copy-on-write checkout for each agent.\n{ONBOARDING_SENTENCE}\n"
        )
    });
    PREAMBLE.as_str()
}

/// Bare `cowshed` prints the preamble, then the command map.
pub fn bare_invocation() -> &'static str {
    static PAGE: LazyLock<String> = LazyLock::new(|| {
        let mut page = String::from(onboarding_preamble());
        page.push('\n');
        page.push_str(command_map());
        page
    });
    PAGE.as_str()
}

/// The whole of `cowshed --help`: how to invoke it, every command, and the global options.
#[must_use]
pub fn overview() -> String {
    let mut page = String::from(
        "cowshed — warm git workspaces\n\nusage: cowshed --version | cowshed [--json] [-q] [--project <git-root>] <command> [arguments]\n\n",
    );
    page.push_str(command_map());
    page.push_str(
        "\nroot options:\n  -V, --version                print the npm package version\n",
    );
    page.push_str("\nglobal options:\n");
    render_options(&mut page, GLOBALS);
    page.push('\n');
    page.push_str(ONBOARDING_SENTENCE);
    page.push('\n');
    page.push_str("run `cowshed <command> --help` for one command's flags and what they mean\n");
    page
}

/// The command named exactly `name`.
#[must_use]
pub fn command_named(name: &str) -> Option<&'static CommandSpec> {
    COMMANDS.iter().copied().find(|spec| spec.name == name)
}

/// Commands within an edit distance of two of `name`, for correcting a typo like `sscache`.
///
/// Two catches a doubled, dropped, swapped, or mistyped character, which is what a near miss is.
/// Only the closest commands are returned, so a two-letter typo cannot answer with every other
/// two-letter verb on the list.
#[must_use]
pub fn nearest_commands(name: &str) -> Vec<&'static str> {
    const LIMIT: usize = 2;
    let mut best = LIMIT;
    let mut nearest = Vec::new();
    for spec in COMMANDS {
        let distance = edit_distance(name, spec.name);
        if distance > best {
            continue;
        }
        if distance < best {
            best = distance;
            nearest.clear();
        }
        nearest.push(spec.name);
    }
    nearest
}

/// Levenshtein distance over ASCII command names.
fn edit_distance(left: &str, right: &str) -> usize {
    let mut previous: Vec<usize> = (0..=right.len()).collect();
    let mut current = vec![0; right.len() + 1];
    for (row, left_byte) in left.bytes().enumerate() {
        current[0] = row + 1;
        for (column, right_byte) in right.bytes().enumerate() {
            let substitution = previous[column] + usize::from(left_byte != right_byte);
            current[column + 1] = substitution
                .min(previous[column + 1] + 1)
                .min(current[column] + 1);
        }
        std::mem::swap(&mut previous, &mut current);
    }
    previous[right.len()]
}

fn render_command_map() -> String {
    let grammars: Vec<String> = COMMANDS.iter().map(|spec| spec.map_grammar()).collect();
    // The summary column starts after the widest grammar that still leaves the widest summary
    // inside the page. A grammar wider than that takes its summary two spaces later instead of
    // pushing every other line to the right for its sake.
    let widest_summary = COMMANDS
        .iter()
        .map(|spec| spec.summary.len())
        .max()
        .unwrap_or(0);
    let column = grammars
        .iter()
        .map(String::len)
        .filter(|width| width + 2 + widest_summary <= WIDTH)
        .max()
        .unwrap_or(0)
        + 2;
    let mut map = String::from("commands:\n");
    for (spec, grammar) in COMMANDS.iter().zip(&grammars) {
        pad(&mut map, 2);
        map.push_str(grammar);
        pad(&mut map, column.saturating_sub(grammar.len()).max(2));
        map.push_str(spec.summary);
        map.push('\n');
    }
    map
}

/// One `  --flag <value>   meaning` line per option, meanings aligned into one column.
fn render_options(out: &mut String, options: &[Opt]) {
    /// Past this, one long flag would push every meaning beside it off the page; its own meaning
    /// starts on the next line instead.
    const MEANING_COLUMN: usize = 26;
    let column = options
        .iter()
        .map(|option| option.spelling.len() + 4)
        .filter(|width| *width <= MEANING_COLUMN)
        .max()
        .unwrap_or(MEANING_COLUMN);
    for option in options {
        pad(out, 2);
        out.push_str(option.spelling);
        let written = 2 + option.spelling.len();
        if written + 2 > column {
            out.push('\n');
            wrap(out, option.meaning, column, 0);
        } else {
            pad(out, column - written);
            wrap(out, option.meaning, column, column);
        }
    }
}

/// Wrap `text` at [`WIDTH`], indenting every line by `indent`. `open` is the column the cursor
/// already sits at, or 0 when the line still has to be indented.
fn wrap(out: &mut String, text: &str, indent: usize, open: usize) {
    let mut column = open;
    for word in text.split_whitespace() {
        if column == 0 {
            pad(out, indent);
            column = indent;
        } else if column > indent && column + 1 + word.len() > WIDTH {
            out.push('\n');
            pad(out, indent);
            column = indent;
        } else if column > indent {
            out.push(' ');
            column += 1;
        }
        out.push_str(word);
        column += word.len();
    }
    out.push('\n');
}

fn pad(out: &mut String, spaces: usize) {
    for _ in 0..spaces {
        out.push(' ');
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn usage_is_the_option_table_printed() {
        let new = command_named("new").unwrap();
        assert_eq!(
            new.usage(),
            "new <name> [--ref <rev>] [--from <ws>] [--browse] [--slot <n>] [--register] [--git-worktree]"
        );
        assert_eq!(new.hint(), format!("cowshed {}", new.usage()));

        // The child argv follows the options, wherever the flags end.
        let exec = command_named("exec").unwrap();
        assert!(exec.usage().starts_with("exec <ws> [--stdin]"));
        assert!(exec.usage().ends_with("-- <cmd...>"));
    }

    #[test]
    fn every_command_page_documents_every_flag_of_its_usage_line() {
        for spec in COMMANDS {
            let page = spec.page();
            assert!(
                page.starts_with(&format!("usage: cowshed {}", spec.usage())),
                "{} must lead with its usage line",
                spec.name
            );
            for option in spec.options {
                assert!(
                    page.contains(option.spelling),
                    "{} omits {}",
                    spec.name,
                    option.spelling
                );
                let first = option.meaning.split_whitespace().next().unwrap();
                assert!(
                    page.contains(first),
                    "{} documents {} without a meaning",
                    spec.name,
                    option.spelling
                );
            }
            for global in GLOBALS {
                assert!(
                    page.contains(global.spelling),
                    "{} omits globals",
                    spec.name
                );
            }
        }
    }

    #[test]
    fn the_map_names_every_command_and_the_flags_that_fit() {
        let map = command_map();
        for spec in COMMANDS {
            assert!(
                map.lines()
                    .any(|line| line.trim_start().starts_with(spec.name)
                        && line.contains(spec.summary)),
                "{} is missing from the map",
                spec.name
            );
        }
        // `new`'s flags are the ones a workspace is created with; the map names them rather than
        // leaving `cowshed new --help` as the only way to learn they exist.
        assert!(
            map.contains("new <name> [--ref|--from|--browse|--slot|--register|--git-worktree]")
        );
        // `exec`'s eleven flags do not fit a scannable line, so the map defers to the page.
        assert!(map.contains("exec <ws> [options] -- <cmd...>"));
        assert!(map.lines().all(|line| line.len() <= WIDTH));
    }

    #[test]
    fn typos_suggest_the_command_that_was_meant() {
        assert_eq!(nearest_commands("sscache"), ["sccache"]);
        assert_eq!(nearest_commands("statuss"), Vec::<&str>::new());
        assert_eq!(nearest_commands("remove"), Vec::<&str>::new());
        assert_eq!(nearest_commands("lands"), ["land"]);
        assert_eq!(nearest_commands("adopr"), ["adopt"]);
        // Nothing within two edits of a word that is not a near miss of any verb.
        assert_eq!(nearest_commands("zzzzzz"), Vec::<&str>::new());
    }
    #[test]
    fn prose_wraps_and_the_overview_carries_the_map_and_the_globals() {
        let overview = overview();
        assert!(overview.starts_with("cowshed — warm git workspaces\n"));
        assert!(overview.contains(command_map()));
        assert!(overview.contains("--project <git-root>"));
        assert!(overview.contains("cowshed <command> --help"));
        assert!(overview.contains(ONBOARDING_SENTENCE));
        assert!(onboarding_preamble().contains(ONBOARDING_SENTENCE));

        let page = command_named("rm").unwrap().page();
        assert!(page.lines().all(|line| line.len() <= WIDTH), "{page}");
        assert!(page.contains("--abandon"));
        assert!(page.contains("remove a workspace"));
    }

    #[test]
    fn bare_invocation_prints_onboarding_above_the_command_map() {
        let page = bare_invocation();
        assert!(page.starts_with(onboarding_preamble()));
        let rest = page.strip_prefix(onboarding_preamble()).unwrap();
        assert!(rest.starts_with('\n'));
        assert!(rest[1..].starts_with(command_map()));
        assert!(page.find("warm git workspaces").unwrap() < page.find("commands:").unwrap());
    }
}

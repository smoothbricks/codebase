//! Rendering a script template into shell text whose values are never shell text.
//!
//! A [`ScriptCommand`] is literal text `parts` interleaved with `values`. Each value is bound to
//! a shell variable of its own and the slot it occupied becomes a reference to that variable,
//! quoted for the lexical context the slot stands in:
//!
//! | slot context                              | reference                    |
//! |-------------------------------------------|------------------------------|
//! | unquoted, the whole word, `Words`         | `"${__cowshed_N[@]}"` (array) |
//! | unquoted, anything else                   | `"${__cowshed_N}"`            |
//! | inside `"…"`                              | `${__cowshed_N}`              |
//! | inside `'…'`                              | `'"${__cowshed_N}"'`          |
//!
//! Command substitution (`$(…)`, `` `…` ``) and process substitution (`<(…)`, `>(…)`) start a
//! fresh unquoted context. Because a value only ever reaches the shell through a variable's
//! expansion inside double quotes, no value byte is ever lexed, parsed or expanded as shell
//! syntax: substitution is injection-free by construction, and a misjudged context can only
//! misquote a reference, never execute a value. A `Words` value that is not a whole bare word is
//! bound as its elements joined by single spaces.
//!
//! Slots are refused where no quoting makes a variable reference mean the value: inside a
//! comment, inside `$'…'`, and directly after an odd run of backslashes, which would escape the
//! reference's own opening quote.

use crate::api::dto::{ScriptCommand, ScriptValue};

/// A shell variable a rendered script expects to be set before it runs.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Binding {
    Scalar { name: String, value: String },
    Array { name: String, values: Vec<String> },
}

impl Binding {
    pub fn name(&self) -> &str {
        match self {
            Self::Scalar { name, .. } | Self::Array { name, .. } => name,
        }
    }
}

/// A script ready to parse: its text and the variables its references name.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RenderedScript {
    pub text: String,
    pub bindings: Vec<Binding>,
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum RenderError {
    #[error("script value {slot} stands inside a comment, where no value can be substituted")]
    InComment { slot: usize },
    #[error("script value {slot} stands inside $'…', where no value can be substituted")]
    InAnsiQuote { slot: usize },
    #[error(
        "script value {slot} follows an odd run of backslashes, which would escape its quoting"
    )]
    EscapedSlot { slot: usize },
}

/// The variable a slot's value is bound to.
pub fn binding_name(slot: usize) -> String {
    format!("__cowshed_{slot}")
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Opener {
    /// The script itself.
    Top,
    /// `$(`, `<(` or `>(`: ends at the matching `)`.
    Paren,
    /// A backquote: ends at the next unescaped backquote.
    Backquote,
    /// `${`: ends at the matching `}`.
    Brace,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Context {
    Unquoted { opener: Opener, depth: usize },
    Double,
    Single,
    AnsiC,
    Comment,
}

/// Where a slot stands.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SlotContext {
    Unquoted { bare_start: bool },
    Double,
    Single,
    AnsiC,
    Comment,
}

/// A small lexer that follows exactly as much of bash's quoting to know the context of the next
/// character. It never needs to be right about anything else.
struct Lexer {
    stack: Vec<Context>,
    /// The last character of text seen, for word boundaries and backslash runs.
    previous: Option<char>,
    /// Backslashes immediately preceding the current position that are still unconsumed.
    pending_backslashes: usize,
}

fn is_word_break(character: char) -> bool {
    character.is_whitespace() || matches!(character, ';' | '&' | '|' | '(' | ')' | '<' | '>')
}

impl Lexer {
    fn new() -> Self {
        Self {
            stack: vec![Context::Unquoted {
                opener: Opener::Top,
                depth: 0,
            }],
            previous: None,
            pending_backslashes: 0,
        }
    }

    fn top(&self) -> Context {
        *self.stack.last().expect("the lexer always has a context")
    }

    fn top_mut(&mut self) -> &mut Context {
        self.stack
            .last_mut()
            .expect("the lexer always has a context")
    }

    fn at_word_start(&self) -> bool {
        self.previous.is_none_or(is_word_break)
    }

    /// Feed one literal part through the lexer.
    fn feed(&mut self, text: &str) {
        let mut characters = text.chars().peekable();
        while let Some(character) = characters.next() {
            let escaped = self.pending_backslashes % 2 == 1;
            if character == '\\' && !matches!(self.top(), Context::Single | Context::Comment) {
                self.pending_backslashes += 1;
                self.previous = Some(character);
                continue;
            }
            self.pending_backslashes = 0;
            if escaped && !matches!(self.top(), Context::Single | Context::Comment) {
                self.previous = Some(character);
                continue;
            }
            let next = characters.peek().copied();
            match self.top() {
                Context::Comment => {
                    if character == '\n' {
                        self.stack.pop();
                    }
                }
                Context::Single => {
                    if character == '\'' {
                        self.stack.pop();
                    }
                }
                Context::AnsiC => {
                    if character == '\'' {
                        self.stack.pop();
                    }
                }
                Context::Double => match (character, next) {
                    ('"', _) => {
                        self.stack.pop();
                    }
                    ('$', Some('(')) => {
                        characters.next();
                        self.stack.push(Context::Unquoted {
                            opener: Opener::Paren,
                            depth: 0,
                        });
                    }
                    ('$', Some('{')) => {
                        characters.next();
                        self.stack.push(Context::Unquoted {
                            opener: Opener::Brace,
                            depth: 0,
                        });
                    }
                    ('`', _) => self.stack.push(Context::Unquoted {
                        opener: Opener::Backquote,
                        depth: 0,
                    }),
                    _ => {}
                },
                Context::Unquoted { opener, depth } => match (character, next) {
                    ('#', _) if self.at_word_start() => self.stack.push(Context::Comment),
                    ('\'', _) => self.stack.push(Context::Single),
                    ('"', _) => self.stack.push(Context::Double),
                    ('$', Some('\'')) => {
                        characters.next();
                        self.stack.push(Context::AnsiC);
                    }
                    ('$', Some('"')) => {
                        characters.next();
                        self.stack.push(Context::Double);
                    }
                    ('$' | '<' | '>', Some('(')) => {
                        characters.next();
                        self.stack.push(Context::Unquoted {
                            opener: Opener::Paren,
                            depth: 0,
                        });
                    }
                    ('$', Some('{')) => {
                        characters.next();
                        self.stack.push(Context::Unquoted {
                            opener: Opener::Brace,
                            depth: 0,
                        });
                    }
                    ('`', _) if opener == Opener::Backquote => {
                        self.stack.pop();
                    }
                    ('`', _) => self.stack.push(Context::Unquoted {
                        opener: Opener::Backquote,
                        depth: 0,
                    }),
                    ('(', _) if opener != Opener::Brace => {
                        *self.top_mut() = Context::Unquoted {
                            opener,
                            depth: depth + 1,
                        };
                    }
                    (')', _) if opener == Opener::Paren && depth == 0 => {
                        self.stack.pop();
                    }
                    (')', _) if depth > 0 && opener != Opener::Brace => {
                        *self.top_mut() = Context::Unquoted {
                            opener,
                            depth: depth - 1,
                        };
                    }
                    ('{', _) if opener == Opener::Brace => {
                        *self.top_mut() = Context::Unquoted {
                            opener,
                            depth: depth + 1,
                        };
                    }
                    ('}', _) if opener == Opener::Brace && depth == 0 => {
                        self.stack.pop();
                    }
                    ('}', _) if opener == Opener::Brace => {
                        *self.top_mut() = Context::Unquoted {
                            opener,
                            depth: depth - 1,
                        };
                    }
                    _ => {}
                },
            }
            // The outermost unquoted context can never be popped by stray closers.
            if self.stack.is_empty() {
                self.stack.push(Context::Unquoted {
                    opener: Opener::Top,
                    depth: 0,
                });
            }
            self.previous = Some(character);
        }
    }

    fn slot_context(&self) -> SlotContext {
        match self.top() {
            Context::Unquoted { .. } => SlotContext::Unquoted {
                bare_start: self.at_word_start(),
            },
            Context::Double => SlotContext::Double,
            Context::Single => SlotContext::Single,
            Context::AnsiC => SlotContext::AnsiC,
            Context::Comment => SlotContext::Comment,
        }
    }

    /// Record that a slot's reference now stands at the current position: it is part of a word.
    fn after_slot(&mut self) {
        self.previous = Some('x');
    }
}

/// Whether the text after a slot begins a new word, making an unquoted slot the whole word.
fn ends_word(next: &str) -> bool {
    next.chars().next().is_none_or(is_word_break)
}

/// Render `script` into text and the variables it names.
pub fn render(script: &ScriptCommand) -> Result<RenderedScript, RenderError> {
    let parts = script.parts();
    let values = script.values();
    let mut lexer = Lexer::new();
    let mut text = String::with_capacity(parts.iter().map(String::len).sum::<usize>() + 32);
    let mut bindings = Vec::with_capacity(values.len());
    for (slot, value) in values.iter().enumerate() {
        let part = &parts[slot];
        lexer.feed(part);
        text.push_str(part);
        if lexer.pending_backslashes % 2 == 1 {
            return Err(RenderError::EscapedSlot { slot });
        }
        let name = binding_name(slot);
        let context = lexer.slot_context();
        let bare = matches!(context, SlotContext::Unquoted { bare_start: true })
            && ends_word(&parts[slot + 1]);
        let (reference, binding) = match (context, value) {
            (SlotContext::Comment, _) => return Err(RenderError::InComment { slot }),
            (SlotContext::AnsiC, _) => return Err(RenderError::InAnsiQuote { slot }),
            (SlotContext::Unquoted { .. }, ScriptValue::Words(words)) if bare => (
                format!("\"${{{name}[@]}}\""),
                Binding::Array {
                    name,
                    values: words.clone(),
                },
            ),
            (SlotContext::Unquoted { .. }, value) => {
                (format!("\"${{{name}}}\""), scalar(name, value))
            }
            (SlotContext::Double, value) => (format!("${{{name}}}"), scalar(name, value)),
            (SlotContext::Single, value) => (format!("'\"${{{name}}}\"'"), scalar(name, value)),
        };
        text.push_str(&reference);
        bindings.push(binding);
        lexer.after_slot();
    }
    text.push_str(parts.last().expect("a script always has a last part"));
    Ok(RenderedScript { text, bindings })
}

fn scalar(name: String, value: &ScriptValue) -> Binding {
    Binding::Scalar {
        name,
        value: match value {
            ScriptValue::Word(word) => word.clone(),
            ScriptValue::Words(words) => words.join(" "),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn word(value: &str) -> ScriptValue {
        ScriptValue::Word(value.into())
    }

    fn words(values: &[&str]) -> ScriptValue {
        ScriptValue::Words(values.iter().map(|value| (*value).into()).collect())
    }

    fn rendered(parts: &[&str], values: Vec<ScriptValue>) -> Result<RenderedScript, RenderError> {
        render(
            &ScriptCommand::new(parts.iter().map(|part| (*part).into()).collect(), values)
                .expect("a well-shaped script"),
        )
    }

    fn text(parts: &[&str], values: Vec<ScriptValue>) -> String {
        rendered(parts, values).expect("renders").text
    }

    #[test]
    fn each_context_gets_the_reference_that_means_the_value() {
        assert_eq!(
            text(&["echo ", ""], vec![word("x")]),
            r#"echo "${__cowshed_0}""#
        );
        assert_eq!(
            text(&["echo \"hi ", "\""], vec![word("x")]),
            r#"echo "hi ${__cowshed_0}""#
        );
        assert_eq!(
            text(&["echo 'hi ", "'"], vec![word("x")]),
            r#"echo 'hi '"${__cowshed_0}"''"#
        );
        assert_eq!(
            text(&["echo \"$(cat ", ")\""], vec![word("x")]),
            r#"echo "$(cat "${__cowshed_0}")""#,
            "command substitution inside double quotes starts unquoted"
        );
        assert_eq!(
            text(&["diff <(sort ", ") b"], vec![word("x")]),
            r#"diff <(sort "${__cowshed_0}") b"#
        );
        assert_eq!(
            text(&["echo `cat ", "`"], vec![word("x")]),
            r#"echo `cat "${__cowshed_0}"`"#
        );
    }

    #[test]
    fn words_spread_only_as_a_whole_bare_word() {
        let spread = rendered(&["printf '%s\\n' ", ""], vec![words(&["a b", "c"])]).unwrap();
        assert_eq!(spread.text, r#"printf '%s\n' "${__cowshed_0[@]}""#);
        assert_eq!(
            spread.bindings,
            vec![Binding::Array {
                name: "__cowshed_0".into(),
                values: vec!["a b".into(), "c".into()]
            }]
        );
        for (parts, context) in [
            (["echo pre", ""], "glued before"),
            (["echo ", "post"], "glued after"),
            (["echo \"", "\""], "double-quoted"),
        ] {
            let joined = rendered(&parts, vec![words(&["a", "b"])]).unwrap();
            assert_eq!(
                joined.bindings,
                vec![Binding::Scalar {
                    name: "__cowshed_0".into(),
                    value: "a b".into()
                }],
                "{context}"
            );
        }
        assert!(
            rendered(&["for x in ", "; do :; done"], vec![words(&["a"])])
                .unwrap()
                .text
                .contains("[@]"),
            "a word list is a bare position"
        );
        assert!(
            rendered(&["a=(", ")"], vec![words(&["a"])])
                .unwrap()
                .text
                .contains("[@]"),
            "an array literal is a bare position"
        );
    }

    #[test]
    fn a_value_is_never_shell_text() {
        let hostile = r#""; rm -rf /; echo "'$(touch pwned)'`id`"#;
        for parts in [["echo ", ""], ["echo \"hi ", "\""], ["echo 'hi ", "'"]] {
            let script = rendered(&parts, vec![word(hostile)]).unwrap();
            assert!(!script.text.contains("rm -rf"), "{}", script.text);
        }
    }

    #[test]
    fn slots_no_reference_can_serve_are_refused() {
        assert_eq!(
            rendered(&["echo hi # ", "\necho x"], vec![word("v")]),
            Err(RenderError::InComment { slot: 0 })
        );
        assert_eq!(
            rendered(&["echo $'", "'"], vec![word("v")]),
            Err(RenderError::InAnsiQuote { slot: 0 })
        );
        assert_eq!(
            rendered(&["echo \\", ""], vec![word("v")]),
            Err(RenderError::EscapedSlot { slot: 0 })
        );
        assert!(
            rendered(&["echo \\\\", ""], vec![word("v")]).is_ok(),
            "an even run of backslashes is a literal backslash"
        );
        assert!(
            rendered(&["echo '\\", "'"], vec![word("v")]).is_ok(),
            "a backslash inside single quotes escapes nothing"
        );
        assert!(
            rendered(&["echo a#b ", ""], vec![word("v")]).is_ok(),
            "a mid-word # is not a comment"
        );
    }
}

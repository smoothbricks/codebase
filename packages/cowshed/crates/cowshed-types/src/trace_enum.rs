//! A fixed set of names as a trace enum: the trait and the macro that declares one.
//!
//! The Arrow column encoding over these names stays with the trace columns that own it.

/// A fixed set of names, encoded as lmao's `S.enum`: `u16` keys into a dictionary of every name.
/// Declare one with `trace_enum!`.
pub trait TraceEnum: Copy {
    /// Every name, indexed by its key.
    const NAMES: &'static [&'static str];

    fn key(self) -> u16;

    fn name(self) -> &'static str {
        Self::NAMES[usize::from(self.key())]
    }
}
/// Declares a fixed set of names once: the enum, its dictionary and its keys are generated from
/// this one list, in this order.
///
/// ```text
/// trace_enum! {
///     /// What a process waits on.
///     pub enum BlockedOn { None = "none", Lock = "lock" }
/// }
/// ```
#[macro_export]
macro_rules! trace_enum {
    (
        $(#[$meta:meta])*
        $vis:vis enum $name:ident {
            $($(#[doc = $doc:literal])* $variant:ident = $text:literal,)+
        }
    ) => {
        $(#[$meta])*
        #[derive(Clone, Copy, Debug, Eq, PartialEq)]
        $vis enum $name {
            $($(#[doc = $doc])* $variant,)+
        }

        impl $name {
            /// Every value, in declaration order: the order of its dictionary's names.
            pub const ALL: &'static [Self] = &[$(Self::$variant,)+];
        }

        impl $crate::trace_enum::TraceEnum for $name {
            const NAMES: &'static [&'static str] = &[$($text,)+];

            fn key(self) -> u16 {
                let position = Self::ALL
                    .iter()
                    .position(|value| *value == self)
                    .expect("every value is listed in ALL");
                u16::try_from(position).expect("a declared enum has fewer than 2^16 names")
            }
        }

        const _: () = assert!(
            <$name as $crate::trace_enum::TraceEnum>::NAMES.len() <= 1 << 16,
            "an S.enum's keys are u16"
        );
    };
}

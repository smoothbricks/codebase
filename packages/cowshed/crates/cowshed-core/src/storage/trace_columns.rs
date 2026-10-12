//! The custom columns of a lmao trace segment, declared once (13_telemetry.md, "Process-tree
//! spans").
//!
//! [`super::trace_segment`] writes lmao's system-column prefix; a span kind's own columns follow
//! it. Each kind declares them once with `trace_columns!`: the row type, the Arrow fields and the
//! encoder are all generated from that one list, so no writer can hold a second copy of a schema.
//!
//! A column's kind is encoded as `lmao-arrow` encodes the same `S.*` type: [`Category`] and
//! [`Text`] as `Utf8` dictionaries keyed by `u32`, an [`Enum`] as a dictionary of every declared
//! name keyed by `u16`. The integers carry their declared width ([`U16`], [`U32`], [`U64`],
//! [`I32`], [`I64`]), and [`Utf8`] is the plain string column. Its nullability is part of the
//! declaration: an [`Optional`] column is nullable and a row leaves a cell null by holding `None`;
//! a [`Required`] column is non-null and no row can leave it out.
//!
//! A fixed set of names is declared once with `trace_enum!`: the Rust enum, its dictionary and its
//! keys are one list in one order.

use std::marker::PhantomData;
use std::sync::Arc;

use arrow_array::builder::{
    Int32Builder, Int64Builder, StringBuilder, StringDictionaryBuilder, UInt16Builder,
    UInt32Builder, UInt64Builder,
};
use arrow_array::types::{UInt16Type, UInt32Type};
use arrow_array::{ArrayRef, DictionaryArray, StringArray};
use arrow_schema::{ArrowError, DataType};

use super::trace_segment::dict_type;

/// How one kind of cell is encoded, whether or not a column may leave it null.
pub trait TraceValue {
    /// What a present cell holds.
    type Value;
    type Builder: Default;

    fn data_type() -> DataType;

    /// Append `value`, or a null for `None`. Refused only when a dictionary outgrows its keys.
    fn append(builder: &mut Self::Builder, value: Option<&Self::Value>) -> Result<(), ArrowError>;

    fn finish(builder: Self::Builder) -> Result<ArrayRef, ArrowError>;
}

/// One declared column: a [`TraceValue`] and whether a row may leave it null.
pub trait TraceColumn {
    /// What a row holds for this column.
    type Cell;
    type Builder: Default;

    const NULLABLE: bool;

    fn data_type() -> DataType;

    fn append(builder: &mut Self::Builder, cell: &Self::Cell) -> Result<(), ArrowError>;

    fn finish(builder: Self::Builder) -> Result<ArrayRef, ArrowError>;
}

/// A nullable column of `V`: a row holds `None` for a null cell.
pub struct Optional<V>(PhantomData<V>);

impl<V: TraceValue> TraceColumn for Optional<V> {
    type Cell = Option<V::Value>;
    type Builder = V::Builder;

    const NULLABLE: bool = true;

    fn data_type() -> DataType {
        V::data_type()
    }

    fn append(builder: &mut Self::Builder, cell: &Self::Cell) -> Result<(), ArrowError> {
        V::append(builder, cell.as_ref())
    }

    fn finish(builder: Self::Builder) -> Result<ArrayRef, ArrowError> {
        V::finish(builder)
    }
}

/// A non-null column of `V`: a row always holds a value.
pub struct Required<V>(PhantomData<V>);

impl<V: TraceValue> TraceColumn for Required<V> {
    type Cell = V::Value;
    type Builder = V::Builder;

    const NULLABLE: bool = false;

    fn data_type() -> DataType {
        V::data_type()
    }

    fn append(builder: &mut Self::Builder, cell: &Self::Cell) -> Result<(), ArrowError> {
        V::append(builder, Some(cell))
    }

    fn finish(builder: Self::Builder) -> Result<ArrayRef, ArrowError> {
        V::finish(builder)
    }
}

macro_rules! primitive_value {
    ($(#[$doc:meta])* $kind:ident, $value:ty, $builder:ty, $data_type:expr) => {
        $(#[$doc])*
        pub enum $kind {}

        impl TraceValue for $kind {
            type Value = $value;
            type Builder = $builder;

            fn data_type() -> DataType {
                $data_type
            }

            fn append(
                builder: &mut Self::Builder,
                value: Option<&Self::Value>,
            ) -> Result<(), ArrowError> {
                match value {
                    Some(value) => builder.append_value(*value),
                    None => builder.append_null(),
                }
                Ok(())
            }

            fn finish(mut builder: Self::Builder) -> Result<ArrayRef, ArrowError> {
                Ok(Arc::new(builder.finish()))
            }
        }
    };
}

primitive_value!(
    /// `u16`.
    U16,
    u16,
    UInt16Builder,
    DataType::UInt16
);
primitive_value!(
    /// `u32`.
    U32,
    u32,
    UInt32Builder,
    DataType::UInt32
);
primitive_value!(
    /// `u64`.
    U64,
    u64,
    UInt64Builder,
    DataType::UInt64
);
primitive_value!(
    /// `i32`.
    I32,
    i32,
    Int32Builder,
    DataType::Int32
);
primitive_value!(
    /// `i64`: a signed quantity that is never clamped.
    I64,
    i64,
    Int64Builder,
    DataType::Int64
);

/// Plain `Utf8`: one string per row, with no dictionary.
pub enum Utf8 {}

impl TraceValue for Utf8 {
    type Value = String;
    type Builder = StringBuilder;

    fn data_type() -> DataType {
        DataType::Utf8
    }

    fn append(builder: &mut StringBuilder, value: Option<&String>) -> Result<(), ArrowError> {
        match value {
            Some(value) => builder.append_value(value),
            None => builder.append_null(),
        }
        Ok(())
    }

    fn finish(mut builder: StringBuilder) -> Result<ArrayRef, ArrowError> {
        Ok(Arc::new(builder.finish()))
    }
}

macro_rules! dictionary_value {
    ($(#[$doc:meta])* $kind:ident) => {
        $(#[$doc])*
        pub enum $kind {}

        impl TraceValue for $kind {
            type Value = String;
            type Builder = StringDictionaryBuilder<UInt32Type>;

            fn data_type() -> DataType {
                dict_type(DataType::UInt32)
            }

            fn append(
                builder: &mut Self::Builder,
                value: Option<&String>,
            ) -> Result<(), ArrowError> {
                match value {
                    Some(value) => {
                        builder.append(value)?;
                    }
                    None => builder.append_null(),
                }
                Ok(())
            }

            fn finish(mut builder: Self::Builder) -> Result<ArrayRef, ArrowError> {
                Ok(Arc::new(builder.finish()))
            }
        }
    };
}

dictionary_value!(
    /// `S.category`: a repeated value, dictionary-encoded.
    Category
);
dictionary_value!(
    /// `S.text`: free text. lmao encodes it as it encodes a category.
    Text
);

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

/// `S.enum` over `E`'s names.
pub struct Enum<E>(PhantomData<E>);

impl<E: TraceEnum> TraceValue for Enum<E> {
    type Value = E;
    type Builder = UInt16Builder;

    fn data_type() -> DataType {
        dict_type(DataType::UInt16)
    }

    fn append(builder: &mut UInt16Builder, value: Option<&E>) -> Result<(), ArrowError> {
        match value {
            Some(value) => builder.append_value(value.key()),
            None => builder.append_null(),
        }
        Ok(())
    }

    fn finish(mut builder: UInt16Builder) -> Result<ArrayRef, ArrowError> {
        Ok(Arc::new(DictionaryArray::<UInt16Type>::try_new(
            builder.finish(),
            Arc::new(StringArray::from_iter_values(E::NAMES.iter().copied())),
        )?))
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

        impl $crate::storage::trace_columns::TraceEnum for $name {
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
            <$name as $crate::storage::trace_columns::TraceEnum>::NAMES.len() <= 1 << 16,
            "an S.enum's keys are u16"
        );
    };
}

/// Declares a span kind's custom columns once: the row struct, the Arrow fields and the encoder
/// are generated from this one list, in this order. A row's cell for a column is that column's
/// `TraceColumn::Cell`: `Option<V>` for an `Optional<V>`, `V` for a `Required<V>`.
///
/// ```text
/// trace_columns! {
///     #[derive(Default)]
///     pub struct ProbeColumns, fields: probe_fields, arrays: probe_column_arrays;
///     /// The probed process.
///     pid: Optional<U32>,
/// }
/// ```
macro_rules! trace_columns {
    (
        $(#[$meta:meta])*
        $vis:vis struct $row:ident, fields: $fields:ident, arrays: $arrays:ident;
        $($(#[doc = $doc:literal])* $name:ident: $column:ty,)+
    ) => {
        $(#[$meta])*
        #[derive(Clone, Debug, Eq, PartialEq)]
        $vis struct $row {
            $($(#[doc = $doc])* pub $name:
                <$column as $crate::storage::trace_columns::TraceColumn>::Cell,)+
        }

        /// The declared columns' Arrow fields, in order: what every segment of this kind carries
        /// after lmao's system columns, and nothing more.
        $vis fn $fields() -> ::std::vec::Vec<::arrow_schema::Field> {
            ::std::vec![$(::arrow_schema::Field::new(
                ::std::stringify!($name),
                <$column as $crate::storage::trace_columns::TraceColumn>::data_type(),
                <$column as $crate::storage::trace_columns::TraceColumn>::NULLABLE,
            ),)+]
        }

        /// The declared columns of `rows`, each field with its array, as
        /// `trace_segment::trace_batch` takes them.
        $vis fn $arrays<'a>(
            rows: impl ::std::iter::IntoIterator<Item = &'a $row>,
        ) -> ::std::result::Result<
            ::std::vec::Vec<(::arrow_schema::Field, ::arrow_array::ArrayRef)>,
            ::arrow_schema::ArrowError,
        > {
            #[derive(Default)]
            struct Builders {
                $($name: <$column as $crate::storage::trace_columns::TraceColumn>::Builder,)+
            }

            let mut builders = Builders::default();
            for row in rows {
                $(<$column as $crate::storage::trace_columns::TraceColumn>::append(
                    &mut builders.$name,
                    &row.$name,
                )?;)+
            }
            let arrays = ::std::vec![$(
                <$column as $crate::storage::trace_columns::TraceColumn>::finish(builders.$name)?,
            )+];
            Ok($fields().into_iter().zip(arrays).collect())
        }
    };
}

pub(crate) use trace_columns;
pub(crate) use trace_enum;

#[cfg(test)]
mod tests {
    use arrow_array::cast::AsArray as _;
    use arrow_array::types::{Int32Type, Int64Type, UInt16Type, UInt32Type, UInt64Type};
    use arrow_array::{Array as _, RecordBatch};
    use arrow_schema::{Field, Schema};

    use super::*;

    trace_enum! {
        /// A test enum whose declaration order is its key order.
        enum Phase {
            Warm = "warm",
            Cold = "cold",
            Hot = "hot",
        }
    }

    trace_columns! {
        #[derive(Default)]
        struct Probe, fields: probe_fields, arrays: probe_arrays;
        /// Never null.
        id: Required<U64>,
        name: Required<Utf8>,
        note: Optional<Utf8>,
        pid: Optional<U32>,
        cores: Optional<U16>,
        code: Optional<I32>,
        delta: Optional<I64>,
        program: Optional<Category>,
        argv: Optional<Text>,
        phase: Optional<Enum<Phase>>,
    }

    fn rows() -> Vec<Probe> {
        vec![
            Probe {
                id: 7,
                name: "first".to_owned(),
                note: Some("noted".to_owned()),
                pid: Some(4_000_000_000),
                cores: Some(u16::MAX),
                code: Some(-3),
                delta: Some(i64::MIN),
                program: Some("/bin/sh".to_owned()),
                argv: Some("sh -c true".to_owned()),
                phase: Some(Phase::Hot),
            },
            Probe {
                id: 8,
                name: "second".to_owned(),
                program: Some("/bin/sh".to_owned()),
                phase: Some(Phase::Warm),
                ..Probe::default()
            },
        ]
    }

    #[test]
    fn the_declaration_is_the_schema_in_order_with_its_declared_encodings() {
        let declared = probe_fields()
            .into_iter()
            .map(|field| {
                (
                    field.name().clone(),
                    field.data_type().clone(),
                    field.is_nullable(),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(
            declared,
            vec![
                ("id".to_owned(), DataType::UInt64, false),
                ("name".to_owned(), DataType::Utf8, false),
                ("note".to_owned(), DataType::Utf8, true),
                ("pid".to_owned(), DataType::UInt32, true),
                ("cores".to_owned(), DataType::UInt16, true),
                ("code".to_owned(), DataType::Int32, true),
                ("delta".to_owned(), DataType::Int64, true),
                ("program".to_owned(), dict_type(DataType::UInt32), true),
                ("argv".to_owned(), dict_type(DataType::UInt32), true),
                ("phase".to_owned(), dict_type(DataType::UInt16), true),
            ]
        );
    }

    #[test]
    fn rows_encode_as_the_declared_columns_and_a_batch_accepts_them() {
        let rows = rows();
        let columns = probe_arrays(&rows).expect("encode");
        let (fields, arrays): (Vec<Field>, Vec<ArrayRef>) = columns.into_iter().unzip();
        assert_eq!(fields, probe_fields());
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
            .expect("the arrays match the declared fields, required ones holding no null");
        assert_eq!(batch.num_rows(), 2);

        let column = |name: &str| batch.column_by_name(name).expect(name).clone();
        assert_eq!(
            column("id")
                .as_primitive::<UInt64Type>()
                .iter()
                .collect::<Vec<_>>(),
            [Some(7), Some(8)]
        );
        assert!(
            column("id").nulls().is_none() && column("name").nulls().is_none(),
            "a required column carries no validity buffer"
        );
        assert_eq!(column("name").as_string::<i32>().value(1), "second");
        let note = column("note");
        assert_eq!(note.as_string::<i32>().value(0), "noted");
        assert!(note.is_null(1));
        let pid = column("pid");
        assert_eq!(pid.as_primitive::<UInt32Type>().value(0), 4_000_000_000);
        assert!(pid.is_null(1));
        assert_eq!(
            column("cores").as_primitive::<UInt16Type>().value(0),
            u16::MAX
        );
        assert_eq!(column("code").as_primitive::<Int32Type>().value(0), -3);
        assert_eq!(
            column("delta").as_primitive::<Int64Type>().value(0),
            i64::MIN
        );
        assert!(column("delta").is_null(1));
    }

    #[test]
    fn category_text_and_enum_columns_are_dictionaries() {
        let rows = rows();
        let columns = probe_arrays(&rows).expect("encode");
        let array = |name: &str| {
            columns
                .iter()
                .find(|(field, _)| field.name() == name)
                .map(|(_, array)| Arc::clone(array))
                .expect(name)
        };
        let program = array("program");
        let program = program.as_dictionary::<UInt32Type>();
        assert_eq!(
            program.values().as_string::<i32>().len(),
            1,
            "a repeated value is one dictionary entry"
        );
        assert_eq!(program.keys().value(0), program.keys().value(1));
        let argv = array("argv");
        let argv = argv.as_dictionary::<UInt32Type>();
        assert!(argv.is_valid(0) && argv.is_null(1));
        let phase = array("phase");
        let phase = phase.as_dictionary::<UInt16Type>();
        assert_eq!(
            phase.values().as_string::<i32>().iter().collect::<Vec<_>>(),
            [Some("warm"), Some("cold"), Some("hot")],
            "the dictionary holds every declared name, used or not"
        );
        assert_eq!(phase.keys().iter().collect::<Vec<_>>(), [Some(2), Some(0)]);
    }

    #[test]
    fn an_enum_s_keys_follow_its_declaration() {
        assert_eq!(Phase::NAMES, ["warm", "cold", "hot"]);
        for (position, value) in Phase::ALL.iter().enumerate() {
            assert_eq!(usize::from(value.key()), position);
            assert_eq!(value.name(), Phase::NAMES[position]);
        }
    }

    #[test]
    fn no_rows_encode_as_empty_declared_columns() {
        let columns = probe_arrays(&[]).expect("encode");
        assert_eq!(columns.len(), probe_fields().len());
        assert!(columns.iter().all(|(_, array)| array.is_empty()));
    }
}

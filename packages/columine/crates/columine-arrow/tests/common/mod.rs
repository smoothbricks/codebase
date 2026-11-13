//! Construction helpers the columine-arrow tests share.

/// A Schema message of nullable Utf8 fields, given as (name, Arrow dictionary
/// id), where a field with an id is dictionary-encoded over UInt8 indexes and
/// declares exactly that id, and one with `None` is plain.
/// `logical_schema_ipc_bytes` cannot make one that shares an id: arrow's writer
/// numbers each field's dictionary itself.
pub fn schema_message_declaring(declared: &[(&str, Option<i64>)]) -> Vec<u8> {
    let mut builder = flatbuffers::FlatBufferBuilder::new();
    let index_type = arrow_ipc::Int::create(
        &mut builder,
        &arrow_ipc::IntArgs {
            bitWidth: 8,
            is_signed: false,
        },
    );
    let mut fields = Vec::new();
    for (name, id) in declared {
        let name = builder.create_string(name);
        let utf8 = arrow_ipc::Utf8::create(&mut builder, &arrow_ipc::Utf8Args {});
        let dictionary = id.map(|id| {
            arrow_ipc::DictionaryEncoding::create(
                &mut builder,
                &arrow_ipc::DictionaryEncodingArgs {
                    id,
                    indexType: Some(index_type),
                    isOrdered: false,
                    dictionaryKind: arrow_ipc::DictionaryKind::DenseArray,
                },
            )
        });
        let no_children: [flatbuffers::WIPOffset<arrow_ipc::Field<'_>>; 0] = [];
        let children = builder.create_vector(&no_children);
        fields.push(arrow_ipc::Field::create(
            &mut builder,
            &arrow_ipc::FieldArgs {
                name: Some(name),
                nullable: true,
                type_type: arrow_ipc::Type::Utf8,
                type_: Some(utf8.as_union_value()),
                dictionary,
                children: Some(children),
                custom_metadata: None,
            },
        ));
    }
    let fields = builder.create_vector(&fields);
    let schema = arrow_ipc::Schema::create(
        &mut builder,
        &arrow_ipc::SchemaArgs {
            endianness: arrow_ipc::Endianness::Little,
            fields: Some(fields),
            custom_metadata: None,
            features: None,
        },
    );
    let message = arrow_ipc::Message::create(
        &mut builder,
        &arrow_ipc::MessageArgs {
            version: arrow_ipc::MetadataVersion::V5,
            header_type: arrow_ipc::MessageHeader::Schema,
            header: Some(schema.as_union_value()),
            bodyLength: 0,
            custom_metadata: None,
        },
    );
    builder.finish(message, None);
    let flatbuffer = builder.finished_data();
    let padded = flatbuffer.len().next_multiple_of(8);
    let mut bytes = vec![0xFF; 4];
    bytes.extend_from_slice(&u32::try_from(padded).unwrap().to_le_bytes());
    bytes.extend_from_slice(flatbuffer);
    bytes.resize(8 + padded, 0);
    bytes
}

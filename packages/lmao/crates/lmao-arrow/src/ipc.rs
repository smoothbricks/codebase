use std::io::{Read, Write};

use arrow_array::RecordBatch;
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use arrow_schema::ArrowError;

/// Encode exactly one record batch as an Arrow IPC stream.
pub fn write_ipc_stream<W: Write>(sink: &mut W, batch: &RecordBatch) -> Result<(), ArrowError> {
    let mut writer = StreamWriter::try_new(sink, batch.schema_ref())?;
    writer.write(batch)?;
    writer.finish()
}

/// Decode an Arrow IPC stream that must contain exactly one record batch.
pub fn read_single_batch<R: Read>(source: R) -> Result<RecordBatch, ArrowError> {
    let mut reader = StreamReader::try_new(source, None)?;
    let batch = reader.next().transpose()?.ok_or_else(|| {
        ArrowError::ParseError("Arrow IPC stream contains no record batch".into())
    })?;
    if reader.next().transpose()?.is_some() {
        return Err(ArrowError::ParseError(
            "Arrow IPC stream contains more than one record batch".into(),
        ));
    }
    Ok(batch)
}

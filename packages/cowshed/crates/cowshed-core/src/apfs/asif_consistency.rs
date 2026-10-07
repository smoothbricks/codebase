//! Validate ASIF v1's active allocation map before a detached image can be captured.
//!
//! Layout: https://github.com/huven/asif-format . The outer image map is independent of APFS's
//! journal. A valid inner filesystem cannot make missing or multiply allocated image chunks safe.

use std::fs::File;
use std::io;
use std::os::unix::fs::FileExt;

const HEADER_BYTES: usize = 0x50;
const PHYSICAL_MASK: u64 = 0x007f_ffff_ffff_ffff;

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

fn add(left: u64, right: u64) -> io::Result<u64> {
    left.checked_add(right)
        .ok_or_else(|| invalid("ASIF offset overflow"))
}

fn multiply(left: u64, right: u64) -> io::Result<u64> {
    left.checked_mul(right)
        .ok_or_else(|| invalid("ASIF size overflow"))
}

fn be64(bytes: &[u8]) -> u64 {
    u64::from_be_bytes(bytes.try_into().expect("an eight-byte ASIF field"))
}

fn buffer(bytes: u64) -> io::Result<Vec<u8>> {
    let bytes =
        usize::try_from(bytes).map_err(|error| invalid(format!("ASIF buffer size: {error}")))?;
    let mut buffer = Vec::new();
    buffer.try_reserve_exact(bytes).map_err(|error| {
        io::Error::new(
            io::ErrorKind::OutOfMemory,
            format!("ASIF map buffer: {error}"),
        )
    })?;
    buffer.resize(bytes, 0);
    Ok(buffer)
}

fn range(offset: u64, bytes: u64, eof: u64) -> io::Result<()> {
    if add(offset, bytes)? > eof {
        return Err(invalid(format!(
            "ASIF byte range {offset}+{bytes} is past image EOF {eof}"
        )));
    }
    Ok(())
}

/// One bit per physical chunk: ownership checks are indexed, not a scan of prior references.
struct Allocations {
    words: Vec<u64>,
    chunks: u64,
}

impl Allocations {
    fn new(chunks: u64) -> io::Result<Self> {
        let count = usize::try_from(chunks.div_ceil(64))
            .map_err(|error| invalid(format!("ASIF allocation index: {error}")))?;
        let mut words = Vec::new();
        words.try_reserve_exact(count).map_err(|error| {
            io::Error::new(
                io::ErrorKind::OutOfMemory,
                format!("ASIF allocation index: {error}"),
            )
        })?;
        words.resize(count, 0);
        Ok(Self { words, chunks })
    }

    fn reserve(&mut self, chunk: u64) -> io::Result<()> {
        if chunk >= self.chunks {
            return Err(invalid(format!(
                "ASIF reserved chunk {chunk} is past image EOF"
            )));
        }
        let word = usize::try_from(chunk / 64).expect("chunk belongs to the allocated index");
        self.words[word] |= 1 << (chunk % 64);
        Ok(())
    }

    fn claim(&mut self, chunk: u64) -> io::Result<()> {
        if chunk >= self.chunks {
            return Err(invalid(format!(
                "ASIF map advertises physical chunk {chunk} past image EOF"
            )));
        }
        let word = usize::try_from(chunk / 64).expect("chunk belongs to the allocated index");
        let bit = 1 << (chunk % 64);
        if self.words[word] & bit != 0 {
            return Err(invalid(format!(
                "ASIF physical chunk {chunk} is allocated more than once"
            )));
        }
        self.words[word] |= bit;
        Ok(())
    }
}

pub(super) fn validate(file: &File) -> io::Result<()> {
    let eof = file.metadata()?.len();
    let mut header = [0; HEADER_BYTES];
    range(
        0,
        u64::try_from(header.len()).expect("fixed header size"),
        eof,
    )?;
    file.read_exact_at(&mut header, 0)?;
    if &header[..4] != b"shdw" || header[4..8] != 1_u32.to_be_bytes() {
        return Err(invalid(
            "unsupported ASIF image signature or header version",
        ));
    }
    let header_size = u64::from(u32::from_be_bytes(
        header[8..12].try_into().expect("header size"),
    ));
    let directories = [be64(&header[0x10..0x18]), be64(&header[0x18..0x20])];
    let sectors = be64(&header[0x30..0x38]);
    let maximum = be64(&header[0x38..0x40]);
    let chunk = u64::from(u32::from_be_bytes(
        header[0x40..0x44].try_into().expect("chunk size"),
    ));
    let block = u64::from(u16::from_be_bytes(
        header[0x44..0x46].try_into().expect("block size"),
    ));
    let metadata_chunk = be64(&header[0x48..0x50]);
    if header_size < u64::try_from(HEADER_BYTES).expect("fixed header size")
        || block == 0
        || block % 512 != 0
        || chunk == 0
        || chunk % block != 0
        || header[0x46..0x48] != [0, 0]
        || maximum == 0
        || sectors > maximum
    {
        return Err(invalid("invalid ASIF v1 header geometry"));
    }
    let data_per_group = multiply(4, block)?;
    let group_bytes = multiply(8, add(data_per_group, 1)?)?;
    let groups = chunk / group_bytes;
    if groups == 0 {
        return Err(invalid("ASIF chunk cannot hold one allocation group"));
    }
    let data_per_table = multiply(groups, data_per_group)?;
    let entries_per_table = multiply(groups, add(data_per_group, 1)?)?;
    let table_data_bytes = multiply(data_per_table, chunk)?;
    let tables = multiply(maximum, block)?.div_ceil(table_data_bytes);
    let directory_bytes = multiply(add(tables, 1)?, 8)?;
    let table_bytes = multiply(entries_per_table, 8)?
        .div_ceil(block)
        .checked_mul(block)
        .ok_or_else(|| invalid("ASIF aligned table size overflow"))?;
    let spans = [
        (0, header_size),
        (directories[0], directory_bytes),
        (directories[1], directory_bytes),
    ];
    for &(offset, bytes) in &spans {
        range(offset, bytes, eof)?;
    }
    for (index, &(left, left_bytes)) in spans.iter().enumerate() {
        for &(right, right_bytes) in &spans[index + 1..] {
            if left < add(right, right_bytes)? && right < add(left, left_bytes)? {
                return Err(invalid("ASIF header and allocation directories overlap"));
            }
        }
    }
    let mut sequences = [0; 2];
    for (index, offset) in directories.into_iter().enumerate() {
        let mut bytes = [0; 8];
        file.read_exact_at(&mut bytes, offset)?;
        sequences[index] = be64(&bytes);
    }
    if sequences[0] == sequences[1] {
        return Err(invalid(
            "ASIF allocation directories have ambiguous equal generations",
        ));
    }
    let active = if sequences[0] > sequences[1] {
        directories[0]
    } else {
        directories[1]
    };
    if metadata_chunk >= multiply(tables, data_per_table)? {
        return Err(invalid("ASIF metadata chunk is outside its allocation map"));
    }
    let mut allocations = Allocations::new(eof.div_ceil(chunk))?;
    for &(offset, bytes) in &spans {
        for physical in offset / chunk..=add(offset, bytes - 1)? / chunk {
            allocations.reserve(physical)?;
        }
    }
    let mut directory = buffer(multiply(tables, 8)?)?;
    file.read_exact_at(&mut directory, add(active, 8)?)?;
    let mut table = buffer(table_bytes)?;
    let sectors_per_chunk = chunk / block;
    // A group bitmap is exactly one chunk, but one data chunk uses only sectors_per_chunk/4
    // bytes. Reuse that bounded slice rather than reading/copying the whole bitmap per entry.
    let start_padding = if sectors_per_chunk % 4 == 0 { 0 } else { 3 };
    let mut bitmap = buffer(add(sectors_per_chunk, start_padding)?.div_ceil(4))?;
    for (table_index, pointer) in directory.as_chunks::<8>().0.iter().enumerate() {
        let physical_table = be64(pointer);
        if physical_table == 0 {
            continue;
        }
        if physical_table & !PHYSICAL_MASK != 0 {
            return Err(invalid("unsupported flags on an ASIF directory entry"));
        }
        allocations.claim(physical_table)?;
        let table_at = multiply(physical_table, chunk)?;
        range(table_at, table_bytes, eof)?;
        file.read_exact_at(&mut table, table_at)?;
        let table_index = u64::try_from(table_index).expect("directory index fits u64");
        for group in 0..groups {
            let group_entry = multiply(group, add(data_per_group, 1)?)?;
            let bitmap_at = usize::try_from(multiply(add(group_entry, data_per_group)?, 8)?)
                .expect("bitmap reference lies inside the table buffer");
            let physical_bitmap = be64(&table[bitmap_at..bitmap_at + 8]);
            if physical_bitmap & !PHYSICAL_MASK != 0 {
                return Err(invalid("unsupported flags on an ASIF bitmap entry"));
            }
            if physical_bitmap != 0 {
                allocations.claim(physical_bitmap)?;
                range(multiply(physical_bitmap, chunk)?, chunk, eof)?;
            }
            for entry in 0..data_per_group {
                let offset = usize::try_from(multiply(add(group_entry, entry)?, 8)?)
                    .expect("data reference lies inside the table buffer");
                let value = be64(&table[offset..offset + 8]);
                let status = value >> 62;
                let physical = value & PHYSICAL_MASK;
                if status == 0 || status == 2 {
                    if physical != 0 {
                        return Err(invalid(
                            "unsupported physical allocation on a sparse ASIF entry",
                        ));
                    }
                    continue;
                }
                allocations.claim(physical)?;
                let logical = add(
                    multiply(table_index, data_per_table)?,
                    add(multiply(group, data_per_group)?, entry)?,
                )?;
                let first_sector = multiply(logical, sectors_per_chunk)?;
                let visible = if first_sector < sectors {
                    sectors_per_chunk.min(sectors - first_sector)
                } else if logical == metadata_chunk {
                    sectors_per_chunk
                } else {
                    0
                };
                if status == 1 {
                    range(multiply(physical, chunk)?, multiply(visible, block)?, eof)?;
                    continue;
                }
                if physical_bitmap == 0 {
                    return Err(invalid("partial ASIF data entry has no group bitmap"));
                }
                if visible == 0 {
                    continue;
                }
                let bitmap_sector = multiply(entry, sectors_per_chunk)?;
                let first_byte = bitmap_sector / 4;
                let bytes = add(bitmap_sector % 4, visible)?.div_ceil(4);
                let count = usize::try_from(bytes).expect("bitmap slice fits its buffer");
                file.read_exact_at(
                    &mut bitmap[..count],
                    add(multiply(physical_bitmap, chunk)?, first_byte)?,
                )?;
                let mut initialized_end = 0;
                for sector in 0..visible {
                    let bit_sector = add(bitmap_sector % 4, sector)?;
                    let byte =
                        bitmap[usize::try_from(bit_sector / 4).expect("bitmap byte lies in slice")];
                    let state = (byte >> (2 * (bit_sector % 4))) & 3;
                    if state == 3 {
                        return Err(invalid("invalid ASIF bitmap sector state 11"));
                    }
                    if state == 1 {
                        initialized_end = sector + 1;
                    }
                }
                range(
                    multiply(physical, chunk)?,
                    multiply(initialized_end, block)?,
                    eof,
                )?;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::{self, OpenOptions};

    const CHUNK: u64 = 32768;

    struct Image {
        path: std::path::PathBuf,
        file: File,
        chunk: u64,
    }

    impl Image {
        fn new() -> Self {
            Self::with_chunk(32768)
        }

        fn with_chunk(chunk: u32) -> Self {
            let path = std::env::temp_dir().join(format!(
                "cowshed-asif-map-{}.asif",
                uuid::Uuid::new_v4().simple()
            ));
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(&path)
                .unwrap();
            file.set_len(u64::from(chunk) * 5).unwrap();
            let mut header = [0; HEADER_BYTES];
            header[..4].copy_from_slice(b"shdw");
            header[4..8].copy_from_slice(&1_u32.to_be_bytes());
            header[8..12].copy_from_slice(&512_u32.to_be_bytes());
            header[0x10..0x18].copy_from_slice(&512_u64.to_be_bytes());
            header[0x18..0x20].copy_from_slice(&1024_u64.to_be_bytes());
            header[0x30..0x38].copy_from_slice(&64_u64.to_be_bytes());
            header[0x38..0x40].copy_from_slice(&64_u64.to_be_bytes());
            header[0x40..0x44].copy_from_slice(&chunk.to_be_bytes());
            header[0x44..0x46].copy_from_slice(&512_u16.to_be_bytes());
            file.write_all_at(&header, 0).unwrap();
            file.write_all_at(&1_u64.to_be_bytes(), 512).unwrap();
            file.write_all_at(&2_u64.to_be_bytes(), 1024).unwrap();
            file.write_all_at(&1_u64.to_be_bytes(), 1032).unwrap();
            Self {
                path,
                file,
                chunk: u64::from(chunk),
            }
        }

        fn data(&self, logical: u64, physical: u64, status: u64) {
            self.file
                .write_all_at(
                    &((status << 62) | physical).to_be_bytes(),
                    self.chunk + logical * 8,
                )
                .unwrap();
        }
    }

    impl Drop for Image {
        fn drop(&mut self) {
            fs::remove_file(&self.path).unwrap();
        }
    }

    #[test]
    fn bitmap_slices_include_a_non_byte_aligned_first_sector() {
        let image = Image::with_chunk(17920);
        image
            .file
            .write_all_at(&140_u64.to_be_bytes(), 0x30)
            .unwrap();
        image
            .file
            .write_all_at(&140_u64.to_be_bytes(), 0x38)
            .unwrap();
        image.data(1, 2, 3);
        image
            .file
            .write_all_at(&3_u64.to_be_bytes(), image.chunk + 2048 * 8)
            .unwrap();
        image
            .file
            .write_all_at(&[0x55; 10], image.chunk * 3 + 8)
            .unwrap();
        validate(&image.file).unwrap();
    }

    #[test]
    fn valid_sparse_and_full_allocations_are_accepted() {
        let image = Image::new();
        image.data(0, 2, 1);
        image.data(1, 0, 2);
        validate(&image.file).unwrap();
    }

    #[test]
    fn newer_directory_cannot_advertise_appended_chunks_that_never_reached_the_file() {
        let image = Image::new();
        image.data(0, 5, 1);
        let error = validate(&image.file).unwrap_err();
        assert!(error.to_string().contains("past image EOF"), "{error}");
    }

    #[test]
    fn two_logical_chunks_cannot_alias_one_physical_chunk() {
        let image = Image::new();
        image.data(0, 2, 1);
        image.data(1, 2, 1);
        let error = validate(&image.file).unwrap_err();
        assert!(
            error.to_string().contains("allocated more than once"),
            "{error}"
        );
    }

    #[test]
    fn a_data_chunk_cannot_alias_its_allocation_table() {
        let image = Image::new();
        image.data(0, 1, 1);
        assert!(
            validate(&image.file)
                .unwrap_err()
                .to_string()
                .contains("allocated more than once")
        );
    }

    #[test]
    fn legitimate_partial_final_payload_uses_only_visible_initialized_sectors() {
        let image = Image::new();
        image
            .file
            .write_all_at(&76_u64.to_be_bytes(), 0x30)
            .unwrap();
        image
            .file
            .write_all_at(&76_u64.to_be_bytes(), 0x38)
            .unwrap();
        image.file.set_len(CHUNK * 4 + 12 * 512).unwrap();
        image.data(0, 2, 1);
        image.data(1, 4, 3);
        image
            .file
            .write_all_at(&3_u64.to_be_bytes(), CHUNK + 2048 * 8)
            .unwrap();
        image.file.write_all_at(&[0x55; 3], CHUNK * 3 + 16).unwrap();
        validate(&image.file).unwrap();
    }

    #[test]
    fn initialized_partial_sector_past_eof_is_refused() {
        let image = Image::new();
        image.file.set_len(CHUNK * 4 + 512).unwrap();
        image.data(0, 4, 3);
        image
            .file
            .write_all_at(&3_u64.to_be_bytes(), CHUNK + 2048 * 8)
            .unwrap();
        image.file.write_all_at(&[0x05], CHUNK * 3).unwrap();
        assert!(
            validate(&image.file)
                .unwrap_err()
                .to_string()
                .contains("past image EOF")
        );
    }
}

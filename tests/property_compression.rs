use proptest::prelude::*;
use turbolite::tiered::FrameEntry;

#[cfg(feature = "zstd")]
mod zstd_tests {
    use super::*;
    use turbolite::compress::{compress, decompress};

    proptest! {
        #[test]
        fn zstd_compress_decompress_roundtrip(
            data in prop::collection::vec(any::<u8>(), 0..10000),
            level in 1i32..22i32,
        ) {
            let compressed = compress(&data, level, None)
                .expect("compress must not fail");
            let decompressed = decompress(&compressed, None)
                .expect("decompress must not fail");
            prop_assert_eq!(decompressed, data,
                "decompress(compress(data)) must equal original data");
        }

        #[test]
        fn zstd_compress_reduces_size_for_repetitive_data(
            repeat_count in 100usize..1000usize,
        ) {
            let data: Vec<u8> = vec![0xAB; repeat_count];
            let compressed = compress(&data, 3, None)
                .expect("compress must not fail");
            prop_assert!(compressed.len() < data.len(),
                "repetitive data must compress: {} -> {}", compressed.len(), data.len());
        }

        #[test]
        fn zstd_decompress_fails_on_garbage(
            garbage in prop::collection::vec(any::<u8>(), 1..100),
        ) {
            let result = decompress(&garbage, None);
            prop_assert!(result.is_err(),
                "decompressing random bytes must fail, not return garbage");
        }

        #[test]
        fn zstd_empty_data_roundtrip(
            level in 1i32..22i32,
        ) {
            let data: Vec<u8> = Vec::new();
            let compressed = compress(&data, level, None).expect("compress empty");
            let decompressed = decompress(&compressed, None).expect("decompress");
            prop_assert_eq!(decompressed, data);
        }

        #[test]
        fn zstd_single_byte_roundtrip(
            byte in any::<u8>(),
            level in 1i32..22i32,
        ) {
            let data = vec![byte];
            let compressed = compress(&data, level, None).expect("compress");
            let decompressed = decompress(&compressed, None).expect("decompress");
            prop_assert_eq!(decompressed, data);
        }

        #[test]
        fn zstd_all_bytes_roundtrip(
            level in 1i32..22i32,
        ) {
            let data: Vec<u8> = (0..=255).collect();
            let compressed = compress(&data, level, None).expect("compress");
            let decompressed = decompress(&compressed, None).expect("decompress");
            prop_assert_eq!(decompressed, data);
        }

        #[test]
        fn zstd_compression_is_deterministic(
            data in prop::collection::vec(any::<u8>(), 0..1000),
            level in 1i32..22i32,
        ) {
            let c1 = compress(&data, level, None).expect("compress 1");
            let c2 = compress(&data, level, None).expect("compress 2");
            prop_assert_eq!(c1, c2, "compression must be deterministic");
        }
    }
}

proptest! {
    #[test]
    fn seekable_frame_offsets_are_valid(
        num_frames in 1u8..64u8,
        frame_size in 100u32..1_000_000u32,
    ) {
        let mut frame_table: Vec<FrameEntry> = Vec::new();
        let mut offset: u64 = 0;

        for _ in 0..num_frames {
            frame_table.push(FrameEntry {
                offset,
                len: frame_size,
            });
            offset += frame_size as u64;
        }

        prop_assert!(!frame_table.is_empty(), "frame table must not be empty");

        for i in 1..frame_table.len() {
            let prev_end = frame_table[i - 1].offset + frame_table[i - 1].len as u64;
            let curr_start = frame_table[i].offset;

            prop_assert_eq!(curr_start, prev_end,
                "frame {} start must equal frame {} end (no gaps, no overlaps)",
                i, i - 1);

            prop_assert!(frame_table[i].len > 0,
                "frame {} len must be > 0", i);
        }

        let first = &frame_table[0];
        prop_assert_eq!(first.offset, 0, "first frame must start at offset 0");
    }

    #[test]
    fn seekable_frame_offsets_strictly_increasing(
        num_frames in 1u8..32u8,
        base_len in 1u32..100_000u32,
    ) {
        let mut frame_table: Vec<FrameEntry> = Vec::new();
        let mut offset: u64 = 0;

        for i in 0..num_frames {
            let len = base_len + (i as u32 * 100);
            frame_table.push(FrameEntry { offset, len });
            offset += len as u64;
        }

        for i in 1..frame_table.len() {
            prop_assert!(frame_table[i].offset > frame_table[i - 1].offset,
                "frame offsets must be strictly increasing");
        }
    }

    #[test]
    fn seekable_frame_no_overlaps(
        num_frames in 1u8..32u8,
    ) {
        let mut frame_table: Vec<FrameEntry> = Vec::new();
        let mut offset: u64 = 0;

        for i in 0..num_frames {
            let len = 500u32 + (i as u32 * 73);
            frame_table.push(FrameEntry { offset, len });
            offset += len as u64;
        }

        for i in 0..frame_table.len() {
            for j in (i + 1)..frame_table.len() {
                let end_i = frame_table[i].offset + frame_table[i].len as u64;
                let start_j = frame_table[j].offset;
                prop_assert!(start_j >= end_i,
                    "frame {} [{}..{}) must not overlap with frame {} [{}..{})",
                    i, frame_table[i].offset, end_i,
                    j, start_j, start_j + frame_table[j].len as u64);
            }
        }
    }

    #[test]
    fn seekable_frame_total_length_consistent(
        num_frames in 1u8..32u8,
    ) {
        let mut frame_table: Vec<FrameEntry> = Vec::new();
        let mut offset: u64 = 0;

        for i in 0..num_frames {
            let len = 1000u32 + (i as u32 * 256);
            frame_table.push(FrameEntry { offset, len });
            offset += len as u64;
        }

        let total_from_offsets = frame_table.last().map(|f| f.offset + f.len as u64).unwrap_or(0);
        let total_from_lens: u64 = frame_table.iter().map(|f| f.len as u64).sum();

        prop_assert_eq!(total_from_offsets, total_from_lens,
            "total length from offsets must equal sum of all frame lengths");
    }
}

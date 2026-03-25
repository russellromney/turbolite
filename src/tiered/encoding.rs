use super::*;

// ===== Page group encoding/decoding =====
//
// Whole-group compression: raw pages packed together, then single zstd frame.
//
// S3 object format:
//   zstd([u32 page_count][u32 page_size][page_0 bytes][page_1 bytes]...[page_N bytes])
//
// Each page is exactly page_size bytes. Empty trailing pages are omitted
// (page_count tells us how many pages are in the group).

/// Decrypt data if an encryption key is provided, otherwise return as-is (zero-copy).
pub(crate) fn decrypt_if_needed(data: &[u8], encryption_key: Option<&[u8; 32]>) -> io::Result<Vec<u8>> {
    #[cfg(feature = "encryption")]
    if let Some(key) = encryption_key {
        return compress::decrypt_gcm_random_nonce(data, key);
    }
    let _ = encryption_key; // suppress unused warnings when encryption feature is off
    Ok(data.to_vec())
}

/// Encode a page group: pack raw pages and compress as single zstd frame.
/// `pages` is a slice of Option<Vec<u8>> — None means empty (zero-filled) page.
/// Returns the compressed blob for S3.
pub(crate) fn encode_page_group(
    pages: &[Option<Vec<u8>>],
    page_size: u32,
    compression_level: i32,
    #[cfg(feature = "zstd")] encoder_dict: Option<&zstd::dict::EncoderDictionary<'static>>,
    encryption_key: Option<&[u8; 32]>,
) -> io::Result<Vec<u8>> {
    // Find last non-empty page to avoid trailing zeros
    let page_count = pages
        .iter()
        .rposition(|p| p.is_some())
        .map(|i| i + 1)
        .unwrap_or(0) as u32;

    // Build raw buffer: [u32 page_count][u32 page_size][page bytes...]
    let header_len = 8; // 2 × u32
    let raw_len = header_len + page_count as usize * page_size as usize;
    let mut raw = Vec::with_capacity(raw_len);
    raw.extend_from_slice(&page_count.to_le_bytes());
    raw.extend_from_slice(&page_size.to_le_bytes());

    for i in 0..page_count as usize {
        match pages.get(i).and_then(|p| p.as_ref()) {
            Some(data) => {
                raw.extend_from_slice(data);
                // Pad to page_size if data is shorter
                if data.len() < page_size as usize {
                    raw.resize(raw.len() + page_size as usize - data.len(), 0);
                }
            }
            None => {
                // Zero-filled page
                raw.resize(raw.len() + page_size as usize, 0);
            }
        }
    }

    let compressed = compress::compress(
        &raw,
        compression_level,
        #[cfg(feature = "zstd")]
        encoder_dict,
        #[cfg(not(feature = "zstd"))]
        None,
    )?;

    #[cfg(feature = "encryption")]
    if let Some(key) = encryption_key {
        return compress::encrypt_gcm_random_nonce(&compressed, key);
    }

    Ok(compressed)
}

/// Encode a page group as multiple independently-decompressible sub-chunk frames.
/// Returns (compressed_blob, frame_table). Each frame contains `sub_ppg` pages
/// and can be fetched via S3 byte-range GET and decompressed independently.
/// This enables point lookups to download ~128KB instead of ~10MB.
pub(crate) fn encode_page_group_seekable(
    pages: &[Option<Vec<u8>>],
    page_size: u32,
    sub_ppg: u32,
    compression_level: i32,
    #[cfg(feature = "zstd")] encoder_dict: Option<&zstd::dict::EncoderDictionary<'static>>,
    encryption_key: Option<&[u8; 32]>,
) -> io::Result<(Vec<u8>, Vec<FrameEntry>)> {
    // Find last non-empty page to avoid trailing zeros
    let page_count = pages
        .iter()
        .rposition(|p| p.is_some())
        .map(|i| i + 1)
        .unwrap_or(0);

    let num_frames = (page_count + sub_ppg as usize - 1) / sub_ppg as usize;
    let mut blob = Vec::new();
    let mut frame_table = Vec::with_capacity(num_frames);

    for frame_idx in 0..num_frames {
        let start = frame_idx * sub_ppg as usize;
        let end = std::cmp::min(start + sub_ppg as usize, page_count);
        let pages_in_frame = end - start;

        // Build raw sub-chunk: just page bytes (no header — metadata is in manifest)
        let mut raw = Vec::with_capacity(pages_in_frame * page_size as usize);
        for i in start..end {
            match pages.get(i).and_then(|p| p.as_ref()) {
                Some(data) => {
                    raw.extend_from_slice(data);
                    if data.len() < page_size as usize {
                        raw.resize(raw.len() + page_size as usize - data.len(), 0);
                    }
                }
                None => {
                    raw.resize(raw.len() + page_size as usize, 0);
                }
            }
        }

        let offset = blob.len() as u64;
        let mut frame_data = compress::compress(
            &raw,
            compression_level,
            #[cfg(feature = "zstd")]
            encoder_dict,
            #[cfg(not(feature = "zstd"))]
            None,
        )?;

        // Per-frame encryption: random nonce prepended to each frame
        #[cfg(feature = "encryption")]
        if let Some(key) = encryption_key {
            frame_data = compress::encrypt_gcm_random_nonce(&frame_data, key)?;
        }

        frame_table.push(FrameEntry {
            offset,
            len: frame_data.len() as u32,
        });
        blob.extend_from_slice(&frame_data);
    }

    Ok((blob, frame_table))
}

/// Decode a single sub-chunk frame from a seekable page group.
/// Returns the raw page data for the sub-chunk (pages_in_frame × page_size bytes).
pub(crate) fn decode_seekable_subchunk(
    compressed_frame: &[u8],
    #[cfg(feature = "zstd")] decoder_dict: Option<&zstd::dict::DecoderDictionary<'static>>,
    encryption_key: Option<&[u8; 32]>,
) -> io::Result<Vec<u8>> {
    let data = decrypt_if_needed(compressed_frame, encryption_key)?;
    compress::decompress(
        &data,
        #[cfg(feature = "zstd")]
        decoder_dict,
        #[cfg(not(feature = "zstd"))]
        None,
    )
}

/// Decode all frames of a seekable page group (for prefetch / full download).
/// Returns (page_count, page_size, contiguous page data) — same shape as decode_page_group_bulk.
pub(crate) fn decode_page_group_seekable_full(
    data: &[u8],
    frame_table: &[FrameEntry],
    page_size: u32,
    pages_per_group: u32,
    total_page_count: u64,
    group_start: u64,
    #[cfg(feature = "zstd")] decoder_dict: Option<&zstd::dict::DecoderDictionary<'static>>,
    encryption_key: Option<&[u8; 32]>,
) -> io::Result<(u32, u32, Vec<u8>)> {
    let actual_pages = std::cmp::min(
        pages_per_group as u64,
        total_page_count.saturating_sub(group_start),
    ) as u32;
    let mut output = Vec::with_capacity(actual_pages as usize * page_size as usize);

    for (_frame_idx, entry) in frame_table.iter().enumerate() {
        let start = entry.offset as usize;
        let end = start + entry.len as usize;
        if end > data.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("frame extends beyond data: {}..{} > {}", start, end, data.len()),
            ));
        }
        let decrypted = decrypt_if_needed(&data[start..end], encryption_key)?;
        let decompressed = compress::decompress(
            &decrypted,
            #[cfg(feature = "zstd")]
            decoder_dict,
            #[cfg(not(feature = "zstd"))]
            None,
        )?;
        output.extend_from_slice(&decompressed);
    }

    // Truncate to actual page count (last frame may have been padded).
    // Use the smaller of the theoretical max and actual decoded bytes,
    // since the encoder strips trailing empty pages from the last group.
    let decoded_pages = (output.len() / page_size as usize) as u32;
    let actual_pages = std::cmp::min(actual_pages, decoded_pages);
    let expected_len = actual_pages as usize * page_size as usize;
    output.truncate(expected_len);

    Ok((actual_pages, page_size, output))
}

/// Decode a page group: decompress single zstd frame, split into pages.
/// Returns Vec of raw page buffers (each exactly page_size bytes).
pub(crate) fn decode_page_group(
    compressed: &[u8],
    #[cfg(feature = "zstd")] decoder_dict: Option<&zstd::dict::DecoderDictionary<'static>>,
    encryption_key: Option<&[u8; 32]>,
) -> io::Result<(u32, u32, Vec<Vec<u8>>)> {
    let decrypted = decrypt_if_needed(compressed, encryption_key)?;
    let raw = compress::decompress(
        &decrypted,
        #[cfg(feature = "zstd")]
        decoder_dict,
        #[cfg(not(feature = "zstd"))]
        None,
    )?;

    if raw.len() < 8 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "page group too short for header",
        ));
    }

    let page_count = u32::from_le_bytes([raw[0], raw[1], raw[2], raw[3]]);
    let page_size = u32::from_le_bytes([raw[4], raw[5], raw[6], raw[7]]);

    let expected_len = 8 + page_count as usize * page_size as usize;
    if raw.len() < expected_len {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "page group data truncated: expected {} bytes, got {}",
                expected_len,
                raw.len()
            ),
        ));
    }

    let mut pages = Vec::with_capacity(page_count as usize);
    for i in 0..page_count as usize {
        let start = 8 + i * page_size as usize;
        let end = start + page_size as usize;
        pages.push(raw[start..end].to_vec());
    }

    Ok((page_count, page_size, pages))
}

/// Decode a page group, returning the contiguous page data buffer (no per-page allocation).
/// Returns (page_count, page_size, raw_page_data_slice_starting_after_8byte_header).
pub(crate) fn decode_page_group_bulk(
    compressed: &[u8],
    #[cfg(feature = "zstd")] decoder_dict: Option<&zstd::dict::DecoderDictionary<'static>>,
    encryption_key: Option<&[u8; 32]>,
) -> io::Result<(u32, u32, Vec<u8>)> {
    let decrypted = decrypt_if_needed(compressed, encryption_key)?;
    let raw = compress::decompress(
        &decrypted,
        #[cfg(feature = "zstd")]
        decoder_dict,
        #[cfg(not(feature = "zstd"))]
        None,
    )?;

    if raw.len() < 8 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "page group too short for header",
        ));
    }

    let page_count = u32::from_le_bytes([raw[0], raw[1], raw[2], raw[3]]);
    let page_size = u32::from_le_bytes([raw[4], raw[5], raw[6], raw[7]]);

    let expected_len = 8 + page_count as usize * page_size as usize;
    if raw.len() < expected_len {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "page group data truncated: expected {} bytes, got {}",
                expected_len,
                raw.len()
            ),
        ));
    }

    // Return the page data portion (after the 8-byte header) as a contiguous buffer
    let page_data = raw[8..expected_len].to_vec();
    Ok((page_count, page_size, page_data))
}

// ===== Interior Bundle encode/decode =====
//
// Interior bundle: a single S3 object containing all B-tree interior pages.
// Unlike page groups (contiguous pages), the bundle stores non-contiguous pages
// with their page numbers.
//
// Format:
//   zstd([u32 page_count][u32 page_size][page_count × u64 page_nums][page data...])

/// Encode an interior bundle from (page_number, raw_data) pairs.
pub(crate) fn encode_interior_bundle(
    pages: &[(u64, &[u8])],
    page_size: u32,
    compression_level: i32,
    #[cfg(feature = "zstd")] encoder_dict: Option<&zstd::dict::EncoderDictionary<'static>>,
    encryption_key: Option<&[u8; 32]>,
) -> io::Result<Vec<u8>> {
    let page_count = pages.len() as u32;
    let header_len = 8 + pages.len() * 8; // 2×u32 + page_count×u64
    let raw_len = header_len + pages.len() * page_size as usize;
    let mut raw = Vec::with_capacity(raw_len);

    raw.extend_from_slice(&page_count.to_le_bytes());
    raw.extend_from_slice(&page_size.to_le_bytes());

    // Page numbers
    for (pnum, _) in pages {
        raw.extend_from_slice(&pnum.to_le_bytes());
    }

    // Page data
    for (_, data) in pages {
        raw.extend_from_slice(data);
        if data.len() < page_size as usize {
            raw.resize(raw.len() + page_size as usize - data.len(), 0);
        }
    }

    let compressed = compress::compress(
        &raw,
        compression_level,
        #[cfg(feature = "zstd")]
        encoder_dict,
        #[cfg(not(feature = "zstd"))]
        None,
    )?;

    #[cfg(feature = "encryption")]
    if let Some(key) = encryption_key {
        return compress::encrypt_gcm_random_nonce(&compressed, key);
    }

    Ok(compressed)
}

/// Decode an interior bundle: returns Vec of (page_number, raw_page_data).
pub(crate) fn decode_interior_bundle(
    compressed: &[u8],
    #[cfg(feature = "zstd")] decoder_dict: Option<&zstd::dict::DecoderDictionary<'static>>,
    encryption_key: Option<&[u8; 32]>,
) -> io::Result<Vec<(u64, Vec<u8>)>> {
    let decrypted = decrypt_if_needed(compressed, encryption_key)?;
    let raw = compress::decompress(
        &decrypted,
        #[cfg(feature = "zstd")]
        decoder_dict,
        #[cfg(not(feature = "zstd"))]
        None,
    )?;

    if raw.len() < 8 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "interior bundle too short for header",
        ));
    }

    let page_count = u32::from_le_bytes([raw[0], raw[1], raw[2], raw[3]]) as usize;
    let page_size = u32::from_le_bytes([raw[4], raw[5], raw[6], raw[7]]) as usize;

    let pnums_end = 8 + page_count * 8;
    let expected_len = pnums_end + page_count * page_size;
    if raw.len() < expected_len {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "interior bundle truncated: expected {} bytes, got {}",
                expected_len, raw.len()
            ),
        ));
    }

    let mut result = Vec::with_capacity(page_count);
    for i in 0..page_count {
        let pnum_offset = 8 + i * 8;
        let pnum = u64::from_le_bytes(
            raw[pnum_offset..pnum_offset + 8]
                .try_into()
                .expect("8 bytes for u64"),
        );
        let data_offset = pnums_end + i * page_size;
        let data = raw[data_offset..data_offset + page_size].to_vec();
        result.push((pnum, data));
    }

    Ok(result)
}


//! On-disk format for the single-file compressed local database
//! ("TLLOCAL1").
//!
//! Layout of the file:
//!
//! ```text
//!   [ Header        : HEADER_LEN bytes, fixed                ]
//!   [ Page data     : variable-length per-page blobs         ]
//!   [ Page directory: page_count * DIR_ENTRY_LEN bytes       ]
//! ```
//!
//! SQLite's pager only ever sees fixed-size logical pages. Because a
//! compressed page is variable length, the directory maps each logical
//! page index to the `(physical_offset, length)` of its blob in the data
//! region. The directory lives at the tail so it can be rewritten in
//! place when pages move; the header records where it starts.
//!
//! Each page blob is self-describing: a 1-byte codec tag (raw or zstd)
//! followed by the payload. When an encryption key is configured the
//! payload is encrypted (AES-256-CTR) after compression; the tag stays in
//! the clear so a reader knows how to decompress once decrypted. Storing
//! a per-page tag lets incompressible pages fall back to raw without a
//! format flag, and keeps the codec choice local to each page.

use std::io;

/// File magic. Bumping the trailing digit is a hard format break.
pub const MAGIC: &[u8; 8] = b"TLLOCAL1";
/// Current format version. Encoded big-endian after the magic.
pub const FORMAT_VERSION: u16 = 1;
/// Fixed header length. Reserved bytes leave room for future fields
/// without moving the data region.
pub const HEADER_LEN: usize = 64;
/// Directory entry: u64 offset + u32 length, little-endian.
pub const DIR_ENTRY_LEN: usize = 12;
/// Default logical page size when the caller does not specify one.
pub const DEFAULT_PAGE_SIZE: u32 = 4096;
/// Smallest accepted page size (SQLite's minimum). Guards against a corrupt
/// header driving absurd allocations / divisions.
pub const MIN_PAGE_SIZE: u32 = 512;
/// Largest accepted page size. A decompressed page can never exceed this,
/// so it doubles as the per-page decompression cap (anti zip-bomb).
pub const MAX_PAGE_SIZE: u32 = 65536;

/// Header flag: page payloads are encrypted.
pub const FLAG_ENCRYPTED: u16 = 1 << 0;

/// Per-page codec tag: payload stored uncompressed.
const CODEC_RAW: u8 = 0;
/// Per-page codec tag: payload compressed with zstd.
const CODEC_ZSTD: u8 = 1;

/// Fixed-size file header. All multi-byte integers are little-endian
/// except the magic/version preamble, which is byte-stable by design.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Header {
    pub format_version: u16,
    pub flags: u16,
    pub page_size: u32,
    pub page_count: u64,
    pub directory_offset: u64,
}

impl Header {
    pub fn encode(&self) -> [u8; HEADER_LEN] {
        let mut buf = [0u8; HEADER_LEN];
        buf[0..8].copy_from_slice(MAGIC);
        buf[8..10].copy_from_slice(&FORMAT_VERSION.to_be_bytes());
        buf[10..12].copy_from_slice(&self.flags.to_le_bytes());
        buf[12..16].copy_from_slice(&self.page_size.to_le_bytes());
        buf[16..24].copy_from_slice(&self.page_count.to_le_bytes());
        buf[24..32].copy_from_slice(&self.directory_offset.to_le_bytes());
        // bytes 32..64 reserved (zeroed)
        buf
    }

    pub fn decode(buf: &[u8]) -> io::Result<Self> {
        if buf.len() < HEADER_LEN {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "local db header truncated",
            ));
        }
        if &buf[0..8] != MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "not a TLLOCAL1 file (bad magic)",
            ));
        }
        let format_version = u16::from_be_bytes([buf[8], buf[9]]);
        if format_version != FORMAT_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unsupported TLLOCAL1 version {format_version}"),
            ));
        }
        let page_size = u32::from_le_bytes([buf[12], buf[13], buf[14], buf[15]]);
        if !(MIN_PAGE_SIZE..=MAX_PAGE_SIZE).contains(&page_size) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("TLLOCAL1 page_size {page_size} out of range"),
            ));
        }
        let directory_offset = u64::from_le_bytes(buf[24..32].try_into().unwrap());
        if directory_offset < HEADER_LEN as u64 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "TLLOCAL1 directory_offset inside header",
            ));
        }
        Ok(Header {
            format_version,
            flags: u16::from_le_bytes([buf[10], buf[11]]),
            page_size,
            page_count: u64::from_le_bytes(buf[16..24].try_into().unwrap()),
            directory_offset,
        })
    }
}

/// How a page should be (de)compressed and optionally encrypted. The key,
/// when present, is only honored with the `encryption` feature compiled in;
/// otherwise encode/decode return an error rather than silently storing
/// plaintext.
#[derive(Clone)]
pub struct PageCodec {
    pub compress: bool,
    pub level: i32,
    pub key: Option<[u8; 32]>,
}

impl PageCodec {
    pub fn header_flags(&self) -> u16 {
        if self.key.is_some() {
            FLAG_ENCRYPTED
        } else {
            0
        }
    }
}

/// Encode one logical page into its stored blob.
///
/// When a key is configured the payload is encrypted with AES-256-GCM using
/// a fresh random nonce stored in the blob, so rewriting the same page never
/// reuses a keystream (CTR-style many-time-pad) and tampering is detected.
pub fn encode_page(page: &[u8], codec: &PageCodec) -> io::Result<Vec<u8>> {
    let (tag, payload) = compress_page(page, codec)?;
    let payload = maybe_encrypt(payload, codec)?;
    let mut blob = Vec::with_capacity(payload.len() + 1);
    blob.push(tag);
    blob.extend_from_slice(&payload);
    Ok(blob)
}

/// Decode a stored blob back into a logical page. `max_page` caps the
/// decompressed output (anti decompression-bomb); pass the header page size.
pub fn decode_page(blob: &[u8], codec: &PageCodec, max_page: usize) -> io::Result<Vec<u8>> {
    if blob.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "empty page blob",
        ));
    }
    let tag = blob[0];
    let payload = maybe_decrypt(&blob[1..], codec)?;
    match tag {
        CODEC_RAW => {
            if payload.len() > max_page {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "raw page exceeds page size",
                ));
            }
            Ok(payload)
        }
        CODEC_ZSTD => decompress_zstd(&payload, max_page),
        other => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unknown page codec tag {other}"),
        )),
    }
}

fn compress_page(page: &[u8], codec: &PageCodec) -> io::Result<(u8, Vec<u8>)> {
    if !codec.compress {
        return Ok((CODEC_RAW, page.to_vec()));
    }
    let compressed = compress_zstd(page, codec.level)?;
    // Only keep the compressed form when it actually saves space;
    // incompressible pages (already-compressed blobs, encrypted data)
    // fall back to raw so we never inflate the file.
    if compressed.len() < page.len() {
        Ok((CODEC_ZSTD, compressed))
    } else {
        Ok((CODEC_RAW, page.to_vec()))
    }
}

#[cfg(feature = "zstd")]
fn compress_zstd(data: &[u8], level: i32) -> io::Result<Vec<u8>> {
    crate::compress::compress(data, level, None)
}

#[cfg(not(feature = "zstd"))]
fn compress_zstd(data: &[u8], _level: i32) -> io::Result<Vec<u8>> {
    // Without zstd compiled in we cannot compress; signal "no win" by
    // returning the input unchanged so compress_page falls back to raw.
    Ok(data.to_vec())
}

#[cfg(feature = "zstd")]
fn decompress_zstd(data: &[u8], max_page: usize) -> io::Result<Vec<u8>> {
    let out = crate::compress::decompress(data, None)?;
    if out.len() > max_page {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "decompressed page exceeds page size (possible bomb)",
        ));
    }
    Ok(out)
}

#[cfg(not(feature = "zstd"))]
fn decompress_zstd(_data: &[u8], _max_page: usize) -> io::Result<Vec<u8>> {
    Err(io::Error::new(
        io::ErrorKind::InvalidData,
        "page is zstd-compressed but zstd feature is not compiled in",
    ))
}

#[cfg(feature = "encryption")]
fn maybe_encrypt(payload: Vec<u8>, codec: &PageCodec) -> io::Result<Vec<u8>> {
    match codec.key {
        Some(key) => crate::compress::encrypt_gcm_random_nonce(&payload, &[], &key),
        None => Ok(payload),
    }
}

#[cfg(not(feature = "encryption"))]
fn maybe_encrypt(payload: Vec<u8>, codec: &PageCodec) -> io::Result<Vec<u8>> {
    if codec.key.is_some() {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "encryption key supplied but the `encryption` feature is not compiled in",
        ));
    }
    Ok(payload)
}

#[cfg(feature = "encryption")]
fn maybe_decrypt(payload: &[u8], codec: &PageCodec) -> io::Result<Vec<u8>> {
    match codec.key {
        Some(key) => crate::compress::decrypt_gcm_random_nonce(payload, &[], &key),
        None => Ok(payload.to_vec()),
    }
}

#[cfg(not(feature = "encryption"))]
fn maybe_decrypt(payload: &[u8], codec: &PageCodec) -> io::Result<Vec<u8>> {
    if codec.key.is_some() {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "encrypted page but the `encryption` feature is not compiled in",
        ));
    }
    Ok(payload.to_vec())
}

/// Serialize the page directory: `page_count` entries of (offset, len).
pub fn encode_directory(entries: &[(u64, u32)]) -> Vec<u8> {
    let mut out = Vec::with_capacity(entries.len() * DIR_ENTRY_LEN);
    for (offset, len) in entries {
        out.extend_from_slice(&offset.to_le_bytes());
        out.extend_from_slice(&len.to_le_bytes());
    }
    out
}

/// Parse `count` directory entries from `bytes`.
pub fn decode_directory(bytes: &[u8], count: usize) -> io::Result<Vec<(u64, u32)>> {
    let needed = count
        .checked_mul(DIR_ENTRY_LEN)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "directory size overflow"))?;
    if bytes.len() < needed {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "page directory truncated",
        ));
    }
    let mut entries = Vec::with_capacity(count);
    for i in 0..count {
        let base = i * DIR_ENTRY_LEN;
        let offset = u64::from_le_bytes(bytes[base..base + 8].try_into().unwrap());
        let len = u32::from_le_bytes(bytes[base + 8..base + 12].try_into().unwrap());
        entries.push((offset, len));
    }
    Ok(entries)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn plain_codec() -> PageCodec {
        PageCodec {
            compress: true,
            level: 3,
            key: None,
        }
    }

    #[test]
    fn header_roundtrip() {
        let h = Header {
            format_version: FORMAT_VERSION,
            flags: FLAG_ENCRYPTED,
            page_size: 4096,
            page_count: 12345,
            directory_offset: 9_000_000,
        };
        let encoded = h.encode();
        assert_eq!(&encoded[0..8], MAGIC);
        let decoded = Header::decode(&encoded).unwrap();
        assert_eq!(h, decoded);
    }

    #[test]
    fn header_rejects_bad_magic() {
        let mut buf = [0u8; HEADER_LEN];
        buf[0..8].copy_from_slice(b"NOTLOCAL");
        assert!(Header::decode(&buf).is_err());
    }

    #[test]
    fn page_roundtrip_compressible() {
        // Highly compressible page (zeros) -> should pick zstd and shrink.
        let page = vec![0u8; 4096];
        let codec = plain_codec();
        let blob = encode_page(&page, &codec).unwrap();
        assert!(blob.len() < page.len(), "compressible page should shrink");
        let back = decode_page(&blob, &codec, 4096).unwrap();
        assert_eq!(back, page);
    }

    #[test]
    fn page_roundtrip_incompressible_falls_back_to_raw() {
        // Pseudo-random bytes do not compress; encoder must fall back to
        // raw and not inflate beyond payload + 1 tag byte.
        let mut page = vec![0u8; 4096];
        let mut x: u32 = 0x1234_5678;
        for b in page.iter_mut() {
            x = x.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
            *b = (x >> 24) as u8;
        }
        let codec = plain_codec();
        let blob = encode_page(&page, &codec).unwrap();
        assert_eq!(blob.len(), page.len() + 1, "raw fallback = payload + tag");
        let back = decode_page(&blob, &codec, 4096).unwrap();
        assert_eq!(back, page);
    }

    #[test]
    fn page_roundtrip_no_compression() {
        let page = vec![7u8; 1024];
        let codec = PageCodec {
            compress: false,
            level: 0,
            key: None,
        };
        let blob = encode_page(&page, &codec).unwrap();
        assert_eq!(blob.len(), page.len() + 1);
        assert_eq!(decode_page(&blob, &codec, 4096).unwrap(), page);
    }

    #[test]
    fn decode_rejects_oversized_raw_page() {
        let page = vec![1u8; 4096];
        let codec = PageCodec {
            compress: false,
            level: 0,
            key: None,
        };
        let blob = encode_page(&page, &codec).unwrap();
        // A max_page smaller than the stored raw page must be rejected.
        assert!(decode_page(&blob, &codec, 1024).is_err());
    }

    #[test]
    fn header_rejects_out_of_range_page_size() {
        let mut h = Header {
            format_version: FORMAT_VERSION,
            flags: 0,
            page_size: 100, // below MIN_PAGE_SIZE
            page_count: 1,
            directory_offset: HEADER_LEN as u64,
        };
        assert!(Header::decode(&h.encode()).is_err());
        h.page_size = 1 << 20; // above MAX_PAGE_SIZE
        assert!(Header::decode(&h.encode()).is_err());
    }

    #[test]
    fn directory_roundtrip() {
        let entries = vec![(64u64, 100u32), (164, 50), (214, 4096)];
        let bytes = encode_directory(&entries);
        assert_eq!(bytes.len(), entries.len() * DIR_ENTRY_LEN);
        let back = decode_directory(&bytes, entries.len()).unwrap();
        assert_eq!(back, entries);
    }

    #[test]
    fn directory_truncation_errors() {
        let entries = vec![(0u64, 1u32)];
        let bytes = encode_directory(&entries);
        assert!(decode_directory(&bytes, 2).is_err());
    }

    #[cfg(feature = "encryption")]
    #[test]
    fn page_roundtrip_encrypted() {
        let page = vec![0u8; 4096];
        let codec = PageCodec {
            compress: true,
            level: 3,
            key: Some([42u8; 32]),
        };
        let blob = encode_page(&page, &codec).unwrap();
        // Encrypted payload must not equal the plaintext page tail.
        assert_ne!(&blob[1..], &page[..blob.len() - 1]);
        let back = decode_page(&blob, &codec, 4096).unwrap();
        assert_eq!(back, page);
    }

    #[cfg(feature = "encryption")]
    #[test]
    fn encrypted_blob_rewrite_uses_fresh_nonce() {
        // GCM random-nonce: encoding the same page twice must produce
        // different ciphertext (no keystream reuse), yet both decode back.
        let page = vec![3u8; 4096];
        let codec = PageCodec {
            compress: false,
            level: 0,
            key: Some([9u8; 32]),
        };
        let a = encode_page(&page, &codec).unwrap();
        let b = encode_page(&page, &codec).unwrap();
        assert_ne!(a, b, "fresh nonce per encode");
        assert_eq!(decode_page(&a, &codec, 4096).unwrap(), page);
        assert_eq!(decode_page(&b, &codec, 4096).unwrap(), page);
    }

    #[cfg(feature = "encryption")]
    #[test]
    fn encrypted_blob_wrong_key_errors() {
        // GCM is authenticated: the wrong key must fail to decrypt, never
        // return garbage.
        let page = vec![3u8; 4096];
        let codec = PageCodec {
            compress: false,
            level: 0,
            key: Some([9u8; 32]),
        };
        let blob = encode_page(&page, &codec).unwrap();
        let wrong_codec = PageCodec {
            key: Some([1u8; 32]),
            ..codec
        };
        assert!(decode_page(&blob, &wrong_codec, 4096).is_err());
    }
}

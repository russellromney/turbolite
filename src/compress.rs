//! Compression and encryption free functions.
//!
//! Used by TurboliteHandle for page-level compression/decompression.

use std::io;

// ===== ZSTD Compression =====

#[cfg(feature = "zstd")]
use zstd::dict::{DecoderDictionary, EncoderDictionary};

#[cfg(feature = "zstd")]
pub fn compress(
    data: &[u8],
    level: i32,
    encoder_dict: Option<&EncoderDictionary<'static>>,
) -> io::Result<Vec<u8>> {
    use std::io::Write;
    use zstd::encode_all;

    if let Some(encoder_dict) = encoder_dict {
        let mut encoder = zstd::stream::Encoder::with_prepared_dictionary(Vec::new(), encoder_dict)
            .map_err(io::Error::other)?;
        encoder.write_all(data)?;
        encoder
            .finish()
            .map_err(io::Error::other)
    } else {
        encode_all(data, level).map_err(io::Error::other)
    }
}

#[cfg(feature = "zstd")]
pub fn decompress(
    data: &[u8],
    decoder_dict: Option<&DecoderDictionary<'static>>,
) -> io::Result<Vec<u8>> {
    use std::io::Read;
    use zstd::decode_all;

    if let Some(decoder_dict) = decoder_dict {
        let mut decoder = zstd::stream::Decoder::with_prepared_dictionary(data, decoder_dict)
            .map_err(io::Error::other)?;
        let mut output = Vec::new();
        decoder.read_to_end(&mut output)?;
        Ok(output)
    } else {
        decode_all(data).map_err(io::Error::other)
    }
}

/// Decompress with a hard output cap (anti decompression-bomb).
///
/// `max_len` is the largest legitimate decompressed size for the frame
/// (`pages_per_group * page_size + header`). The decoder is bounded so a
/// crafted blob can never allocate more than `max_len + 1` bytes before the
/// call errors — a hostile S3 object cannot drive the tiered decode path into
/// an OOM. Mirrors the per-page cap on the local file-format path.
#[cfg(feature = "zstd")]
pub fn decompress_capped(
    data: &[u8],
    decoder_dict: Option<&DecoderDictionary<'static>>,
    max_len: usize,
) -> io::Result<Vec<u8>> {
    use std::io::Read;

    // Read at most max_len + 1 bytes: the extra byte lets us detect overflow
    // without allocating the full bomb.
    let cap = max_len.saturating_add(1);
    let mut output = Vec::new();
    let n = if let Some(decoder_dict) = decoder_dict {
        let decoder = zstd::stream::Decoder::with_prepared_dictionary(data, decoder_dict)
            .map_err(io::Error::other)?;
        decoder.take(cap as u64).read_to_end(&mut output)?
    } else {
        let decoder = zstd::stream::Decoder::new(data)
            .map_err(io::Error::other)?;
        decoder.take(cap as u64).read_to_end(&mut output)?
    };
    if n > max_len {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "decompressed output exceeds cap (possible decompression bomb)",
        ));
    }
    Ok(output)
}

// ===== LZ4 Compression =====

#[cfg(all(feature = "lz4", not(feature = "zstd")))]
pub fn compress(data: &[u8], _level: i32, _: Option<&()>) -> io::Result<Vec<u8>> {
    use lz4_flex::compress_prepend_size;
    Ok(compress_prepend_size(data))
}

#[cfg(all(feature = "lz4", not(feature = "zstd")))]
pub fn decompress(data: &[u8], _: Option<&()>) -> io::Result<Vec<u8>> {
    use lz4_flex::decompress_size_prepended;
    decompress_size_prepended(data).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

/// Decompress with a hard output cap (anti decompression-bomb).
#[cfg(all(feature = "lz4", not(feature = "zstd")))]
pub fn decompress_capped(data: &[u8], _: Option<&()>, max_len: usize) -> io::Result<Vec<u8>> {
    let out = decompress(data, None)?;
    if out.len() > max_len {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "decompressed output exceeds cap (possible decompression bomb)",
        ));
    }
    Ok(out)
}

// ===== Snappy Compression =====

#[cfg(all(feature = "snappy", not(feature = "zstd"), not(feature = "lz4")))]
pub fn compress(data: &[u8], _level: i32, _: Option<&()>) -> io::Result<Vec<u8>> {
    use snap::write::FrameEncoder;
    use std::io::Write;
    let mut encoder = FrameEncoder::new(Vec::new());
    encoder.write_all(data)?;
    match encoder.into_inner() {
        Ok(v) => Ok(v),
        Err(e) => Err(io::Error::new(io::ErrorKind::Other, e.to_string())),
    }
}

#[cfg(all(feature = "snappy", not(feature = "zstd"), not(feature = "lz4")))]
pub fn decompress(data: &[u8], _: Option<&()>) -> io::Result<Vec<u8>> {
    use snap::read::FrameDecoder;
    use std::io::Read;
    let mut decoder = FrameDecoder::new(data);
    let mut output = Vec::new();
    decoder.read_to_end(&mut output)?;
    Ok(output)
}

/// Decompress with a hard output cap (anti decompression-bomb).
#[cfg(all(feature = "snappy", not(feature = "zstd"), not(feature = "lz4")))]
pub fn decompress_capped(data: &[u8], _: Option<&()>, max_len: usize) -> io::Result<Vec<u8>> {
    use snap::read::FrameDecoder;
    use std::io::Read;
    let cap = max_len.saturating_add(1);
    let mut output = Vec::new();
    let n = FrameDecoder::new(data)
        .take(cap as u64)
        .read_to_end(&mut output)?;
    if n > max_len {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "decompressed output exceeds cap (possible decompression bomb)",
        ));
    }
    Ok(output)
}

// ===== Gzip Compression =====

#[cfg(all(
    feature = "gzip",
    not(feature = "zstd"),
    not(feature = "lz4"),
    not(feature = "snappy")
))]
pub fn compress(data: &[u8], level: i32, _: Option<&()>) -> io::Result<Vec<u8>> {
    use flate2::{write::GzEncoder, Compression};
    use std::io::Write;
    let mut encoder = GzEncoder::new(Vec::new(), Compression::new(level as u32));
    encoder.write_all(data)?;
    encoder.finish()
}

#[cfg(all(
    feature = "gzip",
    not(feature = "zstd"),
    not(feature = "lz4"),
    not(feature = "snappy")
))]
pub fn decompress(data: &[u8], _: Option<&()>) -> io::Result<Vec<u8>> {
    use flate2::read::GzDecoder;
    use std::io::Read;
    let mut decoder = GzDecoder::new(data);
    let mut output = Vec::new();
    decoder.read_to_end(&mut output)?;
    Ok(output)
}

/// Decompress with a hard output cap (anti decompression-bomb).
#[cfg(all(
    feature = "gzip",
    not(feature = "zstd"),
    not(feature = "lz4"),
    not(feature = "snappy")
))]
pub fn decompress_capped(data: &[u8], _: Option<&()>, max_len: usize) -> io::Result<Vec<u8>> {
    use flate2::read::GzDecoder;
    use std::io::Read;
    let cap = max_len.saturating_add(1);
    let mut output = Vec::new();
    let n = GzDecoder::new(data)
        .take(cap as u64)
        .read_to_end(&mut output)?;
    if n > max_len {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "decompressed output exceeds cap (possible decompression bomb)",
        ));
    }
    Ok(output)
}

// ===== No Compression (fallback) =====

#[cfg(not(any(
    feature = "zstd",
    feature = "lz4",
    feature = "snappy",
    feature = "gzip"
)))]
pub fn compress(data: &[u8], _level: i32, _: Option<&()>) -> io::Result<Vec<u8>> {
    Ok(data.to_vec())
}

#[cfg(not(any(
    feature = "zstd",
    feature = "lz4",
    feature = "snappy",
    feature = "gzip"
)))]
pub fn decompress(data: &[u8], _: Option<&()>) -> io::Result<Vec<u8>> {
    Ok(data.to_vec())
}

/// Decompress with a hard output cap (anti decompression-bomb).
#[cfg(not(any(
    feature = "zstd",
    feature = "lz4",
    feature = "snappy",
    feature = "gzip"
)))]
pub fn decompress_capped(data: &[u8], _: Option<&()>, max_len: usize) -> io::Result<Vec<u8>> {
    if data.len() > max_len {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "decompressed output exceeds cap (possible decompression bomb)",
        ));
    }
    Ok(data.to_vec())
}

// ===== AES-GCM Encryption with random nonce (nonce-reuse safe) =====
// Output: [12-byte random nonce][ciphertext + 16-byte GCM tag]
// Total overhead: GCM_RANDOM_NONCE_OVERHEAD (28) bytes per frame.
//
// A deterministic page-number-derived GCM nonce was removed: rewriting a page
// reused the nonce, which under GCM both XORs the two plaintexts and (worse)
// leaks the GHASH authentication subkey, enabling tag forgery. Every rewritable
// path must use a fresh random nonce stored inline.

/// Byte overhead of the random-nonce GCM frame: 12-byte nonce + 16-byte tag.
#[cfg(feature = "encryption")]
pub const GCM_RANDOM_NONCE_OVERHEAD: usize = 12 + 16;

/// Encrypt with a fresh random GCM nonce, binding `aad` into the auth tag.
///
/// `aad` is additional authenticated data: not encrypted, not stored inline, but
/// covered by the GCM tag. Decryption only succeeds when the same `aad` is
/// supplied. Callers pass a slot-identity AAD (see `tiered::keys`) so a valid
/// frame cannot be swapped onto another slot. Pass `&[]` when there is no slot
/// to bind (e.g. positional local-cache frames keyed by file offset).
#[cfg(feature = "encryption")]
pub fn encrypt_gcm_random_nonce(data: &[u8], aad: &[u8], key: &[u8; 32]) -> io::Result<Vec<u8>> {
    use aes_gcm::{aead::Aead, aead::KeyInit, aead::Payload, Aes256Gcm, Nonce};
    use rand::RngCore;

    let cipher = Aes256Gcm::new(key.into());
    let mut nonce_bytes = [0u8; 12];
    rand::thread_rng().fill_bytes(&mut nonce_bytes);
    let nonce = Nonce::from_slice(&nonce_bytes);

    let ciphertext = cipher
        .encrypt(nonce, Payload { msg: data, aad })
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Encryption failed: {}", e)))?;

    // Prepend nonce to ciphertext
    let mut result = Vec::with_capacity(12 + ciphertext.len());
    result.extend_from_slice(&nonce_bytes);
    result.extend_from_slice(&ciphertext);
    Ok(result)
}

/// Decrypt a random-nonce GCM frame, authenticating it against `aad`.
///
/// Must be the same `aad` passed to `encrypt_gcm_random_nonce`; a mismatch (e.g.
/// a frame relocated to a different slot) fails the tag check and errors.
#[cfg(feature = "encryption")]
pub fn decrypt_gcm_random_nonce(data: &[u8], aad: &[u8], key: &[u8; 32]) -> io::Result<Vec<u8>> {
    use aes_gcm::{aead::Aead, aead::KeyInit, aead::Payload, Aes256Gcm, Nonce};

    if data.len() < 12 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "encrypted data too short for nonce",
        ));
    }

    let nonce = Nonce::from_slice(&data[..12]);
    let cipher = Aes256Gcm::new(key.into());

    cipher
        .decrypt(
            nonce,
            Payload {
                msg: &data[12..],
                aad,
            },
        )
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Decryption failed: {}", e)))
}

// ===== AES-CTR Encryption (no size overhead) =====
//
// CONTRACT: `nonce` MUST be unique per (key, plaintext-write). CTR reuses the
// keystream whenever the same nonce is reused under the same key, so two writes
// with the same nonce leak `P1 ^ P2`. Callers that rewrite the same logical
// slot MUST pass a fresh random nonce each time (e.g. SubChunkTracker::persist
// generates a random 64-bit nonce and stores it inline). Do NOT pass a purely
// positional value (page number / file offset) for any rewritable target.

#[cfg(feature = "encryption")]
pub fn encrypt_ctr(data: &[u8], nonce: u64, key: &[u8; 32]) -> io::Result<Vec<u8>> {
    use aes::Aes256;
    use ctr::cipher::{KeyIvInit, StreamCipher};
    type Aes256Ctr = ctr::Ctr128BE<Aes256>;

    let mut iv = [0u8; 16];
    iv[0..8].copy_from_slice(&nonce.to_le_bytes());

    let mut cipher = Aes256Ctr::new(key.into(), &iv.into());
    let mut result = data.to_vec();
    cipher.apply_keystream(&mut result);
    Ok(result)
}

#[cfg(feature = "encryption")]
pub fn decrypt_ctr(data: &[u8], nonce: u64, key: &[u8; 32]) -> io::Result<Vec<u8>> {
    // CTR mode: encryption and decryption are the same operation
    encrypt_ctr(data, nonce, key)
}

/// CTR with an explicit keystream byte offset (`skip`). Used by the encrypted
/// passthrough sidecar: a frame is encrypted under `nonce` starting at the
/// write's first byte; a later read of a sub-range seeks the keystream by the
/// in-extent byte offset so partial reads decrypt correctly. Encryption and
/// decryption are the same operation.
#[cfg(feature = "encryption")]
pub fn ctr_xor_at(data: &[u8], nonce: u64, skip: u64, key: &[u8; 32]) -> io::Result<Vec<u8>> {
    use aes::Aes256;
    use ctr::cipher::{KeyIvInit, StreamCipher, StreamCipherSeek};
    type Aes256Ctr = ctr::Ctr128BE<Aes256>;

    let mut iv = [0u8; 16];
    iv[0..8].copy_from_slice(&nonce.to_le_bytes());

    let mut cipher = Aes256Ctr::new(key.into(), &iv.into());
    cipher.seek(skip);
    let mut result = data.to_vec();
    cipher.apply_keystream(&mut result);
    Ok(result)
}

#[cfg(all(test, feature = "encryption"))]
mod crypto_regression_tests {
    //! Regression tests for the page/WAL encryption nonce-reuse fix.
    //! These pin the property that makes in-place rewrites safe: a fresh
    //! random nonce per encryption, so the same plaintext at the same
    //! position never produces the same ciphertext/keystream twice.
    use super::*;

    const KEY: [u8; 32] = [7u8; 32];

    #[test]
    fn gcm_random_nonce_differs_on_rewrite_and_round_trips() {
        // Encrypting the same plaintext twice (a page rewrite) must yield
        // different ciphertext — proves the nonce is fresh per write, not
        // derived from a fixed position. Both must still decrypt back.
        let plain = b"a sqlite page worth of bytes (pretend)".to_vec();
        let c1 = encrypt_gcm_random_nonce(&plain, &[], &KEY).unwrap();
        let c2 = encrypt_gcm_random_nonce(&plain, &[], &KEY).unwrap();
        assert_ne!(
            c1, c2,
            "rewrite must not reuse nonce/ciphertext (two-time pad)"
        );
        assert_eq!(decrypt_gcm_random_nonce(&c1, &[], &KEY).unwrap(), plain);
        assert_eq!(decrypt_gcm_random_nonce(&c2, &[], &KEY).unwrap(), plain);
        // Inline nonce is 12 bytes + 16-byte GCM tag overhead.
        assert_eq!(c1.len(), plain.len() + 12 + 16);
    }

    #[test]
    fn gcm_wrong_key_fails_to_decrypt() {
        let plain = b"secret".to_vec();
        let c = encrypt_gcm_random_nonce(&plain, &[], &KEY).unwrap();
        let wrong = [8u8; 32];
        assert!(decrypt_gcm_random_nonce(&c, &[], &wrong).is_err());
    }

    #[test]
    fn gcm_aad_mismatch_fails_round_trips_when_matched() {
        // A frame bound to one slot's AAD must not decrypt under another's,
        // and must round-trip under its own. This is the swap-prevention
        // property: a valid encrypted blob can't be relocated to a foreign slot.
        // (The slot-identity AAD helpers themselves are exercised in
        // tiered::test_encoding; here we pin the underlying GCM AAD contract.)
        let plain = b"a sqlite page worth of bytes (pretend)".to_vec();
        let aad1 = b"slot-1";
        let aad2 = b"slot-2";
        let c = encrypt_gcm_random_nonce(&plain, aad1, &KEY).unwrap();
        assert!(
            decrypt_gcm_random_nonce(&c, aad2, &KEY).is_err(),
            "frame bound to slot 1 must not decrypt under slot 2's AAD"
        );
        assert_eq!(
            decrypt_gcm_random_nonce(&c, aad1, &KEY).unwrap(),
            plain,
            "frame must round-trip under its own slot AAD"
        );
    }

    #[test]
    fn ctr_distinct_nonces_give_distinct_keystreams() {
        // The WAL passthrough draws a fresh random nonce per write; distinct
        // nonces must yield distinct ciphertext for identical plaintext, so
        // an in-place frame rewrite under a new nonce can't reuse keystream.
        let plain = vec![0xABu8; 64];
        let a = encrypt_ctr(&plain, 1, &KEY).unwrap();
        let b = encrypt_ctr(&plain, 2, &KEY).unwrap();
        assert_ne!(a, b, "different nonces must not reuse keystream");
        // Same nonce round-trips (decrypt == encrypt for CTR).
        assert_eq!(decrypt_ctr(&a, 1, &KEY).unwrap(), plain);
    }

    #[test]
    fn ctr_xor_at_partial_read_seek_matches_full_frame() {
        // A frame is encrypted whole at `skip = 0`; a later sub-range read
        // seeks the keystream by its in-extent byte offset and must decrypt
        // to exactly that slice of the original plaintext.
        let plain: Vec<u8> = (0..200u32).map(|i| i as u8).collect();
        let nonce = 0x1234_5678_9abc_def0u64;
        let whole_ct = ctr_xor_at(&plain, nonce, 0, &KEY).unwrap();
        // Read bytes [50, 90) of the frame: decrypt that ciphertext slice
        // with skip = 50.
        let sub_ct = &whole_ct[50..90];
        let sub_plain = ctr_xor_at(sub_ct, nonce, 50, &KEY).unwrap();
        assert_eq!(sub_plain, &plain[50..90]);
    }
}

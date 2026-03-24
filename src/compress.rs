//! Compression and encryption free functions.
//!
//! These are extracted from CompressedHandle so they can be reused by
//! both CompressedHandle and TieredHandle without duplication.

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
        let mut encoder =
            zstd::stream::Encoder::with_prepared_dictionary(Vec::new(), encoder_dict)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        encoder.write_all(data)?;
        encoder
            .finish()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))
    } else {
        encode_all(data, level).map_err(|e| io::Error::new(io::ErrorKind::Other, e))
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
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        let mut output = Vec::new();
        decoder.read_to_end(&mut output)?;
        Ok(output)
    } else {
        decode_all(data).map_err(|e| io::Error::new(io::ErrorKind::Other, e))
    }
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
    decompress_size_prepended(data)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
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

// ===== No Compression (fallback) =====

#[cfg(not(any(feature = "zstd", feature = "lz4", feature = "snappy", feature = "gzip")))]
pub fn compress(data: &[u8], _level: i32, _: Option<&()>) -> io::Result<Vec<u8>> {
    Ok(data.to_vec())
}

#[cfg(not(any(feature = "zstd", feature = "lz4", feature = "snappy", feature = "gzip")))]
pub fn decompress(data: &[u8], _: Option<&()>) -> io::Result<Vec<u8>> {
    Ok(data.to_vec())
}

// ===== AES-GCM Encryption (for main DB pages — adds 16-byte auth tag) =====

#[cfg(feature = "encryption")]
pub fn encrypt_gcm(data: &[u8], page_num: u64, key: &[u8; 32]) -> io::Result<Vec<u8>> {
    use aes_gcm::{aead::Aead, aead::KeyInit, Aes256Gcm, Nonce};

    let cipher = Aes256Gcm::new(key.into());
    let mut nonce_bytes = [0u8; 12];
    nonce_bytes[0..8].copy_from_slice(&page_num.to_le_bytes());
    let nonce = Nonce::from_slice(&nonce_bytes);

    cipher
        .encrypt(nonce, data)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Encryption failed: {}", e)))
}

#[cfg(feature = "encryption")]
pub fn decrypt_gcm(data: &[u8], page_num: u64, key: &[u8; 32]) -> io::Result<Vec<u8>> {
    use aes_gcm::{aead::Aead, aead::KeyInit, Aes256Gcm, Nonce};

    let cipher = Aes256Gcm::new(key.into());
    let mut nonce_bytes = [0u8; 12];
    nonce_bytes[0..8].copy_from_slice(&page_num.to_le_bytes());
    let nonce = Nonce::from_slice(&nonce_bytes);

    cipher
        .decrypt(nonce, data)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Decryption failed: {}", e)))
}

// ===== AES-GCM Encryption with random nonce (for S3 — nonce-reuse safe) =====
// Output: [12-byte random nonce][ciphertext + 16-byte GCM tag]
// Total overhead: 28 bytes per frame (negligible on ~256KB frames)

#[cfg(feature = "encryption")]
pub fn encrypt_gcm_random_nonce(data: &[u8], key: &[u8; 32]) -> io::Result<Vec<u8>> {
    use aes_gcm::{aead::Aead, aead::KeyInit, Aes256Gcm, Nonce};
    use rand::RngCore;

    let cipher = Aes256Gcm::new(key.into());
    let mut nonce_bytes = [0u8; 12];
    rand::thread_rng().fill_bytes(&mut nonce_bytes);
    let nonce = Nonce::from_slice(&nonce_bytes);

    let ciphertext = cipher
        .encrypt(nonce, data)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Encryption failed: {}", e)))?;

    // Prepend nonce to ciphertext
    let mut result = Vec::with_capacity(12 + ciphertext.len());
    result.extend_from_slice(&nonce_bytes);
    result.extend_from_slice(&ciphertext);
    Ok(result)
}

#[cfg(feature = "encryption")]
pub fn decrypt_gcm_random_nonce(data: &[u8], key: &[u8; 32]) -> io::Result<Vec<u8>> {
    use aes_gcm::{aead::Aead, aead::KeyInit, Aes256Gcm, Nonce};

    if data.len() < 12 {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "encrypted data too short for nonce"));
    }

    let nonce = Nonce::from_slice(&data[..12]);
    let cipher = Aes256Gcm::new(key.into());

    cipher
        .decrypt(nonce, &data[12..])
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Decryption failed: {}", e)))
}

// ===== AES-CTR Encryption (for WAL passthrough — no size overhead) =====

#[cfg(feature = "encryption")]
pub fn encrypt_ctr(data: &[u8], offset: u64, key: &[u8; 32]) -> io::Result<Vec<u8>> {
    use aes::Aes256;
    use ctr::cipher::{KeyIvInit, StreamCipher};
    type Aes256Ctr = ctr::Ctr128BE<Aes256>;

    let mut iv = [0u8; 16];
    iv[0..8].copy_from_slice(&offset.to_le_bytes());

    let mut cipher = Aes256Ctr::new(key.into(), &iv.into());
    let mut result = data.to_vec();
    cipher.apply_keystream(&mut result);
    Ok(result)
}

#[cfg(feature = "encryption")]
pub fn decrypt_ctr(data: &[u8], offset: u64, key: &[u8; 32]) -> io::Result<Vec<u8>> {
    // CTR mode: encryption and decryption are the same operation
    encrypt_ctr(data, offset, key)
}

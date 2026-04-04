//! Dictionary compression support for zstd.
//!
//! Train compression dictionaries from sample data for 5-10x better compression
//! on Redis-like workloads (repeated keys, similar structures).
//!
//! ## Usage
//!
//! ```ignore
//! use sqlite_compress_vfs::dict::train_dictionary;
//!
//! // Collect sample data (e.g., from existing database or logs)
//! let samples: Vec<Vec<u8>> = collect_samples();
//!
//! // Train 100KB dictionary
//! let dict = train_dictionary(&samples, 100 * 1024)?;
//!
//! // Save for later use
//! std::fs::write("app.dict", &dict)?;
//! ```

use std::io;

#[cfg(feature = "zstd")]
use zstd::dict::{from_samples, EncoderDictionary};

/// Train a compression dictionary from sample data.
///
/// # Arguments
/// * `samples` - Representative data samples (ideally 100+ samples)
/// * `dict_size` - Target dictionary size in bytes (typically 100KB-500KB)
///
/// # Returns
/// Trained dictionary bytes that can be saved and reused
///
/// # Example
/// ```ignore
/// let samples = vec![
///     b"user:1234:session:active".to_vec(),
///     b"user:5678:session:expired".to_vec(),
///     // ... more samples
/// ];
/// let dict = train_dictionary(&samples, 100 * 1024)?;
/// ```
#[cfg(feature = "zstd")]
pub fn train_dictionary(samples: &[Vec<u8>], dict_size: usize) -> io::Result<Vec<u8>> {
    let sample_refs: Vec<&[u8]> = samples.iter().map(|s| s.as_slice()).collect();

    from_samples(&sample_refs, dict_size)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Dictionary training failed: {}", e)))
}

/// Train a dictionary from an existing SQLite database.
///
/// This reads all pages from the database and uses them as training samples.
/// Much more convenient than manually collecting samples!
///
/// # Arguments
/// * `db_path` - Path to existing SQLite database
/// * `dict_size` - Target dictionary size in bytes
///
/// # Example
/// ```ignore
/// // Train from existing production database
/// let dict = train_from_database("prod.db", 100 * 1024)?;
/// std::fs::write("prod.dict", &dict)?;
///
/// // Use with new VFS
/// // Pass dictionary via TurboliteConfig.dictionary field
/// ```
#[cfg(feature = "zstd")]
pub fn train_from_database(db_path: &str, dict_size: usize) -> io::Result<Vec<u8>> {
    use std::fs::File;
    use std::io::Read;

    // TODO: Implement actual SQLite page extraction
    // For now, just read chunks of the file
    let mut file = File::open(db_path)?;
    let mut samples = Vec::new();

    let mut buffer = vec![0u8; 4096]; // Standard SQLite page size
    while let Ok(n) = file.read(&mut buffer) {
        if n == 0 {
            break;
        }
        samples.push(buffer[..n].to_vec());
    }

    train_dictionary(&samples, dict_size)
}

/// Compress data using a pre-trained dictionary.
///
/// Dictionaries typically improve compression ratios by 2-5x on structured data.
#[cfg(feature = "zstd")]
pub fn compress_with_dict(data: &[u8], dict: &[u8], level: i32) -> io::Result<Vec<u8>> {
    use std::io::Write;

    let encoder_dict = EncoderDictionary::copy(dict, level);
    let mut encoder = zstd::stream::Encoder::with_prepared_dictionary(Vec::new(), &encoder_dict)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
    encoder.write_all(data)?;
    encoder.finish()
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))
}

/// Decompress data using a dictionary.
#[cfg(feature = "zstd")]
pub fn decompress_with_dict(data: &[u8], dict: &[u8]) -> io::Result<Vec<u8>> {
    use std::io::Read;
    use zstd::dict::DecoderDictionary;

    let decoder_dict = DecoderDictionary::copy(dict);
    let mut decoder = zstd::stream::Decoder::with_prepared_dictionary(data, &decoder_dict)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
    let mut output = Vec::new();
    decoder.read_to_end(&mut output)?;
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[cfg(feature = "zstd")]
    fn test_dictionary_training() {
        // Create sample data with repeated patterns
        let samples: Vec<Vec<u8>> = (0..100)
            .map(|i| format!("user:{}:session:active:timestamp:1234567890", i).into_bytes())
            .collect();

        let dict = train_dictionary(&samples, 1024).unwrap();
        assert!(!dict.is_empty());
        assert!(dict.len() <= 1024);
    }

    #[test]
    #[cfg(feature = "zstd")]
    fn test_compress_decompress_with_dict() {
        let samples: Vec<Vec<u8>> = (0..50)
            .map(|i| format!("key:{}:value:data", i).into_bytes())
            .collect();

        let dict = train_dictionary(&samples, 1024).unwrap();

        let data = b"key:12345:value:test_data";
        let compressed = compress_with_dict(data, &dict, 3).unwrap();
        let decompressed = decompress_with_dict(&compressed, &dict).unwrap();

        assert_eq!(data.as_slice(), decompressed.as_slice());

        // Dictionary compression should be better than raw
        let raw_compressed = zstd::encode_all(&data[..], 3).unwrap();
        assert!(compressed.len() <= raw_compressed.len());
    }
}

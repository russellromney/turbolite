//! Append-only staging log for two-phase checkpoint (Phase Kursk).
//!
//! When `SyncMode::LocalThenFlush`, dirty pages are written to a staging log
//! during `write_all_at()` (alongside the normal disk cache write). The staging
//! log captures exact page contents at checkpoint time, immune to overwrite by
//! subsequent checkpoints.
//!
//! Format: fixed-size records, no framing:
//! ```text
//! [page_num: u64 LE][page_data: [u8; page_size]]
//! ```
//!
//! `flush_to_s3()` reads from staging logs instead of the live disk cache,
//! guaranteeing it uploads the exact state that was checkpointed.

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

/// A single pending flush: staging log path + metadata recorded by sync().
#[derive(Debug)]
pub(crate) struct PendingFlush {
    /// Path to the staging log file.
    pub staging_path: PathBuf,
    /// Manifest version at checkpoint time.
    pub version: u64,
    /// Page size at checkpoint time (for decoding the fixed-size records).
    pub page_size: u32,
}

/// Writer for a staging log file. Opened lazily on first dirty write,
/// closed + fsynced in sync().
pub(crate) struct StagingWriter {
    writer: BufWriter<File>,
    path: PathBuf,
    page_size: u32,
    pages_written: u64,
}

impl StagingWriter {
    /// Open a new staging log for writing.
    pub fn open(staging_dir: &Path, version: u64, page_size: u32) -> io::Result<Self> {
        fs::create_dir_all(staging_dir)?;
        let path = staging_dir.join(format!("{}.log", version));
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&path)?;
        Ok(Self {
            writer: BufWriter::with_capacity(256 * 1024, file),
            path,
            page_size,
            pages_written: 0,
        })
    }

    /// Append a page to the staging log.
    /// Format: [page_num: u64 LE][page_data: [u8; page_size]]
    pub fn append(&mut self, page_num: u64, data: &[u8]) -> io::Result<()> {
        debug_assert_eq!(
            data.len(),
            self.page_size as usize,
            "staging append: data.len()={} != page_size={}",
            data.len(),
            self.page_size,
        );
        self.writer.write_all(&page_num.to_le_bytes())?;
        self.writer.write_all(data)?;
        self.pages_written += 1;
        Ok(())
    }

    /// Flush buffers, fsync, and return the path for PendingFlush.
    pub fn finalize(mut self) -> io::Result<(PathBuf, u64)> {
        self.writer.flush()?;
        self.writer.get_ref().sync_all()?;
        Ok((self.path, self.pages_written))
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn pages_written(&self) -> u64 {
        self.pages_written
    }
}

/// Read all pages from a staging log into a HashMap.
/// Returns page_num -> page_data.
pub(crate) fn read_staging_log(
    path: &Path,
    page_size: u32,
) -> io::Result<HashMap<u64, Vec<u8>>> {
    let file = File::open(path)?;
    let file_len = file.metadata()?.len();
    let record_size = 8 + page_size as u64;

    if file_len == 0 {
        return Ok(HashMap::new());
    }

    if file_len % record_size != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "staging log {} has invalid size {} (not a multiple of record_size {})",
                path.display(),
                file_len,
                record_size,
            ),
        ));
    }

    let num_records = (file_len / record_size) as usize;
    let mut pages = HashMap::with_capacity(num_records);
    let mut reader = BufReader::with_capacity(256 * 1024, file);
    let mut page_num_buf = [0u8; 8];
    let mut page_buf = vec![0u8; page_size as usize];

    for _ in 0..num_records {
        reader.read_exact(&mut page_num_buf)?;
        reader.read_exact(&mut page_buf)?;
        let page_num = u64::from_le_bytes(page_num_buf);
        // Later writes for the same page_num win (HashMap::insert overwrites).
        // This is correct: if the same page was written multiple times in one
        // checkpoint, the last write is the committed state.
        pages.insert(page_num, page_buf.clone());
    }

    Ok(pages)
}

/// Scan staging directory for leftover .log files from interrupted flushes.
/// Returns PendingFlush entries sorted by version (oldest first).
pub(crate) fn recover_staging_logs(
    staging_dir: &Path,
    page_size: u32,
) -> io::Result<Vec<PendingFlush>> {
    if !staging_dir.exists() {
        return Ok(Vec::new());
    }

    let mut pending = Vec::new();
    for entry in fs::read_dir(staging_dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("log") {
            continue;
        }
        // Parse version from filename: "{version}.log"
        let stem = path.file_stem().and_then(|s| s.to_str()).unwrap_or("");
        let version: u64 = match stem.parse() {
            Ok(v) => v,
            Err(_) => {
                eprintln!(
                    "[staging] WARN: ignoring non-numeric staging file: {}",
                    path.display()
                );
                continue;
            }
        };
        // Validate file is non-empty and well-formed
        let meta = entry.metadata()?;
        if meta.len() == 0 {
            eprintln!(
                "[staging] removing empty staging file: {}",
                path.display()
            );
            let _ = fs::remove_file(&path);
            continue;
        }
        let record_size = 8 + page_size as u64;
        if meta.len() % record_size != 0 {
            eprintln!(
                "[staging] WARN: staging file {} has invalid size {} (record_size={}), skipping",
                path.display(),
                meta.len(),
                record_size,
            );
            continue;
        }
        pending.push(PendingFlush {
            staging_path: path,
            version,
            page_size,
        });
    }

    // Sort by version: process oldest first
    pending.sort_by_key(|p| p.version);
    Ok(pending)
}

/// Delete a staging log file after successful upload.
pub(crate) fn remove_staging_log(path: &Path) {
    if let Err(e) = fs::remove_file(path) {
        eprintln!(
            "[staging] WARN: failed to remove staging file {}: {}",
            path.display(),
            e,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn staging_write_read_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let staging_dir = dir.path().join("staging");
        let page_size = 4096u32;

        // Write 3 pages
        let mut writer = StagingWriter::open(&staging_dir, 1, page_size).unwrap();
        let page0 = vec![0xAAu8; page_size as usize];
        let page5 = vec![0xBBu8; page_size as usize];
        let page10 = vec![0xCCu8; page_size as usize];
        writer.append(0, &page0).unwrap();
        writer.append(5, &page5).unwrap();
        writer.append(10, &page10).unwrap();
        let (path, count) = writer.finalize().unwrap();
        assert_eq!(count, 3);

        // Read back
        let pages = read_staging_log(&path, page_size).unwrap();
        assert_eq!(pages.len(), 3);
        assert_eq!(pages[&0], page0);
        assert_eq!(pages[&5], page5);
        assert_eq!(pages[&10], page10);
    }

    #[test]
    fn staging_duplicate_page_last_write_wins() {
        let dir = tempfile::tempdir().unwrap();
        let staging_dir = dir.path().join("staging");
        let page_size = 64u32; // small for test

        let mut writer = StagingWriter::open(&staging_dir, 1, page_size).unwrap();
        let first = vec![0x11u8; page_size as usize];
        let second = vec![0x22u8; page_size as usize];
        writer.append(7, &first).unwrap();
        writer.append(7, &second).unwrap();
        let (path, count) = writer.finalize().unwrap();
        assert_eq!(count, 2);

        let pages = read_staging_log(&path, page_size).unwrap();
        assert_eq!(pages.len(), 1);
        assert_eq!(pages[&7], second); // last write wins
    }

    #[test]
    fn staging_empty_log() {
        let dir = tempfile::tempdir().unwrap();
        let staging_dir = dir.path().join("staging");
        let page_size = 4096u32;

        let writer = StagingWriter::open(&staging_dir, 1, page_size).unwrap();
        let (path, count) = writer.finalize().unwrap();
        assert_eq!(count, 0);

        let pages = read_staging_log(&path, page_size).unwrap();
        assert!(pages.is_empty());
    }

    #[test]
    fn staging_recover_multiple_logs() {
        let dir = tempfile::tempdir().unwrap();
        let staging_dir = dir.path().join("staging");
        let page_size = 64u32;

        // Create 3 staging logs with different versions
        for version in [5, 2, 10] {
            let mut writer = StagingWriter::open(&staging_dir, version, page_size).unwrap();
            writer.append(0, &vec![version as u8; page_size as usize]).unwrap();
            writer.finalize().unwrap();
        }

        let recovered = recover_staging_logs(&staging_dir, page_size).unwrap();
        assert_eq!(recovered.len(), 3);
        // Sorted by version ascending
        assert_eq!(recovered[0].version, 2);
        assert_eq!(recovered[1].version, 5);
        assert_eq!(recovered[2].version, 10);
    }

    #[test]
    fn staging_recover_skips_empty_and_invalid() {
        let dir = tempfile::tempdir().unwrap();
        let staging_dir = dir.path().join("staging");
        fs::create_dir_all(&staging_dir).unwrap();
        let page_size = 64u32;

        // Valid log
        let mut writer = StagingWriter::open(&staging_dir, 1, page_size).unwrap();
        writer.append(0, &vec![0xAAu8; page_size as usize]).unwrap();
        writer.finalize().unwrap();

        // Empty log (will be removed)
        File::create(staging_dir.join("2.log")).unwrap();

        // Invalid size log
        let mut f = File::create(staging_dir.join("3.log")).unwrap();
        f.write_all(&[0u8; 5]).unwrap(); // not a multiple of record_size

        // Non-numeric filename
        File::create(staging_dir.join("abc.log")).unwrap();

        // Non-log file
        File::create(staging_dir.join("4.txt")).unwrap();

        let recovered = recover_staging_logs(&staging_dir, page_size).unwrap();
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].version, 1);
    }

    #[test]
    fn staging_recover_nonexistent_dir() {
        let dir = tempfile::tempdir().unwrap();
        let staging_dir = dir.path().join("does_not_exist");
        let recovered = recover_staging_logs(&staging_dir, 4096).unwrap();
        assert!(recovered.is_empty());
    }

    #[test]
    fn staging_invalid_file_size_error() {
        let dir = tempfile::tempdir().unwrap();
        let staging_dir = dir.path().join("staging");
        fs::create_dir_all(&staging_dir).unwrap();
        let path = staging_dir.join("1.log");
        let mut f = File::create(&path).unwrap();
        // Write partial record (5 bytes, not a multiple of 8 + page_size)
        f.write_all(&[0u8; 5]).unwrap();

        let result = read_staging_log(&path, 64);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("invalid size"));
    }
}

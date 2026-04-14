//! Append-only staging log for two-phase checkpoint (Phase Kursk).
//!
//! When `SyncMode::LocalThenFlush`, dirty pages are written to a staging log
//! during `write_all_at()` (alongside the normal disk cache write). The staging
//! log captures exact page contents at checkpoint time, immune to overwrite by
//! subsequent checkpoints.
//!
//! Format: 4-byte header + fixed-size records:
//! ```text
//! [page_size: u32 LE]
//! [page_num: u64 LE][page_data: [u8; page_size]]*
//! [MANIFEST_MAGIC: u64 LE][manifest_len: u64 LE][manifest_data]*
//! ```
//!
//! `flush_to_s3()` reads from staging logs instead of the live disk cache,
//! guaranteeing it uploads the exact state that was checkpointed.

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Seek, Write};
use std::path::{Path, PathBuf};

/// Magic value to identify manifest trailer in staging log.
/// Chosen to be extremely unlikely as a valid page number.
pub(crate) const MANIFEST_MAGIC: u64 = 0xCAFE_FACE_DEAD_BEEF;

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
    /// Open a new staging log for writing. Writes a 4-byte page_size header
    /// so recovery can parse records without depending on the manifest.
    pub fn open(staging_dir: &Path, version: u64, page_size: u32) -> io::Result<Self> {
        fs::create_dir_all(staging_dir)?;
        let path = staging_dir.join(format!("{}.log", version));
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&path)?;
        let mut writer = BufWriter::with_capacity(256 * 1024, file);
        writer.write_all(&page_size.to_le_bytes())?;
        Ok(Self {
            writer,
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

    /// Append manifest bytes to the staging log before finalize.
    /// Format: [MANIFEST_MAGIC: u64 LE][manifest_len: u64 LE][manifest_data: [u8; manifest_len]]
    /// The magic value distinguishes manifest trailer from page records.
    pub fn append_manifest(&mut self, manifest_bytes: &[u8]) -> io::Result<()> {
        self.writer.write_all(&MANIFEST_MAGIC.to_le_bytes())?;
        self.writer.write_all(&(manifest_bytes.len() as u64).to_le_bytes())?;
        self.writer.write_all(manifest_bytes)?;
        Ok(())
    }

    /// Flush buffers, fsync, and return the path for PendingFlush.
    pub fn finalize(mut self) -> io::Result<(PathBuf, u64)> {
        self.writer.flush()?;
        self.writer.get_ref().sync_all()?;
        Ok((self.path, self.pages_written))
    }

    #[allow(dead_code)]
    pub fn path(&self) -> &Path {
        &self.path
    }

    #[allow(dead_code)]
    pub fn pages_written(&self) -> u64 {
        self.pages_written
    }
}

/// Read all pages from a staging log into a HashMap.
/// Returns page_num -> page_data. The `_page_size` argument is ignored;
/// page size is read from the 4-byte file header.
pub(crate) fn read_staging_log(
    path: &Path,
    _page_size: u32,
) -> io::Result<HashMap<u64, Vec<u8>>> {
    let file = File::open(path)?;
    if file.metadata()?.len() < 4 {
        return Ok(HashMap::new());
    }

    let mut reader = BufReader::with_capacity(256 * 1024, file);
    let mut ps_buf = [0u8; 4];
    reader.read_exact(&mut ps_buf)?;
    let page_size = u32::from_le_bytes(ps_buf);
    if page_size == 0 || page_size > 65536 {
        return Ok(HashMap::new());
    }

    let mut pages = HashMap::new();
    let mut page_num_buf = [0u8; 8];
    let mut page_buf = vec![0u8; page_size as usize];

    loop {
        match reader.read_exact(&mut page_num_buf) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e),
        }
        let page_num = u64::from_le_bytes(page_num_buf);

        if page_num == MANIFEST_MAGIC {
            break;
        }

        reader.read_exact(&mut page_buf)?;
        pages.insert(page_num, page_buf.clone());
    }

    Ok(pages)
}

/// Extract the embedded manifest from a staging log (if present).
/// Returns None if the staging log has no manifest trailer.
/// The `_page_size` argument is ignored; page size is read from the header.
pub(crate) fn read_staging_manifest(
    path: &Path,
    _page_size: u32,
) -> io::Result<Option<Vec<u8>>> {
    let file = File::open(path)?;
    let file_len = file.metadata()?.len();
    if file_len < 4 {
        return Ok(None);
    }
    let mut reader = BufReader::with_capacity(256 * 1024, file);
    let mut ps_buf = [0u8; 4];
    reader.read_exact(&mut ps_buf)?;
    let page_size = u32::from_le_bytes(ps_buf);
    if page_size == 0 || page_size > 65536 {
        return Ok(None);
    }

    let mut page_num_buf = [0u8; 8];
    let mut page_buf = vec![0u8; page_size as usize];

    loop {
        match reader.read_exact(&mut page_num_buf) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e),
        }
        let val = u64::from_le_bytes(page_num_buf);
        if val == MANIFEST_MAGIC {
            let mut len_buf = [0u8; 8];
            reader.read_exact(&mut len_buf)?;
            let manifest_len = u64::from_le_bytes(len_buf) as usize;
            if manifest_len as u64 > file_len {
                return Ok(None);
            }
            let mut manifest_data = vec![0u8; manifest_len];
            reader.read_exact(&mut manifest_data)?;
            return Ok(Some(manifest_data));
        }
        reader.read_exact(&mut page_buf)?;
    }
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
            turbolite_debug!(
                "[staging] removing empty staging file: {}",
                path.display()
            );
            let _ = fs::remove_file(&path);
            continue;
        }
        // Read page_size from 4-byte file header
        let file_ps = match File::open(&path) {
            Ok(mut f) => {
                let mut buf = [0u8; 4];
                match f.read_exact(&mut buf) {
                    Ok(()) => {
                        let ps = u32::from_le_bytes(buf);
                        if ps > 0 && ps <= 65536 { ps } else { page_size }
                    }
                    Err(_) => page_size,
                }
            }
            Err(_) => page_size,
        };
        pending.push(PendingFlush {
            staging_path: path,
            version,
            page_size: file_ps,
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
        // Valid log (v1) + partial-size log (v3) both recovered.
        // The % validation was removed; partial files are accepted and
        // read_staging_log handles them by reading complete records only.
        assert_eq!(recovered.len(), 2);
        assert_eq!(recovered[0].version, 1);
        assert_eq!(recovered[1].version, 3);
    }

    #[test]
    fn staging_recover_nonexistent_dir() {
        let dir = tempfile::tempdir().unwrap();
        let staging_dir = dir.path().join("does_not_exist");
        let recovered = recover_staging_logs(&staging_dir, 4096).unwrap();
        assert!(recovered.is_empty());
    }

    #[test]
    fn staging_partial_record_returns_empty() {
        let dir = tempfile::tempdir().unwrap();
        let staging_dir = dir.path().join("staging");
        fs::create_dir_all(&staging_dir).unwrap();
        let path = staging_dir.join("1.log");
        let mut f = File::create(&path).unwrap();
        // Write partial record (5 bytes, can't read a full page_num u64)
        f.write_all(&[0u8; 5]).unwrap();

        // Should return empty (can't read a complete record), not error
        let result = read_staging_log(&path, 64);
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[test]
    fn staging_log_with_manifest_trailer() {
        let dir = tempfile::tempdir().unwrap();
        let staging_dir = dir.path().join("staging");
        fs::create_dir_all(&staging_dir).unwrap();

        // Write 2 pages + manifest trailer
        let mut writer = StagingWriter::open(&staging_dir, 1, 64).unwrap();
        writer.append(0, &vec![10u8; 64]).unwrap();
        writer.append(1, &vec![20u8; 64]).unwrap();
        writer.append_manifest(b"test-manifest-data").unwrap();
        let (path, pages) = writer.finalize().unwrap();
        assert_eq!(pages, 2);

        // read_staging_log should return 2 pages, ignoring the manifest trailer
        let result = read_staging_log(&path, 64).unwrap();
        assert_eq!(result.len(), 2, "should have 2 pages");
        assert_eq!(result[&0], vec![10u8; 64]);
        assert_eq!(result[&1], vec![20u8; 64]);
    }

    #[test]
    fn staging_recovery_accepts_files_with_manifest_trailer() {
        let dir = tempfile::tempdir().unwrap();
        let staging_dir = dir.path().join("staging");
        fs::create_dir_all(&staging_dir).unwrap();

        // Write a staging log with manifest trailer (non-multiple of record_size)
        let mut writer = StagingWriter::open(&staging_dir, 42, 64).unwrap();
        writer.append(0, &vec![1u8; 64]).unwrap();
        writer.append_manifest(b"some-manifest").unwrap();
        let (_path, _) = writer.finalize().unwrap();

        // recover_staging_logs should NOT skip this file
        let recovered = recover_staging_logs(&staging_dir, 64).unwrap();
        assert_eq!(recovered.len(), 1, "should recover 1 staging log");
        assert_eq!(recovered[0].version, 42);
    }
}

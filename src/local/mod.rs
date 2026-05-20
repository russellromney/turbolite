//! Single-file compressed local databases — the simplest turbolite mode.
//!
//! `open_local` returns a normal [`rusqlite::Connection`] backed by one
//! compressed file on disk. There is no manifest, no page-group sidecar,
//! no staging directory, no tokio, and no remote storage. SQLite's own
//! pager and rollback journal stay on; the journal is held in memory so
//! that, at rest, the database is exactly one file.
//!
//! ```ignore
//! let conn = turbolite::open_local("mydata.db")?;
//! conn.execute_batch("CREATE TABLE t(x); INSERT INTO t VALUES (1);")?;
//! ```
//!
//! # How it stays one file
//!
//! The whole database is kept as a flat byte image in memory while open.
//! Reads and writes hit that image directly. On `sync` (i.e. transaction
//! commit) the image is chunked into pages, each page is compressed (and
//! optionally encrypted), and the result is written to a temp file that is
//! atomically renamed over the target. A crash therefore leaves either the
//! previous complete database or the next one — never a torn file. Because
//! the on-disk file is always a complete snapshot, SQLite's rollback
//! journal never needs to survive a crash, so it lives in memory.
//!
//! See [`file_format`] for the byte layout.

pub mod file_format;

use std::borrow::Cow;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use sqlite_vfs::{
    DatabaseHandle, LockKind, OpenKind, OpenOptions, Vfs, WalDisabled,
};

use file_format::{
    decode_directory, decode_page, encode_directory, encode_page, Header, PageCodec,
    DEFAULT_PAGE_SIZE, FORMAT_VERSION, HEADER_LEN,
};

/// The tiered (cloud/replicated) mode keeps its metadata in this sidecar
/// file next to the database. `open_local` refuses to open a database that
/// has one, because that database belongs to the tiered VFS, not here.
const TIERED_SIDECAR: &str = "local_state.msgpack";

/// Options controlling how a local database file is stored.
#[derive(Clone)]
pub struct LocalOptions {
    /// Storage chunk size, in bytes, used for per-page compression. Does
    /// not have to match SQLite's page size, but matching it (4096) gives
    /// the best ratio. Adopted from the file header when reopening.
    pub page_size: u32,
    /// Compress pages with zstd. When false, pages are stored raw.
    pub compress: bool,
    /// zstd compression level.
    pub level: i32,
    /// Optional 32-byte key. When set, page payloads are encrypted with
    /// AES-256-CTR. Requires the `encryption` feature.
    pub encryption_key: Option<[u8; 32]>,
}

impl Default for LocalOptions {
    fn default() -> Self {
        LocalOptions {
            page_size: DEFAULT_PAGE_SIZE,
            compress: true,
            level: 3,
            encryption_key: None,
        }
    }
}

impl LocalOptions {
    fn codec(&self) -> PageCodec {
        PageCodec {
            compress: self.compress,
            level: self.level,
            key: self.encryption_key,
        }
    }
}

/// Errors from opening a local database.
#[derive(Debug)]
pub enum LocalError {
    /// The path (or its directory) already belongs to a tiered turbolite
    /// database — opening it as a single-file local DB would corrupt it.
    TieredSidecarPresent(PathBuf),
    /// An I/O error preparing the file or VFS.
    Io(io::Error),
    /// An error from SQLite while opening the connection.
    Sqlite(rusqlite::Error),
}

impl std::fmt::Display for LocalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LocalError::TieredSidecarPresent(p) => write!(
                f,
                "{} looks like a tiered turbolite database (found {TIERED_SIDECAR}); \
                 use the tiered VFS / Builder, not open_local",
                p.display()
            ),
            LocalError::Io(e) => write!(f, "local db I/O error: {e}"),
            LocalError::Sqlite(e) => write!(f, "local db sqlite error: {e}"),
        }
    }
}

impl std::error::Error for LocalError {}

impl From<io::Error> for LocalError {
    fn from(e: io::Error) -> Self {
        LocalError::Io(e)
    }
}

impl From<rusqlite::Error> for LocalError {
    fn from(e: rusqlite::Error) -> Self {
        LocalError::Sqlite(e)
    }
}

/// Open (or create) a single-file compressed local database with default
/// options (4096-byte pages, zstd level 3, no encryption).
pub fn open_local<P: AsRef<Path>>(path: P) -> Result<rusqlite::Connection, LocalError> {
    open_local_with(path, LocalOptions::default())
}

/// Open (or create) a single-file compressed local database with explicit
/// options.
pub fn open_local_with<P: AsRef<Path>>(
    path: P,
    options: LocalOptions,
) -> Result<rusqlite::Connection, LocalError> {
    let path = path.as_ref().to_path_buf();
    refuse_if_tiered(&path)?;

    let vfs = LocalCompressedVfs {
        path: path.clone(),
        codec: options.codec(),
        page_size: options.page_size,
    };

    let vfs_name = unique_vfs_name();
    sqlite_vfs::register(&vfs_name, vfs, false)
        .map_err(|e| LocalError::Io(io::Error::new(io::ErrorKind::Other, format!("{e:?}"))))?;

    let flags = rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
        | rusqlite::OpenFlags::SQLITE_OPEN_CREATE;
    let path_str = path
        .to_str()
        .ok_or_else(|| LocalError::Io(io::Error::new(io::ErrorKind::InvalidInput, "non-utf8 path")))?;
    let conn = rusqlite::Connection::open_with_flags_and_vfs(path_str, flags, &vfs_name)?;
    Ok(conn)
}

/// Refuse to open a path that belongs to a tiered database: either the
/// path itself is a directory holding the sidecar, or the sidecar sits
/// alongside the file.
fn refuse_if_tiered(path: &Path) -> Result<(), LocalError> {
    if path.is_dir() && path.join(TIERED_SIDECAR).exists() {
        return Err(LocalError::TieredSidecarPresent(path.to_path_buf()));
    }
    if let Some(parent) = path.parent() {
        let sidecar = parent.join(TIERED_SIDECAR);
        if sidecar.exists() {
            return Err(LocalError::TieredSidecarPresent(sidecar));
        }
    }
    Ok(())
}

fn unique_vfs_name() -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("turbolite_local_{}_{}", std::process::id(), n)
}

/// A file-first VFS: it serves exactly one database path. The main
/// database is stored compressed; SQLite's journal/temp files are served
/// from memory and never touch disk.
struct LocalCompressedVfs {
    path: PathBuf,
    codec: PageCodec,
    page_size: u32,
}

impl LocalCompressedVfs {
    fn is_main(&self, db: &str) -> bool {
        Path::new(db) == self.path
    }
}

impl Vfs for LocalCompressedVfs {
    type Handle = LocalHandle;

    fn open(&self, db: &str, opts: OpenOptions) -> Result<LocalHandle, io::Error> {
        if opts.kind == OpenKind::MainDb && self.is_main(db) {
            let main = MainHandle::open(self.path.clone(), self.codec.clone(), self.page_size)?;
            Ok(LocalHandle::Main(main))
        } else {
            // Journal, temp, and any non-main file lives in memory only.
            Ok(LocalHandle::Mem(MemHandle::default()))
        }
    }

    fn delete(&self, db: &str) -> Result<(), io::Error> {
        if self.is_main(db) {
            match std::fs::remove_file(&self.path) {
                Ok(()) => Ok(()),
                Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
                Err(e) => Err(e),
            }
        } else {
            // In-memory transient file: deleting is a no-op.
            Ok(())
        }
    }

    fn exists(&self, db: &str) -> Result<bool, io::Error> {
        if self.is_main(db) {
            Ok(self.path.exists())
        } else {
            // Transient files never persist, so none ever "exist" between
            // opens. This is what tells SQLite there is no hot journal.
            Ok(false)
        }
    }

    fn temporary_name(&self) -> String {
        format!("turbolite-local-temp-{}", unique_vfs_name())
    }

    fn random(&self, buffer: &mut [i8]) {
        // Cheap, dependency-free PRNG seeded from time + address. The VFS
        // only uses this for temp names and SQLite's rollback salt.
        let mut x = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0x9E37_79B9_7F4A_7C15)
            ^ (buffer.as_ptr() as u64);
        for slot in buffer.iter_mut() {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            *slot = (x & 0xff) as i8;
        }
    }

    fn sleep(&self, duration: Duration) -> Duration {
        std::thread::sleep(duration);
        duration
    }

    fn full_pathname<'a>(&self, db: &'a str) -> Result<Cow<'a, str>, io::Error> {
        Ok(Cow::Borrowed(db))
    }
}

/// One handle, either the compressed main database or an in-memory
/// transient file.
pub enum LocalHandle {
    Main(MainHandle),
    Mem(MemHandle),
}

/// The compressed main database, held as a flat byte image while open.
pub struct MainHandle {
    path: PathBuf,
    codec: PageCodec,
    page_size: u32,
    image: Vec<u8>,
    dirty: bool,
    lock: LockKind,
}

impl MainHandle {
    fn open(path: PathBuf, codec: PageCodec, page_size: u32) -> Result<Self, io::Error> {
        let (image, page_size) = match std::fs::read(&path) {
            Ok(bytes) if bytes.is_empty() => (Vec::new(), page_size),
            Ok(bytes) => decode_file(&bytes, &codec)?,
            Err(e) if e.kind() == io::ErrorKind::NotFound => (Vec::new(), page_size),
            Err(e) => return Err(e),
        };
        Ok(MainHandle {
            path,
            codec,
            page_size,
            image,
            dirty: false,
            lock: LockKind::None,
        })
    }

    /// Chunk the image into pages, compress/encrypt each, and atomically
    /// replace the on-disk file. A crash mid-write leaves the previous
    /// file intact (the rename is atomic).
    fn persist(&mut self) -> Result<(), io::Error> {
        if !self.dirty {
            return Ok(());
        }
        let page_size = self.page_size as usize;
        let mut data: Vec<u8> = Vec::new();
        let mut directory: Vec<(u64, u32)> = Vec::new();
        let mut page_index: u64 = 0;
        for chunk in self.image.chunks(page_size) {
            let nonce = page_index * self.page_size as u64;
            let blob = encode_page(chunk, &self.codec, nonce)?;
            let offset = (HEADER_LEN + data.len()) as u64;
            let len = u32::try_from(blob.len()).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "page blob too large")
            })?;
            data.extend_from_slice(&blob);
            directory.push((offset, len));
            page_index += 1;
        }
        let directory_offset = (HEADER_LEN + data.len()) as u64;
        let header = Header {
            format_version: FORMAT_VERSION,
            flags: self.codec.header_flags(),
            page_size: self.page_size,
            page_count: directory.len() as u64,
            directory_offset,
        };

        let mut file_bytes = Vec::with_capacity(directory_offset as usize + directory.len() * 12);
        file_bytes.extend_from_slice(&header.encode());
        file_bytes.extend_from_slice(&data);
        file_bytes.extend_from_slice(&encode_directory(&directory));

        atomic_write(&self.path, &file_bytes)?;
        self.dirty = false;
        Ok(())
    }
}

/// Decode a complete TLLOCAL1 file into its flat image. Returns the image
/// and the page size recorded in the header.
fn decode_file(bytes: &[u8], codec: &PageCodec) -> Result<(Vec<u8>, u32), io::Error> {
    let header = Header::decode(bytes)?;
    let dir_off = header.directory_offset as usize;
    if dir_off > bytes.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "directory offset past end of file",
        ));
    }
    let directory = decode_directory(&bytes[dir_off..], header.page_count as usize)?;
    let mut image = Vec::with_capacity(header.page_count as usize * header.page_size as usize);
    for (i, (offset, len)) in directory.iter().enumerate() {
        let start = *offset as usize;
        let end = start
            .checked_add(*len as usize)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "page extent overflow"))?;
        if end > bytes.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "page blob extends past end of file",
            ));
        }
        let nonce = i as u64 * header.page_size as u64;
        let page = decode_page(&bytes[start..end], codec, nonce)?;
        image.extend_from_slice(&page);
    }
    Ok((image, header.page_size))
}

/// Write `bytes` to `path` atomically: temp file, fsync, rename, fsync dir.
fn atomic_write(path: &Path, bytes: &[u8]) -> Result<(), io::Error> {
    use std::io::Write;
    let tmp = tmp_path(path);
    {
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(bytes)?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, path)?;
    if let Some(parent) = path.parent() {
        // Best-effort directory fsync so the rename is durable. Failure
        // here is non-fatal (some platforms reject opening a dir).
        if let Ok(dir) = std::fs::File::open(parent) {
            let _ = dir.sync_all();
        }
    }
    Ok(())
}

fn tmp_path(path: &Path) -> PathBuf {
    let mut name = path.file_name().map(|n| n.to_os_string()).unwrap_or_default();
    name.push(format!(".tmp-local-{}", std::process::id()));
    match path.parent() {
        Some(parent) => parent.join(name),
        None => PathBuf::from(name),
    }
}

/// Read `buf` from a flat byte image at `offset`. Fills the available
/// prefix and reports `UnexpectedEof` if the read runs past the end, which
/// SQLite handles by zero-filling the tail (a short read).
fn image_read(image: &[u8], buf: &mut [u8], offset: u64) -> Result<(), io::Error> {
    let start = offset as usize;
    if start >= image.len() {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "read past end"));
    }
    let avail = &image[start..];
    let n = avail.len().min(buf.len());
    buf[..n].copy_from_slice(&avail[..n]);
    if n < buf.len() {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "short read"));
    }
    Ok(())
}

/// Write `buf` into a flat byte image at `offset`, extending with zeros as
/// needed.
fn image_write(image: &mut Vec<u8>, buf: &[u8], offset: u64) {
    let start = offset as usize;
    let end = start + buf.len();
    if end > image.len() {
        image.resize(end, 0);
    }
    image[start..end].copy_from_slice(buf);
}

/// An in-memory transient file (rollback journal / temp db).
#[derive(Default)]
pub struct MemHandle {
    buf: Vec<u8>,
    lock: LockKind,
}

impl DatabaseHandle for LocalHandle {
    type WalIndex = WalDisabled;

    fn size(&self) -> Result<u64, io::Error> {
        match self {
            LocalHandle::Main(h) => Ok(h.image.len() as u64),
            LocalHandle::Mem(h) => Ok(h.buf.len() as u64),
        }
    }

    fn read_exact_at(&mut self, buf: &mut [u8], offset: u64) -> Result<(), io::Error> {
        match self {
            LocalHandle::Main(h) => image_read(&h.image, buf, offset),
            LocalHandle::Mem(h) => image_read(&h.buf, buf, offset),
        }
    }

    fn write_all_at(&mut self, buf: &[u8], offset: u64) -> Result<(), io::Error> {
        match self {
            LocalHandle::Main(h) => {
                image_write(&mut h.image, buf, offset);
                h.dirty = true;
                Ok(())
            }
            LocalHandle::Mem(h) => {
                image_write(&mut h.buf, buf, offset);
                Ok(())
            }
        }
    }

    fn sync(&mut self, _data_only: bool) -> Result<(), io::Error> {
        match self {
            LocalHandle::Main(h) => h.persist(),
            LocalHandle::Mem(_) => Ok(()),
        }
    }

    fn set_len(&mut self, size: u64) -> Result<(), io::Error> {
        let size = size as usize;
        match self {
            LocalHandle::Main(h) => {
                h.image.resize(size, 0);
                h.dirty = true;
                Ok(())
            }
            LocalHandle::Mem(h) => {
                h.buf.resize(size, 0);
                Ok(())
            }
        }
    }

    fn lock(&mut self, lock: LockKind) -> Result<bool, io::Error> {
        // Single-process embedded file: locking is bookkeeping only.
        match self {
            LocalHandle::Main(h) => h.lock = lock,
            LocalHandle::Mem(h) => h.lock = lock,
        }
        Ok(true)
    }

    fn unlock(&mut self, lock: LockKind) -> Result<bool, io::Error> {
        self.lock(lock)
    }

    fn reserved(&mut self) -> Result<bool, io::Error> {
        Ok(false)
    }

    fn current_lock(&self) -> Result<LockKind, io::Error> {
        Ok(match self {
            LocalHandle::Main(h) => h.lock,
            LocalHandle::Mem(h) => h.lock,
        })
    }

    fn wal_index(&self, _readonly: bool) -> Result<Self::WalIndex, io::Error> {
        Ok(WalDisabled)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir() -> std::path::PathBuf {
        let p = std::env::temp_dir().join(format!(
            "turbolite-l1-{}-{}",
            std::process::id(),
            unique_vfs_name()
        ));
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    #[test]
    fn l1_lifecycle_persists_across_reopen() {
        let dir = temp_dir();
        let db = dir.join("data.db");

        {
            let conn = open_local(&db).expect("open");
            conn.execute_batch(
                "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
                 INSERT INTO t(v) VALUES ('alpha'),('beta'),('gamma');",
            )
            .expect("write");
        }

        // At rest: exactly one file, no journal, no sidecar.
        assert!(db.exists(), "db file present");
        assert!(!dir.join("data.db-journal").exists(), "no journal at rest");
        assert!(!dir.join(TIERED_SIDECAR).exists(), "no tiered sidecar");

        let conn = open_local(&db).expect("reopen");
        let count: i64 = conn
            .query_row("SELECT count(*) FROM t", [], |r| r.get(0))
            .expect("count");
        assert_eq!(count, 3);
        let v: String = conn
            .query_row("SELECT v FROM t WHERE id=2", [], |r| r.get(0))
            .expect("row");
        assert_eq!(v, "beta");

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn l1_single_file_is_compressed() {
        let dir = temp_dir();
        let db = dir.join("big.db");
        {
            let conn = open_local(&db).expect("open");
            conn.execute_batch("CREATE TABLE t(v TEXT);").unwrap();
            let mut stmt = conn.prepare("INSERT INTO t(v) VALUES (?1)").unwrap();
            // Highly compressible repeated text.
            let blob = "the quick brown fox ".repeat(50);
            for _ in 0..2000 {
                stmt.execute([&blob]).unwrap();
            }
        }
        let file_size = std::fs::metadata(&db).unwrap().len();

        // Reopen and measure the logical (uncompressed) image size, which
        // is what a plain SQLite file would occupy.
        let logical = {
            let h = MainHandle::open(db.clone(), LocalOptions::default().codec(), DEFAULT_PAGE_SIZE)
                .unwrap();
            h.image.len() as u64
        };

        assert!(
            file_size as f64 <= logical as f64 * 0.7,
            "compressed file {file_size} should be <=70% of logical {logical}"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn l1_refuses_over_tiered_sidecar() {
        let dir = temp_dir();
        // Simulate a tiered database directory.
        std::fs::write(dir.join(TIERED_SIDECAR), b"x").unwrap();
        let db = dir.join("app.db");
        let err = open_local(&db).expect_err("must refuse tiered dir");
        assert!(matches!(err, LocalError::TieredSidecarPresent(_)));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[cfg(feature = "encryption")]
    #[test]
    fn l1_encrypted_roundtrip() {
        let dir = temp_dir();
        let db = dir.join("enc.db");
        let opts = LocalOptions {
            encryption_key: Some([7u8; 32]),
            ..Default::default()
        };
        {
            let conn = open_local_with(&db, opts.clone()).expect("open");
            conn.execute_batch("CREATE TABLE t(v TEXT); INSERT INTO t VALUES ('secret');")
                .unwrap();
        }
        // Raw file must not contain the plaintext.
        let raw = std::fs::read(&db).unwrap();
        assert!(
            !raw.windows(6).any(|w| w == b"secret"),
            "plaintext leaked into encrypted file"
        );
        let conn = open_local_with(&db, opts).expect("reopen");
        let v: String = conn
            .query_row("SELECT v FROM t", [], |r| r.get(0))
            .unwrap();
        assert_eq!(v, "secret");
        std::fs::remove_dir_all(&dir).ok();
    }
}

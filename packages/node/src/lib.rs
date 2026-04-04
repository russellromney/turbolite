use napi::bindgen_prelude::*;
use napi_derive::napi;
use rusqlite::{Connection, OpenFlags};
use turbolite::tiered::{register as tiered_register, TurboliteConfig, TurboliteVfs, StorageBackend};
use std::sync::atomic::{AtomicU64, Ordering};

static VFS_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Options for opening a Database.
#[napi(object)]
pub struct DatabaseOptions {
    /// Storage mode: "local" (default) or "s3".
    pub mode: Option<String>,
    /// Zstd compression level 1-22 (local mode, default 3). Pass null for no compression.
    pub compression: Option<i32>,
    /// S3 bucket name (required when mode = "s3").
    pub bucket: Option<String>,
    /// S3 endpoint URL, e.g. for Tigris or MinIO (mode = "s3").
    pub endpoint: Option<String>,
    /// S3 key prefix (mode = "s3", default "turbolite").
    pub prefix: Option<String>,
    /// Local cache directory (mode = "s3", defaults to db file's parent directory).
    pub cache_dir: Option<String>,
    /// AWS region (mode = "s3").
    pub region: Option<String>,
}

/// A SQLite database connection with TurboliteVfs (local or S3 cloud storage).
#[napi]
pub struct Database {
    conn: Option<Connection>,
}

#[napi]
impl Database {
    /// Open a database.
    ///
    /// @param path - Path to the database file.
    /// @param options - Options object. Omit for local compressed mode.
    ///   Local mode: `{ compression: 3 }` (zstd level 1-22; omit for no compression).
    ///   S3 mode:    `{ mode: "s3", bucket: "my-bucket", endpoint: "..." }`.
    #[napi(constructor)]
    pub fn new(path: String, options: Option<DatabaseOptions>) -> Result<Self> {
        let path = std::path::Path::new(&path);
        let abs_path = if path.is_absolute() {
            path.to_path_buf()
        } else {
            std::env::current_dir()
                .map_err(|e| Error::from_reason(e.to_string()))?
                .join(path)
        };
        let base_dir = abs_path
            .parent()
            .unwrap_or(std::path::Path::new("."))
            .to_path_buf();

        let vfs_name = format!(
            "turbolite-node-{}",
            VFS_COUNTER.fetch_add(1, Ordering::SeqCst)
        );

        let mode = options
            .as_ref()
            .and_then(|o| o.mode.as_deref())
            .unwrap_or("local");

        let flags = OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE;

        let conn = if mode == "s3" {
            let bucket = options
                .as_ref()
                .and_then(|o| o.bucket.clone())
                .ok_or_else(|| Error::from_reason("bucket is required for s3 mode"))?;
            let endpoint_url = options
                .as_ref()
                .and_then(|o| o.endpoint.clone())
                .filter(|s| !s.is_empty());
            let prefix = options
                .as_ref()
                .and_then(|o| o.prefix.clone())
                .unwrap_or_else(|| "turbolite".to_string());
            let cache_dir = options
                .as_ref()
                .and_then(|o| o.cache_dir.as_ref().map(|s| std::path::PathBuf::from(s)))
                .unwrap_or_else(|| base_dir.clone());
            let region = options
                .as_ref()
                .and_then(|o| o.region.clone())
                .filter(|s| !s.is_empty())
                .or_else(|| std::env::var("TURBOLITE_REGION").ok().filter(|s| !s.is_empty()));

            let config = TurboliteConfig {
                storage_backend: StorageBackend::S3 {
                    bucket,
                    prefix,
                    endpoint_url,
                    region,
                },
                cache_dir,
                ..Default::default()
            };

            let vfs = TurboliteVfs::new(config)
                .map_err(|e| Error::from_reason(format!("create cloud VFS: {e}")))?;
            tiered_register(&vfs_name, vfs)
                .map_err(|e| Error::from_reason(format!("register cloud VFS: {e}")))?;

            let conn = Connection::open_with_flags_and_vfs(&abs_path, flags, &vfs_name)
                .map_err(|e| Error::from_reason(format!("open: {e}")))?;

            // S3 mode: 64KB pages for fewer S3 round trips, WAL mode for
            // concurrent reads during checkpoint.
            conn.execute_batch(
                "PRAGMA page_size=65536;
                 PRAGMA journal_mode=WAL;"
            ).map_err(|e| Error::from_reason(format!("set S3 pragmas: {e}")))?;

            conn
        } else {
            let compression = options.as_ref().and_then(|o| o.compression).unwrap_or(3);
            let config = TurboliteConfig {
                storage_backend: StorageBackend::Local,
                cache_dir: base_dir,
                compression_level: compression,
                ..Default::default()
            };
            let vfs = TurboliteVfs::new(config)
                .map_err(|e| Error::from_reason(format!("create local VFS: {e}")))?;
            tiered_register(&vfs_name, vfs)
                .map_err(|e| Error::from_reason(format!("register VFS: {e}")))?;

            Connection::open_with_flags_and_vfs(&abs_path, flags, &vfs_name)
                .map_err(|e| Error::from_reason(format!("open: {e}")))?
        };

        Ok(Database { conn: Some(conn) })
    }

    /// Execute SQL that returns no rows (DDL, INSERT, UPDATE, DELETE).
    #[napi]
    pub fn exec(&self, sql: String) -> Result<()> {
        let conn = self
            .conn
            .as_ref()
            .ok_or_else(|| Error::from_reason("database is closed"))?;
        conn.execute_batch(&sql)
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Execute a SELECT and return rows as an array of objects.
    #[napi]
    pub fn query(&self, sql: String) -> Result<Vec<serde_json::Value>> {
        let conn = self
            .conn
            .as_ref()
            .ok_or_else(|| Error::from_reason("database is closed"))?;

        let mut stmt = conn
            .prepare(&sql)
            .map_err(|e| Error::from_reason(e.to_string()))?;

        let col_count = stmt.column_count();
        let col_names: Vec<String> = (0..col_count)
            .map(|i| stmt.column_name(i).unwrap_or("?").to_string())
            .collect();

        let mut rows = Vec::new();
        let mut result = stmt
            .query([])
            .map_err(|e| Error::from_reason(e.to_string()))?;

        while let Some(row) = result
            .next()
            .map_err(|e| Error::from_reason(e.to_string()))?
        {
            let mut obj = serde_json::Map::new();
            for (i, name) in col_names.iter().enumerate() {
                let val = match row.get_ref(i) {
                    Ok(rusqlite::types::ValueRef::Null) => serde_json::Value::Null,
                    Ok(rusqlite::types::ValueRef::Integer(n)) => serde_json::json!(n),
                    Ok(rusqlite::types::ValueRef::Real(f)) => serde_json::json!(f),
                    Ok(rusqlite::types::ValueRef::Text(s)) => {
                        serde_json::Value::String(String::from_utf8_lossy(s).into_owned())
                    }
                    Ok(rusqlite::types::ValueRef::Blob(b)) => {
                        serde_json::Value::String(format!("blob:{} bytes", b.len()))
                    }
                    Err(e) => serde_json::Value::String(format!("error: {e}")),
                };
                obj.insert(name.clone(), val);
            }
            rows.push(serde_json::Value::Object(obj));
        }

        Ok(rows)
    }

    /// Close the database connection.
    #[napi]
    pub fn close(&mut self) {
        self.conn.take();
    }
}

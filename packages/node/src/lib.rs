use napi::bindgen_prelude::*;
use napi_derive::napi;
use rusqlite::{Connection, OpenFlags};
use sqlite_compress_encrypt_vfs::{register, CompressedVfs};
use std::sync::atomic::{AtomicU64, Ordering};

static VFS_COUNTER: AtomicU64 = AtomicU64::new(0);

/// A SQLite database connection with transparent compression.
#[napi]
pub struct Database {
    conn: Option<Connection>,
}

#[napi]
impl Database {
    /// Open a database with transparent zstd compression.
    ///
    /// @param path - Path to the database file.
    /// @param compression - zstd compression level 1-22 (default 3). Pass null for no compression.
    #[napi(constructor)]
    pub fn new(path: String, compression: Option<i32>) -> Result<Self> {
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
            .unwrap_or(std::path::Path::new("."));

        let vfs_name = format!(
            "turbolite-node-{}",
            VFS_COUNTER.fetch_add(1, Ordering::SeqCst)
        );

        let vfs = match compression {
            Some(level) => CompressedVfs::new(base_dir, level),
            None => CompressedVfs::passthrough(base_dir),
        };

        register(&vfs_name, vfs)
            .map_err(|e| Error::from_reason(format!("register VFS: {e}")))?;

        let flags = OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE;
        let conn = Connection::open_with_flags_and_vfs(&abs_path, flags, &vfs_name)
            .map_err(|e| Error::from_reason(format!("open: {e}")))?;

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

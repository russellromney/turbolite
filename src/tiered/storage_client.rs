use super::*;

// ===== StorageClient: unified local + S3 storage abstraction =====

/// Unified storage client for page groups and manifests.
/// Local: reads/writes files under `{base_dir}/pg/` and `{base_dir}/manifest.msgpack`.
/// S3: delegates to the existing S3Client.
pub(crate) enum StorageClient {
    Local {
        base_dir: PathBuf,
    },
    #[cfg(feature = "tiered")]
    S3(Arc<s3_client::S3Client>),
}

impl StorageClient {
    /// Create a StorageClient from the config's effective backend.
    /// For S3 mode, requires a pre-constructed S3Client (created with tokio runtime).
    /// For Local mode, creates the `pg/` directory.
    pub(crate) fn local(base_dir: PathBuf) -> io::Result<Self> {
        let pg_dir = base_dir.join("pg");
        fs::create_dir_all(&pg_dir)?;
        Ok(StorageClient::Local { base_dir })
    }

    #[cfg(feature = "tiered")]
    pub(crate) fn s3(client: Arc<s3_client::S3Client>) -> Self {
        StorageClient::S3(client)
    }

    /// Whether this is a local-only client.
    pub(crate) fn is_local(&self) -> bool {
        match self {
            StorageClient::Local { .. } => true,
            #[cfg(feature = "tiered")]
            StorageClient::S3(_) => false,
        }
    }

    // ── Page group operations ──

    /// Fetch a page group by its key (e.g., "pg/0_v1").
    /// Returns Ok(None) if the key doesn't exist.
    pub(crate) fn get_page_group(&self, key: &str) -> io::Result<Option<Vec<u8>>> {
        match self {
            StorageClient::Local { base_dir } => {
                let path = base_dir.join(key);
                match fs::read(&path) {
                    Ok(data) => Ok(Some(data)),
                    Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
                    Err(e) => Err(e),
                }
            }
            #[cfg(feature = "tiered")]
            StorageClient::S3(s3) => s3.get_page_group(key),
        }
    }

    /// Fetch multiple page groups by key in parallel. Returns found groups.
    pub(crate) fn get_page_groups_by_key(&self, keys: &[String]) -> io::Result<HashMap<String, Vec<u8>>> {
        match self {
            StorageClient::Local { base_dir } => {
                let mut result = HashMap::new();
                for key in keys {
                    let path = base_dir.join(key);
                    match fs::read(&path) {
                        Ok(data) => { result.insert(key.clone(), data); }
                        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                        Err(e) => return Err(e),
                    }
                }
                Ok(result)
            }
            #[cfg(feature = "tiered")]
            StorageClient::S3(s3) => s3.get_page_groups_by_key(keys),
        }
    }

    /// Store page groups. Each entry: (key, data).
    /// Local: writes to `{base_dir}/{key}` via atomic tmp+rename.
    pub(crate) fn put_page_groups(&self, groups: &[(String, Vec<u8>)]) -> io::Result<()> {
        match self {
            StorageClient::Local { base_dir } => {
                for (key, data) in groups {
                    let path = base_dir.join(key);
                    if let Some(parent) = path.parent() {
                        fs::create_dir_all(parent)?;
                    }
                    let tmp = path.with_extension("tmp");
                    fs::write(&tmp, data)?;
                    fs::rename(&tmp, &path)?;
                }
                Ok(())
            }
            #[cfg(feature = "tiered")]
            StorageClient::S3(s3) => s3.put_page_groups(groups),
        }
    }

    /// Delete page groups by key.
    pub(crate) fn delete_page_groups(&self, keys: &[String]) -> io::Result<()> {
        match self {
            StorageClient::Local { base_dir } => {
                for key in keys {
                    let path = base_dir.join(key);
                    match fs::remove_file(&path) {
                        Ok(()) => {}
                        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                        Err(e) => return Err(e),
                    }
                }
                Ok(())
            }
            #[cfg(feature = "tiered")]
            StorageClient::S3(s3) => s3.delete_objects(keys),
        }
    }

    // ── Manifest operations ──

    /// Fetch the manifest. Returns Ok(None) if no manifest exists (new database).
    pub(crate) fn get_manifest(&self) -> io::Result<Option<Manifest>> {
        match self {
            StorageClient::Local { base_dir } => {
                // Load from local manifest (same format as LocalManifest)
                match manifest::LocalManifest::load(base_dir)? {
                    Some(local) => {
                        let mut m = local.manifest;
                        m.build_page_index();
                        Ok(Some(m))
                    }
                    None => Ok(None),
                }
            }
            #[cfg(feature = "tiered")]
            StorageClient::S3(s3) => s3.get_manifest(),
        }
    }

    /// Store the manifest.
    pub(crate) fn put_manifest(&self, manifest: &Manifest, dirty_groups: &[u64]) -> io::Result<()> {
        match self {
            StorageClient::Local { base_dir } => {
                let local = manifest::LocalManifest {
                    manifest: manifest.clone(),
                    dirty_groups: dirty_groups.to_vec(),
                };
                local.persist(base_dir)
            }
            #[cfg(feature = "tiered")]
            StorageClient::S3(s3) => s3.put_manifest(manifest),
        }
    }

    /// Check if a database exists at this storage location.
    pub(crate) fn exists(&self) -> io::Result<bool> {
        match self {
            StorageClient::Local { base_dir } => {
                Ok(base_dir.join("manifest.msgpack").exists())
            }
            #[cfg(feature = "tiered")]
            StorageClient::S3(s3) => {
                Ok(s3.get_manifest()?.is_some())
            }
        }
    }

    // ── Key generation helpers (same format regardless of backend) ──

    /// Generate a page group key: `pg/{group_id}_v{version}`
    pub(crate) fn page_group_key(group_id: u64, version: u64) -> String {
        format!("pg/{}_v{}", group_id, version)
    }

    /// Generate an interior bundle key: `ibc/{chunk_id}_v{version}`
    pub(crate) fn interior_chunk_key(chunk_id: u32, version: u64) -> String {
        format!("ibc/{}_v{}", chunk_id, version)
    }

    /// Generate an index leaf bundle key: `ixb/{chunk_id}_v{version}`
    pub(crate) fn index_chunk_key(chunk_id: u32, version: u64) -> String {
        format!("ixb/{}_v{}", chunk_id, version)
    }

    // ── S3-specific operations (no-op for local) ──

    /// Byte-range GET (S3 only). Local mode reads the full file.
    pub(crate) fn range_get(&self, key: &str, start: u64, len: u32) -> io::Result<Option<Vec<u8>>> {
        match self {
            StorageClient::Local { base_dir } => {
                use std::os::unix::fs::FileExt;
                let path = base_dir.join(key);
                match File::open(&path) {
                    Ok(file) => {
                        let mut buf = vec![0u8; len as usize];
                        file.read_exact_at(&mut buf, start)?;
                        Ok(Some(buf))
                    }
                    Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
                    Err(e) => Err(e),
                }
            }
            #[cfg(feature = "tiered")]
            StorageClient::S3(s3) => s3.range_get(key, start, len),
        }
    }

    /// Diagnostics: number of GETs performed.
    pub(crate) fn fetch_count(&self) -> u64 {
        match self {
            StorageClient::Local { .. } => 0,
            #[cfg(feature = "tiered")]
            StorageClient::S3(s3) => s3.fetch_count.load(Ordering::Relaxed),
        }
    }

    /// Diagnostics: bytes fetched.
    pub(crate) fn fetch_bytes(&self) -> u64 {
        match self {
            StorageClient::Local { .. } => 0,
            #[cfg(feature = "tiered")]
            StorageClient::S3(s3) => s3.fetch_bytes.load(Ordering::Relaxed),
        }
    }
}

#[cfg(test)]
#[path = "test_storage_client.rs"]
mod tests;

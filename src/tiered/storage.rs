//! Sync-facing helpers around an `Arc<dyn hadb_storage::StorageBackend>`.
//!
//! turbolite is a SQLite VFS; SQLite calls into us synchronously. The
//! `StorageBackend` trait is async so that real backends (S3, HTTP) can
//! use their native async clients. This module bridges the two with
//! `async_rt::block_on` and provides typed wrappers for the turbolite
//! specific serialised shapes (manifests, page groups).
//!
//! Functions here intentionally take `&dyn StorageBackend` rather than
//! generic parameters so call sites don't monomorphise per backend and
//! the compiler doesn't need to know about any concrete impl crate.

use std::collections::HashMap;
use std::io;

use hadb_storage::StorageBackend;
use tokio::runtime::Handle as TokioHandle;

use super::async_rt::block_on;
use super::keys;
use super::manifest::{self, Manifest};

/// Fetch a page group / chunk / override object by key.
///
/// `Ok(None)` means the backend has no object for this key (distinct from
/// IO error). Callers that need to distinguish "missing" from "failed" are
/// relying on that split.
pub(crate) fn get_page_group(
    backend: &dyn StorageBackend,
    runtime: &TokioHandle,
    key: &str,
) -> io::Result<Option<Vec<u8>>> {
    block_on(runtime, backend.get(key))
        .map_err(|e| io::Error::other(format!("storage get {key}: {e}")))
}

/// Fetch a byte range from an object. `start`/`len` semantics match the
/// `StorageBackend::range_get` contract.
pub(crate) fn range_get(
    backend: &dyn StorageBackend,
    runtime: &TokioHandle,
    key: &str,
    start: u64,
    len: u32,
) -> io::Result<Option<Vec<u8>>> {
    let result = block_on(runtime, backend.range_get(key, start, len))
        .map_err(|e| io::Error::other(format!("storage range_get {key}: {e}")))?;
    validate_range_get_result(key, start, len, result)
}

pub(crate) fn validate_range_get_result(
    key: &str,
    start: u64,
    len: u32,
    result: Option<Vec<u8>>,
) -> io::Result<Option<Vec<u8>>> {
    if let Some(bytes) = &result {
        if bytes.len() != len as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "storage range_get {key} returned {} bytes for requested exact range {start}+{len}",
                    bytes.len()
                ),
            ));
        }
    }
    Ok(result)
}

/// Upload a batch of `(key, bytes)`. Uses the backend's native `put_many`
/// so HTTP / S3 backends can parallelise; local falls back to the default
/// serial loop.
pub(crate) fn put_page_groups(
    backend: &dyn StorageBackend,
    runtime: &TokioHandle,
    uploads: &[(String, Vec<u8>)],
) -> io::Result<()> {
    if uploads.is_empty() {
        return Ok(());
    }
    block_on(runtime, backend.put_many(uploads))
        .map_err(|e| io::Error::other(format!("storage put_many: {e}")))
}

/// Single-key put. Thin wrapper over `StorageBackend::put`.
pub(crate) fn put(
    backend: &dyn StorageBackend,
    runtime: &TokioHandle,
    key: &str,
    data: &[u8],
) -> io::Result<()> {
    block_on(runtime, backend.put(key, data))
        .map_err(|e| io::Error::other(format!("storage put {key}: {e}")))
}

/// Batch delete. See `StorageBackend::delete_many`.
pub(crate) fn delete_objects(
    backend: &dyn StorageBackend,
    runtime: &TokioHandle,
    keys_: &[String],
) -> io::Result<usize> {
    if keys_.is_empty() {
        return Ok(0);
    }
    block_on(runtime, backend.delete_many(keys_))
        .map_err(|e| io::Error::other(format!("storage delete_many: {e}")))
}

/// List all keys. Paginates under the hood via the trait's `after` cursor.
pub(crate) fn list_all_keys(
    backend: &dyn StorageBackend,
    runtime: &TokioHandle,
) -> io::Result<Vec<String>> {
    list_all_keys_with_prefix(backend, runtime, "")
}

/// List all keys under a given prefix.
pub(crate) fn list_all_keys_with_prefix(
    backend: &dyn StorageBackend,
    runtime: &TokioHandle,
    prefix: &str,
) -> io::Result<Vec<String>> {
    block_on(runtime, async move {
        let mut out: Vec<String> = Vec::new();
        let mut after: Option<String> = None;
        loop {
            let batch = backend
                .list(prefix, after.as_deref())
                .await
                .map_err(|e| io::Error::other(format!("storage list: {e}")))?;
            if batch.is_empty() {
                break;
            }
            after = batch.last().cloned();
            out.extend(batch);
            // Backends are expected to implement pagination themselves if
            // they limit page size; if the returned batch is smaller than
            // whatever their page cap is, another call still terminates
            // because the next `after` yields an empty list.
            if after.is_none() {
                break;
            }
            // Guard against pathological backends that always return at
            // least one key.
            if out.len() > 10_000_000 {
                return Err(io::Error::other("list overran 10M keys"));
            }
            // One-shot backends (like LocalStorage's single-pass walk) will
            // return the full list in one call; the second `list` with the
            // tail-cursor returns empty and we break.
        }
        Ok(out)
    })
}

/// Fetch and deserialise the canonical turbolite manifest.
pub(crate) fn get_manifest(
    backend: &dyn StorageBackend,
    runtime: &TokioHandle,
) -> io::Result<Option<Manifest>> {
    match get_page_group(backend, runtime, keys::MANIFEST_KEY)? {
        None => Ok(None),
        Some(bytes) => {
            let mut m = manifest::decode_manifest_bytes(&bytes)?;
            m.detect_and_normalize_strategy();
            Ok(Some(m))
        }
    }
}

/// Serialise + upload the canonical manifest. `MANIFEST_KEY` is the
/// source-of-truth object every turbolite backend stores.
///
/// Best-effort CAS: before overwriting, fetch the remote manifest and reject
/// the upload if it already has a version >= the one we are writing (C1). This
/// prevents a slow/follower writer from silently clobbering a newer manifest.
/// It is not a true atomic compare-and-swap because the backend trait does not
/// expose ETags, but it closes the most common cross-writer race window.
pub(crate) fn put_manifest(
    backend: &dyn StorageBackend,
    runtime: &TokioHandle,
    manifest: &Manifest,
) -> io::Result<()> {
    // Optimistic version check. First write (version 1) skips the fetch.
    if manifest.version > 1 {
        match get_manifest(backend, runtime) {
            Ok(Some(remote)) if remote.version >= manifest.version => {
                return Err(io::Error::other(format!(
                    "manifest CAS conflict: remote v{} >= local v{}",
                    remote.version, manifest.version
                )));
            }
            Ok(_) => {}
            Err(e) => {
                // If we cannot verify the remote manifest, we must not proceed:
                // publishing without checking the version could overwrite a newer
                // manifest. The caller can retry after the transient read error
                // resolves.
                return Err(io::Error::other(format!(
                    "manifest version check failed: cannot read remote manifest: {e}"
                )));
            }
        }
    }

    let bytes = rmp_serde::to_vec(manifest)
        .map_err(|e| io::Error::other(format!("encode manifest: {e}")))?;
    put(backend, runtime, keys::MANIFEST_KEY, &bytes)
}

/// Fetch multiple page groups by key, returning a `HashMap<key, bytes>`.
/// Missing keys are omitted from the output (use `.len()` or a per-key
/// check to detect). This matches the previous `get_page_groups_by_key`
/// contract used by `materialize_to_file`.
pub(crate) fn get_page_groups_by_key(
    backend: &dyn StorageBackend,
    runtime: &TokioHandle,
    key_list: &[String],
) -> io::Result<HashMap<String, Vec<u8>>> {
    block_on(runtime, async move {
        let mut out: HashMap<String, Vec<u8>> = HashMap::with_capacity(key_list.len());
        for k in key_list {
            match backend.get(k).await {
                Ok(Some(bytes)) => {
                    out.insert(k.clone(), bytes);
                }
                Ok(None) => {}
                Err(e) => {
                    return Err(io::Error::other(format!("storage get {k}: {e}")));
                }
            }
        }
        Ok(out)
    })
}

/// Batch-existence check. Returns true iff `keys::MANIFEST_KEY` is present.
/// Used by the VFS `exists()` entry point when the local cache has no copy
/// of a file that might live in the backend.
pub(crate) fn manifest_exists(
    backend: &dyn StorageBackend,
    runtime: &TokioHandle,
) -> io::Result<bool> {
    block_on(runtime, backend.exists(keys::MANIFEST_KEY))
        .map_err(|e| io::Error::other(format!("storage exists: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;
    use async_trait::async_trait;
    use std::sync::Arc;

    struct WrongLengthRangeBackend {
        bytes: Vec<u8>,
    }

    #[async_trait]
    impl StorageBackend for WrongLengthRangeBackend {
        async fn get(&self, _key: &str) -> Result<Option<Vec<u8>>> {
            Ok(Some(vec![1, 2, 3, 4, 5, 6]))
        }

        async fn put(&self, _key: &str, _data: &[u8]) -> Result<()> {
            Ok(())
        }

        async fn delete(&self, _key: &str) -> Result<()> {
            Ok(())
        }

        async fn list(&self, _prefix: &str, _after: Option<&str>) -> Result<Vec<String>> {
            Ok(Vec::new())
        }

        async fn put_if_absent(&self, _key: &str, _data: &[u8]) -> Result<hadb_storage::CasResult> {
            Ok(hadb_storage::CasResult {
                success: true,
                etag: None,
            })
        }

        async fn put_if_match(
            &self,
            _key: &str,
            _data: &[u8],
            _etag: &str,
        ) -> Result<hadb_storage::CasResult> {
            Ok(hadb_storage::CasResult {
                success: true,
                etag: None,
            })
        }

        async fn range_get(&self, _key: &str, _start: u64, _len: u32) -> Result<Option<Vec<u8>>> {
            Ok(Some(self.bytes.clone()))
        }
    }

    #[test]
    fn range_get_rejects_wrong_length_backend_response() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        for bytes in [vec![1], vec![1, 2, 3, 4, 5, 6]] {
            let backend: Arc<dyn StorageBackend> = Arc::new(WrongLengthRangeBackend { bytes });
            let err = range_get(backend.as_ref(), rt.handle(), "g0", 1, 2).unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::InvalidData);
            assert!(err.to_string().contains("exact range"));
        }
    }
}

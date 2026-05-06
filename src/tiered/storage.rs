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
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("storage get {key}: {e}")))
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
    block_on(runtime, backend.range_get(key, start, len)).map_err(|e| {
        io::Error::new(
            io::ErrorKind::Other,
            format!("storage range_get {key}: {e}"),
        )
    })
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
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("storage put_many: {e}")))
}

/// Single-key put. Thin wrapper over `StorageBackend::put`.
pub(crate) fn put(
    backend: &dyn StorageBackend,
    runtime: &TokioHandle,
    key: &str,
    data: &[u8],
) -> io::Result<()> {
    block_on(runtime, backend.put(key, data))
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("storage put {key}: {e}")))
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
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("storage delete_many: {e}")))
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
                .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("storage list: {e}")))?;
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
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    "list overran 10M keys",
                ));
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
pub(crate) fn put_manifest(
    backend: &dyn StorageBackend,
    runtime: &TokioHandle,
    manifest: &Manifest,
) -> io::Result<()> {
    let bytes = rmp_serde::to_vec(manifest)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("encode manifest: {e}")))?;
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
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        format!("storage get {k}: {e}"),
                    ));
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
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("storage exists: {e}")))
}

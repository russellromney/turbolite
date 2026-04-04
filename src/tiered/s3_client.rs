// When cloud feature is disabled, S3Client is a zero-size type that can never
// be constructed. The type exists so Option<Arc<S3Client>> compiles everywhere.
#[cfg(not(feature = "cloud"))]
pub(crate) struct S3Client {
    _private: (),  // prevent construction
}

#[cfg(feature = "cloud")]
use super::*;

// ===== S3Client (sync wrapper around async SDK) =====

#[cfg(feature = "cloud")]
/// Synchronous S3 client wrapping the async AWS SDK.
pub(crate) struct S3Client {
    pub(crate) client: aws_sdk_s3::Client,
    pub(crate) bucket: String,
    pub(crate) prefix: String,
    pub(crate) runtime: TokioHandle,
    /// S3 GET count (for benchmarking / diagnostics)
    pub(crate) fetch_count: AtomicU64,
    /// S3 bytes fetched (for benchmarking / diagnostics)
    pub(crate) fetch_bytes: AtomicU64,
    /// S3 PUT count (for benchmarking / diagnostics)
    pub(crate) put_count: AtomicU64,
    /// S3 bytes uploaded (for benchmarking / diagnostics)
    pub(crate) put_bytes: AtomicU64,
}

#[cfg(feature = "cloud")]
impl S3Client {
    /// Create a new S3 client.
    pub(crate) async fn new_async(config: &TurboliteConfig) -> io::Result<Self> {
        eprintln!("[s3] new_async: loading aws_config...");
        let mut aws_config = aws_config::from_env();

        if let Some(region) = &config.region {
            eprintln!("[s3] setting region: {}", region);
            aws_config =
                aws_config.region(aws_sdk_s3::config::Region::new(region.clone()));
        }

        let aws_config = aws_config.load().await;
        eprintln!("[s3] aws_config loaded");

        let mut s3_config = aws_sdk_s3::config::Builder::from(&aws_config);
        if let Some(endpoint) = &config.endpoint_url {
            eprintln!("[s3] setting endpoint: {}", endpoint);
            s3_config = s3_config.endpoint_url(endpoint);
        } else {
            eprintln!("[s3] using default AWS S3 endpoint");
        }

        let client = aws_sdk_s3::Client::from_conf(s3_config.build());
        eprintln!("[s3] S3 client created");

        let runtime = config
            .runtime_handle
            .clone()
            .or_else(|| TokioHandle::try_current().ok())
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::Other,
                    "No tokio runtime available. Pass runtime_handle in TurboliteConfig \
                     or call from within a tokio context.",
                )
            })?;

        Ok(Self {
            client,
            bucket: config.bucket.clone(),
            prefix: config.prefix.clone(),
            runtime,
            fetch_count: AtomicU64::new(0),
            fetch_bytes: AtomicU64::new(0),
            put_count: AtomicU64::new(0),
            put_bytes: AtomicU64::new(0),
        })
    }

    /// Blocking constructor.
    pub(crate) fn new_blocking(config: &TurboliteConfig, runtime: &TokioHandle) -> io::Result<Self> {
        Self::block_on(runtime, Self::new_async(config))
    }

    /// Run an async future on the tokio runtime, handling both
    /// "inside tokio" and "outside tokio" cases.
    pub(crate) fn block_on<F: std::future::Future<Output = T>, T>(
        handle: &TokioHandle,
        fut: F,
    ) -> T {
        match TokioHandle::try_current() {
            Ok(_) => tokio::task::block_in_place(|| handle.block_on(fut)),
            Err(_) => handle.block_on(fut),
        }
    }

    pub(crate) fn s3_key(&self, suffix: &str) -> String {
        if self.prefix.is_empty() {
            suffix.to_string()
        } else {
            format!("{}/{}", self.prefix, suffix)
        }
    }

    pub(crate) fn manifest_key_msgpack(&self) -> String {
        self.s3_key("manifest.msgpack")
    }

    pub(crate) fn manifest_key_json(&self) -> String {
        self.s3_key("manifest.json")
    }

    /// Generate versioned S3 key for a page group.
    pub(crate) fn page_group_key(&self, group_id: u64, version: u64) -> String {
        self.s3_key(&format!("pg/{}_v{}", group_id, version))
    }

    /// Generate versioned S3 key for a chunked interior bundle piece.
    pub(crate) fn interior_chunk_key(&self, chunk_id: u32, version: u64) -> String {
        self.s3_key(&format!("ibc/{}_v{}", chunk_id, version))
    }

    /// Generate versioned S3 key for a chunked index leaf bundle piece.
    pub(crate) fn index_chunk_key(&self, chunk_id: u32, version: u64) -> String {
        self.s3_key(&format!("ixb/{}_v{}", chunk_id, version))
    }

    /// Phase Drift: override frame key.
    pub(crate) fn override_frame_key(&self, group_id: u64, frame_idx: usize, version: u64) -> String {
        self.s3_key(&format!("pg/{}_f{}_v{}", group_id, frame_idx, version))
    }

    // --- Generic GET/PUT ---

    pub(crate) async fn get_object_async(&self, key: &str) -> io::Result<Option<Vec<u8>>> {
        let mut retries = 0u32;
        loop {
            match self
                .client
                .get_object()
                .bucket(&self.bucket)
                .key(key)
                .send()
                .await
            {
                Ok(resp) => {
                    let bytes = resp
                        .body
                        .collect()
                        .await
                        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?
                        .into_bytes();
                    self.fetch_count.fetch_add(1, Ordering::Relaxed);
                    self.fetch_bytes.fetch_add(bytes.len() as u64, Ordering::Relaxed);
                    return Ok(Some(bytes.to_vec()));
                }
                Err(e) => {
                    if is_not_found(&e) {
                        return Ok(None);
                    }
                    retries += 1;
                    if retries >= 3 {
                        return Err(io::Error::new(
                            io::ErrorKind::Other,
                            format!("S3 GET {} failed after 3 retries: {:?}", key, e),
                        ));
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(
                        100 * (1 << retries),
                    ))
                    .await;
                }
            }
        }
    }

    /// Byte-range GET: fetch [start..start+len) from an S3 object.
    pub(crate) async fn range_get_async(&self, key: &str, start: u64, len: u32) -> io::Result<Option<Vec<u8>>> {
        let range = format!("bytes={}-{}", start, start + len as u64 - 1);
        let mut retries = 0u32;
        loop {
            match self
                .client
                .get_object()
                .bucket(&self.bucket)
                .key(key)
                .range(&range)
                .send()
                .await
            {
                Ok(resp) => {
                    let bytes = resp
                        .body
                        .collect()
                        .await
                        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?
                        .into_bytes();
                    self.fetch_count.fetch_add(1, Ordering::Relaxed);
                    self.fetch_bytes.fetch_add(bytes.len() as u64, Ordering::Relaxed);
                    return Ok(Some(bytes.to_vec()));
                }
                Err(e) => {
                    if is_not_found(&e) {
                        return Ok(None);
                    }
                    retries += 1;
                    if retries >= 3 {
                        return Err(io::Error::new(
                            io::ErrorKind::Other,
                            format!("S3 range GET {} ({}) failed after 3 retries: {:?}", key, range, e),
                        ));
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(
                        100 * (1 << retries),
                    ))
                    .await;
                }
            }
        }
    }

    /// Blocking byte-range GET.
    pub(crate) fn range_get(&self, key: &str, start: u64, len: u32) -> io::Result<Option<Vec<u8>>> {
        S3Client::block_on(&self.runtime, self.range_get_async(key, start, len))
    }

    pub(crate) async fn put_object_async(&self, key: &str, data: Vec<u8>, content_type: Option<&str>) -> io::Result<()> {
        let mut retries = 0u32;
        loop {
            let body = aws_sdk_s3::primitives::ByteStream::from(data.clone());
            let mut req = self
                .client
                .put_object()
                .bucket(&self.bucket)
                .key(key)
                .body(body);
            if let Some(ct) = content_type {
                req = req.content_type(ct);
            }
            let data_len = data.len() as u64;
            match req.send().await {
                Ok(_) => {
                    self.put_count.fetch_add(1, Ordering::Relaxed);
                    self.put_bytes.fetch_add(data_len, Ordering::Relaxed);
                    return Ok(());
                }
                Err(e) => {
                    retries += 1;
                    if retries >= 3 {
                        return Err(io::Error::new(
                            io::ErrorKind::Other,
                            format!("S3 PUT {} failed after 3 retries: {}", key, e),
                        ));
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(
                        100 * (1 << retries),
                    ))
                    .await;
                }
            }
        }
    }

    // --- Page group operations ---

    /// Fetch a page group by its S3 key.
    pub(crate) fn get_page_group(&self, key: &str) -> io::Result<Option<Vec<u8>>> {
        S3Client::block_on(&self.runtime, self.get_object_async(key))
    }

    /// Fetch multiple page groups in parallel by S3 key.
    /// Returns key → bytes for found objects.
    #[allow(dead_code)]
    pub(crate) fn get_page_groups_by_key(&self, keys: &[String]) -> io::Result<HashMap<String, Vec<u8>>> {
        S3Client::block_on(&self.runtime, self.get_page_groups_by_key_async(keys))
    }

    #[allow(dead_code)]
    pub(crate) async fn get_page_groups_by_key_async(&self, keys: &[String]) -> io::Result<HashMap<String, Vec<u8>>> {
        let mut handles = Vec::with_capacity(keys.len());
        for key in keys {
            let client = self.client.clone();
            let bucket = self.bucket.clone();
            let key = key.clone();
            handles.push(tokio::spawn(async move {
                let mut retries = 0u32;
                loop {
                    match client
                        .get_object()
                        .bucket(&bucket)
                        .key(&key)
                        .send()
                        .await
                    {
                        Ok(resp) => {
                            let bytes = resp
                                .body
                                .collect()
                                .await
                                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?
                                .into_bytes();
                            return Ok::<_, io::Error>((key, Some(bytes.to_vec())));
                        }
                        Err(e) => {
                            if is_not_found(&e) {
                                return Ok((key, None));
                            }
                            retries += 1;
                            if retries >= 3 {
                                return Err(io::Error::new(
                                    io::ErrorKind::Other,
                                    format!("S3 GET {} failed: {}", key, e),
                                ));
                            }
                            tokio::time::sleep(std::time::Duration::from_millis(
                                100 * (1 << retries),
                            ))
                            .await;
                        }
                    }
                }
            }));
        }

        let mut result = HashMap::new();
        for handle in handles {
            let (key, data) = handle
                .await
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))??;
            if let Some(bytes) = data {
                self.fetch_count.fetch_add(1, Ordering::Relaxed);
                self.fetch_bytes.fetch_add(bytes.len() as u64, Ordering::Relaxed);
                result.insert(key, bytes);
            }
        }
        Ok(result)
    }

    /// Upload multiple page groups in parallel. Each entry is (s3_key, raw_bytes).
    pub(crate) fn put_page_groups(&self, groups: &[(String, Vec<u8>)]) -> io::Result<()> {
        S3Client::block_on(&self.runtime, self.put_page_groups_async(groups))
    }

    pub(crate) async fn put_page_groups_async(&self, groups: &[(String, Vec<u8>)]) -> io::Result<()> {
        let total_bytes: u64 = groups.iter().map(|(_, d)| d.len() as u64).sum();
        let total_puts = groups.len() as u64;
        let mut handles = Vec::with_capacity(groups.len());
        for (key, data) in groups {
            let client = self.client.clone();
            let bucket = self.bucket.clone();
            let key = key.clone();
            let data = data.clone();
            handles.push(tokio::spawn(async move {
                let mut retries = 0u32;
                loop {
                    let body =
                        aws_sdk_s3::primitives::ByteStream::from(data.clone());
                    match client
                        .put_object()
                        .bucket(&bucket)
                        .key(&key)
                        .body(body)
                        .send()
                        .await
                    {
                        Ok(_) => return Ok::<_, io::Error>(()),
                        Err(e) => {
                            retries += 1;
                            if retries >= 3 {
                                return Err(io::Error::new(
                                    io::ErrorKind::Other,
                                    format!("S3 PUT {} failed: {}", key, e),
                                ));
                            }
                            tokio::time::sleep(std::time::Duration::from_millis(
                                100 * (1 << retries),
                            ))
                            .await;
                        }
                    }
                }
            }));
        }

        for handle in handles {
            handle
                .await
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))??;
        }
        // All PUTs succeeded; update counters
        self.put_count.fetch_add(total_puts, Ordering::Relaxed);
        self.put_bytes.fetch_add(total_bytes, Ordering::Relaxed);
        Ok(())
    }

    // --- Manifest ---

    pub(crate) fn get_manifest(&self) -> io::Result<Option<Manifest>> {
        S3Client::block_on(&self.runtime, self.get_manifest_async())
    }

    /// Phase Thermopylae: try msgpack first, fall back to JSON for automigration.
    pub(crate) async fn get_manifest_async(&self) -> io::Result<Option<Manifest>> {
        // Try msgpack first
        let msgpack_key = self.manifest_key_msgpack();
        if let Some(bytes) = self.get_object_async(&msgpack_key).await? {
            let mut manifest: Manifest = rmp_serde::from_slice(&bytes).map_err(|e| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Invalid manifest msgpack: {}", e),
                )
            })?;
            manifest.build_page_index();
            return Ok(Some(manifest));
        }
        // Fall back to JSON (pre-Thermopylae manifests)
        let json_key = self.manifest_key_json();
        match self.get_object_async(&json_key).await? {
            Some(bytes) => {
                let mut manifest: Manifest = serde_json::from_slice(&bytes).map_err(|e| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("Invalid manifest JSON: {}", e),
                    )
                })?;
                manifest.build_page_index();
                Ok(Some(manifest))
            }
            None => Ok(None),
        }
    }

    pub(crate) fn put_manifest(&self, manifest: &Manifest) -> io::Result<()> {
        S3Client::block_on(&self.runtime, self.put_manifest_async(manifest))
    }

    /// Phase Thermopylae: always write msgpack.
    pub(crate) async fn put_manifest_async(&self, manifest: &Manifest) -> io::Result<()> {
        let key = self.manifest_key_msgpack();
        let data = rmp_serde::to_vec(manifest)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        self.put_object_async(&key, data, Some("application/msgpack")).await
    }

    /// Delete a batch of S3 objects by key. Handles batching (AWS limit: 1000/request).
    pub(crate) fn delete_objects(&self, keys: &[String]) -> io::Result<()> {
        if keys.is_empty() {
            return Ok(());
        }
        S3Client::block_on(&self.runtime, self.delete_objects_async(keys))
    }

    pub(crate) async fn delete_objects_async(&self, keys: &[String]) -> io::Result<()> {
        for batch in keys.chunks(1000) {
            let objects: Vec<aws_sdk_s3::types::ObjectIdentifier> = batch
                .iter()
                .map(|key| {
                    aws_sdk_s3::types::ObjectIdentifier::builder()
                        .key(key)
                        .build()
                        .expect("ObjectIdentifier requires key")
                })
                .collect();

            let delete = aws_sdk_s3::types::Delete::builder()
                .set_objects(Some(objects))
                .quiet(true)
                .build()
                .map_err(|e| {
                    io::Error::new(
                        io::ErrorKind::Other,
                        format!("Failed to build Delete request: {}", e),
                    )
                })?;

            self.client
                .delete_objects()
                .bucket(&self.bucket)
                .delete(delete)
                .send()
                .await
                .map_err(|e| {
                    io::Error::new(
                        io::ErrorKind::Other,
                        format!("S3 batch delete failed: {}", e),
                    )
                })?;
        }
        Ok(())
    }

    /// Fire-and-forget async delete for background GC. Owns the keys vec.
    /// Logs errors internally instead of returning them.
    pub(crate) async fn delete_objects_async_owned(&self, keys: Vec<String>) {
        let count = keys.len();
        if let Err(e) = self.delete_objects_async(&keys).await {
            eprintln!("[gc] ERROR: background delete of {} objects failed: {}", count, e);
        } else {
            eprintln!("[gc] deleted {} old versions", count);
        }
    }

    /// List all S3 object keys under this client's prefix.
    pub(crate) fn list_all_keys(&self) -> io::Result<Vec<String>> {
        S3Client::block_on(&self.runtime, self.list_all_keys_async())
    }

    pub(crate) async fn list_all_keys_async(&self) -> io::Result<Vec<String>> {
        let mut all_keys = Vec::new();
        let mut continuation_token: Option<String> = None;
        loop {
            let mut req = self.client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(&self.prefix);
            if let Some(token) = &continuation_token {
                req = req.continuation_token(token);
            }
            let resp = req.send().await.map_err(|e| {
                io::Error::new(io::ErrorKind::Other, format!("S3 list failed: {}", e))
            })?;
            for obj in resp.contents() {
                if let Some(key) = obj.key() {
                    all_keys.push(key.to_string());
                }
            }
            if resp.is_truncated() == Some(true) {
                continuation_token = resp.next_continuation_token().map(|s| s.to_string());
            } else {
                break;
            }
        }
        Ok(all_keys)
    }

    /// List all S3 keys under an arbitrary prefix (not limited to self.prefix).
    pub(crate) async fn list_all_keys_with_prefix(&self, prefix: &str) -> io::Result<Vec<String>> {
        let mut all_keys = Vec::new();
        let mut continuation_token: Option<String> = None;
        loop {
            let mut req = self.client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(prefix);
            if let Some(token) = &continuation_token {
                req = req.continuation_token(token);
            }
            let resp = req.send().await.map_err(|e| {
                io::Error::new(io::ErrorKind::Other, format!("S3 list (prefix={}) failed: {}", prefix, e))
            })?;
            for obj in resp.contents() {
                if let Some(key) = obj.key() {
                    all_keys.push(key.to_string());
                }
            }
            if resp.is_truncated() == Some(true) {
                continuation_token = resp.next_continuation_token().map(|s| s.to_string());
            } else {
                break;
            }
        }
        Ok(all_keys)
    }
}

#[cfg(feature = "cloud")]
/// Check if an S3 error is a 404 / NoSuchKey.
pub(crate) fn is_not_found<E: std::fmt::Display + std::fmt::Debug>(
    err: &aws_sdk_s3::error::SdkError<E>,
) -> bool {
    match err {
        aws_sdk_s3::error::SdkError::ServiceError(service_err) => {
            service_err.raw().status().as_u16() == 404
        }
        _ => false,
    }
}

#[cfg(test)]
#[path = "test_s3_client.rs"]
mod tests;


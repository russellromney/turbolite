//! HTTP storage client for turbolite page groups and manifests.
//!
//! Speaks the Grabby `/v1/sync/pages/` API with Bearer token auth.
//! Sync interface (using block_on) matching S3Client's pattern.

use super::*;

/// HTTP storage client wrapping async reqwest with sync block_on interface.
pub(crate) struct HttpClient {
    client: reqwest::Client,
    endpoint: String,
    token: String,
    prefix: String,
    runtime: TokioHandle,
    /// Shared fence token, updated by the lease renewal loop.
    fence_token: Arc<AtomicU64>,
    pub(crate) fetch_count: AtomicU64,
    pub(crate) fetch_bytes: AtomicU64,
    pub(crate) put_count: AtomicU64,
    pub(crate) put_bytes: AtomicU64,
}

impl HttpClient {
    pub(crate) fn new(
        endpoint: &str,
        token: &str,
        prefix: &str,
        runtime: TokioHandle,
        fence_token: Arc<AtomicU64>,
    ) -> Self {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .expect("failed to build HTTP client");
        Self {
            client,
            endpoint: endpoint.trim_end_matches('/').to_string(),
            token: token.to_string(),
            prefix: prefix.trim_end_matches('/').to_string(),
            runtime,
            fence_token,
            fetch_count: AtomicU64::new(0),
            fetch_bytes: AtomicU64::new(0),
            put_count: AtomicU64::new(0),
            put_bytes: AtomicU64::new(0),
        }
    }

    fn url(&self, key: &str) -> String {
        if self.prefix.is_empty() {
            format!("{}/v1/sync/pages/{}", self.endpoint, key)
        } else {
            format!("{}/v1/sync/pages/{}/{}", self.endpoint, self.prefix, key)
        }
    }

    fn block_on<F: std::future::Future<Output = T>, T>(handle: &TokioHandle, fut: F) -> T {
        match TokioHandle::try_current() {
            Ok(_) => tokio::task::block_in_place(|| handle.block_on(fut)),
            Err(_) => handle.block_on(fut),
        }
    }

    // ── Async core operations ──

    async fn get_object_async(&self, key: &str) -> io::Result<Option<Vec<u8>>> {
        let mut retries = 0u32;
        loop {
            let resp = self
                .client
                .get(&self.url(key))
                .bearer_auth(&self.token)
                .send()
                .await
                .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("HTTP GET {}: {}", key, e)))?;

            match resp.status().as_u16() {
                200 => {
                    let bytes = resp
                        .bytes()
                        .await
                        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
                    self.fetch_count.fetch_add(1, Ordering::Relaxed);
                    self.fetch_bytes.fetch_add(bytes.len() as u64, Ordering::Relaxed);
                    return Ok(Some(bytes.to_vec()));
                }
                404 => return Ok(None),
                status => {
                    retries += 1;
                    if retries >= 3 {
                        return Err(io::Error::new(
                            io::ErrorKind::Other,
                            format!("HTTP GET {} returned {} after 3 retries", key, status),
                        ));
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(100 * (1 << retries))).await;
                }
            }
        }
    }

    #[allow(dead_code)]
    async fn range_get_async(&self, key: &str, start: u64, len: u32) -> io::Result<Option<Vec<u8>>> {
        let range = format!("bytes={}-{}", start, start + len as u64 - 1);
        let mut retries = 0u32;
        loop {
            let resp = self
                .client
                .get(&self.url(key))
                .bearer_auth(&self.token)
                .header("Range", &range)
                .send()
                .await
                .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("HTTP range GET {}: {}", key, e)))?;

            match resp.status().as_u16() {
                200 | 206 => {
                    let bytes = resp
                        .bytes()
                        .await
                        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
                    self.fetch_count.fetch_add(1, Ordering::Relaxed);
                    self.fetch_bytes.fetch_add(bytes.len() as u64, Ordering::Relaxed);
                    return Ok(Some(bytes.to_vec()));
                }
                404 => return Ok(None),
                status => {
                    retries += 1;
                    if retries >= 3 {
                        return Err(io::Error::new(
                            io::ErrorKind::Other,
                            format!("HTTP range GET {} ({}) returned {} after 3 retries", key, range, status),
                        ));
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(100 * (1 << retries))).await;
                }
            }
        }
    }

    fn fence_header_value(&self) -> Option<String> {
        let fence = self.fence_token.load(Ordering::SeqCst);
        if fence > 0 { Some(fence.to_string()) } else { None }
    }

    async fn put_object_async(&self, key: &str, data: Vec<u8>) -> io::Result<()> {
        let mut retries = 0u32;
        let data_len = data.len() as u64;
        loop {
            let mut req = self
                .client
                .put(&self.url(key))
                .bearer_auth(&self.token)
                .body(data.clone());
            if let Some(fence) = self.fence_header_value() {
                req = req.header("Fence-Token", fence);
            }
            let resp = req
                .send()
                .await
                .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("HTTP PUT {}: {}", key, e)))?;

            if resp.status().is_success() {
                self.put_count.fetch_add(1, Ordering::Relaxed);
                self.put_bytes.fetch_add(data_len, Ordering::Relaxed);
                return Ok(());
            }

            retries += 1;
            if retries >= 3 {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    format!("HTTP PUT {} returned {} after 3 retries", key, resp.status()),
                ));
            }
            tokio::time::sleep(std::time::Duration::from_millis(100 * (1 << retries))).await;
        }
    }

    async fn delete_objects_async(&self, keys: &[String]) -> io::Result<()> {
        if keys.is_empty() {
            return Ok(());
        }
        let url = format!("{}/v1/sync/pages/_delete", self.endpoint);

        let mut req = self
            .client
            .post(&url)
            .bearer_auth(&self.token)
            .json(&serde_json::json!({
                "keys": keys.iter().map(|k| {
                    if self.prefix.is_empty() { k.clone() }
                    else { format!("{}/{}", self.prefix, k) }
                }).collect::<Vec<_>>()
            }));
        if let Some(fence) = self.fence_header_value() {
            req = req.header("Fence-Token", fence);
        }
        let resp = req
            .send()
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("HTTP batch delete: {}", e)))?;

        if !resp.status().is_success() {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!("HTTP batch delete returned {}", resp.status()),
            ));
        }
        Ok(())
    }

    // ── Parallel batch operations ──

    #[allow(dead_code)]
    async fn get_page_groups_by_key_async(&self, keys: &[String]) -> io::Result<HashMap<String, Vec<u8>>> {
        let mut handles = Vec::with_capacity(keys.len());
        for key in keys {
            let client = self.client.clone();
            let url = self.url(key);
            let token = self.token.clone();
            let key = key.clone();
            handles.push(tokio::spawn(async move {
                let mut retries = 0u32;
                loop {
                    let resp = client
                        .get(&url)
                        .bearer_auth(&token)
                        .send()
                        .await
                        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

                    match resp.status().as_u16() {
                        200 => {
                            let bytes = resp.bytes().await
                                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
                            return Ok::<_, io::Error>(Some((key, bytes.to_vec())));
                        }
                        404 => return Ok(None),
                        _ => {
                            retries += 1;
                            if retries >= 3 {
                                return Err(io::Error::new(
                                    io::ErrorKind::Other,
                                    format!("HTTP GET {} failed after 3 retries", key),
                                ));
                            }
                            tokio::time::sleep(std::time::Duration::from_millis(100 * (1 << retries))).await;
                        }
                    }
                }
            }));
        }

        let mut result = HashMap::new();
        for handle in handles {
            match handle.await {
                Ok(Ok(Some((key, data)))) => {
                    self.fetch_count.fetch_add(1, Ordering::Relaxed);
                    self.fetch_bytes.fetch_add(data.len() as u64, Ordering::Relaxed);
                    result.insert(key, data);
                }
                Ok(Ok(None)) => {} // Not found, skip
                Ok(Err(e)) => return Err(e),
                Err(e) => return Err(io::Error::new(io::ErrorKind::Other, e)),
            }
        }
        Ok(result)
    }

    async fn put_page_groups_async(&self, groups: &[(String, Vec<u8>)]) -> io::Result<()> {
        let mut handles = Vec::with_capacity(groups.len());
        let fence_value = self.fence_header_value();
        for (key, data) in groups {
            let client = self.client.clone();
            let url = self.url(key);
            let token = self.token.clone();
            let data = data.clone();
            let fence = fence_value.clone();
            handles.push(tokio::spawn(async move {
                let mut retries = 0u32;
                loop {
                    let mut req = client
                        .put(&url)
                        .bearer_auth(&token)
                        .body(data.clone());
                    if let Some(ref f) = fence {
                        req = req.header("Fence-Token", f.as_str());
                    }
                    let resp = req
                        .send()
                        .await
                        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

                    if resp.status().is_success() {
                        return Ok::<_, io::Error>(data.len() as u64);
                    }
                    retries += 1;
                    if retries >= 3 {
                        return Err(io::Error::new(
                            io::ErrorKind::Other,
                            format!("HTTP PUT failed after 3 retries: {}", resp.status()),
                        ));
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(100 * (1 << retries))).await;
                }
            }));
        }

        for handle in handles {
            match handle.await {
                Ok(Ok(bytes)) => {
                    self.put_count.fetch_add(1, Ordering::Relaxed);
                    self.put_bytes.fetch_add(bytes, Ordering::Relaxed);
                }
                Ok(Err(e)) => return Err(e),
                Err(e) => return Err(io::Error::new(io::ErrorKind::Other, e)),
            }
        }
        Ok(())
    }

    // ── Sync wrappers (for VFS compatibility) ──

    pub(crate) fn get_page_group(&self, key: &str) -> io::Result<Option<Vec<u8>>> {
        Self::block_on(&self.runtime, self.get_object_async(key))
    }

    #[allow(dead_code)]
    pub(crate) fn get_page_groups_by_key(&self, keys: &[String]) -> io::Result<HashMap<String, Vec<u8>>> {
        Self::block_on(&self.runtime, self.get_page_groups_by_key_async(keys))
    }

    pub(crate) fn put_page_groups(&self, groups: &[(String, Vec<u8>)]) -> io::Result<()> {
        Self::block_on(&self.runtime, self.put_page_groups_async(groups))
    }

    pub(crate) fn delete_objects(&self, keys: &[String]) -> io::Result<()> {
        Self::block_on(&self.runtime, self.delete_objects_async(keys))
    }

    #[allow(dead_code)]
    pub(crate) fn range_get(&self, key: &str, start: u64, len: u32) -> io::Result<Option<Vec<u8>>> {
        Self::block_on(&self.runtime, self.range_get_async(key, start, len))
    }

    pub(crate) fn get_manifest(&self) -> io::Result<Option<Manifest>> {
        Self::block_on(&self.runtime, self.get_manifest_async())
    }

    async fn get_manifest_async(&self) -> io::Result<Option<Manifest>> {
        // Manifest is stored as a regular page object at manifest.msgpack
        let key = "manifest.msgpack";
        match self.get_object_async(key).await? {
            Some(bytes) => {
                let mut manifest: Manifest = rmp_serde::from_slice(&bytes).map_err(|e| {
                    io::Error::new(io::ErrorKind::InvalidData, format!("Invalid manifest msgpack: {}", e))
                })?;
                manifest.build_page_index();
                Ok(Some(manifest))
            }
            None => {
                // Fall back to JSON for old databases
                match self.get_object_async("manifest.json").await? {
                    Some(bytes) => {
                        let mut manifest: Manifest = serde_json::from_slice(&bytes).map_err(|e| {
                            io::Error::new(io::ErrorKind::InvalidData, format!("Invalid manifest JSON: {}", e))
                        })?;
                        manifest.build_page_index();
                        Ok(Some(manifest))
                    }
                    None => Ok(None),
                }
            }
        }
    }

    pub(crate) fn put_manifest(&self, manifest: &Manifest) -> io::Result<()> {
        Self::block_on(&self.runtime, self.put_manifest_async(manifest))
    }

    async fn put_manifest_async(&self, manifest: &Manifest) -> io::Result<()> {
        let data = rmp_serde::to_vec(manifest)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        self.put_object_async("manifest.msgpack", data).await
    }
}

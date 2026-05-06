use super::*;
use std::sync::{Arc, Mutex, OnceLock};

const LOCAL_STATE_FILE: &str = "local_state.msgpack";
static LOCAL_STATE_LOCKS: OnceLock<Mutex<HashMap<PathBuf, Arc<Mutex<()>>>>> = OnceLock::new();

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct LocalState {
    #[serde(default)]
    pub(crate) format_version: u32,
    #[serde(default)]
    pub(crate) manifest: Option<Manifest>,
    #[serde(default)]
    pub(crate) dirty_groups: Option<Vec<u64>>,
    #[serde(default)]
    pub(crate) page_bitmap: Option<Vec<u8>>,
    #[serde(default)]
    pub(crate) sub_chunk_tracker: Option<Vec<TrackerEntry>>,
    #[serde(default)]
    pub(crate) cache_index: Option<CacheIndexState>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct TrackerEntry {
    pub(crate) id: SubChunkId,
    pub(crate) tier: u8,
    pub(crate) access_count: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct CacheIndexState {
    pub(crate) entries: HashMap<u64, CacheIndexEntry>,
    pub(crate) next_offset: u64,
}

impl LocalState {
    fn normalized(mut self) -> Self {
        if self.format_version == 0 {
            self.format_version = 1;
        }
        self
    }
}

pub(crate) fn path(cache_dir: &Path) -> PathBuf {
    cache_dir.join(LOCAL_STATE_FILE)
}

pub(crate) fn load(cache_dir: &Path) -> io::Result<Option<LocalState>> {
    let path = path(cache_dir);
    let data = match fs::read(&path) {
        Ok(data) => data,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    let state: LocalState = rmp_serde::from_slice(&data).map_err(|e| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("deserialize local_state.msgpack: {e}"),
        )
    })?;
    Ok(Some(state.normalized()))
}

pub(crate) fn load_or_default(cache_dir: &Path) -> io::Result<LocalState> {
    Ok(load(cache_dir)?.unwrap_or_default().normalized())
}

pub(crate) fn persist(cache_dir: &Path, state: &LocalState) -> io::Result<()> {
    fs::create_dir_all(cache_dir)?;
    let path = path(cache_dir);
    let tmp = cache_dir.join("local_state.msgpack.tmp");
    let mut state = state.clone().normalized();
    state.format_version = 1;
    let data = rmp_serde::to_vec(&state).map_err(|e| {
        io::Error::new(
            io::ErrorKind::Other,
            format!("serialize local_state.msgpack: {e}"),
        )
    })?;
    {
        let mut file = FsOpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&tmp)?;
        use std::io::Write;
        file.write_all(&data)?;
        file.sync_all()?;
    }
    fs::rename(&tmp, path)?;
    // Fsync the directory so the rename itself is durable after power loss.
    // Some platforms/filesystems may not support directory fsync; surface the
    // error so callers know the local recovery state was not durably published.
    FsOpenOptions::new()
        .read(true)
        .open(cache_dir)?
        .sync_all()?;
    Ok(())
}

fn update_lock(cache_dir: &Path) -> Arc<Mutex<()>> {
    let locks = LOCAL_STATE_LOCKS.get_or_init(|| Mutex::new(HashMap::new()));
    let key = cache_dir.to_path_buf();
    let mut guard = locks.lock().expect("local_state lock table poisoned");
    guard
        .entry(key)
        .or_insert_with(|| Arc::new(Mutex::new(())))
        .clone()
}

pub(crate) fn update<F>(cache_dir: &Path, f: F) -> io::Result<()>
where
    F: FnOnce(&mut LocalState),
{
    let lock = update_lock(cache_dir);
    let _guard = lock.lock().expect("local_state update lock poisoned");
    let mut state = load_or_default(cache_dir)?;
    f(&mut state);
    persist(cache_dir, &state)
}

pub(crate) fn tracker_entries(
    present: &HashSet<SubChunkId>,
    tiers: &HashMap<SubChunkId, SubChunkTier>,
    counts: &HashMap<SubChunkId, u32>,
) -> Vec<TrackerEntry> {
    present
        .iter()
        .map(|id| TrackerEntry {
            id: *id,
            tier: tiers.get(id).copied().unwrap_or(SubChunkTier::Data) as u8,
            access_count: counts.get(id).copied().unwrap_or(0),
        })
        .collect()
}

pub(crate) fn tracker_maps(
    entries: Vec<TrackerEntry>,
) -> (
    HashSet<SubChunkId>,
    HashMap<SubChunkId, SubChunkTier>,
    HashMap<SubChunkId, u32>,
) {
    let mut present = HashSet::new();
    let mut tiers = HashMap::new();
    let mut counts = HashMap::new();
    for entry in entries {
        present.insert(entry.id);
        let tier = match entry.tier {
            0 => SubChunkTier::Pinned,
            1 => SubChunkTier::Index,
            _ => SubChunkTier::Data,
        };
        tiers.insert(entry.id, tier);
        if entry.access_count > 0 {
            counts.insert(entry.id, entry.access_count);
        }
    }
    (present, tiers, counts)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn update_preserves_concurrent_fields() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().to_path_buf();
        let barrier = Arc::new(std::sync::Barrier::new(2));

        let a_path = path.clone();
        let a_barrier = barrier.clone();
        let a = std::thread::spawn(move || {
            a_barrier.wait();
            update(&a_path, |state| {
                let mut manifest = Manifest::empty();
                manifest.version = 7;
                state.manifest = Some(manifest);
            })
            .expect("manifest update");
        });

        let b_path = path.clone();
        let b_barrier = barrier.clone();
        let b = std::thread::spawn(move || {
            b_barrier.wait();
            update(&b_path, |state| {
                state.page_bitmap = Some(vec![0b0010_0000]);
            })
            .expect("bitmap update");
        });

        a.join().expect("thread a");
        b.join().expect("thread b");

        let state = load(&path).expect("load").expect("state");
        assert_eq!(state.manifest.expect("manifest").version, 7);
        assert_eq!(state.page_bitmap.expect("bitmap"), vec![0b0010_0000]);
    }
}

use std::path::{Path, PathBuf};

use anyhow::{anyhow, Result};
use async_trait::async_trait;

use super::SharedTurboliteVfs;

/// walrust snapshot-source adapter over Turbolite checkpointed base state.
///
/// This lets walrust restore a SQLite database from Turbolite's published page
/// groups and then replay only the WAL deltas after Turbolite's checkpoint.
#[derive(Clone)]
pub struct TurboliteSnapshotSource {
    vfs: SharedTurboliteVfs,
}

impl TurboliteSnapshotSource {
    pub fn new(vfs: SharedTurboliteVfs) -> Self {
        Self { vfs }
    }

    pub fn vfs(&self) -> &SharedTurboliteVfs {
        &self.vfs
    }
}

#[async_trait]
impl walrust_core::SnapshotSource for TurboliteSnapshotSource {
    async fn materialize(&self, output: &Path) -> Result<u64> {
        let vfs = self.vfs.clone();
        let output: PathBuf = output.to_path_buf();
        tokio::task::spawn_blocking(move || vfs.shared_state().materialize_to_file(&output))
            .await
            .map_err(|e| anyhow!("turbolite materialize task panicked: {e}"))?
            .map_err(|e| anyhow!("turbolite materialize failed: {e}"))
    }

    async fn checkpoint_version(&self) -> Result<u64> {
        Ok(self.vfs.manifest().change_counter)
    }
}

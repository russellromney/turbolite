//! A minimal in-memory VFS implemented against [`sqlite_plugin`].
//!
//! It proves end-to-end that we can register a sqlite-plugin VFS and run a real
//! SQLite database through it (open + CRUD), independent of turbolite's tiered
//! storage / cache. The tiered backend in [`crate::tiered`] is the real VFS;
//! this is a standalone smoke check and a candidate for removal.

use std::collections::HashMap;
use std::sync::Mutex;

use sqlite_plugin::flags::{AccessFlags, LockLevel, OpenOpts};
use sqlite_plugin::vars;
use sqlite_plugin::vfs::{register_static, RegisterOpts, Vfs, VfsHandle, VfsResult};

/// One in-memory "file" (main DB, rollback journal, etc.).
#[derive(Default)]
struct MemFile {
    name: String,
    data: Vec<u8>,
    delete_on_close: bool,
}

/// Opaque per-open handle: an index into the in-memory file table.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct SpikeHandle {
    id: usize,
    readonly: bool,
}

impl VfsHandle for SpikeHandle {
    fn readonly(&self) -> bool {
        self.readonly
    }
    fn in_memory(&self) -> bool {
        false
    }
}

#[derive(Default)]
struct SpikeState {
    next_id: usize,
    files: HashMap<usize, MemFile>,
}

/// Minimal in-memory VFS. Single mutex over all state — correctness over
/// concurrency for the spike (the real tiered VFS lands in Phase 2).
pub struct SpikeVfs {
    state: Mutex<SpikeState>,
}

impl SpikeVfs {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(SpikeState::default()),
        }
    }
}

impl Default for SpikeVfs {
    fn default() -> Self {
        Self::new()
    }
}

impl Vfs for SpikeVfs {
    type Handle = SpikeHandle;

    fn open(&self, path: Option<&str>, opts: OpenOpts) -> VfsResult<SpikeHandle> {
        let mut st = self.state.lock().unwrap();
        let readonly = opts.mode().is_readonly();
        // Reuse an existing handle for the same named file.
        if let Some(path) = path {
            if let Some(id) = st
                .files
                .iter()
                .find_map(|(id, f)| (f.name == path).then_some(*id))
            {
                return Ok(SpikeHandle { id, readonly });
            }
        }
        let id = st.next_id;
        st.next_id += 1;
        st.files.insert(
            id,
            MemFile {
                name: path.unwrap_or_default().to_owned(),
                data: Vec::new(),
                delete_on_close: opts.delete_on_close(),
            },
        );
        Ok(SpikeHandle { id, readonly })
    }

    fn delete(&self, path: &str) -> VfsResult<()> {
        self.state.lock().unwrap().files.retain(|_, f| f.name != path);
        Ok(())
    }

    fn access(&self, path: &str, _flags: AccessFlags) -> VfsResult<bool> {
        Ok(self
            .state
            .lock()
            .unwrap()
            .files
            .values()
            .any(|f| f.name == path))
    }

    fn file_size(&self, h: &mut SpikeHandle) -> VfsResult<usize> {
        Ok(self
            .state
            .lock()
            .unwrap()
            .files
            .get(&h.id)
            .map_or(0, |f| f.data.len()))
    }

    fn truncate(&self, h: &mut SpikeHandle, size: usize) -> VfsResult<()> {
        if let Some(f) = self.state.lock().unwrap().files.get_mut(&h.id) {
            f.data.resize(size, 0);
        }
        Ok(())
    }

    fn write(&self, h: &mut SpikeHandle, offset: usize, buf: &[u8]) -> VfsResult<usize> {
        let mut st = self.state.lock().unwrap();
        let f = st.files.get_mut(&h.id).ok_or(vars::SQLITE_IOERR_WRITE)?;
        let end = offset + buf.len();
        if end > f.data.len() {
            f.data.resize(end, 0);
        }
        f.data[offset..end].copy_from_slice(buf);
        Ok(buf.len())
    }

    fn read(&self, h: &mut SpikeHandle, offset: usize, buf: &mut [u8]) -> VfsResult<usize> {
        let st = self.state.lock().unwrap();
        let f = st.files.get(&h.id).ok_or(vars::SQLITE_IOERR_READ)?;
        if offset >= f.data.len() {
            return Ok(0);
        }
        let len = buf.len().min(f.data.len() - offset);
        buf[..len].copy_from_slice(&f.data[offset..offset + len]);
        Ok(len)
    }

    fn lock(&self, _h: &mut SpikeHandle, _level: LockLevel) -> VfsResult<()> {
        Ok(())
    }

    fn unlock(&self, _h: &mut SpikeHandle, _level: LockLevel) -> VfsResult<()> {
        Ok(())
    }

    fn check_reserved_lock(&self, _h: &mut SpikeHandle) -> VfsResult<bool> {
        Ok(false)
    }

    fn sync(&self, _h: &mut SpikeHandle) -> VfsResult<()> {
        Ok(())
    }

    fn close(&self, h: SpikeHandle) -> VfsResult<()> {
        let mut st = self.state.lock().unwrap();
        if st.files.get(&h.id).is_some_and(|f| f.delete_on_close) {
            st.files.remove(&h.id);
        }
        Ok(())
    }
}

/// Register the spike VFS under `name` (not the default VFS).
pub fn register(name: &str) -> Result<(), i32> {
    let cname = std::ffi::CString::new(name).map_err(|_| vars::SQLITE_ERROR)?;
    register_static(cname, SpikeVfs::new(), RegisterOpts { make_default: false })?;
    Ok(())
}

#[cfg(test)]
mod spike_tests {
    //! End-to-end: a real SQLite DB driven through the sqlite-plugin VFS.
    use rusqlite::{Connection, OpenFlags};

    #[test]
    fn crud_through_sqlite_plugin_vfs() {
        super::register("spike_vfs").expect("register spike vfs");
        let conn = Connection::open_with_flags_and_vfs(
            "spike_main.db",
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
            "spike_vfs",
        )
        .expect("open db through spike vfs");

        conn.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);")
            .expect("create table");
        conn.execute("INSERT INTO t (v) VALUES (?1)", ["hello"]).unwrap();
        conn.execute("INSERT INTO t (v) VALUES (?1)", ["world"]).unwrap();

        let n: i64 = conn
            .query_row("SELECT count(*) FROM t", [], |r| r.get(0))
            .unwrap();
        assert_eq!(n, 2, "rows visible through sqlite-plugin VFS");

        let v: String = conn
            .query_row("SELECT v FROM t WHERE id = 1", [], |r| r.get(0))
            .unwrap();
        assert_eq!(v, "hello", "row content round-trips through the VFS");
    }
}

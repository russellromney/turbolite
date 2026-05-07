/**
 * turbolite -- SQLite with compressed page groups and optional S3 cloud storage for Node.js.
 *
 * Local mode (default), file-first:
 *
 *   import { connect } from 'turbolite';
 *   const db = connect('/data/app.db');
 *   // /data/app.db is the local page image (turbolite-owned).
 *   // /data/app.db-turbolite/ holds hidden implementation state
 *   // (manifest, cache, staging logs).
 *   // db is a better-sqlite3 Database with the full API:
 *   // prepared statements, param binding, transactions, etc.
 *
 * S3 cloud mode:
 *
 *   const db = connect('/data/app.db', {
 *     mode: 's3',
 *     bucket: 'my-bucket',
 *     endpoint: 'https://t3.storage.dev',
 *   });
 *
 * Note: app.db is turbolite's compressed page image. It is not promised
 * to be opened directly by stock sqlite3. To get a stock SQLite file
 * that the standard sqlite3 CLI can open, run
 *   db.exec("VACUUM INTO 'export.sqlite'")
 * while connected via turbolite.
 */

import path from 'path';
import fs from 'fs';
import os from 'os';
import Database from 'better-sqlite3';

export interface ConnectOptions {
  /** Storage mode: "local" (default) or "s3". */
  mode?: 'local' | 's3';
  /** S3 bucket name (required when mode = "s3"). */
  bucket?: string;
  /** S3 key prefix (mode = "s3", default "turbolite"). */
  prefix?: string;
  /** S3 endpoint URL, e.g. for Tigris or MinIO (mode = "s3"). */
  endpoint?: string;
  /** AWS region (mode = "s3"). */
  region?: string;
  /** Local cache directory (mode = "s3", defaults to /tmp/turbolite). */
  cacheDir?: string;
  /** Zstd compression level 1-22 (default 3). */
  compressionLevel?: number;
  /** Prefetch worker threads (default num_cpus + 1). */
  prefetchThreads?: number;
  /** Open in read-only mode. */
  readOnly?: boolean;
  /**
   * In-memory page cache size (default "64MB"). turbolite manages its own
   * manifest-aware page cache. Set to "0" to disable.
   */
  pageCache?: string;
}

let _vfsCounter = 0;

/**
 * Find the bundled loadable extension binary.
 * Returns the path without file extension (SQLite convention).
 */
function findExt(): string {
  const envPath = process.env.TURBOLITE_EXT_PATH;
  if (envPath) {
    const resolved = envPath.replace(/\.(dylib|so|dll)$/, '');
    const withExt = addPlatformExt(resolved);
    if (fs.existsSync(withExt)) return resolved;
    throw new Error(
      `TURBOLITE_EXT_PATH set but extension not found: ${withExt}`
    );
  }

  // Look in the package root (one level up from src/ at build time,
  // or same dir as compiled dist/)
  const searchDirs = [__dirname, path.join(__dirname, '..')];
  const plat = os.platform();
  const arch = os.arch();
  const ext = plat === 'darwin' ? 'dylib' : plat === 'win32' ? 'dll' : 'so';

  // Platform-specific name (from CI release builds), then generic name
  const platKey = `${plat === 'darwin' ? 'darwin' : plat === 'win32' ? 'win32' : 'linux'}-${arch === 'arm64' ? 'arm64' : 'x64'}`;
  const candidates = [
    `turbolite.${platKey}.${ext}`,
    `turbolite.${ext}`,
    `libturbolite_ffi.${ext}`,
    `libturbolite.${ext}`,
  ];

  for (const dir of searchDirs) {
    for (const name of candidates) {
      const fullPath = path.join(dir, name);
      if (fs.existsSync(fullPath)) {
        return fullPath.replace(/\.(dylib|so|dll)$/, '');
      }
    }
  }

  throw new Error(
    `turbolite extension not found. Build with: npm run build-ext`
  );
}

function addPlatformExt(basePath: string): string {
  const plat = os.platform();
  const ext = plat === 'darwin' ? 'dylib' : plat === 'win32' ? 'dll' : 'so';
  return `${basePath}.${ext}`;
}

/**
 * Open a better-sqlite3 Database with a URI filename.
 *
 * better-sqlite3 validates `path.dirname(filename)` exists before calling
 * sqlite3_open_v2, but this check doesn't understand URI filenames
 * (e.g. "file:/path/to/db?vfs=turbolite"). The underlying SQLite is compiled
 * with SQLITE_USE_URI=1, so it handles URIs correctly. We validate the actual
 * file path ourselves, then temporarily bypass the JS-level directory check.
 */
function openUri(
  uri: string,
  absPath: string,
  options: Database.Options,
): Database.Database {
  const dir = path.dirname(absPath);
  if (!fs.existsSync(dir)) {
    throw new TypeError(
      `Cannot open database because the directory does not exist: ${dir}`
    );
  }
  // Temporarily make better-sqlite3's directory check pass for the URI string.
  // Safe because better-sqlite3 is fully synchronous.
  const uriDir = path.dirname(uri);
  const origExistsSync = fs.existsSync;
  (fs as any).existsSync = (p: string) => {
    if (p === uriDir) return true;
    return origExistsSync.call(fs, p);
  };
  try {
    return new Database(uri, options);
  } finally {
    (fs as any).existsSync = origExistsSync;
  }
}

// Bootstrap connection for extension loading and VFS registration.
// Lazily created and kept alive for the process lifetime so that
// turbolite_register_vfs() calls can be made at any time.
let _bootstrap: Database.Database | null = null;
let _loadedS3 = false;

function ensureBootstrap(): Database.Database {
  if (_bootstrap) return _bootstrap;
  _bootstrap = new Database(':memory:');
  _bootstrap.loadExtension(findExt());
  if (process.env.TURBOLITE_BUCKET) {
    _loadedS3 = true;
  }
  return _bootstrap;
}

/**
 * Load the turbolite extension into an existing better-sqlite3 Database.
 * Registers the "turbolite" VFS (local mode) globally. If TURBOLITE_BUCKET
 * is set, also registers "turbolite-s3".
 */
export function load(db: Database.Database): void {
  db.loadExtension(findExt());
}

/**
 * Return the hidden sidecar directory path for a file-first database path.
 *
 * For `/data/app.db` this is `/data/app.db-turbolite`. Useful for tests or
 * tooling that asserts on layout without hardcoding the suffix rule.
 */
export function stateDirForDatabasePath(dbPath: string): string {
  return `${dbPath}-turbolite`;
}

/**
 * Open a turbolite database. Returns a standard better-sqlite3 Database
 * with full API: prepared statements, param binding, transactions,
 * user-defined functions, aggregates, etc.
 *
 * Each call creates an isolated VFS instance (local mode) so multiple
 * databases in the same process don't share manifest or cache state.
 */
export function connect(
  dbPath: string,
  opts?: ConnectOptions,
): Database.Database {
  const {
    mode = 'local',
    bucket,
    prefix,
    endpoint,
    region,
    cacheDir,
    compressionLevel,
    prefetchThreads,
    readOnly = false,
    pageCache = '64MB',
  } = opts ?? {};

  if (mode !== 'local' && mode !== 's3') {
    throw new Error(`mode must be 'local' or 's3', got '${mode}'`);
  }

  // Set env vars BEFORE loading the extension. The C init function checks
  // TURBOLITE_BUCKET to decide whether to register turbolite-s3.
  if (mode === 's3') {
    const effectiveBucket = bucket ?? process.env.TURBOLITE_BUCKET;
    if (!effectiveBucket) {
      throw new Error(
        "mode='s3' requires a bucket. Pass bucket or set TURBOLITE_BUCKET."
      );
    }
    process.env.TURBOLITE_BUCKET = effectiveBucket;
    if (prefix != null) process.env.TURBOLITE_PREFIX = prefix;
    if (endpoint != null) process.env.TURBOLITE_ENDPOINT_URL = endpoint;
    if (region != null) process.env.TURBOLITE_REGION = region;
    if (cacheDir != null) process.env.TURBOLITE_CACHE_DIR = cacheDir;
    if (readOnly) process.env.TURBOLITE_READ_ONLY = 'true';
  }

  if (compressionLevel != null) {
    process.env.TURBOLITE_COMPRESSION_LEVEL = String(compressionLevel);
  }
  if (prefetchThreads != null) {
    process.env.TURBOLITE_PREFETCH_THREADS = String(prefetchThreads);
  }
  process.env.TURBOLITE_MEM_CACHE_BUDGET = pageCache;

  // Ensure the extension is loaded (registers SQL functions + global VFS).
  const bootstrap = ensureBootstrap();

  const absPath = path.resolve(dbPath);
  let vfsName: string;

  if (mode === 'local') {
    // File-first: each local database gets its own VFS keyed to the
    // user-supplied path. The path itself is the local page image; sidecar
    // metadata lives at `${absPath}-turbolite/`.
    vfsName = `turbolite-node-${_vfsCounter++}`;
    const dbDir = path.dirname(absPath);
    if (!fs.existsSync(dbDir)) {
      throw new Error(
        `Cannot open database because the directory does not exist: ${dbDir}`
      );
    }
    const rc = bootstrap
      .prepare('SELECT turbolite_register_file_first_vfs(?, ?)')
      .pluck()
      .get(vfsName, absPath) as number;
    if (rc !== 0) {
      throw new Error(
        `Failed to register file-first VFS '${vfsName}' for ${absPath}`
      );
    }
  } else {
    // S3 mode uses the global "turbolite-s3" VFS registered at init time.
    if (!_loadedS3) {
      throw new Error(
        'Cannot use S3 mode: TURBOLITE_BUCKET was not set when the extension ' +
        'was first loaded. Set TURBOLITE_BUCKET in the environment before ' +
        'the first turbolite.connect() call.'
      );
    }
    vfsName = 'turbolite-s3';
  }

  const db = openUri(`file:${absPath}?vfs=${vfsName}`, absPath, {
    readonly: readOnly,
  });

  // turbolite manages its own manifest-aware page cache. Disable SQLite's
  // built-in cache so all reads go through turbolite's VFS.
  db.pragma('cache_size = 0');

  if (mode === 's3') {
    db.pragma('page_size = 65536');
    db.pragma('journal_mode = WAL');
  }

  return db;
}

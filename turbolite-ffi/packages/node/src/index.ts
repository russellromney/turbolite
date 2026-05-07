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
import crypto from 'crypto';
import Database from 'better-sqlite3';

export interface ConnectOptions {
  /** Storage mode: "local" (default) or "s3". */
  mode?: 'local' | 's3';
  /** S3 bucket name (required when mode = "s3"). */
  bucket?: string;
  /** S3 key prefix. Defaults to a stable prefix derived from the database path. */
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
const _s3VfsByFingerprint = new Map<string, string>();

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

function s3Fingerprint(parts: Array<string | number | boolean | undefined | null>): string {
  return parts
    .map((part) => {
      if (part == null) return '';
      return String(part);
    })
    .join('\0');
}

function defaultS3VfsName(fingerprint: string): string {
  return `turbolite-s3-${crypto
    .createHash('sha256')
    .update(fingerprint)
    .digest('hex')
    .slice(0, 16)}`;
}

function defaultS3Prefix(absPath: string): string {
  const stem = path.basename(absPath) || 'database';
  const digest = crypto.createHash('sha256').update(absPath).digest('hex').slice(0, 16);
  return `turbolite/${stem}-${digest}`;
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

function ensureBootstrap(): Database.Database {
  if (_bootstrap) return _bootstrap;
  _bootstrap = new Database(':memory:');
  _bootstrap.loadExtension(findExt());
  return _bootstrap;
}

/**
 * Load the turbolite extension into an existing better-sqlite3 Database.
 * Registers the extension SQL helpers and default VFS entries.
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

  const effectiveBucket = bucket ?? process.env.TURBOLITE_BUCKET;
  const absPath = path.resolve(dbPath);
  const effectivePrefix =
    prefix ?? process.env.TURBOLITE_PREFIX ?? defaultS3Prefix(absPath);
  const effectiveEndpoint =
    endpoint ??
    process.env.TURBOLITE_ENDPOINT_URL ??
    process.env.AWS_ENDPOINT_URL;
  const effectiveRegion = region ?? process.env.TURBOLITE_REGION ?? process.env.AWS_REGION;

  if (mode === 's3') {
    if (!effectiveBucket) {
      throw new Error(
        "mode='s3' requires a bucket. Pass bucket or set TURBOLITE_BUCKET."
      );
    }
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
    const fingerprint = s3Fingerprint([
      absPath,
      effectiveBucket,
      effectivePrefix,
      effectiveEndpoint,
      effectiveRegion,
      cacheDir,
      compressionLevel,
      prefetchThreads,
      readOnly,
      pageCache,
    ]);
    let registered = _s3VfsByFingerprint.get(fingerprint);
    if (!registered) {
      registered = defaultS3VfsName(fingerprint);
      const rc = bootstrap
        .prepare('SELECT turbolite_register_s3_file_first_vfs(?, ?, ?, ?, ?, ?)')
        .pluck()
        .get(
          registered,
          absPath,
          effectiveBucket,
          effectivePrefix,
          effectiveEndpoint ?? null,
          effectiveRegion ?? null,
        ) as number;
      if (rc !== 0) {
        throw new Error(
          `Failed to register S3 file-first VFS '${registered}' for ${absPath}`
        );
      }
      _s3VfsByFingerprint.set(fingerprint, registered);
    }
    if (!registered) {
      throw new Error(
        `Failed to resolve S3 file-first VFS for ${absPath}`
      );
    }
    vfsName = registered;
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

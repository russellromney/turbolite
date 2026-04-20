"use strict";
/**
 * turbolite -- SQLite with compressed page groups and optional S3 cloud storage for Node.js.
 *
 * Local mode (default):
 *
 *   import { connect } from 'turbolite';
 *   const db = connect('my.db');
 *   // db is a better-sqlite3 Database with full API:
 *   // prepared statements, param binding, transactions, etc.
 *
 * S3 cloud mode:
 *
 *   const db = connect('my.db', {
 *     mode: 's3',
 *     bucket: 'my-bucket',
 *     endpoint: 'https://t3.storage.dev',
 *   });
 */
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.load = load;
exports.connect = connect;
const path_1 = __importDefault(require("path"));
const fs_1 = __importDefault(require("fs"));
const os_1 = __importDefault(require("os"));
const better_sqlite3_1 = __importDefault(require("better-sqlite3"));
let _vfsCounter = 0;
/**
 * Find the bundled loadable extension binary.
 * Returns the path without file extension (SQLite convention).
 */
function findExt() {
    const envPath = process.env.TURBOLITE_EXT_PATH;
    if (envPath) {
        const resolved = envPath.replace(/\.(dylib|so|dll)$/, '');
        const withExt = addPlatformExt(resolved);
        if (fs_1.default.existsSync(withExt))
            return resolved;
        throw new Error(`TURBOLITE_EXT_PATH set but extension not found: ${withExt}`);
    }
    // Look in the package root (one level up from src/ at build time,
    // or same dir as compiled dist/)
    const searchDirs = [__dirname, path_1.default.join(__dirname, '..')];
    const plat = os_1.default.platform();
    const arch = os_1.default.arch();
    const ext = plat === 'darwin' ? 'dylib' : plat === 'win32' ? 'dll' : 'so';
    // Platform-specific name (from CI release builds), then generic name
    const platKey = `${plat === 'darwin' ? 'darwin' : plat === 'win32' ? 'win32' : 'linux'}-${arch === 'arm64' ? 'arm64' : 'x64'}`;
    const candidates = [
        `turbolite.${platKey}.${ext}`,
        `turbolite.${ext}`,
        `libturbolite.${ext}`,
    ];
    for (const dir of searchDirs) {
        for (const name of candidates) {
            const fullPath = path_1.default.join(dir, name);
            if (fs_1.default.existsSync(fullPath)) {
                return fullPath.replace(/\.(dylib|so|dll)$/, '');
            }
        }
    }
    throw new Error(`turbolite extension not found. Build with: npm run build-ext`);
}
function addPlatformExt(basePath) {
    const plat = os_1.default.platform();
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
function openUri(uri, absPath, options) {
    const dir = path_1.default.dirname(absPath);
    if (!fs_1.default.existsSync(dir)) {
        throw new TypeError(`Cannot open database because the directory does not exist: ${dir}`);
    }
    // Temporarily make better-sqlite3's directory check pass for the URI string.
    // Safe because better-sqlite3 is fully synchronous.
    const uriDir = path_1.default.dirname(uri);
    const origExistsSync = fs_1.default.existsSync;
    fs_1.default.existsSync = (p) => {
        if (p === uriDir)
            return true;
        return origExistsSync.call(fs_1.default, p);
    };
    try {
        return new better_sqlite3_1.default(uri, options);
    }
    finally {
        fs_1.default.existsSync = origExistsSync;
    }
}
// Bootstrap connection for extension loading and VFS registration.
// Lazily created and kept alive for the process lifetime so that
// turbolite_register_vfs() calls can be made at any time.
let _bootstrap = null;
let _loadedS3 = false;
function ensureBootstrap() {
    if (_bootstrap)
        return _bootstrap;
    _bootstrap = new better_sqlite3_1.default(':memory:');
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
function load(db) {
    db.loadExtension(findExt());
}
/**
 * Open a turbolite database. Returns a standard better-sqlite3 Database
 * with full API: prepared statements, param binding, transactions,
 * user-defined functions, aggregates, etc.
 *
 * Each call creates an isolated VFS instance (local mode) so multiple
 * databases in the same process don't share manifest or cache state.
 */
function connect(dbPath, opts) {
    const { mode = 'local', bucket, prefix, endpoint, region, cacheDir, compressionLevel, prefetchThreads, readOnly = false, pageCache = '64MB', } = opts ?? {};
    if (mode !== 'local' && mode !== 's3') {
        throw new Error(`mode must be 'local' or 's3', got '${mode}'`);
    }
    // Set env vars BEFORE loading the extension. The C init function checks
    // TURBOLITE_BUCKET to decide whether to register turbolite-s3.
    if (mode === 's3') {
        const effectiveBucket = bucket ?? process.env.TURBOLITE_BUCKET;
        if (!effectiveBucket) {
            throw new Error("mode='s3' requires a bucket. Pass bucket or set TURBOLITE_BUCKET.");
        }
        process.env.TURBOLITE_BUCKET = effectiveBucket;
        if (prefix != null)
            process.env.TURBOLITE_PREFIX = prefix;
        if (endpoint != null)
            process.env.TURBOLITE_ENDPOINT_URL = endpoint;
        if (region != null)
            process.env.TURBOLITE_REGION = region;
        if (cacheDir != null)
            process.env.TURBOLITE_CACHE_DIR = cacheDir;
        if (readOnly)
            process.env.TURBOLITE_READ_ONLY = 'true';
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
    const absPath = path_1.default.resolve(dbPath);
    let vfsName;
    if (mode === 'local') {
        // Each local database gets its own VFS with isolated manifest, cache,
        // and page group state. This matches the napi-rs behavior where each
        // Database() call created a separate VFS instance.
        vfsName = `turbolite-${_vfsCounter++}`;
        const dbDir = path_1.default.dirname(absPath);
        if (!fs_1.default.existsSync(dbDir)) {
            throw new Error(`Cannot open database because the directory does not exist: ${dbDir}`);
        }
        const rc = bootstrap
            .prepare('SELECT turbolite_register_vfs(?, ?)')
            .pluck()
            .get(vfsName, dbDir);
        if (rc !== 0) {
            throw new Error(`Failed to register VFS '${vfsName}' for ${dbDir}`);
        }
    }
    else {
        // S3 mode uses the global "turbolite-s3" VFS registered at init time.
        if (!_loadedS3) {
            throw new Error('Cannot use S3 mode: TURBOLITE_BUCKET was not set when the extension ' +
                'was first loaded. Set TURBOLITE_BUCKET in the environment before ' +
                'the first turbolite.connect() call.');
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
//# sourceMappingURL=index.js.map
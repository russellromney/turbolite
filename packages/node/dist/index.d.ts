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
/**
 * Load the turbolite extension into an existing better-sqlite3 Database.
 * Registers the "turbolite" VFS (local mode) globally. If TURBOLITE_BUCKET
 * is set, also registers "turbolite-s3".
 */
export declare function load(db: Database.Database): void;
/**
 * Open a turbolite database. Returns a standard better-sqlite3 Database
 * with full API: prepared statements, param binding, transactions,
 * user-defined functions, aggregates, etc.
 *
 * Each call creates an isolated VFS instance (local mode) so multiple
 * databases in the same process don't share manifest or cache state.
 */
export declare function connect(dbPath: string, opts?: ConnectOptions): Database.Database;
//# sourceMappingURL=index.d.ts.map
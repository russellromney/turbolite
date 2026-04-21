// @ts-ignore wa-sqlite module resolution
import SQLiteAsyncESMFactory from "wa-sqlite/dist/wa-sqlite-async.mjs";
// @ts-ignore wa-sqlite module resolution
import * as SQLite from "wa-sqlite";
import { parseManifest, buildPageIndex } from "./manifest.js";
import { PageFetcher } from "./fetcher.js";
import { PageCache } from "./cache.js";
import { TurboliteVFS } from "./vfs.js";
import type { VfsState } from "./vfs.js";
import type { TurboliteConfig, CacheStats, Manifest, PageLocation } from "./types.js";

export type { TurboliteConfig, CacheStats, Manifest, PageLocation };
export type { FrameEntry, BTreeManifestEntry, GroupingStrategy } from "./types.js";

/**
 * Read-only browser client for turbolite S3-backed SQLite databases.
 *
 * Usage:
 * ```ts
 * const db = await TurboliteDB.open({
 *   baseUrl: "/api/turbolite",
 *   prefix: "databases/my-app",
 * });
 *
 * const rows = await db.query("SELECT * FROM users WHERE id = ?", [42]);
 * db.close();
 * ```
 */
let vfsCounter = 0;

export class TurboliteDB {
  private sqlite3: any;
  private db: number;
  private cache: PageCache;
  private fetcher: PageFetcher;
  private vfsState: VfsState;
  private config: TurboliteConfig;
  private _closed = false;

  private constructor(
    sqlite3: any,
    db: number,
    cache: PageCache,
    fetcher: PageFetcher,
    vfsState: VfsState,
    config: TurboliteConfig,
  ) {
    this.sqlite3 = sqlite3;
    this.db = db;
    this.cache = cache;
    this.fetcher = fetcher;
    this.vfsState = vfsState;
    this.config = config;
  }

  /**
   * Open a turbolite database from S3 (or a proxy).
   *
   * 1. Fetches the manifest
   * 2. Prefetches interior B-tree pages (for instant traversal)
   * 3. Registers a read-only VFS with wa-sqlite
   * 4. Opens the database
   */
  static async open(config: TurboliteConfig): Promise<TurboliteDB> {
    const fetcher = new PageFetcher(config);
    const cache = new PageCache(config);

    // Fetch and parse manifest
    const manifestBytes = await fetcher.fetchManifest();
    const manifest = parseManifest(manifestBytes);
    const pageIndex = buildPageIndex(manifest);

    // Shared state: VFS reads from this, refresh() swaps it
    const vfsState: VfsState = { manifest, pageIndex };

    // Initialize OPFS cache (keyed by manifest version for coherency)
    const dbName = config.prefix.replace(/\//g, "_");
    await cache.init(dbName, manifest.version);

    // Prefetch interior + index bundles in parallel
    await TurboliteDB.prefetchBundles(manifest, fetcher, cache);

    // Initialize wa-sqlite (async build for handleAsync support)
    const module = await SQLiteAsyncESMFactory();
    const sqlite3 = SQLite.Factory(module);

    // Register our read-only VFS (counter ensures uniqueness across multiple opens)
    const vfsName = `turbolite-${dbName}-${vfsCounter++}`;
    const vfs = new TurboliteVFS(vfsName, vfsState, cache, fetcher);
    sqlite3.vfs_register(vfs, false);

    // Open database in read-only mode with our VFS
    const db = await sqlite3.open_v2(
      "db.sqlite",
      SQLite.SQLITE_OPEN_READONLY,
      vfsName,
    );

    return new TurboliteDB(sqlite3, db, cache, fetcher, vfsState, config);
  }

  /**
   * Execute a query and return all rows as objects.
   *
   * ```ts
   * const users = await db.query<{ id: number; name: string }>(
   *   "SELECT id, name FROM users WHERE active = ?",
   *   [1],
   * );
   * ```
   */
  async query<T extends Record<string, unknown> = Record<string, unknown>>(
    sql: string,
    params: (string | number | null | Uint8Array)[] = [],
  ): Promise<T[]> {
    this.ensureOpen();
    const rows: T[] = [];

    for await (const stmt of this.sqlite3.statements(this.db, sql)) {
      if (stmt === null) continue;

      // Bind parameters
      if (params.length > 0) {
        this.sqlite3.bind_collection(stmt, params);
      }

      // Collect column names
      const colNames = this.sqlite3.column_names(stmt);

      // Step through rows
      while ((await this.sqlite3.step(stmt)) === SQLite.SQLITE_ROW) {
        const row: Record<string, unknown> = {};
        for (let i = 0; i < colNames.length; i++) {
          row[colNames[i]] = this.sqlite3.column(stmt, i);
        }
        rows.push(row as T);
      }
    }

    return rows;
  }

  /**
   * Execute a statement (or multiple) without returning rows.
   * Useful for PRAGMA commands.
   */
  async exec(sql: string): Promise<void> {
    this.ensureOpen();
    await this.sqlite3.exec(this.db, sql);
  }

  /**
   * Re-fetch the manifest from S3 and update internal state.
   * Call this for long-lived connections when the backend may have
   * re-imported the database. Clears stale cached pages, prefetches
   * new interior/index bundles.
   *
   * Returns true if the manifest version changed, false if unchanged.
   */
  async refresh(): Promise<boolean> {
    this.ensureOpen();
    const manifestBytes = await this.fetcher.fetchManifest();
    const newManifest = parseManifest(manifestBytes);

    if (newManifest.version === this.vfsState.manifest.version) {
      return false;
    }

    const newPageIndex = buildPageIndex(newManifest);

    // Clear stale pages and re-init OPFS with new version
    await this.cache.clear();
    const dbName = this.config.prefix.replace(/\//g, "_");
    await this.cache.init(dbName, newManifest.version);

    // Prefetch new bundles
    await TurboliteDB.prefetchBundles(newManifest, this.fetcher, this.cache);

    // Swap manifest in the shared VfsState (VFS sees new data immediately)
    this.vfsState.manifest = newManifest;
    this.vfsState.pageIndex = newPageIndex;

    return true;
  }

  /** Prefetch interior + index bundles into cache. */
  private static async prefetchBundles(
    manifest: Manifest,
    fetcher: PageFetcher,
    cache: PageCache,
  ): Promise<void> {
    const bundlePromises = [
      ...Object.values(manifest.interior_chunk_keys),
      ...Object.values(manifest.index_chunk_keys),
    ].map(async (key) => {
      const bundle = await fetcher.fetchBundle(key);
      await cache.putPages(bundle.map((b) => [b.pageNum, b.data]));
    });
    await Promise.all(bundlePromises);
  }

  /** Get cache and fetch statistics. */
  stats(): CacheStats {
    return this.cache.getStats(this.fetcher);
  }

  /** Get the parsed manifest. */
  getManifest(): Manifest {
    return this.vfsState.manifest;
  }

  /** Clear all cached pages (memory + OPFS). */
  async clearCache(): Promise<void> {
    await this.cache.clear();
  }

  /** Close the database and release resources. */
  async close(): Promise<void> {
    if (this._closed) return;
    this._closed = true;
    await this.sqlite3.close(this.db);
  }

  private ensureOpen(): void {
    if (this._closed) throw new Error("Database is closed");
  }
}

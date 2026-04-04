import type { CacheStats, Manifest, TurboliteConfig, PageLocation } from "./types.js";
import type { PageFetcher } from "./fetcher.js";
import { pageLocation } from "./manifest.js";

/**
 * Two-tier page cache: in-memory LRU + optional OPFS persistence.
 *
 * When SQLite reads a page:
 * 1. Check in-memory cache (instant)
 * 2. Check OPFS cache if enabled (fast, async)
 * 3. Fetch from remote (S3/proxy), decompress, cache everywhere
 *
 * The unit of fetch is the page group (or frame within a seekable group).
 * All pages from a fetched group are cached, amortizing the S3 cost.
 */
export class PageCache {
  private readonly memCache = new Map<number, Uint8Array>();
  private memBytes = 0;
  private readonly maxMemBytes: number;
  /** LRU tracking: pageNum -> last access generation. Higher = more recently used. */
  private readonly accessGen = new Map<number, number>();
  private generation = 0;
  private opfsDir: FileSystemDirectoryHandle | null = null;
  private readonly useOpfs: boolean;

  hits = 0;
  misses = 0;

  constructor(config: TurboliteConfig) {
    this.maxMemBytes = config.maxCacheBytes ?? 64 * 1024 * 1024;
    this.useOpfs = config.opfsCache ?? false;
  }

  /**
   * Initialize OPFS directory for persistent caching.
   * Directory is keyed by dbName + manifest version so a re-import
   * (new version) gets a clean cache instead of serving stale pages.
   */
  async init(dbName: string, manifestVersion: number): Promise<void> {
    if (!this.useOpfs) return;
    try {
      const root = await navigator.storage.getDirectory();
      const turboliteDir = await root.getDirectoryHandle("turbolite", { create: true });
      const versionedName = `${dbName}_v${manifestVersion}`;
      this.opfsDir = await turboliteDir.getDirectoryHandle(versionedName, { create: true });

      // Clean up old versions (best-effort, non-blocking)
      this.cleanOldVersions(turboliteDir, dbName, versionedName);
    } catch {
      // OPFS not available (e.g. non-secure context). Fall back to memory-only.
      this.opfsDir = null;
    }
  }

  /** Remove OPFS directories for older manifest versions of this database. */
  private async cleanOldVersions(
    turboliteDir: FileSystemDirectoryHandle,
    dbPrefix: string,
    currentName: string,
  ): Promise<void> {
    try {
      for await (const name of (turboliteDir as any).keys()) {
        if (name.startsWith(dbPrefix + "_v") && name !== currentName) {
          await turboliteDir.removeEntry(name, { recursive: true });
        }
      }
    } catch {
      // Non-critical; stale dirs waste space but don't cause correctness issues
    }
  }

  /** Get a page from cache. Returns null on miss. */
  getPage(pageNum: number): Uint8Array | null {
    const page = this.memCache.get(pageNum);
    if (page) {
      this.hits++;
      this.touchLru(pageNum);
      return page;
    }
    return null;
  }

  /**
   * Get a page, fetching from OPFS or remote on miss.
   * Fetches the entire group on remote miss (caches all sibling pages).
   */
  async getPageAsync(
    pageNum: number,
    manifest: Manifest,
    pageIndex: Map<number, PageLocation>,
    fetcher: PageFetcher,
  ): Promise<Uint8Array> {
    // 1. In-memory
    const mem = this.getPage(pageNum);
    if (mem) return mem;

    // 2. OPFS
    if (this.opfsDir) {
      const opfsPage = await this.readOpfs(pageNum, manifest.page_size);
      if (opfsPage) {
        this.hits++;
        this.putMem(pageNum, opfsPage);
        return opfsPage;
      }
    }

    // 3. Remote fetch
    this.misses++;
    const loc = pageLocation(manifest, pageIndex, pageNum);
    if (!loc) {
      throw new Error(`Page ${pageNum} not found in manifest`);
    }

    const pages = await fetcher.fetchPagesForGroup(manifest, loc.group_id, loc.index);

    // Cache all pages from the fetched unit
    const opfsWrites: Promise<void>[] = [];
    for (const [pnum, data] of pages) {
      // Make a copy so the fetcher's buffer can be GC'd
      const copy = new Uint8Array(data.length);
      copy.set(data);
      this.putMem(pnum, copy);
      if (this.opfsDir) {
        opfsWrites.push(this.writeOpfs(pnum, copy));
      }
    }
    if (opfsWrites.length > 0) {
      await Promise.all(opfsWrites);
    }

    const result = this.memCache.get(pageNum);
    if (!result) {
      throw new Error(`Page ${pageNum} not in fetched group ${loc.group_id}`);
    }
    return result;
  }

  /**
   * Bulk-insert pages (used for interior/index bundle prefetch).
   * Copies each page so callers' buffers (often subarrays of a larger
   * decompressed blob) can be garbage collected.
   */
  async putPages(pages: Iterable<[number, Uint8Array]>): Promise<void> {
    const opfsWrites: Promise<void>[] = [];
    for (const [pageNum, data] of pages) {
      const copy = new Uint8Array(data.length);
      copy.set(data);
      this.putMem(pageNum, copy);
      if (this.opfsDir) {
        opfsWrites.push(this.writeOpfs(pageNum, copy));
      }
    }
    if (opfsWrites.length > 0) {
      await Promise.all(opfsWrites);
    }
  }

  getStats(fetcher: PageFetcher): CacheStats {
    return {
      hits: this.hits,
      misses: this.misses,
      fetchCount: fetcher.fetchCount,
      fetchBytes: fetcher.fetchBytes,
      cachedPages: this.memCache.size,
      cachedBytes: this.memBytes,
    };
  }

  /** Clear all cached data. */
  async clear(): Promise<void> {
    this.memCache.clear();
    this.memBytes = 0;
    this.accessGen.clear();
    this.generation = 0;
    if (this.opfsDir) {
      try {
        const root = await navigator.storage.getDirectory();
        const turboliteDir = await root.getDirectoryHandle("turbolite");
        // Remove and recreate
        await turboliteDir.removeEntry(this.opfsDir.name, { recursive: true });
        this.opfsDir = await turboliteDir.getDirectoryHandle(this.opfsDir.name, { create: true });
      } catch {
        // Ignore cleanup errors
      }
    }
  }

  private putMem(pageNum: number, data: Uint8Array): void {
    if (this.memCache.has(pageNum)) {
      this.touchLru(pageNum);
      return;
    }

    // Evict if over budget
    while (this.memBytes + data.byteLength > this.maxMemBytes && this.memCache.size > 0) {
      this.evictLru();
    }

    this.memCache.set(pageNum, data);
    this.memBytes += data.byteLength;
    this.accessGen.set(pageNum, ++this.generation);
  }

  private touchLru(pageNum: number): void {
    this.accessGen.set(pageNum, ++this.generation);
  }

  /** Evict the least-recently-used page. O(n) but only called during eviction. */
  private evictLru(): void {
    let minGen = Infinity;
    let victim = -1;
    for (const [pageNum, gen] of this.accessGen) {
      if (gen < minGen) {
        minGen = gen;
        victim = pageNum;
      }
    }
    if (victim >= 0) {
      const data = this.memCache.get(victim);
      if (data) {
        this.memBytes -= data.byteLength;
      }
      this.memCache.delete(victim);
      this.accessGen.delete(victim);
    }
  }

  private async readOpfs(pageNum: number, pageSize: number): Promise<Uint8Array | null> {
    if (!this.opfsDir) return null;
    try {
      const fileHandle = await this.opfsDir.getFileHandle(`p${pageNum}`);
      const file = await fileHandle.getFile();
      if (file.size !== pageSize) return null;
      const buf = await file.arrayBuffer();
      return new Uint8Array(buf);
    } catch {
      return null;
    }
  }

  private async writeOpfs(pageNum: number, data: Uint8Array): Promise<void> {
    if (!this.opfsDir) return;
    const fh = await this.opfsDir.getFileHandle(`p${pageNum}`, { create: true });
    const writable = await fh.createWritable();
    await writable.write(data as unknown as FileSystemWriteChunkType);
    await writable.close();
  }
}

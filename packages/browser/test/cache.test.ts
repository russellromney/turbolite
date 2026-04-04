import { describe, it, expect, vi } from "vitest";
import { PageCache } from "../src/cache.js";
import type { TurboliteConfig, Manifest, PageLocation } from "../src/types.js";
import type { PageFetcher } from "../src/fetcher.js";
import { pageLocation, buildPageIndex } from "../src/manifest.js";

function makeConfig(overrides: Partial<TurboliteConfig> = {}): TurboliteConfig {
  return {
    baseUrl: "http://localhost",
    prefix: "test",
    maxCacheBytes: 1024 * 1024, // 1MB
    opfsCache: false,
    ...overrides,
  };
}

function makePage(pageNum: number, pageSize: number): Uint8Array {
  const buf = new Uint8Array(pageSize);
  // Fill with recognizable pattern
  const view = new DataView(buf.buffer);
  view.setUint32(0, pageNum, true);
  return buf;
}

describe("PageCache", () => {
  it("returns null on miss", async () => {
    const cache = new PageCache(makeConfig());
    expect(cache.getPage(0)).toBeNull();
  });

  it("caches and retrieves pages", async () => {
    const cache = new PageCache(makeConfig());
    const page = makePage(42, 4096);
    await cache.putPages([[42, page]]);
    const result = cache.getPage(42);
    expect(result).toBeDefined();
    expect(result![0]).toBe(42); // first byte of our pattern
  });

  it("tracks hits and misses", async () => {
    const cache = new PageCache(makeConfig());
    await cache.putPages([[1, makePage(1, 4096)]]);

    cache.getPage(1); // hit
    cache.getPage(1); // hit
    cache.getPage(999); // miss (returns null, not counted as miss by getPage)

    expect(cache.hits).toBe(2);
  });

  it("evicts LRU pages when over budget", async () => {
    // 4 pages * 4096 bytes = 16KB budget
    const cache = new PageCache(makeConfig({ maxCacheBytes: 4096 * 4 }));

    // Insert 4 pages (fills cache)
    for (let i = 0; i < 4; i++) {
      await cache.putPages([[i, makePage(i, 4096)]]);
    }

    // All 4 should be present
    for (let i = 0; i < 4; i++) {
      expect(cache.getPage(i)).not.toBeNull();
    }

    // Insert a 5th page; should evict page 0 (LRU)
    // But page 0 was touched by getPage above, so the real LRU depends on access order.
    // Let's do a clean test: insert without accessing
    const cache2 = new PageCache(makeConfig({ maxCacheBytes: 4096 * 4 }));
    for (let i = 0; i < 4; i++) {
      await cache2.putPages([[i, makePage(i, 4096)]]);
    }
    // Now insert page 4; page 0 should be evicted (oldest in LRU)
    await cache2.putPages([[4, makePage(4, 4096)]]);
    expect(cache2.getPage(0)).toBeNull();
    expect(cache2.getPage(4)).not.toBeNull();
  });

  it("fetches on async miss via fetcher", async () => {
    const cache = new PageCache(makeConfig());
    const manifest: Manifest = {
      version: 1,
      page_count: 256,
      page_size: 4096,
      pages_per_group: 256,
      page_group_keys: ["pg/0_v1"],
      interior_chunk_keys: {},
      index_chunk_keys: {},
      frame_tables: [],
      sub_pages_per_frame: 0,
      strategy: "Positional",
      group_pages: [],
      btrees: {},
    };
    const pageIndex = buildPageIndex(manifest);

    // Mock fetcher that returns all pages for a group
    const mockPages = new Map<number, Uint8Array>();
    for (let i = 0; i < 256; i++) {
      mockPages.set(i, makePage(i, 4096));
    }

    const mockFetcher = {
      fetchPagesForGroup: vi.fn().mockResolvedValue(mockPages),
      fetchCount: 1,
      fetchBytes: 1000,
    } as unknown as PageFetcher;

    const result = await cache.getPageAsync(42, manifest, pageIndex, mockFetcher);
    expect(result).toBeDefined();

    // Should have fetched once
    expect(mockFetcher.fetchPagesForGroup).toHaveBeenCalledOnce();
    expect(mockFetcher.fetchPagesForGroup).toHaveBeenCalledWith(manifest, 0, 42);

    // All sibling pages should now be cached
    expect(cache.getPage(0)).not.toBeNull();
    expect(cache.getPage(100)).not.toBeNull();
    expect(cache.getPage(255)).not.toBeNull();

    // Second access should be a cache hit (no additional fetch)
    await cache.getPageAsync(42, manifest, pageIndex, mockFetcher);
    expect(mockFetcher.fetchPagesForGroup).toHaveBeenCalledOnce();
  });
});

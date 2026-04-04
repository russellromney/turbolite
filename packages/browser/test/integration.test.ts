import { describe, it, expect, beforeAll } from "vitest";
import { execSync } from "child_process";
import { encode } from "@msgpack/msgpack";
import { parseManifest, buildPageIndex, pageLocation } from "../src/manifest.js";
import { PageFetcher } from "../src/fetcher.js";
import { PageCache } from "../src/cache.js";
import type { Manifest, TurboliteConfig } from "../src/types.js";

/**
 * Integration test: exercises the full pipeline with real zstd-compressed data.
 *
 * Creates turbolite-format page groups (matching the Rust encoder's output),
 * compresses with real zstd, serves through a mock fetch, and verifies
 * pages are correctly decompressed and returned.
 */

const PAGE_SIZE = 4096;
const PAGES_PER_GROUP = 4;
const SUB_PAGES_PER_FRAME = 2;

/** Create a recognizable page: first 4 bytes = page number (LE), rest = 0xAA pattern */
function makeSyntheticPage(pageNum: number): Uint8Array {
  const buf = new Uint8Array(PAGE_SIZE);
  buf.fill(0xaa);
  const view = new DataView(buf.buffer);
  view.setUint32(0, pageNum, true);
  return buf;
}

/**
 * Encode a legacy single-frame page group matching turbolite's format:
 * zstd([u32 page_count][u32 page_size][page_0][page_1]...[page_N])
 */
function encodeLegacyGroup(pageNums: number[]): Uint8Array {
  const headerSize = 8;
  const raw = new Uint8Array(headerSize + pageNums.length * PAGE_SIZE);
  const view = new DataView(raw.buffer);
  view.setUint32(0, pageNums.length, true);
  view.setUint32(4, PAGE_SIZE, true);
  for (let i = 0; i < pageNums.length; i++) {
    raw.set(makeSyntheticPage(pageNums[i]), headerSize + i * PAGE_SIZE);
  }
  return raw;
}

/**
 * Encode an interior bundle matching turbolite's format:
 * zstd([u32 page_count][u32 page_size][page_count x u64 page_nums][page data...])
 */
function encodeInteriorBundle(pageNums: number[]): Uint8Array {
  const headerSize = 8 + pageNums.length * 8;
  const raw = new Uint8Array(headerSize + pageNums.length * PAGE_SIZE);
  const view = new DataView(raw.buffer);
  view.setUint32(0, pageNums.length, true);
  view.setUint32(4, PAGE_SIZE, true);
  for (let i = 0; i < pageNums.length; i++) {
    // u64 LE page number
    view.setUint32(8 + i * 8, pageNums[i], true);
    view.setUint32(8 + i * 8 + 4, 0, true);
  }
  for (let i = 0; i < pageNums.length; i++) {
    raw.set(makeSyntheticPage(pageNums[i]), headerSize + i * PAGE_SIZE);
  }
  return raw;
}

/**
 * Encode seekable multi-frame page group:
 * [frame_0: zstd(sub_ppf pages)][frame_1: zstd(sub_ppf pages)]...
 * Returns { blob, frameTable }
 */
function encodeSeekableGroup(pageNums: number[]): {
  frames: Uint8Array[];
  rawFrames: Uint8Array[];
} {
  const frames: Uint8Array[] = [];
  const rawFrames: Uint8Array[] = [];
  for (let i = 0; i < pageNums.length; i += SUB_PAGES_PER_FRAME) {
    const framePages = pageNums.slice(i, i + SUB_PAGES_PER_FRAME);
    const raw = new Uint8Array(framePages.length * PAGE_SIZE);
    for (let j = 0; j < framePages.length; j++) {
      raw.set(makeSyntheticPage(framePages[j]), j * PAGE_SIZE);
    }
    rawFrames.push(raw);
    frames.push(raw); // Will be compressed below
  }
  return { frames, rawFrames };
}

/** Compress data using the system zstd CLI. */
function zstdCompress(data: Uint8Array): Uint8Array {
  // Write to a temp file, compress, read back
  const tmpIn = `/tmp/turbolite_test_${Date.now()}_${Math.random().toString(36).slice(2)}`;
  const tmpOut = `${tmpIn}.zst`;
  const fs = require("fs");
  try {
    fs.writeFileSync(tmpIn, data);
    execSync(`zstd -3 -f "${tmpIn}" -o "${tmpOut}"`, { stdio: "pipe" });
    return new Uint8Array(fs.readFileSync(tmpOut));
  } finally {
    try { fs.unlinkSync(tmpIn); } catch {}
    try { fs.unlinkSync(tmpOut); } catch {}
  }
}

describe("integration: full pipeline with real zstd", () => {
  // 8 pages, 2 groups of 4 pages each
  // Group 0: pages [0,1,2,3] - legacy single-frame
  // Group 1: pages [4,5,6,7] - seekable multi-frame
  // Interior bundle: page 0 (simulated root page)

  let mockFiles: Map<string, Uint8Array>;
  let manifest: Manifest;
  let seekableFrameTable: Array<{ offset: number; len: number }>;

  beforeAll(() => {
    mockFiles = new Map();

    // --- Group 0: legacy single-frame ---
    const group0Raw = encodeLegacyGroup([0, 1, 2, 3]);
    const group0Compressed = zstdCompress(group0Raw);
    mockFiles.set("db/test/pg/0_v1", group0Compressed);

    // --- Group 1: seekable multi-frame ---
    const { frames: group1Frames } = encodeSeekableGroup([4, 5, 6, 7]);
    const compressedFrames = group1Frames.map((f) => zstdCompress(f));
    // Concatenate frames into a single blob
    const totalLen = compressedFrames.reduce((s, f) => s + f.length, 0);
    const group1Blob = new Uint8Array(totalLen);
    seekableFrameTable = [];
    let offset = 0;
    for (const cf of compressedFrames) {
      seekableFrameTable.push({ offset, len: cf.length });
      group1Blob.set(cf, offset);
      offset += cf.length;
    }
    mockFiles.set("db/test/pg/1_v1", group1Blob);

    // --- Interior bundle: page 0 ---
    const interiorRaw = encodeInteriorBundle([0]);
    const interiorCompressed = zstdCompress(interiorRaw);
    mockFiles.set("db/test/ibc/0_v1", interiorCompressed);

    // --- Manifest ---
    const manifestData: Record<string, unknown> = {
      version: 1,
      page_count: 8,
      page_size: PAGE_SIZE,
      pages_per_group: PAGES_PER_GROUP,
      page_group_keys: ["pg/0_v1", "pg/1_v1"],
      interior_chunk_keys: { 0: "ibc/0_v1" },
      index_chunk_keys: {},
      frame_tables: [
        [], // Group 0: legacy (no frame table)
        seekableFrameTable, // Group 1: seekable
      ],
      sub_pages_per_frame: SUB_PAGES_PER_FRAME,
      strategy: "Positional",
      group_pages: [],
      btrees: {},
    };
    const manifestBytes = encode(manifestData);
    mockFiles.set("db/test/manifest.msgpack", new Uint8Array(manifestBytes));

    manifest = parseManifest(new Uint8Array(manifestBytes));
  });

  function makeMockFetch(): typeof globalThis.fetch {
    return (async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = typeof input === "string" ? input : input.toString();
      // Extract path after base URL
      const path = url.replace("http://test/", "");
      const data = mockFiles.get(path);

      if (!data) {
        return { ok: false, status: 404, statusText: "Not Found" } as Response;
      }

      // Handle Range requests
      const rangeHeader = (init?.headers as Record<string, string>)?.Range;
      if (rangeHeader) {
        const match = rangeHeader.match(/bytes=(\d+)-(\d+)/);
        if (match) {
          const start = parseInt(match[1]);
          const end = parseInt(match[2]) + 1;
          const slice = data.slice(start, end);
          return {
            ok: true,
            status: 206,
            statusText: "Partial Content",
            arrayBuffer: () => Promise.resolve(slice.buffer.slice(slice.byteOffset, slice.byteOffset + slice.byteLength)),
          } as unknown as Response;
        }
      }

      return {
        ok: true,
        status: 200,
        statusText: "OK",
        arrayBuffer: () => Promise.resolve(data.buffer.slice(data.byteOffset, data.byteOffset + data.byteLength)),
      } as unknown as Response;
    }) as typeof globalThis.fetch;
  }

  it("fetches and decompresses a legacy page group (returns all pages)", async () => {
    const config: TurboliteConfig = {
      baseUrl: "http://test",
      prefix: "db/test",
      fetch: makeMockFetch(),
    };
    const fetcher = new PageFetcher(config);
    // Legacy group: targetIndex doesn't matter, whole group is fetched
    const pages = await fetcher.fetchPagesForGroup(manifest, 0, 0);

    expect(pages.size).toBe(4);

    for (let i = 0; i < 4; i++) {
      const page = pages.get(i)!;
      expect(page).toBeDefined();
      expect(page.byteLength).toBe(PAGE_SIZE);
      const view = new DataView(page.buffer, page.byteOffset, page.byteLength);
      expect(view.getUint32(0, true)).toBe(i);
      expect(page[4]).toBe(0xaa);
      expect(page[PAGE_SIZE - 1]).toBe(0xaa);
    }
  });

  it("seekable group: fetches only the frame containing the target page", async () => {
    const config: TurboliteConfig = {
      baseUrl: "http://test",
      prefix: "db/test",
      fetch: makeMockFetch(),
    };
    const fetcher = new PageFetcher(config);

    // Group 1 is seekable with SUB_PAGES_PER_FRAME=2
    // Pages [4,5,6,7] -> frame 0 has [4,5], frame 1 has [6,7]

    // Request page at index 0 (page 4) -> should fetch frame 0 only
    const frame0Pages = await fetcher.fetchPagesForGroup(manifest, 1, 0);
    expect(frame0Pages.size).toBe(2); // only pages 4,5
    expect(frame0Pages.has(4)).toBe(true);
    expect(frame0Pages.has(5)).toBe(true);
    expect(frame0Pages.has(6)).toBe(false);
    expect(frame0Pages.has(7)).toBe(false);

    const page4 = frame0Pages.get(4)!;
    const view4 = new DataView(page4.buffer, page4.byteOffset, page4.byteLength);
    expect(view4.getUint32(0, true)).toBe(4);

    // Request page at index 3 (page 7) -> should fetch frame 1 only
    const frame1Pages = await fetcher.fetchPagesForGroup(manifest, 1, 3);
    expect(frame1Pages.size).toBe(2); // only pages 6,7
    expect(frame1Pages.has(6)).toBe(true);
    expect(frame1Pages.has(7)).toBe(true);
    expect(frame1Pages.has(4)).toBe(false);
  });

  it("fetches and decompresses an interior bundle", async () => {
    const config: TurboliteConfig = {
      baseUrl: "http://test",
      prefix: "db/test",
      fetch: makeMockFetch(),
    };
    const fetcher = new PageFetcher(config);
    const bundle = await fetcher.fetchBundle("ibc/0_v1");

    expect(bundle.length).toBe(1);
    expect(bundle[0].pageNum).toBe(0);
    expect(bundle[0].data.byteLength).toBe(PAGE_SIZE);
    const view = new DataView(bundle[0].data.buffer, bundle[0].data.byteOffset, bundle[0].data.byteLength);
    expect(view.getUint32(0, true)).toBe(0);
  });

  it("cache serves pages after initial fetch", async () => {
    const config: TurboliteConfig = {
      baseUrl: "http://test",
      prefix: "db/test",
      fetch: makeMockFetch(),
      maxCacheBytes: 1024 * 1024,
    };
    const fetcher = new PageFetcher(config);
    const cache = new PageCache(config);
    const pageIndex = buildPageIndex(manifest);

    // First access: cache miss, fetches group 0
    const page2 = await cache.getPageAsync(2, manifest, pageIndex, fetcher);
    expect(page2.byteLength).toBe(PAGE_SIZE);
    const view = new DataView(page2.buffer, page2.byteOffset, page2.byteLength);
    expect(view.getUint32(0, true)).toBe(2);

    // All pages from group 0 should now be cached
    const page0 = cache.getPage(0);
    expect(page0).not.toBeNull();
    const view0 = new DataView(page0!.buffer, page0!.byteOffset, page0!.byteLength);
    expect(view0.getUint32(0, true)).toBe(0);

    const page3 = cache.getPage(3);
    expect(page3).not.toBeNull();

    // Pages from group 1 should NOT be cached yet
    expect(cache.getPage(4)).toBeNull();

    // Stats should show 1 miss (triggered fetch) and cache hits
    expect(cache.misses).toBe(1);
    expect(cache.hits).toBeGreaterThanOrEqual(2); // page0 + page3 sync gets

    // Fetch page from group 1 (seekable: page 5 is index 1, frame 0 with spf=2)
    const page5 = await cache.getPageAsync(5, manifest, pageIndex, fetcher);
    const view5 = new DataView(page5.buffer, page5.byteOffset, page5.byteLength);
    expect(view5.getUint32(0, true)).toBe(5);

    // Seekable: only pages 4,5 (frame 0) should be cached, not 6,7
    expect(cache.getPage(4)).not.toBeNull();
    expect(cache.getPage(5)).not.toBeNull();
    expect(cache.getPage(6)).toBeNull(); // frame 1, not fetched
    expect(cache.getPage(7)).toBeNull(); // frame 1, not fetched

    // Fetch page 7 (index 3, frame 1) to cache the rest
    await cache.getPageAsync(7, manifest, pageIndex, fetcher);
    expect(cache.getPage(6)).not.toBeNull();
    expect(cache.getPage(7)).not.toBeNull();
  });

  it("full pipeline: manifest -> locate -> fetch -> decompress -> verify all pages", async () => {
    const config: TurboliteConfig = {
      baseUrl: "http://test",
      prefix: "db/test",
      fetch: makeMockFetch(),
    };
    const fetcher = new PageFetcher(config);
    const cache = new PageCache(config);

    // Parse manifest
    const manifestBytes = mockFiles.get("db/test/manifest.msgpack")!;
    const m = parseManifest(manifestBytes);
    expect(m.version).toBe(1);
    expect(m.page_count).toBe(8);
    expect(m.page_size).toBe(PAGE_SIZE);
    const pageIdx = buildPageIndex(m);

    // Prefetch interior bundle
    const bundle = await fetcher.fetchBundle("ibc/0_v1");
    await cache.putPages(bundle.map((b) => [b.pageNum, b.data]));

    // Verify every single page
    for (let pageNum = 0; pageNum < 8; pageNum++) {
      // Locate page
      const loc = pageLocation(m, pageIdx, pageNum);
      expect(loc).not.toBeNull();
      expect(loc!.group_id).toBe(Math.floor(pageNum / PAGES_PER_GROUP));

      // Fetch via cache
      const page = await cache.getPageAsync(pageNum, m, pageIdx, fetcher);
      expect(page.byteLength).toBe(PAGE_SIZE);

      // Verify content
      const view = new DataView(page.buffer, page.byteOffset, page.byteLength);
      expect(view.getUint32(0, true)).toBe(pageNum);
      expect(page[4]).toBe(0xaa);
      expect(page[PAGE_SIZE - 1]).toBe(0xaa);
    }

    // Verify stats
    const stats = cache.getStats(fetcher);
    expect(stats.cachedPages).toBe(8);
    expect(stats.fetchCount).toBeGreaterThan(0);
    expect(stats.fetchBytes).toBeGreaterThan(0);
  });

  it("BTreeAware manifest with non-contiguous page groups", async () => {
    // Create a BTreeAware manifest where pages are NOT in sequential order
    const btreeManifest: Record<string, unknown> = {
      version: 1,
      page_count: 8,
      page_size: PAGE_SIZE,
      pages_per_group: PAGES_PER_GROUP,
      page_group_keys: ["pg/0_v1", "pg/1_v1"],
      interior_chunk_keys: {},
      index_chunk_keys: {},
      frame_tables: [[], []],
      sub_pages_per_frame: 0,
      strategy: "BTreeAware",
      // Group 0 has pages [0,2,4,6], Group 1 has pages [1,3,5,7]
      // (interleaved, not sequential)
      group_pages: [[0, 2, 4, 6], [1, 3, 5, 7]],
      btrees: {},
    };

    // Create matching compressed groups
    const group0Raw = new Uint8Array(8 + 4 * PAGE_SIZE);
    const group0View = new DataView(group0Raw.buffer);
    group0View.setUint32(0, 4, true);
    group0View.setUint32(4, PAGE_SIZE, true);
    [0, 2, 4, 6].forEach((pn, i) => {
      const page = makeSyntheticPage(pn);
      group0Raw.set(page, 8 + i * PAGE_SIZE);
    });

    const group1Raw = new Uint8Array(8 + 4 * PAGE_SIZE);
    const group1View = new DataView(group1Raw.buffer);
    group1View.setUint32(0, 4, true);
    group1View.setUint32(4, PAGE_SIZE, true);
    [1, 3, 5, 7].forEach((pn, i) => {
      const page = makeSyntheticPage(pn);
      group1Raw.set(page, 8 + i * PAGE_SIZE);
    });

    const btreeFiles = new Map<string, Uint8Array>();
    btreeFiles.set("db/btree/pg/0_v1", zstdCompress(group0Raw));
    btreeFiles.set("db/btree/pg/1_v1", zstdCompress(group1Raw));

    const manifestBytes = encode(btreeManifest);
    btreeFiles.set("db/btree/manifest.msgpack", new Uint8Array(manifestBytes));

    const mockFetch = (async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = typeof input === "string" ? input : input.toString();
      const path = url.replace("http://test/", "");
      const data = btreeFiles.get(path);
      if (!data) return { ok: false, status: 404 } as Response;
      return {
        ok: true,
        status: 200,
        arrayBuffer: () => Promise.resolve(data.buffer.slice(data.byteOffset, data.byteOffset + data.byteLength)),
      } as unknown as Response;
    }) as typeof globalThis.fetch;

    const config: TurboliteConfig = {
      baseUrl: "http://test",
      prefix: "db/btree",
      fetch: mockFetch,
    };
    const fetcher = new PageFetcher(config);
    const cache = new PageCache(config);
    const m = parseManifest(new Uint8Array(manifestBytes));
    const pageIdx = buildPageIndex(m);

    expect(m.strategy).toBe("BTreeAware");

    // Verify page locations
    expect(pageLocation(m, pageIdx, 0)).toEqual({ group_id: 0, index: 0 });
    expect(pageLocation(m, pageIdx, 2)).toEqual({ group_id: 0, index: 1 });
    expect(pageLocation(m, pageIdx, 1)).toEqual({ group_id: 1, index: 0 });
    expect(pageLocation(m, pageIdx, 7)).toEqual({ group_id: 1, index: 3 });

    // Fetch page 4 (in group 0, index 2)
    const page4 = await cache.getPageAsync(4, m, pageIdx, fetcher);
    const view4 = new DataView(page4.buffer, page4.byteOffset, page4.byteLength);
    expect(view4.getUint32(0, true)).toBe(4);

    // Group 0 pages should all be cached (0, 2, 4, 6)
    expect(cache.getPage(0)).not.toBeNull();
    expect(cache.getPage(2)).not.toBeNull();
    expect(cache.getPage(6)).not.toBeNull();
    // Group 1 pages should NOT be cached
    expect(cache.getPage(1)).toBeNull();
    expect(cache.getPage(3)).toBeNull();
  });
});

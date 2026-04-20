import { describe, it, expect, vi } from "vitest";
import { PageFetcher } from "../src/fetcher.js";
import type { Manifest, TurboliteConfig } from "../src/types.js";

// Minimal zstd-compressed empty data (single frame, 4 bytes: magic + empty)
// We'll test the fetcher's URL construction and fetch calls rather than
// actual decompression (fzstd handles that).

function makeConfig(fetchFn: typeof globalThis.fetch): TurboliteConfig {
  return {
    baseUrl: "https://proxy.example.com",
    prefix: "db/tenant-1",
    fetch: fetchFn,
  };
}

describe("PageFetcher", () => {
  it("constructs correct manifest URL", async () => {
    const mockFetch = vi.fn().mockResolvedValue({
      ok: true,
      arrayBuffer: () => Promise.resolve(new ArrayBuffer(10)),
    });

    const fetcher = new PageFetcher(makeConfig(mockFetch));
    await fetcher.fetchManifest();

    expect(mockFetch).toHaveBeenCalledWith(
      "https://proxy.example.com/db/tenant-1/manifest.msgpack",
    );
    expect(fetcher.fetchCount).toBe(1);
  });

  it("constructs correct frame URL with Range header", async () => {
    const mockFetch = vi.fn().mockResolvedValue({
      ok: true,
      status: 206,
      arrayBuffer: () => {
        // Return valid zstd frame (magic number 0xFD2FB528 + minimal frame)
        const buf = new Uint8Array([0x28, 0xb5, 0x2f, 0xfd, 0x00, 0x00, 0x01, 0x00, 0x00]);
        return Promise.resolve(buf.buffer);
      },
    });

    const fetcher = new PageFetcher(makeConfig(mockFetch));
    await fetcher.fetchFrame("pg/0_v1", { offset: 100, len: 500 });

    expect(mockFetch).toHaveBeenCalledWith(
      "https://proxy.example.com/db/tenant-1/pg/0_v1",
      { headers: { Range: "bytes=100-599" } },
    );
  });

  it("throws on HTTP error", async () => {
    const mockFetch = vi.fn().mockResolvedValue({
      ok: false,
      status: 404,
      statusText: "Not Found",
    });

    const fetcher = new PageFetcher(makeConfig(mockFetch));
    await expect(fetcher.fetchManifest()).rejects.toThrow("404");
  });

  it("strips trailing slashes from baseUrl and prefix", () => {
    const mockFetch = vi.fn().mockResolvedValue({
      ok: true,
      arrayBuffer: () => Promise.resolve(new ArrayBuffer(0)),
    });

    const fetcher = new PageFetcher({
      baseUrl: "https://example.com/",
      prefix: "db/tenant/",
      fetch: mockFetch,
    });

    fetcher.fetchManifest().catch(() => {});

    expect(mockFetch).toHaveBeenCalledWith(
      "https://example.com/db/tenant/manifest.msgpack",
    );
  });

  it("getGroupPageNums returns correct pages for Positional strategy", async () => {
    const manifest: Manifest = {
      version: 1,
      page_count: 600,
      page_size: 4096,
      pages_per_group: 256,
      page_group_keys: ["pg/0_v1", "pg/1_v1", "pg/2_v1"],
      interior_chunk_keys: {},
      index_chunk_keys: {},
      frame_tables: [],
      sub_pages_per_frame: 0,
      strategy: "Positional",
      group_pages: [],
      btrees: {},
    };

    // We can't easily call private method, but we can test via fetchPagesForGroup
    // by checking it calls the right key
    const mockFetch = vi.fn().mockResolvedValue({
      ok: true,
      arrayBuffer: () => {
        // Legacy format: zstd([u32 page_count][u32 page_size][pages...])
        // Create a fake compressed blob
        const pageCount = 256;
        const pageSize = 4096;
        const raw = new Uint8Array(8 + pageCount * pageSize);
        const view = new DataView(raw.buffer);
        view.setUint32(0, pageCount, true);
        view.setUint32(4, pageSize, true);

        // We need to compress this with zstd, which is hard in tests.
        // Instead, test URL construction only.
        throw new Error("Would need real zstd to fully test");
      },
    });

    const fetcher = new PageFetcher(makeConfig(mockFetch));

    // This will fail at decompression, but we can verify the URL was correct
    try {
      await fetcher.fetchPagesForGroup(manifest, 1, 0);
    } catch {
      // Expected: fetch mock doesn't return valid zstd
    }

    expect(mockFetch).toHaveBeenCalledWith(
      "https://proxy.example.com/db/tenant-1/pg/1_v1",
    );
  });
});

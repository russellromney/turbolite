import { describe, it, expect } from "vitest";
import { encode } from "@msgpack/msgpack";
import { parseManifest, buildPageIndex, pageLocation } from "../src/manifest.js";
import type { Manifest } from "../src/types.js";

function makeManifest(overrides: Partial<Manifest> = {}): Uint8Array {
  const base: Record<string, unknown> = {
    version: 1,
    page_count: 1024,
    page_size: 4096,
    pages_per_group: 256,
    page_group_keys: ["pg/0_v1", "pg/1_v1", "pg/2_v1", "pg/3_v1"],
    interior_chunk_keys: {},
    index_chunk_keys: {},
    frame_tables: [],
    sub_pages_per_frame: 0,
    strategy: "Positional",
    group_pages: [],
    btrees: {},
    ...overrides,
  };
  return encode(base);
}

describe("parseManifest", () => {
  it("parses a Positional manifest", () => {
    const data = makeManifest();
    const m = parseManifest(data);
    expect(m.version).toBe(1);
    expect(m.page_count).toBe(1024);
    expect(m.page_size).toBe(4096);
    expect(m.pages_per_group).toBe(256);
    expect(m.strategy).toBe("Positional");
    expect(m.page_group_keys).toHaveLength(4);
  });

  it("auto-detects BTreeAware when group_pages present", () => {
    const data = makeManifest({
      strategy: "Positional",
      group_pages: [[0, 1, 2], [3, 4, 5]],
    });
    const m = parseManifest(data);
    expect(m.strategy).toBe("BTreeAware");
  });

  it("handles empty manifest fields gracefully", () => {
    const data = encode({ version: 0 });
    const m = parseManifest(data);
    expect(m.version).toBe(0);
    expect(m.page_count).toBe(0);
    expect(m.page_group_keys).toEqual([]);
    expect(m.frame_tables).toEqual([]);
  });
});

describe("buildPageIndex", () => {
  it("returns empty map for Positional strategy", () => {
    const m = parseManifest(makeManifest());
    const idx = buildPageIndex(m);
    expect(idx.size).toBe(0);
  });

  it("builds reverse index for BTreeAware strategy", () => {
    const m = parseManifest(makeManifest({
      strategy: "BTreeAware",
      group_pages: [[10, 20, 30], [40, 50]],
    }));
    const idx = buildPageIndex(m);
    expect(idx.size).toBe(5);
    expect(idx.get(10)).toEqual({ group_id: 0, index: 0 });
    expect(idx.get(30)).toEqual({ group_id: 0, index: 2 });
    expect(idx.get(50)).toEqual({ group_id: 1, index: 1 });
  });
});

describe("pageLocation", () => {
  it("computes Positional location arithmetically", () => {
    const m = parseManifest(makeManifest());
    const idx = buildPageIndex(m);

    const loc = pageLocation(m, idx, 0);
    expect(loc).toEqual({ group_id: 0, index: 0 });

    const loc2 = pageLocation(m, idx, 257);
    expect(loc2).toEqual({ group_id: 1, index: 1 });

    const loc3 = pageLocation(m, idx, 1023);
    expect(loc3).toEqual({ group_id: 3, index: 255 });
  });

  it("returns null for out-of-range pages", () => {
    const m = parseManifest(makeManifest());
    const idx = buildPageIndex(m);
    expect(pageLocation(m, idx, 1024)).toBeNull();
    expect(pageLocation(m, idx, 99999)).toBeNull();
  });

  it("looks up BTreeAware location from index", () => {
    const m = parseManifest(makeManifest({
      strategy: "BTreeAware",
      group_pages: [[10, 20, 30], [40, 50]],
    }));
    const idx = buildPageIndex(m);

    expect(pageLocation(m, idx, 20)).toEqual({ group_id: 0, index: 1 });
    expect(pageLocation(m, idx, 40)).toEqual({ group_id: 1, index: 0 });
    expect(pageLocation(m, idx, 999)).toBeNull();
  });
});

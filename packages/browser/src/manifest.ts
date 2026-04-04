import { decode } from "@msgpack/msgpack";
import type { Manifest, PageLocation } from "./types.js";

/**
 * Parse a msgpack-encoded manifest blob into a Manifest object.
 * Matches the Rust `rmp_serde::to_vec` output.
 */
export function parseManifest(data: Uint8Array): Manifest {
  const raw = decode(data) as Record<string, unknown>;

  const manifest: Manifest = {
    version: (raw.version as number) ?? 0,
    page_count: (raw.page_count as number) ?? 0,
    page_size: (raw.page_size as number) ?? 0,
    pages_per_group: (raw.pages_per_group as number) ?? 256,
    page_group_keys: (raw.page_group_keys as string[]) ?? [],
    interior_chunk_keys: (raw.interior_chunk_keys as Record<number, string>) ?? {},
    index_chunk_keys: (raw.index_chunk_keys as Record<number, string>) ?? {},
    frame_tables: (raw.frame_tables as Manifest["frame_tables"]) ?? [],
    sub_pages_per_frame: (raw.sub_pages_per_frame as number) ?? 0,
    strategy: (raw.strategy as Manifest["strategy"]) ?? "Positional",
    group_pages: (raw.group_pages as number[][]) ?? [],
    btrees: (raw.btrees as Manifest["btrees"]) ?? {},
  };

  // Auto-detect BTreeAware if group_pages is populated
  if (manifest.group_pages.length > 0 && manifest.strategy === "Positional") {
    manifest.strategy = "BTreeAware";
  }

  return manifest;
}

/** Build page_num -> PageLocation reverse index for BTreeAware manifests. */
export function buildPageIndex(manifest: Manifest): Map<number, PageLocation> {
  const index = new Map<number, PageLocation>();

  if (manifest.strategy === "Positional") {
    // No index needed; page_location is arithmetic
    return index;
  }

  for (let gid = 0; gid < manifest.group_pages.length; gid++) {
    const pages = manifest.group_pages[gid];
    for (let idx = 0; idx < pages.length; idx++) {
      index.set(pages[idx], { group_id: gid, index: idx });
    }
  }

  return index;
}

/** Look up where a page lives in the manifest. */
export function pageLocation(
  manifest: Manifest,
  pageIndex: Map<number, PageLocation>,
  pageNum: number,
): PageLocation | null {
  if (manifest.strategy === "BTreeAware") {
    return pageIndex.get(pageNum) ?? null;
  }

  // Positional: arithmetic
  const ppg = manifest.pages_per_group;
  if (ppg === 0 || pageNum >= manifest.page_count) return null;

  return {
    group_id: Math.floor(pageNum / ppg),
    index: pageNum % ppg,
  };
}

import { decompress } from "fzstd";
import type { Manifest, FrameEntry, TurboliteConfig } from "./types.js";

/**
 * Fetches and decompresses turbolite page groups from S3 (or a proxy).
 *
 * Supports two encoding formats:
 * 1. Seekable multi-frame: range GET a single frame, decompress independently
 * 2. Legacy single-frame: download entire group, decompress, split into pages
 */
export class PageFetcher {
  private readonly baseUrl: string;
  private readonly prefix: string;
  private readonly fetchFn: typeof globalThis.fetch;

  fetchCount = 0;
  fetchBytes = 0;

  constructor(config: TurboliteConfig) {
    this.baseUrl = config.baseUrl.replace(/\/$/, "");
    this.prefix = config.prefix.replace(/\/$/, "");
    this.fetchFn = config.fetch ?? globalThis.fetch.bind(globalThis);
  }

  /** Fetch the manifest from S3. Tries msgpack first, falls back to JSON. */
  async fetchManifest(): Promise<Uint8Array> {
    const url = `${this.baseUrl}/${this.prefix}/manifest.msgpack`;
    const resp = await this.fetchFn(url);
    if (!resp.ok) {
      throw new Error(`Failed to fetch manifest: ${resp.status} ${resp.statusText}`);
    }
    const buf = await resp.arrayBuffer();
    this.fetchCount++;
    this.fetchBytes += buf.byteLength;
    return new Uint8Array(buf);
  }

  /**
   * Fetch and decompress a single sub-chunk frame from a seekable page group.
   * Uses HTTP Range header for partial download.
   * Returns raw page bytes (sub_pages_per_frame * page_size bytes).
   */
  async fetchFrame(
    s3Key: string,
    frame: FrameEntry,
  ): Promise<Uint8Array> {
    const url = `${this.baseUrl}/${this.prefix}/${s3Key}`;
    const rangeEnd = frame.offset + frame.len - 1;
    const resp = await this.fetchFn(url, {
      headers: { Range: `bytes=${frame.offset}-${rangeEnd}` },
    });

    if (!resp.ok && resp.status !== 206) {
      throw new Error(`Failed to fetch frame: ${resp.status} ${resp.statusText}`);
    }

    const compressed = new Uint8Array(await resp.arrayBuffer());
    this.fetchCount++;
    this.fetchBytes += compressed.byteLength;
    return decompress(compressed);
  }

  /**
   * Fetch and decompress an entire page group (legacy single-frame format).
   * Returns (page_count, page_size, raw page data).
   */
  async fetchGroupFull(s3Key: string): Promise<{
    pageCount: number;
    pageSize: number;
    data: Uint8Array;
  }> {
    const url = `${this.baseUrl}/${this.prefix}/${s3Key}`;
    const resp = await this.fetchFn(url);
    if (!resp.ok) {
      throw new Error(`Failed to fetch group: ${resp.status} ${resp.statusText}`);
    }

    const compressed = new Uint8Array(await resp.arrayBuffer());
    this.fetchCount++;
    this.fetchBytes += compressed.byteLength;

    const raw = decompress(compressed);

    if (raw.byteLength < 8) {
      throw new Error("Page group too short for header");
    }

    const view = new DataView(raw.buffer, raw.byteOffset, raw.byteLength);
    const pageCount = view.getUint32(0, true);
    const pageSize = view.getUint32(4, true);

    return {
      pageCount,
      pageSize,
      data: raw.subarray(8),
    };
  }

  /**
   * Fetch and decompress an interior/index bundle.
   * Returns array of (page_number, raw_page_data).
   */
  async fetchBundle(s3Key: string): Promise<Array<{ pageNum: number; data: Uint8Array }>> {
    const url = `${this.baseUrl}/${this.prefix}/${s3Key}`;
    const resp = await this.fetchFn(url);
    if (!resp.ok) {
      throw new Error(`Failed to fetch bundle: ${resp.status} ${resp.statusText}`);
    }

    const compressed = new Uint8Array(await resp.arrayBuffer());
    this.fetchCount++;
    this.fetchBytes += compressed.byteLength;

    const raw = decompress(compressed);

    if (raw.byteLength < 8) {
      throw new Error("Bundle too short for header");
    }

    const view = new DataView(raw.buffer, raw.byteOffset, raw.byteLength);
    const pageCount = view.getUint32(0, true);
    const pageSize = view.getUint32(4, true);

    const pnumsEnd = 8 + pageCount * 8;
    const expectedLen = pnumsEnd + pageCount * pageSize;
    if (raw.byteLength < expectedLen) {
      throw new Error(`Bundle truncated: expected ${expectedLen}, got ${raw.byteLength}`);
    }

    const result: Array<{ pageNum: number; data: Uint8Array }> = [];
    for (let i = 0; i < pageCount; i++) {
      const pnumOffset = 8 + i * 8;
      // Read u64 LE, but JS numbers are safe up to 2^53
      const lo = view.getUint32(pnumOffset, true);
      const hi = view.getUint32(pnumOffset + 4, true);
      const pageNum = lo + hi * 0x100000000;

      const dataOffset = pnumsEnd + i * pageSize;
      result.push({
        pageNum,
        data: raw.subarray(dataOffset, dataOffset + pageSize),
      });
    }

    return result;
  }

  /**
   * Fetch pages containing a specific page from a page group.
   * For seekable groups, fetches only the frame containing targetIndex.
   * For legacy groups, fetches the whole group (no way to seek).
   * Returns a Map of pageNum -> Uint8Array for all pages in the fetched unit.
   *
   * @param targetIndex - index of the requested page within the group
   */
  async fetchPagesForGroup(
    manifest: Manifest,
    groupId: number,
    targetIndex: number,
  ): Promise<Map<number, Uint8Array>> {
    const key = manifest.page_group_keys[groupId];
    if (!key) {
      throw new Error(`No S3 key for group ${groupId}`);
    }

    const pages = new Map<number, Uint8Array>();
    const groupPageNums = this.getGroupPageNums(manifest, groupId);

    const frameTable = manifest.frame_tables[groupId];
    const isSeekable = frameTable && frameTable.length > 0 && manifest.sub_pages_per_frame > 0;

    if (isSeekable) {
      // Fetch only the frame containing the target page
      const spf = manifest.sub_pages_per_frame;
      const frameIdx = Math.floor(targetIndex / spf);

      if (frameIdx >= frameTable.length) {
        throw new Error(`Frame index ${frameIdx} out of range for group ${groupId}`);
      }

      const raw = await this.fetchFrame(key, frameTable[frameIdx]);
      const startIdx = frameIdx * spf;
      const pagesInFrame = Math.min(spf, groupPageNums.length - startIdx);

      for (let i = 0; i < pagesInFrame; i++) {
        const pageNum = groupPageNums[startIdx + i];
        const offset = i * manifest.page_size;
        pages.set(pageNum, raw.subarray(offset, offset + manifest.page_size));
      }
    } else {
      // Legacy: fetch entire group
      const { pageCount, data } = await this.fetchGroupFull(key);

      for (let i = 0; i < pageCount && i < groupPageNums.length; i++) {
        const offset = i * manifest.page_size;
        pages.set(
          groupPageNums[i],
          data.subarray(offset, offset + manifest.page_size),
        );
      }
    }

    return pages;
  }

  private getGroupPageNums(manifest: Manifest, groupId: number): number[] {
    if (manifest.strategy === "BTreeAware") {
      return manifest.group_pages[groupId] ?? [];
    }
    // Positional
    const ppg = manifest.pages_per_group;
    const start = groupId * ppg;
    const end = Math.min(start + ppg, manifest.page_count);
    const nums: number[] = [];
    for (let i = start; i < end; i++) nums.push(i);
    return nums;
  }
}

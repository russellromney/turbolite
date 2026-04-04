/**
 * Mirrors the Rust Manifest struct from turbolite's tiered storage.
 * Deserialized from msgpack (manifest.msgpack on S3).
 */
export interface Manifest {
  version: number;
  page_count: number;
  page_size: number;
  pages_per_group: number;
  page_group_keys: string[];
  interior_chunk_keys: Record<number, string>;
  index_chunk_keys: Record<number, string>;
  frame_tables: FrameEntry[][];
  sub_pages_per_frame: number;
  strategy: GroupingStrategy;
  group_pages: number[][];
  btrees: Record<number, BTreeManifestEntry>;
}

export interface FrameEntry {
  offset: number;
  len: number;
}

export interface PageLocation {
  group_id: number;
  index: number;
}

export type GroupingStrategy = "Positional" | "BTreeAware";

export interface BTreeManifestEntry {
  name: string;
  obj_type: string;
  group_ids: number[];
}

/** Configuration for connecting to a turbolite database. */
export interface TurboliteConfig {
  /**
   * Base URL for fetching page groups and manifest.
   * Can be a backend proxy (e.g. "/api/turbolite") or direct S3 URL.
   * The client appends /{prefix}/manifest.msgpack, /{prefix}/pg/..., etc.
   */
  baseUrl: string;

  /** S3 key prefix (e.g. "databases/tenant-123"). */
  prefix: string;

  /**
   * Custom fetch function. Defaults to globalThis.fetch.
   * Useful for adding auth headers via a backend proxy.
   */
  fetch?: typeof globalThis.fetch;

  /** Maximum in-memory cache size in bytes. Default: 64MB. */
  maxCacheBytes?: number;

  /** Enable OPFS persistence for the page cache. Default: false. */
  opfsCache?: boolean;
}

/** Stats exposed for diagnostics. */
export interface CacheStats {
  hits: number;
  misses: number;
  fetchCount: number;
  fetchBytes: number;
  cachedPages: number;
  cachedBytes: number;
}

import { describe, it, expect, beforeAll } from "vitest";
import { execSync } from "child_process";
import * as fs from "fs";
import * as path from "path";
import { encode } from "@msgpack/msgpack";
import { parseManifest, buildPageIndex } from "../src/manifest.js";
import { PageFetcher } from "../src/fetcher.js";
import { PageCache } from "../src/cache.js";
import { TurboliteVFS } from "../src/vfs.js";
import type { TurboliteConfig, Manifest } from "../src/types.js";

/**
 * End-to-end test: creates a real SQLite database, encodes it in turbolite
 * format (page groups + manifest), serves via mock fetch, registers the VFS
 * with wa-sqlite, and runs SQL queries through the full stack.
 */

const PAGE_SIZE = 4096; // SQLite default

/** Compress with system zstd CLI */
function zstdCompress(data: Uint8Array): Uint8Array {
  const tmpIn = `/tmp/turbolite_e2e_${Date.now()}_${Math.random().toString(36).slice(2)}`;
  const tmpOut = `${tmpIn}.zst`;
  try {
    fs.writeFileSync(tmpIn, data);
    execSync(`zstd -3 -f "${tmpIn}" -o "${tmpOut}"`, { stdio: "pipe" });
    return new Uint8Array(fs.readFileSync(tmpOut));
  } finally {
    try { fs.unlinkSync(tmpIn); } catch {}
    try { fs.unlinkSync(tmpOut); } catch {}
  }
}

/** Create a real SQLite database and return the raw bytes */
function createTestDatabase(): Uint8Array {
  const dbPath = `/tmp/turbolite_e2e_${Date.now()}.db`;
  try {
    // Use sqlite3 CLI to create a real database
    execSync(`sqlite3 "${dbPath}" "
      PRAGMA page_size=${PAGE_SIZE};
      PRAGMA journal_mode=DELETE;
      CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT, age INTEGER);
      CREATE INDEX idx_users_age ON users(age);
      INSERT INTO users VALUES (1, 'Alice', 'alice@example.com', 30);
      INSERT INTO users VALUES (2, 'Bob', 'bob@example.com', 25);
      INSERT INTO users VALUES (3, 'Charlie', 'charlie@example.com', 35);
      INSERT INTO users VALUES (4, 'Diana', 'diana@example.com', 28);
      INSERT INTO users VALUES (5, 'Eve', 'eve@example.com', 32);
      CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER, title TEXT, body TEXT);
      INSERT INTO posts VALUES (1, 1, 'Hello World', 'First post by Alice');
      INSERT INTO posts VALUES (2, 2, 'Testing', 'Bob writes a test');
      INSERT INTO posts VALUES (3, 1, 'Update', 'Alice posts again');
    "`, { stdio: "pipe" });
    return new Uint8Array(fs.readFileSync(dbPath));
  } finally {
    try { fs.unlinkSync(dbPath); } catch {}
  }
}

/**
 * Encode raw SQLite bytes into turbolite's S3 format.
 * Creates page groups, interior bundles, and a manifest.
 */
function encodeTurboliteFormat(dbBytes: Uint8Array): {
  files: Map<string, Uint8Array>;
  manifest: Manifest;
} {
  const files = new Map<string, Uint8Array>();
  const pageCount = dbBytes.byteLength / PAGE_SIZE;
  const pagesPerGroup = 256; // all pages fit in one group for this tiny DB
  const subPagesPerFrame = 2;

  // Read all pages
  const pages: Uint8Array[] = [];
  for (let i = 0; i < pageCount; i++) {
    pages.push(dbBytes.subarray(i * PAGE_SIZE, (i + 1) * PAGE_SIZE));
  }

  // Detect interior pages (B-tree internal nodes)
  const interiorPageNums: number[] = [];
  const indexLeafPageNums: number[] = [];
  for (let i = 0; i < pageCount; i++) {
    const typeByte = pages[i][0];
    // Page 0 has SQLite header; B-tree type is at offset 100
    const hdrOffset = i === 0 ? 100 : 0;
    const pageType = pages[i][hdrOffset];

    if (pageType === 0x05 || pageType === 0x02) {
      // Interior table/index B-tree page
      interiorPageNums.push(i);
    } else if (pageType === 0x0a) {
      indexLeafPageNums.push(i);
    }
  }

  // Encode page groups with seekable multi-frame format
  const numGroups = Math.ceil(pageCount / pagesPerGroup);
  const pageGroupKeys: string[] = [];
  const frameTables: Array<Array<{ offset: number; len: number }>> = [];

  for (let gid = 0; gid < numGroups; gid++) {
    const start = gid * pagesPerGroup;
    const end = Math.min(start + pagesPerGroup, pageCount);
    const groupPages = pages.slice(start, end);

    // Encode as seekable multi-frame
    const compressedFrames: Uint8Array[] = [];
    const frameTable: Array<{ offset: number; len: number }> = [];
    let offset = 0;

    for (let f = 0; f < groupPages.length; f += subPagesPerFrame) {
      const framePages = groupPages.slice(f, f + subPagesPerFrame);
      const raw = new Uint8Array(framePages.length * PAGE_SIZE);
      for (let j = 0; j < framePages.length; j++) {
        raw.set(framePages[j], j * PAGE_SIZE);
      }
      const compressed = zstdCompress(raw);
      frameTable.push({ offset, len: compressed.byteLength });
      compressedFrames.push(compressed);
      offset += compressed.byteLength;
    }

    // Concatenate all frames
    const blob = new Uint8Array(offset);
    let pos = 0;
    for (const cf of compressedFrames) {
      blob.set(cf, pos);
      pos += cf.byteLength;
    }

    const key = `pg/${gid}_v1`;
    pageGroupKeys.push(key);
    frameTables.push(frameTable);
    files.set(`db/e2e/${key}`, blob);
  }

  // Encode interior bundle
  const interiorChunkKeys: Record<number, string> = {};
  if (interiorPageNums.length > 0) {
    const headerSize = 8 + interiorPageNums.length * 8;
    const raw = new Uint8Array(headerSize + interiorPageNums.length * PAGE_SIZE);
    const view = new DataView(raw.buffer);
    view.setUint32(0, interiorPageNums.length, true);
    view.setUint32(4, PAGE_SIZE, true);
    for (let i = 0; i < interiorPageNums.length; i++) {
      view.setUint32(8 + i * 8, interiorPageNums[i], true);
      view.setUint32(8 + i * 8 + 4, 0, true);
    }
    for (let i = 0; i < interiorPageNums.length; i++) {
      raw.set(pages[interiorPageNums[i]], headerSize + i * PAGE_SIZE);
    }
    const key = "ibc/0_v1";
    interiorChunkKeys[0] = key;
    files.set(`db/e2e/${key}`, zstdCompress(raw));
  }

  // Encode index leaf bundle
  const indexChunkKeys: Record<number, string> = {};
  if (indexLeafPageNums.length > 0) {
    const headerSize = 8 + indexLeafPageNums.length * 8;
    const raw = new Uint8Array(headerSize + indexLeafPageNums.length * PAGE_SIZE);
    const view = new DataView(raw.buffer);
    view.setUint32(0, indexLeafPageNums.length, true);
    view.setUint32(4, PAGE_SIZE, true);
    for (let i = 0; i < indexLeafPageNums.length; i++) {
      view.setUint32(8 + i * 8, indexLeafPageNums[i], true);
      view.setUint32(8 + i * 8 + 4, 0, true);
    }
    for (let i = 0; i < indexLeafPageNums.length; i++) {
      raw.set(pages[indexLeafPageNums[i]], headerSize + i * PAGE_SIZE);
    }
    const key = "ixb/0_v1";
    indexChunkKeys[0] = key;
    files.set(`db/e2e/${key}`, zstdCompress(raw));
  }

  // Build manifest
  const manifestData: Record<string, unknown> = {
    version: 1,
    page_count: pageCount,
    page_size: PAGE_SIZE,
    pages_per_group: pagesPerGroup,
    page_group_keys: pageGroupKeys,
    interior_chunk_keys: interiorChunkKeys,
    index_chunk_keys: indexChunkKeys,
    frame_tables: frameTables,
    sub_pages_per_frame: subPagesPerFrame,
    strategy: "Positional",
    group_pages: [],
    btrees: {},
  };

  const manifestBytes = new Uint8Array(encode(manifestData));
  files.set("db/e2e/manifest.msgpack", manifestBytes);

  const manifest = parseManifest(manifestBytes);

  return { files, manifest };
}

/** Patch globalThis.fetch to serve the wa-sqlite WASM binary from disk */
function patchWasmFetch(originalFetch: typeof globalThis.fetch): typeof globalThis.fetch {
  const wasmPath = path.join(
    __dirname,
    "../node_modules/wa-sqlite/dist/wa-sqlite-async.wasm",
  );
  const wasmBuf = fs.readFileSync(wasmPath);
  const ab = wasmBuf.buffer.slice(wasmBuf.byteOffset, wasmBuf.byteOffset + wasmBuf.byteLength);

  return async (input: RequestInfo | URL, init?: RequestInit) => {
    const url = typeof input === "string" ? input : input.toString();
    if (url.endsWith(".wasm")) {
      return new Response(ab, {
        status: 200,
        headers: { "Content-Type": "application/wasm" },
      });
    }
    return originalFetch(input, init);
  };
}

let e2eVfsCounter = 0;

describe("e2e: SQL queries through turbolite VFS", () => {
  let mockFiles: Map<string, Uint8Array>;
  let manifest: Manifest;

  // wa-sqlite instances (loaded once)
  let sqlite3: any;
  let SQLite: any;

  beforeAll(async () => {
    // Create real SQLite database and encode as turbolite format
    const dbBytes = createTestDatabase();
    const encoded = encodeTurboliteFormat(dbBytes);
    mockFiles = encoded.files;
    manifest = encoded.manifest;

    // Load wa-sqlite (patch fetch for WASM binary)
    const savedFetch = globalThis.fetch;
    globalThis.fetch = patchWasmFetch(savedFetch);

    const { default: SQLiteAsyncESMFactory } = await import(
      "wa-sqlite/dist/wa-sqlite-async.mjs"
    );
    SQLite = await import("wa-sqlite");
    const module = await SQLiteAsyncESMFactory();
    sqlite3 = SQLite.Factory(module);

    globalThis.fetch = savedFetch;
  });

  function makeMockFetch(): typeof globalThis.fetch {
    return (async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = typeof input === "string" ? input : input.toString();
      const urlPath = url.replace("http://test/", "");
      const data = mockFiles.get(urlPath);

      if (!data) {
        return { ok: false, status: 404, statusText: "Not Found" } as Response;
      }

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
            arrayBuffer: () =>
              Promise.resolve(
                slice.buffer.slice(
                  slice.byteOffset,
                  slice.byteOffset + slice.byteLength,
                ),
              ),
          } as unknown as Response;
        }
      }

      return {
        ok: true,
        status: 200,
        arrayBuffer: () =>
          Promise.resolve(
            data.buffer.slice(data.byteOffset, data.byteOffset + data.byteLength),
          ),
      } as unknown as Response;
    }) as typeof globalThis.fetch;
  }

  async function openDb(): Promise<{ db: number; cache: PageCache; fetcher: PageFetcher }> {
    const config: TurboliteConfig = {
      baseUrl: "http://test",
      prefix: "db/e2e",
      fetch: makeMockFetch(),
      maxCacheBytes: 64 * 1024 * 1024,
    };

    const fetcher = new PageFetcher(config);
    const cache = new PageCache(config);
    const pageIndex = buildPageIndex(manifest);

    // Prefetch interior + index bundles
    for (const key of Object.values(manifest.interior_chunk_keys)) {
      const bundle = await fetcher.fetchBundle(key);
      await cache.putPages(bundle.map((b) => [b.pageNum, b.data]));
    }
    for (const key of Object.values(manifest.index_chunk_keys)) {
      const bundle = await fetcher.fetchBundle(key);
      await cache.putPages(bundle.map((b) => [b.pageNum, b.data]));
    }

    // Register VFS (counter ensures uniqueness across multiple openDb calls)
    const vfsName = `turbolite-e2e-${e2eVfsCounter++}`;
    const vfs = new TurboliteVFS(vfsName, { manifest, pageIndex }, cache, fetcher);
    sqlite3.vfs_register(vfs, false);

    const db = await sqlite3.open_v2(
      `e2e-${Date.now()}.db`,
      SQLite.SQLITE_OPEN_READONLY,
      vfsName,
    );

    return { db, cache, fetcher };
  }

  it("reads table schema", async () => {
    const { db } = await openDb();
    try {
      const tables: string[] = [];
      await sqlite3.exec(
        db,
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name",
        (row: any[]) => tables.push(row[0] as string),
      );
      expect(tables).toEqual(["posts", "users"]);
    } finally {
      await sqlite3.close(db);
    }
  });

  it("SELECT * FROM users", async () => {
    const { db } = await openDb();
    try {
      const rows: any[] = [];
      await sqlite3.exec(
        db,
        "SELECT id, name, email, age FROM users ORDER BY id",
        (row: any[], columns: string[]) => {
          rows.push(Object.fromEntries(columns.map((c, i) => [c, row[i]])));
        },
      );

      expect(rows).toHaveLength(5);
      expect(rows[0]).toEqual({ id: 1, name: "Alice", email: "alice@example.com", age: 30 });
      expect(rows[1]).toEqual({ id: 2, name: "Bob", email: "bob@example.com", age: 25 });
      expect(rows[4]).toEqual({ id: 5, name: "Eve", email: "eve@example.com", age: 32 });
    } finally {
      await sqlite3.close(db);
    }
  });

  it("WHERE clause with parameter binding", async () => {
    const { db } = await openDb();
    try {
      const rows: any[] = [];
      for await (const stmt of sqlite3.statements(db, "SELECT name FROM users WHERE age > ?")) {
        if (!stmt) continue;
        sqlite3.bind(stmt, 1, 29);
        while ((await sqlite3.step(stmt)) === SQLite.SQLITE_ROW) {
          rows.push(sqlite3.column(stmt, 0));
        }
      }
      expect(rows.sort()).toEqual(["Alice", "Charlie", "Eve"]);
    } finally {
      await sqlite3.close(db);
    }
  });

  it("JOIN query across tables", async () => {
    const { db } = await openDb();
    try {
      const rows: any[] = [];
      await sqlite3.exec(
        db,
        `SELECT u.name, p.title
         FROM posts p JOIN users u ON u.id = p.user_id
         ORDER BY p.id`,
        (row: any[], columns: string[]) => {
          rows.push(Object.fromEntries(columns.map((c, i) => [c, row[i]])));
        },
      );

      expect(rows).toEqual([
        { name: "Alice", title: "Hello World" },
        { name: "Bob", title: "Testing" },
        { name: "Alice", title: "Update" },
      ]);
    } finally {
      await sqlite3.close(db);
    }
  });

  it("aggregate query", async () => {
    const { db } = await openDb();
    try {
      const rows: any[] = [];
      await sqlite3.exec(
        db,
        "SELECT COUNT(*) as cnt, AVG(age) as avg_age FROM users",
        (row: any[], columns: string[]) => {
          rows.push(Object.fromEntries(columns.map((c, i) => [c, row[i]])));
        },
      );

      expect(rows).toHaveLength(1);
      expect(rows[0].cnt).toBe(5);
      expect(rows[0].avg_age).toBe(30); // (30+25+35+28+32)/5 = 30
    } finally {
      await sqlite3.close(db);
    }
  });

  it("index scan query", async () => {
    const { db } = await openDb();
    try {
      const rows: any[] = [];
      await sqlite3.exec(
        db,
        "SELECT name FROM users WHERE age = 25",
        (row: any[]) => rows.push(row[0]),
      );
      expect(rows).toEqual(["Bob"]);
    } finally {
      await sqlite3.close(db);
    }
  });

  it("cache stats after queries", async () => {
    const { db, cache, fetcher } = await openDb();
    try {
      // Run a query to populate cache
      await sqlite3.exec(db, "SELECT * FROM users");
      await sqlite3.exec(db, "SELECT * FROM posts");

      const stats = cache.getStats(fetcher);
      expect(stats.cachedPages).toBeGreaterThan(0);
      expect(stats.fetchCount).toBeGreaterThan(0);
      expect(stats.hits).toBeGreaterThan(0);
    } finally {
      await sqlite3.close(db);
    }
  });
});

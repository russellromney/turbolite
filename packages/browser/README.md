# @turbolite/browser

Read-only browser client for turbolite S3-backed SQLite databases. Query your database directly in the browser with no backend SQL engine needed.

## How it works

1. Fetches the turbolite manifest (page group layout) from your backend
2. Prefetches B-tree interior pages for instant traversal
3. On query, fetches only the needed page group frames via HTTP Range requests
4. Decompresses with zstd and feeds pages to wa-sqlite's WASM SQLite engine
5. Caches pages in memory (LRU) and optionally in OPFS for persistence

## Install

```bash
npm install @turbolite/browser
```

## Usage

```ts
import { TurboliteDB } from "@turbolite/browser";

const db = await TurboliteDB.open({
  baseUrl: "/api/turbolite",  // your backend proxy
  prefix: "databases/my-app", // S3 key prefix
});

const rows = await db.query("SELECT * FROM users WHERE id = ?", [42]);
console.log(rows);

await db.close();
```

## Backend proxy

The browser client needs a backend to proxy requests to S3 (to avoid exposing credentials). Your proxy just forwards requests:

```
GET /api/turbolite/{prefix}/manifest.msgpack  -> S3
GET /api/turbolite/{prefix}/pg/0_v1           -> S3 (supports Range headers)
GET /api/turbolite/{prefix}/int/0_v1          -> S3 (interior bundles)
```

Any S3-compatible storage works: AWS S3, Cloudflare R2, Tigris, MinIO, etc.

## Configuration

```ts
const db = await TurboliteDB.open({
  baseUrl: "/api/turbolite",
  prefix: "databases/my-app",

  // Optional: custom fetch (e.g. add auth headers)
  fetch: (url, init) => fetch(url, {
    ...init,
    headers: { ...init?.headers, Authorization: `Bearer ${token}` },
  }),

  // Optional: max in-memory cache (default 64MB)
  maxCacheBytes: 32 * 1024 * 1024,

  // Optional: persist cache to OPFS across page loads
  opfsCache: true,
});
```

## Diagnostics

```ts
const stats = db.stats();
// { hits, misses, fetchCount, fetchBytes, cachedPages, cachedBytes }
```

## Limitations

- **Read-only.** Writes require the server-side turbolite engine.
- Requires the async wa-sqlite build (included automatically).
- OPFS cache requires a secure context (HTTPS).

## License

Apache-2.0

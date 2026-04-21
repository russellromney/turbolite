# turbolite

SQLite for Node.js with compressed page groups and optional S3 cloud storage. Returns standard **better-sqlite3** connections with full API: prepared statements, param binding, transactions, user-defined functions, aggregates, and more.

## Install

```bash
npm install turbolite
```

The postinstall script patches better-sqlite3 to enable URI filename support (`SQLITE_USE_URI=1`) and rebuilds from source. This is required for VFS selection via URI.

## Usage

### Local mode (compressed)

```js
const turbolite = require('turbolite');

const db = turbolite.connect('my.db');

// Full better-sqlite3 API: prepared statements, param binding, transactions
const insert = db.prepare('INSERT INTO users (name, age) VALUES (?, ?)');
insert.run('alice', 30);
insert.run('bob', 25);

const rows = db.prepare('SELECT * FROM users WHERE age > ?').all(20);
// [{ id: 1, name: 'alice', age: 30 }, { id: 2, name: 'bob', age: 25 }]

// Transactions
const batchInsert = db.transaction((users) => {
  for (const u of users) insert.run(u.name, u.age);
});
batchInsert([{ name: 'charlie', age: 35 }]);

db.close();
```

### S3 cloud mode

Pages are compressed locally and durably synced to S3 (or any S3-compatible store).

```js
const db = turbolite.connect('my.db', {
  mode: 's3',
  bucket: 'my-bucket',
  region: 'us-east-1',
});
```

With a custom S3-compatible endpoint (Tigris, MinIO, etc.):

```js
const db = turbolite.connect('my.db', {
  mode: 's3',
  bucket: 'my-bucket',
  endpoint: 'https://fly.storage.tigris.dev',
  region: 'auto',
});
```

## API

### `turbolite.connect(path, options?)`

Open a database. Returns a standard `better-sqlite3.Database`.

- **path** `string` -- Path to the database file.
- **options** `object` -- Optional. Defaults to local compressed mode.

#### Options

| Option | Type | Default | Description |
|---|---|---|---|
| `mode` | `'local' \| 's3'` | `'local'` | Storage mode. |
| `bucket` | `string` | -- | S3 bucket name (required for `mode='s3'`). |
| `endpoint` | `string` | AWS S3 | Custom S3 endpoint URL. |
| `prefix` | `string` | `'turbolite'` | S3 key prefix. |
| `region` | `string` | SDK default | AWS region. |
| `cacheDir` | `string` | `/tmp/turbolite` | Local cache directory (S3 mode). |
| `compressionLevel` | `number` | `3` | Zstd compression level 1-22. |
| `readOnly` | `boolean` | `false` | Open in read-only mode. |
| `pageCache` | `string` | `'64MB'` | In-memory page cache size. Set to `'0'` to disable. |

### `turbolite.load(db)`

Load the turbolite extension into an existing better-sqlite3 Database.

## Architecture

Each `connect()` call creates an isolated VFS instance with its own manifest, cache, and page group state. This ensures multiple databases in the same process don't share state.

Under the hood:
1. The turbolite loadable extension is loaded into better-sqlite3's SQLite
2. `turbolite_register_vfs(name, cache_dir)` creates a named VFS instance
3. The database is opened via URI: `file:path?vfs=turbolite-N`
4. `PRAGMA cache_size=0` disables SQLite's cache (turbolite manages its own)

## Build from source

```bash
cd packages/node

# Build the loadable extension (requires Rust)
npm run build-ext

# Install dependencies (patches + rebuilds better-sqlite3 for URI support)
npm install

# Run tests
npm test
```

## Environment variables

| Variable | Description |
|---|---|
| `TURBOLITE_EXT_PATH` | Override path to the loadable extension binary |
| `TURBOLITE_BUCKET` | S3 bucket name (S3 mode) |
| `TURBOLITE_REGION` | AWS region (S3 mode) |
| `TURBOLITE_ENDPOINT_URL` | Custom S3 endpoint URL (S3 mode) |
| `TURBOLITE_MEM_CACHE_BUDGET` | Page cache size (default `64MB`) |
| `TURBOLITE_COMPRESSION_LEVEL` | Zstd level 1-22 (default `3`) |

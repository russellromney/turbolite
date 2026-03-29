# turbolite

Compressed SQLite for Node.js with optional S3 tiered storage. Transparent zstd compression via a custom VFS — just `npm install` and use.

## Install

```bash
npm install turbolite
```

## Usage

### Local mode (compressed)

```js
import { Database } from "turbolite";

const db = new Database("my.db");

db.exec(`
  CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER);
  INSERT INTO users VALUES (1, 'alice', 30);
  INSERT INTO users VALUES (2, 'bob', 25);
`);

const rows = db.query("SELECT * FROM users WHERE age > 20");
// [{ id: 1, name: 'alice', age: 30 }, { id: 2, name: 'bob', age: 25 }]

db.close();
```

### S3 tiered mode

Pages are compressed locally and durably synced to S3 (or any S3-compatible store). Any instance with access to the same bucket can read the database.

```js
import { Database } from "turbolite";

const db = new Database("my.db", {
  mode: "s3",
  bucket: "my-bucket",
  region: "us-east-1",
});
```

With a custom S3-compatible endpoint (Tigris, MinIO, etc.):

```js
const db = new Database("my.db", {
  mode: "s3",
  bucket: "my-bucket",
  endpoint: "https://fly.storage.tigris.dev",
  region: "auto",
});
```

AWS credentials are read from the standard chain: `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` env vars, `~/.aws/credentials`, or instance metadata.

## API

### `new Database(path, options?)`

Open a database.

- **path** `string` — Path to the database file.
- **options** `object` — Optional. Defaults to local compressed mode.

#### Local mode options

| Option | Type | Default | Description |
|---|---|---|---|
| `compression` | `number \| null` | `null` | zstd compression level 1–22. `null` for no compression. |

```js
// zstd level 9
const db = new Database("my.db", { compression: 9 });

// No compression
const db = new Database("my.db", { compression: null });
```

#### S3 tiered mode options

Set `mode: "s3"` to enable S3 tiered storage.

| Option | Type | Default | Description |
|---|---|---|---|
| `mode` | `"s3"` | — | Enables S3 tiered storage. |
| `bucket` | `string` | — | **Required.** S3 bucket name. Also read from `TURBOLITE_BUCKET`. |
| `region` | `string` | SDK default | AWS region (e.g. `"us-east-1"`). Also read from `TURBOLITE_REGION`. |
| `endpoint` | `string` | AWS S3 | Custom S3 endpoint URL. Also read from `TURBOLITE_ENDPOINT_URL`. |
| `prefix` | `string` | `"turbolite"` | S3 key prefix for all stored objects. |
| `cache_dir` | `string` | db file's directory | Local directory for the page cache. |

**Note:** For AWS S3 Express One Zone buckets (names ending in `--x-s3`), omit `endpoint` — the SDK autodiscovers the correct zone endpoint from the bucket name.

### `db.exec(sql)`

Execute SQL that returns no rows (DDL, INSERT, UPDATE, DELETE).

### `db.query(sql)`

Execute a SELECT and return rows as an array of plain objects.

### `db.close()`

Close the database connection.

## Environment variables

| Variable | Description |
|---|---|
| `TURBOLITE_BUCKET` | S3 bucket name (S3 mode) |
| `TURBOLITE_REGION` | AWS region (S3 mode) |
| `TURBOLITE_ENDPOINT_URL` | Custom S3 endpoint URL (S3 mode) |
| `AWS_ACCESS_KEY_ID` | AWS credentials |
| `AWS_SECRET_ACCESS_KEY` | AWS credentials |

## Why a wrapped Database class?

Node can't use `sqlite3.load_extension()` with better-sqlite3 (compiled with `SQLITE_USE_URI=0`), preventing VFS selection via URI. The wrapped `Database` class embeds SQLite with the VFS pre-registered.

## Build from source

```bash
cd packages/node
npm install
npx @napi-rs/cli build --release --platform
```

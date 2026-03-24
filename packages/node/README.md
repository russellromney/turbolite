# turbolite

Compressed SQLite for Node.js. Transparent zstd compression via a custom VFS — just `npm install` and use.

## Install

```bash
npm install turbolite
```

## Usage

```js
const { Database } = require("turbolite");

// Open a compressed database (zstd level 3 by default)
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

### ESM

```js
import { Database } from "turbolite";
```

### Options

```js
// Custom compression level (1-22, higher = smaller but slower)
const db = new Database("my.db", 9);

// No compression (passthrough mode)
const db = new Database("my.db", null);
```

## API

### `new Database(path, compression?)`

Open a database with transparent compression.

- **path**: Path to the database file.
- **compression**: zstd level 1-22 (default 3). `null` for no compression.

### `db.exec(sql)`

Execute SQL that returns no rows (DDL, INSERT, UPDATE, DELETE).

### `db.query(sql)`

Execute a SELECT and return rows as an array of objects.

### `db.close()`

Close the database connection.

## Why a wrapped Database class?

The Python package uses `sqlite3.load_extension()` so you keep your existing `sqlite3` code. Node can't do this because better-sqlite3 compiles with `SQLITE_USE_URI=0`, preventing VFS selection via URI. The wrapped `Database` class embeds SQLite with the VFS pre-registered.

## Build from source

```bash
cd packages/node
npm install
npx @napi-rs/cli build --release --platform
```

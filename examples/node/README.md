# turbolite — Node.js examples

HTTP API servers backed by turbolite-compressed SQLite.

## Local compressed (`local.mjs`)

```bash
npm install turbolite
node examples/node/local.mjs
```

## S3 tiered (`tiered.mjs`)

```bash
npm install turbolite
TURBOLITE_BUCKET=my-bucket node examples/node/tiered.mjs
```

Or via Make:

```bash
make example-node          # local
make example-node-tiered   # S3 tiered
```

## Try it

```bash
curl -X POST localhost:3000/books -d '{"title":"Dune","year":1965}'
curl localhost:3000/books
```

## How it works

1. `new Database(path)` - open a compressed database (local mode)
2. `new Database(path, { mode: "s3", bucket: "..." })` - open an S3 tiered database
3. Standard better-sqlite3-compatible API from there - `exec`, `query`, `prepare`

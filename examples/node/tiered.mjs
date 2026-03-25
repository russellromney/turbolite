/**
 * turbolite example — Node.js S3 tiered
 *
 * A small HTTP API server backed by turbolite with S3 tiered storage.
 * Pages are compressed locally and synced to S3 for durability.
 *
 * Run:
 *   npm install turbolite
 *   TURBOLITE_BUCKET=my-bucket node examples/node/tiered.mjs
 *
 * Or via Make:
 *   make example-node-tiered
 *
 * Then:
 *   curl -X POST localhost:3000/books -d '{"title":"Dune","year":1965}'
 *   curl localhost:3000/books
 *
 * Required environment variables:
 *   TURBOLITE_BUCKET           S3 bucket name
 *
 * Optional:
 *   TURBOLITE_ENDPOINT_URL     S3 endpoint (Tigris, MinIO, etc.)
 *   TURBOLITE_REGION           AWS region
 *   TURBOLITE_PREFIX           S3 key prefix (default "turbolite")
 *   TURBOLITE_CACHE_DIR        local cache directory
 *   AWS_ACCESS_KEY_ID          S3 credentials
 *   AWS_SECRET_ACCESS_KEY      S3 credentials
 */

import { Database } from "turbolite";
import http from "http";
import { mkdtempSync, rmSync } from "fs";
import { tmpdir } from "os";
import { join } from "path";

// ── Set up database ─────────────────────────────────────────────────

const dataDir = mkdtempSync(join(tmpdir(), "turbolite-tiered-"));

// S3 tiered mode: pages compressed locally, synced to S3.
// Requires TURBOLITE_BUCKET to be set.
const db = new Database(join(dataDir, "books.db"), {
  mode: "s3",
  bucket: process.env.TURBOLITE_BUCKET || "my-bucket",
  endpoint: process.env.TURBOLITE_ENDPOINT_URL,
});

db.exec(`
  CREATE TABLE books (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT NOT NULL,
    year INTEGER
  )
`);

// ── HTTP server ─────────────────────────────────────────────────────

const server = http.createServer((req, res) => {
  if (req.method === "GET" && req.url === "/books") {
    const rows = db.query("SELECT * FROM books ORDER BY year");
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify(rows));

  } else if (req.method === "POST" && req.url === "/books") {
    let body = "";
    req.on("data", (c) => (body += c));
    req.on("end", () => {
      const { title, year } = JSON.parse(body);
      db.exec(`INSERT INTO books (title, year) VALUES ('${title}', ${year})`);
      const rows = db.query("SELECT * FROM books ORDER BY id DESC LIMIT 1");
      res.writeHead(201, { "Content-Type": "application/json" });
      res.end(JSON.stringify(rows[0]));
    });

  } else {
    res.writeHead(404);
    res.end("Not found");
  }
});

process.on("SIGINT", () => {
  db.close();
  rmSync(dataDir, { recursive: true, force: true });
  process.exit(0);
});

server.listen(3000, () => {
  console.log(`turbolite (S3 tiered) -- listening on :3000`);
});

/**
 * turbolite example — Node.js local compressed
 *
 * A small HTTP API server backed by turbolite-compressed SQLite (local mode).
 * See tiered.mjs for S3 tiered storage.
 *
 * Run:
 *   npm install turbolite
 *   node examples/node/local.mjs
 *
 * Or via Make:
 *   make example-node
 *
 * Then:
 *   curl -X POST localhost:3000/books -d '{"title":"Dune","year":1965}'
 *   curl localhost:3000/books
 */

import { Database } from "turbolite";
import http from "http";
import { mkdtempSync, rmSync } from "fs";
import { tmpdir } from "os";
import { join } from "path";

// ── Set up database ─────────────────────────────────────────────────

const dataDir = mkdtempSync(join(tmpdir(), "turbolite-example-"));
const db = new Database(join(dataDir, "books.db"));

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
  console.log(`turbolite — listening on :3000`);
});

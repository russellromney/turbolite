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

import { connect } from "turbolite";
import http from "http";
import { mkdtempSync, rmSync } from "fs";
import { tmpdir } from "os";
import { join } from "path";

// ── Set up database ─────────────────────────────────────────────────

const dataDir = mkdtempSync(join(tmpdir(), "turbolite-example-"));
const dbPath = join(dataDir, "books.db");
// File-first local mode (the default): the user-visible artifact is
// `books.db`. Hidden implementation state (manifest, cache, staging logs)
// lives next to it at `books.db-turbolite/`.
//
// `books.db` is turbolite's compressed page image. It is not promised to
// be opened by stock sqlite3. To get a normal SQLite file the standard
// CLI can read, run `db.exec("VACUUM INTO 'books-export.sqlite'")`.
const db = connect(dbPath);

db.exec(`
  CREATE TABLE books (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT NOT NULL,
    year INTEGER
  )
`);

// ── HTTP server ─────────────────────────────────────────────────────

const listAll = db.prepare("SELECT * FROM books ORDER BY year");
const insertBook = db.prepare("INSERT INTO books (title, year) VALUES (?, ?)");
const lastBook = db.prepare("SELECT * FROM books ORDER BY id DESC LIMIT 1");

const server = http.createServer((req, res) => {
  if (req.method === "GET" && req.url === "/books") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify(listAll.all()));

  } else if (req.method === "POST" && req.url === "/books") {
    let body = "";
    req.on("data", (c) => (body += c));
    req.on("end", () => {
      const { title, year } = JSON.parse(body);
      insertBook.run(title, year);
      res.writeHead(201, { "Content-Type": "application/json" });
      res.end(JSON.stringify(lastBook.get()));
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

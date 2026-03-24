/**
 * turbolite example — Node.js (http + koffi)
 *
 * A small HTTP API server backed by turbolite-compressed SQLite.
 *
 * Run:
 *   make lib-bundled
 *   make example-node
 *
 * Then:
 *   curl -X POST localhost:3000/books -d '{"title":"Dune","year":1965}'
 *   curl localhost:3000/books
 */

import koffi from "koffi";
import http from "http";
import { mkdtempSync, rmSync } from "fs";
import { tmpdir } from "os";
import { join, resolve, dirname } from "path";
import { fileURLToPath } from "url";

const __dirname = dirname(fileURLToPath(import.meta.url));

// ── Load turbolite ──────────────────────────────────────────────────

const ext = process.platform === "darwin" ? "dylib" : "so";
const lib = koffi.load(
  resolve(__dirname, `../../target/release/libsqlite_compress_encrypt_vfs.${ext}`)
);

const turbolite_version = lib.func("turbolite_version", "str", []);
const turbolite_last_error = lib.func("turbolite_last_error", "str", []);
const turbolite_register_compressed = lib.func("turbolite_register_compressed", "int", ["str", "str", "int"]);
const turbolite_open = lib.func("turbolite_open", "void*", ["str", "str"]);
const turbolite_exec = lib.func("turbolite_exec", "int", ["void*", "str"]);
const turbolite_query_json = lib.func("turbolite_query_json", "str", ["void*", "str"]);
const turbolite_close = lib.func("turbolite_close", "void", ["void*"]);

// ── Set up database ─────────────────────────────────────────────────

const dataDir = mkdtempSync(join(tmpdir(), "turbolite-example-"));
turbolite_register_compressed("demo", dataDir, 3);
const db = turbolite_open(join(dataDir, "books.db"), "demo");
turbolite_exec(db, `
  CREATE TABLE books (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT NOT NULL,
    year INTEGER
  )
`);

// ── HTTP server ─────────────────────────────────────────────────────

const server = http.createServer((req, res) => {
  if (req.method === "GET" && req.url === "/books") {
    const json = turbolite_query_json(db, "SELECT * FROM books ORDER BY year");
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(json);

  } else if (req.method === "POST" && req.url === "/books") {
    let body = "";
    req.on("data", (c) => (body += c));
    req.on("end", () => {
      const { title, year } = JSON.parse(body);
      turbolite_exec(db, `INSERT INTO books (title, year) VALUES ('${title}', ${year})`);
      const json = turbolite_query_json(db, "SELECT * FROM books ORDER BY id DESC LIMIT 1");
      res.writeHead(201, { "Content-Type": "application/json" });
      res.end(json);
    });

  } else {
    res.writeHead(404);
    res.end("Not found");
  }
});

process.on("SIGINT", () => {
  turbolite_close(db);
  rmSync(dataDir, { recursive: true, force: true });
  process.exit(0);
});

server.listen(3000, () => {
  console.log(`turbolite ${turbolite_version()} — listening on :3000`);
});

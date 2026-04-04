/**
 * End-to-end integration test for turbolite shared library from Node.js.
 *
 * Full pipeline: register VFS → open → create → insert → query → verify → close.
 * Uses koffi for C FFI bindings.
 *
 * Run:
 *   make test-ffi-node
 */

import koffi from "koffi";
import { mkdtempSync, rmSync } from "fs";
import { tmpdir } from "os";
import { join, resolve, dirname } from "path";
import { fileURLToPath } from "url";

const __dirname = dirname(fileURLToPath(import.meta.url));

// ── Load library ────────────────────────────────────────────────────

const ext = process.platform === "darwin" ? "dylib" : "so";
const libPath = resolve(
  __dirname,
  `../../target/release/libturbolite.${ext}`
);

const lib = koffi.load(libPath);

// ── Declare FFI functions ───────────────────────────────────────────

const turbolite_version = lib.func("turbolite_version", "str", []);
const turbolite_last_error = lib.func("turbolite_last_error", "str", []);
const turbolite_register_local = lib.func(
  "turbolite_register_local",
  "int",
  ["str", "str", "int"]
);
const turbolite_open = lib.func("turbolite_open", "void*", ["str", "str"]);
const turbolite_exec = lib.func("turbolite_exec", "int", ["void*", "str"]);
const turbolite_query_json = lib.func("turbolite_query_json", "str", [
  "void*",
  "str",
]);
const turbolite_free_string = lib.func("turbolite_free_string", "void", [
  "void*",
]);
const turbolite_close = lib.func("turbolite_close", "void", ["void*"]);
const turbolite_clear_caches = lib.func("turbolite_clear_caches", "void", []);
const turbolite_invalidate_cache = lib.func(
  "turbolite_invalidate_cache",
  "int",
  ["str"]
);

// ── Test harness ────────────────────────────────────────────────────

let passed = 0;
let failed = 0;

function test(name, fn) {
  try {
    fn();
    console.log(`  PASS  ${name}`);
    passed++;
  } catch (e) {
    console.log(`  FAIL  ${name}: ${e.message}`);
    failed++;
  }
}

function assert(cond, msg) {
  if (!cond) throw new Error(msg || "assertion failed");
}

function assertEqual(a, b, msg) {
  if (a !== b)
    throw new Error(msg || `expected ${JSON.stringify(b)}, got ${JSON.stringify(a)}`);
}

function makeTmpDir() {
  return mkdtempSync(join(tmpdir(), "turbolite-node-"));
}

function queryJSON(db, sql) {
  const raw = turbolite_query_json(db, sql);
  if (!raw) {
    throw new Error(`query failed: ${turbolite_last_error() || "unknown"}`);
  }
  return JSON.parse(raw);
}

// ── Tests ───────────────────────────────────────────────────────────

test("version returns valid string", () => {
  const v = turbolite_version();
  assert(v && v.length > 0, "empty version");
  assert(v.includes("."), `unexpected format: ${v}`);
});

test("null name returns error", () => {
  const rc = turbolite_register_local(null, "/tmp", 3);
  assertEqual(rc, -1);
  const err = turbolite_last_error();
  assert(err && err.includes("name"), `error should mention name: ${err}`);
});

test("null base_dir returns error", () => {
  const rc = turbolite_register_local("node-null-test", null, 3);
  assertEqual(rc, -1);
});

test("register local VFS", () => {
  const dir = makeTmpDir();
  try {
    assertEqual(turbolite_register_local("node-local", dir, 3), 0);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test("open and close database", () => {
  const dir = makeTmpDir();
  try {
    turbolite_register_local("node-openclose", dir, 3);
    const db = turbolite_open(join(dir, "test.db"), "node-openclose");
    assert(db, `open returned null: ${turbolite_last_error()}`);
    turbolite_close(db);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test("exec CREATE TABLE + INSERT", () => {
  const dir = makeTmpDir();
  try {
    turbolite_register_local("node-exec", dir, 3);
    const db = turbolite_open(join(dir, "test.db"), "node-exec");
    assert(db);

    assertEqual(
      turbolite_exec(
        db,
        "CREATE TABLE t (id INTEGER, val TEXT); INSERT INTO t VALUES (1, 'hello');"
      ),
      0
    );
    turbolite_close(db);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test("bad SQL returns error", () => {
  const dir = makeTmpDir();
  try {
    turbolite_register_local("node-badsql", dir, 3);
    const db = turbolite_open(join(dir, "test.db"), "node-badsql");
    assert(db);

    assertEqual(turbolite_exec(db, "NOT VALID SQL"), -1);
    assert(turbolite_last_error(), "expected error message");

    turbolite_close(db);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test("full round-trip: create, insert, query, verify", () => {
  const dir = makeTmpDir();
  try {
    turbolite_register_local("node-roundtrip", dir, 3);
    const db = turbolite_open(join(dir, "test.db"), "node-roundtrip");
    assert(db);

    turbolite_exec(
      db,
      `CREATE TABLE users (id INTEGER, name TEXT, age INTEGER);
       INSERT INTO users VALUES (1, 'alice', 30);
       INSERT INTO users VALUES (2, 'bob', 25);
       INSERT INTO users VALUES (3, 'carol', 35);`
    );

    const rows = queryJSON(db, "SELECT * FROM users ORDER BY id");
    assertEqual(rows.length, 3);
    assertEqual(rows[0].name, "alice");
    assertEqual(rows[0].age, 30);
    assertEqual(rows[1].name, "bob");
    assertEqual(rows[2].age, 35);

    turbolite_close(db);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test("query with WHERE clause", () => {
  const dir = makeTmpDir();
  try {
    turbolite_register_local("node-where", dir, 3);
    const db = turbolite_open(join(dir, "test.db"), "node-where");
    assert(db);

    turbolite_exec(
      db,
      `CREATE TABLE items (id INTEGER, price REAL);
       INSERT INTO items VALUES (1, 9.99);
       INSERT INTO items VALUES (2, 19.99);
       INSERT INTO items VALUES (3, 29.99);`
    );

    const rows = queryJSON(
      db,
      "SELECT * FROM items WHERE price > 15.0 ORDER BY id"
    );
    assertEqual(rows.length, 2);
    assertEqual(rows[0].id, 2);
    assertEqual(rows[1].price, 29.99);

    turbolite_close(db);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test("data persists across connections", () => {
  const dir = makeTmpDir();
  try {
    turbolite_register_local("node-persist", dir, 3);
    const dbPath = join(dir, "persist.db");

    // Write
    let db = turbolite_open(dbPath, "node-persist");
    assert(db);
    turbolite_exec(
      db,
      "CREATE TABLE kv (k TEXT, v TEXT); INSERT INTO kv VALUES ('hello', 'world');"
    );
    turbolite_close(db);

    // Re-open and read
    db = turbolite_open(dbPath, "node-persist");
    assert(db);
    const rows = queryJSON(db, "SELECT * FROM kv");
    assertEqual(rows.length, 1);
    assertEqual(rows[0].k, "hello");
    assertEqual(rows[0].v, "world");
    turbolite_close(db);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test("NULL handles don't crash", () => {
  turbolite_close(null);
  turbolite_free_string(null);
  assertEqual(turbolite_invalidate_cache(null), -1);
});

test("clear_caches does not crash", () => {
  turbolite_clear_caches();
});

// ── Tiered S3 tests (only run when TIERED_TEST_BUCKET is set) ────────

const TIERED_BUCKET = process.env.TIERED_TEST_BUCKET;
let hasTiered = false;
try {
  lib.func("turbolite_register_tiered", "int", [
    "str", "str", "str", "str", "str", "str",
  ]);
  hasTiered = true;
} catch (_) {}

if (hasTiered && TIERED_BUCKET) {
  const turbolite_register_tiered = lib.func(
    "turbolite_register_tiered",
    "int",
    ["str", "str", "str", "str", "str", "str"]
  );

  const TIERED_ENDPOINT = process.env.AWS_ENDPOINT_URL || null;
  const TIERED_REGION = process.env.AWS_REGION || "auto";

  function tieredPrefix(tag) {
    return `test/ffi-node/${tag}/${Date.now()}${Math.random().toString(36).slice(2)}`;
  }

  test("tiered: register S3 VFS", () => {
    const dir = makeTmpDir();
    try {
      const rc = turbolite_register_tiered(
        "node-tiered-reg",
        TIERED_BUCKET,
        tieredPrefix("reg"),
        dir,
        TIERED_ENDPOINT,
        TIERED_REGION
      );
      assertEqual(rc, 0, `register failed: ${turbolite_last_error()}`);
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  test("tiered: full round-trip via S3", () => {
    const dir = makeTmpDir();
    try {
      const rc = turbolite_register_tiered(
        "node-tiered-rt",
        TIERED_BUCKET,
        tieredPrefix("roundtrip"),
        dir,
        TIERED_ENDPOINT,
        TIERED_REGION
      );
      assertEqual(rc, 0);

      const db = turbolite_open(join(dir, "test.db"), "node-tiered-rt");
      assert(db, `open failed: ${turbolite_last_error()}`);

      turbolite_exec(
        db,
        `CREATE TABLE users (id INTEGER, name TEXT, age INTEGER);
         INSERT INTO users VALUES (1, 'alice', 30);
         INSERT INTO users VALUES (2, 'bob', 25);
         INSERT INTO users VALUES (3, 'carol', 35);`
      );

      const rows = queryJSON(db, "SELECT * FROM users ORDER BY id");
      assertEqual(rows.length, 3);
      assertEqual(rows[0].name, "alice");
      assertEqual(rows[1].name, "bob");
      assertEqual(rows[2].age, 35);

      turbolite_close(db);
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  test("tiered: null bucket returns error", () => {
    const dir = makeTmpDir();
    try {
      const rc = turbolite_register_tiered(
        "node-tiered-null", null, "prefix", dir, null, null
      );
      assertEqual(rc, -1);
      const err = turbolite_last_error();
      assert(err && err.includes("bucket"), `error should mention bucket: ${err}`);
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });
} else if (hasTiered) {
  console.log("  SKIP  tiered tests (set TIERED_TEST_BUCKET to enable)");
} else {
  console.log("  SKIP  tiered tests (library built without tiered feature)");
}

// ── Summary ─────────────────────────────────────────────────────────

console.log();
console.log(`Loaded: ${libPath}`);
console.log(`Results: ${passed} passed, ${failed} failed`);
if (failed) process.exit(1);

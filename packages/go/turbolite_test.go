package turbolite

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func TestMain(m *testing.M) {
	// Propagate TIERED_TEST_BUCKET to TURBOLITE_BUCKET so the extension
	// registers the S3 VFS during bootstrap (which happens on the first
	// Open() call, typically a local-mode test).
	if b := os.Getenv("TIERED_TEST_BUCKET"); b != "" && os.Getenv("TURBOLITE_BUCKET") == "" {
		os.Setenv("TURBOLITE_BUCKET", b)
	}
	os.Exit(m.Run())
}

func tmpDir(t *testing.T) string {
	t.Helper()
	return t.TempDir()
}

func openLocal(t *testing.T) *sql.DB {
	t.Helper()
	dir := tmpDir(t)
	db, err := Open(filepath.Join(dir, "test.db"), nil)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// ===== Happy path =====

func TestBasicCRUD(t *testing.T) {
	db := openLocal(t)

	_, err := db.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
	if err != nil {
		t.Fatalf("CREATE: %v", err)
	}
	_, err = db.Exec("INSERT INTO t VALUES (1, 'hello')")
	if err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	_, err = db.Exec("INSERT INTO t VALUES (2, 'world')")
	if err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	rows, err := db.Query("SELECT id, val FROM t ORDER BY id")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	defer rows.Close()

	type row struct {
		id  int
		val string
	}
	var results []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.id, &r.val); err != nil {
			t.Fatalf("Scan: %v", err)
		}
		results = append(results, r)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(results))
	}
	if results[0].val != "hello" || results[1].val != "world" {
		t.Fatalf("unexpected values: %v", results)
	}
}

func TestPreparedStatements(t *testing.T) {
	db := openLocal(t)

	db.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")

	stmt, err := db.Prepare("INSERT INTO users (name, age) VALUES (?, ?)")
	if err != nil {
		t.Fatalf("Prepare INSERT: %v", err)
	}
	defer stmt.Close()

	stmt.Exec("Alice", 30)
	stmt.Exec("Bob", 25)
	stmt.Exec("Charlie", 35)

	rows, err := db.Query("SELECT name FROM users WHERE age > ? ORDER BY name", 28)
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	defer rows.Close()

	var names []string
	for rows.Next() {
		var name string
		rows.Scan(&name)
		names = append(names, name)
	}
	if len(names) != 2 || names[0] != "Alice" || names[1] != "Charlie" {
		t.Fatalf("expected [Alice Charlie], got %v", names)
	}

	var age int
	err = db.QueryRow("SELECT age FROM users WHERE name = ?", "Bob").Scan(&age)
	if err != nil {
		t.Fatalf("QueryRow: %v", err)
	}
	if age != 25 {
		t.Fatalf("expected age 25, got %d", age)
	}
}

func TestTransactions(t *testing.T) {
	db := openLocal(t)

	db.Exec("CREATE TABLE counter (id INTEGER PRIMARY KEY, val INTEGER)")
	db.Exec("INSERT INTO counter VALUES (1, 0)")

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}

	for i := 0; i < 100; i++ {
		_, err := tx.Exec("UPDATE counter SET val = val + 1 WHERE id = 1")
		if err != nil {
			t.Fatalf("UPDATE in tx: %v", err)
		}
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	var val int
	db.QueryRow("SELECT val FROM counter WHERE id = 1").Scan(&val)
	if val != 100 {
		t.Fatalf("expected 100, got %d", val)
	}
}

func TestTransactionRollback(t *testing.T) {
	db := openLocal(t)

	db.Exec("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT UNIQUE)")
	db.Exec("INSERT INTO items VALUES (1, 'existing')")

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}

	tx.Exec("INSERT INTO items (name) VALUES ('new1')")
	_, err = tx.Exec("INSERT INTO items (name) VALUES ('existing')")
	if err == nil {
		t.Fatal("expected UNIQUE constraint error")
	}

	tx.Rollback()

	var count int
	db.QueryRow("SELECT COUNT(*) FROM items").Scan(&count)
	if count != 1 {
		t.Fatalf("expected 1 row after rollback, got %d", count)
	}
}

func TestCacheSizeIsZero(t *testing.T) {
	db := openLocal(t)

	var cacheSize int
	err := db.QueryRow("PRAGMA cache_size").Scan(&cacheSize)
	if err != nil {
		t.Fatalf("PRAGMA: %v", err)
	}
	if cacheSize != 0 {
		t.Fatalf("expected cache_size=0, got %d", cacheSize)
	}
}

func TestMultipleConnections(t *testing.T) {
	dir := tmpDir(t)
	dbPath := filepath.Join(dir, "multi.db")

	db1, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("Open db1: %v", err)
	}
	defer db1.Close()

	db1.Exec("CREATE TABLE shared (id INTEGER PRIMARY KEY, val TEXT)")
	db1.Exec("INSERT INTO shared VALUES (1, 'from_db1')")

	db2, err := Open(dbPath, &Options{ReadOnly: true})
	if err != nil {
		t.Fatalf("Open db2: %v", err)
	}
	defer db2.Close()

	var val string
	err = db2.QueryRow("SELECT val FROM shared WHERE id = 1").Scan(&val)
	if err != nil {
		t.Fatalf("QueryRow db2: %v", err)
	}
	if val != "from_db1" {
		t.Fatalf("expected 'from_db1', got %q", val)
	}
}

func TestCompressionLevel(t *testing.T) {
	dir := tmpDir(t)
	db, err := Open(filepath.Join(dir, "comp.db"), &Options{CompressionLevel: 9})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	db.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
	db.Exec("INSERT INTO t VALUES (1, 'compressed')")

	var val string
	db.QueryRow("SELECT val FROM t WHERE id = 1").Scan(&val)
	if val != "compressed" {
		t.Fatalf("expected 'compressed', got %q", val)
	}
}

func TestBlobSupport(t *testing.T) {
	db := openLocal(t)

	db.Exec("CREATE TABLE blobs (id INTEGER PRIMARY KEY, data BLOB)")

	input := []byte{0x00, 0x01, 0x02, 0xff, 0xfe}
	_, err := db.Exec("INSERT INTO blobs VALUES (1, ?)", input)
	if err != nil {
		t.Fatalf("INSERT blob: %v", err)
	}

	var output []byte
	err = db.QueryRow("SELECT data FROM blobs WHERE id = 1").Scan(&output)
	if err != nil {
		t.Fatalf("SELECT blob: %v", err)
	}
	if len(output) != len(input) {
		t.Fatalf("blob length mismatch: %d vs %d", len(output), len(input))
	}
	for i := range input {
		if output[i] != input[i] {
			t.Fatalf("blob byte %d: expected %x, got %x", i, input[i], output[i])
		}
	}
}

func TestWALMode(t *testing.T) {
	db := openLocal(t)

	db.Exec("PRAGMA journal_mode=WAL")

	var mode string
	db.QueryRow("PRAGMA journal_mode").Scan(&mode)
	if mode != "wal" {
		t.Fatalf("expected WAL mode, got %q", mode)
	}

	db.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY)")
	db.Exec("INSERT INTO t VALUES (1)")
}

// ===== Persistence =====

func TestPersistenceAcrossReopen(t *testing.T) {
	dir := tmpDir(t)
	dbPath := filepath.Join(dir, "persist.db")

	db1, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("Open1: %v", err)
	}
	db1.Exec("CREATE TABLE kv (k TEXT PRIMARY KEY, v TEXT)")
	db1.Exec("INSERT INTO kv VALUES ('color', 'blue')")
	db1.Exec("INSERT INTO kv VALUES ('size', 'large')")
	db1.Close()

	db2, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("Open2: %v", err)
	}
	defer db2.Close()

	var count int
	db2.QueryRow("SELECT COUNT(*) FROM kv").Scan(&count)
	if count != 2 {
		t.Fatalf("expected 2 rows after reopen, got %d", count)
	}

	var v string
	db2.QueryRow("SELECT v FROM kv WHERE k = 'color'").Scan(&v)
	if v != "blue" {
		t.Fatalf("expected 'blue', got %q", v)
	}
}

func TestSchemaPersistence(t *testing.T) {
	dir := tmpDir(t)
	dbPath := filepath.Join(dir, "schema.db")

	db1, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("Open1: %v", err)
	}
	db1.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT UNIQUE)")
	db1.Exec("CREATE INDEX idx_users_email ON users(email)")
	db1.Exec("INSERT INTO users VALUES (1, 'alice', 'alice@example.com')")
	db1.Close()

	db2, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("Open2: %v", err)
	}
	defer db2.Close()

	// Schema should be intact
	var tableName string
	err = db2.QueryRow("SELECT name FROM sqlite_master WHERE type='table' AND name='users'").Scan(&tableName)
	if err != nil {
		t.Fatalf("table lookup: %v", err)
	}
	if tableName != "users" {
		t.Fatalf("expected 'users', got %q", tableName)
	}

	// Index should be intact
	var idxName string
	err = db2.QueryRow("SELECT name FROM sqlite_master WHERE type='index' AND name='idx_users_email'").Scan(&idxName)
	if err != nil {
		t.Fatalf("index lookup: %v", err)
	}

	// Data should be intact
	var name string
	db2.QueryRow("SELECT name FROM users WHERE email = ?", "alice@example.com").Scan(&name)
	if name != "alice" {
		t.Fatalf("expected 'alice', got %q", name)
	}
}

// ===== Concurrent reads =====

func TestConcurrentReaders(t *testing.T) {
	dir := tmpDir(t)
	dbPath := filepath.Join(dir, "concurrent.db")

	writer, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("Open writer: %v", err)
	}
	defer writer.Close()

	writer.Exec("PRAGMA journal_mode=WAL")
	writer.Exec("CREATE TABLE data (id INTEGER PRIMARY KEY, val INTEGER)")
	stmt, _ := writer.Prepare("INSERT INTO data VALUES (?, ?)")
	for i := 0; i < 100; i++ {
		stmt.Exec(i, i*10)
	}
	stmt.Close()

	// Open multiple readers
	readers := make([]*sql.DB, 3)
	for i := range readers {
		r, err := Open(dbPath, &Options{ReadOnly: true})
		if err != nil {
			t.Fatalf("Open reader %d: %v", i, err)
		}
		defer r.Close()
		readers[i] = r
	}

	// Read concurrently from goroutines
	var wg sync.WaitGroup
	errs := make([]error, len(readers))
	for i, r := range readers {
		wg.Add(1)
		go func(idx int, db *sql.DB) {
			defer wg.Done()
			var sum int
			err := db.QueryRow("SELECT SUM(val) FROM data").Scan(&sum)
			if err != nil {
				errs[idx] = err
				return
			}
			expected := 99 * 100 / 2 * 10
			if sum != expected {
				errs[idx] = fmt.Errorf("reader %d: expected sum %d, got %d", idx, expected, sum)
			}
		}(i, r)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("reader %d error: %v", i, err)
		}
	}
}

// ===== Failure modes =====

func TestModeValidation(t *testing.T) {
	_, err := Open("/tmp/bad.db", &Options{Mode: "invalid"})
	if err == nil {
		t.Fatal("expected error for invalid mode")
	}
	if !strings.Contains(err.Error(), "'local' or 's3'") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestS3RequiresBucket(t *testing.T) {
	os.Unsetenv("TURBOLITE_BUCKET")

	_, err := Open("/tmp/s3_nobucket.db", &Options{Mode: "s3"})
	if err == nil {
		t.Fatal("expected error without bucket")
	}
	if !strings.Contains(err.Error(), "Bucket") && !strings.Contains(err.Error(), "bucket") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestOpenNonexistentDirectory(t *testing.T) {
	_, err := Open("/nonexistent/deep/path/db.db", nil)
	if err == nil {
		t.Fatal("expected error for nonexistent directory")
	}
	if !strings.Contains(err.Error(), "directory does not exist") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestInvalidSQL(t *testing.T) {
	db := openLocal(t)

	_, err := db.Exec("NOT VALID SQL AT ALL")
	if err == nil {
		t.Fatal("expected error for invalid SQL")
	}
}

func TestDuplicatePrimaryKey(t *testing.T) {
	db := openLocal(t)

	db.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
	db.Exec("INSERT INTO t VALUES (1, 'first')")

	_, err := db.Exec("INSERT INTO t VALUES (1, 'second')")
	if err == nil {
		t.Fatal("expected error for duplicate primary key")
	}

	// Original row should be intact
	var val string
	db.QueryRow("SELECT val FROM t WHERE id = 1").Scan(&val)
	if val != "first" {
		t.Fatalf("expected 'first', got %q", val)
	}
}

func TestQueryNonexistentTable(t *testing.T) {
	db := openLocal(t)

	_, err := db.Query("SELECT * FROM nonexistent_table")
	if err == nil {
		t.Fatal("expected error for nonexistent table")
	}
	if !strings.Contains(err.Error(), "no such table") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestFindExtWithBadPath(t *testing.T) {
	// Temporarily set a bad extension path
	orig := os.Getenv("TURBOLITE_EXT_PATH")
	defer func() {
		if orig != "" {
			os.Setenv("TURBOLITE_EXT_PATH", orig)
		} else {
			os.Unsetenv("TURBOLITE_EXT_PATH")
		}
		// Reset the sync.Once so future tests can find the extension
		extPathOnce = sync.Once{}
		extPath = ""
		extPathErr = nil
	}()

	os.Setenv("TURBOLITE_EXT_PATH", "/nonexistent/turbolite.dylib")
	extPathOnce = sync.Once{}
	extPath = ""
	extPathErr = nil

	_, err := Open(filepath.Join(t.TempDir(), "test.db"), nil)
	if err == nil {
		t.Fatal("expected error with bad extension path")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestFindExtRejectsDirectory(t *testing.T) {
	orig := os.Getenv("TURBOLITE_EXT_PATH")
	defer func() {
		if orig != "" {
			os.Setenv("TURBOLITE_EXT_PATH", orig)
		} else {
			os.Unsetenv("TURBOLITE_EXT_PATH")
		}
		extPathOnce = sync.Once{}
		extPath = ""
		extPathErr = nil
	}()

	// Point to a directory, not a file
	os.Setenv("TURBOLITE_EXT_PATH", t.TempDir())
	extPathOnce = sync.Once{}
	extPath = ""
	extPathErr = nil

	_, err := Open(filepath.Join(t.TempDir(), "test.db"), nil)
	if err == nil {
		t.Fatal("expected error when TURBOLITE_EXT_PATH is a directory")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// ===== Edge cases =====

func TestEmptyTable(t *testing.T) {
	db := openLocal(t)

	db.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")

	var count int
	db.QueryRow("SELECT COUNT(*) FROM t").Scan(&count)
	if count != 0 {
		t.Fatalf("expected 0 rows, got %d", count)
	}

	err := db.QueryRow("SELECT val FROM t WHERE id = 1").Scan(new(string))
	if err != sql.ErrNoRows {
		t.Fatalf("expected sql.ErrNoRows, got %v", err)
	}
}

func TestNullValues(t *testing.T) {
	db := openLocal(t)

	db.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
	db.Exec("INSERT INTO t VALUES (1, NULL)")

	var val sql.NullString
	db.QueryRow("SELECT val FROM t WHERE id = 1").Scan(&val)
	if val.Valid {
		t.Fatalf("expected NULL, got %q", val.String)
	}
}

func TestLargeText(t *testing.T) {
	db := openLocal(t)

	db.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")

	bigStr := strings.Repeat("x", 1_000_000) // 1MB
	_, err := db.Exec("INSERT INTO t VALUES (1, ?)", bigStr)
	if err != nil {
		t.Fatalf("INSERT large text: %v", err)
	}

	var val string
	db.QueryRow("SELECT val FROM t WHERE id = 1").Scan(&val)
	if len(val) != 1_000_000 {
		t.Fatalf("expected 1M chars, got %d", len(val))
	}
	if val != bigStr {
		t.Fatal("large text mismatch")
	}
}

func TestManyRows(t *testing.T) {
	db := openLocal(t)

	db.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)")

	tx, _ := db.Begin()
	stmt, _ := tx.Prepare("INSERT INTO t VALUES (?, ?)")
	for i := 0; i < 10000; i++ {
		stmt.Exec(i, i*2)
	}
	stmt.Close()
	tx.Commit()

	var count int
	db.QueryRow("SELECT COUNT(*) FROM t").Scan(&count)
	if count != 10000 {
		t.Fatalf("expected 10000 rows, got %d", count)
	}

	var sum int64
	db.QueryRow("SELECT SUM(val) FROM t").Scan(&sum)
	if sum != 9999*10000 {
		t.Fatalf("expected sum %d, got %d", 9999*10000, sum)
	}
}

// ===== S3 mode =====

func TestS3WriteRead(t *testing.T) {
	bucket := os.Getenv("TIERED_TEST_BUCKET")
	if bucket == "" {
		bucket = os.Getenv("TURBOLITE_BUCKET")
	}
	if bucket == "" {
		t.Skip("no TIERED_TEST_BUCKET or TURBOLITE_BUCKET set")
	}

	dir := tmpDir(t)
	db, err := Open(filepath.Join(dir, "s3_test.db"), &Options{
		Mode:     "s3",
		Bucket:   bucket,
		Endpoint: os.Getenv("AWS_ENDPOINT_URL"),
		Region:   "auto",
	})
	if err != nil {
		t.Fatalf("Open S3: %v", err)
	}
	defer db.Close()

	db.Exec("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)")
	stmt, _ := db.Prepare("INSERT INTO items VALUES (?, ?)")
	for i := 0; i < 50; i++ {
		stmt.Exec(i, fmt.Sprintf("item_%d", i))
	}
	stmt.Close()

	db.Exec("PRAGMA wal_checkpoint(TRUNCATE)")

	var count int
	db.QueryRow("SELECT COUNT(*) FROM items").Scan(&count)
	if count != 50 {
		t.Fatalf("expected 50 rows, got %d", count)
	}

	var name string
	db.QueryRow("SELECT name FROM items WHERE id = ?", 25).Scan(&name)
	if name != "item_25" {
		t.Fatalf("expected 'item_25', got %q", name)
	}
}

func TestS3CacheSizeZero(t *testing.T) {
	bucket := os.Getenv("TIERED_TEST_BUCKET")
	if bucket == "" {
		bucket = os.Getenv("TURBOLITE_BUCKET")
	}
	if bucket == "" {
		t.Skip("no TIERED_TEST_BUCKET or TURBOLITE_BUCKET set")
	}

	dir := tmpDir(t)
	db, err := Open(filepath.Join(dir, "s3_cache.db"), &Options{
		Mode:     "s3",
		Bucket:   bucket,
		Endpoint: os.Getenv("AWS_ENDPOINT_URL"),
		Region:   "auto",
	})
	if err != nil {
		t.Fatalf("Open S3: %v", err)
	}
	defer db.Close()

	var cacheSize int
	db.QueryRow("PRAGMA cache_size").Scan(&cacheSize)
	if cacheSize != 0 {
		t.Fatalf("expected cache_size=0 in S3 mode, got %d", cacheSize)
	}
}

func TestS3Transactions(t *testing.T) {
	bucket := os.Getenv("TIERED_TEST_BUCKET")
	if bucket == "" {
		bucket = os.Getenv("TURBOLITE_BUCKET")
	}
	if bucket == "" {
		t.Skip("no TIERED_TEST_BUCKET or TURBOLITE_BUCKET set")
	}

	dir := tmpDir(t)
	db, err := Open(filepath.Join(dir, "s3_txn.db"), &Options{
		Mode:     "s3",
		Bucket:   bucket,
		Endpoint: os.Getenv("AWS_ENDPOINT_URL"),
		Region:   "auto",
	})
	if err != nil {
		t.Fatalf("Open S3: %v", err)
	}
	defer db.Close()

	db.Exec("CREATE TABLE counter (id INTEGER PRIMARY KEY, val INTEGER)")
	db.Exec("INSERT INTO counter VALUES (1, 0)")

	tx, _ := db.Begin()
	for i := 0; i < 50; i++ {
		tx.Exec("UPDATE counter SET val = val + 1 WHERE id = 1")
	}
	tx.Commit()

	var val int
	db.QueryRow("SELECT val FROM counter WHERE id = 1").Scan(&val)
	if val != 50 {
		t.Fatalf("expected 50, got %d", val)
	}
}

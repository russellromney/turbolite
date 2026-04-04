// End-to-end integration test for turbolite shared library from Go.
//
// Full pipeline: register VFS → open → create → insert → query → verify → close.
//
// Run:
//   make test-ffi-go
package main

/*
#cgo LDFLAGS: -L${SRCDIR}/../../target/release -lturbolite
#include <stdlib.h>

// turbolite FFI declarations
extern const char* turbolite_version();
extern const char* turbolite_last_error();
extern int turbolite_register_local(const char* name, const char* base_dir, int compression_level);
extern void* turbolite_open(const char* path, const char* vfs_name);
extern int turbolite_exec(void* db, const char* sql);
extern char* turbolite_query_json(void* db, const char* sql);
extern void turbolite_free_string(char* s);
extern void turbolite_close(void* db);
extern void turbolite_clear_caches();
extern int turbolite_invalidate_cache(const char* path);

// Tiered S3 VFS (may not be present if built without tiered feature).
// We use dlsym at runtime to check availability.
extern int turbolite_register_tiered(const char* name, const char* bucket,
    const char* prefix, const char* cache_dir,
    const char* endpoint_url, const char* region) __attribute__((weak));
*/
import "C"

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unsafe"
)

var passed, failed int

func test(name string, fn func() error) {
	if err := fn(); err != nil {
		fmt.Printf("  FAIL  %s: %s\n", name, err)
		failed++
	} else {
		fmt.Printf("  PASS  %s\n", name)
		passed++
	}
}

func cstr(s string) *C.char { return C.CString(s) }

func queryJSON(db unsafe.Pointer, sql string) ([]map[string]interface{}, error) {
	csql := cstr(sql)
	defer C.free(unsafe.Pointer(csql))

	ptr := C.turbolite_query_json(db, csql)
	if ptr == nil {
		errMsg := C.GoString(C.turbolite_last_error())
		return nil, fmt.Errorf("query failed: %s", errMsg)
	}
	defer C.turbolite_free_string(ptr)

	raw := C.GoString(ptr)
	var rows []map[string]interface{}
	if err := json.Unmarshal([]byte(raw), &rows); err != nil {
		return nil, fmt.Errorf("json decode: %w", err)
	}
	return rows, nil
}

func main() {
	// --- Version ---
	test("version returns valid string", func() error {
		v := C.GoString(C.turbolite_version())
		if len(v) == 0 {
			return fmt.Errorf("empty version")
		}
		if !strings.Contains(v, ".") {
			return fmt.Errorf("unexpected format: %s", v)
		}
		return nil
	})

	// --- Null args ---
	test("null name returns error", func() error {
		rc := C.turbolite_register_local(nil, cstr("/tmp"), 3)
		if rc != -1 {
			return fmt.Errorf("expected -1, got %d", rc)
		}
		err := C.GoString(C.turbolite_last_error())
		if !strings.Contains(err, "name") {
			return fmt.Errorf("error should mention 'name': %s", err)
		}
		return nil
	})

	test("null base_dir returns error", func() error {
		name := cstr("go-null-test")
		defer C.free(unsafe.Pointer(name))
		rc := C.turbolite_register_local(name, nil, 3)
		if rc != -1 {
			return fmt.Errorf("expected -1, got %d", rc)
		}
		return nil
	})

	// --- Registration ---
	test("register compressed VFS", func() error {
		dir, _ := os.MkdirTemp("", "turbolite-go-")
		defer os.RemoveAll(dir)
		name := cstr("go-compressed")
		defer C.free(unsafe.Pointer(name))
		base := cstr(dir)
		defer C.free(unsafe.Pointer(base))
		rc := C.turbolite_register_local(name, base, 3)
		if rc != 0 {
			return fmt.Errorf("rc=%d: %s", rc, C.GoString(C.turbolite_last_error()))
		}
		return nil
	})

	// --- Open/Close ---
	test("open and close database", func() error {
		dir, _ := os.MkdirTemp("", "turbolite-go-")
		defer os.RemoveAll(dir)
		name := cstr("go-openclose")
		defer C.free(unsafe.Pointer(name))
		base := cstr(dir)
		defer C.free(unsafe.Pointer(base))
		C.turbolite_register_local(name, base, 3)

		dbPath := cstr(filepath.Join(dir, "test.db"))
		defer C.free(unsafe.Pointer(dbPath))
		db := C.turbolite_open(dbPath, name)
		if db == nil {
			return fmt.Errorf("open returned NULL: %s", C.GoString(C.turbolite_last_error()))
		}
		C.turbolite_close(db)
		return nil
	})

	// --- Exec ---
	test("exec CREATE + INSERT", func() error {
		dir, _ := os.MkdirTemp("", "turbolite-go-")
		defer os.RemoveAll(dir)
		name := cstr("go-exec")
		defer C.free(unsafe.Pointer(name))
		base := cstr(dir)
		defer C.free(unsafe.Pointer(base))
		C.turbolite_register_local(name, base, 3)

		dbPath := cstr(filepath.Join(dir, "test.db"))
		defer C.free(unsafe.Pointer(dbPath))
		db := C.turbolite_open(dbPath, name)
		if db == nil {
			return fmt.Errorf("open failed")
		}
		defer C.turbolite_close(db)

		sql := cstr("CREATE TABLE t (id INTEGER, val TEXT); INSERT INTO t VALUES (1, 'hello');")
		defer C.free(unsafe.Pointer(sql))
		rc := C.turbolite_exec(db, sql)
		if rc != 0 {
			return fmt.Errorf("exec failed: %s", C.GoString(C.turbolite_last_error()))
		}
		return nil
	})

	test("bad SQL returns error", func() error {
		dir, _ := os.MkdirTemp("", "turbolite-go-")
		defer os.RemoveAll(dir)
		name := cstr("go-badsql")
		defer C.free(unsafe.Pointer(name))
		base := cstr(dir)
		defer C.free(unsafe.Pointer(base))
		C.turbolite_register_local(name, base, 3)

		dbPath := cstr(filepath.Join(dir, "test.db"))
		defer C.free(unsafe.Pointer(dbPath))
		db := C.turbolite_open(dbPath, name)
		defer C.turbolite_close(db)

		sql := cstr("NOT VALID SQL")
		defer C.free(unsafe.Pointer(sql))
		rc := C.turbolite_exec(db, sql)
		if rc != -1 {
			return fmt.Errorf("expected -1, got %d", rc)
		}
		return nil
	})

	// --- Full round-trip ---
	test("full round-trip: create, insert, query, verify", func() error {
		dir, _ := os.MkdirTemp("", "turbolite-go-")
		defer os.RemoveAll(dir)
		name := cstr("go-roundtrip")
		defer C.free(unsafe.Pointer(name))
		base := cstr(dir)
		defer C.free(unsafe.Pointer(base))
		C.turbolite_register_local(name, base, 3)

		dbPath := cstr(filepath.Join(dir, "test.db"))
		defer C.free(unsafe.Pointer(dbPath))
		db := C.turbolite_open(dbPath, name)
		if db == nil {
			return fmt.Errorf("open failed")
		}
		defer C.turbolite_close(db)

		sql := cstr(`CREATE TABLE users (id INTEGER, name TEXT, age INTEGER);
			INSERT INTO users VALUES (1, 'alice', 30);
			INSERT INTO users VALUES (2, 'bob', 25);
			INSERT INTO users VALUES (3, 'carol', 35);`)
		defer C.free(unsafe.Pointer(sql))
		C.turbolite_exec(db, sql)

		rows, err := queryJSON(db, "SELECT * FROM users ORDER BY id")
		if err != nil {
			return err
		}
		if len(rows) != 3 {
			return fmt.Errorf("expected 3 rows, got %d", len(rows))
		}
		if rows[0]["name"] != "alice" {
			return fmt.Errorf("row 0 name: %v", rows[0]["name"])
		}
		if rows[1]["name"] != "bob" {
			return fmt.Errorf("row 1 name: %v", rows[1]["name"])
		}
		// JSON numbers decode as float64
		if rows[2]["age"].(float64) != 35 {
			return fmt.Errorf("row 2 age: %v", rows[2]["age"])
		}
		return nil
	})

	// --- Persistence ---
	test("data persists across connections", func() error {
		dir, _ := os.MkdirTemp("", "turbolite-go-")
		defer os.RemoveAll(dir)
		name := cstr("go-persist")
		defer C.free(unsafe.Pointer(name))
		base := cstr(dir)
		defer C.free(unsafe.Pointer(base))
		C.turbolite_register_local(name, base, 3)

		dbPath := cstr(filepath.Join(dir, "persist.db"))
		defer C.free(unsafe.Pointer(dbPath))

		// Write
		db := C.turbolite_open(dbPath, name)
		if db == nil {
			return fmt.Errorf("open failed")
		}
		sql := cstr("CREATE TABLE kv (k TEXT, v TEXT); INSERT INTO kv VALUES ('hello', 'world');")
		defer C.free(unsafe.Pointer(sql))
		C.turbolite_exec(db, sql)
		C.turbolite_close(db)

		// Re-open and read
		db = C.turbolite_open(dbPath, name)
		if db == nil {
			return fmt.Errorf("reopen failed")
		}
		defer C.turbolite_close(db)

		rows, err := queryJSON(db, "SELECT * FROM kv")
		if err != nil {
			return err
		}
		if len(rows) != 1 {
			return fmt.Errorf("expected 1 row, got %d", len(rows))
		}
		if rows[0]["k"] != "hello" || rows[0]["v"] != "world" {
			return fmt.Errorf("unexpected row: %v", rows[0])
		}
		return nil
	})

	// --- NULL safety ---
	test("NULL handles don't crash", func() error {
		C.turbolite_close(nil)
		C.turbolite_free_string(nil)
		db := C.turbolite_open(nil, cstr("any"))
		if db != nil {
			return fmt.Errorf("expected NULL from open(NULL)")
		}
		return nil
	})

	test("clear_caches does not crash", func() error {
		C.turbolite_clear_caches()
		return nil
	})

	// --- Tiered S3 tests (only when TIERED_TEST_BUCKET is set) ---
	bucket := os.Getenv("TIERED_TEST_BUCKET")
	// Check if tiered symbol is available (weak symbol will be nil if not linked)
	hasTiered := C.turbolite_register_tiered != nil
	if hasTiered && bucket != "" {
		endpointURL := os.Getenv("AWS_ENDPOINT_URL")
		region := os.Getenv("AWS_REGION")
		if region == "" {
			region = "auto"
		}

		tieredPrefix := func(tag string) string {
			return fmt.Sprintf("test/ffi-go/%s/%d", tag, time.Now().UnixNano())
		}

		test("tiered: register S3 VFS", func() error {
			dir, _ := os.MkdirTemp("", "turbolite-go-tiered-")
			defer os.RemoveAll(dir)

			cName := cstr("go-tiered-reg")
			defer C.free(unsafe.Pointer(cName))
			cBucket := cstr(bucket)
			defer C.free(unsafe.Pointer(cBucket))
			cPrefix := cstr(tieredPrefix("reg"))
			defer C.free(unsafe.Pointer(cPrefix))
			cCache := cstr(dir)
			defer C.free(unsafe.Pointer(cCache))

			var cEndpoint *C.char
			if endpointURL != "" {
				cEndpoint = cstr(endpointURL)
				defer C.free(unsafe.Pointer(cEndpoint))
			}
			cRegion := cstr(region)
			defer C.free(unsafe.Pointer(cRegion))

			rc := C.turbolite_register_tiered(cName, cBucket, cPrefix, cCache, cEndpoint, cRegion)
			if rc != 0 {
				return fmt.Errorf("rc=%d: %s", rc, C.GoString(C.turbolite_last_error()))
			}
			return nil
		})

		test("tiered: full round-trip via S3", func() error {
			dir, _ := os.MkdirTemp("", "turbolite-go-tiered-")
			defer os.RemoveAll(dir)

			cName := cstr("go-tiered-rt")
			defer C.free(unsafe.Pointer(cName))
			cBucket := cstr(bucket)
			defer C.free(unsafe.Pointer(cBucket))
			cPrefix := cstr(tieredPrefix("roundtrip"))
			defer C.free(unsafe.Pointer(cPrefix))
			cCache := cstr(dir)
			defer C.free(unsafe.Pointer(cCache))

			var cEndpoint *C.char
			if endpointURL != "" {
				cEndpoint = cstr(endpointURL)
				defer C.free(unsafe.Pointer(cEndpoint))
			}
			cRegion := cstr(region)
			defer C.free(unsafe.Pointer(cRegion))

			rc := C.turbolite_register_tiered(cName, cBucket, cPrefix, cCache, cEndpoint, cRegion)
			if rc != 0 {
				return fmt.Errorf("register: rc=%d: %s", rc, C.GoString(C.turbolite_last_error()))
			}

			dbPath := cstr(filepath.Join(dir, "test.db"))
			defer C.free(unsafe.Pointer(dbPath))
			db := C.turbolite_open(dbPath, cName)
			if db == nil {
				return fmt.Errorf("open failed: %s", C.GoString(C.turbolite_last_error()))
			}
			defer C.turbolite_close(db)

			sql := cstr(`CREATE TABLE users (id INTEGER, name TEXT, age INTEGER);
				INSERT INTO users VALUES (1, 'alice', 30);
				INSERT INTO users VALUES (2, 'bob', 25);
				INSERT INTO users VALUES (3, 'carol', 35);`)
			defer C.free(unsafe.Pointer(sql))
			C.turbolite_exec(db, sql)

			rows, err := queryJSON(db, "SELECT * FROM users ORDER BY id")
			if err != nil {
				return err
			}
			if len(rows) != 3 {
				return fmt.Errorf("expected 3 rows, got %d", len(rows))
			}
			if rows[0]["name"] != "alice" {
				return fmt.Errorf("row 0 name: %v", rows[0]["name"])
			}
			if rows[2]["age"].(float64) != 35 {
				return fmt.Errorf("row 2 age: %v", rows[2]["age"])
			}
			return nil
		})

		test("tiered: null bucket returns error", func() error {
			dir, _ := os.MkdirTemp("", "turbolite-go-tiered-")
			defer os.RemoveAll(dir)

			cName := cstr("go-tiered-null")
			defer C.free(unsafe.Pointer(cName))
			cCache := cstr(dir)
			defer C.free(unsafe.Pointer(cCache))
			cPrefix := cstr("prefix")
			defer C.free(unsafe.Pointer(cPrefix))

			rc := C.turbolite_register_tiered(cName, nil, cPrefix, cCache, nil, nil)
			if rc != -1 {
				return fmt.Errorf("expected -1, got %d", rc)
			}
			errMsg := C.GoString(C.turbolite_last_error())
			if !strings.Contains(errMsg, "bucket") {
				return fmt.Errorf("error should mention bucket: %s", errMsg)
			}
			return nil
		})
	} else if hasTiered {
		fmt.Println("  SKIP  tiered tests (set TIERED_TEST_BUCKET to enable)")
	} else {
		fmt.Println("  SKIP  tiered tests (library built without tiered feature)")
	}

	// --- Summary ---
	fmt.Printf("\nResults: %d passed, %d failed\n", passed, failed)
	if failed > 0 {
		os.Exit(1)
	}
}

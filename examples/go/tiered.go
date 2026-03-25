// turbolite example — Go S3 tiered (net/http + cgo)
//
// A small HTTP API server backed by turbolite with S3 tiered storage.
// Pages are compressed locally and synced to S3 for durability.
//
// Run:
//   make lib-bundled
//   TURBOLITE_BUCKET=my-bucket make example-go-tiered
//
// Then:
//   curl -X POST localhost:8080/books -d '{"title":"Dune","year":1965}'
//   curl localhost:8080/books
//
// Required:
//   TURBOLITE_BUCKET          S3 bucket name
//
// Optional:
//   TURBOLITE_ENDPOINT_URL    S3 endpoint (Tigris, MinIO, etc.)
//   TURBOLITE_REGION          AWS region

//go:build ignore

package main

/*
#cgo LDFLAGS: -L${SRCDIR}/../../target/release -lturbolite
#include <stdlib.h>

extern const char* turbolite_version();
extern const char* turbolite_last_error();
extern int turbolite_register_tiered(const char* name, const char* bucket,
    const char* prefix, const char* cache_dir,
    const char* endpoint_url, const char* region);
extern void* turbolite_open(const char* path, const char* vfs_name);
extern int turbolite_exec(void* db, const char* sql);
extern char* turbolite_query_json(void* db, const char* sql);
extern void turbolite_free_string(char* s);
extern void turbolite_close(void* db);
*/
import "C"

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"unsafe"
)

var db unsafe.Pointer

func cstr(s string) *C.char { return C.CString(s) }

func nullable(s string) *C.char {
	if s == "" {
		return nil
	}
	return C.CString(s)
}

func query(sql string) ([]byte, error) {
	csql := cstr(sql)
	defer C.free(unsafe.Pointer(csql))
	ptr := C.turbolite_query_json(db, csql)
	if ptr == nil {
		return nil, fmt.Errorf("%s", C.GoString(C.turbolite_last_error()))
	}
	defer C.turbolite_free_string(ptr)
	return []byte(C.GoString(ptr)), nil
}

func exec(sql string) error {
	csql := cstr(sql)
	defer C.free(unsafe.Pointer(csql))
	if C.turbolite_exec(db, csql) != 0 {
		return fmt.Errorf("%s", C.GoString(C.turbolite_last_error()))
	}
	return nil
}

func main() {
	bucket := os.Getenv("TURBOLITE_BUCKET")
	if bucket == "" {
		fmt.Fprintln(os.Stderr, "FATAL: TURBOLITE_BUCKET is required")
		os.Exit(1)
	}

	cacheDir, _ := os.MkdirTemp("", "turbolite-tiered-")
	defer os.RemoveAll(cacheDir)

	name := cstr("tiered")
	defer C.free(unsafe.Pointer(name))
	cbucket := cstr(bucket)
	defer C.free(unsafe.Pointer(cbucket))
	prefix := cstr("books")
	defer C.free(unsafe.Pointer(prefix))
	ccache := cstr(cacheDir)
	defer C.free(unsafe.Pointer(ccache))
	endpoint := nullable(os.Getenv("TURBOLITE_ENDPOINT_URL"))
	if endpoint != nil {
		defer C.free(unsafe.Pointer(endpoint))
	}
	region := nullable(os.Getenv("TURBOLITE_REGION"))
	if region != nil {
		defer C.free(unsafe.Pointer(region))
	}

	if C.turbolite_register_tiered(name, cbucket, prefix, ccache, endpoint, region) != 0 {
		fmt.Fprintf(os.Stderr, "FATAL: register tiered VFS: %s\n", C.GoString(C.turbolite_last_error()))
		os.Exit(1)
	}

	dbPath := cstr(filepath.Join(cacheDir, "books.db"))
	defer C.free(unsafe.Pointer(dbPath))
	db = C.turbolite_open(dbPath, name)
	defer C.turbolite_close(db)

	exec("CREATE TABLE IF NOT EXISTS books (id INTEGER PRIMARY KEY AUTOINCREMENT, title TEXT, year INTEGER)")

	http.HandleFunc("GET /books", func(w http.ResponseWriter, r *http.Request) {
		data, err := query("SELECT * FROM books ORDER BY year")
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write(data)
	})

	http.HandleFunc("POST /books", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var book struct {
			Title string `json:"title"`
			Year  int    `json:"year"`
		}
		json.Unmarshal(body, &book)

		sql := fmt.Sprintf("INSERT INTO books (title, year) VALUES ('%s', %d)", book.Title, book.Year)
		if err := exec(sql); err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		w.WriteHeader(201)
		data, _ := query("SELECT * FROM books ORDER BY id DESC LIMIT 1")
		w.Header().Set("Content-Type", "application/json")
		w.Write(data)
	})

	fmt.Printf("turbolite %s (S3 tiered) -- listening on :8080\n", C.GoString(C.turbolite_version()))
	http.ListenAndServe(":8080", nil)
}

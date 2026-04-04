//go:build ignore

// turbolite example — Go local compressed (net/http + cgo)
//
// A small HTTP API server backed by turbolite-compressed SQLite (local mode).
// See tiered.go for S3 tiered storage.
//
// Run:
//   make lib-bundled
//   make example-go
//
// Then:
//   curl -X POST localhost:8080/books -d '{"title":"Dune","year":1965}'
//   curl localhost:8080/books
package main

/*
#cgo LDFLAGS: -L${SRCDIR}/../../target/release -lturbolite
#include <stdlib.h>

extern const char* turbolite_version();
extern const char* turbolite_last_error();
extern int turbolite_register_local(const char* name, const char* base_dir, int level);
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
	dataDir, _ := os.MkdirTemp("", "turbolite-example-")
	defer os.RemoveAll(dataDir)

	name := cstr("demo")
	defer C.free(unsafe.Pointer(name))
	base := cstr(dataDir)
	defer C.free(unsafe.Pointer(base))
	C.turbolite_register_local(name, base, 3)

	dbPath := cstr(filepath.Join(dataDir, "books.db"))
	defer C.free(unsafe.Pointer(dbPath))
	db = C.turbolite_open(dbPath, name)
	defer C.turbolite_close(db)

	exec("CREATE TABLE books (id INTEGER PRIMARY KEY AUTOINCREMENT, title TEXT, year INTEGER)")

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

	fmt.Printf("turbolite %s — listening on :8080\n", C.GoString(C.turbolite_version()))
	http.ListenAndServe(":8080", nil)
}

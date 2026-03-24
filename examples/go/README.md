# turbolite — Go example

An HTTP API server backed by turbolite-compressed SQLite. Uses `net/http` (stdlib) + cgo.

## Run

```bash
make lib-bundled
make example-go
```

## Try it

```bash
curl -X POST localhost:8080/books -d '{"title":"Dune","year":1965}'
curl localhost:8080/books
```

## How it works

1. Declare FFI functions in the cgo preamble (`extern` declarations)
2. `turbolite_register_compressed` — register a zstd-compressed VFS
3. `turbolite_open` — open a database through the VFS
4. HTTP handlers call `turbolite_exec` (writes) and `turbolite_query_json` (reads)

Strings passed to C must be allocated with `C.CString` and freed with `C.free`.

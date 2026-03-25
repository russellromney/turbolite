# turbolite — Go examples

HTTP API servers backed by turbolite-compressed SQLite. Uses `net/http` (stdlib) + cgo.

## Local compressed (`local.go`)

```bash
make lib-bundled
make example-go
```

## S3 tiered (`tiered.go`)

```bash
make lib-bundled
TURBOLITE_BUCKET=my-bucket make example-go-tiered
```

## Try it

```bash
curl -X POST localhost:8080/books -d '{"title":"Dune","year":1965}'
curl localhost:8080/books
```

## How it works

1. Declare FFI functions in the cgo preamble (`extern` declarations)
2. `turbolite_register_compressed` (local) or `turbolite_register_tiered` (S3)
3. `turbolite_open` - open a database through the VFS
4. HTTP handlers call `turbolite_exec` (writes) and `turbolite_query_json` (reads)

Strings passed to C must be allocated with `C.CString` and freed with `C.free`.

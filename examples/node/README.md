# turbolite — Node.js example

An HTTP API server backed by turbolite-compressed SQLite. Uses stdlib `http` + [koffi](https://koffi.dev) for FFI.

## Run

```bash
make lib-bundled
make example-node
```

## Try it

```bash
curl -X POST localhost:3000/books -d '{"title":"Dune","year":1965}'
curl localhost:3000/books
```

## How it works

1. `koffi.load(libPath)` — load the turbolite `.dylib` / `.so`
2. `turbolite_register_compressed` — register a zstd-compressed VFS
3. `turbolite_open` — open a database through the VFS
4. HTTP handlers call `turbolite_exec` (writes) and `turbolite_query_json` (reads)

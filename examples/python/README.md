# turbolite — Python examples

FastAPI servers backed by turbolite-compressed SQLite.

## Local compressed (`local.py`)

```bash
pip install turbolite fastapi[standard]
python examples/python/local.py
```

## S3 tiered (`tiered.py`)

```bash
pip install turbolite fastapi[standard]
TURBOLITE_BUCKET=my-bucket python examples/python/tiered.py
```

Or via Make:

```bash
make example-python          # local
make example-python-tiered   # S3 tiered
```

## Try it

```bash
curl -X POST localhost:8000/books \
     -H 'Content-Type: application/json' \
     -d '{"title": "Dune", "year": 1965}'

curl localhost:8000/books
```

## How it works

1. `turbolite.connect(path)` - open a compressed database (local mode)
2. `turbolite.connect(path, mode="s3", bucket="...")` - open an S3 tiered database
3. Standard `sqlite3` API from there - `execute`, `fetchall`, `commit`

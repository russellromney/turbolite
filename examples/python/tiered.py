#!/usr/bin/env python3
# /// script
# dependencies = ["fastapi[standard]", "turbolite"]
# ///
"""
turbolite example — Python S3 tiered (FastAPI)

A tiny API server backed by turbolite with S3 tiered storage.
Pages are compressed locally and synced to S3 for durability.

Run:
    pip install turbolite fastapi[standard]
    python examples/python/tiered.py

Or via Make:
    make example-python-tiered

Then:
    curl -X POST localhost:8000/books -H 'Content-Type: application/json' \
         -d '{"title": "Dune", "year": 1965}'
    curl localhost:8000/books

Required environment variables:
    TURBOLITE_BUCKET or pass bucket= to connect()

Optional:
    TURBOLITE_ENDPOINT_URL   S3 endpoint (Tigris, MinIO, etc.)
    TURBOLITE_REGION         AWS region
    TURBOLITE_PREFIX         S3 key prefix (default "turbolite")
    TURBOLITE_CACHE_DIR      local cache directory
    AWS_ACCESS_KEY_ID        S3 credentials
    AWS_SECRET_ACCESS_KEY    S3 credentials
"""

import os
import tempfile

import turbolite
from fastapi import FastAPI
from pydantic import BaseModel

# ── Set up database ─────────────────────────────────────────────────

data_dir = tempfile.mkdtemp(prefix="turbolite-tiered-")
db_path = os.path.join(data_dir, "books.db")

# S3 tiered mode: pages compressed locally, synced to S3.
# Fails fast if bucket is not configured.
conn = turbolite.connect(
    db_path,
    mode="s3",
    bucket=os.environ.get("TURBOLITE_BUCKET", "my-bucket"),
    endpoint=os.environ.get("TURBOLITE_ENDPOINT_URL"),
)

conn.execute("""
    CREATE TABLE IF NOT EXISTS books (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT NOT NULL,
        year INTEGER
    )
""")
conn.commit()

# ── FastAPI app ──────────────────────────────────────────────────────

app = FastAPI()


class Book(BaseModel):
    title: str
    year: int


@app.get("/books")
def list_books():
    rows = conn.execute("SELECT id, title, year FROM books ORDER BY year").fetchall()
    return [{"id": r[0], "title": r[1], "year": r[2]} for r in rows]


@app.post("/books", status_code=201)
def add_book(book: Book):
    conn.execute(
        "INSERT INTO books (title, year) VALUES (?, ?)",
        (book.title, book.year),
    )
    conn.commit()
    row = conn.execute("SELECT id, title, year FROM books ORDER BY id DESC LIMIT 1").fetchone()
    return {"id": row[0], "title": row[1], "year": row[2]}


if __name__ == "__main__":
    import uvicorn
    print(f"turbolite {turbolite.__version__} (S3 tiered) -- data dir: {data_dir}")
    uvicorn.run(app, host="0.0.0.0", port=8000)

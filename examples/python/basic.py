#!/usr/bin/env python3
# /// script
# dependencies = ["fastapi[standard]"]
# ///
"""
turbolite example — Python (FastAPI + ctypes)

A tiny API server backed by turbolite-compressed SQLite.

Run:
    make lib-bundled
    uv run examples/python/basic.py

Then:
    curl -X POST localhost:8000/books -H 'Content-Type: application/json' \
         -d '{"title": "Dune", "year": 1965}'
    curl localhost:8000/books
"""

import ctypes
import json
import os
import platform
import sys
import tempfile

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

# ── Load turbolite ───────────────────────────────────────────────────

ext = "dylib" if platform.system() == "Darwin" else "so"
lib_path = os.path.join(
    os.path.dirname(__file__), "..", "..", "target", "release",
    f"libsqlite_compress_encrypt_vfs.{ext}",
)
if not os.path.exists(lib_path):
    sys.exit(f"ERROR: library not found. Run `make lib-bundled` first.")

lib = ctypes.CDLL(os.path.abspath(lib_path))
lib.turbolite_last_error.restype = ctypes.c_char_p
lib.turbolite_register_compressed.restype = ctypes.c_int
lib.turbolite_register_compressed.argtypes = [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_int]
lib.turbolite_open.restype = ctypes.c_void_p
lib.turbolite_open.argtypes = [ctypes.c_char_p, ctypes.c_char_p]
lib.turbolite_exec.restype = ctypes.c_int
lib.turbolite_exec.argtypes = [ctypes.c_void_p, ctypes.c_char_p]
lib.turbolite_query_json.restype = ctypes.c_void_p
lib.turbolite_query_json.argtypes = [ctypes.c_void_p, ctypes.c_char_p]
lib.turbolite_free_string.restype = None
lib.turbolite_free_string.argtypes = [ctypes.c_void_p]
lib.turbolite_close.restype = None
lib.turbolite_close.argtypes = [ctypes.c_void_p]


def query(db, sql):
    ptr = lib.turbolite_query_json(db, sql.encode())
    if not ptr:
        raise RuntimeError(lib.turbolite_last_error().decode())
    try:
        return json.loads(ctypes.cast(ptr, ctypes.c_char_p).value.decode())
    finally:
        lib.turbolite_free_string(ptr)


# ── Set up database ─────────────────────────────────────────────────

data_dir = tempfile.mkdtemp(prefix="turbolite-example-")
lib.turbolite_register_compressed(b"demo", data_dir.encode(), 3)
db = lib.turbolite_open(os.path.join(data_dir, "books.db").encode(), b"demo")
lib.turbolite_exec(db, b"""
    CREATE TABLE IF NOT EXISTS books (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT NOT NULL,
        year INTEGER
    )
""")

# ── FastAPI app ──────────────────────────────────────────────────────

app = FastAPI()


class Book(BaseModel):
    title: str
    year: int


@app.get("/books")
def list_books():
    return query(db, "SELECT * FROM books ORDER BY year")


@app.post("/books", status_code=201)
def add_book(book: Book):
    sql = f"INSERT INTO books (title, year) VALUES ('{book.title}', {book.year})"
    if lib.turbolite_exec(db, sql.encode()) != 0:
        raise HTTPException(500, lib.turbolite_last_error().decode())
    return query(db, "SELECT * FROM books ORDER BY id DESC LIMIT 1")[0]


if __name__ == "__main__":
    import uvicorn
    print(f"Data dir: {data_dir}")
    uvicorn.run(app, host="0.0.0.0", port=8000)

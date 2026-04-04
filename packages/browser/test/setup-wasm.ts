/**
 * Vitest setup: patch globalThis.fetch to serve wa-sqlite WASM from disk.
 * Emscripten's module loader uses fetch() to load .wasm files, which
 * doesn't work in Node without this shim.
 */
import * as fs from "fs";
import * as path from "path";

const wasmPath = path.join(
  __dirname,
  "../node_modules/wa-sqlite/dist/wa-sqlite-async.wasm",
);

const originalFetch = globalThis.fetch;

globalThis.fetch = async (input: RequestInfo | URL, init?: RequestInit) => {
  const url = typeof input === "string" ? input : input.toString();
  if (url.endsWith(".wasm")) {
    const wasmBuf = fs.readFileSync(wasmPath);
    const ab = wasmBuf.buffer.slice(
      wasmBuf.byteOffset,
      wasmBuf.byteOffset + wasmBuf.byteLength,
    );
    return new Response(ab, {
      status: 200,
      headers: { "Content-Type": "application/wasm" },
    });
  }
  if (originalFetch) {
    return originalFetch(input, init);
  }
  throw new Error(`Unhandled fetch: ${url}`);
};

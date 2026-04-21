// @ts-nocheck - wa-sqlite type declarations don't match runtime behavior
import * as VFS from "wa-sqlite/src/VFS.js";
import type { Manifest, PageLocation } from "./types.js";
import type { PageCache } from "./cache.js";
import type { PageFetcher } from "./fetcher.js";

/** Shared mutable state so TurboliteDB.refresh() can swap the manifest without re-registering the VFS. */
export interface VfsState {
  manifest: Manifest;
  pageIndex: Map<number, PageLocation>;
}

interface FileState {
  fileId: number;
  filename: string;
  size: number;
  flags: number;
}

/**
 * Read-only wa-sqlite VFS backed by turbolite page groups.
 *
 * Extends VFS.Base and uses handleAsync() for async page fetches.
 * xRead calls are served from the page cache, which fetches from S3 on miss.
 * All writes return SQLITE_READONLY.
 */
export class TurboliteVFS extends VFS.Base {
  name: string;
  private files = new Map<number, FileState>();
  state: VfsState;
  private cache: PageCache;
  private fetcher: PageFetcher;

  constructor(
    name: string,
    state: VfsState,
    cache: PageCache,
    fetcher: PageFetcher,
  ) {
    super();
    this.name = name;
    this.state = state;
    this.cache = cache;
    this.fetcher = fetcher;
  }

  xOpen(name, fileId, flags, pOutFlags) {
    const filename = name ?? `tmp-${fileId}`;
    const isMainDb = !!(flags & VFS.SQLITE_OPEN_MAIN_DB);
    const size = isMainDb ? this.state.manifest.page_count * this.state.manifest.page_size : 0;

    this.files.set(fileId, { fileId, filename, size, flags });
    pOutFlags.setInt32(0, VFS.SQLITE_OPEN_READONLY, true);
    return VFS.SQLITE_OK;
  }

  xClose(fileId) {
    this.files.delete(fileId);
    return VFS.SQLITE_OK;
  }

  xRead(fileId, pData, iOffset) {
    const file = this.files.get(fileId);
    if (!file) return VFS.SQLITE_IOERR;

    // Non-main-db files (WAL, journal) are empty
    if (file.size === 0) {
      pData.fill(0);
      return VFS.SQLITE_IOERR_SHORT_READ;
    }

    return this.handleAsync(async () => {
      const pageSize = this.state.manifest.page_size;
      const iAmt = pData.byteLength;

      // Fast path: aligned full-page read
      if (iOffset % pageSize === 0 && iAmt === pageSize) {
        const pageNum = iOffset / pageSize;
        try {
          const pageData = await this.cache.getPageAsync(
            pageNum,
            this.state.manifest,
            this.state.pageIndex,
            this.fetcher,
          );
          pData.set(pageData);
          return VFS.SQLITE_OK;
        } catch {
          return VFS.SQLITE_IOERR;
        }
      }

      // Slow path: partial or cross-page read
      let bytesWritten = 0;
      let offset = iOffset;
      const iAmt2 = pData.byteLength;

      while (bytesWritten < iAmt2) {
        const pageNum = Math.floor(offset / pageSize);
        const pageOffset = offset % pageSize;
        const bytesToRead = Math.min(pageSize - pageOffset, iAmt2 - bytesWritten);

        if (pageNum >= this.state.manifest.page_count) {
          pData.fill(0, bytesWritten);
          return VFS.SQLITE_IOERR_SHORT_READ;
        }

        try {
          const pageData = await this.cache.getPageAsync(
            pageNum,
            this.state.manifest,
            this.state.pageIndex,
            this.fetcher,
          );
          pData.set(
            pageData.subarray(pageOffset, pageOffset + bytesToRead),
            bytesWritten,
          );
        } catch {
          return VFS.SQLITE_IOERR;
        }

        bytesWritten += bytesToRead;
        offset += bytesToRead;
      }

      return VFS.SQLITE_OK;
    });
  }

  xWrite() {
    return VFS.SQLITE_READONLY;
  }

  xTruncate() {
    return VFS.SQLITE_READONLY;
  }

  xSync() {
    return VFS.SQLITE_OK;
  }

  xFileSize(fileId, pSize64) {
    const file = this.files.get(fileId);
    if (!file) return VFS.SQLITE_IOERR;
    pSize64.setBigInt64(0, BigInt(file.size), true);
    return VFS.SQLITE_OK;
  }

  xDelete() {
    return VFS.SQLITE_READONLY;
  }

  xAccess(_name, _flags, pResOut) {
    pResOut.setInt32(0, 0, true);
    return VFS.SQLITE_OK;
  }

  xDeviceCharacteristics() {
    return VFS.SQLITE_IOCAP_IMMUTABLE;
  }
}

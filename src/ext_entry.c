/*
 * turbolite — SQLite loadable extension entry point.
 *
 * This C shim provides:
 * 1. The sqlite3_turbolite_init entry point that SQLite calls on load_extension()
 * 2. Symbol shims for sqlite3_vfs_register, sqlite3_vfs_find, sqlite3_snprintf,
 *    and sqlite3_uri_boolean that route through the extension API table.
 *    The Rust sqlite-vfs crate calls these symbols directly via extern "C",
 *    but loadable extensions must use the API table — not direct symbols.
 *
 * Build: compiled by the cc crate when the "loadable-extension" feature is enabled.
 */

#include <sqlite3ext.h>
SQLITE_EXTENSION_INIT1

#include <stdarg.h>
#include <stdio.h>

#ifndef TURBOLITE_VERSION
#define TURBOLITE_VERSION "0.1.0"
#endif

/* Rust function — registers the compressed VFS. Defined in src/ext.rs. */
extern int turbolite_ext_register_vfs(void);

/* ── SQL functions ──────────────────────────────────────────────── */

static void turbolite_version_func(
    sqlite3_context *ctx,
    int argc,
    sqlite3_value **argv
) {
    (void)argc;
    (void)argv;
    sqlite3_result_text(ctx, TURBOLITE_VERSION, -1, SQLITE_TRANSIENT);
}

/* ── Entry point ────────────────────────────────────────────────── */

#ifdef _WIN32
__declspec(dllexport)
#endif
int sqlite3_turbolite_init(
    sqlite3 *db,
    char **pzErrMsg,
    const sqlite3_api_routines *pApi
) {
    int rc;
    SQLITE_EXTENSION_INIT2(pApi);

    /* Register turbolite_version() SQL function */
    rc = sqlite3_create_function_v2(
        db, "turbolite_version", 0,
        SQLITE_UTF8 | SQLITE_DETERMINISTIC,
        0, turbolite_version_func, 0, 0, 0
    );
    if (rc != SQLITE_OK) {
        *pzErrMsg = sqlite3_mprintf("turbolite: failed to register turbolite_version()");
        return rc;
    }

    /* Call Rust to register the compressed VFS */
    rc = turbolite_ext_register_vfs();
    if (rc != 0) {
        *pzErrMsg = sqlite3_mprintf("turbolite: failed to register VFS");
        return SQLITE_ERROR;
    }

    return SQLITE_OK;
}

/* ── Symbol shims ───────────────────────────────────────────────
 *
 * The sqlite-vfs Rust crate declares these as extern "C" and calls them
 * directly. In a loadable extension, these symbols don't exist in the
 * linking environment — they must be routed through the API table.
 *
 * We #undef the sqlite3ext.h macros so we can define real C functions
 * with the original names. The linker resolves the Rust extern "C"
 * declarations to these functions within the same shared library.
 * ─────────────────────────────────────────────────────────────── */

#undef sqlite3_vfs_register
#undef sqlite3_vfs_find
#undef sqlite3_snprintf
#undef sqlite3_uri_boolean

int sqlite3_vfs_register(sqlite3_vfs *pVfs, int makeDflt) {
    return sqlite3_api->vfs_register(pVfs, makeDflt);
}

sqlite3_vfs *sqlite3_vfs_find(const char *zVfsName) {
    return sqlite3_api->vfs_find(zVfsName);
}

/*
 * sqlite3_snprintf is variadic. The API table's function pointer is also
 * variadic, so we can't forward va_args through it. Instead we provide
 * a simple implementation using vsnprintf. The sqlite-vfs crate only
 * calls this with "%s" format (bounded string copy), so this suffices.
 */
char *sqlite3_snprintf(int n, char *zBuf, const char *zFormat, ...) {
    if (n <= 0) return zBuf;
    va_list ap;
    va_start(ap, zFormat);
    vsnprintf(zBuf, (size_t)n, zFormat, ap);
    va_end(ap);
    zBuf[n - 1] = '\0'; /* sqlite3_snprintf always null-terminates */
    return zBuf;
}

int sqlite3_uri_boolean(const char *zFilename, const char *zParam, int bDflt) {
    return sqlite3_api->uri_boolean(zFilename, zParam, bDflt);
}

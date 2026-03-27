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
#include <string.h>

#ifndef TURBOLITE_VERSION
#define TURBOLITE_VERSION "0.1.0"
#endif

/* Rust function -- registers the compressed VFS. Defined in src/ext.rs. */
extern int turbolite_ext_register_vfs(void);

/* Rust function -- runs EQP on SQL and pushes plan to global queue.
 * Defined in src/tiered/query_plan.rs (Phase Marne). */
extern void turbolite_trace_push_plan(sqlite3 *db, const char *sql);

/* Rust bench functions -- cache control and S3 counters.
 * Defined in src/ext.rs. Available when built with tiered feature. */
extern int turbolite_bench_clear_cache(int mode);
extern int turbolite_bench_reset_s3(void);
extern long long turbolite_bench_s3_gets(void);
extern long long turbolite_bench_s3_bytes(void);

/* Rust function -- runtime config (prefetch tuning).
 * Defined in src/tiered/settings.rs. */
extern int turbolite_config_set(const char *key, const char *value);

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

/*
 * turbolite_clear_cache(mode TEXT)
 * mode: 'all', 'data', 'interior'
 * Returns 0 on success.
 */
static void turbolite_clear_cache_func(
    sqlite3_context *ctx,
    int argc,
    sqlite3_value **argv
) {
    (void)argc;
    const char *mode = (const char *)sqlite3_value_text(argv[0]);
    if (!mode) {
        sqlite3_result_error(ctx, "turbolite_clear_cache: mode required", -1);
        return;
    }
    int m = -1;
    if (strcmp(mode, "all") == 0) m = 0;
    else if (strcmp(mode, "data") == 0) m = 1;
    else if (strcmp(mode, "interior") == 0) m = 2;
    else {
        sqlite3_result_error(ctx, "turbolite_clear_cache: mode must be 'all', 'data', or 'interior'", -1);
        return;
    }
    int rc = turbolite_bench_clear_cache(m);
    sqlite3_result_int(ctx, rc);
}

/* turbolite_reset_s3_counters() -- reset S3 GET/byte counters. */
static void turbolite_reset_s3_func(
    sqlite3_context *ctx,
    int argc,
    sqlite3_value **argv
) {
    (void)argc;
    (void)argv;
    int rc = turbolite_bench_reset_s3();
    sqlite3_result_int(ctx, rc);
}

/* turbolite_s3_gets() -- return S3 GET count since last reset. */
static void turbolite_s3_gets_func(
    sqlite3_context *ctx,
    int argc,
    sqlite3_value **argv
) {
    (void)argc;
    (void)argv;
    sqlite3_result_int64(ctx, turbolite_bench_s3_gets());
}

/* turbolite_s3_bytes() -- return S3 bytes fetched since last reset. */
static void turbolite_s3_bytes_func(
    sqlite3_context *ctx,
    int argc,
    sqlite3_value **argv
) {
    (void)argc;
    (void)argv;
    sqlite3_result_int64(ctx, turbolite_bench_s3_bytes());
}

/*
 * turbolite_config_set(key TEXT, value TEXT)
 * Runtime prefetch tuning. Keys: 'prefetch_search', 'prefetch_lookup',
 * 'prefetch' (both), 'prefetch_reset', 'plan_aware'.
 * Returns 0 on success, error on invalid key/value.
 */
static void turbolite_config_set_func(
    sqlite3_context *ctx,
    int argc,
    sqlite3_value **argv
) {
    (void)argc;
    const char *key = (const char *)sqlite3_value_text(argv[0]);
    const char *value = (const char *)sqlite3_value_text(argv[1]);
    if (!key || !value) {
        sqlite3_result_error(ctx, "turbolite_config_set: key and value required", -1);
        return;
    }
    int rc = turbolite_config_set(key, value);
    if (rc != 0) {
        sqlite3_result_error(ctx, "turbolite_config_set: invalid key or value", -1);
        return;
    }
    sqlite3_result_int(ctx, 0);
}

/* ── Phase Marne: trace callback for query-plan-aware prefetch ──── */

/*
 * Trace callback installed via sqlite3_trace_v2(SQLITE_TRACE_STMT).
 * Fires at the start of sqlite3_step(), before the VDBE executes.
 * Calls Rust to run EQP on the SQL string and push planned accesses
 * to the global queue. The VFS drains the queue on first cache miss.
 *
 * IMPORTANT: sqlite3_trace_v2 supports exactly ONE callback per
 * connection. Installing this callback overwrites any previously
 * registered trace callback. Conversely, if another extension or
 * user code calls sqlite3_trace_v2 after turbolite loads, our
 * callback is silently replaced and plan-aware prefetch stops
 * working (the VFS falls back to hop-schedule prefetch with no
 * error). If you need both turbolite's plan-aware prefetch and
 * your own trace callback, a future turbolite_chain_trace_v2()
 * API will support chaining.
 *
 * Overhead: EQP adds ~10us per sqlite3_step() call. This is
 * negligible for cold/warm queries where prefetch saves 100ms+,
 * but is non-zero overhead for fully-cached hot queries.
 *
 * Reentrant guard: running EQP inside the callback triggers another
 * sqlite3_step() which fires the trace again. The Rust side filters
 * out EXPLAIN statements, but the C guard prevents the FFI roundtrip
 * entirely for the inner call.
 */
static __thread int turbolite_trace_reentrant = 0;

static int turbolite_trace_callback(
    unsigned trace_type,
    void *ctx,
    void *p,       /* sqlite3_stmt* */
    void *x        /* const char* expanded SQL */
) {
    (void)ctx;
    if (trace_type != SQLITE_TRACE_STMT) return 0;
    if (turbolite_trace_reentrant) return 0;

    const char *sql = (const char *)x;
    if (!sql || sql[0] == '\0') return 0;

    /* Get the db handle from the statement */
    sqlite3_stmt *stmt = (sqlite3_stmt *)p;
    sqlite3 *db = sqlite3_db_handle(stmt);

    turbolite_trace_reentrant = 1;
    turbolite_trace_push_plan(db, sql);
    turbolite_trace_reentrant = 0;
    return 0;
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

    /* Phase Marne: install trace callback for query-plan-aware prefetch.
     * SQLITE_TRACE_STMT fires at start of sqlite3_step() with the SQL string.
     * The callback runs EQP and pushes planned B-tree accesses to the global
     * queue, which the VFS drains on first cache miss.
     *
     * WARNING: this overwrites any existing trace callback on this connection.
     * See the comment on turbolite_trace_callback above for details. */
    rc = sqlite3_trace_v2(db, SQLITE_TRACE_STMT, turbolite_trace_callback, 0);
    if (rc != SQLITE_OK) {
        /* Non-fatal: VFS falls back to hop schedule without query plan */
    }

    /* Call Rust to register the compressed VFS */
    rc = turbolite_ext_register_vfs();
    if (rc != 0) {
        *pzErrMsg = sqlite3_mprintf("turbolite: failed to register VFS");
        return SQLITE_ERROR;
    }

    /* Bench SQL functions: cache control and S3 counters.
     * These are no-ops (return 1) if no tiered VFS was registered. */
    sqlite3_create_function_v2(
        db, "turbolite_clear_cache", 1,
        SQLITE_UTF8, 0, turbolite_clear_cache_func, 0, 0, 0
    );
    sqlite3_create_function_v2(
        db, "turbolite_reset_s3_counters", 0,
        SQLITE_UTF8, 0, turbolite_reset_s3_func, 0, 0, 0
    );
    sqlite3_create_function_v2(
        db, "turbolite_s3_gets", 0,
        SQLITE_UTF8, 0, turbolite_s3_gets_func, 0, 0, 0
    );
    sqlite3_create_function_v2(
        db, "turbolite_s3_bytes", 0,
        SQLITE_UTF8, 0, turbolite_s3_bytes_func, 0, 0, 0
    );

    /* Runtime prefetch tuning: turbolite_config_set(key, value).
     * Keys: 'prefetch_search', 'prefetch_lookup', 'prefetch', 'prefetch_reset', 'plan_aware'.
     * Pushes to global settings queue; VFS drains on next read. */
    sqlite3_create_function_v2(
        db, "turbolite_config_set", 2,
        SQLITE_UTF8, 0, turbolite_config_set_func, 0, 0, 0
    );

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
#undef sqlite3_trace_v2
#undef sqlite3_db_handle

int sqlite3_trace_v2(sqlite3 *db, unsigned mask,
                     int (*xCallback)(unsigned, void*, void*, void*),
                     void *pCtx) {
    return sqlite3_api->trace_v2(db, mask, xCallback, pCtx);
}

sqlite3 *sqlite3_db_handle(sqlite3_stmt *stmt) {
    return sqlite3_api->db_handle(stmt);
}

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

/* Phase Marne: shims for EQP execution from Rust (query_plan.rs).
 * The Rust code declares these as extern "C" and calls them directly.
 * In the loadable extension, they must route through the API table. */

#undef sqlite3_prepare_v2
#undef sqlite3_step
#undef sqlite3_column_text
#undef sqlite3_finalize

int sqlite3_prepare_v2(sqlite3 *db, const char *zSql, int nByte,
                       sqlite3_stmt **ppStmt, const char **pzTail) {
    return sqlite3_api->prepare_v2(db, zSql, nByte, ppStmt, pzTail);
}

int sqlite3_step(sqlite3_stmt *stmt) {
    return sqlite3_api->step(stmt);
}

const unsigned char *sqlite3_column_text(sqlite3_stmt *stmt, int iCol) {
    return sqlite3_api->column_text(stmt, iCol);
}

int sqlite3_finalize(sqlite3_stmt *stmt) {
    return sqlite3_api->finalize(stmt);
}

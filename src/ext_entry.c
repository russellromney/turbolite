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

/* Rust function -- discovers schema from sqlite_master for leaf chasing.
 * Defined in src/tiered/query_plan.rs (Phase Jena-d). */
extern void turbolite_discover_schema(sqlite3 *db);

/* Rust function -- signals query completion for between-query eviction.
 * Defined in src/tiered/query_plan.rs (Phase Stalingrad). */
extern void turbolite_trace_end_query(void);

/* Rust bench functions -- cache control and S3 counters.
 * Defined in src/ext.rs. Available when built with tiered feature. */
extern int turbolite_bench_clear_cache(int mode);
extern int turbolite_bench_reset_s3(void);
extern long long turbolite_bench_s3_gets(void);
extern long long turbolite_bench_s3_bytes(void);

/* Rust function -- full GC scan. Defined in src/ext.rs. */
extern int turbolite_gc(void);

/* Rust function -- compact B-tree groups. Defined in src/ext.rs. */
extern const char *turbolite_compact(void);

/* Rust function -- runtime config (prefetch tuning).
 * Defined in src/tiered/settings.rs. */
extern int turbolite_config_set(const char *key, const char *value);

/* Rust functions -- Phase Stalingrad: cache eviction + observability.
 * Defined in src/ext.rs. */
extern int turbolite_evict_tree(const char *tree_names);
extern int turbolite_evict(const char *tier);
extern const char *turbolite_cache_info(void);
extern const char *turbolite_warm(sqlite3 *db, const char *sql);
extern int turbolite_evict_query(sqlite3 *db, const char *sql);
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

/*
 * turbolite_evict_tree(tree_names TEXT)
 * Evict cached data for named B-trees. Accepts comma-separated names.
 * Example: SELECT turbolite_evict_tree('audit_log, idx_audit_date');
 * Returns number of groups evicted.
 */
static void turbolite_evict_tree_func(
    sqlite3_context *ctx,
    int argc,
    sqlite3_value **argv
) {
    (void)argc;
    const char *names = (const char *)sqlite3_value_text(argv[0]);
    if (!names) {
        sqlite3_result_error(ctx, "turbolite_evict_tree: tree names required", -1);
        return;
    }
    int evicted = turbolite_evict_tree(names);
    if (evicted < 0) {
        sqlite3_result_error(ctx, "turbolite_evict_tree: no tiered VFS registered", -1);
        return;
    }
    sqlite3_result_int(ctx, evicted);
}

/*
 * turbolite_cache_info()
 * Returns JSON with cache size, tier breakdown, group counts, S3 stats.
 */
static void turbolite_cache_info_func(
    sqlite3_context *ctx,
    int argc,
    sqlite3_value **argv
) {
    (void)argc;
    (void)argv;
    const char *json = turbolite_cache_info();
    if (!json) {
        sqlite3_result_error(ctx, "turbolite_cache_info: no tiered VFS registered", -1);
        return;
    }
    sqlite3_result_text(ctx, json, -1, SQLITE_TRANSIENT);
}

/*
 * turbolite_evict(tier TEXT)
 * Evict cached sub-chunks by tier: 'data', 'index', or 'all'.
 * Returns number of sub-chunks evicted.
 */
static void turbolite_evict_func(
    sqlite3_context *ctx,
    int argc,
    sqlite3_value **argv
) {
    (void)argc;
    const char *tier = (const char *)sqlite3_value_text(argv[0]);
    if (!tier) {
        sqlite3_result_error(ctx, "turbolite_evict: tier argument required ('data', 'index', or 'all')", -1);
        return;
    }
    int evicted = turbolite_evict(tier);
    if (evicted < 0) {
        sqlite3_result_error(ctx, "turbolite_evict: no tiered VFS registered", -1);
        return;
    }
    sqlite3_result_int(ctx, evicted);
}

/*
 * turbolite_evict_query(sql TEXT)
 * Evict cached data for trees referenced by a SQL query. Runs EQP,
 * extracts tree names, evicts their groups.
 * Returns number of groups evicted.
 */
static void turbolite_evict_query_func(
    sqlite3_context *ctx,
    int argc,
    sqlite3_value **argv
) {
    (void)argc;
    const char *sql = (const char *)sqlite3_value_text(argv[0]);
    if (!sql) {
        sqlite3_result_error(ctx, "turbolite_evict_query: SQL argument required", -1);
        return;
    }
    sqlite3 *db = sqlite3_context_db_handle(ctx);
    int evicted = turbolite_evict_query(db, sql);
    if (evicted < 0) {
        sqlite3_result_error(ctx, "turbolite_evict_query: no tiered VFS registered", -1);
        return;
    }
    sqlite3_result_int(ctx, evicted);
}

/*
 * turbolite_warm(sql TEXT)
 * Pre-warm cache for a planned query. Runs EQP, extracts tree names,
 * submits their page groups to the prefetch pool. Non-blocking.
 * Returns JSON: {"trees_warmed": [...], "groups_submitted": N}
 */
static void turbolite_warm_func(
    sqlite3_context *ctx,
    int argc,
    sqlite3_value **argv
) {
    (void)argc;
    const char *sql = (const char *)sqlite3_value_text(argv[0]);
    if (!sql) {
        sqlite3_result_error(ctx, "turbolite_warm: SQL argument required", -1);
        return;
    }
    sqlite3 *db = sqlite3_context_db_handle(ctx);
    const char *json = turbolite_warm(db, sql);
    if (!json) {
        sqlite3_result_error(ctx, "turbolite_warm: no tiered VFS registered", -1);
        return;
    }
    sqlite3_result_text(ctx, json, -1, SQLITE_TRANSIENT);
}

/*
 * turbolite_gc()
 * Full orphan scan: list all S3 objects under prefix, delete those not in manifest.
 * Returns number of deleted objects.
 */
static void turbolite_gc_func(
    sqlite3_context *ctx,
    int argc,
    sqlite3_value **argv
) {
    (void)argc;
    (void)argv;
    int deleted = turbolite_gc();
    if (deleted < 0) {
        sqlite3_result_error(ctx, "turbolite_gc: no tiered VFS registered or S3 error", -1);
        return;
    }
    sqlite3_result_int(ctx, deleted);
}

/*
 * turbolite_compact()
 * Compact B-tree page groups: re-walk B-trees, repack groups with >30% dead space.
 * Returns JSON report with compaction results.
 */
static void turbolite_compact_func(
    sqlite3_context *ctx,
    int argc,
    sqlite3_value **argv
) {
    (void)argc;
    (void)argv;
    const char *json = turbolite_compact();
    if (!json) {
        sqlite3_result_error(ctx, "turbolite_compact: no tiered VFS registered or compaction error", -1);
        return;
    }
    sqlite3_result_text(ctx, json, -1, SQLITE_TRANSIENT);
}

/* ── Trace callback: frontrun prefetch + between-query eviction ─── */

/*
 * Trace callback installed via sqlite3_trace_v2 with two event types:
 *
 * SQLITE_TRACE_STMT (Phase Marne): fires at the start of sqlite3_step(),
 * before the VDBE executes. Calls Rust to run EQP on the SQL string and
 * push planned accesses to the global queue. The VFS drains the queue on
 * first cache miss.
 *
 * SQLITE_TRACE_PROFILE (Phase Stalingrad): fires when a statement
 * finishes. Signals query completion so the VFS can run between-query
 * eviction on the next read. The x argument points to an int64_t with
 * elapsed nanoseconds (unused for now, available for future stats).
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
 * but is non-zero overhead for fully-cached hot queries. The
 * PROFILE callback is near-zero cost (one atomic store).
 *
 * Reentrant guard: running EQP inside the callback triggers another
 * sqlite3_step() which fires the trace again. The Rust side filters
 * out EXPLAIN statements, but the C guard prevents the FFI roundtrip
 * entirely for the inner call. The guard also prevents profile events
 * from inner EQP statements from signaling false query completions.
 */
static __thread int turbolite_trace_reentrant = 0;

static int turbolite_trace_callback(
    unsigned trace_type,
    void *ctx,
    void *p,       /* sqlite3_stmt* */
    void *x        /* STMT: const char* SQL; PROFILE: int64_t* nanoseconds */
) {
    (void)ctx;
    if (turbolite_trace_reentrant) return 0;

    if (trace_type == SQLITE_TRACE_STMT) {
        const char *sql = (const char *)x;
        if (!sql || sql[0] == '\0') return 0;

        sqlite3_stmt *stmt = (sqlite3_stmt *)p;
        sqlite3 *db = sqlite3_db_handle(stmt);

        turbolite_trace_reentrant = 1;

        /* Phase Jena-d: discover schema for leaf chasing.
         * Re-discovers on every connection (no static flag) because different
         * connections may open different databases. The VFS side deduplicates
         * via schema_info.is_none() check. */
        turbolite_discover_schema(db);

        turbolite_trace_push_plan(db, sql);
        turbolite_trace_reentrant = 0;
    }
    else if (trace_type == SQLITE_TRACE_PROFILE) {
        turbolite_trace_end_query();
    }
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

    /* Install trace callback for frontrun prefetch + between-query eviction.
     * SQLITE_TRACE_STMT: fires at start of sqlite3_step(), runs EQP, pushes
     * planned B-tree accesses to the global queue (VFS drains on first read).
     * SQLITE_TRACE_PROFILE: fires when statement finishes, signals end-of-query
     * so VFS can run eviction on next read.
     *
     * WARNING: this overwrites any existing trace callback on this connection.
     * See the comment on turbolite_trace_callback above for details. */
    rc = sqlite3_trace_v2(db, SQLITE_TRACE_STMT | SQLITE_TRACE_PROFILE,
                          turbolite_trace_callback, 0);
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

    /* Phase Stalingrad: cache eviction + observability. */
    sqlite3_create_function_v2(
        db, "turbolite_evict_tree", 1,
        SQLITE_UTF8, 0, turbolite_evict_tree_func, 0, 0, 0
    );
    sqlite3_create_function_v2(
        db, "turbolite_cache_info", 0,
        SQLITE_UTF8, 0, turbolite_cache_info_func, 0, 0, 0
    );
    sqlite3_create_function_v2(
        db, "turbolite_evict", 1,
        SQLITE_UTF8, 0, turbolite_evict_func, 0, 0, 0
    );
    sqlite3_create_function_v2(
        db, "turbolite_warm", 1,
        SQLITE_UTF8, 0, turbolite_warm_func, 0, 0, 0
    );
    sqlite3_create_function_v2(
        db, "turbolite_evict_query", 1,
        SQLITE_UTF8, 0, turbolite_evict_query_func, 0, 0, 0
    );
    sqlite3_create_function_v2(
        db, "turbolite_gc", 0,
        SQLITE_UTF8, 0, turbolite_gc_func, 0, 0, 0
    );
    sqlite3_create_function_v2(
        db, "turbolite_compact", 0,
        SQLITE_UTF8, 0, turbolite_compact_func, 0, 0, 0
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
#undef sqlite3_column_int64
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

sqlite3_int64 sqlite3_column_int64(sqlite3_stmt *stmt, int iCol) {
    return sqlite3_api->column_int64(stmt, iCol);
}

int sqlite3_finalize(sqlite3_stmt *stmt) {
    return sqlite3_api->finalize(stmt);
}

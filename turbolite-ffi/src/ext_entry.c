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

/* Rust function -- registers a named local VFS with isolated state.
 * Defined in src/ext.rs. */
extern int turbolite_ext_register_named_vfs(const char *name, const char *cache_dir);

/* Rust function -- registers a file-first local VFS keyed to a database
 * path. The caller's path is the local page image; sidecar metadata lives
 * at `<db_path>-turbolite/`. Defined in src/ext.rs. */
extern int turbolite_ext_register_file_first_vfs(const char *name, const char *db_path);

/* Rust function -- registers a file-first S3 VFS keyed to one logical
 * volume. Unlike the process-global "turbolite-s3" convenience VFS, this
 * lets bindings create one VFS identity per bucket/prefix/db_path. */
extern int turbolite_ext_register_s3_file_first_vfs(
    const char *name,
    const char *db_path,
    const char *bucket,
    const char *prefix,
    const char *endpoint,
    const char *region
);

/* Rust function -- runs EQP on SQL and pushes plan to global queue.
 * Defined in src/tiered/query_plan.rs. */
extern void turbolite_trace_push_plan(sqlite3 *db, const char *sql);

/* Rust function -- signals query completion for between-query eviction.
 * Defined in src/tiered/query_plan.rs. */
extern void turbolite_trace_end_query(void);

/* Rust bench functions -- cache control and S3 counters.
 * Defined in src/ext.rs. Available when built with tiered feature. */
extern int turbolite_bench_clear_cache(int mode);
extern int turbolite_bench_flush_to_storage(void);
extern int turbolite_bench_reset_s3(void);
extern long long turbolite_bench_s3_gets(void);
extern long long turbolite_bench_s3_bytes(void);
extern long long turbolite_bench_s3_puts(void);
extern long long turbolite_bench_s3_put_bytes(void);

/* Rust function -- full GC scan. Defined in src/ext.rs. */
extern int turbolite_gc(void);

/* Rust function -- compact B-tree groups. Defined in src/ext.rs. */
extern const char *turbolite_compact(void);

/* `turbolite_install_config_functions` and its scalar-function
 * counterpart are defined in turbolite-ffi/src/install.rs — they're
 * what embedders call per connection. The extension entry point
 * registers this as a SQLite auto-extension (via the adapter below)
 * so every newly-opened sqlite3 connection picks it up automatically,
 * with each connection's own handle queue captured as pApp. */
extern int turbolite_install_config_functions(sqlite3 *db);

/* Forward decl; defined at the bottom of this file. */
static int install_config_functions_autoext(
    sqlite3 *db, char **pzErrMsg, const sqlite3_api_routines *pApi);

/* Rust functions -- cache eviction + observability.
 * Defined in src/ext.rs. */
extern int turbolite_evict_tree(const char *tree_names);
extern int turbolite_evict(const char *tier);
extern const char *turbolite_cache_info(void);
extern const char *turbolite_warm(sqlite3 *db, const char *sql);
extern int turbolite_evict_query(sqlite3 *db, const char *sql);
/* ── SQL functions ──────────────────────────────────────────────── */

/*
 * turbolite_register_vfs(name TEXT, cache_dir TEXT)
 *
 * Lower-level: register a named local VFS with isolated state under a
 * caller-owned cache directory. Each call creates a new VFS instance with
 * its own manifest, cache, and page group state. The local database image
 * is stored at `<cache_dir>/data.cache`. Bindings that expose a user-facing
 * `app.db` should call `turbolite_register_file_first_vfs` instead.
 *
 * Example: SELECT turbolite_register_vfs('turbolite-0', '/path/to/dbdir');
 * Then:    sqlite3_open_v2("file:db?vfs=turbolite-0", ...)
 *
 * Returns 0 on success, 1 on error.
 */
static void turbolite_register_vfs_func(
    sqlite3_context *ctx,
    int argc,
    sqlite3_value **argv
) {
    (void)argc;
    const char *name = (const char *)sqlite3_value_text(argv[0]);
    const char *cache_dir = (const char *)sqlite3_value_text(argv[1]);
    if (!name || !cache_dir) {
        sqlite3_result_error(ctx, "turbolite_register_vfs: name and cache_dir required", -1);
        return;
    }
    int rc = turbolite_ext_register_named_vfs(name, cache_dir);
    sqlite3_result_int(ctx, rc);
}

/*
 * turbolite_register_file_first_vfs(name TEXT, db_path TEXT)
 *
 * Register a file-first local VFS keyed to a database path. The caller's
 * `db_path` is the local page image; sidecar metadata lives under
 * `<db_path>-turbolite/`. This is the recommended user-facing entry point.
 *
 * Example: SELECT turbolite_register_file_first_vfs('app', '/data/app.db');
 * Then:    sqlite3_open_v2("/data/app.db", ..., "app");
 *
 * Returns 0 on success, 1 on error.
 */
static void turbolite_register_file_first_vfs_func(
    sqlite3_context *ctx,
    int argc,
    sqlite3_value **argv
) {
    (void)argc;
    const char *name = (const char *)sqlite3_value_text(argv[0]);
    const char *db_path = (const char *)sqlite3_value_text(argv[1]);
    if (!name || !db_path) {
        sqlite3_result_error(
            ctx,
            "turbolite_register_file_first_vfs: name and db_path required",
            -1);
        return;
    }
    int rc = turbolite_ext_register_file_first_vfs(name, db_path);
    sqlite3_result_int(ctx, rc);
}

/*
 * turbolite_register_s3_file_first_vfs(
 *   name TEXT, db_path TEXT, bucket TEXT, prefix TEXT, endpoint TEXT, region TEXT
 * )
 *
 * Register a file-first S3 VFS keyed to one logical volume. The caller's
 * `db_path` is the local page image; sidecar metadata lives under
 * `<db_path>-turbolite/`; remote objects live under the supplied
 * bucket/prefix. This is the multi-volume-safe S3 entry point.
 *
 * Returns 0 on success, 1 on error.
 */
static void turbolite_register_s3_file_first_vfs_func(
    sqlite3_context *ctx,
    int argc,
    sqlite3_value **argv
) {
    (void)argc;
    const char *name = (const char *)sqlite3_value_text(argv[0]);
    const char *db_path = (const char *)sqlite3_value_text(argv[1]);
    const char *bucket = (const char *)sqlite3_value_text(argv[2]);
    const char *prefix = (const char *)sqlite3_value_text(argv[3]);
    const char *endpoint = (const char *)sqlite3_value_text(argv[4]);
    const char *region = (const char *)sqlite3_value_text(argv[5]);
    if (!name || !db_path || !bucket) {
        sqlite3_result_error(
            ctx,
            "turbolite_register_s3_file_first_vfs: name, db_path, and bucket required",
            -1);
        return;
    }
    int rc = turbolite_ext_register_s3_file_first_vfs(
        name, db_path, bucket, prefix, endpoint, region);
    sqlite3_result_int(ctx, rc);
}

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

/* turbolite_flush_to_storage() -- drain local-then-flush staging logs. */
static void turbolite_flush_to_storage_func(
    sqlite3_context *ctx,
    int argc,
    sqlite3_value **argv
) {
    (void)argc;
    (void)argv;
    int rc = turbolite_bench_flush_to_storage();
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

/* turbolite_s3_puts() -- return S3 PUT count since last reset. */
static void turbolite_s3_puts_func(
    sqlite3_context *ctx,
    int argc,
    sqlite3_value **argv
) {
    (void)argc;
    (void)argv;
    sqlite3_result_int64(ctx, turbolite_bench_s3_puts());
}

/* turbolite_s3_put_bytes() -- return S3 bytes uploaded since last reset. */
static void turbolite_s3_put_bytes_func(
    sqlite3_context *ctx,
    int argc,
    sqlite3_value **argv
) {
    (void)argc;
    (void)argv;
    sqlite3_result_int64(ctx, turbolite_bench_s3_put_bytes());
}

/*
 * turbolite_config_set / turbolite_install_config_functions
 *
 * Both live in turbolite-ffi/src/install.rs (Rust, callable from C).
 * The scalar function is NOT auto-registered at extension load —
 * callers invoke `turbolite_install_config_functions(db)` per
 * connection, which captures THIS connection's handle queue via
 * sqlite3_create_function_v2's pApp so multi-connection-per-thread
 * routing is correct. See install.rs for the contract.
 */

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
 * SQLITE_TRACE_STMT: fires at the start of sqlite3_step(),
 * before the VDBE executes. Calls Rust to run EQP on the SQL string and
 * push planned accesses to the global queue. The VFS drains the queue on
 * first cache miss.
 *
 * SQLITE_TRACE_PROFILE: fires when a statement
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
int turbolite_c_sqlite3_turbolite_init(
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

    /* register turbolite_register_vfs(name, cache_dir) SQL function.
     * Creates isolated VFS instances for multi-database support. */
    rc = sqlite3_create_function_v2(
        db, "turbolite_register_vfs", 2,
        SQLITE_UTF8, 0, turbolite_register_vfs_func, 0, 0, 0
    );
    if (rc != SQLITE_OK) {
        *pzErrMsg = sqlite3_mprintf("turbolite: failed to register turbolite_register_vfs()");
        return rc;
    }

    /* register turbolite_register_file_first_vfs(name, db_path) SQL function.
     * File-first registration: the caller's db_path is the local page image
     * and sidecar metadata lives at <db_path>-turbolite/. */
    rc = sqlite3_create_function_v2(
        db, "turbolite_register_file_first_vfs", 2,
        SQLITE_UTF8, 0, turbolite_register_file_first_vfs_func, 0, 0, 0
    );
    if (rc != SQLITE_OK) {
        *pzErrMsg = sqlite3_mprintf(
            "turbolite: failed to register turbolite_register_file_first_vfs()");
        return rc;
    }

    /* register turbolite_register_s3_file_first_vfs(...) SQL function.
     * S3 file-first registration: one VFS per logical volume. */
    rc = sqlite3_create_function_v2(
        db, "turbolite_register_s3_file_first_vfs", 6,
        SQLITE_UTF8, 0, turbolite_register_s3_file_first_vfs_func, 0, 0, 0
    );
    if (rc != SQLITE_OK) {
        *pzErrMsg = sqlite3_mprintf(
            "turbolite: failed to register turbolite_register_s3_file_first_vfs()");
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
        db, "turbolite_flush_to_storage", 0,
        SQLITE_UTF8, 0, turbolite_flush_to_storage_func, 0, 0, 0
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
    sqlite3_create_function_v2(
        db, "turbolite_s3_puts", 0,
        SQLITE_UTF8, 0, turbolite_s3_puts_func, 0, 0, 0
    );
    sqlite3_create_function_v2(
        db, "turbolite_s3_put_bytes", 0,
        SQLITE_UTF8, 0, turbolite_s3_put_bytes_func, 0, 0, 0
    );

    /* cache eviction + observability. */
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

    /* Register `turbolite_install_config_functions` as an auto-extension
     * so every connection opened after this extension is loaded picks up
     * the scalar function on its own handle queue via pApp — no explicit
     * per-connection install call required from language bindings. See
     * `install_config_functions_autoext` below for the callback adapter. */
    rc = sqlite3_auto_extension((void(*)(void))install_config_functions_autoext);
    if (rc != SQLITE_OK) {
        *pzErrMsg = sqlite3_mprintf(
            "turbolite: sqlite3_auto_extension(install_config_functions) failed"
        );
        return rc;
    }
    return SQLITE_OK;
}

/*
 * Auto-extension adapter: SQLite calls this for every newly-opened
 * sqlite3 connection. We forward to the Rust-side
 * `turbolite_install_config_functions`, ignoring SQLITE_MISUSE (which
 * just means the connection isn't turbolite-backed — we register on
 * every connection in the process, but only turbolite connections need
 * the scalar).
 *
 * The SQLite auto-extension callback signature is the same as the
 * loadable-extension entry point; SQLITE_EXTENSION_INIT2 is NOT
 * expected here (the API table is already initialized by the outer
 * sqlite3_turbolite_init call that registered us).
 */
static int install_config_functions_autoext(
    sqlite3 *db, char **pzErrMsg, const sqlite3_api_routines *pApi
) {
    (void)pzErrMsg;
    (void)pApi;
    int rc = turbolite_install_config_functions(db);
    /* SQLITE_MISUSE (21) is the expected "no turbolite handle active"
     * response for non-turbolite connections. Treat it as a benign
     * no-op so we don't break unrelated sqlite3_open calls in the same
     * process. */
    if (rc == 21 /* SQLITE_MISUSE */) {
        return SQLITE_OK;
    }
    return rc;
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
#undef sqlite3_auto_extension
#undef sqlite3_exec
#undef sqlite3_free
#undef sqlite3_create_function_v2
#undef sqlite3_user_data
#undef sqlite3_value_text
#undef sqlite3_result_error
#undef sqlite3_result_int
#undef sqlite3_file_control

/* Shims for the Rust-side `install_config_functions` + its scalar
 * function (src/install.rs). Each extern "C" symbol Rust references
 * must route through the sqlite3_api table under loadable-ext mode.
 * Standalone cdylib builds don't compile this file — they resolve
 * these names against libsqlite3-sys's linked libsqlite3. */

int sqlite3_auto_extension(void(*xEntryPoint)(void)) {
    return sqlite3_api->auto_extension(xEntryPoint);
}

int sqlite3_exec(sqlite3 *db, const char *sql,
                 int (*cb)(void*, int, char**, char**),
                 void *arg, char **errmsg) {
    return sqlite3_api->exec(db, sql, cb, arg, errmsg);
}

void sqlite3_free(void *p) {
    sqlite3_api->free(p);
}

int sqlite3_create_function_v2(
    sqlite3 *db, const char *zFunctionName, int nArg, int eTextRep,
    void *pApp,
    void (*xFunc)(sqlite3_context*, int, sqlite3_value**),
    void (*xStep)(sqlite3_context*, int, sqlite3_value**),
    void (*xFinal)(sqlite3_context*),
    void (*xDestroy)(void*)
) {
    return sqlite3_api->create_function_v2(
        db, zFunctionName, nArg, eTextRep, pApp,
        xFunc, xStep, xFinal, xDestroy);
}

void *sqlite3_user_data(sqlite3_context *ctx) {
    return sqlite3_api->user_data(ctx);
}

const unsigned char *sqlite3_value_text(sqlite3_value *v) {
    return sqlite3_api->value_text(v);
}

void sqlite3_result_error(sqlite3_context *ctx, const char *msg, int len) {
    sqlite3_api->result_error(ctx, msg, len);
}

void sqlite3_result_int(sqlite3_context *ctx, int val) {
    sqlite3_api->result_int(ctx, val);
}

int sqlite3_file_control(sqlite3 *db, const char *zDbName, int op, void *arg) {
    return sqlite3_api->file_control(db, zDbName, op, arg);
}

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

/* shims for EQP execution from Rust (query_plan.rs).
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

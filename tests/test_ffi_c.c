/*
 * End-to-end integration test for turbolite shared library.
 *
 * Proves that turbolite.h compiles and the .dylib/.so works from plain C.
 * Full pipeline: register VFS → open → create → insert → query → verify → close.
 *
 * Build & run:
 *   make test-ffi-c
 */

#include "../turbolite.h"
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static int passed = 0;
static int failed = 0;

#define TEST(name)                                                             \
    do {                                                                       \
        printf("  ");                                                          \
    } while (0);                                                               \
    const char *_test_name = name;                                             \
    int _test_ok = 1;

#define ASSERT(cond)                                                           \
    do {                                                                       \
        if (!(cond)) {                                                         \
            printf("FAIL  %s: %s (line %d)\n", _test_name, #cond, __LINE__);   \
            _test_ok = 0;                                                      \
            failed++;                                                          \
            goto cleanup;                                                      \
        }                                                                      \
    } while (0)

#define PASS()                                                                 \
    do {                                                                       \
        if (_test_ok) {                                                        \
            printf("PASS  %s\n", _test_name);                                  \
            passed++;                                                          \
        }                                                                      \
    } while (0)

static char tmpdir_buf[256];

static const char *make_tmpdir(void) {
    snprintf(tmpdir_buf, sizeof(tmpdir_buf), "%s/turbolite-c-XXXXXX",
             getenv("TMPDIR") ? getenv("TMPDIR") : "/tmp");
    return mkdtemp(tmpdir_buf);
}

/* ── Tests ─────────────────────────────────────────────────────────── */

static void test_version(void) {
    TEST("version returns valid string");
    const char *v = turbolite_version();
    ASSERT(v != NULL);
    ASSERT(strlen(v) > 0);
    ASSERT(strchr(v, '.') != NULL); /* semver has dots */
    PASS();
cleanup:;
}

static void test_null_args(void) {
    TEST("null arguments return error");
    int rc = turbolite_register_local(NULL, "/tmp", 3);
    ASSERT(rc == -1);
    const char *err = turbolite_last_error();
    ASSERT(err != NULL);
    ASSERT(strstr(err, "name") != NULL);

    rc = turbolite_register_local("test", NULL, 3);
    ASSERT(rc == -1);
    err = turbolite_last_error();
    ASSERT(strstr(err, "base_dir") != NULL);
    PASS();
cleanup:;
}

static void test_register_compressed(void) {
    TEST("register compressed VFS");
    const char *dir = make_tmpdir();
    ASSERT(dir != NULL);
    int rc = turbolite_register_local("c-compressed", dir, 3);
    ASSERT(rc == 0);
    PASS();
cleanup:;
}


static void test_open_close(void) {
    TEST("open and close database");
    const char *dir = make_tmpdir();
    ASSERT(dir != NULL);
    turbolite_register_local("c-openclose", dir, 3);

    char db_path[512];
    snprintf(db_path, sizeof(db_path), "%s/test.db", dir);

    void *db = turbolite_open(db_path, "c-openclose");
    ASSERT(db != NULL);
    turbolite_close(db);
    PASS();
cleanup:;
}

static void test_exec(void) {
    TEST("exec CREATE TABLE + INSERT");
    const char *dir = make_tmpdir();
    ASSERT(dir != NULL);
    turbolite_register_local("c-exec", dir, 3);

    char db_path[512];
    snprintf(db_path, sizeof(db_path), "%s/test.db", dir);
    void *db = turbolite_open(db_path, "c-exec");
    ASSERT(db != NULL);

    int rc = turbolite_exec(db, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)");
    ASSERT(rc == 0);
    rc = turbolite_exec(db, "INSERT INTO t VALUES (1, 'alice')");
    ASSERT(rc == 0);

    turbolite_close(db);
    PASS();
cleanup:;
}

static void test_bad_sql(void) {
    TEST("bad SQL returns error");
    const char *dir = make_tmpdir();
    ASSERT(dir != NULL);
    turbolite_register_local("c-badsql", dir, 3);

    char db_path[512];
    snprintf(db_path, sizeof(db_path), "%s/test.db", dir);
    void *db = turbolite_open(db_path, "c-badsql");
    ASSERT(db != NULL);

    int rc = turbolite_exec(db, "NOT VALID SQL");
    ASSERT(rc == -1);
    ASSERT(turbolite_last_error() != NULL);

    turbolite_close(db);
    PASS();
cleanup:;
}

static void test_roundtrip(void) {
    TEST("full round-trip: create, insert, query, verify");
    const char *dir = make_tmpdir();
    ASSERT(dir != NULL);
    turbolite_register_local("c-roundtrip", dir, 3);

    char db_path[512];
    snprintf(db_path, sizeof(db_path), "%s/test.db", dir);
    void *db = turbolite_open(db_path, "c-roundtrip");
    ASSERT(db != NULL);

    turbolite_exec(db, "CREATE TABLE users (id INTEGER, name TEXT, age INTEGER);"
                       "INSERT INTO users VALUES (1, 'alice', 30);"
                       "INSERT INTO users VALUES (2, 'bob', 25);"
                       "INSERT INTO users VALUES (3, 'carol', 35);");

    char *json = turbolite_query_json(db, "SELECT * FROM users ORDER BY id");
    ASSERT(json != NULL);

    /* Verify JSON contains expected data */
    ASSERT(strstr(json, "\"alice\"") != NULL);
    ASSERT(strstr(json, "\"bob\"") != NULL);
    ASSERT(strstr(json, "\"carol\"") != NULL);
    ASSERT(strstr(json, "30") != NULL);
    ASSERT(strstr(json, "25") != NULL);
    ASSERT(strstr(json, "35") != NULL);

    turbolite_free_string(json);
    turbolite_close(db);
    PASS();
cleanup:;
}

static void test_persistence(void) {
    TEST("data persists across connections");
    const char *dir = make_tmpdir();
    ASSERT(dir != NULL);
    turbolite_register_local("c-persist", dir, 3);

    char db_path[512];
    snprintf(db_path, sizeof(db_path), "%s/persist.db", dir);

    /* Write */
    void *db = turbolite_open(db_path, "c-persist");
    ASSERT(db != NULL);
    turbolite_exec(db, "CREATE TABLE kv (k TEXT, v TEXT);"
                       "INSERT INTO kv VALUES ('hello', 'world');");
    turbolite_close(db);

    /* Re-open and read */
    db = turbolite_open(db_path, "c-persist");
    ASSERT(db != NULL);
    char *json = turbolite_query_json(db, "SELECT * FROM kv");
    ASSERT(json != NULL);
    ASSERT(strstr(json, "\"hello\"") != NULL);
    ASSERT(strstr(json, "\"world\"") != NULL);

    turbolite_free_string(json);
    turbolite_close(db);
    PASS();
cleanup:;
}

static void test_null_safety(void) {
    TEST("NULL handles don't crash");
    turbolite_close(NULL);
    turbolite_free_string(NULL);
    void *db = turbolite_open(NULL, "any");
    ASSERT(db == NULL);
    ASSERT(turbolite_invalidate_cache(NULL) == -1);
    PASS();
cleanup:;
}

static void test_clear_caches(void) {
    TEST("clear_caches does not crash");
    turbolite_clear_caches();
    PASS();
cleanup:;
}

/* ── Tiered S3 tests (only when TURBOLITE_TIERED is defined + env set) ── */

#ifdef TURBOLITE_TIERED

static void test_tiered_register(void) {
    const char *bucket = getenv("TIERED_TEST_BUCKET");
    if (!bucket) return; /* skip silently; printed in main */

    TEST("tiered: register S3 VFS");
    const char *dir = make_tmpdir();
    ASSERT(dir != NULL);

    const char *endpoint = getenv("AWS_ENDPOINT_URL");
    const char *region = getenv("AWS_REGION");
    if (!region) region = "auto";

    int rc = turbolite_register_tiered(
        "c-tiered-reg", bucket, "test/ffi-c/reg", dir, endpoint, region);
    ASSERT(rc == 0);
    PASS();
cleanup:;
}

static void test_tiered_roundtrip(void) {
    const char *bucket = getenv("TIERED_TEST_BUCKET");
    if (!bucket) return;

    TEST("tiered: full round-trip via S3");
    const char *dir = make_tmpdir();
    ASSERT(dir != NULL);

    const char *endpoint = getenv("AWS_ENDPOINT_URL");
    const char *region = getenv("AWS_REGION");
    if (!region) region = "auto";

    int rc = turbolite_register_tiered(
        "c-tiered-rt", bucket, "test/ffi-c/roundtrip", dir, endpoint, region);
    ASSERT(rc == 0);

    char db_path[512];
    snprintf(db_path, sizeof(db_path), "%s/test.db", dir);
    void *db = turbolite_open(db_path, "c-tiered-rt");
    ASSERT(db != NULL);

    turbolite_exec(db, "CREATE TABLE users (id INTEGER, name TEXT, age INTEGER);"
                       "INSERT INTO users VALUES (1, 'alice', 30);"
                       "INSERT INTO users VALUES (2, 'bob', 25);"
                       "INSERT INTO users VALUES (3, 'carol', 35);");

    char *json = turbolite_query_json(db, "SELECT * FROM users ORDER BY id");
    ASSERT(json != NULL);
    ASSERT(strstr(json, "\"alice\"") != NULL);
    ASSERT(strstr(json, "\"bob\"") != NULL);
    ASSERT(strstr(json, "\"carol\"") != NULL);

    turbolite_free_string(json);
    turbolite_close(db);
    PASS();
cleanup:;
}

static void test_tiered_null_bucket(void) {
    TEST("tiered: null bucket returns error");
    const char *dir = make_tmpdir();
    ASSERT(dir != NULL);
    int rc = turbolite_register_tiered("c-tiered-null", NULL, "prefix", dir, NULL, NULL);
    ASSERT(rc == -1);
    const char *err = turbolite_last_error();
    ASSERT(err != NULL);
    ASSERT(strstr(err, "bucket") != NULL);
    PASS();
cleanup:;
}

#endif /* TURBOLITE_TIERED */

/* ── Main ──────────────────────────────────────────────────────────── */

int main(void) {
    test_version();
    test_null_args();
    test_register_compressed();
    test_open_close();
    test_exec();
    test_bad_sql();
    test_roundtrip();
    test_persistence();
    test_null_safety();
    test_clear_caches();

#ifdef TURBOLITE_TIERED
    if (getenv("TIERED_TEST_BUCKET")) {
        test_tiered_register();
        test_tiered_roundtrip();
        test_tiered_null_bucket();
    } else {
        printf("  SKIP  tiered tests (set TIERED_TEST_BUCKET to enable)\n");
    }
#else
    printf("  SKIP  tiered tests (compiled without -DTURBOLITE_TIERED)\n");
#endif

    printf("\nResults: %d passed, %d failed\n", passed, failed);
    return failed ? 1 : 0;
}

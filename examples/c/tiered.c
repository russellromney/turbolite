/*
 * turbolite example — C (S3 tiered)
 *
 * A sensor data logger that writes readings to S3 tiered SQLite.
 * Pages are compressed locally and synced to S3 for durability.
 *
 * Build & run:
 *   make lib-bundled header
 *   TURBOLITE_BUCKET=my-bucket make example-c-tiered
 *
 * Required:
 *   TURBOLITE_BUCKET          S3 bucket name
 *
 * Optional:
 *   TURBOLITE_ENDPOINT_URL    S3 endpoint (Tigris, MinIO, etc.)
 *   TURBOLITE_REGION          AWS region
 *   AWS_ACCESS_KEY_ID         S3 credentials
 *   AWS_SECRET_ACCESS_KEY     S3 credentials
 */

#include "../../turbolite.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

static void die(const char *msg) {
    const char *err = turbolite_last_error();
    fprintf(stderr, "FATAL: %s: %s\n", msg, err ? err : "unknown");
    exit(1);
}

static char tmpdir_buf[256];

int main(void) {
    const char *bucket = getenv("TURBOLITE_BUCKET");
    if (!bucket) {
        fprintf(stderr, "FATAL: TURBOLITE_BUCKET is required\n");
        return 1;
    }

    /* Create local cache directory */
    snprintf(tmpdir_buf, sizeof(tmpdir_buf), "%s/turbolite-tiered-XXXXXX",
             getenv("TMPDIR") ? getenv("TMPDIR") : "/tmp");
    const char *cache_dir = mkdtemp(tmpdir_buf);
    if (!cache_dir) { perror("mkdtemp"); return 1; }

    printf("turbolite %s — sensor logger (S3 tiered)\n", turbolite_version());
    printf("Bucket: %s\n", bucket);
    printf("Cache dir: %s\n\n", cache_dir);

    /* Register tiered VFS and open database */
    const char *endpoint = getenv("TURBOLITE_ENDPOINT_URL");
    const char *region = getenv("TURBOLITE_REGION");
    if (turbolite_register_tiered("tiered", bucket, "sensors", cache_dir, endpoint, region) != 0)
        die("register tiered VFS");

    char db_path[512];
    snprintf(db_path, sizeof(db_path), "%s/sensors.db", cache_dir);

    void *db = turbolite_open(db_path, "tiered");
    if (!db) die("open database");

    turbolite_exec(db,
        "CREATE TABLE IF NOT EXISTS readings ("
        "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
        "  sensor TEXT NOT NULL,"
        "  value REAL NOT NULL,"
        "  ts INTEGER NOT NULL"
        ")");

    /* Simulate logging sensor readings */
    srand((unsigned)time(NULL));
    for (int i = 0; i < 10; i++) {
        char sql[256];
        double temp = 20.0 + (rand() % 100) / 10.0;
        snprintf(sql, sizeof(sql),
            "INSERT INTO readings (sensor, value, ts) VALUES ('temp-01', %.1f, %ld)",
            temp, (long)time(NULL) + i);

        if (turbolite_exec(db, sql) != 0)
            die("insert reading");
    }
    printf("Logged 10 readings.\n");

    /* Query summary */
    char *json = turbolite_query_json(db,
        "SELECT sensor, COUNT(*) as count, "
        "ROUND(AVG(value), 1) as avg_temp, "
        "ROUND(MIN(value), 1) as min_temp, "
        "ROUND(MAX(value), 1) as max_temp "
        "FROM readings GROUP BY sensor");
    if (!json) die("query");

    printf("Summary: %s\n", json);
    turbolite_free_string(json);

    turbolite_close(db);
    return 0;
}

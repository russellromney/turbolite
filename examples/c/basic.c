/*
 * turbolite example — C
 *
 * A sensor data logger that writes readings to compressed SQLite.
 *
 * Build & run:
 *   make lib-bundled header
 *   make example-c
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
    /* Create data directory */
    snprintf(tmpdir_buf, sizeof(tmpdir_buf), "%s/turbolite-sensor-XXXXXX",
             getenv("TMPDIR") ? getenv("TMPDIR") : "/tmp");
    const char *data_dir = mkdtemp(tmpdir_buf);
    if (!data_dir) { perror("mkdtemp"); return 1; }

    printf("turbolite %s — sensor logger\n", turbolite_version());
    printf("Data dir: %s\n\n", data_dir);

    /* Register VFS and open database */
    if (turbolite_register_compressed("sensor", data_dir, 3) != 0)
        die("register VFS");

    char db_path[512];
    snprintf(db_path, sizeof(db_path), "%s/sensors.db", data_dir);

    void *db = turbolite_open(db_path, "sensor");
    if (!db) die("open database");

    turbolite_exec(db,
        "CREATE TABLE readings ("
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

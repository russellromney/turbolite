# turbolite — C examples

Sensor data loggers that write readings to turbolite-compressed SQLite.

## Local compressed (`local.c`)

```bash
make example-c
```

## S3 tiered (`tiered.c`)

```bash
TURBOLITE_BUCKET=my-bucket make example-c-tiered
```

## What they do

Simulate an IoT sensor logger: generate 10 temperature readings, store them in a turbolite database, then query aggregate stats (count, avg, min, max).

## How it works

1. `#include "turbolite.h"` - auto-generated C header from `make header`
2. `turbolite_register_local_file_first` (local file-first) or
   `turbolite_register_cloud` (S3). The lower-level
   `turbolite_register_local(name, cache_dir, level)` is also available
   when you want turbolite to own a directory and store the local image
   at `<cache_dir>/data.cache`.
3. `turbolite_open` - open a database through the VFS
4. `turbolite_exec` - insert sensor readings
5. `turbolite_query_json` - query aggregates, get JSON back

In file-first mode the user-supplied database path (`sensors.db` in
the example) is the local page image. Hidden implementation state lives
next to it under `sensors.db-turbolite/`.

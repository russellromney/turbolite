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
2. `turbolite_register_compressed` (local) or `turbolite_register_tiered` (S3)
3. `turbolite_open` - open a database through the VFS
4. `turbolite_exec` - insert sensor readings
5. `turbolite_query_json` - query aggregates, get JSON back

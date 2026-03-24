# turbolite — C example

A sensor data logger that writes readings to compressed SQLite.

## Run

```bash
make example-c
```

## What it does

Simulates an IoT sensor logger — generates 10 temperature readings, stores them in a turbolite-compressed database, then queries aggregate stats (count, avg, min, max).

## How it works

1. `#include "turbolite.h"` — auto-generated C header from `make header`
2. `turbolite_register_compressed` — register a zstd-compressed VFS
3. `turbolite_open` — open a database through the VFS
4. `turbolite_exec` — insert sensor readings
5. `turbolite_query_json` — query aggregates, get JSON back

# Claude Development Guidelines

## Secrets

Managed via [Soup](https://getsoup.dev) (`~/.soup/bin/soup`).

- Project: `sqlces`, environment: `development`
- `soup run -p sqlces -e development -- <cmd>` injects secrets as env vars
- `soup secrets list -p sqlces -e development` to view keys
- `soup secrets set -p sqlces -e development KEY VALUE` to add/update

Secrets stored:
- `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` — Tigris S3-compatible credentials
- `AWS_ENDPOINT_URL` — Tigris endpoint (`https://t3.storage.dev`)
- `AWS_REGION` — `auto`
- `TIERED_TEST_BUCKET` — S3 bucket for integration tests (`sqlces-test`)

## Running Tests

```bash
# Unit tests (no credentials needed)
cargo test --features tiered,zstd --lib

# Integration tests (require Tigris credentials via soup)
soup run -p sqlces -e development -- cargo test --features tiered,zstd --test tiered_test -- --ignored
```

## Code Style

- Concise over verbose
- No over-engineering or premature abstractions
- Keep files under 1000 lines — split when approaching this limit
- Fail fast — `.expect()` not `.unwrap_or()` for critical values

## Roadmap Phase Naming

Phases in ROADMAP.md are named after historical battles/wars, NOT numbered. This avoids renumbering when inserting new phases.

- Each phase header includes adjacency links: `> After: PhaseName · Before: PhaseName`
- Subphases within a phase use letters or numbers (a, b, c or 1, 2, 3)
- New phases pick any unique battle name; ordering is by adjacency links, not chronology
- When referencing phases in code comments or docs, use the battle name (e.g., "see Midway" not "see Phase 16")

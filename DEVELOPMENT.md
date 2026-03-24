# Development Guidelines

## Secrets

Managed via [Soup](https://getsoup.dev) (`~/.soup/bin/soup`).

- Project: `turbolite`, environment: `development`
- `soup run -p turbolite -e development -- <cmd>` injects secrets as env vars
- `soup secrets list -p turbolite -e development` to view keys
- `soup secrets set -p turbolite -e development KEY VALUE` to add/update

Secrets stored:
- `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` — Tigris S3-compatible credentials
- `AWS_ENDPOINT_URL` — Tigris endpoint (`https://t3.storage.dev`)
- `AWS_REGION` — `auto`
- `TIERED_TEST_BUCKET` — S3 bucket for integration tests (`turbolite-test`)

You can use these as environment variables as well.

## Running Tests

```bash
# Unit tests (no credentials needed)
cargo test --features tiered,zstd --lib

# Integration tests (require Tigris credentials via soup)
soup run -p turbolite -e development -- cargo test --features tiered,zstd --test tiered_test -- --ignored
```

## Code Style

- Concise over verbose
- No over-engineering or premature abstractions
- Keep files under 1000 lines — split when approaching this limit
- Fail fast — `.expect()` not `.unwrap_or()` for critical values

## Roadmap Phase Naming

Phases in ROADMAP.md use the format `Phase <Name>`, NOT numbers. This avoids renumbering when inserting new phases.

- Names must be unique across the project
- Pick from: space missions, battles, mountains, or epic-sounding words (e.g., Phase Apollo, Phase Midway, Phase Denali, Phase Obsidian)
- Each phase header includes adjacency links: `> After: Phase X · Before: Phase Y`
- Subphases within a phase use letters or numbers (a, b, c or 1, 2, 3)
- Ordering is by adjacency links, not alphabetical or chronological
- Reference phases by name in code comments and docs (e.g., "see Phase Midway")

# Development Guidelines

## Secrets

Set these environment variables for S3/Tigris integration tests:

- `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` — S3-compatible credentials
- `AWS_ENDPOINT_URL` — S3 endpoint (Tigris: `https://fly.storage.tigris.dev`)
- `AWS_REGION` — region (e.g. `auto` for Tigris, `us-east-2` for AWS)
- `TIERED_TEST_BUCKET` — S3 bucket for integration tests

Use a `.env` file, your shell profile, or any secrets manager to inject them.

## Running Tests

```bash
# Unit tests (no credentials needed)
cargo test --features tiered,zstd --lib

# Integration tests (require S3/Tigris credentials in environment)
cargo test --features tiered,zstd --test tiered_test -- --ignored
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

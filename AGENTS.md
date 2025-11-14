# Repository Guidelines

## Core Scope
- `crates/cli` hosts the CLI binaries. The `plinko` module exposes `logs-plinko`, `balance-diffs-plinko`, and `transactions-plinko` which extract data from reth MDBX or RPC, emit ZSTD parquet, and persist incremental state in `plinko_cache/` (ignored).
- `crates/freeze` implements shared extraction logic. Plinko-specific datasets live under `crates/freeze/src/datasets/*_plinko/`; keep schemas deterministic because the downstream PIR pipeline depends on fixed column order and datatype.
- Long-form docs stay inside `book/`. Any one-off research notes or post-mortems should live in external docs—do not add root-level Markdown dumps.
- `scripts/generate_address_mapping.py` is the only script committed in this repo. It turns balance parquet outputs into `address-mapping.bin`. The massive `.bin` artifacts are generated on demand and are always gitignored.

## Build & Test Expectations
- Rust toolchain: `cargo fmt --all`, `cargo clippy --workspace --all-targets --all-features -D warnings`, and `cargo test --workspace` must pass before pushing.
- Focused tests: `cargo test -p cryo_cli plinko` for the CLI interface and `cargo test -p cryo_freeze balance_diffs_plinko` for dataset regressions.
- Python bindings: run `maturin develop -m crates/python/Cargo.toml` plus `pytest crates/python` when touching adapter code.
- Avoid committing binaries: delete `local_parquet_output/`, `plinko_cache/`, `*.bin`, and other generated outputs after local validation. The `.gitignore` file already blocks them—keep it current if new scratch paths appear.

## Data & Deployment Workflow
1. **Log extraction** – `cryo logs-plinko --reth-db /path/to/reth/db --blocks 23700000:23800000 --output ./local_parquet_output`.
2. **Balance diffs** – `cryo balance-diffs-plinko --parquet ./local_parquet_output --output ./plinko_cache`.
3. **Address mapping** – `python3 scripts/generate_address_mapping.py ./plinko_cache -o ./artifacts/address-mapping.bin`.
4. **Dataset packaging** – copy `database.bin`, `address-mapping.bin`, and `hint.bin` into deployment repos (never this repo). Document the source block range and SHA-256 in PR descriptions.
5. **UI/RPC consumers** live in dedicated repos; this repo only contains the CLI + extraction logic.

## Coding Style & Reviews
- Keep modules small and composable. Each new CLI flag should have a typed struct entry in `crates/cli/src/plinko/args.rs`, validation in `parse.rs`, and unit tests in `tests.rs`.
- Prefer returning `eyre::Report` or typed error enums instead of panics; `unwrap`/`expect` are allowed only in tests or obvious `Option` destructors.
- When touching MDBX code, describe the expected `reth` layout in comments so future contributors can replay the setup.
- PRs must include: summary, testing commands, dataset/range info, and any operational steps (e.g., “copied database.bin to Vultr”). Attach screenshots/logs when UX or deployment changes are involved.
- Ask for reviews early if schema or public CLI changes are planned—downstream consumers break when columns/order change without notice.

## Operational Notes
- Reth MDBX paths differ per host; capture them in PR descriptions (`~/.local/share/reth/mainnet/db` vs remote mounts) and never hardcode.
- Continuous jobs should use `--incremental` with sane `--poll-interval`; leave comments in `plinko_cache/state.json` describing when it was last reset.
- Always verify `scripts/generate_address_mapping.py` output count matches `DB_SIZE` passed to deployments; drift indicates missing parquet files.
- We deploy via Docker Compose on Vultr (`plinko-pir-deploy`). Infra updates live in that repo; here we only document the commands (`docker compose -f docker-compose-real-ethereum.yml up -d`).

Staying disciplined about artifacts and documentation keeps this repo lean and reviewable. If a change touches anything outside the Rust workspace or the mapping script, call it out explicitly so reviewers know where to look.

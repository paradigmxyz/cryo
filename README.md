# ❄️ cryo — Plinko Data Edition

This fork of `cryo` focuses on producing the artifacts required by the Plinko PIR stack:

- High-volume **log extraction** from either a local reth MDBX database or a remote RPC.
- Deterministic **balance diff snapshots** and parquet outputs that match the PIR schema.
- A lightweight **address-mapping generator** that mirrors the layout used by the deployed Plinko CDN.

All binaries remain Rust-based and ship as part of the `cryo_cli` crate; UI or PIR server code lives in separate repositories.

---

## Contents

1. [Quick Start](#quick-start)
2. [Plinko Workflows](#plinko-workflows)
3. [Repository Layout](#repository-layout)
4. [Development](#development)
5. [Cleaning Up Artifacts](#cleaning-up-artifacts)
6. [Contributing & PRs](#contributing--prs)

---

## Quick Start

### Install Rust prerequisites

```bash
rustup target add x86_64-unknown-linux-gnu
cargo install cargo-nextest
```

### Build the CLI

```bash
cargo build --release -p cryo_cli
```

The binary lands in `target/release/cryo`.

### Common commands

| Task | Command |
|------|---------|
| Extract 100k logs from reth MDBX | `cryo logs-plinko --reth-db ~/.local/share/reth/mainnet/db --blocks 23700000:23800000 --output ./local_parquet_output` |
| Run incremental log sync from RPC | `cryo logs-plinko --rpc https://ethereum-rpc.publicnode.com --blocks latest:120000 --output ./local_parquet_output --incremental --poll-interval 12` |
| Produce balance parquet shards | `cryo balance-diffs-plinko --parquet ./local_parquet_output --output ./plinko_cache --chunk-size 1000` |
| Generate address mapping | `python3 scripts/generate_address_mapping.py ./plinko_cache -o ./artifacts/address-mapping.bin` |
| List CLI options | `cryo logs-plinko --help` |

> **Tip:** Provide both `--reth-db` and `--rpc` to let the CLI fall back automatically if MDBX becomes unavailable mid-run.

---

## Plinko Workflows

### 1. Log Extraction

- Source selection is mutual exclusive: at least one of `--reth-db` or `--rpc` must be set.
- Chunking defaults to 1,000 blocks per parquet file; override with `--chunk-size`.
- Incremental mode (`--incremental`) tracks `latest` ranges and writes `state.json` under the output directory so Ctrl+C resumes where it left off.

### 2. Balance Diffs

- `plinko_cache/` stores parquet outputs for `balance_diffs_plinko`. Each file covers the same block window as the input logs.
- Output schema is fixed (address, index, balance_before, balance_after, block_number). Changing it requires updating downstream PIR consumers, so coordinate through code review.

### 3. Address Mapping

- Use `scripts/generate_address_mapping.py` after balance diffs complete. The script walks all `balance_diffs_blocks-*.parquet` files, deduplicates addresses, assigns indices, and writes a binary blob (`<index: u32 LE><address: 20 bytes>`).
- Always record the resulting entry count and SHA-256 in your PR description. That count must match the `DB_SIZE` used when deploying the PIR server.
- This repo stores the script only; the generated `.bin` files are ignored via `.gitignore`.

### 4. Packaging & Deployment

1. Copy `database.bin`, `address-mapping.bin`, `hint.bin`, and any incremental deltas into the dedicated deployment repository (`plinko-pir-deploy`).
2. Update the compose file there with the new `DB_SIZE`.
3. Restart the stack (`docker compose -f docker-compose-real-ethereum.yml up -d`) and verify `/cdn/snapshots/latest/manifest.json`.
4. Document the block range and hashes in the PR that introduced the extraction.

---

## Repository Layout

```
crates/
  cli/                  # cryo_cli entrypoint, including plinko subcommands
    src/plinko/         # args, parsing, runner, tests for logs/balance/tx datasets
  freeze/               # dataset implementations + parquet writers
  python/               # PyO3 adapters (maturin)
book/                   # mdBook documentation for legacy cryo usage
scripts/
  generate_address_mapping.py
AGENTS.md               # Contributor guidelines (see below)
README.md               # You are here
.github/                # CI workflows (lint/test)
```

Legacy research notes (Phase summaries, MDBX experiments, etc.) have been removed. Keep the root clean—place exploratory notes in issues or external documents.

---

## Development

1. **Formatting & linting**
   ```bash
   cargo fmt --all
   cargo clippy --workspace --all-targets --all-features -D warnings
   ```
2. **Testing**
   ```bash
   cargo test --workspace
   cargo test -p cryo_cli plinko
   cargo test -p cryo_freeze balance_diffs_plinko::tests
   ```
3. **Python bindings** (when touching `crates/python`)
   ```bash
   maturin develop -m crates/python/Cargo.toml
   pytest crates/python
   ```
4. **Address mapping script**
   ```bash
   python3 -m venv .venv && source .venv/bin/activate
   pip install pyarrow
   python3 scripts/generate_address_mapping.py ./plinko_cache -o ./artifacts/address-mapping.bin
   ```

---

## Cleaning Up Artifacts

The following paths are gitignored and should never be committed:

- `local_parquet_output/`, `plinko_cache/`, and any `*.parquet` or `.json` output files.
- `database.bin`, `address-mapping.bin`, `hint.bin`, delta bundles, or log archives.
- Local tooling folders (`.claude*/`, `.taskmaster/`, `services/`, `todo-app/`) and IDE files (`*.code-workspace`, `.DS_Store`).

Before opening a PR run `git status` and delete anything unexpected.

---

## Contributing & PRs

- Review [AGENTS.md](AGENTS.md) for expectations on testing, documentation, and operational notes.
- Summaries must include the block range processed, the source (reth path or RPC), and any artifacts copied to deployment hosts.
- Attach logs/screenshots when changes impact deployments (e.g., new compose env vars) so reviewers can independently verify.
- For schema or CLI flag changes, update this README plus the relevant mdBook chapter and add migration notes to your PR description.

With the cryo CLI + mapping script cleaned up and documented, the repository is ready for focused Plinko data work. Happy freezing!

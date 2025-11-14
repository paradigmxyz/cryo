#!/usr/bin/env python3
"""Generate address-mapping.bin from Plinko balance parquet exports.

This script scans every parquet file in the provided directory, collects the
unique Ethereum addresses, assigns them deterministic indices, and writes the
binary mapping format expected by Plinko PIR (4 byte LE index + 20 byte
address).
"""

from __future__ import annotations

import argparse
import logging
import struct
from pathlib import Path
from typing import Iterable

import pyarrow.parquet as pq


logger = logging.getLogger(__name__)


def iter_parquet_batches(parquet_path: Path, batch_size: int = 200_000):
    """Yield address batches from a parquet file as memoryview objects."""

    logger.debug("Scanning %s", parquet_path)
    pf = pq.ParquetFile(parquet_path)
    for batch in pf.iter_batches(columns=["address"], batch_size=batch_size):
        yield batch.column(0)


def collect_addresses(paths: Iterable[Path]) -> list[bytes]:
    seen: set[bytes] = set()
    total_files = 0
    for path in sorted(paths):
        total_files += 1
        for column in iter_parquet_batches(path):
            for value in column:
                if value is not None:
                    seen.add(bytes(value))
        if total_files % 10 == 0:
            logger.info("Processed %d files, %d unique addresses", total_files, len(seen))

    logger.info(
        "Finished scanning %d files, collected %d unique addresses",
        total_files,
        len(seen),
    )
    return sorted(seen)


def write_mapping(addresses: list[bytes], output_path: Path) -> None:
    with output_path.open("wb") as f:
        for idx, address in enumerate(addresses):
            if len(address) != 20:
                raise ValueError(f"Address {address.hex()} has length {len(address)}")
            f.write(struct.pack("<I", idx))
            f.write(address)

    logger.info(
        "Wrote %d entries to %s (%.2f MB)",
        len(addresses),
        output_path,
        output_path.stat().st_size / 1_048_576,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "input",
        type=Path,
        help="Directory containing balance_diffs_blocks-*.parquet files",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        default=Path("address-mapping.bin"),
        help="Path to write the binary mapping",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable verbose logging",
    )

    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO)

    if not args.input.is_dir():
        raise SystemExit(f"Input directory {args.input} does not exist")

    parquet_files = list(args.input.glob("balance_diffs_blocks-*.parquet"))
    if not parquet_files:
        raise SystemExit(f"No parquet files found in {args.input}")

    addresses = collect_addresses(parquet_files)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    write_mapping(addresses, args.output)


if __name__ == "__main__":
    main()

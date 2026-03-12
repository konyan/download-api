#!/usr/bin/env python3
"""
Generate CSV data in format:
bookId,check,success
42000000001,false,false
...

Default range:
42000000001-42000009999
"""

from __future__ import annotations

import argparse
import csv
from pathlib import Path


def generate_csv(output: Path, start: int, end: int) -> None:
    if end < start:
        raise ValueError("end must be greater than or equal to start")

    with output.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["bookId", "check", "success"])

        for book_id in range(start, end + 1):
            writer.writerow([str(book_id), "false", "false"])


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate drama bookId CSV")
    parser.add_argument(
        "--start",
        type=int,
        default=42000000001,
        help="Start bookId (default: 42000000001)",
    )
    parser.add_argument(
        "--end",
        type=int,
        default=42000009999,
        help="End bookId inclusive (default: 42000009999)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("drama-book_ids.csv"),
        help="Output CSV file (default: drama-book_ids.csv)",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        generate_csv(args.output, args.start, args.end)
    except ValueError as err:
        print(f"[ERROR] {err}")
        return 1

    print(f"[OK] Generated: {args.output}")
    print(f"[OK] Range: {args.start} - {args.end}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

from __future__ import annotations

import argparse
import csv
from pathlib import Path

# Reuse the canonical field order from the main scraper.
try:
    from master import FIELDS
except Exception:  # pragma: no cover - defensive fallback if master import ever fails
    FIELDS = ["zip_code", "listing_url", "name", "address", "phone", "agency_type", "about"]


def gather_csv_files(input_root: Path) -> list[Path]:
    """Return all listings.csv files under the given root (recursive)."""
    return sorted(
        path
        for path in input_root.rglob("listings.csv")
        if path.is_file()
    )


def combine_csvs(input_root: Path, output_csv: Path, dedupe_key: str | None = "listing_url") -> dict[str, int]:
    """Combine per-ZIP listings CSVs into a single file."""
    files = gather_csv_files(input_root)
    output_csv.parent.mkdir(parents=True, exist_ok=True)

    seen: set[tuple] = set()
    rows: list[dict[str, str]] = []

    for csv_path in files:
        # Skip the destination file if it already exists inside the tree
        if csv_path.resolve() == output_csv.resolve():
            continue

        with csv_path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            if not reader.fieldnames:
                continue

            for row in reader:
                if dedupe_key:
                    key = (row.get(dedupe_key, ""),)
                    if key in seen:
                        continue
                    seen.add(key)
                rows.append(row)

    tmp_csv = output_csv.with_suffix(output_csv.suffix + ".tmp")
    with tmp_csv.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in FIELDS})
    tmp_csv.replace(output_csv)

    return {
        "files": len(files),
        "rows_written": len(rows),
        "deduped": len(seen) if dedupe_key else len(rows),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Combine per-ZIP listings.csv files into one CSV.")
    parser.add_argument(
        "--input-root",
        type=Path,
        default=Path("output"),
        help="Root directory containing per-ZIP folders (default: output)",
    )
    parser.add_argument(
        "--output-csv",
        type=Path,
        default=Path("output/combined_listings.csv"),
        help="Where to write the combined CSV (default: output/combined_listings.csv)",
    )
    parser.add_argument(
        "--no-dedupe",
        action="store_true",
        help="Disable deduplication (by listing_url when enabled).",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    stats = combine_csvs(
        input_root=args.input_root,
        output_csv=args.output_csv,
        dedupe_key=None if args.no_dedupe else "listing_url",
    )
    print(
        f"Combined {stats['files']} file(s) -> {args.output_csv} "
        f"(rows written: {stats['rows_written']}, deduped: {stats['deduped']})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

from __future__ import annotations

import argparse
import csv
from collections import defaultdict
from pathlib import Path


def gather_listings_csvs(root: Path) -> list[Path]:
    return sorted(path for path in root.rglob("listings.csv") if path.is_file())


def build_zip_report(files: list[Path]) -> dict[str, int]:
    counts: dict[str, int] = defaultdict(int)
    for csv_path in files:
        with csv_path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                zip_code = (row.get("zip_code") or "").strip()
                if not zip_code:
                    continue
                counts[zip_code] += 1
    return counts


def write_report(counts: dict[str, int], output_csv: Path) -> None:
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    tmp_csv = output_csv.with_suffix(output_csv.suffix + ".tmp")
    with tmp_csv.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["zip_code", "rows"])
        writer.writeheader()
        for zip_code in sorted(counts):
            writer.writerow({"zip_code": zip_code, "rows": counts[zip_code]})
    tmp_csv.replace(output_csv)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate a report of which ZIP codes have listing data."
    )
    parser.add_argument(
        "--input-root",
        type=Path,
        default=Path("output"),
        help="Root directory containing per-ZIP listings.csv files (default: output)",
    )
    parser.add_argument(
        "--output-csv",
        type=Path,
        default=Path("output/zip_report.csv"),
        help="Where to write the ZIP coverage report (default: output/zip_report.csv)",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    files = gather_listings_csvs(args.input_root)
    counts = build_zip_report(files)
    write_report(counts, args.output_csv)
    print(
        f"Report created for {len(counts)} ZIP(s) from {len(files)} file(s) -> {args.output_csv}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

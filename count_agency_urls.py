from __future__ import annotations

import argparse
import json
from pathlib import Path


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Summarize agency_urls.json counts under an output root.")
    p.add_argument("--root", type=Path, default=Path("output"), help="Root folder to scan (default: output)")
    p.add_argument(
        "--report",
        type=Path,
        default=Path("output/nonzero_agency_urls.csv"),
        help="CSV path to write non-zero counts (default: output/nonzero_agency_urls.csv)",
    )
    return p.parse_args()


def count_files(root: Path) -> tuple[list[tuple[Path, int]], int]:
    rows: list[tuple[Path, int]] = []
    total = 0
    for f in sorted(root.rglob("agency_urls.json")):
        try:
            data = json.loads(f.read_text(encoding="utf-8"))
            count = len(data) if isinstance(data, list) else 0
        except Exception:
            count = 0
        if count > 0:
            rows.append((f, count))
            total += count
    return rows, total


def write_report(rows: list[tuple[Path, int]], report: Path) -> None:
    report.parent.mkdir(parents=True, exist_ok=True)
    lines = ["zip_path,urls"] + [f"{p},{c}" for p, c in rows]
    report.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    args = parse_args()
    rows, total = count_files(args.root)
    write_report(rows, args.report)
    print(f"nonzero files: {len(rows)}")
    print(f"total urls: {total}")
    print(f"written -> {args.report}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

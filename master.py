from __future__ import annotations

import argparse
from concurrent.futures import ProcessPoolExecutor, as_completed
import csv
import json
import logging
import re
import sys
import time
from datetime import datetime
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import urljoin, urlparse

from playwright.sync_api import Page, TimeoutError, sync_playwright


DEFAULT_START_URL = "https://carelistings.com/homecare/california"
PHONE_PATTERN = re.compile(
    r"(?:\+?1[\s.\-]?)?\(?\d{3}\)?[\s.\-]?\d{3}[\s.\-]?\d{4}"
)
PROFILE_ID_PATH_RE = re.compile(r"/[0-9a-f]{24}/?$", re.IGNORECASE)

LOG_PATH = Path("output/logs/scraper.log")


def _setup_logger() -> logging.Logger:
    logger = logging.getLogger("carelisting")
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    try:
        LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(LOG_PATH, encoding="utf-8")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    except Exception:
        # If file handler fails (e.g., permissions), keep console logging.
        pass

    return logger


LOGGER = _setup_logger()


FIELDS = ["zip_code", "listing_url", "name", "address", "phone", "agency_type", "about"]


def wait_for_page_ready(page: Page, timeout_ms: int = 45000) -> None:
    """Best-effort wait to ensure the page is fully loaded."""
    try:
        page.wait_for_load_state("domcontentloaded", timeout=timeout_ms)
    except Exception:
        LOGGER.debug("wait_for_load_state domcontentloaded timed out")
    try:
        page.wait_for_load_state("networkidle", timeout=timeout_ms)
    except Exception:
        LOGGER.debug("wait_for_load_state networkidle timed out")


def _load_zip_status(status_path: Path, zip_codes: list[str]) -> dict[str, set[str]]:
    """Load or initialize tracking for ZIP progress so reruns can pick up missed ZIPs."""
    processed: set[str] = set()
    failed: set[str] = set()
    pending: set[str] = set(zip_codes)
    try:
        data = json.loads(status_path.read_text(encoding="utf-8"))
        processed = set(data.get("processed", []))
        failed = set(data.get("failed", []))
        pending = set(data.get("pending", [])) or (set(zip_codes) - processed - failed)
    except Exception:
        pass
    # Ensure only ZIPs from the current run remain.
    processed &= set(zip_codes)
    failed &= set(zip_codes)
    pending = (set(zip_codes) - processed - failed) | (pending & set(zip_codes))
    return {"processed": processed, "failed": failed, "pending": pending}


def _save_zip_status(status_path: Path, status: dict[str, set[str]]) -> None:
    status_path.parent.mkdir(parents=True, exist_ok=True)
    data = {
        "processed": sorted(status["processed"]),
        "failed": sorted(status["failed"]),
        "pending": sorted(status["pending"]),
    }
    status_path.write_text(json.dumps(data, indent=2), encoding="utf-8")


@dataclass
class ListingRecord:
    zip_code: str
    listing_url: str
    name: str
    address: str
    phone: str
    agency_type: str
    about: str


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="CareListings scraper: collect URLs + scrape details (single-file)."
    )
    sub = parser.add_subparsers(dest="command", required=True)

    collect = sub.add_parser("collect", help="Collect agency profile URLs for ZIPs.")
    collect.add_argument("--zip-file", type=Path, required=True)
    collect.add_argument("--start-url", default=DEFAULT_START_URL)
    collect.add_argument("--urls-json", type=Path, default=Path("output/agency_urls.json"))
    collect.add_argument("--headful", action="store_true")
    collect.add_argument("--delay", type=float, default=0.8)
    collect.add_argument("--max-load-more-clicks", type=int, default=300)
    collect.add_argument("--before-zip-delay", type=float, default=1.0)
    collect.add_argument("--after-zip-delay", type=float, default=2.0)

    scrape = sub.add_parser("scrape", help="Scrape agency details from URLs JSON.")
    scrape.add_argument("--urls-json", type=Path, default=Path("output/agency_urls.json"))
    scrape.add_argument("--output-json", type=Path, default=Path("output/listings.json"))
    scrape.add_argument("--output-csv", type=Path, default=Path("output/listings.csv"))
    scrape.add_argument("--remaining-json", type=Path, default=Path("output/remaining.json"))
    scrape.add_argument("--headful", action="store_true")
    scrape.add_argument("--before-scrape-delay", type=float, default=2.0)
    scrape.add_argument("--page-settle-delay", type=float, default=2.0)
    scrape.add_argument("--after-scrape-delay", type=float, default=2.0)
    scrape.add_argument("--retries", type=int, default=3)
    scrape.add_argument("--retry-backoff", type=float, default=3.0)
    scrape.add_argument("--force", action="store_true")
    scrape.add_argument("--only-incomplete", action="store_true")
    scrape.add_argument("--limit", type=int, default=0)

    run_all = sub.add_parser("run-all", help="Collect + scrape in one command.")
    run_all.add_argument("--zip-file", type=Path, required=True)
    run_all.add_argument("--start-url", default=DEFAULT_START_URL)
    run_all.add_argument("--urls-json", type=Path, default=Path("output/agency_urls.json"))
    run_all.add_argument("--output-json", type=Path, default=Path("output/listings.json"))
    run_all.add_argument("--output-csv", type=Path, default=Path("output/listings.csv"))
    run_all.add_argument("--remaining-json", type=Path, default=Path("output/remaining.json"))
    run_all.add_argument("--headful", action="store_true")
    run_all.add_argument("--delay", type=float, default=0.8)
    run_all.add_argument("--max-load-more-clicks", type=int, default=300)
    run_all.add_argument("--before-zip-delay", type=float, default=1.0)
    run_all.add_argument("--after-zip-delay", type=float, default=2.0)
    run_all.add_argument("--before-scrape-delay", type=float, default=2.0)
    run_all.add_argument("--page-settle-delay", type=float, default=2.0)
    run_all.add_argument("--after-scrape-delay", type=float, default=2.0)
    run_all.add_argument("--retries", type=int, default=3)
    run_all.add_argument("--retry-backoff", type=float, default=3.0)
    run_all.add_argument("--force", action="store_true")
    run_all.add_argument("--only-incomplete", action="store_true")

    workflow = sub.add_parser(
        "workflow",
        help="Run collect -> scrape -> scrape-remaining automatically (loop).",
    )
    workflow.add_argument("--zip-file", type=Path, required=True)
    workflow.add_argument("--start-url", default=DEFAULT_START_URL)
    workflow.add_argument("--urls-json", type=Path, default=Path("output/agency_urls.json"))
    workflow.add_argument("--output-json", type=Path, default=Path("output/listings.json"))
    workflow.add_argument("--output-csv", type=Path, default=Path("output/listings.csv"))
    workflow.add_argument("--remaining-json", type=Path, default=Path("output/remaining.json"))
    workflow.add_argument("--headful", action="store_true")

    # Collection tuning
    workflow.add_argument("--delay", type=float, default=0.8)
    workflow.add_argument("--max-load-more-clicks", type=int, default=300)
    workflow.add_argument("--before-zip-delay", type=float, default=1.0)
    workflow.add_argument("--after-zip-delay", type=float, default=2.0)

    # Scrape tuning
    workflow.add_argument("--before-scrape-delay", type=float, default=2.0)
    workflow.add_argument("--page-settle-delay", type=float, default=2.0)
    workflow.add_argument("--after-scrape-delay", type=float, default=2.0)
    workflow.add_argument("--retries", type=int, default=3)
    workflow.add_argument("--retry-backoff", type=float, default=3.0)
    workflow.add_argument("--force", action="store_true")
    workflow.add_argument("--only-incomplete", action="store_true")

    # Remaining loop control
    workflow.add_argument(
        "--max-remaining-rounds",
        type=int,
        default=3,
        help="How many times to re-scrape remaining.json (0 = skip).",
    )
    workflow.add_argument(
        "--remaining-limit-per-round",
        type=int,
        default=0,
        help="Process only first N remaining items per round (0 = all).",
    )

    interactive = sub.add_parser(
        "interactive",
        help="Interactive mode: enter ZIPs, run workflow, write per-ZIP outputs.",
    )
    interactive.add_argument(
        "--output-root",
        type=Path,
        default=Path("output"),
        help="Root folder where per-ZIP folders are created.",
    )
    interactive.add_argument("--start-url", default=DEFAULT_START_URL)
    interactive.add_argument("--headful", action="store_true")
    interactive.add_argument("--delay", type=float, default=0.8)
    interactive.add_argument("--max-load-more-clicks", type=int, default=300)
    interactive.add_argument("--before-zip-delay", type=float, default=1.0)
    interactive.add_argument("--after-zip-delay", type=float, default=2.0)
    interactive.add_argument("--before-scrape-delay", type=float, default=2.0)
    interactive.add_argument("--page-settle-delay", type=float, default=2.0)
    interactive.add_argument("--after-scrape-delay", type=float, default=2.0)
    interactive.add_argument("--retries", type=int, default=3)
    interactive.add_argument("--retry-backoff", type=float, default=3.0)
    interactive.add_argument("--force", action="store_true")
    interactive.add_argument("--only-incomplete", action="store_true")
    interactive.add_argument("--max-remaining-rounds", type=int, default=3)
    interactive.add_argument("--remaining-limit-per-round", type=int, default=0)
    interactive.add_argument(
        "--parallel-zips",
        type=int,
        default=1,
        help="How many ZIP workflows to run in parallel (processes).",
    )

    scrape_remaining = sub.add_parser(
        "scrape-remaining",
        help="Re-scrape incomplete rows from remaining.json and merge into listings.",
    )
    scrape_remaining.add_argument(
        "--remaining-json", type=Path, default=Path("output/remaining.json")
    )
    scrape_remaining.add_argument(
        "--output-json", type=Path, default=Path("output/listings.json")
    )
    scrape_remaining.add_argument("--output-csv", type=Path, default=Path("output/listings.csv"))
    scrape_remaining.add_argument("--headful", action="store_true")
    scrape_remaining.add_argument("--before-scrape-delay", type=float, default=2.0)
    scrape_remaining.add_argument("--page-settle-delay", type=float, default=2.0)
    scrape_remaining.add_argument("--after-scrape-delay", type=float, default=2.0)
    scrape_remaining.add_argument("--retries", type=int, default=3)
    scrape_remaining.add_argument("--retry-backoff", type=float, default=3.0)
    scrape_remaining.add_argument("--limit", type=int, default=0)

    build_remaining = sub.add_parser(
        "build-remaining", help="Create remaining.json from output/listings.json"
    )
    build_remaining.add_argument(
        "--output-json", type=Path, default=Path("output/listings.json")
    )
    build_remaining.add_argument(
        "--remaining-json", type=Path, default=Path("output/remaining.json")
    )

    return parser.parse_args(argv)


def read_zip_codes(path: Path) -> list[str]:
    if not path.exists():
        raise FileNotFoundError(f"ZIP file not found: {path}")

    zip_codes: list[str] = []
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if "," in line:
            candidate = line.split(",")[0].strip()
        else:
            candidate = line
        digits = re.sub(r"\D", "", candidate)
        if len(digits) >= 5:
            zip_codes.append(digits[:5])
    if not zip_codes:
        raise ValueError("No valid ZIP codes found in file.")
    return _dedupe_preserve_order(zip_codes)


def _normalize_zip(zip_code: str) -> str:
    digits = re.sub(r"\D", "", zip_code or "")
    return digits[:5] if len(digits) >= 5 else ""


def _dedupe_preserve_order(items: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for x in items:
        if x in seen:
            continue
        seen.add(x)
        out.append(x)
    return out


def _read_zip_codes_from_any_file(path: Path) -> list[str]:
    if not path.exists():
        raise FileNotFoundError(path)

    zips: list[str] = []
    if path.suffix.lower() == ".csv":
        # Try to locate a "zip" column; otherwise fall back to scanning all cells.
        with path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            if reader.fieldnames:
                zip_cols = [c for c in reader.fieldnames if "zip" in c.lower()]
            else:
                zip_cols = []

            if zip_cols:
                zip_col = zip_cols[0]
                for row in reader:
                    z = _normalize_zip(str(row.get(zip_col) or ""))
                    if z:
                        zips.append(z)
            else:
                # Fallback: scan each cell and take the first ZIP-looking value per row.
                f.seek(0)
                cell_reader = csv.reader(f)
                for row in cell_reader:
                    for cell in row:
                        z = _normalize_zip(cell)
                        if z:
                            zips.append(z)
                            break
    else:
        # Treat as a text-ish file: one ZIP per line, or ZIP first column.
        for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
            if not line.strip():
                continue
            first = line.split(",")[0].strip()
            z = _normalize_zip(first)
            if z:
                zips.append(z)

    return _dedupe_preserve_order(zips)


def _prompt_how_many(max_count: int) -> int:
    while True:
        raw = input(f"How many ZIPs do you want to scrape? (1-{max_count}, Enter=all): ").strip()
        if raw == "":
            return max_count
        try:
            n = int(raw)
            if 1 <= n <= max_count:
                return n
            print(f"Enter a number between 1 and {max_count}.", flush=True)
        except ValueError:
            print("Enter a valid integer.", flush=True)


def _prompt_zip_codes() -> list[str]:
    print("ZIP input:", flush=True)
    print("  1) Load ZIPs from a file (csv/txt)", flush=True)
    print("  2) Enter ZIPs manually", flush=True)
    while True:
        choice = input("Select (1/2): ").strip()
        if choice in ("1", "2"):
            break
        print("Please enter 1 or 2.", flush=True)

    if choice == "1":
        while True:
            raw_path = input("Path to ZIP file (e.g. Book2.csv): ").strip().strip('"')
            if not raw_path:
                print("Enter a file path.", flush=True)
                continue
            path = Path(raw_path)
            if not path.exists():
                print(f"File not found: {path}", flush=True)
                continue
            zips = _read_zip_codes_from_any_file(path)
            if not zips:
                print("No ZIP codes found in that file.", flush=True)
                continue
            print(f"Found {len(zips)} ZIP codes in file.", flush=True)
            n = _prompt_how_many(len(zips))
            return zips[:n]

    # Manual entry
    while True:
        raw = input("How many ZIP codes do you want to enter? ").strip()
        try:
            n = int(raw)
            if n <= 0:
                print("Enter a number >= 1.", flush=True)
                continue
            break
        except ValueError:
            print("Enter a valid integer.", flush=True)

    zips: list[str] = []
    for i in range(1, n + 1):
        while True:
            z = _normalize_zip(input(f"ZIP #{i}: ").strip())
            if not z:
                print("Invalid ZIP. Example: 95128", flush=True)
                continue
            zips.append(z)
            break

    return _dedupe_preserve_order(zips)


def _prompt_parallel_workers(default_value: int = 1, max_value: int = 8) -> int:
    while True:
        raw = input(
            f"How many ZIPs to run in parallel? (1-{max_value}, Enter={default_value}): "
        ).strip()
        if raw == "":
            return max(1, min(default_value, max_value))
        try:
            n = int(raw)
            if 1 <= n <= max_value:
                return n
            print(f"Enter a number between 1 and {max_value}.", flush=True)
        except ValueError:
            print("Enter a valid integer.", flush=True)


def _is_blank(v: Any) -> bool:
    return v is None or (isinstance(v, str) and v.strip() == "")


def _is_complete_dict(obj: dict[str, Any]) -> bool:
    return all(not _is_blank(obj.get(k)) for k in ("name", "address", "phone", "agency_type", "about"))


def _is_complete_record(rec: ListingRecord) -> bool:
    return (
        not _is_blank(rec.name)
        and not _is_blank(rec.address)
        and not _is_blank(rec.phone)
        and not _is_blank(rec.agency_type)
        and not _is_blank(rec.about)
    )


def _merge_record(existing: ListingRecord, new: ListingRecord) -> ListingRecord:
    def pick(old: str, fresh: str) -> str:
        return old if not _is_blank(old) else fresh

    return ListingRecord(
        zip_code=pick(existing.zip_code, new.zip_code),
        listing_url=existing.listing_url or new.listing_url,
        name=pick(existing.name, new.name),
        address=pick(existing.address, new.address),
        phone=pick(existing.phone, new.phone),
        agency_type=pick(existing.agency_type, new.agency_type),
        about=pick(existing.about, new.about),
    )


def _merge_fill_dict(old: dict[str, Any], new: dict[str, Any]) -> dict[str, Any]:
    merged = dict(old)
    for k in FIELDS:
        if k not in new:
            continue
        if _is_blank(merged.get(k)) and not _is_blank(new.get(k)):
            merged[k] = new[k]
    return merged


def _read_json_list(path: Path) -> list[Any]:
    if not path.exists():
        return []
    raw = path.read_text(encoding="utf-8")
    if not raw.strip():
        return []
    try:
        obj = json.loads(raw)
    except json.JSONDecodeError:
        # Keep corrupt contents around, but don't crash.
        try:
            path.with_suffix(path.suffix + ".corrupt").write_text(raw, encoding="utf-8")
        except Exception:
            pass
        return []
    if not isinstance(obj, list):
        return []
    return obj


def _write_json_list(path: Path, items: list[Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(items, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def _format_eta(seconds: float) -> str:
    s = max(0, int(seconds))
    h, rem = divmod(s, 3600)
    m, sec = divmod(rem, 60)
    if h > 0:
        return f"{h:02d}:{m:02d}:{sec:02d}"
    return f"{m:02d}:{sec:02d}"


def write_outputs(records: list[ListingRecord], csv_path: Path, json_path: Path) -> None:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.parent.mkdir(parents=True, exist_ok=True)

    tmp_csv = csv_path.with_suffix(csv_path.suffix + ".tmp")
    tmp_json = json_path.with_suffix(json_path.suffix + ".tmp")

    with tmp_csv.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        writer.writeheader()
        for record in records:
            writer.writerow(asdict(record))
    tmp_csv.replace(csv_path)

    with tmp_json.open("w", encoding="utf-8") as f:
        json.dump([asdict(r) for r in records], f, ensure_ascii=False, indent=2)
    tmp_json.replace(json_path)


def first_visible(page: Page, selectors: Iterable[str]):
    for selector in selectors:
        locator = page.locator(selector).first
        if locator.count() > 0 and locator.is_visible():
            return locator
    return None


def fill_zip_and_search(page: Page, zip_code: str, timeout_ms: int = 20000) -> None:
    input_selectors = [
        "input[placeholder*='Zip' i]",
        "input[placeholder*='ZIP' i]",
        "input[placeholder*='zipcode' i]",
        "input[placeholder*='Enter ZIP' i]",
        "input[aria-label*='zip' i]",
        "input[name*='zip' i]",
        "input[name='zipcode']",
        "input[name='zipCode']",
        "input[id*='zip' i]",
        "input[data-testid*='zip' i]",
        "[role='searchbox']",
        "[role='combobox'][aria-label*='zip' i]",
        "input[type='search']",
        "input[type='text']",
    ]
    try:
        page.wait_for_selector(", ".join(input_selectors), timeout=timeout_ms)
    except Exception:
        # Proceed to scan manually; this may happen if the page layout changed.
        LOGGER.warning("zip input wait_for_selector timed out, scanning manually")

    zip_input = first_visible(page, input_selectors)
    if zip_input is None:
        # Detect common block pages (e.g., Cloudflare).
        block_markers = page.locator("text=cloudflare, text=Attention Required, text=Please enable JavaScript").count()
        if block_markers > 0:
            raise RuntimeError("Blocked or challenge page detected (e.g., Cloudflare).")
        raise RuntimeError("Could not find ZIP search input.")

    zip_input.click()
    zip_input.fill(zip_code)

    button_selectors = [
        "button:has-text('Search')",
        "button:has-text('search')",
        "input[type='submit']",
        "[role='button']:has-text('Search')",
    ]
    search_button = first_visible(page, button_selectors)
    if search_button is not None:
        search_button.click()
    else:
        zip_input.press("Enter")

    # Results might be empty (0 agencies). In that case the page still renders a
    # heading like "0 Homecare Agencies in 00603" but no "View Listing" links.
    page.wait_for_selector(
        "#page-heading, a:has-text('View Listing'), #load-more-btn, button:has-text('Load More')",
        timeout=timeout_ms,
    )


def get_results_count_from_heading(page: Page) -> int | None:
    heading = page.locator("#page-heading").first
    if heading.count() == 0:
        return None
    text = " ".join(heading.inner_text().split())
    # Example: "0 Homecare Agencies in 00603"
    m = re.search(r"^\s*(\d+)\s+Homecare Agencies\b", text, flags=re.IGNORECASE)
    if not m:
        return None
    try:
        return int(m.group(1))
    except ValueError:
        return None


def click_load_more_until_done(page: Page, delay_seconds: float, max_clicks: int) -> int:
    clicks = 0
    while clicks < max_clicks:
        button = page.locator(
            "#load-more-btn, button:has-text('Load More Results'), button:has-text('Load More')"
        ).first
        if button.count() == 0 or not button.is_visible():
            break

        try:
            button.scroll_into_view_if_needed(timeout=5000)
        except Exception:
            pass

        before = page.locator("a:has-text('View Listing')").count()
        try:
            button.click(timeout=5000)
        except Exception:
            break

        # Wait for more results to appear (or button disappears).
        start = time.time()
        increased = False
        while (time.time() - start) < 12.0:
            page.wait_for_timeout(250)
            now = page.locator("a:has-text('View Listing')").count()
            if now > before:
                increased = True
                break
            if button.count() == 0 or not button.is_visible():
                break
        if not increased:
            break

        clicks += 1
        page.wait_for_timeout(int(delay_seconds * 1000))
    return clicks


def is_agency_profile_url(url: str) -> bool:
    try:
        path = urlparse(url).path or ""
    except Exception:
        return False
    return bool(PROFILE_ID_PATH_RE.search(path))


def collect_listing_urls(page: Page, start_url: str) -> list[str]:
    urls: list[str] = []
    seen: set[str] = set()

    for selector in ("a:has-text('View Listing')", "a[href*='/homecare/']"):
        locator = page.locator(selector)
        for i in range(locator.count()):
            href = locator.nth(i).get_attribute("href")
            if not href:
                continue
            absolute = urljoin(start_url, href)
            if not is_agency_profile_url(absolute):
                continue
            if absolute in seen:
                continue
            seen.add(absolute)
            urls.append(absolute)
    return urls


def reveal_phone_if_needed(page: Page) -> None:
    reveal = first_visible(
        page,
        [
            ".show-phone-btn-profile",
            ".show-phone-btn-mobile",
            "button:has-text('Show Phone')",
            "[role='button']:has-text('Show Phone')",
            "a:has-text('Show Phone')",
        ],
    )
    if reveal is not None:
        try:
            reveal.scroll_into_view_if_needed(timeout=5000)
        except Exception:
            pass
        reveal.click()
        page.wait_for_timeout(350)


def extract_phone(page: Page) -> str:
    # Some profiles store the phone in data attributes (even before clicking "Show Phone").
    data_phone_loc = page.locator("[data-phone]").first
    if data_phone_loc.count() > 0:
        attr = (data_phone_loc.get_attribute("data-phone") or "").strip()
        if attr:
            return attr

    for selector in (".show-phone-btn-profile", ".show-phone-btn-mobile"):
        phone_node = page.locator(selector).first
        if phone_node.count() > 0:
            data_phone = (phone_node.get_attribute("data-phone") or "").strip()
            if data_phone:
                return data_phone

    tel_link = page.locator("a[href^='tel:']").first
    if tel_link.count() > 0:
        text = tel_link.inner_text().strip()
        if text:
            return text
        href = tel_link.get_attribute("href") or ""
        return href.replace("tel:", "").strip()

    body_text = page.inner_text("body")
    match = PHONE_PATTERN.search(body_text)
    return match.group(0) if match else ""


def extract_text_near_label(page: Page, label: str) -> str:
    patterns = [
        f"xpath=//*[self::h2 or self::h3 or self::strong or self::span][contains(translate(normalize-space(.), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), '{label.lower()}')]/following::*[1]",
        f"xpath=//*[contains(translate(normalize-space(.), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), '{label.lower()}')][1]/following::*[1]",
    ]
    for pattern in patterns:
        loc = page.locator(pattern).first
        if loc.count() > 0:
            text = " ".join(loc.inner_text().split())
            if text:
                return text
    return ""


def extract_about_text(page: Page) -> str:
    # Best-case: the modern profile pages use a dedicated About container.
    about_container = page.locator(".about-section-container .about-description-modern").first
    if about_container.count() > 0:
        paras = about_container.locator("p")
        parts: list[str] = []
        if paras.count() > 0:
            for i in range(min(paras.count(), 20)):
                t = " ".join(paras.nth(i).inner_text().split())
                if t:
                    parts.append(t)
        else:
            t = " ".join(about_container.inner_text().split())
            if t:
                parts.append(t)

        # Keep paragraph boundaries but avoid scraping unrelated map/controls text.
        joined = "\n\n".join(parts).strip()
        if joined:
            return joined

    # Fallback: find an "About" header and take the first meaningful paragraph after it.
    about_root = page.locator(
        "xpath=//*[self::h2 or self::h3 or self::a or self::div][contains(translate(normalize-space(.), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'about')]"
    ).first
    if about_root.count() > 0:
        section_paras = about_root.locator("xpath=following::p")
        for i in range(min(section_paras.count(), 10)):
            text = " ".join(section_paras.nth(i).inner_text().split())
            if len(text) > 80:
                return text

    # Last-resort heuristic.
    candidates = page.locator("p")
    for i in range(min(candidates.count(), 200)):
        text = " ".join(candidates.nth(i).inner_text().split())
        if len(text) > 140 and "homecare" in text.lower():
            return text
    return ""


def scrape_listing_reuse_page(
    page: Page,
    listing_url: str,
    zip_code: str,
    page_settle_delay: float,
) -> ListingRecord:
    page.goto(listing_url, wait_until="load", timeout=45000)
    wait_for_page_ready(page)
    # Ensure the main profile UI is present; if not, treat as a soft failure.
    try:
        page.wait_for_selector(".profile-header-modern, h1", timeout=15000)
    except Exception:
        pass
    if page_settle_delay > 0:
        page.wait_for_timeout(int(page_settle_delay * 1000))
    reveal_phone_if_needed(page)

    name = ""
    for selector in (
        ".profile-header-modern h1.profile-title-modern",
        "h1.profile-title-modern",
        "h1",
        "h2",
        "[itemprop='name']",
    ):
        loc = page.locator(selector).first
        if loc.count() > 0:
            name = " ".join(loc.inner_text().split())
            if name:
                break

    address = ""
    profile_address = page.locator(
        ".profile-header-modern .profile-info-item div[style*='line-height']"
    ).first
    if profile_address.count() > 0:
        address = " ".join(profile_address.inner_text().split())
    if not address:
        first_info_item = page.locator(".profile-header-modern .profile-info-item").first
        if first_info_item.count() > 0:
            address = " ".join(first_info_item.inner_text().split())
    addr_loc = page.locator("[itemprop='address']").first
    if not address and addr_loc.count() > 0:
        address = " ".join(addr_loc.inner_text().split())
    if not address:
        address = extract_text_near_label(page, "address")

    agency_type = ""
    subtitle = page.locator(".profile-subtitle-modern").first
    if subtitle.count() > 0:
        agency_type = " ".join(subtitle.inner_text().split())
    if not agency_type:
        agency_type = extract_text_near_label(page, "agency type")
    if not agency_type:
        agency_type = extract_text_near_label(page, "type")

    about = extract_about_text(page)
    phone = extract_phone(page)

    return ListingRecord(
        zip_code=zip_code,
        listing_url=listing_url,
        name=name,
        address=address,
        phone=phone,
        agency_type=agency_type,
        about=about,
    )


def cmd_collect(args: argparse.Namespace) -> int:
    zip_codes = read_zip_codes(args.zip_file)

    existing = _read_json_list(args.urls_json)
    collected: list[dict[str, Any]] = [x for x in existing if isinstance(x, dict)]
    seen_urls: set[str] = {str(x.get("listing_url")) for x in collected if x.get("listing_url")}

    out: list[dict[str, Any]] = []
    for it in collected:
        url = str(it.get("listing_url") or "").strip()
        if not url or not is_agency_profile_url(url):
            continue
        out.append({"zip_code": str(it.get("zip_code") or ""), "listing_url": url})
    collected = out

    zip_status_path = args.urls_json.parent / "zip_status.json"
    zip_status = _load_zip_status(zip_status_path, zip_codes)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=not args.headful)
        context = browser.new_context()
        page = context.new_page()
        page.goto(args.start_url, wait_until="load", timeout=45000)
        wait_for_page_ready(page)
        started_at = time.time()
        total_zips = len(zip_codes)

        for idx, zip_code in enumerate(zip_codes, start=1):
            if args.before_zip_delay > 0:
                time.sleep(args.before_zip_delay)

            print(f"[{idx}/{len(zip_codes)}] collecting URLs for ZIP {zip_code}...", flush=True)
            success = False
            try:
                fill_zip_and_search(page, zip_code)
                result_count = get_results_count_from_heading(page)
                if result_count == 0:
                    # No listings for this ZIP.
                    urls: list[str] = []
                    click_count = 0
                else:
                    click_count = click_load_more_until_done(
                        page=page,
                        delay_seconds=args.delay,
                        max_clicks=args.max_load_more_clicks,
                    )
                    urls = collect_listing_urls(page, args.start_url)
                new_count = 0
                for u in urls:
                    if u in seen_urls:
                        continue
                    seen_urls.add(u)
                    collected.append({"zip_code": zip_code, "listing_url": u})
                    new_count += 1
                # Persist after each ZIP.
                _write_json_list(args.urls_json, collected)
                print(
                    f"  found={len(urls)} new={new_count} total_saved={len(collected)} "
                    f"(load-more clicks: {click_count})",
                    flush=True,
                )
                success = True
            except KeyboardInterrupt:
                # Always keep what we already collected.
                _write_json_list(args.urls_json, collected)
                print("Interrupted. Saved collected URLs so far.", flush=True)
                break
            except TimeoutError:
                print("  timeout collecting URLs, skipping ZIP", flush=True)
                LOGGER.warning("collect timeout zip=%s url=%s", zip_code, args.start_url)
            except Exception as exc:
                print(f"  failed collecting URLs: {exc}", flush=True)
                LOGGER.exception("collect failed zip=%s url=%s error=%s", zip_code, args.start_url, exc)
            finally:
                # Persist even if the ZIP failed mid-way (best-effort).
                _write_json_list(args.urls_json, collected)
                page.goto(args.start_url, wait_until="load", timeout=45000)
                wait_for_page_ready(page)
                # Update ZIP status for reruns.
                zip_status["pending"].discard(zip_code)
                if success:
                    zip_status["processed"].add(zip_code)
                else:
                    zip_status["failed"].add(zip_code)
                _save_zip_status(zip_status_path, zip_status)
                proc_count = len(zip_status["processed"])
                fail_count = len(zip_status["failed"])
                pend_count = len(zip_status["pending"])
                print(
                    f"  zip status -> processed={proc_count} failed={fail_count} pending={pend_count}",
                    flush=True,
                )
                elapsed = time.time() - started_at
                left = max(0, total_zips - idx)
                avg_per_zip = elapsed / idx if idx else 0
                eta = avg_per_zip * left
                print(
                    f"  progress: {idx}/{total_zips} | left={left} | eta={_format_eta(eta)}",
                    flush=True,
                )
                if args.after_zip_delay > 0:
                    time.sleep(args.after_zip_delay)

        browser.close()

    print(f"Done. Saved {len(collected)} listing URLs -> {args.urls_json}", flush=True)
    return 0


def cmd_collect_zip_codes(
    zip_codes: list[str],
    *,
    start_url: str,
    urls_json: Path,
    headful: bool,
    delay: float,
    max_load_more_clicks: int,
    before_zip_delay: float,
    after_zip_delay: float,
) -> int:
    # Wrapper for interactive mode so we don't need to create a ZIP file.
    tmp_zip_file = urls_json.parent / "_zip_codes.tmp.txt"
    tmp_zip_file.parent.mkdir(parents=True, exist_ok=True)
    tmp_zip_file.write_text("\n".join(zip_codes) + "\n", encoding="utf-8")
    try:
        return cmd_collect(
            argparse.Namespace(
                zip_file=tmp_zip_file,
                start_url=start_url,
                urls_json=urls_json,
                headful=headful,
                delay=delay,
                max_load_more_clicks=max_load_more_clicks,
                before_zip_delay=before_zip_delay,
                after_zip_delay=after_zip_delay,
            )
        )
    finally:
        try:
            tmp_zip_file.unlink(missing_ok=True)  # py3.12+
        except Exception:
            pass


def cmd_scrape(args: argparse.Namespace) -> int:
    url_items_raw = _read_json_list(args.urls_json)
    url_items: list[dict[str, Any]] = [x for x in url_items_raw if isinstance(x, dict)]
    if args.limit and args.limit > 0:
        url_items = url_items[: args.limit]

    existing_raw = _read_json_list(args.output_json)
    existing_dicts: list[dict[str, Any]] = [x for x in existing_raw if isinstance(x, dict)]
    all_by_url: dict[str, ListingRecord] = {}
    for x in existing_dicts:
        try:
            rec = ListingRecord(
                zip_code=str(x.get("zip_code") or ""),
                listing_url=str(x.get("listing_url") or ""),
                name=str(x.get("name") or ""),
                address=str(x.get("address") or ""),
                phone=str(x.get("phone") or ""),
                agency_type=str(x.get("agency_type") or ""),
                about=str(x.get("about") or ""),
            )
            if rec.listing_url:
                all_by_url[rec.listing_url] = rec
        except Exception:
            continue

    remaining: list[dict[str, Any]] = []
    processed = 0
    newly_added = 0
    updated = 0
    skipped_complete = 0
    skipped_existing = 0

    targets: list[tuple[str, str]] = []
    for it in url_items:
        url = str(it.get("listing_url") or "").strip()
        zip_code = str(it.get("zip_code") or "").strip()
        if not url or not is_agency_profile_url(url):
            continue
        existing_rec = all_by_url.get(url)
        if existing_rec is not None and args.only_incomplete and _is_complete_record(existing_rec):
            skipped_complete += 1
            continue
        if existing_rec is not None and not args.force and not args.only_incomplete:
            skipped_existing += 1
            continue
        targets.append((url, zip_code))

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=not args.headful)
        context = browser.new_context()
        detail_page = context.new_page()
        started_at = time.time()
        total = len(targets)

        for idx, (url, zip_code) in enumerate(targets, start=1):
            existing_rec = all_by_url.get(url)

            if args.before_scrape_delay > 0:
                time.sleep(args.before_scrape_delay)

            left = max(0, total - idx)
            elapsed = time.time() - started_at
            avg = elapsed / idx if idx else 0
            eta = avg * left
            print(
                f"[{idx}/{total}] scraping {url} | left={left} | eta={_format_eta(eta)}",
                flush=True,
            )
            processed += 1

            record: ListingRecord | None = None
            for attempt in range(1, max(args.retries, 1) + 1):
                try:
                    record = scrape_listing_reuse_page(
                        page=detail_page,
                        listing_url=url,
                        zip_code=zip_code,
                        page_settle_delay=args.page_settle_delay,
                    )
                    break
                except KeyboardInterrupt:
                    # Persist current progress and exit.
                    write_outputs(list(all_by_url.values()), args.output_csv, args.output_json)
                    _write_json_list(args.remaining_json, remaining)
                    print("Interrupted. Saved progress so far.", flush=True)
                    browser.close()
                    return 130
                except TimeoutError:
                    if attempt < args.retries:
                        time.sleep(args.retry_backoff * attempt)
                        continue
                    print(f"  timeout: {url}", flush=True)
                    LOGGER.warning("scrape timeout url=%s zip=%s attempt=%s", url, zip_code, attempt)
                    record = None
                except Exception as exc:
                    if attempt < args.retries:
                        time.sleep(args.retry_backoff * attempt)
                        continue
                    print(f"  error: {type(exc).__name__}: {exc}", flush=True)
                    LOGGER.exception("scrape error url=%s zip=%s attempt=%s error=%s", url, zip_code, attempt, exc)
                    record = None

            if record is None:
                remaining.append(
                    {
                        "zip_code": zip_code,
                        "listing_url": url,
                        "name": "",
                        "address": "",
                        "phone": "",
                        "agency_type": "",
                        "about": "",
                    }
                )
            else:
                if existing_rec is None:
                    all_by_url[url] = record
                    newly_added += 1
                else:
                    merged = _merge_record(existing_rec, record)
                    if asdict(merged) != asdict(existing_rec):
                        updated += 1
                    all_by_url[url] = merged

                if not _is_complete_record(all_by_url[url]):
                    remaining.append(asdict(all_by_url[url]))

            # Persist after each URL.
            write_outputs(list(all_by_url.values()), args.output_csv, args.output_json)
            _write_json_list(args.remaining_json, remaining)

            if args.after_scrape_delay > 0:
                time.sleep(args.after_scrape_delay)

        browser.close()

    print(
        "Done. "
        f"processed={processed} new={newly_added} updated={updated} "
        f"skipped_complete={skipped_complete} skipped_existing={skipped_existing} "
        f"total_saved={len(all_by_url)} remaining={len(remaining)} "
        f"-> {args.output_json} + {args.output_csv}",
        flush=True,
    )
    return 0


def cmd_build_remaining(args: argparse.Namespace) -> int:
    data_raw = _read_json_list(args.output_json)
    data: list[dict[str, Any]] = [x for x in data_raw if isinstance(x, dict)]
    remaining = [r for r in data if any(_is_blank(r.get(k)) for k in ("name", "address", "phone", "agency_type", "about"))]
    _write_json_list(args.remaining_json, remaining)
    print(f"total={len(data)} remaining={len(remaining)} -> {args.remaining_json}", flush=True)
    return 0


def cmd_scrape_remaining(args: argparse.Namespace) -> int:
    remaining_raw = _read_json_list(args.remaining_json)
    remaining_items: list[dict[str, Any]] = [x for x in remaining_raw if isinstance(x, dict)]
    if args.limit and args.limit > 0:
        remaining_items = remaining_items[: args.limit]

    listings_raw = _read_json_list(args.output_json)
    listings: list[dict[str, Any]] = [x for x in listings_raw if isinstance(x, dict)]

    completed_by_url: dict[str, dict[str, Any]] = {
        str(it.get("listing_url")): it for it in listings if it.get("listing_url")
    }
    still_remaining: list[dict[str, Any]] = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=not args.headful)
        context = browser.new_context()
        detail_page = context.new_page()
        started_at = time.time()
        total = len(remaining_items)

        for idx, item in enumerate(remaining_items, start=1):
            url = str(item.get("listing_url") or "").strip()
            zip_code = str(item.get("zip_code") or "").strip()
            if not url:
                continue

            if args.before_scrape_delay > 0:
                time.sleep(args.before_scrape_delay)
            left = max(0, total - idx)
            elapsed = time.time() - started_at
            avg = elapsed / idx if idx else 0
            eta = avg * left
            print(
                f"[{idx}/{total}] retrying {url} | left={left} | eta={_format_eta(eta)}",
                flush=True,
            )

            merged = dict(item)
            success = False
            for attempt in range(1, max(args.retries, 1) + 1):
                try:
                    rec = scrape_listing_reuse_page(
                        page=detail_page,
                        listing_url=url,
                        zip_code=zip_code,
                        page_settle_delay=args.page_settle_delay,
                    )
                    merged = _merge_fill_dict(merged, asdict(rec))
                    if _is_complete_dict(merged):
                        success = True
                    break
                except KeyboardInterrupt:
                    # Persist whatever we have so far and exit.
                    merged_listings = list(completed_by_url.values())
                    records: list[ListingRecord] = []
                    for x in merged_listings:
                        try:
                            records.append(
                                ListingRecord(
                                    zip_code=str(x.get("zip_code") or ""),
                                    listing_url=str(x.get("listing_url") or ""),
                                    name=str(x.get("name") or ""),
                                    address=str(x.get("address") or ""),
                                    phone=str(x.get("phone") or ""),
                                    agency_type=str(x.get("agency_type") or ""),
                                    about=str(x.get("about") or ""),
                                )
                            )
                        except Exception:
                            continue
                    write_outputs(records, args.output_csv, args.output_json)
                    _write_json_list(args.remaining_json, still_remaining)
                    print("Interrupted. Saved progress so far.", flush=True)
                    browser.close()
                    return 130
                except TimeoutError:
                    if attempt < args.retries:
                        time.sleep(args.retry_backoff * attempt)
                        continue
                    print(f"  timeout: {url}", flush=True)
                    LOGGER.warning("scrape-remaining timeout url=%s zip=%s attempt=%s", url, zip_code, attempt)
                    break
                except Exception as exc:
                    if attempt < args.retries:
                        time.sleep(args.retry_backoff * attempt)
                        continue
                    print(f"  error: {type(exc).__name__}: {exc}", flush=True)
                    LOGGER.exception("scrape-remaining error url=%s zip=%s attempt=%s error=%s", url, zip_code, attempt, exc)
                    break

            if success and _is_complete_dict(merged):
                completed_by_url[url] = merged
                print(f"[{idx}/{len(remaining_items)}] completed: {url}", flush=True)
            else:
                still_remaining.append(merged)
                print(f"[{idx}/{len(remaining_items)}] remaining: {url}", flush=True)

            # Persist after each item.
            merged_listings = list(completed_by_url.values())
            # Rewrite listings.json/csv using ListingRecord conversion (best-effort).
            records: list[ListingRecord] = []
            for x in merged_listings:
                try:
                    records.append(
                        ListingRecord(
                            zip_code=str(x.get("zip_code") or ""),
                            listing_url=str(x.get("listing_url") or ""),
                            name=str(x.get("name") or ""),
                            address=str(x.get("address") or ""),
                            phone=str(x.get("phone") or ""),
                            agency_type=str(x.get("agency_type") or ""),
                            about=str(x.get("about") or ""),
                        )
                    )
                except Exception:
                    continue
            write_outputs(records, args.output_csv, args.output_json)
            _write_json_list(args.remaining_json, still_remaining)

            if args.after_scrape_delay > 0:
                time.sleep(args.after_scrape_delay)

        browser.close()

    print(
        f"Done. listings={len(completed_by_url)} remaining={len(still_remaining)} "
        f"-> {args.output_json} + {args.remaining_json}",
        flush=True,
    )
    return 0


def cmd_run_all(args: argparse.Namespace) -> int:
    rc = cmd_collect(
        argparse.Namespace(
            zip_file=args.zip_file,
            start_url=args.start_url,
            urls_json=args.urls_json,
            headful=args.headful,
            delay=args.delay,
            max_load_more_clicks=args.max_load_more_clicks,
            before_zip_delay=args.before_zip_delay,
            after_zip_delay=args.after_zip_delay,
        )
    )
    if rc != 0:
        return rc
    return cmd_scrape(
        argparse.Namespace(
            urls_json=args.urls_json,
            output_json=args.output_json,
            output_csv=args.output_csv,
            remaining_json=args.remaining_json,
            headful=args.headful,
            before_scrape_delay=args.before_scrape_delay,
            page_settle_delay=args.page_settle_delay,
            after_scrape_delay=args.after_scrape_delay,
            retries=args.retries,
            retry_backoff=args.retry_backoff,
            force=args.force,
            only_incomplete=args.only_incomplete,
            limit=0,
        )
    )


def _count_remaining(path: Path) -> int:
    items = _read_json_list(path)
    return len([x for x in items if isinstance(x, dict)])


def cmd_workflow(args: argparse.Namespace) -> int:
    # Step 1: Collect listing URLs
    rc = cmd_collect(
        argparse.Namespace(
            zip_file=args.zip_file,
            start_url=args.start_url,
            urls_json=args.urls_json,
            headful=args.headful,
            delay=args.delay,
            max_load_more_clicks=args.max_load_more_clicks,
            before_zip_delay=args.before_zip_delay,
            after_zip_delay=args.after_zip_delay,
        )
    )
    if rc != 0:
        return rc

    # Step 2: Scrape details from collected URLs
    rc = cmd_scrape(
        argparse.Namespace(
            urls_json=args.urls_json,
            output_json=args.output_json,
            output_csv=args.output_csv,
            remaining_json=args.remaining_json,
            headful=args.headful,
            before_scrape_delay=args.before_scrape_delay,
            page_settle_delay=args.page_settle_delay,
            after_scrape_delay=args.after_scrape_delay,
            retries=args.retries,
            retry_backoff=args.retry_backoff,
            force=args.force,
            only_incomplete=args.only_incomplete,
            limit=0,
        )
    )
    if rc != 0:
        return rc

    # Step 3: Re-scrape remaining in rounds (optional).
    if args.max_remaining_rounds <= 0:
        return 0

    prev_remaining = _count_remaining(args.remaining_json)
    for round_idx in range(1, args.max_remaining_rounds + 1):
        if prev_remaining <= 0:
            break
        print(
            f"Remaining round {round_idx}/{args.max_remaining_rounds} (items={prev_remaining})",
            flush=True,
        )
        rc = cmd_scrape_remaining(
            argparse.Namespace(
                remaining_json=args.remaining_json,
                output_json=args.output_json,
                output_csv=args.output_csv,
                headful=args.headful,
                before_scrape_delay=args.before_scrape_delay,
                page_settle_delay=args.page_settle_delay,
                after_scrape_delay=args.after_scrape_delay,
                retries=args.retries,
                retry_backoff=args.retry_backoff,
                limit=args.remaining_limit_per_round,
            )
        )
        if rc != 0:
            return rc

        now_remaining = _count_remaining(args.remaining_json)
        if now_remaining >= prev_remaining:
            # No progress; stop to avoid looping forever.
            print(
                f"No remaining progress (still {now_remaining}). Stopping rounds.",
                flush=True,
            )
            break
        prev_remaining = now_remaining

    return 0


def cmd_interactive(args: argparse.Namespace) -> int:
    zip_codes = _prompt_zip_codes()
    if not zip_codes:
        print("No ZIP codes entered.", flush=True)
        return 1

    # Ask parallelism at runtime so users don't need CLI flags.
    max_parallel = min(8, max(1, len(zip_codes)))
    args.parallel_zips = _prompt_parallel_workers(
        default_value=max(1, int(args.parallel_zips)),
        max_value=max_parallel,
    )

    print(f"ZIPs: {', '.join(zip_codes)}", flush=True)

    parallel = max(1, int(args.parallel_zips))
    if parallel == 1 or len(zip_codes) == 1:
        for idx, zip_code in enumerate(zip_codes, start=1):
            rc = _run_single_zip_workflow(zip_code, args, idx=idx, total=len(zip_codes))
            if rc != 0:
                return rc
    else:
        print(
            f"Running in parallel mode: {parallel} ZIPs at a time",
            flush=True,
        )
        worker_cfg = _to_worker_config(args)
        try:
            with ProcessPoolExecutor(max_workers=parallel) as pool:
                futures = {
                    pool.submit(_run_single_zip_workflow_worker, zip_code, worker_cfg): zip_code
                    for zip_code in zip_codes
                }
                done_count = 0
                for future in as_completed(futures):
                    zip_code = futures[future]
                    done_count += 1
                    try:
                        rc = future.result()
                    except Exception as exc:
                        print(f"ZIP {zip_code} failed: {type(exc).__name__}: {exc}", flush=True)
                        return 1
                    if rc != 0:
                        print(f"ZIP {zip_code} failed with exit code {rc}", flush=True)
                        return rc
                    print(f"[{done_count}/{len(zip_codes)}] ZIP {zip_code} finished", flush=True)
        except KeyboardInterrupt:
            print("Interrupted. Parallel workers stopped.", flush=True)
            return 130

    print("\nAll ZIPs complete.", flush=True)
    return 0


def _to_worker_config(args: argparse.Namespace) -> dict[str, Any]:
    return {
        "output_root": str(args.output_root),
        "start_url": str(args.start_url),
        "headful": bool(args.headful),
        "delay": float(args.delay),
        "max_load_more_clicks": int(args.max_load_more_clicks),
        "before_zip_delay": float(args.before_zip_delay),
        "after_zip_delay": float(args.after_zip_delay),
        "before_scrape_delay": float(args.before_scrape_delay),
        "page_settle_delay": float(args.page_settle_delay),
        "after_scrape_delay": float(args.after_scrape_delay),
        "retries": int(args.retries),
        "retry_backoff": float(args.retry_backoff),
        "force": bool(args.force),
        "only_incomplete": bool(args.only_incomplete),
        "max_remaining_rounds": int(args.max_remaining_rounds),
        "remaining_limit_per_round": int(args.remaining_limit_per_round),
    }


def _from_worker_config(cfg: dict[str, Any]) -> argparse.Namespace:
    return argparse.Namespace(
        output_root=Path(cfg["output_root"]),
        start_url=cfg["start_url"],
        headful=cfg["headful"],
        delay=cfg["delay"],
        max_load_more_clicks=cfg["max_load_more_clicks"],
        before_zip_delay=cfg["before_zip_delay"],
        after_zip_delay=cfg["after_zip_delay"],
        before_scrape_delay=cfg["before_scrape_delay"],
        page_settle_delay=cfg["page_settle_delay"],
        after_scrape_delay=cfg["after_scrape_delay"],
        retries=cfg["retries"],
        retry_backoff=cfg["retry_backoff"],
        force=cfg["force"],
        only_incomplete=cfg["only_incomplete"],
        max_remaining_rounds=cfg["max_remaining_rounds"],
        remaining_limit_per_round=cfg["remaining_limit_per_round"],
        parallel_zips=1,
    )


def _run_single_zip_workflow_worker(zip_code: str, cfg: dict[str, Any]) -> int:
    args = _from_worker_config(cfg)
    return _run_single_zip_workflow(zip_code, args, idx=0, total=0, quiet=True)


def _run_single_zip_workflow(
    zip_code: str,
    args: argparse.Namespace,
    *,
    idx: int,
    total: int,
    quiet: bool = False,
) -> int:
    # Per-ZIP output folder.
    zip_dir = args.output_root / zip_code
    urls_json = zip_dir / "agency_urls.json"
    listings_json = zip_dir / "listings.json"
    listings_csv = zip_dir / "listings.csv"
    remaining_json = zip_dir / "remaining.json"

    if not quiet:
        print(f"\n[{idx}/{total}] Running workflow for ZIP {zip_code}", flush=True)

    # Collect URLs for this ZIP into its own JSON.
    rc = cmd_collect_zip_codes(
        [zip_code],
        start_url=args.start_url,
        urls_json=urls_json,
        headful=args.headful,
        delay=args.delay,
        max_load_more_clicks=args.max_load_more_clicks,
        before_zip_delay=args.before_zip_delay,
        after_zip_delay=args.after_zip_delay,
    )
    if rc != 0:
        return rc

    # Scrape those URLs into per-ZIP listings files.
    rc = cmd_scrape(
        argparse.Namespace(
            urls_json=urls_json,
            output_json=listings_json,
            output_csv=listings_csv,
            remaining_json=remaining_json,
            headful=args.headful,
            before_scrape_delay=args.before_scrape_delay,
            page_settle_delay=args.page_settle_delay,
            after_scrape_delay=args.after_scrape_delay,
            retries=args.retries,
            retry_backoff=args.retry_backoff,
            force=args.force,
            only_incomplete=args.only_incomplete,
            limit=0,
        )
    )
    if rc != 0:
        return rc

    # Loop remaining for this ZIP.
    if args.max_remaining_rounds > 0:
        prev_remaining = _count_remaining(remaining_json)
        for round_idx in range(1, args.max_remaining_rounds + 1):
            if prev_remaining <= 0:
                break
            print(
                f"ZIP {zip_code}: remaining round {round_idx}/{args.max_remaining_rounds} (items={prev_remaining})",
                flush=True,
            )
            rc = cmd_scrape_remaining(
                argparse.Namespace(
                    remaining_json=remaining_json,
                    output_json=listings_json,
                    output_csv=listings_csv,
                    headful=args.headful,
                    before_scrape_delay=args.before_scrape_delay,
                    page_settle_delay=args.page_settle_delay,
                    after_scrape_delay=args.after_scrape_delay,
                    retries=args.retries,
                    retry_backoff=args.retry_backoff,
                    limit=args.remaining_limit_per_round,
                )
            )
            if rc != 0:
                return rc
            now_remaining = _count_remaining(remaining_json)
            if now_remaining >= prev_remaining:
                print(f"ZIP {zip_code}: no remaining progress, stopping.", flush=True)
                break
            prev_remaining = now_remaining

    if not quiet:
        print(f"ZIP {zip_code} done -> {zip_dir}", flush=True)
    return 0


def main(argv: list[str] | None = None) -> int:
    start_ts = datetime.now()
    start_seconds = time.time()

    # If user runs "python master.py" with no args, default to interactive prompts.
    if argv is None:
        argv = sys.argv[1:]
    if not argv:
        rc = cmd_interactive(
            argparse.Namespace(
                output_root=Path("output"),
                start_url=DEFAULT_START_URL,
                headful=False,
                delay=0.8,
                max_load_more_clicks=300,
                before_zip_delay=1.0,
                after_zip_delay=2.0,
                before_scrape_delay=2.0,
                page_settle_delay=2.0,
                after_scrape_delay=2.0,
                retries=3,
                retry_backoff=3.0,
                force=False,
                only_incomplete=False,
                max_remaining_rounds=3,
                remaining_limit_per_round=0,
                parallel_zips=1,
            )
        )
    else:
        args = parse_args(argv)
        if args.command == "collect":
            rc = cmd_collect(args)
        elif args.command == "scrape":
            rc = cmd_scrape(args)
        elif args.command == "run-all":
            rc = cmd_run_all(args)
        elif args.command == "workflow":
            rc = cmd_workflow(args)
        elif args.command == "interactive":
            rc = cmd_interactive(args)
        elif args.command == "build-remaining":
            rc = cmd_build_remaining(args)
        elif args.command == "scrape-remaining":
            rc = cmd_scrape_remaining(args)
        else:
            rc = 1

    end_ts = datetime.now()
    duration_seconds = int(time.time() - start_seconds)
    msg = (
        f"Start: {start_ts.isoformat(timespec='seconds')} | "
        f"End: {end_ts.isoformat(timespec='seconds')} | "
        f"Duration: {_format_eta(duration_seconds)}"
    )
    print(msg, flush=True)
    LOGGER.info(msg)
    return rc


if __name__ == "__main__":
    raise SystemExit(main())


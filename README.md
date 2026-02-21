## CareListing ZIP Scraper

Scrapes home care agency listing data from CareListing by iterating ZIP codes.

### What it extracts

- Name
- Address
- Phone number (clicks "Show Phone" first when present)
- Agency Type
- About text
- Listing URL and source ZIP

### Setup

1. Install dependencies:
   - `pip install -e .`
2. Install Playwright browser:
   - `playwright install chromium`

### ZIP file format

Provide a text or CSV file with at least one ZIP per line.

Examples:

```text
94103
90001
10001
```

or:

```csv
94103,San Francisco
90001,Los Angeles
```

### Run

```bash
python master.py
```

This starts interactive mode (asks ZIP source/file and ZIP count).

### Two-step pipeline (recommended)

For large ZIP lists, it is often better to split the job into:

1) Collect all listing URLs into a JSON file
2) Scrape each listing URL from that JSON file

Collect URLs:

```bash
python master.py collect --zip-file zip_codes.txt --urls-json output/agency_urls.json --headful
```

Scrape details from the collected URLs:

```bash
python master.py scrape --urls-json output/agency_urls.json --output-json output/listings.json --output-csv output/listings.csv --headful
```

Both steps save incrementally, so you can stop and resume.

### One command (master CLI)

Collect + scrape:

```bash
python master.py run-all --zip-file zip_codes.txt --headful
```

Full workflow (collect -> scrape -> scrape-remaining loop):

```bash
python master.py workflow --zip-file zip_codes.txt --headful
```

Re-scrape incomplete ones:

```bash
python master.py scrape-remaining --headful
```

### Run on EC2 with tmux

Start tmux:

```bash
tmux new -s care-scrape
```

Run scraper:

```bash
uv run python master.py
```

Detach safely (leave running):

- `Ctrl+b`, then `d`

Reattach later:

```bash
tmux attach -t care-scrape
```

Optional flags:

- `--start-url` (default: California homecare page)
- `--headful` to run with visible browser
- `--output-csv` and `--output-json` for custom output paths
- `--max-load-more-clicks` safety cap per ZIP

### Output

By default it writes:

- `output/listings.csv`
- `output/listings.json`
- `output/logs/scraper.log` (timestamps, timeouts/errors, start/end/duration)
- `output/zip_status.json` tracks collect progress (`processed`, `failed`, `pending` ZIPs)

### Notes

- Selectors are resilient but website changes can still require updates.
- This scraper de-duplicates listing URLs across ZIP searches.
- Rerun failed/pending ZIPs: open `output/zip_status.json`, copy the `failed` or `pending` ZIPs to a file, then rerun collect pointing to that file (e.g., `python master.py collect --zip-file failed_zips.txt --urls-json output/agency_urls.json --headful`).

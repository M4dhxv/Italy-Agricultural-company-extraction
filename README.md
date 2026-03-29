<div align="center">

# Italy Agricultural Machinery — Company Extraction Pipeline

**Production-grade intelligence pipeline for mapping the Italian agricultural machinery manufacturing landscape**

[![Python](https://img.shields.io/badge/Python-3.9%2B-3776AB?style=flat-square&logo=python&logoColor=white)](https://www.python.org)
[![Gemini](https://img.shields.io/badge/Gemini-2.5%20Flash-4285F4?style=flat-square&logo=google&logoColor=white)](https://deepmind.google/technologies/gemini/)
[![ATECO](https://img.shields.io/badge/ATECO-28.30-2E7D32?style=flat-square)](https://www.istat.it/it/archivio/17888)
[![License](https://img.shields.io/badge/License-MIT-gray?style=flat-square)](LICENSE)

*Discovers, scrapes, enriches and AI-classifies every agricultural machinery manufacturer in Italy.*

</div>

---

## Overview

This pipeline was built to support **market mapping and deal sourcing** for the Italian agricultural machinery sector (NACE/ATECO 28.30). It systematically identifies manufacturers — not dealers or distributors — across the full Italian market, then enriches each company with financials, leadership, ownership structure, and AI-extracted product intelligence.

The output is a **clean, analyst-ready dataset** exportable as CSV, ready to be loaded directly into a financial model, CRM, or data room.

---

## What It Does

```
DISCOVER  ──▶  SCRAPE  ──▶  CLASSIFY (AI)  ──▶  ENRICH  ──▶  OUTPUT
```

| Stage | Sources | Output |
|---|---|---|
| **Discovery** | Google Maps API, Google Search (Apify), Europages, FederUnacoma, EIMA | `discovery_output.json` |
| **Scraping** | Priority-selected pages (about / products / contact), BS4 + Apify fallback | Raw text per company |
| **Classification** | Gemini 2.5 Flash — manufacturer vs. dealer/distributor | `is_manufacturer`, `is_agri_manufacturer`, `confidence` |
| **Enrichment** | Revenue, employees, directors, parent company/group | Structured fields per company |
| **Output** | Atomic JSON append + CSV export | `raw_master.json`, `final_dataset.csv` |

---

## Dataset Output Schema

Every company in the final CSV includes:

| Field | Description |
|---|---|
| `name` | Legal company name |
| `website` | Official website URL |
| `city` / `region` / `country` | Location |
| `is_manufacturer` | `true` if the company designs and builds machinery |
| `is_agri_manufacturer` | `true` if the machinery is specifically agricultural |
| `primary_category` | Product category (Tillage / Harvesting / Spraying / etc.) |
| `products_detected` | Up to 10 product types, translated to English |
| `confidence` | AI confidence score (0.0 – 1.0) |
| `evidence` | AI-generated classification rationale |
| `revenue` | Reported annual revenue (€) |
| `employees` | Headcount |
| `directors` | C-suite / board names |
| `ownership_group` | Parent company or industrial group |
| `scrape_source` | `bs4` \| `apify_fallback` \| `none` |
| `scrape_status` | `success` \| `failed` \| `skipped_directory` |

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                        run_pipeline.py                           │
│                      Main Orchestrator                           │
└────────────────────────────┬─────────────────────────────────────┘
                             │
          ┌──────────────────┼──────────────────┐
          │                  │                  │
   ┌──────▼──────┐   ┌───────▼──────┐   ┌──────▼──────┐
   │  DISCOVERY  │   │   SCRAPING   │   │  ENRICHMENT │
   │             │   │              │   │             │
   │ Google Maps │   │ Smart BS4    │   │ Financials  │
   │ Google Srch │   │ URL dedup    │   │ Directors   │
   │ Europages   │   │ Priority sel.│   │ Ownership   │
   │ FederUnacoma│   │ Apify backup │   │ Products    │
   │ EIMA        │   │              │   │             │
   └──────┬──────┘   └───────┬──────┘   └──────┬──────┘
          │                  │                  │
          └──────────────────┼──────────────────┘
                             │
              ┌──────────────▼──────────────┐
              │       Gemini 2.5 Flash       │
              │   AI Classification Layer   │
              │                             │
              │  • Manufacturer detection   │
              │  • Product extraction       │
              │  • Confidence scoring       │
              └──────────────┬──────────────┘
                             │
          ┌──────────────────┼──────────────────┐
          │                  │                  │
   ┌──────▼──────┐   ┌───────▼──────┐   ┌──────▼──────┐
   │raw_master   │   │final_dataset │   │ failures    │
   │   .json     │   │    .csv      │   │    .csv     │
   │(append-only)│   │(analyst CSV) │   │(error log)  │
   └─────────────┘   └──────────────┘   └─────────────┘
```

---

## Scraper Intelligence

The scraping layer applies a multi-step URL selection algorithm to maximise signal quality while minimising crawl cost:

1. **Normalize** — lowercase, strip query strings and trailing slashes
2. **Collapse depth** — paths deeper than 3 segments are truncated to prevent product-detail URL explosion
3. **Deduplicate** — collapsed paths stored in a Python `set()`, eliminating redundant variants
4. **Prioritize** — URLs categorized into strict buckets: `contact → about → products → brands → other`
5. **Cap per category** — contact: 1, about: 2, products: 2, brands: 1
6. **Enforce limit** — maximum **6 pages per company** including homepage

Contact pages are always scraped for emails and phone numbers, but are **never** included in the Gemini AI prompt to prevent classification noise.

---

## AI Classification

Each company's web content is analyzed by **Gemini 2.5 Flash** with a strict institutional-grade prompt:

**Hard reject conditions:**
- Explicitly identified as `dealer`, `distributor`, `reseller`, `rivenditore`, or `distributore`

**Positive classification signals:**
- Verbs: *produce, manufacture, build, design, construct, realise*
- Ownership language: *our machines, our products, made in Italy, production facility*

**Outputs per company:**
```json
{
  "is_manufacturer": true,
  "is_agri_manufacturer": true,
  "products_detected": ["Round Baler", "Forage Wrapper", "Bale Handler"],
  "confidence": 0.94,
  "evidence": "Company explicitly states manufacturing of hay and forage machinery since 1978."
}
```

Product names are always translated to English regardless of source language.

---

## Product Taxonomy

Detected products are mapped to one of the following categories:

| Category | Example Products |
|---|---|
| Tillage | Plough, Harrow, Rotary Tiller, Cultivator, Subsoiler |
| Seeding & Planting | Seeder, Transplanter, Precision Seeder |
| Crop Protection | Sprayer, Boom Sprayer, Atomiser, Mist Blower |
| Fertilising | Fertiliser Spreader, Manure Spreader, Slurry Spreader |
| Harvesting | Combine Harvester, Grape Harvester, Picker |
| Hay & Forage | Mower, Baler, Rake, Mower Conditioner |
| Mulching & Shredding | Mulcher, Shredder, Chipper |
| Forestry | Forestry Harvester, Feller, Skidder, Forestry Grapple |
| Transport & Handling | Trailer, Wagon, Elevator, Loader |
| Irrigation | Irrigator, Centre Pivot, Sprinkler |
| Precision Agriculture | Precision Farming Systems |
| General Agricultural Machinery | Multi-category manufacturers |

---

## Project Structure

```
agri-pipeline/
│
├── run_pipeline.py              # Main entry point — orchestrates all stages
├── website_enrichment_pass.py   # Standalone enrichment pass
├── rebuild_discovery.py         # Re-run discovery independently
├── re_evaluate_failures.py      # Retry previously failed records
├── salvage_apify_websites.py    # Recover Apify website data
│
├── src/
│   ├── config.py                # All constants, keywords, categories, API settings
│   ├── scraper.py               # Smart scraper — URL dedup, priority selection, BS4
│   ├── intelligence_layer.py    # Gemini 2.5 Flash — classification + prompt
│   ├── apify_client.py          # Apify REST wrapper — Google Search + website crawl
│   ├── http_client.py           # httpx client — retry + exponential backoff
│   ├── output_builder.py        # raw_master.json → final_dataset.csv
│   ├── product_extractor.py     # Product names → taxonomy categories
│   ├── financial_extractor.py   # Revenue / employee extraction
│   ├── directors_extractor.py   # C-suite name extraction
│   ├── ownership_extractor.py   # Parent company / group extraction
│   ├── website_resolver.py      # Website discovery via Google Search
│   ├── failure_logger.py        # Structured error logging
│   ├── state_manager.py         # Checkpoint persistence
│   └── discovery/
│       ├── google_maps.py       # Google Places API
│       ├── google_search.py     # Apify Google Search
│       └── europages_csv.py     # Europages CSV parser
│
├── .env.example
├── requirements.txt
└── README.md
```

---

## Setup

### 1. Clone and install

```bash
git clone https://github.com/M4dhxv/Italy-Agricultural-company-extraction.git
cd Italy-Agricultural-company-extraction

python -m venv .venv
source .venv/bin/activate   # macOS/Linux

pip install -r requirements.txt
```

### 2. Configure credentials

```bash
cp .env.example .env
```

```env
APIFY_API_TOKEN=...        # apify.com — Google Search + website crawler
GOOGLE_MAPS_API_KEY=...    # Google Cloud Console → Places API
GEMINI_API_KEY=...         # aistudio.google.com
```

---

## Usage

```bash
# Enrichment only (recommended — uses existing discovery_output.json)
python run_pipeline.py --mode enrichment --resume --skip-no-website

# Full pipeline — discovery + enrichment
python run_pipeline.py --restart

# Rebuild CSV from existing data (no re-processing)
python run_pipeline.py --csv-only

# Retry all previously failed records
python re_evaluate_failures.py
```

> Use `--resume` at all times after the first run. The pipeline checkpoints after every company and will pick up exactly where it left off after any interruption.

---

## Fault Tolerance

| Mechanism | Detail |
|---|---|
| **Per-company checkpointing** | `state.json` updated after every company. `--resume` always safe. |
| **Atomic writes** | `raw_master.json` written via `os.replace()` — never partially corrupted. |
| **Apify hard cap** | 60-second timeout on fallback crawler. Zero retries on dead domains. |
| **Failure isolation** | Any unhandled exception is caught, logged to `failures.csv`, and the pipeline continues. |
| **Duplicate prevention** | `--resume` checks `raw_master.json` on startup to skip already-processed companies. |

---

## Requirements

- Python 3.9+
- `httpx[http2]` — HTTP client with HTTP/2 support
- `beautifulsoup4` + `lxml` — HTML parsing
- `google-genai` — Gemini 2.5 Flash
- `tenacity` — Retry logic with exponential backoff
- `tqdm` — Progress tracking
- `python-dotenv` — Environment configuration

---

<div align="center">

Built for institutional-grade deal sourcing and market intelligence.

</div>

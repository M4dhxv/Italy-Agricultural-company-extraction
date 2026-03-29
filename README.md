# 🌾 ATECO 28.30 — Italian Agricultural Machinery Pipeline

A production-grade, fault-tolerant data pipeline for **discovering, scraping, enriching, and classifying Italian agricultural machinery manufacturers** (NACE/ATECO code 28.30).

Outputs a clean, analyst-ready CSV and raw JSON dataset for every company found — including financials, directors, ownership, product categories, and Gemini AI classification.

---

## 📐 Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                        run_pipeline.py                      │
│                     (Main Orchestrator)                     │
└─────────────┬────────────────────────────────┬──────────────┘
              │                                │
     ┌────────▼────────┐              ┌────────▼────────┐
     │   DISCOVERY     │              │   ENRICHMENT    │
     │                 │              │                 │
     │ • Google Maps   │              │ • Smart Scraper │
     │ • Google Search │              │ • Gemini 2.5 AI │
     │ • Europages CSV │              │ • Financials    │
     │ • FederUnacoma  │              │ • Directors     │
     │ • EIMA          │              │ • Ownership     │
     └────────┬────────┘              └────────┬────────┘
              │                                │
              └──────────────┬─────────────────┘
                             │
                   ┌─────────▼─────────┐
                   │   OUTPUT BUILDER  │
                   │                   │
                   │  raw_master.json  │
                   │  final_dataset.csv│
                   │  failures.csv     │
                   └───────────────────┘
```

---

## ✨ Features

- **Multi-source discovery** — Google Maps API, Google Search (via Apify), Europages CSV, FederUnacoma, EIMA
- **Smart page selection** — URL normalization, depth collapsing, keyword-based priority scoring (about/products/contact), category caps to prevent crawl bloat
- **Gemini 2.5 Flash AI classification** — Determines if a company is an agricultural machinery *manufacturer* (not just a dealer or distributor), extracts product categories and confidence scores
- **Contact scraping with AI separation** — Contact pages are scraped for phone/email/address data but are never fed to the Gemini prompt
- **Financial enrichment** — Revenue, profit, employee count extracted from public Italian business databases via Apify
- **Directors & ownership extraction** — C-suite names and parent company/group relationships
- **Fault-tolerant resume** — Persists progress to `state.json` after every company; restart anytime with `--resume`
- **Failure logging** — Every error is recorded to `failures.csv` with stage, source, error type and message
- **Apify fallback** — If BS4 scraping returns too little content, Apify's headless crawler steps in (60s hard cap to prevent hangs)

---

## 🗂 Project Structure

```
agri-pipeline/
├── run_pipeline.py              # Main orchestrator — entry point for all modes
├── website_enrichment_pass.py   # Standalone enrichment pass for website data
├── rebuild_discovery.py         # Re-run discovery independently
├── re_evaluate_failures.py      # Retry previously failed records
├── salvage_apify_websites.py    # Recover Apify-scraped website data
│
├── src/
│   ├── config.py                # All constants, keywords, categories, API configs
│   ├── scraper.py               # Smart BS4 scraper with URL dedup & priority selection
│   ├── intelligence_layer.py    # Gemini 2.5 Flash classification + prompt logic
│   ├── apify_client.py          # Apify REST API wrapper (search + website crawler)
│   ├── http_client.py           # Shared httpx client with retry + exponential backoff
│   ├── output_builder.py        # CSV builder from raw_master.json
│   ├── product_extractor.py     # Maps raw product names → taxonomy categories
│   ├── financial_extractor.py   # Financial data extraction via Apify search
│   ├── directors_extractor.py   # C-suite name extraction via Apify search
│   ├── ownership_extractor.py   # Parent company / group extraction via Apify search
│   ├── website_resolver.py      # Resolves missing websites via Google Search
│   ├── failure_logger.py        # Structured failure logging to failures.csv
│   ├── state_manager.py         # Checkpoint save/load (state.json)
│   └── discovery/
│       ├── google_maps.py       # Google Places API discovery
│       ├── google_search.py     # Apify-powered Google Search discovery
│       └── europages_csv.py     # Europages CSV parser
│
├── raw_master.json              # Append-only enriched company records (auto-generated)
├── final_dataset.csv            # Clean analyst-ready CSV output (auto-generated)
├── failures.csv                 # Structured error log (auto-generated)
├── state.json                   # Resume checkpoint (auto-generated)
├── discovery_output.json        # Raw discovery results (auto-generated)
│
├── requirements.txt
├── .env.example
└── pytest.ini
```

---

## 🚀 Getting Started

### 1. Clone & set up environment

```bash
git clone https://github.com/your-username/agri-pipeline.git
cd agri-pipeline

python -m venv .venv
source .venv/bin/activate        # macOS / Linux
# .venv\Scripts\activate         # Windows

pip install -r requirements.txt
```

### 2. Configure API keys

Copy the example env file and fill in your keys:

```bash
cp .env.example .env
```

Edit `.env`:

```env
# Required for Google Search discovery, financial, directors, ownership enrichment
APIFY_API_TOKEN=your_apify_token_here

# Required for Google Maps/Places-based discovery only
GOOGLE_MAPS_API_KEY=your_google_maps_api_key_here

# Required for Gemini AI classification
GEMINI_API_KEY=your_gemini_api_key_here
```

> **Where to get keys:**
> - **Apify** → [apify.com](https://apify.com) — free tier available
> - **Google Maps API** → [Google Cloud Console](https://console.cloud.google.com) → Enable "Places API"
> - **Gemini API** → [Google AI Studio](https://aistudio.google.com)

---

## 🎮 Running the Pipeline

### Full pipeline — discovery + enrichment

```bash
# Fresh start (wipes all previous state)
python run_pipeline.py --restart

# Resume from last checkpoint (safe to use after crash/interrupt)
python run_pipeline.py --resume

# Resume, skip companies without a known website
python run_pipeline.py --resume --skip-no-website

# Process only the first N companies (useful for testing)
python run_pipeline.py --resume --limit 50
```

### Enrichment only (skip discovery)

```bash
# Enrich from existing discovery_output.json
python run_pipeline.py --mode enrichment --resume --skip-no-website
```

### Rebuild CSV from existing data

```bash
# Regenerate final_dataset.csv from raw_master.json without re-running enrichment
python run_pipeline.py --csv-only
```

### Re-evaluate failures

```bash
# Retry all records that previously failed enrichment
python re_evaluate_failures.py
```

---

## 📊 Output Files

| File | Description |
|---|---|
| `raw_master.json` | Full enriched records for every company, appended after each one is processed. Source of truth. |
| `final_dataset.csv` | Clean analyst-ready CSV — one row per company. Used for analysis and reporting. |
| `failures.csv` | Detailed failure log — stage, error type, message, and source URL for every failed company. |
| `state.json` | Current pipeline checkpoint. Stores the last processed company index for safe resume. |
| `discovery_output.json` | Raw discovery output used as input for the enrichment phase. |
| `pipeline.log` | Full debug/info log of the entire pipeline run. |

---

## 🧠 Gemini Classification Logic

Each company's scraped website content is passed to **Gemini 2.5 Flash** with a strict classification prompt:

- `is_manufacturer` — `true` only if the company **designs, produces, or builds** machinery
- `is_agri_manufacturer` — `true` only if the machinery is specifically **agricultural**
- `products_detected` — Up to 10 representative product names (always translated to English)
- `confidence` — A 0.0–1.0 confidence score
- `evidence` — A 1–2 sentence justification

> **Hard reject signals:** `dealer`, `distributor`, `reseller`, `rivenditore`, `distributore`
> 
> Contact pages are scraped for emails/phone numbers but are **never** included in the AI prompt to prevent noise.

---

## 🔧 Scraper Page Selection

The smart scraper applies a strict priority-based URL pipeline:

1. **Normalize** — lowercase, strip query params and trailing slashes
2. **Collapse depth** — paths deeper than 3 segments are truncated (prevents detail-page explosion)
3. **Deduplicate** — collapsed paths are stored in a `set()` to eliminate redundant variants
4. **Categorize** — URLs are sorted into buckets: `contact → about → products → brands → other`
5. **Cap per category** — `contact: 1`, `about: 2`, `products: 2`, `brands: 1`
6. **Max 6 pages total** — always includes homepage + prioritized pages

---

## ⚙️ Configuration

All pipeline constants live in `src/config.py`:

| Constant | Default | Description |
|---|---|---|
| `MAX_PAGES` | 6 | Max pages scraped per company |
| `MIN_TEXT_LENGTH` | 1500 | Min characters needed to skip Apify fallback |
| `REQUEST_TIMEOUT` | 30s | HTTP request timeout |
| `REQUEST_DELAY` | 1.5s | Polite delay between requests |
| `BACKOFF_BASE` | 2.0s | Exponential backoff base |
| `MAX_RETRIES` | 3 | Max retry attempts for HTTP requests |
| `APIFY_MAX_RESULTS` | 10 | Max search results per Apify query |

---

## 🛡 Fault Tolerance

- **Resume anywhere** — Every successfully processed company is checkpointed. Use `--resume` to pick up exactly where you left off after any crash or interrupt.
- **Apify 60s cap** — The Apify website crawler fallback has a hard 60-second timeout and zero retries to prevent pipeline hangs on dead domains.
- **Atomic file writes** — `raw_master.json` is written atomically using `os.replace()` to prevent corruption.
- **Failure isolation** — Any unhandled exception on a single company is caught, logged to `failures.csv`, and the pipeline continues to the next company.

---

## 🧪 Running Tests

```bash
pytest
```

---

## 📋 Requirements

- Python 3.9+
- `httpx[http2]` — HTTP client
- `beautifulsoup4` + `lxml` — HTML parsing
- `google-genai` — Gemini 2.5 Flash AI
- `tenacity` — Retry logic
- `tqdm` — Progress bars
- `python-dotenv` — Environment variable loading

See `requirements.txt` for pinned versions.

---

## 📄 License

MIT License — see `LICENSE` for details.

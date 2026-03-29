"""
config.py — Central configuration for the ATECO 28.30 pipeline.
All constants, keywords, categories, and source URLs live here.
"""

# ---------------------------------------------------------------------------
# ATECO / NACE target codes
# ---------------------------------------------------------------------------
TARGET_ATECO = ["28.30", "28.30.1", "28.30.9"]
TARGET_NACE  = ["28.30"]

# ---------------------------------------------------------------------------
# Product keyword map  (Italian keyword → English label)
# ---------------------------------------------------------------------------
KEYWORD_MAP: dict[str, str] = {
    # Tillage
    "aratro": "plough",
    "aratri": "plough",
    "erpice": "harrow",
    "erpici": "harrow",
    "fresa": "rotary tiller",
    "subsoiler": "subsoiler",
    "ripper": "ripper",
    "scarificatore": "scarifier",
    "vomere": "ploughshare",
    "coltivatore": "cultivator",
    "estirpatore": "cultivator",
    "vangatrice": "spading machine",
    # Seeding & planting
    "seminatrice": "seeder",
    "seminatrici": "seeder",
    "trapiantatrice": "transplanter",
    "piantatrice": "planter",
    "distributore di semi": "seed distributor",
    "precision seeder": "precision seeder",
    # Spraying
    "irroratrice": "sprayer",
    "irroratrici": "sprayer",
    "atomizzatore": "atomiser",
    "barra irrorante": "boom sprayer",
    "nebulizzatore": "mist blower",
    "diserbo": "herbicide applicator",
    # Fertilising
    "spandiconcime": "fertiliser spreader",
    "spandiletame": "manure spreader",
    "spandiliquame": "slurry spreader",
    "spandisolido": "solid spreader",
    "distributore di fertilizzanti": "fertiliser distributor",
    # Harvesting
    "mietitrebbia": "combine harvester",
    "mietitrebbiatrice": "combine harvester",
    "testatrice": "harvesting head",
    "vendemmiatrice": "grape harvester",
    "raccoglitrice": "picker",
    "falciatrice": "mower",
    "falciacondizionatrice": "mower conditioner",
    "andanatore": "windrow",
    "ranghinatore": "rake",
    "rotoimballatrice": "round baler",
    "imballatrice": "baler",
    "pressa": "baler",
    "mietilega": "binder",
    "trebbia": "thresher",
    # Hay & forage
    "fasciatore": "wrapper",
    "raccoglitore": "pick-up loader",
    "carriarmato": "forage wagon",
    # Mulching & shredding
    "trincia": "mulcher",
    "trinciatore": "shredder",
    "trinciatrice": "mulcher",
    "cippatrice": "chipper",
    # Transport & handling
    "rimorchio": "trailer",
    "carro": "wagon",
    "elevatore": "elevator",
    "nastro trasportatore": "conveyor",
    "sollevatore": "loader",
    # Forestry
    "testata forestale": "forestry head",
    "abbattitrice": "feller",
    "harvester forestale": "forestry harvester",
    "esboscatore": "skidder",
    "pinza forestale": "forestry grapple",
    # Irrigation
    "irrigatore": "irrigator",
    "pivot": "centre pivot",
    "aspersor": "sprinkler",
    # Generic manufacturing signals (English)
    "plough": "plough",
    "harrow": "harrow",
    "seeder": "seeder",
    "sprayer": "sprayer",
    "spreader": "spreader",
    "harvester": "harvester",
    "baler": "baler",
    "mulcher": "mulcher",
    "cultivator": "cultivator",
    "tractor implement": "tractor implement",
    "agricultural machinery": "agricultural machinery",
    "farm equipment": "farm equipment",
    "precision farming": "precision farming",
    "macchine agricole": "agricultural machinery",
    "macchine per l'agricoltura": "agricultural machinery",
    "attrezzatura agricola": "agricultural equipment",
    "costruzione di macchine agricole": "agricultural machinery manufacturing",
    "produzione di macchine agricole": "agricultural machinery manufacturing",
}

# ---------------------------------------------------------------------------
# Category taxonomy — maps English labels to a category bucket
# ---------------------------------------------------------------------------
CATEGORY_MAP: dict[str, str] = {
    "plough": "Tillage",
    "harrow": "Tillage",
    "rotary tiller": "Tillage",
    "scarifier": "Tillage",
    "cultivator": "Tillage",
    "subsoiler": "Tillage",
    "spading machine": "Tillage",

    "seeder": "Seeding & Planting",
    "transplanter": "Seeding & Planting",
    "planter": "Seeding & Planting",
    "precision seeder": "Seeding & Planting",

    "sprayer": "Crop Protection",
    "atomiser": "Crop Protection",
    "boom sprayer": "Crop Protection",
    "mist blower": "Crop Protection",

    "fertiliser spreader": "Fertilising",
    "manure spreader": "Fertilising",
    "slurry spreader": "Fertilising",

    "combine harvester": "Harvesting",
    "grape harvester": "Harvesting",
    "picker": "Harvesting",
    "mower": "Hay & Forage",
    "mower conditioner": "Hay & Forage",
    "baler": "Hay & Forage",
    "rake": "Hay & Forage",

    "mulcher": "Mulching & Shredding",
    "shredder": "Mulching & Shredding",
    "chipper": "Mulching & Shredding",

    "forestry harvester": "Forestry",
    "forestry head": "Forestry",
    "feller": "Forestry",
    "skidder": "Forestry",
    "forestry grapple": "Forestry",

    "trailer": "Transport & Handling",
    "wagon": "Transport & Handling",
    "elevator": "Transport & Handling",
    "loader": "Transport & Handling",

    "irrigator": "Irrigation",
    "centre pivot": "Irrigation",
    "sprinkler": "Irrigation",

    "agricultural machinery": "General Agricultural Machinery",
    "agricultural machinery manufacturing": "General Agricultural Machinery",
    "farm equipment": "General Agricultural Machinery",
    "agricultural equipment": "General Agricultural Machinery",
    "precision farming": "Precision Agriculture",
    "tractor implement": "Tractor Implements",
}

# ---------------------------------------------------------------------------
# Manufacturer filter signals
# ---------------------------------------------------------------------------
MANUFACTURER_ACCEPT_SIGNALS = [
    "produciamo", "fabbrichiamo", "costruiamo", "realizziamo", "progettiamo",
    "produzione propria", "made in italy", "made in", "we manufacture",
    "we produce", "we design", "we build", "our machines", "our products",
    "manufacturing", "produttore", "costruttore", "fabbricante",
    "stabilimento produttivo", "linea di produzione", "officina",
    "agricultural machinery manufacturer", "costruzione macchine",
    "produzione macchine", "fabbricazione macchine", "ateco 28.30",
    "nace 28.30",
]

MANUFACTURER_REJECT_SIGNALS = [
    "ricambi", "spare parts", "distribuzione", "rivenditore exclusivo",
    "authorized dealer", "concessionario", "rivenditore autorizzato",
    "distributore", "solo ricambi", "only spare parts", "service center",
    "centro assistenza", "officina di riparazione",
]

# ---------------------------------------------------------------------------
# Discovery source configs
# ---------------------------------------------------------------------------
GOOGLE_MAPS_QUERIES = [
    "produttore macchine agricole",
    "costruttore macchine agricole",
    "fabbricazione macchine agricole",
]

GOOGLE_SEARCH_DISCOVERY_QUERIES = [
    "ATECO 28.30 aziende Italia",
    "produttore macchine agricole Italia",
    "costruttore macchine agricole Italia",
    "agricultural machinery manufacturer Italy",
    "site:federunacoma.it associati",
    "site:eima.it espositori",
    "ATECO 28.30 Srl Italia",
]

FINANCIAL_QUERY_TEMPLATES = [
    'site:fatturatoitalia.it "{name}"',
    'site:reportaziende.it "{name}"',
    'site:aziende.it "{name}"',
    'site:ufficiocamerale.it "{name}"',
    'site:companyreports.it "{name}"',
    '"{name}" fatturato',
    '"{name}" bilancio',
    '"{name}" ricavi',
    '"{name}" utile',
    '"{name}" fatturato 2025',
    '"{name}" bilancio 2025',
    '"{name}" revenue',
    '"{name}" financial results',
]

DIRECTORS_QUERY_TEMPLATES = [
    '"{name}" amministratore delegato',
    '"{name}" CEO',
    '"{name}" presidente',
    '"{name}" direttore generale',
    '"{name}" board of directors',
]

OWNERSHIP_QUERY_TEMPLATES = [
    '"{name}" gruppo',
    '"{name}" subsidiary',
    '"{name}" acquisita da',
    '"{name}" holding',
    '"{name}" parent company',
]

WEBSITE_RESOLUTION_TEMPLATES = [
    '"{name}" sito ufficiale',
    '"{name}" official website',
    '"{name}" Italy agricultural machinery website',
]

# ---------------------------------------------------------------------------
# Scraper page priority
# ---------------------------------------------------------------------------
PRIORITY_PATHS = ["/", "/about", "/azienda", "/chi-siamo", "/products", "/prodotti"]
SECONDARY_PATHS = ["/solutions", "/soluzioni", "/applicazioni", "/applications",
                   "/en/products", "/en/about"]
MAX_SCRAPE_DEPTH = 2

# ---------------------------------------------------------------------------
# HTTP settings
# ---------------------------------------------------------------------------
REQUEST_TIMEOUT  = 30       # seconds
MAX_RETRIES      = 3
BACKOFF_BASE     = 2.0      # seconds (exponential: 2, 4, 8)
REQUEST_DELAY    = 1.5      # seconds between requests (polite)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "it-IT,it;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# ---------------------------------------------------------------------------
# Apify
# ---------------------------------------------------------------------------
APIFY_BASE_URL  = "https://api.apify.com/v2"
APIFY_SEARCH_ACTOR = "apify~google-search-scraper"
APIFY_MAX_RESULTS = 10

# ---------------------------------------------------------------------------
# Google Maps
# ---------------------------------------------------------------------------
GOOGLE_PLACES_URL = "https://maps.googleapis.com/maps/api/place/textsearch/json"
GOOGLE_GEOCODE_URL = "https://maps.googleapis.com/maps/api/geocode/json"
GOOGLE_PLACES_REGION = "it"
GOOGLE_PLACES_MAX_PAGES = 3   # up to 60 results per query (20/page)

# ---------------------------------------------------------------------------
# File paths
# ---------------------------------------------------------------------------
RAW_MASTER_PATH  = "raw_master.json"
FINAL_CSV_PATH   = "final_dataset.csv"
FAILURES_CSV_PATH = "failures.csv"
STATE_PATH       = "state.json"

# ---------------------------------------------------------------------------
# Employee threshold
# ---------------------------------------------------------------------------
SMALL_COMPANY_THRESHOLD = 5

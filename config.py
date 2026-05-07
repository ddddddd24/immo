"""Load and validate environment variables."""
import os
from dotenv import load_dotenv

load_dotenv()

# ── Mock mode — set MOCK_MODE=true in .env to skip all paid APIs ──────────────
MOCK_MODE: bool = os.getenv("MOCK_MODE", "false").lower() in ("true", "1", "yes")

# ── Scraper engine — default is Playwright (free). Set USE_APIFY=true to use Apify instead ──
USE_APIFY: bool = os.getenv("USE_APIFY", "false").lower() in ("true", "1", "yes")

# ── Optional: Claude scoring (costs ~$0.005/listing extra) ───────────────────
ENABLE_SCORING: bool = os.getenv("ENABLE_SCORING", "false").lower() in ("true", "1", "yes")
MIN_SCORE: int = int(os.getenv("MIN_SCORE", "6"))

# ── Optional: Claude photo analysis (costs ~$0.012/listing extra) ─────────────
ENABLE_PHOTO_ANALYSIS: bool = os.getenv("ENABLE_PHOTO_ANALYSIS", "false").lower() in ("true", "1", "yes")


def _require(key: str) -> str:
    val = os.getenv(key)
    if not val:
        if MOCK_MODE:
            return f"mock_{key}"
        raise EnvironmentError(f"Missing required env var: {key}")
    return val


# Required (skipped / stubbed in mock mode)
ANTHROPIC_API_KEY: str = _require("ANTHROPIC_API_KEY")
TELEGRAM_BOT_TOKEN: str = _require("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID: str = _require("TELEGRAM_CHAT_ID")
APIFY_API_KEY: str = _require("APIFY_API_KEY")
LBC_EMAIL: str = _require("LBC_EMAIL")
LBC_PASSWORD: str = _require("LBC_PASSWORD")

# DeepSeek (Anthropic-compatible endpoint — cheap LLM alternative)
USE_DEEPSEEK: bool = os.getenv("USE_DEEPSEEK", "false").lower() in ("true", "1", "yes")
DEEPSEEK_API_KEY: str = os.getenv("DEEPSEEK_API_KEY", "")
DEEPSEEK_MODEL: str = os.getenv("DEEPSEEK_MODEL", "deepseek-v4-flash")

# Optional / defaults
DB_PATH: str = os.getenv("DB_PATH", "data/bot.db")
LOG_FILE: str = os.getenv("LOG_FILE", "leboncoin_bot.log")
MAX_MESSAGES_PER_HOUR: int = int(os.getenv("MAX_MESSAGES_PER_HOUR", "20"))
# CLAUDE_MODEL is reused by both Claude and DeepSeek (Anthropic-compatible API).
# Default depends on USE_DEEPSEEK so swapping providers is one env-var flip.
_DEFAULT_LLM_MODEL = DEEPSEEK_MODEL if USE_DEEPSEEK else "claude-sonnet-4-6"
CLAUDE_MODEL: str = os.getenv("CLAUDE_MODEL", _DEFAULT_LLM_MODEL)

# SeLoger credentials (optional — SeLoger scraping disabled if missing)
SELOGER_EMAIL: str = os.getenv("SELOGER_EMAIL", "")
SELOGER_PASSWORD: str = os.getenv("SELOGER_PASSWORD", "")

# Apify actor IDs
APIFY_SEARCH_ACTOR: str = "ecomscrape/leboncoin-product-search-scraper"
APIFY_SEND_ACTOR: str = "saswave/leboncoin-action-automation-scraper"

# Default LeBonCoin search URL
DEFAULT_SEARCH_URL: str = (
    "https://www.leboncoin.fr/recherche"
    "?category=10"
    "&real_estate_type=1,2"
    "&price=min-1100"
    # Saint-Denis added (commute target, was missing → 0 listings dept 93).
    # Asnières + Courbevoie added (92 west, missing).
    "&locations=Paris,Saint-Denis,Boulogne-Billancourt,Neuilly-sur-Seine,Levallois-Perret,Clichy,"
    "Asni%C3%A8res-sur-Seine,Courbevoie,Issy-les-Moulineaux,Montrouge,Malakoff,Vanves,Ivry-sur-Seine,"
    "Le%20Kremlin-Bic%C3%AAtre,Charenton-le-Pont,Saint-Mand%C3%A9,Vincennes,"
    "Montreuil,Bagnolet,Pantin,Saint-Ouen,Aubervilliers,"
    "Alfortville,Maisons-Alfort,Saint-Maur-des-Foss%C3%A9s"
)

# Default SeLoger search URL (Paris, meublé, 25m²+, max 1000€)
# ⚠️  Copie l'URL depuis ton navigateur après avoir fait une recherche SeLoger avec tes critères
DEFAULT_SEARCH_SELOGER_URL: str = os.getenv(
    "SELOGER_SEARCH_URL",
    "https://www.seloger.com/classified-search"
    "?distributionTypes=Rent"
    "&estateTypes=House,Apartment"
    "&locations=eyJwbGFjZUlkcyI6WyJTVFJURlI0NDA5MDQ1Il0sImR1cmF0aW9uIjoiNjAiLCJtb2RlIjoiVHJhbnNpdCJ9"
    "&priceMax=1000"
    "&projectTypes=Stock"
    "&spaceMin=25",
)

# Default PAP.fr search URL (Paris + IDF, meublé, 25m²+, max 1000€)
DEFAULT_SEARCH_PAP_URL: str = os.getenv(
    "PAP_SEARCH_URL",
    # Was: paris-75g439 (Paris only). Now IDF entier (g439) → couvre 75/77/78/91/92/93/94/95
    # Filter relâché : surface-min retiré (couvre 14-24m² aussi pour transition couple)
    "https://www.pap.fr/annonce/locations-ile-de-france-g439"
    "?loyer-max=1100&ameublement=meuble",
)

# Default Bien'ici search URL (Paris IDF, meublé, 14m²+, max 1000€)
# Filter relâché : surface-min 25→14 pour catch sub-25m² listings (Charenton 14m² case)
DEFAULT_SEARCH_BIENICI_URL: str = os.getenv(
    "BIENICI_SEARCH_URL",
    "https://www.bienici.com/recherche/location/ile-de-france"
    "?prix-max=1100&surface-min=14",
)

# Default Logic-Immo search URL (Paris, max 1100€).
# Re-enabled 2026-05-03: scraper now uses Camoufox + the React testid scheme
# (data-base attribute holds the URL-encoded detail URL). The classified-search
# endpoint with Aviv geo id (AD08FR31096 = Paris) returns ~25 listings.
DEFAULT_SEARCH_LOGICIMMO_URL: str = os.getenv(
    "LOGICIMMO_SEARCH_URL",
    "https://www.logic-immo.com/classified-search"
    "?distributionTypes=Rent"
    "&estateTypes=House,Apartment"
    "&locations=AD08FR31096"
    "&priceMax=1100",
)

# ── Phase 2: student / young-pro platforms ────────────────────────────────────
# Empty string disables the source — dispatcher skips empty URLs in
# _campaign_sources(). Studapart and Paris Attitude have site-specific parsers;
# the others are disabled by default until proper parsers are written.
# Studapart — public listings via Camoufox (stealth Playwright gets
# fingerprinted and served an SEO fallback page).
DEFAULT_SEARCH_STUDAPART_URL: str = os.getenv(
    "STUDAPART_SEARCH_URL",
    "https://www.studapart.com/fr/logement-etudiant-paris",
)

# Paris Attitude — public listings, ~40 per page on the index URL.
# Typical inventory is 1500€+/month (expat medium-term furnished), so the
# 1000€ budget ceiling will filter out most results.
DEFAULT_SEARCH_PARISATTITUDE_URL: str = os.getenv(
    "PARISATTITUDE_SEARCH_URL",
    "https://www.parisattitude.com/rent-apartment/furnished-rental/index,rentals.aspx",
)

# Lodgis — Paris medium/long-term furnished. Mostly 1000€+, expat segment.
DEFAULT_SEARCH_LODGIS_URL: str = os.getenv(
    "LODGIS_SEARCH_URL",
    "https://www.lodgis.com/en/paris,long-term-rentals/rentals-furnished-paris_1.cat.html",
)

# ImmoJeune — student housing aggregator.
DEFAULT_SEARCH_IMMOJEUNE_URL: str = os.getenv(
    "IMMOJEUNE_SEARCH_URL",
    "https://www.immojeune.com/logement-etudiant/paris-75.html",
)

# LocService — owner-direct rentals, French-market.
DEFAULT_SEARCH_LOCSERVICE_URL: str = os.getenv(
    "LOCSERVICE_SEARCH_URL",
    "https://www.locservice.fr/paris-75/location-appartement.html",
)

# EntreParticuliers — particulier-à-particulier. 12 listings per arrondissement
# page, the scraper iterates Paris 75001-75020 in parallel.
DEFAULT_SEARCH_ENTREPARTICULIERS_URL: str = os.getenv(
    "ENTREPARTICULIERS_SEARCH_URL",
    "https://www.entreparticuliers.com/annonces-immobilieres/appartement/location/paris-75",
)

# L'Adresse (agency network) — 40 listings/page on IDF search.
DEFAULT_SEARCH_LADRESSE_URL: str = os.getenv(
    "LADRESSE_SEARCH_URL",
    "https://www.ladresse.com/recherche/location/appartement/ile-de-france",
)

# Century 21 (agency network) — paginates 9 pages on Paris search.
DEFAULT_SEARCH_CENTURY21_URL: str = os.getenv(
    "CENTURY21_SEARCH_URL",
    "https://www.century21.fr/annonces/f/location/v-paris/",
)

# Wizi.io — managed-rental aggregator. Public API at app.wizi.eu/api/public,
# small inventory (~15-30 listings Paris).
DEFAULT_SEARCH_WIZI_URL: str = os.getenv(
    "WIZI_SEARCH_URL",
    "https://desk.wizi.eu/#/app/search?city=Paris&lat=48.856614&long=2.3522219",
)

# Laforêt (agency network) — server-rendered, GTM data attrs embedded.
DEFAULT_SEARCH_LAFORET_URL: str = os.getenv(
    "LAFORET_SEARCH_URL",
    "https://www.laforet.com/ville/location-appartement-paris-75000",
)

# Guy Hoquet (agency network) — XHR endpoint with Laravel session filters.
# Search URL is just the parent page; the scraper hits /biens/result with
# the city slug paris-75056_c4 and price_max parsed from priceMax query.
DEFAULT_SEARCH_GUYHOQUET_URL: str = os.getenv(
    "GUYHOQUET_SEARCH_URL",
    "https://www.guy-hoquet.com/annonces/location/paris/?priceMax=1100",
)

# Inli (CDC Habitat — logement intermédiaire IDF). Off-radar — paginates
# per-département, scraper expands the 8 IDF depts internally.
DEFAULT_SEARCH_INLI_URL: str = os.getenv(
    "INLI_SEARCH_URL",
    "https://www.inli.fr/locations/offres/idf",
)

# Gens de Confiance (P2P trust network) — server-renders 30 listings/page in
# React-on-Rails JSON blob. Off-radar (login required only to contact, not
# to browse). ~4900 IDF active rentals. User needs 3 sponsors to message.
DEFAULT_SEARCH_GENSDECONFIANCE_URL: str = os.getenv(
    "GENSDECONFIANCE_SEARCH_URL",
    "https://www.gensdeconfiance.com/fr/s/immobilier/locations-immobilieres",
)

# CDC Habitat (public sister of Inli) — 573k national units, ~44 IDF listings
# ≤1100€ CC, 62% intermediate (sweet spot for SNCF alternant). Server-rendered.
DEFAULT_SEARCH_CDC_URL: str = os.getenv(
    "CDC_SEARCH_URL",
    "https://www.cdc-habitat.fr/recherche/location/ile-de-france",
)

# FNAIM — federated portal of 12 000 independent agencies. ~1100 IDF listings.
# Many small agencies skip the big portals → real off-radar volume.
DEFAULT_SEARCH_FNAIM_URL: str = os.getenv(
    "FNAIM_SEARCH_URL",
    "https://www.fnaim.fr/liste-annonces-immobilieres/18-location-appartement-ile-de-france.htm",
)

# Roomlala disabled — site redirects geo-aware and 404s on every URL pattern
# tried with Camoufox; site may be restructured or geo-blocked. Set the env
# var manually if you find a working URL.
DEFAULT_SEARCH_ROOMLALA_URL: str = os.getenv("ROOMLALA_SEARCH_URL", "")

# ── Fast poller (mode veille) ─────────────────────────────────────────────────
FAST_POLL_INTERVAL_MIN: int = int(os.getenv("FAST_POLL_INTERVAL_MIN", "15"))

# ── Dossier pre-screening (off by default — costs ~$0.003/listing extra) ──────
ENABLE_PRESCREENING: bool = os.getenv("ENABLE_PRESCREENING", "false").lower() in ("true", "1", "yes")

# ── Stale contact threshold (days without reply before flagged as ghost) ──────
STALE_DAYS: int = int(os.getenv("STALE_DAYS", "5"))

# ── High-interest score threshold (only used when ENABLE_SCORING=true) ───────
# Listings scored ≥ INTEREST_THRESHOLD trigger a 🔥 priority alert in addition
# to the regular contact flow. Default 8/10 = strong match only.
INTEREST_THRESHOLD: int = int(os.getenv("INTEREST_THRESHOLD", "8"))

# ── Google Sheets sync (optional — auto-disabled if either var is missing) ───
# See sheets_sync.py module docstring for setup steps.
GOOGLE_SHEET_ID: str = os.getenv("GOOGLE_SHEET_ID", "")
GOOGLE_SERVICE_ACCOUNT_JSON: str = os.getenv(
    "GOOGLE_SERVICE_ACCOUNT_JSON", "data/google_service_account.json"
)
SYNC_AFTER_CAMPAIGN: bool = os.getenv("SYNC_AFTER_CAMPAIGN", "true").lower() in ("true", "1", "yes")

# ── Contact-message preparation ───────────────────────────────────────────────
# When false, /campagne stops after scoring and persisting; no per-listing
# DeepSeek call to generate a contact message. Saves ~3s + $0.0002 per
# eligible listing. Set ENABLE_CONTACT_PREP=false in .env to disable while
# keeping default true here for tests + dispatcher safety.
ENABLE_CONTACT_PREP: bool = os.getenv("ENABLE_CONTACT_PREP", "true").lower() in ("true", "1", "yes")

# ── Push alerts (instant Telegram notif on hot listings) ─────────────────────
# When true, a Telegram message fires during /campagne for any newly-persisted
# listing matching strict hot criteria: score>=PUSH_MIN_SCORE, price<=PUSH_MAX_PRICE,
# weight-3 zone (Paris 11/12/13, Vincennes, Saint-Mandé, Charenton), real phone,
# fresh (<24h published or scrape-marker today). Off by default.
ENABLE_PUSH_ALERTS: bool = os.getenv("ENABLE_PUSH_ALERTS", "false").lower() in ("true", "1", "yes")
PUSH_MIN_SCORE: int = int(os.getenv("PUSH_MIN_SCORE", "7"))
PUSH_MAX_PRICE: int = int(os.getenv("PUSH_MAX_PRICE", "1100"))
PUSH_RATE_PER_MIN: int = int(os.getenv("PUSH_RATE_PER_MIN", "20"))
PUSH_MAX_PER_CAMPAIGN: int = int(os.getenv("PUSH_MAX_PER_CAMPAIGN", "200"))

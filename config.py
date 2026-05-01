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
    "&price=min-1000"
    "&furnished=1"
    "&square=25-max"
    "&locations=Paris,Boulogne-Billancourt,Neuilly-sur-Seine,Levallois-Perret,Clichy,"
    "Issy-les-Moulineaux,Montrouge,Malakoff,Vanves,Ivry-sur-Seine,"
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
    "https://www.pap.fr/annonce/locations-appartement-paris-75g439-ile-de-france-g439"
    "?loyer-max=1000&surface-min=25&ameublement=meuble",
)

# Default Bien'ici search URL (Paris IDF, meublé, 25m²+, max 1000€)
DEFAULT_SEARCH_BIENICI_URL: str = os.getenv(
    "BIENICI_SEARCH_URL",
    "https://www.bienici.com/recherche/location/paris-ile-de-france"
    "?prix-max=1000&surface-min=25&meuble=true",
)

# Default Logic-Immo search URL (Paris IDF, meublé, 25m²+, max 1000€)
# ⚠️ Copy the URL from your browser after filtering on logic-immo.com with your criteria
DEFAULT_SEARCH_LOGICIMMO_URL: str = os.getenv(
    "LOGICIMMO_SEARCH_URL",
    "https://www.logic-immo.com/location-appartement/paris,ile-de-france"
    "?avec=meuble&surface-min=25&prix-max=1000",
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

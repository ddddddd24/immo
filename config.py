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

# Optional / defaults
DB_PATH: str = os.getenv("DB_PATH", "data/bot.db")
LOG_FILE: str = os.getenv("LOG_FILE", "leboncoin_bot.log")
MAX_MESSAGES_PER_HOUR: int = int(os.getenv("MAX_MESSAGES_PER_HOUR", "20"))
CLAUDE_MODEL: str = os.getenv("CLAUDE_MODEL", "claude-sonnet-4-6")

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

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

# Default LeBonCoin search URL — IDF entier via region code r_12.
# Verified 2026-05-07 (Camoufox warm context, 60 listings):
#   dept distrib  {'75': 4, '77': 1, '78': 7, '91': 18, '92': 7, '93': 7, '94': 6, '95': 10}
# Single region token covers all 8 départements (75/77/78/91/92/93/94/95) and
# replaces the previous hand-curated city allowlist that kept missing
# edge communes (Saint-Denis, Asnières, Courbevoie, Saint-Maur, etc.).
DEFAULT_SEARCH_URL: str = (
    "https://www.leboncoin.fr/recherche"
    "?category=10"
    "&real_estate_type=1,2"
    "&price=min-1100"
    "&locations=r_12"
)

# Default SeLoger search URL — IDF entier (8 départements).
# Replaces the previous transit isochrone (STRTFR4409045 = 60min Transit), which
# biased toward Paris + inner ring and missed départements 77/78/91/95.
# locations payload is base64-encoded JSON: {"placeIds":["AD08FR75","AD08FR77",
# "AD08FR78","AD08FR91","AD08FR92","AD08FR93","AD08FR94","AD08FR95"]} — one
# admin-division ID per IDF département (AD08FR{depcode}).
# priceMax bumped 1000→1100 to match LBC default and budget headroom.
DEFAULT_SEARCH_SELOGER_URL: str = os.getenv(
    "SELOGER_SEARCH_URL",
    "https://www.seloger.com/classified-search"
    "?distributionTypes=Rent"
    "&estateTypes=House,Apartment"
    "&locations=eyJwbGFjZUlkcyI6WyJBRDA4RlI3NSIsIkFEMDhGUjc3IiwiQUQwOEZSNzgiLCJBRDA4RlI5MSIsIkFEMDhGUjkyIiwiQUQwOEZSOTMiLCJBRDA4RlI5NCIsIkFEMDhGUjk1Il19"
    "&priceMax=1100"
    "&projectTypes=Stock"
    "&spaceMin=25",
)

# Default PAP.fr search URL (IDF entier, meublé, max 1100€)
DEFAULT_SEARCH_PAP_URL: str = os.getenv(
    "PAP_SEARCH_URL",
    # Region code IDF = g471 (g439 était Paris-only malgré le slug "ile-de-france").
    # Couvre 75/77/78/91/92/93/94/95. Pas de surface-min → catch sub-25m² (14-24m²).
    "https://www.pap.fr/annonce/locations-ile-de-france-g471"
    "?loyer-max=1100&ameublement=meuble",
)

# Default Bien'ici search URL (Paris IDF, meublé, 14m²+, max 1000€)
# Filter relâché : surface-min 25→14 pour catch sub-25m² listings (Charenton 14m² case)
DEFAULT_SEARCH_BIENICI_URL: str = os.getenv(
    "BIENICI_SEARCH_URL",
    "https://www.bienici.com/recherche/location/ile-de-france"
    "?prix-max=1100&surface-min=14",
)

# Default Logic-Immo search URL (IDF entier, max 1100€).
# 2026-05-05 : `AD08FR12` = Île-de-France region (was AD08FR31096 = Paris seul).
# Live curl_cffi probe is 403-blocked by DataDome, but the scraper goes through
# Camoufox + the React testid scheme (data-base attribute holds the URL-encoded
# detail URL). Aviv geo IDs follow `AD0{level}FR{insee}` — `AD08` is region tier,
# Paris ville is `AD0{?}FR75056` etc.
DEFAULT_SEARCH_LOGICIMMO_URL: str = os.getenv(
    "LOGICIMMO_SEARCH_URL",
    "https://www.logic-immo.com/classified-search"
    "?distributionTypes=Rent"
    "&estateTypes=House,Apartment"
    "&locations=AD08FR12"
    "&priceMax=1100",
)

# ── Phase 2: student / young-pro platforms ────────────────────────────────────
# Empty string disables the source — dispatcher skips empty URLs in
# _campaign_sources(). Studapart and Paris Attitude have site-specific parsers;
# the others are disabled by default until proper parsers are written.
# Studapart — public listings via Camoufox (stealth Playwright gets
# fingerprinted and served an SEO fallback page).
# 2026-05-05 : passé à `ile-de-france` (couvrait juste Paris-75 avant).
# Live probe HTTP 200 avec listings pour 75/77/78/91/92/93/94/95.
# NOTE: l'API interne réplique côté scraper utilise un template capturé une
# fois via Camoufox — l'URL ici sert de seed page pour la capture initiale.
DEFAULT_SEARCH_STUDAPART_URL: str = os.getenv(
    "STUDAPART_SEARCH_URL",
    "https://www.studapart.com/fr/logement-etudiant-ile-de-france",
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
# 2026-05-05 — pas d'URL IDF/région : le site n'indexe que des slugs de ville
# (paris-75, lyon-69, etc.). `/logement-etudiant/ile-de-france.html` et
# `/logement-etudiant/region-11.html` retournent HTTP 404. Reste donc Paris-75
# uniquement. Faute d'aggrégat IDF, on garde la cible historique.
DEFAULT_SEARCH_IMMOJEUNE_URL: str = os.getenv(
    "IMMOJEUNE_SEARCH_URL",
    "https://www.immojeune.com/logement-etudiant/paris-75.html",
)

# LocService — owner-direct rentals, French-market.
# 2026-05-05 — pas d'URL IDF aggrégée (`/ile-de-france/...` 404, `/region-11/`
# slug renvoie l'Aude). En revanche chaque département IDF a son propre slug
# (`paris-75`, `hauts-de-seine-92`, etc.). Le scraper a été modifié pour
# itérer les 8 départements IDF en parallèle ; cette URL sert de fallback
# si la sentinelle dept-iter détecte une URL custom.
DEFAULT_SEARCH_LOCSERVICE_URL: str = os.getenv(
    "LOCSERVICE_SEARCH_URL",
    "https://www.locservice.fr/paris-75/location-appartement.html",
)

# EntreParticuliers — particulier-à-particulier.
# 2026-05-05 — Le scraper a été élargi de "20 arrondissements Paris" à "8
# départements IDF" (75/77/78/91/92/93/94/95). Chaque page dept renvoie ~12
# listings sous le pattern `/annonces-immobilieres/appartement/location/{slug}/{slug2}/ref-{id}`
# (le path canonique URL d'index est en /location/appartement/{dept}-{nn}).
# Pre-existing regex bug (1-segment vs 2-segment slug) corrigé en passant.
DEFAULT_SEARCH_ENTREPARTICULIERS_URL: str = os.getenv(
    "ENTREPARTICULIERS_SEARCH_URL",
    "https://www.entreparticuliers.com/annonces-immobilieres/location/appartement/paris-75",
)

# L'Adresse (agency network) — 40 listings/page on IDF search.
DEFAULT_SEARCH_LADRESSE_URL: str = os.getenv(
    "LADRESSE_SEARCH_URL",
    "https://www.ladresse.com/recherche/location/appartement/ile-de-france",
)

# Century 21 (agency network) — paginates 9 pages.
# 2026-05-05 — Le site n'a pas d'URL région (toutes les variantes
# `r-ile-de-france`, `d-ile-de-france`, `v-ile-de-france` retournent HTTP 410).
# Seul le pattern `v-{ville}` fonctionne. Le scraper a été élargi pour itérer
# 12 villes IDF clés (Paris, Versailles, Nanterre, Créteil, Cergy, Meaux, etc.)
# en parallèle. Cette URL sert de seed Paris ; les autres villes sont
# hardcoded dans le scraper.
DEFAULT_SEARCH_CENTURY21_URL: str = os.getenv(
    "CENTURY21_SEARCH_URL",
    "https://www.century21.fr/annonces/f/location/v-paris/",
)

# Wizi.io — managed-rental aggregator. Public API at app.wizi.eu/api/public.
# 2026-05-05 — L'API n'a pas de filtre région ; elle utilise lat/lon comme
# *centroïde de tri* (par distance). Avec Paris (48.8566, 2.3522), les premiers
# ~80 résultats paginés sont en IDF (75/77/78/91/92/93/94/95) avant de
# déborder sur le national. On garde donc le centroïde Paris : c'est déjà
# le couvercle IDF naturel. Le param `city=Paris` est purement un label UI
# côté SPA (le backend ignore ce paramètre).
DEFAULT_SEARCH_WIZI_URL: str = os.getenv(
    "WIZI_SEARCH_URL",
    "https://desk.wizi.eu/#/app/search?city=Paris&lat=48.856614&long=2.3522219",
)

# Laforêt (agency network) — server-rendered, GTM data attrs embedded.
# 2026-05-05 — passé Paris-only → IDF entier via la route `/region/`. Live
# probe: HTTP 200, ~2567 annonces dans la titre Laforêt, listings depuis
# 75/77/78/91/92/93/94/95. Le scraper applique le cap de 1100€ client-side
# car `filter[max]=N` casse le scope région.
DEFAULT_SEARCH_LAFORET_URL: str = os.getenv(
    "LAFORET_SEARCH_URL",
    "https://www.laforet.com/region/location-appartement-ile-de-france",
)

# Guy Hoquet (agency network) — XHR endpoint with Laravel session filters.
# 2026-05-05 — Bascule Paris → IDF region. Live probe via /biens/search-localization
# avec q=ile-de-france retourne `{slug:"11_c1", region_code:"11"}`. Le scraper
# utilise désormais `_GH_IDF_SLUG = "11_c1"` (au lieu de paris-75056_c4) ;
# cette URL n'est qu'un référent SPA. Listings vérifiés depuis 92/77/91/93/94.
DEFAULT_SEARCH_GUYHOQUET_URL: str = os.getenv(
    "GUYHOQUET_SEARCH_URL",
    "https://www.guy-hoquet.com/annonces/location/ile-de-france/?priceMax=1100",
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

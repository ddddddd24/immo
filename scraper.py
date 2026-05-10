"""Multi-source scraper: LeBonCoin + SeLoger.

Default engine : Playwright (free, runs locally).
Fallback engine: Apify (cloud, set USE_APIFY=true in .env to force it).
Source is auto-detected from the URL (leboncoin.fr vs seloger.com).
"""
import asyncio
import json
import logging
import random
import re as _re
import time
from pathlib import Path
from typing import Optional

import httpx
from playwright.async_api import async_playwright
from playwright_stealth import Stealth

import config
from agent import Listing

logger = logging.getLogger(__name__)

_APIFY_BASE = "https://api.apify.com/v2"
_POLL_INTERVAL = 5
_MAX_WAIT = 300

# Persistent browser profiles — keeps cookies/session across runs.
# Per-site dirs to avoid cookie pollution across portals (LBC, PAP, Bien'ici, Logic-Immo).
def _user_data_dir(site: str) -> str:
    return str(Path(f"data/browser_profile_{site}"))


# ─── Shared Playwright browser helper ────────────────────────────────────────

async def _handle_cookie_banner(page) -> None:
    """Dismiss the Didomi consent popup if present."""
    try:
        btn = page.locator("#didomi-notice-agree-button")
        if await btn.is_visible(timeout=4_000):
            await asyncio.sleep(random.uniform(0.8, 1.5))  # human pause
            await btn.click()
            logger.debug("Cookie banner dismissed")
    except Exception:
        pass  # banner not present or already dismissed


def _extract_next_data(html: str) -> Optional[dict]:
    """Parse the __NEXT_DATA__ JSON blob from a rendered HTML page."""
    match = _re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, _re.DOTALL)
    if not match:
        return None
    try:
        return json.loads(match.group(1))
    except json.JSONDecodeError:
        return None


async def _pw_get_next_data(url: str, site: str = "leboncoin") -> Optional[dict]:
    """Fetch a Next.js page and return parsed __NEXT_DATA__.

    Two-stage: Playwright + stealth first (fast, persistent cookies). If the
    page returns no __NEXT_DATA__ (DataDome fallback page), retry via
    Camoufox which fingerprint-masks more aggressively. Returns the parsed
    JSON dict or None if both engines fail.
    """
    profile_dir = _user_data_dir(site)
    Path(profile_dir).mkdir(parents=True, exist_ok=True)

    # ─── Attempt 1: Playwright + stealth ──────────────────────────────────────
    html: Optional[str] = None
    try:
        async with async_playwright() as pw:
            ctx = await pw.chromium.launch_persistent_context(
                user_data_dir=profile_dir,
                headless=True,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--window-size=1280,800",
                ],
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/131.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1280, "height": 800},
                locale="fr-FR",
            )
            page = await ctx.new_page()
            await Stealth().apply_stealth_async(page)
            try:
                await page.goto(url, wait_until="load", timeout=30_000)
                await _handle_cookie_banner(page)
                await asyncio.sleep(random.uniform(1.5, 3.0))
                html = await page.content()
            finally:
                await ctx.close()
    except Exception as exc:
        logger.warning("[%s] Playwright fetch raised: %s — will try Camoufox", site, exc)

    if html:
        data = _extract_next_data(html)
        if data is not None:
            return data
        logger.warning(
            "[%s] __NEXT_DATA__ missing in Playwright HTML — DataDome likely "
            "served a fallback page. Trying Camoufox.",
            site,
        )

    # ─── Attempt 2: Camoufox fallback (anti-detect Firefox) ──────────────────
    # Pass `site=` explicitly — for LBC and SeLoger this routes to the warm
    # per-site context (DataDome cookie reused across scrapes).
    cam_html = await _fetch_html_with_camoufox(url, post_delay=(3.0, 5.0), site=site)
    if not cam_html:
        logger.warning("[%s] Camoufox also failed to retrieve HTML for %s", site, url)
        return None
    data = _extract_next_data(cam_html)
    if data is None:
        logger.warning("[%s] __NEXT_DATA__ still missing even via Camoufox", site)
    return data


# ─── Source detection ─────────────────────────────────────────────────────────

def _is_seloger(url: str) -> bool:
    return "seloger.com" in url


# ─── Listing normalisation ────────────────────────────────────────────────────

def _dig(obj, *path, default=None):
    """Walk a nested dict chain safely. Returns `default` the moment any level
    isn't a dict (instead of crashing on '.get'). Used everywhere we parse
    third-party JSON whose schema can drift on us.
    """
    for key in path:
        if not isinstance(obj, dict):
            return default
        obj = obj.get(key)
    return obj if obj is not None else default


def _ensure_dict(value) -> dict:
    """Return value if it's a dict, else an empty dict."""
    return value if isinstance(value, dict) else {}


def _ensure_list(value) -> list:
    """Return value if it's a list, else an empty list."""
    return value if isinstance(value, list) else []


def _to_int_safe(value) -> Optional[int]:
    """Coerce string/int/None → int or None. Used for surface (m²) parsing."""
    if value is None or value == "":
        return None
    try:
        return int(str(value).strip())
    except (ValueError, TypeError):
        return None


# ─── Geocoding helpers — city/arr → ZIP ──────────────────────────────────────
# `is_critical_zone()` and dashboard filters key off a 5-digit zip embedded in
# `Listing.location`. Several scrapers (inli, lodgis card view, wizi list view,
# laforet 75xxx-only, parisattitude) used to ship listings with city-only
# strings, hiding 2500+ rows from dept-based filtering. Helpers below normalise
# any (city, arrondissement, raw zip) tuple into a "City, ZZZZZ" suffix.

# Major IDF communes — 92/93/94/95 within or near our preferred zones, plus a
# handful of common 77/78/91 fallbacks so we don't drop those listings entirely
# (the dept-prefix dealbreaker in preferences.is_critical_zone takes care of
# them once a zip is present).
_IDF_CITY_ZIP: dict[str, str] = {
    # 92 — Hauts-de-Seine
    "antony": "92160",
    "asnieres-sur-seine": "92600", "asnieres": "92600",
    "bagneux": "92220",
    "bois-colombes": "92270",
    "boulogne-billancourt": "92100", "boulogne": "92100",
    "bourg-la-reine": "92340",
    "chatenay-malabry": "92290",
    "chatillon": "92320",
    "chaville": "92370",
    "clamart": "92140",
    "clichy": "92110",
    "colombes": "92700",
    "courbevoie": "92400",
    "fontenay-aux-roses": "92260",
    "garches": "92380",
    "gennevilliers": "92230",
    "issy-les-moulineaux": "92130", "issy": "92130",
    "la-garenne-colombes": "92250",
    "le-plessis-robinson": "92350", "plessis-robinson": "92350",
    "levallois-perret": "92300", "levallois": "92300",
    "malakoff": "92240",
    "meudon": "92190",
    "montrouge": "92120",
    "nanterre": "92000",
    "neuilly-sur-seine": "92200", "neuilly": "92200",
    "puteaux": "92800",
    "rueil-malmaison": "92500", "rueil": "92500",
    "saint-cloud": "92210",
    "sceaux": "92330",
    "sevres": "92310",
    "suresnes": "92150",
    "vanves": "92170",
    "villeneuve-la-garenne": "92390",
    # 93 — Seine-Saint-Denis
    "aubervilliers": "93300",
    "aulnay-sous-bois": "93600",
    "bagnolet": "93170",
    "le-blanc-mesnil": "93150", "blanc-mesnil": "93150",
    "bobigny": "93000",
    "bondy": "93140",
    "le-bourget": "93350",
    "clichy-sous-bois": "93390",
    "drancy": "93700",
    "epinay-sur-seine": "93800", "epinay": "93800",
    "gagny": "93220",
    "ile-saint-denis": "93450", "l-ile-saint-denis": "93450",
    "le-pre-saint-gervais": "93310", "pre-saint-gervais": "93310",
    "le-raincy": "93340", "raincy": "93340",
    "les-lilas": "93260",
    "les-pavillons-sous-bois": "93320", "pavillons-sous-bois": "93320",
    "livry-gargan": "93190",
    "montfermeil": "93370",
    "montreuil": "93100",
    "neuilly-plaisance": "93360",
    "neuilly-sur-marne": "93330",
    "noisy-le-grand": "93160",
    "noisy-le-sec": "93130",
    "pantin": "93500",
    "pierrefitte-sur-seine": "93380", "pierrefitte": "93380",
    "romainville": "93230",
    "rosny-sous-bois": "93110",
    "saint-denis": "93200",
    "saint-ouen": "93400", "saint-ouen-sur-seine": "93400",
    "sevran": "93270",
    "stains": "93240",
    "tremblay-en-france": "93290",
    "villemomble": "93250",
    "villepinte": "93420",
    "villetaneuse": "93430",
    # 94 — Val-de-Marne
    "alfortville": "94140",
    "arcueil": "94110",
    "boissy-saint-leger": "94470",
    "bonneuil-sur-marne": "94380",
    "bry-sur-marne": "94360",
    "cachan": "94230",
    "champigny-sur-marne": "94500", "champigny": "94500",
    "charenton-le-pont": "94220", "charenton": "94220",
    "chennevieres-sur-marne": "94430",
    "choisy-le-roi": "94600",
    "creteil": "94000",
    "fontenay-sous-bois": "94120",
    "fresnes": "94260",
    "gentilly": "94250",
    "ivry-sur-seine": "94200", "ivry": "94200",
    "joinville-le-pont": "94340", "joinville": "94340",
    "kremlin-bicetre": "94270", "le-kremlin-bicetre": "94270",
    "le-perreux-sur-marne": "94170", "perreux-sur-marne": "94170", "le-perreux": "94170",
    "limeil-brevannes": "94450",
    "maisons-alfort": "94700",
    "nogent-sur-marne": "94130", "nogent": "94130",
    "orly": "94310",
    "saint-mande": "94160",
    "saint-maur-des-fosses": "94100", "saint-maur": "94100",
    "saint-maurice": "94410",
    "sucy-en-brie": "94370",
    "thiais": "94320",
    "valenton": "94460",
    "villejuif": "94800",
    "villeneuve-saint-georges": "94190",
    "vincennes": "94300",
    "vitry-sur-seine": "94400", "vitry": "94400",
    # 95 — Val-d'Oise
    "argenteuil": "95100",
    "bezons": "95870",
    "cergy": "95000",
    "deuil-la-barre": "95170",
    "eaubonne": "95600",
    "enghien-les-bains": "95880",
    "ermont": "95120",
    "franconville": "95130",
    "garges-les-gonesse": "95140",
    "gonesse": "95500",
    "herblay-sur-seine": "95220", "herblay": "95220",
    "montmorency": "95160",
    "pontoise": "95300",
    "saint-gratien": "95210",
    "sannois": "95110",
    "sarcelles": "95200",
    "soisy-sous-montmorency": "95230",
    "taverny": "95150",
    "villiers-le-bel": "95400",
    # 77/78/91 (carriers — most are dealbreakers but need zip to be caught)
    "carrieres-sous-poissy": "78955",
    "chelles": "77500",
    "evry-courcouronnes": "91000", "evry": "91000",
    "massy": "91300",
    "meaux": "77100",
    "melun": "77000",
    "palaiseau": "91120",
    "saint-quentin-en-yvelines": "78280",
    "trappes": "78190",
    "versailles": "78000",
}

# Strip accents / punctuation for table lookups — listings often arrive as
# "Saint-Maur" / "saint maur" / "SAINT-MAUR" / "Saint Maur des Fossés".
_ACCENT_MAP = str.maketrans(
    "àâäéèêëîïôöùûüçÀÂÄÉÈÊËÎÏÔÖÙÛÜÇ",
    "aaaeeeeiioouuucAAAEEEEIIOOUUUC",
)


def _normalize_city_key(city: str) -> str:
    """Normalize city name for _IDF_CITY_ZIP lookup: lowercase, no accents,
    spaces/apostrophes → hyphens, strip 'le/la/les' prefix variants."""
    if not city:
        return ""
    s = city.strip().translate(_ACCENT_MAP).lower()
    s = _re.sub(r"[''`]", "-", s)
    s = _re.sub(r"\s+", "-", s)
    s = _re.sub(r"-+", "-", s).strip("-")
    return s


def _paris_arrondissement_to_zip(label: str) -> Optional[str]:
    """Map a Paris arrondissement label → 5-digit zip.
    Accepts: 'Paris 1', 'Paris 1°', 'Paris 1er', 'Paris 12eme', 'Paris 12ème',
    'paris-12', '1er arrondissement', etc. Returns None if no match."""
    if not label:
        return None
    blob = label.translate(_ACCENT_MAP).lower()
    # Plain "paris" with no number → ambiguous, skip
    m = _re.search(r"paris[\s\-]*(\d{1,2})\b", blob)
    if not m:
        m = _re.search(r"\b(\d{1,2})\s*(?:er|eme|e|°)?\s*arrondissement", blob)
    if not m:
        return None
    try:
        n = int(m.group(1))
    except ValueError:
        return None
    if 1 <= n <= 20:
        return f"750{n:02d}"
    return None


def _zip_for_location(text: str) -> Optional[str]:
    """Resolve a free-form location string → 5-digit zip.

    Order: explicit 5-digit match → Paris arrondissement → IDF city table.
    Used by inli/lodgis when the source surfaces only a city name."""
    if not text:
        return None
    # Already-embedded zip? Re-use it.
    zm = _re.search(r"\b(\d{5})\b", text)
    if zm:
        return zm.group(1)
    if (z := _paris_arrondissement_to_zip(text)):
        return z
    # Try every word group as a city key (handles "Studio Saint-Denis 25m²")
    for token in _re.findall(r"[A-Za-zÀ-ÿ][A-Za-zÀ-ÿ\-' ]{2,40}", text):
        key = _normalize_city_key(token)
        if key in _IDF_CITY_ZIP:
            return _IDF_CITY_ZIP[key]
    return None


def detect_housing_type(title: str, description: str = "") -> tuple[str, Optional[int]]:
    """Classify a listing by housing type. Returns (type, roommate_count).

    Categories (priority order — most specific first):
      'coliving'  — coliving spaces (often resemble coloc but managed)
      'coloc'     — colocation, with optional roommate count
      'residence' — student residence (NOMAD, Studéa, Twenty Campus...)
      'chambre'   — single room rental (in someone's place)
      'studio'    — studio (single-room apartment)
      'T1'..'T5'  — typology (T1=1pce, T2=2pces, ...)
      ''          — unclassified
    """
    blob = f"{title} {description}".lower()

    if "coliving" in blob:
        m = _re.search(r"(\d+)\s*(?:chambres?|colocataires?|personnes?)", blob)
        return "coliving", _to_int_safe(m.group(1)) if m else None

    if _re.search(r"\bcoloc(?:ation|ataire)?", blob):
        for pat in (
            r"coloc\w*\s+(?:à|de|à\s+\d+|pour)?\s*(\d+)\s*(?:chambres?|pers|colocataires?)",
            r"(\d+)\s*colocataires?",
            r"(\d+)\s*chambres?\s+\w*coloc",
            r"coloc\w*\s+(\d+)\s*chambres?",
        ):
            m = _re.search(pat, blob)
            if m:
                n = _to_int_safe(m.group(1))
                if n and 2 <= n <= 10:
                    return "coloc", n
        return "coloc", None

    # Per-person pricing OR explicit X-person occupancy = coloc disguised
    # Patterns: "700 / pers", "700€/pers", "par personne", "pour 3 personnes",
    # "X chambres pour X personnes" (where X > 1)
    if _re.search(r"/\s*pers\b|par\s+personne|/\s*personne", blob):
        # Try to extract the # of persons
        m = _re.search(r"(\d+)\s*(?:chambres?|personnes?|colocataires?)", blob)
        n = _to_int_safe(m.group(1)) if m else None
        return "coloc", n if (n and 2 <= n <= 10) else None
    if _re.search(r"(?:pour|à|de|avec)\s+(\d+)\s+personnes?\b", blob):
        m = _re.search(r"(?:pour|à|de|avec)\s+(\d+)\s+personnes?\b", blob)
        n = _to_int_safe(m.group(1)) if m else None
        if n and n > 2:  # 1-2 personnes = couple, OK
            return "coloc", n

    if _re.search(r"r[ée]sidence\s+(?:[ée]tudiante|jeune|service)", blob):
        return "residence", None

    # Chambre — anywhere in first 30 chars of title (more permissive than before)
    if _re.search(r"^\s*chambre\b", title.lower()):
        return "chambre", None
    if _re.search(r"\bchambre\s+(?:à\s+lou|chez|libre|dispo|meubl|priv)", blob):
        return "chambre", None

    # T1/T2/etc — common French apartment typology shorthand
    m = _re.search(r"\b(t|f)\s?([1-5])\b", blob)
    if m:
        return f"T{m.group(2)}", None

    if _re.search(r"\bstudio\b", blob):
        return "studio", None

    if _re.search(r"(\d+)\s*pi[èe]ces?", blob):
        m = _re.search(r"(\d+)\s*pi[èe]ces?", blob)
        n = _to_int_safe(m.group(1))
        if n and 1 <= n <= 5:
            return f"T{n}", None

    return "", None


def _parse_price(raw) -> Optional[int]:
    """Extract a single rent value from raw input.

    Walks the string left-to-right, accumulating digits and thousand-separators
    (space, NBSP, dot, comma) for the FIRST contiguous number. Stops at the
    first non-numeric break after at least one digit — this prevents
    `"2100€ + charges 350€"` from yielding 2100350.

    Returns None for values outside a plausible monthly-rent band [50, 50000].
    """
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        value = int(raw)
        return value if 50 <= value <= 50000 else None

    s = str(raw)
    seen_digit = False
    chars: list[str] = []
    for c in s:
        if c.isdigit():
            chars.append(c)
            seen_digit = True
        elif c in (" ", "\xa0", ".", ","):
            if seen_digit:
                chars.append(c)
            # else: leading separator, ignore
        else:
            if seen_digit:
                break
            # else: leading non-digit (e.g. currency), keep scanning

    if not seen_digit:
        return None
    cleaned = "".join(c for c in chars if c.isdigit())
    if not cleaned:
        return None
    try:
        value = int(cleaned)
    except ValueError:
        return None
    return value if 50 <= value <= 50000 else None


def _ad_to_listing(ad: dict, url: str = "") -> Optional[Listing]:
    """Convert a LBC ad dict (from __NEXT_DATA__) to a Listing.

    Hardened against schema drift: every nested field is type-checked with
    _ensure_dict / _ensure_list before chained .get() calls. A single
    malformed field returns reasonable defaults instead of crashing.
    """
    if not isinstance(ad, dict):
        return None
    lbc_id = str(ad.get("list_id") or ad.get("id") or "")
    if not lbc_id:
        return None

    if not url:
        url = f"https://www.leboncoin.fr/ad/locations/{lbc_id}"

    # Price + Surface + dealbreaker fields — all in attributes array.
    price_raw = None
    surface = None
    floor = None
    elevator = None
    furnished = None
    heating_type = None
    charges_included = None
    for attr in _ensure_list(ad.get("attributes")):
        if not isinstance(attr, dict):
            continue
        key = attr.get("key")
        values = attr.get("values")
        first_val = values[0] if isinstance(values, list) and values else None
        v = attr.get("value") or first_val
        if key == "price" and price_raw is None:
            price_raw = attr.get("value_label") or first_val
        elif key in ("square", "surface") and surface is None:
            surface = _to_int_safe(first_val or attr.get("value_label"))
        elif key == "floor_number":
            floor = _to_int_safe(v)
        elif key == "elevator":
            elevator = (str(v) == "1")
        elif key == "furnished":
            furnished = (str(v) in ("1", "furnished", "meublé", "true"))
        elif key == "heating_type":
            heating_type = str(v) if v else None
        elif key == "charges_included":
            charges_included = (str(v) == "1")
    # Apply dealbreakers
    if furnished is False:
        return None  # explicitly non-meublé
    if isinstance(floor, int) and floor > 3 and elevator is False:
        return None  # étage>3 sans ascenseur
    if price_raw is None:
        p = ad.get("price")
        if isinstance(p, list):
            price_raw = p[0] if p else None
        else:
            price_raw = p

    loc = _ensure_dict(ad.get("location"))
    location = ", ".join(filter(None, [loc.get("city"), loc.get("zipcode")]))

    owner = _ensure_dict(ad.get("owner"))
    seller_name = owner.get("name") or owner.get("store_name") or ""
    seller_type_hint = owner.get("type") or ""

    # Photos: ad.images may be a dict-of-lists, a flat list, or absent entirely
    images_field = ad.get("images")
    images_pool: list = []
    if isinstance(images_field, dict):
        images_pool = (
            _ensure_list(images_field.get("urls_large"))
            or _ensure_list(images_field.get("urls"))
        )
    elif isinstance(images_field, list):
        images_pool = images_field
    images: list[str] = []
    for img in images_pool:
        if isinstance(img, dict):
            u = img.get("url") or img.get("thumb_url") or ""
        elif isinstance(img, str):
            u = img
        else:
            u = ""
        if u:
            images.append(u)

    # Append structured tags to description so the scoring LLM picks up
    # floor / elevator / furnished / heating without having to re-parse.
    description = ad.get("body") or ""
    tags = []
    if floor is not None: tags.append(f"[ÉTAGE: {floor}]")
    if elevator is True: tags.append("[ASCENSEUR: oui]")
    elif elevator is False: tags.append("[ASCENSEUR: non]")
    if furnished is True: tags.append("[MEUBLÉ: oui]")
    if heating_type: tags.append(f"[CHAUFFAGE: {heating_type}]")
    if charges_included is True: tags.append("[CHARGES: comprises]")
    elif charges_included is False: tags.append("[CHARGES: en sus]")
    if tags:
        description = (description + "\n" + " ".join(tags)).strip()

    # Publication date: LBC's `first_publication_date` (ISO 8601 timestamp)
    pub_at = ad.get("first_publication_date") or ad.get("index_date")

    return Listing(
        lbc_id=lbc_id,
        title=ad.get("subject") or ad.get("title") or "",
        description=description,
        price=_parse_price(price_raw),
        location=location,
        seller_name=seller_name,
        url=url,
        seller_type_hint=seller_type_hint,
        images=images,
        surface=surface,
        published_at=pub_at if isinstance(pub_at, str) else None,
    )


def _item_to_listing(item: dict) -> Optional[Listing]:
    """Convert a raw Apify item dict to a Listing."""
    lbc_id = str(item.get("id") or item.get("listId") or item.get("list_id") or "")
    if not lbc_id:
        return None
    url = item.get("url") or item.get("link") or f"https://www.leboncoin.fr/ad/locations/{lbc_id}"
    owner = item.get("owner") or {}
    return Listing(
        lbc_id=lbc_id,
        title=item.get("title") or item.get("subject") or "",
        description=item.get("body") or item.get("description") or "",
        price=_parse_price(item.get("price") or item.get("priceRaw")),
        location=item.get("location") or item.get("city") or item.get("area") or "",
        seller_name=owner.get("name") or item.get("sellerName") or "",
        url=url,
        seller_type_hint=owner.get("type") or item.get("sellerType") or "",
    )


# ─── SeLoger listing normalisation ───────────────────────────────────────────

def _seloger_extract_price(ad: dict) -> Optional[int]:
    """Extract the monthly rent from a SeLoger ad. The default `hardFacts.price.value`
    sometimes shows charges-only (e.g. 200€) for coloc-style listings; fall back to
    the formatted strings that include /mois or the monthlyRent field if available."""
    hf = ad.get("hardFacts") or {}
    pd = hf.get("price") or {}
    # Try multiple sources in order of reliability
    candidates = [
        ad.get("monthlyRent"),
        ad.get("rent"),
        pd.get("value"),
        pd.get("formatted"),
        pd.get("ariaLabel"),
    ]
    for c in candidates:
        p = _parse_price(c)
        if p and 200 <= p <= 50000:
            return p
    return None


def _seloger_walk_facts(ad: dict) -> dict:
    """Walk a SeLoger ad dict recursively and pull all {type, value} pairs
    into a flat lookup. Useful for extracting numberOfFloors, availability,
    livingSpace etc. without depending on path."""
    out: dict = {}
    def _walk(o):
        if isinstance(o, dict):
            t = o.get("type")
            v = o.get("value")
            if isinstance(t, str) and v is not None and t not in out:
                out[t] = v
            for vv in o.values():
                _walk(vv)
        elif isinstance(o, list):
            for vv in o:
                _walk(vv)
    _walk(ad)
    return out


def _seloger_ad_to_listing(ad: dict, url: str = "") -> Optional[Listing]:
    """Convert a SeLoger classifiedsData entry to a Listing.

    Applies dealbreakers from search-API data:
      - availability after Sept 2026 → return None
      - floor > 3 (no elevator info available here, would need detail page)

    Structure (from classifiedsData in pageProps):
      id, url, location.address.{city, zipCode}, hardFacts.price.value,
      hardFacts.{title, keyfacts}, cardProvider.title (seller name),
      legacyTracking.id (numeric legacy id), and nested {type, value}
      items for numberOfFloors / availability / livingSpace.
    """
    sl_id = str(ad.get("id") or ad.get("classifiedId") or "")
    if not sl_id:
        return None

    # Pull facts (numberOfFloors, availability, etc.)
    facts = _seloger_walk_facts(ad)

    # Availability: parse "dès le DD/MM/YYYY" or "à partir du …".
    # > 2026-09 = dealbreaker. Otherwise persist as available_from (YYYY-MM-DD).
    avail_raw = facts.get("availability", "")
    avail_from = None
    if isinstance(avail_raw, str):
        m = _re.search(r"(\d{2})/(\d{2})/(\d{4})", avail_raw)
        if m:
            yyyy_mm_dd = f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
            if yyyy_mm_dd[:7] > "2026-09":
                return None  # available too late
            avail_from = yyyy_mm_dd

    # Prefix to avoid ID collision with LBC IDs in the DB
    listing_id = f"sl_{sl_id}"

    if not url:
        url = ad.get("url") or f"https://www.seloger.com/annonces/locations/{sl_id}.htm"

    # Price — in hardFacts.price.value as "990 €"
    hard_facts = ad.get("hardFacts") or {}
    price_dict = hard_facts.get("price") or {}
    price_raw = price_dict.get("value") or price_dict.get("formatted") or ""

    # Location — location.address.{city, zipCode}
    loc = ad.get("location") or {}
    address = loc.get("address") or {}
    city = address.get("city") or address.get("localityName") or ""
    zipcode = address.get("zipCode") or address.get("postalCode") or ""
    location = ", ".join(filter(None, [city, zipcode]))

    # Seller — cardProvider.title
    card_provider = ad.get("cardProvider") or {}
    seller_name = card_provider.get("title") or ""

    # Title: combine hardFacts.title with keyfacts for a useful description
    hf_title = hard_facts.get("title") or ""
    keyfacts = hard_facts.get("keyfacts") or []
    title = hf_title + (" — " + ", ".join(keyfacts) if keyfacts else "")

    # Surface: extract from keyfacts (entries like '37 m²')
    surface = None
    for kf in keyfacts if isinstance(keyfacts, list) else []:
        m = _re.search(r"(\d+)(?:[,.]\d+)?\s*m²", str(kf))
        if m:
            surface = _to_int_safe(m.group(1))
            break

    # Description: not available in search results (only on listing detail page).
    # We append structured tags from the search-API facts so the scoring LLM
    # picks up floor / availability without an extra detail-page fetch.
    description = ad.get("description") or ad.get("descriptif") or ""
    tags = []
    floors_raw = facts.get("numberOfFloors", "")
    if isinstance(floors_raw, str) and floors_raw:
        if floors_raw.upper().startswith("RDC"):
            tags.append("[ÉTAGE: RDC]")
        else:
            fm = _re.search(r"(\d+)", floors_raw)
            if fm:
                tags.append(f"[ÉTAGE: {fm.group(1)}]")
    if isinstance(avail_raw, str) and avail_raw:
        tags.append(f"[DISPO: {avail_raw}]")
    if tags:
        description = (description + "\n" + " ".join(tags)).strip()

    # Extract photo URLs from gallery.images[].url
    gallery = ad.get("gallery") or {}
    images = [
        img.get("url", "")
        for img in (gallery.get("images") or [])
        if img.get("url")
    ]

    parsed_price = _parse_price(price_raw)
    if parsed_price and surface and parsed_price / surface < 10:
        logger.warning(
            "[SELOGER] %s: anomalous price %d€/%dm² (%.1f€/m²) — likely per-room",
            listing_id, parsed_price, surface, parsed_price / surface,
        )
        parsed_price = None

    # Publication date from metadata.creationDate (when listing first posted)
    pub_at = _dig(ad, "metadata", "creationDate") or _dig(ad, "metadata", "updateDate")

    return Listing(
        lbc_id=listing_id,
        title=title,
        description=description,
        price=parsed_price,
        location=location,
        seller_name=seller_name,
        url=url,
        seller_type_hint="pro",
        source="seloger",
        images=images,
        surface=surface,
        published_at=pub_at if isinstance(pub_at, str) else None,
        available_from=avail_from,
    )


# ─── Playwright scrapers (free) ───────────────────────────────────────────────

async def _search_with_playwright(search_url: str, max_results: int) -> list[Listing]:
    """LBC scraper — paginates via &page=N. The first page warms the
    DataDome challenge (~30s); subsequent pages reuse the persistent profile
    and load in 5-10s each. Cap at 5 pages × ~25 listings = ~125 max."""
    logger.info("[PLAYWRIGHT] Scraping search page: %s", search_url)

    listings: list[Listing] = []
    seen_ids: set[str] = set()
    sep = "&" if "?" in search_url else "?"
    MAX_PAGES = 5

    for page_num in range(1, MAX_PAGES + 1):
        page_url = search_url if page_num == 1 else f"{search_url}{sep}page={page_num}"
        try:
            data = await _pw_get_next_data(page_url)
        except Exception as exc:
            logger.warning("[PLAYWRIGHT] page %d fetch error: %s", page_num, exc)
            break
        if not data:
            if page_num == 1:
                raise RuntimeError(
                    "Playwright could not fetch search page — DataDome blocked or page structure changed"
                )
            break

        props = _ensure_dict(_dig(data, "props", "pageProps"))
        ads = (
            _dig(props, "searchData", "ads")
            or props.get("ads")
            or []
        )
        ads = _ensure_list(ads)
        if not ads:
            if page_num == 1:
                logger.warning("No ads found in __NEXT_DATA__. pageProps keys: %s", list(props.keys()))
            break

        new_count = 0
        for ad in ads:
            if not isinstance(ad, dict):
                continue
            try:
                lbc_id = str(ad.get("list_id") or ad.get("id") or "")
                if not lbc_id or lbc_id in seen_ids:
                    continue
                seen_ids.add(lbc_id)
                url = f"https://www.leboncoin.fr/ad/locations/{lbc_id}"
                listing = _ad_to_listing(ad, url)
                if listing:
                    listings.append(listing)
                    new_count += 1
                    if len(listings) >= max_results:
                        return listings
            except Exception as exc:
                logger.warning("Skipping malformed LBC ad: %s", exc)
        logger.info("[PLAYWRIGHT] page %d: +%d new listings (total %d)", page_num, new_count, len(listings))
        if new_count == 0:  # pagination exhausted
            break

    logger.info("[PLAYWRIGHT] Found %d listings (paginated %d pages)", len(listings), page_num)

    # Phase 2: enrich listings whose API body is empty by fetching the detail
    # page. The LBC search API often returns body='' for new/recent listings;
    # the detail page reliably has the full description in its __NEXT_DATA__.
    # Cap at 30 to avoid runaway cost; concurrent via Camoufox pool.
    empty_body = [l for l in listings if not (l.description or "").strip() or
                  len((l.description or "").strip()) < 80][:30]
    if empty_body:
        logger.info("[LBC] enriching %d listings with empty body via detail page", len(empty_body))
        sem = asyncio.Semaphore(4)  # Camoufox is heavy
        async def _enrich(lst):
            async with sem:
                try:
                    data = await _pw_get_next_data(lst.url, site="leboncoin")
                except Exception:
                    return
                if not data:
                    return
                ad = _dig(data, "props", "pageProps", "ad") or \
                     _dig(data, "props", "pageProps", "adView", "ad") or {}
                body = ad.get("body") if isinstance(ad, dict) else None
                if isinstance(body, str) and len(body) > 50:
                    # Prepend the real body, preserving any [ÉTAGE/MEUBLÉ] tags
                    existing_tags = lst.description or ""
                    lst.description = (body + "\n" + existing_tags).strip()[:1500]
        await asyncio.gather(*(_enrich(l) for l in empty_body), return_exceptions=True)
        n_filled = sum(1 for l in empty_body if len((l.description or "").strip()) >= 80)
        logger.info("[LBC] body enrichment done: %d/%d filled", n_filled, len(empty_body))
    return listings


async def _fetch_single_with_playwright(lbc_url: str) -> Optional[Listing]:
    logger.info("[PLAYWRIGHT] Fetching single listing: %s", lbc_url)
    data = await _pw_get_next_data(lbc_url)
    if not data:
        return None

    props = data.get("props", {}).get("pageProps", {})
    ad = props.get("ad") or props.get("adView", {}).get("ad") or {}
    if not ad:
        logger.warning("No ad object in __NEXT_DATA__. Keys: %s", list(props.keys()))
        return None

    return _ad_to_listing(ad, lbc_url)


# ─── SeLoger Playwright scrapers ─────────────────────────────────────────────

def _seloger_parse_fetcher_html(ssr_html: str) -> Optional[dict]:
    """Shared parser for SeLoger SSR HTML (window['__UFRN_FETCHER__']).
    Returns the decompressed serp dict or None. Used by both curl_cffi
    (fast path) and the Camoufox fallback."""
    m = _re.search(
        r'window\["__UFRN_FETCHER__"\]=JSON\.parse\("(.*?)"\);\s*</script>',
        ssr_html, _re.DOTALL
    )
    if not m:
        logger.warning("[SELOGER] __UFRN_FETCHER__ not found")
        return None

    try:
        raw_str = m.group(1).encode("utf-8").decode("unicode_escape")
        fetcher = json.loads(raw_str)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        logger.warning("[SELOGER] Failed to parse __UFRN_FETCHER__: %s", exc)
        return None

    serp_raw = fetcher.get("data", {}).get("classified-serp-init-data")
    if not serp_raw or not isinstance(serp_raw, str) or not serp_raw.strip():
        logger.warning("[SELOGER] classified-serp-init-data is empty")
        return None

    try:
        import lzstring as _lzs
        decompressed = _lzs.LZString().decompressFromBase64(serp_raw)
        if not decompressed:
            raise ValueError("LZString decompression returned empty result")
        return {"_raw": json.loads(decompressed)}
    except Exception as exc:
        logger.warning("[SELOGER] Failed to decompress/parse serp data: %s", exc)
        return None


async def _pw_get_seloger_data(url: str) -> Optional[dict]:
    """Fetch SeLoger page via curl_cffi (Chrome TLS fingerprint) to bypass
    DataDome — ~5× faster than Camoufox and no browser overhead.
    Extracts listing data from window["__UFRN_FETCHER__"] in the SSR HTML.

    Fallback chain when curl_cffi is blocked (HTTP 403/captcha):
        curl_cffi → warm Camoufox "seloger" context (DataDome cookie reused)
    """
    from curl_cffi.requests import AsyncSession

    ssr_html: Optional[str] = None
    try:
        async with AsyncSession(impersonate="chrome120", timeout=30) as session:
            r = await session.get(url, allow_redirects=True)
            if r.status_code == 200:
                ssr_html = r.text
            else:
                logger.warning("[SELOGER] HTTP %s on %s — trying Camoufox", r.status_code, url[:80])
    except Exception as exc:
        logger.warning("[SELOGER] curl_cffi fetch failed: %s — trying Camoufox", exc)

    # Fast-path parse
    if ssr_html:
        data = _seloger_parse_fetcher_html(ssr_html)
        if data is not None:
            return data
        logger.warning("[SELOGER] curl_cffi got HTML but no fetcher data — trying Camoufox")

    # Camoufox fallback via the warm "seloger" context. First call cold-starts
    # the browser + solves the DataDome challenge (~30s); subsequent calls
    # reuse the cookie jar and run in 5-10s.
    cam_html = await _fetch_html_with_camoufox(
        url, post_delay=(2.0, 4.0), site="seloger", goto_timeout=60_000,
    )
    if not cam_html:
        return None
    return _seloger_parse_fetcher_html(cam_html)


def _seloger_enrich_detail(html: str) -> dict:
    """Extract dealbreaker info from a SeLoger detail page HTML.
    The site uses plain text labels: "Non meublé", "Pas d'ascenseur", etc.,
    plus structured "label / Oui|Non" pairs near `font-toroka` spans."""
    out: dict = {}
    if "Non meublé" in html:
        out["furnished"] = False
    elif "Pas d'ascenseur" in html:
        out["elevator"] = False
    # Combined check (both patterns can coexist)
    if out.get("elevator") is None and "Pas d'ascenseur" in html:
        out["elevator"] = False
    if "Avec ascenseur" in html:
        out["elevator"] = True
    if (m := _re.search(r"Étage\s+(\d+)\s*/\s*\d+", html)):
        try: out["floor"] = int(m.group(1))
        except Exception: pass
    return out


def _seloger_extract_description(html: str) -> Optional[str]:
    """Extract the full SeLoger detail-page description from the SSR JSON.

    The text lives inside the `__UFRN_LIFECYCLE_SERVERREQUEST__` script tag
    at JSON path `*.mainDescription.description`. The HTML data-testid attribute
    truncates at ~10 visible lines — only the JSON path has the full text.
    Typically returns 500-2500 chars of clean French.
    """
    import json as _json
    m = _re.search(
        r'<script id="__UFRN_LIFECYCLE_SERVERREQUEST__"[^>]*>'
        r'window\["__UFRN_LIFECYCLE_SERVERREQUEST__"\]=JSON\.parse\("(.*?)"\);\s*</script>',
        html, _re.DOTALL,
    )
    if not m:
        return None
    try:
        json_str = _json.loads('"' + m.group(1) + '"')
        data = _json.loads(json_str)
    except (_json.JSONDecodeError, ValueError):
        return None

    found: list[str] = []
    def _walk(o):
        if isinstance(o, dict):
            md = o.get("mainDescription")
            if isinstance(md, dict) and isinstance(md.get("description"), str):
                found.append(md["description"])
                return
            for v in o.values():
                if found: return
                _walk(v)
        elif isinstance(o, list):
            for v in o:
                if found: return
                _walk(v)
    _walk(data)
    return found[0] if found else None


async def _search_seloger_with_playwright(search_url: str, max_results: int) -> list[Listing]:
    """SeLoger scraper — paginates via &page=N (default cap = 50 pages × ~30
    listings ≈ 1500 listings, covers IDF inventory). Uses curl_cffi instead of
    Camoufox for 5× speedup."""
    logger.info("[SELOGER] Scraping search page: %s", search_url)
    sep = "&" if "?" in search_url else "?"

    listings: list[Listing] = []
    seen_ids: set[str] = set()
    MAX_PAGES = 50

    async def _fetch_page(page_num: int):
        page_url = f"{search_url}{sep}page={page_num}" if page_num > 1 else search_url
        try:
            return await _pw_get_seloger_data(page_url)
        except Exception as exc:
            logger.warning("[SELOGER] page %d failed: %s", page_num, exc)
            return None

    # Fetch in waves of 5 to balance speed vs. SeLoger rate-limiting tolerance
    page = 1
    while page <= MAX_PAGES and len(listings) < max_results:
        wave = list(range(page, min(page + 5, MAX_PAGES + 1)))
        datas = await asyncio.gather(*(_fetch_page(p) for p in wave))
        any_new = False
        for data in datas:
            if not data:
                continue
            raw = data.get("_raw") or data
            page_props = raw.get("pageProps") or {}
            cps_ids: list = page_props.get("classifieds") or []
            classified_map: dict = page_props.get("classifiedsData") or {}
            for cps_id in cps_ids:
                ad = classified_map.get(cps_id)
                if not ad or not isinstance(ad, dict):
                    continue
                lst = _seloger_ad_to_listing(ad)
                if lst and lst.lbc_id not in seen_ids:
                    seen_ids.add(lst.lbc_id)
                    listings.append(lst)
                    any_new = True
                    if len(listings) >= max_results:
                        break
            if len(listings) >= max_results:
                break
        if not any_new:  # all 5 pages returned same listings → done
            break
        page += 5

    # Phase 2: detail-page enrichment for non-meublé / no-ascenseur
    if listings:
        from curl_cffi.requests import AsyncSession
        sem_d = asyncio.Semaphore(10)
        async with AsyncSession(impersonate="chrome120", timeout=20) as session:
            async def _enrich(lst):
                async with sem_d:
                    try:
                        r = await session.get(lst.url, allow_redirects=True)
                        if r.status_code != 200:
                            return None
                        html = r.text
                        info = _seloger_enrich_detail(html)
                        full_desc = _seloger_extract_description(html)
                    except Exception:
                        return None
                    if info.get("furnished") is False:
                        return lst.lbc_id  # drop: non-meublé
                    floor = info.get("floor")
                    if isinstance(floor, int) and floor > 3 and info.get("elevator") is False:
                        return lst.lbc_id  # drop: étage>3 sans asc
                    if full_desc and len(full_desc) > 50:
                        lst.description = (full_desc + "\n" + (lst.description or "")).strip()
                    tags = []
                    if info.get("elevator") is True: tags.append("[ASCENSEUR: oui]")
                    elif info.get("elevator") is False: tags.append("[ASCENSEUR: non]")
                    if info.get("furnished") is True: tags.append("[MEUBLÉ: oui]")
                    if tags:
                        lst.description = (lst.description + "\n" + " ".join(tags)).strip()
                    return None
            drops = await asyncio.gather(*(_enrich(l) for l in listings))
            drop_set = {d for d in drops if d}
            if drop_set:
                logger.info("[SELOGER] Dropped %d (non-meublé / étage>3 sans asc)", len(drop_set))
                listings = [l for l in listings if l.lbc_id not in drop_set]

    logger.info("[SELOGER] Parsed %d listings (with detail enrichment)", len(listings))
    return listings


async def _fetch_seloger_single_with_playwright(url: str) -> Optional[Listing]:
    logger.info("[SELOGER] Fetching single listing: %s", url)
    data = await _pw_get_seloger_data(url)
    if not data:
        return None

    raw = data.get("_raw") or data
    props = raw.get("props", {}).get("pageProps", raw)
    ad = (
        props.get("classified")
        or props.get("classifiedDetail")
        or props.get("initialProps", {}).get("classified")
        or props.get("annonce")
        or {}
    )
    if not ad:
        logger.warning("[SELOGER] No ad object found. Keys: %s", list(props.keys()))
        return None

    return _seloger_ad_to_listing(ad, url)


# ─── PAP.fr scraper ──────────────────────────────────────────────────────────

def _parse_pap_listing(item, base_url: str = "https://www.pap.fr") -> Optional[Listing]:
    """Parse a PAP.fr BeautifulSoup search-list-item into a Listing."""
    import re as _re2
    from bs4 import BeautifulSoup

    href = ""
    link = item.find("a", href=_re2.compile(r"/annonces/"))
    if link:
        href = link.get("href", "")

    pid_m = _re2.search(r"-r(\d+)$", href)
    if not pid_m:
        return None
    pap_id = f"pap_{pid_m.group(1)}"
    url = base_url + href

    text = item.get_text(separator=" ", strip=True)
    text = _re2.sub(r"\s+", " ", text)

    # Price: "3.100 €" or "900 €" (dot or space as thousands sep, non-breaking spaces)
    price_raw = None
    pm = _re2.search(r"([\d][\d\s\xa0\.]*)\s*€", text)
    if pm:
        price_str = pm.group(1).replace(".", "").replace("\xa0", "").replace(" ", "")
        try:
            price_raw = int(price_str)
        except ValueError:
            pass

    # Location: text after "€ " up to the room/surface info
    location = ""
    loc_m = _re2.search(r"€\s+(.+?)\s+\d+\s*pièces?", text, _re2.IGNORECASE)
    if not loc_m:
        loc_m = _re2.search(r"€\s+([\w\s\(\)éàèùâêîôûç-]+?)\s+\d+\s*m²", text, _re2.IGNORECASE)
    if loc_m:
        location = loc_m.group(1).strip()

    # Description: after surface "m²" marker
    description = ""
    desc_m = _re2.search(r"\d+\s*m²\s+(.+)", text, _re2.DOTALL)
    if desc_m:
        description = desc_m.group(1).strip()

    # Title: build from location + surface like PAP displays it
    title_m = _re2.search(r"(\d+)(?:[,.]\d+)?\s*m²", text)
    surface = title_m.group(1) if title_m else ""
    title = f"Appartement {surface}m² — {location}" if surface and location else text[:80]

    # Images
    images = [
        img.get("src", "")
        for img in item.find_all("img", src=_re2.compile(r"cdn\.pap\.fr"))
        if img.get("src")
    ]
    # Deduplicate cloned carousel images (PAP duplicates for owl-carousel)
    seen_imgs: set = set()
    unique_imgs = []
    for img_url in images:
        if img_url not in seen_imgs:
            seen_imgs.add(img_url)
            unique_imgs.append(img_url)
    images = unique_imgs

    return Listing(
        lbc_id=pap_id,
        title=title,
        description=description,
        price=_parse_price(price_raw),  # range-check [50, 50000] like other parsers
        location=location,
        seller_name="Particulier",  # PAP is always owner-to-renter
        url=url,
        seller_type_hint="particulier",
        source="pap",
        images=images,
        surface=_to_int_safe(surface),
    )


async def _pap_enrich_detail(html: str) -> dict:
    """Extract dealbreaker info from a PAP detail page HTML."""
    out: dict = {}
    # Phone — French phone format anywhere in HTML text (PAP shows it directly)
    if (m := _re.search(r"(?:0[1-9]|\+33[1-9])[\s.\-]?(?:\d{2}[\s.\-]?){4}", _re.sub(r"<[^>]+>", " ", html))):
        # Normalize: strip spaces/dots/dashes
        out["phone"] = _re.sub(r"[\s.\-]", "", m.group(0))
    # JSON-LD additionalProperty
    import json as _json
    for block in _re.findall(r'<script type="application/ld\+json">(.*?)</script>', html, _re.DOTALL):
        try:
            data = _json.loads(block)
        except Exception:
            continue
        for prop in data.get("additionalProperty", []) if isinstance(data, dict) else []:
            if not isinstance(prop, dict): continue
            name = (prop.get("name") or "").lower()
            val = prop.get("value")
            if name == "meublé":
                out["furnished"] = (str(val).lower() == "oui")
            elif name == "surface":
                try: out["surface"] = int(float(val))
                except Exception: pass
    # Regex
    if (m := _re.search(r"(\d+)(?:er|ème|e|ᵉ|ᵈ)?\s*[éE]tage", html)):
        try: out["floor"] = int(m.group(1))
        except Exception: pass
    if "sans ascenseur" in html.lower():
        out["elevator"] = False
    elif _re.search(r"avec ascenseur|\(ascenseur\)", html, _re.I):
        out["elevator"] = True
    if (m := _re.search(r"(\d+)\s*mois\s*minimum", html, _re.I)):
        try: out["min_lease_months"] = int(m.group(1))
        except Exception: pass
    if (m := _re.search(r"Disponible le (\d{1,2})\s+(\w+)\s+(\d{4})", html)):
        # Convert French month name to digit
        months = {"janvier":1,"février":2,"mars":3,"avril":4,"mai":5,"juin":6,
                  "juillet":7,"août":8,"septembre":9,"octobre":10,"novembre":11,"décembre":12}
        mn = months.get(m.group(2).lower())
        if mn: out["available_yyyy_mm"] = f"{m.group(3)}-{mn:02d}-{int(m.group(1)):02d}"
    return out


async def _fetch_pap_single(url: str) -> Optional[Listing]:
    """Fetch a single PAP detail page (curl_cffi → JSON-LD + regex).

    Used by `/add` flow: user pastes a PAP URL, we extract title/price/surface/
    location/description/photos/phone. Falls back to a minimal Listing on
    parse error so the user at least gets the URL persisted.
    """
    from curl_cffi.requests import AsyncSession
    import json as _json

    pid_m = _re.search(r"-r(\d+)(?:[/?#]|$)", url)
    if not pid_m:
        logger.warning("[PAP single] no id in url: %s", url)
        return None
    pap_id = f"pap_{pid_m.group(1)}"

    html = ""
    try:
        async with AsyncSession(impersonate="chrome120", timeout=20) as session:
            r = await session.get(url, allow_redirects=True)
            if r.status_code == 200:
                html = r.text
            else:
                logger.warning("[PAP single] HTTP %s on %s", r.status_code, url[:80])
    except Exception as exc:
        logger.warning("[PAP single] fetch failed: %s", exc)

    if not html:
        return None

    # JSON-LD first (structured)
    import html as _htmllib
    title = ""
    description = ""
    price: Optional[int] = None
    surface: Optional[int] = None
    city = ""
    zip_c = ""
    images: list[str] = []
    for block in _re.findall(
        r'<script type="application/ld\+json">(.*?)</script>', html, _re.DOTALL
    ):
        try:
            data = _json.loads(block)
        except Exception:
            continue
        # Some pages wrap in a list
        if isinstance(data, list):
            data = next((d for d in data if isinstance(d, dict)), {})
        if not isinstance(data, dict):
            continue
        if not description:
            description = (data.get("description") or "").strip()
        if not title:
            title = (data.get("name") or "").strip()
        offers = data.get("offers")
        if isinstance(offers, dict):
            try:
                if price is None:
                    price = int(float(offers.get("price")))
            except (TypeError, ValueError):
                pass
        elif isinstance(offers, list):
            for o in offers:
                if isinstance(o, dict) and o.get("price") and price is None:
                    try:
                        price = int(float(o["price"]))
                        break
                    except (TypeError, ValueError):
                        pass
        addr = data.get("address") or {}
        if isinstance(addr, dict):
            city = city or (addr.get("addressLocality") or "")
            zip_c = zip_c or (addr.get("postalCode") or "")
        img = data.get("image")
        if isinstance(img, list):
            images.extend(u for u in img if isinstance(u, str))
        elif isinstance(img, str):
            images.append(img)
        addl = data.get("additionalProperty")
        if isinstance(addl, list):
            for prop in addl:
                if not isinstance(prop, dict):
                    continue
                name = (prop.get("name") or "").lower()
                val = prop.get("value")
                if name == "surface" and surface is None:
                    try: surface = int(float(val))
                    except (TypeError, ValueError): pass

    # Fallback regex for price if JSON-LD missed
    if price is None:
        text = _re.sub(r"<[^>]+>", " ", html)
        if (m := _re.search(r"([\d][\d\s\xa0\.]*)\s*€", text)):
            try:
                price = int(m.group(1).replace(".", "").replace("\xa0", "").replace(" ", ""))
            except ValueError:
                pass

    # Fallback regex for surface
    if surface is None:
        if (m := _re.search(r"(\d{1,3})(?:[,.]\d+)?\s*m²", html)):
            try: surface = int(m.group(1))
            except ValueError: pass

    location = f"{city} ({zip_c})" if city and zip_c else (city or zip_c)

    # Unescape HTML entities (&nbsp;, &amp;, &eacute;, …) that JSON-LD can carry
    if title:
        title = _re.sub(r"\s+", " ", _htmllib.unescape(title)).strip()
    if description:
        description = _htmllib.unescape(description).strip()

    if not title:
        if surface and location:
            title = f"Appartement {surface}m² — {location}"
        else:
            title = "Annonce PAP"

    enrich = await _pap_enrich_detail(html)
    phone = enrich.get("phone")
    if surface is None and enrich.get("surface"):
        surface = enrich["surface"]

    avail = enrich.get("available_yyyy_mm")
    tags = []
    if (fl := enrich.get("floor")) is not None:
        tags.append(f"[ÉTAGE: {fl}]")
    if enrich.get("elevator") is True:
        tags.append("[ASCENSEUR: oui]")
    elif enrich.get("elevator") is False:
        tags.append("[ASCENSEUR: non]")
    if enrich.get("furnished") is True:
        tags.append("[MEUBLÉ: oui]")
    if avail:
        tags.append(f"[DISPO: {avail}]")
    if tags:
        description = (description + "\n" + " ".join(tags)).strip()

    seen = set()
    uniq_imgs: list[str] = []
    for u in images:
        if u and u not in seen:
            seen.add(u)
            uniq_imgs.append(u)

    return Listing(
        lbc_id=pap_id,
        title=title[:200],
        description=description,
        price=_parse_price(price),
        location=location,
        seller_name="Particulier",
        url=url,
        seller_type_hint="particulier",
        source="pap",
        images=uniq_imgs,
        surface=surface,
        phone=phone,
        available_from=avail,
    )


async def _search_pap_with_playwright(search_url: str, max_results: int) -> list[Listing]:
    """PAP scraper — curl_cffi search + parallel detail enrichment.
    Search page gives basic info; detail page (JSON-LD + regex) gives
    Meublé / floor / elevator / min lease / available date."""
    from bs4 import BeautifulSoup
    from curl_cffi.requests import AsyncSession
    import re as _re2

    # PAP pagination quirks:
    #   - pages 2+ live under /pagination/ (NOT /annonce/), and
    #   - return 403 unless we send Referer + cookies from page 1.
    # So: fetch page 1 first via curl_cffi, then pages 2..N within the same
    # AsyncSession (cookies preserved) and an explicit Referer header.
    url_no_query, _, query = search_url.partition("?")
    pag_base = url_no_query.replace("/annonce/", "/pagination/", 1)
    htmls: list[Optional[str]] = []
    async with AsyncSession(impersonate="chrome120", timeout=15) as session:
        try:
            r1 = await session.get(search_url, allow_redirects=True)
            htmls.append(r1.text if r1.status_code == 200 else None)
        except Exception as exc:
            logger.warning("[PAP] page 1 fetch failed: %s", exc)
            htmls.append(None)
        # Pages 2..15 — stop at first empty (no more results)
        for n in range(2, 16):
            url = f"{pag_base}-{n}{('?' + query) if query else ''}"
            try:
                r = await session.get(url, headers={"Referer": search_url}, allow_redirects=True)
                if r.status_code != 200:
                    break
                if "search-list-item" not in r.text:
                    break
                htmls.append(r.text)
            except Exception as exc:
                logger.warning("[PAP] page %d fetch failed: %s", n, exc)
                break
    logger.info("[PAP] Fetched %d pages", len([h for h in htmls if h]))

    listings: list[Listing] = []
    seen_ids: set[str] = set()
    for html in htmls:
        if not html:
            continue
        soup = BeautifulSoup(html, "html.parser")
        items = soup.find_all(class_=_re2.compile("search-list-item"))
        for item in items:
            l = _parse_pap_listing(item)
            if not l or l.lbc_id in seen_ids:
                continue
            seen_ids.add(l.lbc_id)
            listings.append(l)
            if len(listings) >= max_results:
                break
        if len(listings) >= max_results:
            break

    # Phase 2: detail-page enrichment (Meublé/floor/elevator/min_lease)
    if listings:
        sem = asyncio.Semaphore(10)
        async with AsyncSession(impersonate="chrome120", timeout=20) as session:
            async def _enrich(lst):
                async with sem:
                    try:
                        r = await session.get(lst.url, allow_redirects=True)
                        if r.status_code != 200:
                            return None
                        info = await _pap_enrich_detail(r.text)
                    except Exception:
                        return None
                    # Hard dealbreakers
                    if info.get("furnished") is False:
                        return lst.lbc_id  # drop: non-meublé
                    if info.get("available_yyyy_mm") and info["available_yyyy_mm"][:7] > "2026-09":
                        return lst.lbc_id  # drop: dispo trop tard
                    floor = info.get("floor")
                    if isinstance(floor, int) and floor > 3 and info.get("elevator") is False:
                        return lst.lbc_id  # drop: étage>3 sans asc
                    if (ph := info.get("phone")):
                        lst.phone = ph
                    if (av := info.get("available_yyyy_mm")):
                        lst.available_from = av
                    tags = []
                    if floor is not None: tags.append(f"[ÉTAGE: {floor}]")
                    if info.get("elevator") is True: tags.append("[ASCENSEUR: oui]")
                    elif info.get("elevator") is False: tags.append("[ASCENSEUR: non]")
                    if info.get("furnished") is True: tags.append("[MEUBLÉ: oui]")
                    if (av := info.get("available_yyyy_mm")): tags.append(f"[DISPO: {av}]")
                    if tags:
                        lst.description = (lst.description + "\n" + " ".join(tags)).strip()
                    return None
            drops = await asyncio.gather(*(_enrich(l) for l in listings))
            drop_set = {d for d in drops if d}
            if drop_set:
                logger.info("[PAP] Dropped %d (dealbreakers from detail page)", len(drop_set))
                listings = [l for l in listings if l.lbc_id not in drop_set]

    logger.info("[PAP] Parsed %d listings (with detail enrichment)", len(listings))
    return listings


# ─── Apify scrapers (paid, opt-in via USE_APIFY=true) ────────────────────────

_APIFY_RETRYABLE_STATUS = {429, 500, 502, 503, 504}


async def _apify_request(coro_factory) -> "httpx.Response":
    """Invoke an httpx coroutine with bounded retry on transient errors.

    coro_factory is a zero-arg callable that returns a fresh coroutine
    (so we can retry without reusing an awaited coroutine). Retries 3 times
    on TransportError and on 429/5xx, with exponential backoff (2s, 4s).
    """
    last_exc: Exception | None = None
    for attempt in range(3):
        try:
            resp = await coro_factory()
            resp.raise_for_status()
            return resp
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code not in _APIFY_RETRYABLE_STATUS:
                raise
            last_exc = exc
            logger.warning(
                "Apify HTTP %d (attempt %d/3)",
                exc.response.status_code, attempt + 1,
            )
        except httpx.TransportError as exc:
            last_exc = exc
            logger.warning("Apify transport error (attempt %d/3): %s", attempt + 1, exc)
        if attempt < 2:
            await asyncio.sleep(2 * (2 ** attempt))
    assert last_exc is not None
    raise last_exc


async def _run_actor(actor_id: str, input_payload: dict) -> list[dict]:
    headers = {"Content-Type": "application/json"}
    params = {"token": config.APIFY_API_KEY}
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await _apify_request(
            lambda: client.post(
                f"{_APIFY_BASE}/acts/{actor_id}/runs",
                json=input_payload, params=params, headers=headers,
            )
        )
        run_id = resp.json()["data"]["id"]
        logger.info("Apify run started: actor=%s run_id=%s", actor_id, run_id)

        s = None
        deadline = time.time() + _MAX_WAIT
        while time.time() < deadline:
            await asyncio.sleep(_POLL_INTERVAL)
            s = await _apify_request(
                lambda: client.get(f"{_APIFY_BASE}/actor-runs/{run_id}", params=params)
            )
            status = s.json()["data"]["status"]
            if status == "SUCCEEDED":
                break
            if status in ("FAILED", "TIMED-OUT", "ABORTED"):
                raise RuntimeError(f"Apify run {run_id} ended: {status}")
        else:
            raise TimeoutError(f"Apify run {run_id} timed out after {_MAX_WAIT}s")

        dataset_id = s.json()["data"]["defaultDatasetId"]
        items = await _apify_request(
            lambda: client.get(
                f"{_APIFY_BASE}/datasets/{dataset_id}/items",
                params={**params, "format": "json", "clean": "true"},
            )
        )
        return items.json()


# ─── Bien'ici listing normalisation ─────────────────────────────────────────

def _bienici_ad_to_listing(ad: dict) -> Optional[Listing]:
    """Convert a Bien'ici ad dict to a Listing."""
    bi_id = str(ad.get("id") or "")
    if not bi_id:
        return None

    listing_id = f"bi_{bi_id}"
    slug = ad.get("slug") or bi_id
    url = ad.get("url") or f"https://www.bienici.com/annonce/{slug}"

    # Price — try several field names; add charges if separate
    price_raw = (
        ad.get("pricePerMonth")
        or ad.get("price")
        or (ad.get("prices") or {}).get("perMonth")
    )
    charges = ad.get("monthlyCharges") or ad.get("charges") or 0
    price_total = _parse_price(price_raw)
    if price_total and charges:
        price_total += int(charges)

    # Location
    city = ad.get("city") or (ad.get("district") or {}).get("name") or ""
    zipcode = ad.get("postalCode") or ad.get("zipCode") or ""
    location = ", ".join(filter(None, [city, zipcode]))

    # Seller
    agency = ad.get("agency") or ad.get("contact") or {}
    seller_name = (
        agency.get("name") or agency.get("agencyName")
        or ad.get("contactName") or ""
    )
    is_pro = bool(ad.get("agency")) or ad.get("adType") == "professional"
    seller_type_hint = "pro" if is_pro else "particulier"

    # Title
    surface = ad.get("surfaceArea") or ad.get("surface") or ""
    prop = "Appartement" if "flat" in (ad.get("propertyType") or "flat") else "Bien"
    title = ad.get("title") or (
        f"{prop} {surface}m² — {city}" if surface else f"{prop} — {city}"
    )

    # Photos
    images = [
        (p.get("url") or p.get("photo") or "")
        for p in (ad.get("photos") or [])
        if isinstance(p, dict)
    ]

    return Listing(
        lbc_id=listing_id,
        title=title,
        description=ad.get("description") or "",
        price=price_total,
        location=location,
        seller_name=seller_name,
        url=url,
        seller_type_hint=seller_type_hint,
        source="bienici",
        images=[u for u in images if u],
        surface=_to_int_safe(surface),
    )


def _bienici_card_to_listing(card) -> Optional[Listing]:
    """Parse a Bien'ici `<article class="ad-overview">` card.

    Card layout (verified 2026-05-02 via Camoufox + commit-strategy):
      <article class="ad-overview" data-id="century-21-202_3862_1458">
        <a href="/annonce/location/epinay-sur-orge/appartement/3pieces/...">
        Visible text:
          "{seller} | {photo_count} | {type} {N} m² | {zip} {city} | {price} € | par mois ..."
    """
    import re as _re2

    raw_id = card.get("data-id") or ""
    if not raw_id:
        # Fallback: try to extract id from href
        link = card.find("a", href=True)
        if link:
            m = _re2.search(r"/([\w-]+_[\d_]+)(?:\?|$)", link["href"])
            if m:
                raw_id = m.group(1)
    if not raw_id:
        return None
    listing_id = f"bi_{raw_id}"

    link = card.find("a", href=True)
    if not link:
        return None
    href = link["href"]
    if href.startswith("/"):
        href = "https://www.bienici.com" + href

    text = _re2.sub(r"\s+", " ", card.get_text(" ", strip=True))

    # Price: "860 €" or "1 234 €"
    price = None
    pm = _re2.search(r"([\d\s\xa0]{3,})\s*€", text)
    if pm:
        digits = _re2.sub(r"\D", "", pm.group(1))
        try:
            price = int(digits)
        except ValueError:
            pass

    # Surface: "61 m²"
    sm = _re2.search(r"(\d+)(?:[,.]\d+)?\s*m²", text)
    surface = _to_int_safe(sm.group(1)) if sm else None

    # Location: "{5-digit zip} {city}"
    loc_m = _re2.search(r"(\d{5})\s+([\wÀ-ÿ\-' ]+?)(?:\s*\|)", text)
    if loc_m:
        location = f"{loc_m.group(2).strip()}, {loc_m.group(1)}"
    else:
        location = ""

    # Title: type + surface portion (e.g. "Appartement meublé 3 pièces 61 m²")
    title_m = _re2.search(r"((?:Appartement|Studio|Maison|F\d|T\d)[^|]*?\d+\s*m²)", text)
    title = (title_m.group(1).strip() if title_m else text[:80]).strip()

    # Seller: first segment before first "|"
    seller = text.split("|", 1)[0].strip() if "|" in text else "Bien'ici"

    images = [
        src for img in card.find_all("img")
        if (src := (img.get("src") or img.get("data-src") or "")).startswith("http")
    ]

    return Listing(
        lbc_id=listing_id,
        title=title,
        description="",
        price=_parse_price(price),
        location=location,
        seller_name=seller[:60] or "Bien'ici",
        url=href,
        seller_type_hint="pro",
        source="bienici",
        images=images,
        surface=surface,
    )


_BIENICI_API = "https://www.bienici.com/realEstateAds.json"
_BIENICI_IDF_ZONE_ID = "-8649"  # Île-de-France region (covers all Paris arr.)


def _bienici_ad_to_listing(ad: dict) -> Optional[Listing]:
    """Convert a Bien'ici realEstateAds.json entry to a Listing."""
    aid = ad.get("id")
    if not aid:
        return None

    # Filter out short-term rentals masquerading as cheap (price = daily rate
    # like 116€/day). Heuristic: any rental priced <300€/month is suspicious;
    # those are typically per-day or per-week values mislabeled.
    price = ad.get("price")
    if not isinstance(price, (int, float)) or price < 300:
        return None
    price = int(price)

    # Filter explicit non-furnished (we requested furnished=True but be safe)
    if ad.get("isFurnished") is False:
        return None

    surface = ad.get("surfaceArea")
    rooms = ad.get("roomsQuantity")
    title = ad.get("title") or f"Appartement {int(surface)}m²" if surface else "Appartement"

    city = ad.get("city") or ""
    zip_c = ad.get("postalCode") or ""
    location = f"{city}, {zip_c}".strip(", ") if zip_c else city

    url = f"https://www.bienici.com/annonce/location/{aid}"

    # Description + structured tags for scoring LLM
    description = ad.get("description") or ""
    tags = []
    floor = ad.get("floor")
    if isinstance(floor, int):
        tags.append(f"[ÉTAGE: {floor}]" if floor > 0 else "[ÉTAGE: RDC]")
    if ad.get("isFurnished") is True:
        tags.append("[MEUBLÉ: oui]")
    heating = ad.get("heating") or ""
    if heating:
        tags.append(f"[CHAUFFAGE: {heating}]")
    if tags:
        description = (description + "\n" + " ".join(tags)).strip()

    photos = []
    for ph in ad.get("photos") or []:
        if isinstance(ph, dict) and (u := ph.get("url")):
            photos.append(u)

    pub = ad.get("publicationDate") or ad.get("modificationDate")

    return Listing(
        lbc_id=f"bi_{aid}",
        title=str(title)[:200],
        description=description,
        price=price,
        location=location,
        seller_name="Bien'ici",
        url=url,
        seller_type_hint="pro" if ad.get("accountType") == "pro" else "",
        source="bienici",
        images=photos,
        surface=int(surface) if isinstance(surface, (int, float)) else None,
        published_at=pub if isinstance(pub, str) else None,
    )


_BIENICI_DETAIL_API = "https://www.bienici.com/realEstateAd.json"


async def _bienici_fetch_detail(client, ad_id: str) -> dict:
    """Fetch Bien'ici single-listing detail JSON. Has `hasElevator` (sometimes)
    and richer fields (heating, agencyRentalFee, safetyDeposit, etc.)."""
    try:
        r = await client.get(f"{_BIENICI_DETAIL_API}?id={ad_id}",
                             headers={"User-Agent": _HTTPX_UA, "Referer": "https://www.bienici.com/"})
        if r.status_code == 200:
            return r.json()
    except Exception as exc:
        logger.debug("Bien'ici detail fetch %s failed: %s", ad_id, exc)
    return {}


async def _fetch_bienici_single(url: str) -> Optional[Listing]:
    """Fetch a single Bien'ici detail page via the realEstateAd.json API.

    URL formats seen:
      • https://www.bienici.com/annonce/location/<id>...
      • https://www.bienici.com/annonce/<id>
    We extract the id, hit the detail API, and reuse `_bienici_ad_to_listing`.
    """
    import httpx as _httpx
    m = _re.search(r"/annonce/(?:[a-z]+/)?([a-zA-Z0-9_-]{6,})", url)
    if not m:
        logger.warning("[BIENICI single] no id in url: %s", url)
        return None
    ad_id = m.group(1)
    try:
        async with _httpx.AsyncClient(headers={"User-Agent": _HTTPX_UA}, timeout=20) as client:
            detail = await _bienici_fetch_detail(client, ad_id)
    except Exception as exc:
        logger.warning("[BIENICI single] fetch failed: %s", exc)
        return None
    if not detail:
        return None
    listing = _bienici_ad_to_listing(detail)
    if not listing:
        return None
    # Override URL with the canonical user-pasted one (strip query/hash)
    listing.url = url.split("?")[0].split("#")[0]
    crd = detail.get("contactRelativeData") or {}
    ph = crd.get("phoneToDisplay")
    if isinstance(ph, str) and ph.strip():
        listing.phone = ph.strip()
    return listing


def _generic_id_from_url(url: str) -> str:
    """Build a stable lbc_id for an unrecognised source. Hash-based so re-adding
    the same URL upserts the same row instead of duplicating."""
    import hashlib as _hl
    h = _hl.sha1(url.encode("utf-8")).hexdigest()[:12]
    return f"manual_{h}"


def _fetch_generic_minimal(url: str) -> Listing:
    """Last-resort fallback: persist a near-empty Listing carrying just the URL.
    User can still click through from the dashboard / Telegram alert."""
    return Listing(
        lbc_id=_generic_id_from_url(url),
        title="Annonce ajoutée manuellement",
        description="",
        price=0,
        location="",
        seller_name="",
        url=url,
        source="manual",
    )


async def _search_bienici_with_playwright(search_url: str, max_results: int) -> list[Listing]:
    """Bien'ici scraper via internal JSON API at `/realEstateAds.json`.

    Two-phase:
      1. Search API returns paginated frames with most data (price, floor,
         isFurnished, etc.) — fast.
      2. For listings with floor > 3, fetch detail API to check hasElevator.
         If floor>3 AND hasElevator=False → drop (dealbreaker).

    `search_url` is kept for dispatcher compat but ignored — we use the API
    directly.
    """
    import httpx, json, urllib.parse

    MAX_BUDGET = 1100  # match dashboard hard cap, no need to fetch above-budget
    listings: list[Listing] = []
    try:
        async with httpx.AsyncClient(headers={"User-Agent": _HTTPX_UA}, timeout=20) as client:
            page = 1
            while True:
                filters = {
                    "size": 50,
                    "from": (page - 1) * 50,
                    "page": page,
                    "filterType": "rent",
                    "propertyType": ["flat", "house"],
                    "maxPrice": MAX_BUDGET,
                    "minArea": 14,
                    "isFurnished": True,
                    "onTheMarket": [True],
                    "zoneIdsByTypes": {"zoneIds": [_BIENICI_IDF_ZONE_ID]},
                }
                qs = urllib.parse.urlencode({"filters": json.dumps(filters)})
                r = await client.get(f"{_BIENICI_API}?{qs}")
                if r.status_code != 200:
                    logger.warning("[BIENICI] API page %d: HTTP %s", page, r.status_code)
                    break
                data = r.json()
                ads = data.get("realEstateAds", [])
                if not ads:
                    break
                for ad in ads:
                    if (l := _bienici_ad_to_listing(ad)):
                        listings.append(l)
                        if len(listings) >= max_results:
                            break
                if len(listings) >= max_results:
                    break
                total = data.get("total", 0)
                if page * 50 >= total:
                    break
                page += 1

            # Phase 2: detail fetch for ALL listings to grab elevator (high-floor
            # only) + phone (everywhere). Bien'ici detail API returns
            # contactRelativeData.phoneToDisplay for most agency listings.
            def _floor_from_desc(desc: str) -> int:
                m = _re.search(r"\[ÉTAGE:\s*(\d+)\]", desc or "")
                return int(m.group(1)) if m else -1
            if listings:
                logger.info("[BIENICI] Fetching detail for %d listings (phone + elevator)", len(listings))
                sem = asyncio.Semaphore(10)
                async def _enrich(lst):
                    async with sem:
                        ad_id = lst.lbc_id.removeprefix("bi_")
                        detail = await _bienici_fetch_detail(client, ad_id)
                        if not detail:
                            return None
                        # Elevator dealbreaker for floor>3
                        if _floor_from_desc(lst.description) > 3 and detail.get("hasElevator") is False:
                            return lst.lbc_id  # drop
                        # Phone extraction
                        crd = detail.get("contactRelativeData") or {}
                        ph = crd.get("phoneToDisplay")
                        if isinstance(ph, str) and ph.strip():
                            lst.phone = ph.strip()
                        # Description backfill: Bien'ici search API sometimes
                        # returns empty description; detail API always has it.
                        if len((lst.description or "").strip()) < 80:
                            full = detail.get("description") or ""
                            if isinstance(full, str) and len(full) > 80:
                                # Preserve any [ÉTAGE: ...] tags appended in card parser
                                tags = _re.findall(r"\[[A-ZÉÈÊ]+:[^\]]+\]", lst.description or "")
                                lst.description = (full + ("\n" + " ".join(tags) if tags else "")).strip()[:1500]
                        return None
                drops = await asyncio.gather(*(_enrich(l) for l in listings))
                drop_set = {d for d in drops if d}
                if drop_set:
                    logger.info("[BIENICI] Dropping %d listings (étage>3 + no elevator)", len(drop_set))
                    listings = [l for l in listings if l.lbc_id not in drop_set]

        logger.info(
            "[BIENICI] Parsed %d listings via API (≤%d€, IDF, furnished)",
            len(listings), MAX_BUDGET,
        )
        return listings
    except Exception as exc:
        logger.warning("[BIENICI] API scrape failed: %s", exc)
        return []


# ─── Logic-Immo listing normalisation ────────────────────────────────────────

def _logicimmo_item_to_listing(item) -> Optional[Listing]:
    """Parse a Logic-Immo BeautifulSoup card element into a Listing."""
    import re as _re2
    from bs4 import BeautifulSoup  # noqa: F401 — already imported at call site

    # Try data attribute for ID first, then extract from href
    li_id = item.get("data-listing-id") or item.get("data-id") or ""
    href = ""
    if not li_id:
        link = item.find("a", href=_re2.compile(r"/(annonce|location)/"))
        if link:
            href = link.get("href", "")
        m = _re2.search(r"-(\d+)\.htm", href)
        if not m:
            return None
        li_id = m.group(1)

    listing_id = f"li_{li_id}"
    url = (
        (f"https://www.logic-immo.com{href}" if href.startswith("/") else href)
        or f"https://www.logic-immo.com/annonce-location-{li_id}.htm"
    )

    text = item.get_text(separator=" ", strip=True)
    text = _re2.sub(r"\s+", " ", text)

    # Price
    price_raw = None
    pm = _re2.search(r"([\d][\d\s\xa0\.]*)\s*€", text)
    if pm:
        try:
            price_raw = int(pm.group(1).replace(".", "").replace("\xa0", "").replace(" ", ""))
        except ValueError:
            pass

    # Location
    location = ""
    loc_el = item.find(class_=_re2.compile(r"city|localisation|location|ville", _re2.I))
    if loc_el:
        location = loc_el.get_text(strip=True)

    # Surface for title
    sm = _re2.search(r"(\d+)(?:[,.]\d+)?\s*m²", text, _re2.IGNORECASE)
    surface = sm.group(1) if sm else ""

    # Title
    title_el = item.find(class_=_re2.compile(r"title|titre|heading", _re2.I))
    title = (
        title_el.get_text(strip=True) if title_el
        else (f"Appartement {surface}m² — {location}" if surface else text[:80])
    )

    # Description
    desc_el = item.find(class_=_re2.compile(r"desc|summary|resume|teaser", _re2.I))
    description = desc_el.get_text(strip=True) if desc_el else ""

    # Seller
    seller_el = item.find(class_=_re2.compile(r"agency|agence|seller|advertiser", _re2.I))
    seller_name = seller_el.get_text(strip=True) if seller_el else "Annonceur"

    images = [
        src for img in item.find_all("img")
        if (src := img.get("src") or img.get("data-src") or "").startswith("http")
    ]

    return Listing(
        lbc_id=listing_id,
        title=title,
        description=description,
        price=_parse_price(price_raw),  # range-check [50, 50000] like other parsers
        location=location,
        seller_name=seller_name,
        url=url,
        seller_type_hint="pro",
        source="logicimmo",
        images=images,
        surface=_to_int_safe(surface),
    )


_LI_PRICE_RE   = _re.compile(r"([\d][\d\s\xa0.,]*)\s*€")
_LI_SURFACE_RE = _re.compile(r"(\d+(?:[.,]\d+)?)\s*m\s*²", _re.IGNORECASE)


def _li_parse_int_money(raw: str | None) -> Optional[int]:
    if not raw:
        return None
    m = _LI_PRICE_RE.search(raw)
    if not m:
        return None
    digits = _re.sub(r"[^\d]", "", m.group(1))
    return int(digits) if digits else None


def _li_parse_surface(raw: str | None) -> Optional[int]:
    if not raw:
        return None
    m = _LI_SURFACE_RE.search(raw)
    if not m:
        return None
    return int(float(m.group(1).replace(",", ".")))


async def _search_logicimmo_with_playwright(search_url: str, max_results: int) -> list[Listing]:
    """Scrape Logic-Immo /classified-search via the warm Camoufox context.

    Stealth Playwright is reliably DataDome-blocked here (captcha-delivery iframe).
    Camoufox walks straight in. With a warm DataDome cookie this runs in ~5-10s
    (cold-start was ~30-45s including browser launch + challenge).

    Uses the persistent "logicimmo" context — only the first scrape after bot
    startup pays the DataDome challenge cost.
    """
    t0 = time.time()
    raw_cards: list = []
    page = None

    try:
        context = await _get_camoufox_context("logicimmo")
        page = await context.new_page()
        try:
            await page.goto(search_url, wait_until="domcontentloaded", timeout=60_000)
            try:
                await page.evaluate(
                    "() => { const r = document.querySelector('#usercentrics-root');"
                    "        if (r) r.remove(); return true; }"
                )
            except Exception:
                pass
            try:
                await page.wait_for_selector(
                    '[data-testid="serp-core-classified-card-testid"]',
                    timeout=20_000,
                )
            except Exception as exc:
                logger.warning("[LOGICIMMO] cards never rendered: %s", exc)
                return []
            raw_cards = await page.evaluate(
                """(maxN) => {
                  const cards = Array.from(document.querySelectorAll(
                    '[data-testid="serp-core-classified-card-testid"]'
                  )).slice(0, maxN);
                  const text = (el, sel) => {
                    const x = el.querySelector(sel);
                    return x ? (x.innerText || x.textContent || '').trim() : '';
                  };
                  return cards.map(c => {
                    const link  = c.querySelector('[data-testid="card-mfe-covering-link-testid"]');
                    let href = '';
                    let summary = '';
                    if (link) {
                      href = link.getAttribute('href') || '';
                      const dataBase = link.getAttribute('data-base') || '';
                      if (!href && dataBase) {
                        try { href = decodeURIComponent(dataBase); } catch (e) { href = dataBase; }
                      }
                      summary = link.getAttribute('title') || '';
                    }
                    const imgs = Array.from(c.querySelectorAll('img'))
                      .map(i => i.src || i.getAttribute('data-src') || '')
                      .filter(s => s.startsWith('http'));
                    const agency = (
                      text(c, '[data-testid*="agency-publisher"]') ||
                      text(c, '[data-testid*="publisher"]')
                    );
                    return {
                      href, summary, imgs, agency,
                      price:    text(c, '[data-testid="cardmfe-price-testid"]'),
                      address:  text(c, '[data-testid="cardmfe-description-box-address"]'),
                      keyfacts: text(c, '[data-testid="cardmfe-keyfacts-testid"]'),
                      desc:     text(c, '[data-testid="cardmfe-description-text-test-id"]'),
                    };
                  });
                }""",
                max_results,
            )
        finally:
            try: await page.close()
            except Exception: pass
    except Exception as exc:
        logger.warning("[LOGICIMMO] Camoufox failed: %s", exc)
        # If the warm context died, mark it dead so the next call recreates.
        try:
            ctx = _CAMOUFOX_CTXS.get("logicimmo")
            if ctx is not None:
                _ = ctx.pages
        except Exception:
            logger.warning("[LOGICIMMO] context died; will recreate on next scrape")
            await _close_site_context("logicimmo")
        return []

    logger.info("[LOGICIMMO] extracted %d raw cards in %.1fs", len(raw_cards), time.time() - t0)

    listings: list[Listing] = []
    for raw in raw_cards:
        href = (raw.get("href") or "").strip()
        if not href:
            continue
        url = href.split("?", 1)[0] if href.startswith("http") else f"https://www.logic-immo.com{href}"

        id_m = _re.search(r"detail-location-(\d+)", href) or _re.search(r"-(\d{5,})", href)
        raw_id = id_m.group(1) if id_m else url.rstrip("/").rsplit("/", 1)[-1].split(".")[0][:32]
        listing_id = f"li_{raw_id}"

        summary = (raw.get("summary") or "").strip()
        price    = _li_parse_int_money(raw.get("price")) or _li_parse_int_money(summary) or 0
        surface  = _li_parse_surface(raw.get("keyfacts")) or _li_parse_surface(summary)
        location = (raw.get("address") or "").strip() or (
            (summary.split(" - ")[1].strip() if summary.count(" - ") >= 2 else "")
        )
        desc     = (raw.get("desc") or "").strip()
        title    = (summary or desc.split("\n", 1)[0])[:140] or (
            f"Appartement {surface}m² — {location}" if surface else (location or "Annonce Logic-Immo")
        )

        if not (50 <= price <= 50_000):
            continue

        listings.append(Listing(
            lbc_id=listing_id,
            title=title,
            description=desc,
            price=price,
            location=location,
            seller_name=(raw.get("agency") or "").strip() or "Annonceur",
            url=url,
            seller_type_hint="pro",
            source="logicimmo",
            images=raw.get("imgs") or [],
            surface=surface,
        ))

    logger.info("[LOGICIMMO] returning %d listings (total %.1fs)", len(listings), time.time() - t0)
    return listings


# ─── Shared HTML fetcher + generic card parser (used by new scrapers) ───────
#
# These were added during Phase 2 (student/young-pro platforms). Each new
# scraper tries __NEXT_DATA__ via _pw_get_next_data first, and falls back to
# _fetch_html_with_stealth + _parse_generic_card. Site-specific structure isn't
# known ahead of time — selectors may need tuning against live HTML on first run.

async def _fetch_html_with_stealth(
    url: str, site: str, post_delay: tuple[float, float] = (2.0, 3.5)
) -> Optional[str]:
    """Fetch fully-rendered HTML using a per-site persistent Chromium profile."""
    profile_dir = _user_data_dir(site)
    Path(profile_dir).mkdir(parents=True, exist_ok=True)
    async with async_playwright() as pw:
        ctx = await pw.chromium.launch_persistent_context(
            user_data_dir=profile_dir,
            headless=True,
            args=["--disable-blink-features=AutomationControlled", "--window-size=1280,800"],
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/131.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 800},
            locale="fr-FR",
        )
        page = await ctx.new_page()
        await Stealth().apply_stealth_async(page)
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=30_000)
            await _handle_cookie_banner(page)
            await asyncio.sleep(random.uniform(*post_delay))
            return await page.content()
        except Exception as exc:
            logger.warning("[%s] page fetch failed: %s", site.upper(), exc)
            return None
        finally:
            await ctx.close()


def _parse_generic_card(item, base_url: str, source: str, prefix: str) -> Optional[Listing]:
    """Best-effort parser for a French rental search-card.

    Common patterns: anchor to detail page, price in €, surface in m², location
    in a 'city/ville/location' class. Returns None if no link or no price.
    """
    import re as _re2

    link = item.find("a", href=True)
    href = link["href"] if link else ""
    if not href:
        return None
    if href.startswith("/"):
        href = base_url.rstrip("/") + href
    elif not href.startswith("http"):
        href = base_url.rstrip("/") + "/" + href

    id_m = (
        _re2.search(r"-(\d{5,})(?:\.htm|/?$)", href)
        or _re2.search(r"/(\d{5,})(?:[/?]|$)", href)
        or _re2.search(r"id[=/](\d{5,})", href, _re2.I)
    )
    raw_id = id_m.group(1) if id_m else href.rsplit("/", 1)[-1].split("?")[0][:32] or "0"
    listing_id = f"{prefix}_{raw_id}"

    text = item.get_text(separator=" ", strip=True)
    text = _re2.sub(r"\s+", " ", text)

    price = None
    pm = _re2.search(r"([\d][\d\s\xa0\.]{1,})\s*€", text)
    if pm:
        try:
            price = int(pm.group(1).replace(".", "").replace("\xa0", "").replace(" ", ""))
        except ValueError:
            pass

    sm = _re2.search(r"(\d+)(?:[,.]\d+)?\s*m²", text, _re2.IGNORECASE)
    surface = sm.group(1) if sm else ""

    title_el = item.find(class_=_re2.compile(r"title|titre|heading|name", _re2.I))
    title = (
        title_el.get_text(strip=True) if title_el
        else (f"Logement {surface}m²" if surface else text[:80])
    )

    loc_el = item.find(class_=_re2.compile(r"city|ville|locality|address|location", _re2.I))
    location = loc_el.get_text(strip=True) if loc_el else ""

    desc_el = item.find(class_=_re2.compile(r"desc|summary|teaser|excerpt|resume", _re2.I))
    description = desc_el.get_text(strip=True) if desc_el else ""

    seller_el = item.find(class_=_re2.compile(r"agency|agence|seller|advertiser|provider|owner", _re2.I))
    seller_name = seller_el.get_text(strip=True) if seller_el else "Annonceur"

    images = [
        src for img in item.find_all("img")
        if (src := (img.get("src") or img.get("data-src") or "")).startswith("http")
    ]

    return Listing(
        lbc_id=listing_id,
        title=title,
        description=description,
        price=price,
        location=location,
        seller_name=seller_name,
        url=href,
        seller_type_hint="",
        source=source,
        images=images,
        surface=_to_int_safe(surface),
    )


def _ads_from_next_data(data: dict) -> list:
    """Extract candidate ad list from a __NEXT_DATA__ blob using common keys."""
    if not data:
        return []
    props = data.get("props", {}).get("pageProps", {}) or {}
    return (
        props.get("ads")
        or props.get("listings")
        or props.get("results")
        or props.get("offers")
        or props.get("items")
        or props.get("annonces")
        or props.get("rooms")
        or (props.get("searchResults") or {}).get("ads")
        or (props.get("searchResults") or {}).get("results")
        or (props.get("searchData") or {}).get("ads")
        or []
    )


def _generic_ad_to_listing(
    ad: dict, base_url: str, source: str, prefix: str
) -> Optional[Listing]:
    """Map a JSON ad dict (from __NEXT_DATA__) to a Listing using common keys."""
    raw_id = str(
        ad.get("id") or ad.get("listingId") or ad.get("uuid")
        or ad.get("slug") or ad.get("reference") or ""
    )
    if not raw_id:
        return None
    listing_id = f"{prefix}_{raw_id}"

    href = ad.get("url") or ad.get("link") or ad.get("permalink") or ""
    if href and href.startswith("/"):
        href = base_url.rstrip("/") + href
    elif not href:
        slug = ad.get("slug") or raw_id
        href = f"{base_url.rstrip('/')}/{slug}"

    price_raw = (
        ad.get("price") or ad.get("rent") or ad.get("monthlyRent")
        or ad.get("pricePerMonth") or ad.get("loyer")
        or (ad.get("prices") or {}).get("perMonth")
    )

    loc = ad.get("location") or ad.get("address") or {}
    if isinstance(loc, str):
        location = loc
    else:
        city = loc.get("city") or loc.get("locality") or ad.get("city") or ""
        zipcode = loc.get("zipCode") or loc.get("postalCode") or ad.get("postalCode") or ""
        location = ", ".join(filter(None, [city, zipcode]))

    seller = ad.get("agency") or ad.get("owner") or ad.get("contact") or {}
    if isinstance(seller, dict):
        seller_name = seller.get("name") or seller.get("title") or "Annonceur"
    else:
        seller_name = str(seller) or "Annonceur"

    title = ad.get("title") or ad.get("subject") or ad.get("name") or ""
    description = ad.get("description") or ad.get("descriptif") or ad.get("body") or ""

    images_raw = ad.get("images") or ad.get("photos") or ad.get("gallery") or []
    images: list[str] = []
    if isinstance(images_raw, list):
        for img in images_raw:
            if isinstance(img, str):
                images.append(img)
            elif isinstance(img, dict):
                u = img.get("url") or img.get("src") or img.get("photo") or ""
                if u:
                    images.append(u)

    return Listing(
        lbc_id=listing_id,
        title=title,
        description=description,
        price=_parse_price(price_raw),
        location=location,
        seller_name=seller_name,
        url=href,
        seller_type_hint="",
        source=source,
        images=images,
    )


async def _search_via_generic(
    search_url: str,
    max_results: int,
    *,
    site: str,
    base_url: str,
    source: str,
    prefix: str,
    card_selectors: list[str],
) -> list[Listing]:
    """Generic Next.js-then-cards search routine for the Phase 2 scrapers.

    Tries __NEXT_DATA__ first; if no usable ad list comes back, falls back to
    HTML card scraping using the supplied list of CSS class regex patterns.
    """
    logger.info("[%s] Scraping: %s", source.upper(), search_url)
    try:
        data = await _pw_get_next_data(search_url, site=site)
        ads = _ads_from_next_data(data) if data else []
        if ads:
            listings = [
                l for l in (
                    _generic_ad_to_listing(a, base_url, source, prefix)
                    for a in ads[:max_results] if isinstance(a, dict)
                ) if l
            ]
            if listings:
                logger.info("[%s] Found %d via __NEXT_DATA__", source.upper(), len(listings))
                return listings

        # Fallback: card scraping
        html = await _fetch_html_with_stealth(search_url, site)
        if not html:
            return []
        from bs4 import BeautifulSoup
        import re as _re2
        soup = BeautifulSoup(html, "html.parser")
        items = []
        for sel in card_selectors:
            items = soup.find_all(class_=_re2.compile(sel, _re2.I))
            if items:
                break
        if not items:
            items = soup.find_all("article") or soup.find_all(attrs={"data-id": True})

        logger.info("[%s] Found %d card elements", source.upper(), len(items))
        listings = [
            l for l in (
                _parse_generic_card(i, base_url, source, prefix)
                for i in items[:max_results]
            ) if l
        ]
        logger.info("[%s] Parsed %d listings", source.upper(), len(listings))
        return listings
    except Exception as exc:
        logger.warning("[%s] Scraping failed: %s", source.upper(), exc)
        return []


# ─── Phase 2 scrapers (student / young-pro platforms) ────────────────────────
# NOTE: selectors below are educated guesses. They follow common French rental
# site patterns and use _search_via_generic which tries __NEXT_DATA__ first.
# Run /search <url> against each platform after first launch and tune the
# `card_selectors` list (and source-specific JSON paths) against live HTML.

# ─── Camoufox per-site persistent contexts ───────────────────────────────────
# Each anti-bot-protected site gets its OWN long-lived Camoufox Browser +
# BrowserContext. The context's cookie jar persists across scrapes, so the
# DataDome cookie obtained on the first (cold) scrape is reused on every
# subsequent scrape — turning ~60-90s cold scrapes into ~5-15s warm ones.
#
# Memory budget: ~200MB per context × 3 sites = ~600MB, comfortably below
# Oracle ARM 24GB. Contexts close cleanly on bot shutdown via
# `shutdown_camoufox_pool` (kept named for caller compat in main.py).
#
# Sites with their own warm context:
#   - "leboncoin"  : LBC DataDome challenge
#   - "seloger"    : SeLoger DataDome (used as Camoufox fallback to curl_cffi)
#   - "logicimmo"  : Logic-Immo DataDome
#   - "_generic"   : shared context for sites without per-site cookies
#                    (Studapart/Lodgis/Bien'ici/ImmoJeune/LocService etc.)
_CAMOUFOX_CTXS: dict[str, "object"] = {}        # site -> BrowserContext
_CAMOUFOX_BROWSERS: dict[str, "object"] = {}    # site -> Browser
_CAMOUFOX_CMS_BY_SITE: dict[str, "object"] = {} # site -> AsyncCamoufox CM (for shutdown)
_CAMOUFOX_LOCKS: dict[str, "asyncio.Lock"] = {} # site -> per-site init/recreate lock
_CAMOUFOX_INIT_LOCK: Optional["asyncio.Lock"] = None  # global lock for lock-dict init

# Sites that get a dedicated warm context. Anything else routes to "_generic".
_NAMED_SITES = ("leboncoin", "seloger", "logicimmo")


# Generic browser-like UA for httpx fetches. Most French rental sites (PAP,
# LocService, Lodgis, ImmoJeune) have NO bot protection — Camoufox/Playwright
# was overkill. Plain httpx with this UA returns clean HTML in <1s/page.
_HTTPX_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)
_HTTPX_HEADERS = {
    "User-Agent": _HTTPX_UA,
    "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


async def _fetch_pages_httpx(urls: list[str], timeout: int = 15) -> list[Optional[str]]:
    """Fetch a list of URLs in parallel via httpx. Returns HTML strings (or None
    on failure) in the same order as input. Used by SSR sites with no bot block."""
    import httpx
    async with httpx.AsyncClient(headers=_HTTPX_HEADERS, timeout=timeout, follow_redirects=True) as client:
        async def _one(url: str) -> Optional[str]:
            try:
                r = await client.get(url)
                return r.text if r.status_code == 200 else None
            except Exception as exc:
                logger.warning("httpx fetch failed for %s: %s", url, exc)
                return None
        return await asyncio.gather(*(_one(u) for u in urls))


async def _fetch_pages_curl_cffi(urls: list[str], timeout: int = 15, impersonate: str = "chrome120") -> list[Optional[str]]:
    """Fetch URLs in parallel via curl_cffi with Chrome TLS fingerprint —
    bypasses Cloudflare bot challenges that block plain httpx.

    Heavier dependency than httpx (libcurl + BoringSSL) but ~10× lighter than
    Playwright and bypasses Cloudflare without a real browser.
    """
    from curl_cffi.requests import AsyncSession
    async with AsyncSession(impersonate=impersonate, timeout=timeout) as session:
        async def _one(url: str) -> Optional[str]:
            try:
                r = await session.get(url, allow_redirects=True)
                return r.text if r.status_code == 200 else None
            except Exception as exc:
                logger.warning("curl_cffi fetch failed for %s: %s", url, exc)
                return None
        return await asyncio.gather(*(_one(u) for u in urls))


def _site_key_from_url(url: str) -> str:
    """Map a URL to a named-context site key, or '_generic' for the rest.

    Used by `_fetch_html_with_camoufox` to route to the right warm context
    when callers don't pass an explicit `site=` kwarg.
    """
    host = url.split("/", 3)[2].lower() if "://" in url else url.lower()
    if "leboncoin.fr" in host:
        return "leboncoin"
    if "seloger.com" in host:
        return "seloger"
    if "logic-immo.com" in host:
        return "logicimmo"
    return "_generic"


async def _launch_camoufox_context(site: str):
    """Cold-start a Camoufox Browser + BrowserContext for `site`.

    Returns (cm, browser, context). `cm` is the AsyncCamoufox context manager,
    held in `_CAMOUFOX_CMS_BY_SITE` so we can `__aexit__` it on shutdown.
    """
    from camoufox.async_api import AsyncCamoufox
    cm = AsyncCamoufox(headless=True, locale=["fr-FR"], os="windows")
    t0 = time.time()
    browser = await cm.__aenter__()
    logger.info("[CAMOUFOX %s] browser launched in %.1fs", site, time.time() - t0)
    t1 = time.time()
    # One persistent context per site → cookies (incl. DataDome) survive across
    # scrapes. We don't pass storage_state — first scrape solves the challenge,
    # subsequent scrapes reuse the in-memory cookie jar.
    context = await browser.new_context(locale="fr-FR", viewport={"width": 1280, "height": 800})
    logger.info("[CAMOUFOX %s] context ready in %.1fs", site, time.time() - t1)
    return cm, browser, context


async def _get_camoufox_context(site: str):
    """Return the warm BrowserContext for `site`, lazily creating it on first
    use. Recreates transparently if the previous context died (DataDome
    refresh challenge can kill the page; the context itself usually survives,
    but we're defensive).

    Per-site lock prevents concurrent scrapes from racing the cold start.
    """
    global _CAMOUFOX_INIT_LOCK
    if _CAMOUFOX_INIT_LOCK is None:
        _CAMOUFOX_INIT_LOCK = asyncio.Lock()
    async with _CAMOUFOX_INIT_LOCK:
        if site not in _CAMOUFOX_LOCKS:
            _CAMOUFOX_LOCKS[site] = asyncio.Lock()
    site_lock = _CAMOUFOX_LOCKS[site]

    async with site_lock:
        ctx = _CAMOUFOX_CTXS.get(site)
        # Probe liveness — closed contexts raise when you call new_page on them
        if ctx is not None:
            try:
                # A cheap liveness check: pages property doesn't I/O, but if
                # the underlying browser died this throws.
                _ = ctx.pages
                return ctx
            except Exception as exc:
                logger.warning("[CAMOUFOX %s] context dead (%s); recreating", site, exc)
                # Fall through to recreate
                await _close_site_context(site)

        cm, browser, context = await _launch_camoufox_context(site)
        _CAMOUFOX_CMS_BY_SITE[site] = cm
        _CAMOUFOX_BROWSERS[site] = browser
        _CAMOUFOX_CTXS[site] = context
        return context


async def _close_site_context(site: str) -> None:
    """Tear down the warm context + browser for `site`. Safe to call on a
    site that was never initialized."""
    cm = _CAMOUFOX_CMS_BY_SITE.pop(site, None)
    _CAMOUFOX_BROWSERS.pop(site, None)
    _CAMOUFOX_CTXS.pop(site, None)
    if cm is not None:
        try:
            await cm.__aexit__(None, None, None)
        except Exception as exc:
            logger.warning("[CAMOUFOX %s] failed to close: %s", site, exc)


async def init_camoufox_pool(size: int = 2) -> int:
    """Pre-warm the named Camoufox contexts (LBC, SeLoger, Logic-Immo).

    `size` is kept for back-compat with main.py's `_post_init` call but is
    now ignored — we always pre-warm the named sites + a generic context.
    Idempotent: re-calling on already-initialized sites is a no-op.

    Returns the number of contexts successfully warmed.
    """
    sites = list(_NAMED_SITES) + ["_generic"]
    n = 0
    for site in sites:
        try:
            await _get_camoufox_context(site)
            n += 1
        except Exception as exc:
            logger.warning("Could not pre-warm Camoufox context for %s: %s", site, exc)
    logger.info("Camoufox contexts initialized: %d/%d ready", n, len(sites))
    return n


async def shutdown_camoufox_pool() -> None:
    """Close every warm Camoufox context. Called from main.py on bot shutdown."""
    sites = list(_CAMOUFOX_CTXS.keys())
    for site in sites:
        await _close_site_context(site)
    _CAMOUFOX_LOCKS.clear()
    logger.info("Camoufox contexts shut down")


async def _fetch_html_with_camoufox(
    url: str,
    post_delay: tuple[float, float] = (5.0, 8.0),
    wait_until: str = "domcontentloaded",
    goto_timeout: int = 60_000,
    site: Optional[str] = None,
) -> Optional[str]:
    """Fetch a page with Camoufox using the warm per-site context.

    Routes to the right named context based on the URL host (or `site` kwarg).
    First call per site cold-starts the browser + solves DataDome (~30s);
    subsequent calls reuse the cookie jar and run in 5-15s.

    Studapart/Lodgis/ImmoJeune/LocService share the "_generic" context.
    Bien'ici callers should pass `wait_until="commit"` (its DataDome
    challenge holds the document open).

    On a single failed scrape we DON'T tear down the context — DataDome
    sometimes serves a captcha for one URL but lets the next through. We
    only recreate when the context itself is dead (see `_get_camoufox_context`).
    """
    # Resolve to a known named site, else share the "_generic" context. Phase 2
    # callers pass site="studapart"/"lodgis"/etc. which map to "_generic" so we
    # don't spawn a warm browser per every minor portal (memory budget).
    raw_site = site or _site_key_from_url(url)
    site_key = raw_site if raw_site in _NAMED_SITES else "_generic"
    context = await _get_camoufox_context(site_key)

    t0 = time.time()
    host = url.split("/")[2] if "://" in url else url[:30]
    def _step(name: str) -> None:
        logger.info("[CAMOUFOX %s/%s] %s @ +%.1fs", site_key, host, name, time.time() - t0)

    page = await context.new_page()
    _step("new_page done")
    try:
        _step(f"goto start (wait_until={wait_until}, timeout={goto_timeout/1000:.0f}s)")
        await page.goto(url, wait_until=wait_until, timeout=goto_timeout)
        _step("goto done")
        await _handle_cookie_banner(page)
        _step("cookie banner done")
        await asyncio.sleep(random.uniform(*post_delay))
        _step("post_delay done")
        html = await page.content()
        _step(f"content done ({len(html)} chars)")
        return html
    except Exception as exc:
        _step(f"EXCEPTION: {type(exc).__name__}: {exc}")
        logger.warning("Camoufox fetch failed for %s: %s", url, exc)
        # If the context itself died, mark it for recreation on next call.
        # Callers above us will retry; the next `_get_camoufox_context`
        # liveness check will see it's dead and rebuild transparently.
        try:
            _ = context.pages
        except Exception:
            logger.warning("[CAMOUFOX %s] context died mid-scrape; will recreate", site_key)
            await _close_site_context(site_key)
        return None
    finally:
        try: await page.close()
        except Exception: pass


def _studapart_card_to_listing(card) -> Optional[Listing]:
    """Parse a Studapart `a.AccomodationBlock` card.

    Card layout (verified 2026-05-01 via Camoufox):
      <a class="AccomodationBlock AccomodationBlock--withDescription"
         href="https://www.studapart.com/fr/logement-{City}/{Slug}/residence/{id}">
      Visible text format examples:
        "Logement en résidence NOMAD Campus The Place à partir de 860€ cc / mois Suresnes"
        "Promotion en cours Logement en résidence Kley Créteil à partir de 660€ cc 7..."
        "Colocation Coliving à Paris (Robida) avec chambre..."
    """
    import re as _re2

    href = card.get("href", "")
    if not href or "/fr/logement-" not in href:
        return None

    # ID from URL: try residence/{id}, else slug fallback
    id_m = _re2.search(r"/residence/(\d+)", href)
    raw_id = id_m.group(1) if id_m else href.rstrip("/").rsplit("/", 1)[-1][:40]
    listing_id = f"sa_{raw_id}"

    text = card.get_text(" ", strip=True)
    text = _re2.sub(r"\s+", " ", text)

    # Price: "à partir de XXX€" pattern (otherwise first standalone N€ run)
    price = None
    pm = _re2.search(r"(?:à\s+partir\s+de\s+)?(\d{3,4})\s*€", text, _re2.IGNORECASE)
    if pm:
        try:
            price = int(pm.group(1))
        except ValueError:
            pass

    # Location: last whitespace-separated token after the price expression
    # Pattern: "...à partir de 860€ cc / mois Suresnes"
    loc_m = _re2.search(r"\d+€[^A-ZÀ-Ÿ]*([A-ZÀ-Ÿ][\wÀ-ÿ\-' ]+?)$", text)
    location = loc_m.group(1).strip() if loc_m else ""
    # Fallback: extract city from URL pattern /fr/logement-{City}/...
    if not location:
        url_loc_m = _re2.search(r"/fr/logement-([A-Za-zÀ-ÿ\-]+)/", href)
        if url_loc_m:
            location = url_loc_m.group(1).replace("-", " ")

    # Title: residence name. Strip status badges then take "résidence NAME"
    # or "Colocation NAME" portion before "à partir de".
    title_m = _re2.search(
        r"(?:résidence|Colocation|Coliving)\s+(.+?)(?:\s+à\s+partir|\s+avec|\s*$)",
        text, _re2.IGNORECASE,
    )
    title = title_m.group(1).strip() if title_m else text[:80]

    images = [
        src for img in card.find_all("img")
        if (src := (img.get("src") or img.get("data-src") or "")).startswith("http")
    ]

    return Listing(
        lbc_id=listing_id,
        title=title,
        description="",
        price=price,
        location=location,
        seller_name="Studapart",
        url=href,
        seller_type_hint="pro",
        source="studapart",
        images=images,
    )


_STUDAPART_TEMPLATE_PATH = "data/studapart_template.json"
_STUDAPART_API_URL = "https://search-api.studapart.com/property"


async def _capture_studapart_template() -> Optional[dict]:
    """One-shot Playwright capture of the search-api POST template. Stores
    to data/studapart_template.json for replay. Returns the captured dict."""
    from playwright.async_api import async_playwright
    captured = []
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            ctx = await browser.new_context()
            page = await ctx.new_page()

            async def on_request(req):
                if "search-api.studapart.com/property" in req.url and req.method == "POST":
                    captured.append({
                        "url": req.url,
                        "post_data": req.post_data,
                    })

            page.on("request", on_request)
            try:
                await page.goto(
                    "https://www.studapart.com/fr/logement-etudiant-paris",
                    wait_until="networkidle", timeout=45_000,
                )
            except Exception:
                pass
            await asyncio.sleep(2)
            await browser.close()
    except Exception as exc:
        logger.warning("[STUDAPART] Template capture failed: %s", exc)
        return None

    if not captured:
        return None
    cap = captured[0]
    try:
        import os
        os.makedirs("data", exist_ok=True)
        with open(_STUDAPART_TEMPLATE_PATH, "w", encoding="utf-8") as f:
            json.dump(cap, f)
    except Exception as exc:
        logger.warning("[STUDAPART] Template save failed: %s", exc)
    return cap


def _studapart_residence_to_listing(item: dict) -> Optional[Listing]:
    """Convert a Studapart property API result to a Listing.

    Dealbreakers applied at scrape time (skip → return None):
      - housingAssistance is False : not APL-eligible (budget breaks)
      - rentedByRoom is True : coloc/coliving (price shown is per-room, not full apt)
      - maxTenantNumber > 2 : 5-person flat-share, not for couple
      - announcementType in {flat_share, coliving, homestay} : shared housing
    Missing field = keep (we treat absence as 'unclear, score normally').
    """
    if not isinstance(item, dict):
        return None
    if item.get("housingAssistance") is False:
        return None
    if item.get("rentedByRoom") is True:
        return None
    mt = item.get("maxTenantNumber")
    if isinstance(mt, (int, float)) and mt > 2:
        return None
    atype = (item.get("announcementType") or "").lower()
    if atype in ("flat_share", "coliving", "homestay"):
        return None
    aid = item.get("_id") or item.get("distinctId")
    if not aid:
        return None
    rent = item.get("rentWithExpensesAmount") or item.get("rentAmount")
    surface = item.get("propertySurface")
    title = item.get("title") or f"Logement {item.get('city', '')}"
    city = item.get("city") or ""
    zip_c = item.get("zipcode") or ""
    location = f"{city}, {zip_c}".strip(", ") if zip_c else city
    canon = item.get("canonicalUrls")
    rel = ""
    if isinstance(canon, dict):
        rel = canon.get("fr") or canon.get("en") or ""
    url = ""
    if isinstance(rel, str) and rel:
        url = f"https://www.studapart.com{rel if rel.startswith('/') else '/' + rel}"
    media = item.get("media") if isinstance(item.get("media"), list) else []
    images = []
    for m in media:
        if isinstance(m, dict):
            u = m.get("finalUrlResidenceSmall")
            if isinstance(u, str):
                images.append(u)
    # onlineAt is a Unix timestamp in seconds
    pub = None
    online = item.get("onlineAt") or item.get("createdAt")
    if isinstance(online, (int, float)):
        from datetime import datetime as _dt
        try: pub = _dt.utcfromtimestamp(online).isoformat()
        except Exception: pass

    # Description was hidden by a prior bug — actually already present in the
    # Elasticsearch _source. Truncate to LLM context budget.
    desc = item.get("description") or ""
    if isinstance(desc, str) and desc:
        desc = _re.sub(r"[ \t]+", " ", desc)
        desc = _re.sub(r"\n{3,}", "\n\n", desc).strip()[:1500]
    else:
        desc = ""

    return Listing(
        lbc_id=f"sa_{aid}",
        title=str(title)[:200],
        description=desc,
        price=int(rent) if isinstance(rent, (int, float)) else None,
        location=location,
        seller_name="Studapart",
        url=url,
        seller_type_hint="agency",
        source="studapart",
        images=images[:6],
        surface=int(surface) if isinstance(surface, (int, float)) else None,
        published_at=pub,
    )


async def _search_studapart_with_playwright(search_url: str, max_results: int) -> list[Listing]:
    """Studapart scraper via internal Elasticsearch API. Captures the request
    template once with Playwright (stored in data/studapart_template.json),
    then replays with httpx, paginating via `must_not.terms.distinctId` to
    walk past the 201-bucket cap (returning ~48 unique per call).

    `search_url` ignored — we use the API directly.
    """
    import os
    import httpx as _httpx

    # Load or capture template
    template_data = None
    if os.path.exists(_STUDAPART_TEMPLATE_PATH):
        try:
            with open(_STUDAPART_TEMPLATE_PATH, "r", encoding="utf-8") as f:
                template_data = json.load(f)
        except Exception:
            template_data = None
    if not template_data:
        logger.info("[STUDAPART] Capturing API template (one-shot Playwright)…")
        template_data = await _capture_studapart_template()
        if not template_data:
            logger.warning("[STUDAPART] Could not capture template — returning 0")
            return []

    base_body = template_data.get("post_data")
    if not base_body:
        return []

    headers = {
        "Content-Type": "application/json",
        "Origin": "https://www.studapart.com",
        "Referer": "https://www.studapart.com/",
        "User-Agent": _HTTPX_UA,
        "Accept": "application/json",
    }

    listings: list[Listing] = []
    seen_ids: set[str] = set()

    try:
        async with _httpx.AsyncClient(headers=headers, timeout=30) as client:
            for attempt in range(8):  # cap pagination loops
                # Inject must_not.terms.distinctId on every Elasticsearch body in 'data'
                body_obj = json.loads(base_body)
                if seen_ids:
                    for d in body_obj.get("data", []):
                        if isinstance(d, dict) and "body" in d:
                            q = d["body"].get("query", {}).get("function_score", {}).get("query", {})
                            if "bool" in q:
                                q["bool"].setdefault("must_not", []).append(
                                    {"terms": {"distinctId": list(seen_ids)}}
                                )

                r = await client.post(_STUDAPART_API_URL, content=json.dumps(body_obj))
                if r.status_code != 200:
                    logger.warning("[STUDAPART] API attempt %d: HTTP %s", attempt + 1, r.status_code)
                    break
                results = r.json().get("results", [])
                new_count = 0
                for item in results:
                    did = item.get("distinctId")
                    if did and did not in seen_ids:
                        seen_ids.add(did)
                        if (l := _studapart_residence_to_listing(item)):
                            listings.append(l)
                            new_count += 1
                            if len(listings) >= max_results:
                                break
                if len(listings) >= max_results or new_count == 0:
                    break
        logger.info("[STUDAPART] Parsed %d listings via API (paginated)", len(listings))
        return listings
    except Exception as exc:
        logger.warning("[STUDAPART] API scrape failed: %s — falling back to HTML", exc)
        # Fallback: old HTML path
        try:
            html = await _fetch_html_with_camoufox(search_url, post_delay=(5.0, 8.0))
            if not html:
                return listings
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(html, "html.parser")
            cards = soup.find_all("a", class_="AccomodationBlock")
            for card in cards[:max_results]:
                if (l := _studapart_card_to_listing(card)):
                    listings.append(l)
        except Exception:
            pass
        return listings


def _parisattitude_card_to_listing(card) -> Optional[Listing]:
    """Parse a single Paris Attitude `div.accommodation-search-card`.

    Card layout (verified 2026-05-01):
      div.accommodation-search-card
        a[href*="/rent-apartment/{slug},apartment,{type},{id}.aspx"]
        .accommodation-card-content__price → "1 550 €"
      Visible text in order:
        "Paris Attitude Selection | 1 bedroom 28m² | Poissonnière, Paris 10 |
         1 550 € | /Month | Available from | 09 May 2026"
    """
    import re as _re2

    link = card.find("a", href=_re2.compile(r"/rent-apartment/[^,]+,apartment,[^,]+,\d+\.aspx"))
    if not link:
        return None
    href = link["href"]
    if href.startswith("/"):
        href = "https://www.parisattitude.com" + href

    id_m = _re2.search(r",(\d+)\.aspx", href)
    if not id_m:
        return None
    listing_id = f"pa_{id_m.group(1)}"

    price = None
    price_el = card.find(class_="accommodation-card-content__price")
    if price_el:
        raw = price_el.get_text(strip=True)
        digits = "".join(c for c in raw if c.isdigit())
        if digits:
            try:
                price = int(digits)
            except ValueError:
                price = None

    parts = [p.strip() for p in card.get_text(separator="|", strip=True).split("|") if p.strip()]
    type_part = next((p for p in parts if "m²" in p or "bedroom" in p.lower() or "studio" in p.lower()), "")
    # Real PA locations look like "Poissonnière, Paris 10" or "Montmartre / Place des
    # Abbesses, Paris 18". The "Paris Attitude Selection" label is a featured-listing
    # tag, not a real address — require a Paris arrondissement number to qualify.
    location = ""
    arr_re = _re.compile(r"\bParis\s+\d{1,2}\b")
    for p in parts:
        if arr_re.search(p) and "€" not in p:
            location = p
            break

    title = type_part if type_part else (parts[1] if len(parts) > 1 else "Appartement")
    if location and type_part:
        title = f"{type_part} — {location}"

    # Surface: type_part is e.g. "1 bedroom 28m²" or "Studio 24m²"
    surface_m = _re.search(r"(\d+)(?:[,.]\d+)?\s*m²", type_part)
    surface = _to_int_safe(surface_m.group(1)) if surface_m else None

    images = [
        src for img in card.find_all("img")
        if (src := (img.get("src") or img.get("data-src") or "")).startswith("http")
    ]

    return Listing(
        lbc_id=listing_id,
        title=title,
        description="",
        price=price,
        location=location,
        seller_name="Paris Attitude",
        url=href,
        seller_type_hint="pro",
        source="parisattitude",
        images=images,
        surface=surface,
    )


_PA_SEARCH_API = "https://prod-api-hanok.parisattitude.com/api/Accommodation/Search"
_PA_PARIS_ZONES = list(range(1, 21))  # arrondissements 1-20


def _frame_to_listing(frame: dict) -> Optional[Listing]:
    """Convert a PA Search API frame to a Listing.

    Filters out listings whose `nextAvailability` is after Sept 30 2026 —
    Illan needs the apt for Aug/Sept 2026 move-in, owners typically don't
    hold for 6+ months. Anything available 2026-10 onwards = dealbreaker."""
    aid = frame.get("accommodationID")
    if not aid:
        return None

    # Pass through ALL listings with their actual nextAvailability — the
    # score-time dealbreaker (agent.py: avail > 2026-09 → score=0) will hide
    # late-availability listings from the dashboard. Filtering at scrape
    # time leaves stale data for listings whose date moves past 2026-09
    # between scrapes.
    avail = frame.get("nextAvailability") or ""

    rent = frame.get("monthlyRent") or frame.get("applicableRent")
    surface = frame.get("carrezSurfaceArea")
    type_label = frame.get("accommodationTypeLabel") or "Studio"
    borough = (frame.get("boroughLabel") or "").strip()
    zip_code = frame.get("zipCode") or ""

    arr_label = "Paris"
    if zip_code.startswith("75") and len(zip_code) == 5:
        try: arr_label = f"Paris {int(zip_code[3:])}"
        except ValueError: pass

    # Embed the 5-digit zip so is_critical_zone() and dashboard dept-filters
    # can match. Without it, 1900+ PA listings were invisible to dept queries.
    base_loc = f"{borough}, {arr_label}" if borough else arr_label
    location = f"{base_loc}, {zip_code}" if zip_code else base_loc
    title = f"{type_label} {int(surface)}m² {borough}".strip() if surface else f"{type_label} {borough}".strip()
    # PA listing URLs auto-redirect from a stub slug — saves us from constructing
    # the canonical slug from boroughLabel.
    url = f"https://www.parisattitude.com/fr/louer-appartement/x,x,x,{aid}.aspx"

    # nextAvailability is structured ISO datetime — extract YYYY-MM-DD directly,
    # no LLM needed. Format: "2026-09-15T00:00:00+02:00" → "2026-09-15".
    avail_from = avail[:10] if isinstance(avail, str) and len(avail) >= 10 else None

    return Listing(
        lbc_id=f"pa_{aid}",
        title=title[:200],
        description="",
        price=int(rent) if rent else None,
        location=location,
        seller_name="Paris Attitude",
        url=url,
        seller_type_hint="pro",
        source="parisattitude",
        surface=int(surface) if surface else None,
        published_at=frame.get("publishedOn") if isinstance(frame.get("publishedOn"), str) else None,
        available_from=avail_from,
    )


async def _search_parisattitude_with_playwright(search_url: str, max_results: int) -> list[Listing]:
    """API-based scraper using Paris Attitude's internal Vue.js backend.

    The HTML index page is capped at 40 cards. The sitemap exposes 7600 URLs
    but each detail page is a 700KB SSR fetch. The Vue SPA's backend at
    `prod-api-hanok.parisattitude.com/api/Accommodation/Search` is
    unauthenticated and returns paginated JSON of ALL listings with full
    summary fields (price, surface, type, borough, GPS, photos).

    Strategy:
      - POST /Search with `mode=1` (long-term), `maxBudget=1500`, `size=100`,
        `geographicalZoneIDs=[1..20]` (Paris arrondissements).
      - Paginate until exhausted. Typically ~7-10 pages, ~3-5s total.
      - Parse JSON frames directly into Listings.

    `search_url` is kept for dispatcher signature compatibility but ignored.
    """
    import httpx

    MAX_BUDGET = 1500  # Cover Illan's 1000€ + margin for "almost in budget" listings
    listings: list[Listing] = []
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            page = 1
            while True:
                body = {
                    "geographicalZoneIDs": _PA_PARIS_ZONES,
                    "page": page,
                    "size": 100,
                    "mode": 1,
                    "maxBudget": MAX_BUDGET,
                }
                r = await client.post(_PA_SEARCH_API, json=body)
                if r.status_code != 200:
                    logger.warning("[PARISATTITUDE] API page %d: HTTP %s", page, r.status_code)
                    break
                data = r.json().get("content", {})
                frames = data.get("accommodationFrames", [])
                if not frames:
                    break
                for f in frames:
                    if (l := _frame_to_listing(f)):
                        listings.append(l)
                pages = data.get("pages", 0)
                if page >= pages:
                    break
                page += 1
        logger.info(
            "[PARISATTITUDE] Parsed %d listings via API (≤%d€, all 20 arr.)",
            len(listings), MAX_BUDGET,
        )
        # Activity check: PA's Search API stops returning listings once the
        # apartment is rented or removed from catalog. The DB keeps stale
        # available_from values until they're refreshed. NULL them when an ID
        # disappears so the dashboard shows blank instead of wrong dates.
        try:
            import database as _db
            seen_ids = {l.lbc_id for l in listings}
            with _db._conn() as conn:
                rows = conn.execute(
                    "SELECT lbc_id FROM listings WHERE source='parisattitude' "
                    "AND available_from IS NOT NULL"
                ).fetchall()
                stale = [r[0] for r in rows if r[0] not in seen_ids]
                if stale:
                    placeholders = ",".join("?" * len(stale))
                    conn.execute(
                        f"UPDATE listings SET available_from=NULL WHERE lbc_id IN ({placeholders})",
                        stale,
                    )
                    logger.info("[PARISATTITUDE] activity check: cleared %d stale dates", len(stale))
        except Exception as exc:
            logger.warning("[PARISATTITUDE] activity check failed: %s", exc)
        return listings
    except Exception as exc:
        logger.warning("[PARISATTITUDE] API scrape failed: %s", exc)
        return []


def _lodgis_card_to_listing(card) -> Optional[Listing]:
    """Parse a Lodgis `div.card__appart`.

    Card text format (verified 2026-05-01, /en/ version):
      "Furnished studio | No.1011056 | 18 m² | Louvre | €1,045 | /month |
       Available from | 01-01-2027 | Paris 1°"
    URL pattern: /en/paris,long-term-rentals/apartment/LPA{slug}-paris-{arr}.mod.html
    """
    import re as _re2

    link = card.find("a", href=True)
    if not link:
        return None
    href = link["href"]
    if href.startswith("/"):
        href = "https://www.lodgis.com" + href

    # ID from "No.NNNNN" element
    num_el = card.find(class_="card__appart__num")
    raw_id = ""
    if num_el:
        m = _re2.search(r"\d+", num_el.get_text(strip=True))
        raw_id = m.group(0) if m else ""
    if not raw_id:
        # Fallback: extract LPA{N} from URL
        m = _re2.search(r"LPA(\d+)", href)
        raw_id = m.group(1) if m else ""
    if not raw_id:
        return None
    listing_id = f"lg_{raw_id}"

    text = _re2.sub(r"\s+", " ", card.get_text(" ", strip=True))

    # Price: "€1,045" — € before number with optional thousands comma
    price = None
    pm = _re2.search(r"€\s*([\d,]+)", text)
    if pm:
        try:
            price = int(pm.group(1).replace(",", ""))
        except ValueError:
            pass

    # Surface: "X m²"
    sm = _re2.search(r"(\d+)(?:[,.]\d+)?\s*m²", text)
    surface = sm.group(1) if sm else ""

    # Location: "Paris X°" + neighborhood preceding €. Lodgis card text never
    # carries an explicit zip, so we synthesise it from the arrondissement
    # number (Paris 1° → 75001) — required for is_critical_zone() to match.
    arr_m = _re2.search(r"Paris\s+(\d{1,2})\s*°?", text)
    location = ""
    if arr_m:
        try:
            arr_n = int(arr_m.group(1))
        except ValueError:
            arr_n = 0
        zip_code = f"750{arr_n:02d}" if 1 <= arr_n <= 20 else ""
        # Try to find neighborhood — usually the segment right before "€"
        neighborhood_m = _re2.search(r"\|\s*([^|€]+?)\s*\|\s*€", text)
        if neighborhood_m:
            location = f"{neighborhood_m.group(1).strip()}, Paris {arr_n}"
        else:
            location = f"Paris {arr_n}"
        if zip_code:
            location = f"{location}, {zip_code}"

    # Title — first non-empty segment
    title_parts = [p.strip() for p in text.split("|") if p.strip()]
    title = title_parts[0] if title_parts else "Furnished apartment"
    if surface and location:
        title = f"{title} {surface}m² — {location}"

    images = [
        src for img in card.find_all("img")
        if (src := (img.get("src") or img.get("data-src") or "")).startswith("http")
    ]

    return Listing(
        lbc_id=listing_id,
        title=title,
        description="",
        price=price,
        location=location,
        seller_name="Lodgis",
        url=href,
        seller_type_hint="pro",
        source="lodgis",
        images=images,
        surface=_to_int_safe(surface),
    )


def _lodgis_enrich_detail(html: str) -> dict:
    """Extract dealbreaker info + description from a Lodgis detail page (EN version)."""
    import html as _htmllib
    from bs4 import BeautifulSoup

    out: dict = {}
    # Floor + lift in same phrase: "on the 4 floor (no lift)"
    if (m := _re.search(r"on the\s+(\d+)\s*(?:st|nd|rd|th)?\s+floor\s*\((no|with)\s+lift\)", html, _re.I)):
        try: out["floor"] = int(m.group(1))
        except Exception: pass
        out["elevator"] = (m.group(2).lower() == "with")
    elif (m := _re.search(r"(\d+)(?:st|nd|rd|th)?\s+floor", html, _re.I)):
        try: out["floor"] = int(m.group(1))
        except Exception: pass
    if (m := _re.search(r"min(?:imum)?\s+(\d+)\s+months?", html, _re.I)):
        try: out["min_lease_months"] = int(m.group(1))
        except Exception: pass
    if (m := _re.search(r"Available from\s*[:\s]*(\d{1,2})[-/](\d{1,2})[-/](\d{4})", html, _re.I)):
        out["available_yyyy_mm"] = f"{m.group(3)}-{int(m.group(2)):02d}-{int(m.group(1)):02d}"

    # Lodgis HQ phone — single tel: link on every detail page (they're the agent for every listing)
    if (m := _re.search(r'href="tel:([+\d]+)"', html)):
        raw = m.group(1).lstrip("+")
        if raw.startswith("33"):
            raw = "0" + raw[2:]
        if _re.fullmatch(r"0[1-9]\d{8}", raw):
            out["phone"] = raw

    # Description: long English paragraph in <div class="appart__infos__description">.
    # Verified live: 645–810 chars of real listing copy (location, surface, transit, nearby amenities).
    try:
        soup = BeautifulSoup(html, "html.parser")
        node = soup.find("div", class_="appart__infos__description")
        if node:
            txt = node.get_text(" ", strip=True)
            txt = _htmllib.unescape(txt)
            txt = _re.sub(r"\s+", " ", txt).strip()
            if 50 <= len(txt) <= 5000:
                out["description"] = txt
    except Exception:
        pass
    return out


async def _search_lodgis_with_playwright(search_url: str, max_results: int) -> list[Listing]:
    """Lodgis scraper — plain httpx + parallel detail enrichment.
    Search returns 51 listings/page; detail page gives floor+elevator+
    available_date. All Lodgis listings are furnished by default."""
    from bs4 import BeautifulSoup

    sep = "&" if "?" in search_url else "?"
    pages = [search_url] + [f"{search_url}{sep}p={n}" for n in range(2, 71)]
    logger.info("[LODGIS] Fetching %d pages via httpx (parallel)", len(pages))
    htmls = await _fetch_pages_httpx(pages)

    listings: list[Listing] = []
    for html in htmls:
        if not html:
            continue
        soup = BeautifulSoup(html, "html.parser")
        for card in soup.find_all("div", class_="card__appart"):
            if (l := _lodgis_card_to_listing(card)):
                listings.append(l)
                if len(listings) >= max_results:
                    break
        if len(listings) >= max_results:
            break

    # Phase 2: detail enrichment for floor+elevator+available_date
    if listings:
        sem = asyncio.Semaphore(10)
        UA = {"User-Agent": _HTTPX_UA}
        import httpx as _httpx
        async with _httpx.AsyncClient(headers=UA, timeout=15, follow_redirects=True) as client:
            async def _enrich(lst):
                async with sem:
                    try:
                        r = await client.get(lst.url)
                        if r.status_code != 200:
                            return None
                        info = _lodgis_enrich_detail(r.text)
                    except Exception:
                        return None
                    if info.get("available_yyyy_mm") and info["available_yyyy_mm"][:7] > "2026-09":
                        return lst.lbc_id
                    floor = info.get("floor")
                    if isinstance(floor, int) and floor > 3 and info.get("elevator") is False:
                        return lst.lbc_id
                    if (av := info.get("available_yyyy_mm")):
                        lst.available_from = av
                    if (ph := info.get("phone")):
                        lst.phone = ph
                    if (desc := info.get("description")):
                        lst.description = desc  # replace empty placeholder
                    tags = []
                    if floor is not None: tags.append(f"[ÉTAGE: {floor}]")
                    if info.get("elevator") is True: tags.append("[ASCENSEUR: oui]")
                    elif info.get("elevator") is False: tags.append("[ASCENSEUR: non]")
                    if (av := info.get("available_yyyy_mm")): tags.append(f"[DISPO: {av}]")
                    if tags:
                        lst.description = (lst.description + "\n" + " ".join(tags)).strip()
                    return None
            drops = await asyncio.gather(*(_enrich(l) for l in listings))
            drop_set = {d for d in drops if d}
            if drop_set:
                logger.info("[LODGIS] Dropped %d (dealbreakers from detail)", len(drop_set))
                listings = [l for l in listings if l.lbc_id not in drop_set]

    logger.info("[LODGIS] Parsed %d listings (with detail enrichment)", len(listings))
    return listings


def _immojeune_card_to_listing(card) -> Optional[Listing]:
    """Parse an ImmoJeune `div.card` element.

    Card text format (verified 2026-05-01):
      "...{TYPE}|{TITLE}|{DESC}|{SURFACE}m² - {PRICE} €|CC|{ZIP} {CITY}"
    Title anchor lives inside `<p class="title">`.
    URL pattern: /{type-segment}/{slug}_{numeric_id}.html
    """
    import re as _re2

    title_p = card.find("p", class_="title")
    if not title_p:
        return None
    link = title_p.find("a", href=True)
    if not link:
        return None
    href = link["href"]
    if href.startswith("/"):
        href = "https://www.immojeune.com" + href

    id_m = _re2.search(r"_(\d+)\.html", href)
    if not id_m:
        return None
    listing_id = f"ij_{id_m.group(1)}"

    title = link.get_text(strip=True)

    text = _re2.sub(r"\s+", " ", card.get_text(" ", strip=True))

    # "X m² - Y €" — Y is the actual rent
    price = None
    surface = ""
    sm = _re2.search(r"(\d+)(?:[,.]\d+)?\s*m²\s*-\s*(\d+)\s*€", text)
    if sm:
        surface = sm.group(1)
        try:
            price = int(sm.group(2))
            if price == 0:
                price = None
        except ValueError:
            pass

    # "{5-digit-zip} {City}" — the city is what we want for dedup matching
    location = ""
    loc_m = _re2.search(r"(\d{5})\s+([A-ZÀ-Ÿ][\wÀ-ÿ\-' ]+?)(?:\s|$|\|)", text)
    if loc_m:
        location = f"{loc_m.group(2).strip()}, {loc_m.group(1)}"

    desc_el = card.find(class_="description")
    description = desc_el.get_text(strip=True) if desc_el else ""

    # ImmoJeune URL prefix encodes housing type — embed as a tag in description
    # so detect_housing_type catches it (URL is more reliable than title for IJ).
    if "/residence-etudiante/" in href:
        description = (description + "\nrésidence étudiante").strip()
    elif "/colocation/" in href:
        description = (description + "\ncolocation").strip()
    elif "/coliving/" in href:
        description = (description + "\ncoliving").strip()
    elif "/location-courte-duree/" in href:
        description = (description + "\nlocation courte durée").strip()

    images = [
        src for img in card.find_all("img")
        if (src := (img.get("src") or img.get("data-src") or "")).startswith("http")
    ]

    return Listing(
        lbc_id=listing_id,
        title=title,
        description=description,
        price=price,
        location=location,
        seller_name="ImmoJeune",
        url=href,
        seller_type_hint="",
        source="immojeune",
        images=images,
        surface=_to_int_safe(surface),
    )


def _immojeune_enrich_detail(html: str) -> dict:
    """Extract ImmoJeune feature flags from icon alt-text + publication date
    from "Publiée il y a X jours/mois" relative phrasing."""
    out: dict = {}
    alts = set(_re.findall(r'alt="([^"]+)"', html))
    if "Meublé" in alts: out["furnished"] = True
    if "Ascenseur" in alts: out["elevator"] = True
    if "Chauffage" in alts: out["has_heating"] = True
    if (m := _re.search(r"Disponible(?:\s+immédiatement|\s+le|\s+à partir du)?\s*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})", html, _re.I)):
        ds = m.group(1).replace("-", "/")
        parts = ds.split("/")
        if len(parts) == 3:
            yyyy = parts[2] if len(parts[2]) == 4 else f"20{parts[2]}"
            try:
                out["available_yyyy_mm"] = f"{yyyy}-{int(parts[1]):02d}"
            except Exception: pass
    # Publication date: "Publiée il y a 2 mois", "Publiée il y a 5 jours"
    if (m := _re.search(r"Publi[ée]e?\s+il y a\s+(\d+)\s+(jour|semaine|mois|an)", html, _re.I)):
        from datetime import datetime as _dt, timedelta as _td
        n = int(m.group(1))
        unit = m.group(2).lower()
        days = n * {"jour": 1, "semaine": 7, "mois": 30, "an": 365}[unit]
        out["published_at"] = (_dt.utcnow() - _td(days=days)).isoformat()
    # Phone: ImmoJeune owners often write "Tel : 06 XX XX XX XX" in description body
    text = _re.sub(r"<[^>]+>", " ", html)
    if (m := _re.search(r"\b(0[1-9](?:[\s.\-]?\d{2}){4})\b", text)):
        out["phone"] = _re.sub(r"[\s.\-]", "", m.group(1))
    return out


async def _search_immojeune_with_playwright(search_url: str, max_results: int) -> list[Listing]:
    """ImmoJeune — plain httpx + parallel detail enrichment via alt-img icons.
    Detail page exposes feature presence as `<img alt="Meublé">` etc."""
    from bs4 import BeautifulSoup

    base_no_ext, _, ext = search_url.rpartition(".html")
    pages = [search_url]
    if ext == "" and base_no_ext:
        pages += [f"{base_no_ext}/{n}" for n in range(2, 16)]
    logger.info("[IMMOJEUNE] Fetching %d pages via httpx", len(pages))
    htmls = await _fetch_pages_httpx(pages)

    listings: list[Listing] = []
    seen_ids: set[str] = set()
    for html in htmls:
        if not html:
            continue
        soup = BeautifulSoup(html, "html.parser")
        for card in soup.find_all("div", class_="card"):
            l = _immojeune_card_to_listing(card)
            if l and l.lbc_id not in seen_ids:
                seen_ids.add(l.lbc_id)
                listings.append(l)
                if len(listings) >= max_results:
                    break
        if len(listings) >= max_results:
            break

    # Phase 2: detail-page enrichment for icon-based features
    if listings:
        sem = asyncio.Semaphore(10)
        import httpx as _httpx
        async with _httpx.AsyncClient(headers={"User-Agent": _HTTPX_UA}, timeout=15, follow_redirects=True) as client:
            async def _enrich(lst):
                async with sem:
                    try:
                        r = await client.get(lst.url)
                        if r.status_code != 200:
                            return None
                        info = _immojeune_enrich_detail(r.text)
                    except Exception:
                        return None
                    if info.get("available_yyyy_mm") and info["available_yyyy_mm"][:7] > "2026-09":
                        return lst.lbc_id
                    if (pub := info.get("published_at")):
                        lst.published_at = pub
                    if (ph := info.get("phone")):
                        lst.phone = ph
                    tags = []
                    if info.get("furnished") is True: tags.append("[MEUBLÉ: oui]")
                    if info.get("elevator") is True: tags.append("[ASCENSEUR: oui]")
                    if (av := info.get("available_yyyy_mm")): tags.append(f"[DISPO: {av}]")
                    if tags:
                        lst.description = (lst.description + "\n" + " ".join(tags)).strip()
                    return None
            drops = await asyncio.gather(*(_enrich(l) for l in listings))
            drop_set = {d for d in drops if d}
            if drop_set:
                logger.info("[IMMOJEUNE] Dropped %d (dispo > sept 2026)", len(drop_set))
                listings = [l for l in listings if l.lbc_id not in drop_set]

    logger.info("[IMMOJEUNE] Parsed %d listings (with detail)", len(listings))
    return listings


def _locservice_card_to_listing(card) -> Optional[Listing]:
    """Parse a LocService `li.accommodation-ad` card.

    Card text format (verified 2026-05-01):
      "Appartement T2 meublé à louer | Paris 17 (75017) | 30 m² | 1 254 € / mois | {desc}"
    URL pattern: /paris-75/location-{type}-paris-{arr}/{numeric_id}
    Price element: <li class="accommodation-ad-characteristic"> containing "€ / mois".
    """
    import re as _re2

    link = card.find("a", href=True)
    if not link:
        return None
    href = link["href"]
    if href.startswith("/"):
        href = "https://www.locservice.fr" + href

    id_m = _re2.search(r"/(\d+)/?$", href)
    if not id_m:
        return None
    listing_id = f"ls_{id_m.group(1)}"

    # Find the price <li> — its text contains "€ / mois" (narrow no-break spaces)
    price = None
    for li in card.find_all("li"):
        txt = li.get_text(" ", strip=True)
        if "mois" in txt and "€" in txt:
            digits = _re2.sub(r"[^\d]", "", txt.split("€")[0])
            if digits:
                try:
                    price = int(digits)
                except ValueError:
                    pass
            break

    text = _re2.sub(r"\s+", " ", card.get_text(" ", strip=True))

    # Title — first text up to the location marker.
    # IDF formats seen:
    #   "Appartement T2 ... | Paris 17 (75017) | ..."  → Paris arr.
    #   "Studio meublé ... | Boulogne-Billancourt (92100) | ..."  → IDF city
    title_m = (
        _re2.search(r"^(.+?)\s+Paris\s+\d{1,2}\s*\(", text)
        or _re2.search(r"^(.+?)\s+[A-ZÉÈÀ][\w\-' ]+?\s*\(\d{5}\)", text)
    )
    title = title_m.group(1).strip() if title_m else (text[:80] if text else "Appartement")

    # Location: prefer "Paris XX, 75XXX"; else "{City}, {Zip}"; else just zip
    location = ""
    loc_m = _re2.search(r"(Paris\s+\d{1,2})\s*\((\d{5})\)", text)
    if loc_m:
        location = f"{loc_m.group(1)}, {loc_m.group(2)}"
    else:
        city_m = _re2.search(r"\b([A-ZÉÈÀ][\w\-' ]+?)\s*\((\d{5})\)", text)
        if city_m:
            location = f"{city_m.group(1).strip()}, {city_m.group(2)}"
        else:
            zip_m = _re2.search(r"\b(\d{5})\b", text)
            if zip_m:
                location = zip_m.group(1)

    # Description — text after the price up to ~500 chars
    desc_m = _re2.search(r"€\s*/\s*mois\s*(.+?)$", text)
    description = desc_m.group(1).strip()[:500] if desc_m else ""

    # Surface: "30 m²" pattern
    surf_m = _re2.search(r"(\d+)(?:[,.]\d+)?\s*m²", text)
    surface = _to_int_safe(surf_m.group(1)) if surf_m else None

    images = [
        src for img in card.find_all("img")
        if (src := (img.get("src") or img.get("data-src") or "")).startswith("http")
    ]

    return Listing(
        lbc_id=listing_id,
        title=title,
        description=description,
        price=price,
        location=location,
        seller_name="Particulier",
        url=href,
        seller_type_hint="particulier",
        source="locservice",
        images=images,
        surface=surface,
    )


def _locservice_enrich_detail(html: str) -> dict:
    """LocService detail page — plain text contains 'non meublé' / floor / asc.
    Also captures the marketing description from <div class='description-content'>."""
    import html as _htmllib
    out: dict = {}
    if _re.search(r"non\s*-?\s*meubl[ée]?", html, _re.I):
        out["furnished"] = False
    if (m := _re.search(r"(\d+)(?:er|ème|e|ᵉ)?\s*[éE]tage", html)):
        try: out["floor"] = int(m.group(1))
        except Exception: pass
    if _re.search(r"(?:pas|sans)\s+d.?ascenseur", html, _re.I):
        out["elevator"] = False
    elif _re.search(r"avec\s+ascenseur", html, _re.I):
        out["elevator"] = True
    # Description — typically 200-1500 chars in <div class="description-content">
    m = _re.search(r'<div[^>]+class="[^"]*description-content[^"]*"[^>]*>(.+?)</div>',
                   html, _re.S)
    if m:
        raw = m.group(1).replace("<br />", "\n").replace("<br/>", "\n").replace("<br>", "\n")
        txt = _re.sub(r"<[^>]+>", "", raw)
        txt = _htmllib.unescape(txt)
        txt = _re.sub(r"[ \t]+", " ", txt)
        txt = _re.sub(r"\n{3,}", "\n\n", txt).strip()
        if 80 <= len(txt) <= 5000:
            out["description"] = txt[:1500]
    return out


async def _search_locservice_with_playwright(search_url: str, max_results: int) -> list[Listing]:
    """LocService — plain httpx + parallel detail enrichment. Detail page
    has 'non meublé' / 'Xème étage' / 'pas d'ascenseur' as plain text.

    2026-05-05 — IDF coverage : LocService n'a pas d'URL aggrégée IDF mais
    chaque département a son slug. On itère donc les 8 départements IDF
    (75/77/78/91/92/93/94/95) chacun sur 4 pages de listings (`-p2`, `-p3`,
    `-p4`). `search_url` est utilisé comme template — si non-LocService, on
    fallback sur Paris.
    """
    from bs4 import BeautifulSoup

    # IDF dept slugs for /(slug)/location-appartement.html
    _IDF_SLUGS = [
        "paris-75",
        "seine-et-marne-77",
        "yvelines-78",
        "essonne-91",
        "hauts-de-seine-92",
        "seine-saint-denis-93",
        "val-de-marne-94",
        "val-d-oise-95",
    ]

    pages: list[str] = []
    if "locservice.fr" in search_url:
        # Build full IDF coverage : for each dept, page 1 + pages 2..4
        for slug in _IDF_SLUGS:
            base_dept = f"https://www.locservice.fr/{slug}/location-appartement"
            pages.append(f"{base_dept}.html")
            for n in range(2, 5):  # 3 extra pages per dept = 4 pages × 8 depts = 32
                pages.append(f"{base_dept}-p{n}.html")
    else:
        # Fallback for direct call with custom URL
        base, _, ext = search_url.rpartition(".html")
        pages = [search_url] + [f"{base}-p{n}.html" for n in range(2, 11) if base]
    logger.info("[LOCSERVICE] Fetching %d pages via httpx (IDF: %d depts)",
                len(pages), len(_IDF_SLUGS) if "locservice.fr" in search_url else 1)
    htmls = await _fetch_pages_httpx(pages)

    listings: list[Listing] = []
    for html in htmls:
        if not html:
            continue
        soup = BeautifulSoup(html, "html.parser")
        for card in soup.find_all("li", class_="accommodation-ad"):
            if (l := _locservice_card_to_listing(card)):
                listings.append(l)
                if len(listings) >= max_results:
                    break
        if len(listings) >= max_results:
            break

    if listings:
        sem = asyncio.Semaphore(10)
        import httpx as _httpx
        async with _httpx.AsyncClient(headers={"User-Agent": _HTTPX_UA}, timeout=15, follow_redirects=True) as client:
            async def _enrich(lst):
                async with sem:
                    try:
                        r = await client.get(lst.url)
                        if r.status_code != 200: return None
                        info = _locservice_enrich_detail(r.text)
                    except Exception:
                        return None
                    if info.get("furnished") is False:
                        return lst.lbc_id
                    floor = info.get("floor")
                    if isinstance(floor, int) and floor > 3 and info.get("elevator") is False:
                        return lst.lbc_id
                    if (desc := info.get("description")):
                        lst.description = desc
                    tags = []
                    if floor is not None: tags.append(f"[ÉTAGE: {floor}]")
                    if info.get("elevator") is True: tags.append("[ASCENSEUR: oui]")
                    elif info.get("elevator") is False: tags.append("[ASCENSEUR: non]")
                    if tags:
                        lst.description = (lst.description + "\n" + " ".join(tags)).strip()
                    return None
            drops = await asyncio.gather(*(_enrich(l) for l in listings))
            drop_set = {d for d in drops if d}
            if drop_set:
                logger.info("[LOCSERVICE] Dropped %d (non-meublé / étage>3 sans asc)", len(drop_set))
                listings = [l for l in listings if l.lbc_id not in drop_set]

    logger.info("[LOCSERVICE] Parsed %d listings (with detail)", len(listings))
    return listings


# ─── EntreParticuliers / L'Adresse / Century 21 (3 new agency/p2p sites) ─────

async def _search_entreparticuliers(search_url: str, max_results: int) -> list[Listing]:
    """EntreParticuliers — 12 listings per dept page. 2026-05-05 update :
    we loop the 8 IDF *departments* (75/77/78/91/92/93/94/95) in parallel —
    each dept URL renders 12 unique recent listings.
    `search_url` is ignored — we use the per-dept index URL pattern.

    Note the live href pattern is `/appartement/location/{city-slug}/{listing-slug}/ref-{id}`
    (TWO slug segments before /ref-), which the previous single-segment regex
    failed to match. Fixed inline.
    """
    base = "https://www.entreparticuliers.com/annonces-immobilieres/location/appartement"
    idf_depts = [
        "paris-75",
        "seine-et-marne-77",
        "yvelines-78",
        "essonne-91",
        "hauts-de-seine-92",
        "seine-saint-denis-93",
        "val-de-marne-94",
        "val-d-oise-95",
    ]
    pages = [f"{base}/{slug}" for slug in idf_depts]
    htmls = await _fetch_pages_curl_cffi(pages)
    listings: list[Listing] = []
    seen: set[str] = set()
    for html in htmls:
        if not html:
            continue
        # ref pattern: href="/annonces-immobilieres/appartement/location/{city-slug}/{listing-slug}/ref-{id}"
        for m in _re.finditer(
            r'href="(/annonces-immobilieres/appartement/location/([a-z0-9-]+)/[a-z0-9-]+/ref-(\d+))"',
            html,
        ):
            href, slug, rid = m.group(1), m.group(2), m.group(3)
            if rid in seen:
                continue
            seen.add(rid)
            url = "https://www.entreparticuliers.com" + href
            # Extract surface from slug if possible (e.g. "studio-de-20m2")
            surface = None
            sm = _re.search(r"(\d+)\s*m2", slug)
            if sm:
                try: surface = int(sm.group(1))
                except ValueError: pass
            # Try to find price near the href in the HTML
            ctx = html[max(0, m.start() - 500): m.end() + 200]
            price = None
            pm = _re.search(r"(\d[\d\s]{1,5})\s*€", ctx)
            if pm:
                digits = "".join(c for c in pm.group(1) if c.isdigit())
                if digits:
                    try: price = int(digits)
                    except ValueError: pass
            listings.append(Listing(
                lbc_id=f"ep_{rid}",
                title=slug.replace("-", " ").capitalize()[:200],
                description="",
                price=price,
                location=slug,  # arrondissement embedded in slug
                seller_name="Particulier",
                url=url,
                seller_type_hint="particulier",
                source="entreparticuliers",
                surface=surface,
            ))
            if len(listings) >= max_results:
                break
        if len(listings) >= max_results:
            break
    logger.info("[ENTREPARTICULIERS] Parsed %d listings (8 IDF depts)", len(listings))
    return listings


async def _search_ladresse(search_url: str, max_results: int) -> list[Listing]:
    """L'Adresse (agency network) — 40 listings/page on IDF search."""
    sep = "?" if "?" not in search_url else "&"
    pages = [search_url] + [f"{search_url}{sep}page={n}" for n in range(2, 11)]
    htmls = await _fetch_pages_curl_cffi(pages)
    listings: list[Listing] = []
    seen: set[str] = set()
    for html in htmls:
        if not html:
            continue
        for m in _re.finditer(
            r'href="(/annonce/location/appartement/([a-z0-9-]+)/(\d+))"',
            html,
        ):
            href, slug, rid = m.group(1), m.group(2), m.group(3)
            if rid in seen:
                continue
            # Filter to Paris arrondissements only
            if not slug.startswith("paris-750"):
                continue
            seen.add(rid)
            url = "https://www.ladresse.com" + href
            ctx = html[max(0, m.start() - 600): m.end() + 200]
            price = None
            pm = _re.search(r"(\d[\d\s]{1,5})\s*€", ctx)
            if pm:
                digits = "".join(c for c in pm.group(1) if c.isdigit())
                if digits:
                    try: price = int(digits)
                    except ValueError: pass
            surface = None
            sm = _re.search(r"(\d+)(?:[,.]\d+)?\s*m²", ctx)
            if sm:
                try: surface = int(sm.group(1))
                except ValueError: pass
            listings.append(Listing(
                lbc_id=f"la_{rid}",
                title=slug.replace("-", " ").capitalize()[:200],
                description="",
                price=price,
                location=slug.replace("paris-", "Paris "),
                seller_name="L'Adresse",
                url=url,
                seller_type_hint="pro",
                source="ladresse",
                surface=surface,
            ))
            if len(listings) >= max_results:
                break
        if len(listings) >= max_results:
            break
    logger.info("[LADRESSE] Parsed %d listings", len(listings))
    return listings


async def _search_century21(search_url: str, max_results: int) -> list[Listing]:
    """Century 21 — paginates /page-N/, ~10 listings/page, 9 pages typically.

    2026-05-05 — IDF coverage : Century 21 n'a pas d'URL région IDF (les
    variantes `r-`, `d-`, `v-ile-de-france` retournent toutes HTTP 410).
    On itère donc les 12 villes IDF clés où Century 21 a une agence active
    (Paris + 11 villes proches couvrant les 7 autres départements).
    Si `search_url` n'est pas Century 21, on fallback sur Paris.
    """
    # Major IDF cities with active C21 agencies (verified live, returns 200 + listings)
    _IDF_CITIES = [
        "paris",            # 75
        "creteil",          # 94
        "versailles",       # 78
        "nanterre",         # 92
        "cergy",            # 95
        "meaux",            # 77
        "saint-maur-des-fosses",  # 94
        "noisy-le-grand",   # 93
        "antony",           # 92
        "vitry-sur-seine",  # 94
        "argenteuil",       # 95
        "evry-courcouronnes",  # 91
    ]

    pages: list[str] = []
    if "century21.fr" in search_url:
        for city in _IDF_CITIES:
            base = f"https://www.century21.fr/annonces/f/location/v-{city}/"
            pages.append(base)
            # 2 additional pages per city (most have <30 listings)
            for n in range(2, 4):
                pages.append(f"{base}page-{n}/")
    else:
        pages = [search_url] + [f"{search_url.rstrip('/')}/page-{n}/" for n in range(2, 10)]
    logger.info("[CENTURY21] Fetching %d pages (IDF: %d cities)",
                len(pages), len(_IDF_CITIES) if "century21.fr" in search_url else 1)
    htmls = await _fetch_pages_curl_cffi(pages)
    listings: list[Listing] = []
    seen: set[str] = set()
    for html in htmls:
        if not html:
            continue
        for m in _re.finditer(r'href="(/trouver_logement/detail/(\d+)/)"', html):
            href, rid = m.group(1), m.group(2)
            if rid in seen:
                continue
            seen.add(rid)
            url = "https://www.century21.fr" + href
            ctx = html[max(0, m.start() - 800): m.end() + 400]
            price = None
            pm = _re.search(r"(\d[\d\s]{1,5})\s*€", ctx)
            if pm:
                digits = "".join(c for c in pm.group(1) if c.isdigit())
                if digits:
                    try: price = int(digits)
                    except ValueError: pass
            surface = None
            sm = _re.search(r"(\d+)(?:[,.]\d+)?\s*m²", ctx)
            if sm:
                try: surface = int(sm.group(1))
                except ValueError: pass
            # Title hint: nearby text
            title_m = _re.search(r"(Appartement|Studio|Maison|Loft)\b[^<]{0,80}", ctx)
            title = title_m.group(0).strip() if title_m else "Appartement"
            # Extract location: look for "(XXXXX)" zipcode in the surrounding ctx,
            # or city slug in the page title. Falls back to "Paris" if nothing.
            zip_m = _re.search(r"\((\d{5})\)", ctx)
            city_in_title_m = _re.search(r"<title>[^<]*?à\s+([\w\-' ]+?)\s*\(\d", html)
            if zip_m:
                location = (city_in_title_m.group(1).strip() + ", " if city_in_title_m else "") + zip_m.group(1)
            else:
                location = city_in_title_m.group(1).strip() if city_in_title_m else "Paris"
            listings.append(Listing(
                lbc_id=f"c21_{rid}",
                title=title[:200],
                description="",
                price=price,
                location=location,
                seller_name="Century 21",
                url=url,
                seller_type_hint="pro",
                source="century21",
                surface=surface,
            ))
            if len(listings) >= max_results:
                break
        if len(listings) >= max_results:
            break
    logger.info("[CENTURY21] Parsed %d listings", len(listings))
    return listings


async def _search_wizi(search_url: str, max_results: int) -> list[Listing]:
    """Wizi.io scraper via discovered public API at app.wizi.eu/api/public/flats/search.
    No auth, paginated by offset. Uses Paris coordinates by default."""
    from curl_cffi.requests import AsyncSession
    from urllib.parse import parse_qs, urlparse

    # Parse lat/lon/city from the SPA hash route
    frag = urlparse(search_url).fragment or urlparse(search_url).query
    if "?" in frag:
        frag = frag.split("?", 1)[1]
    qs = parse_qs(frag)
    lat = (qs.get("lat") or ["48.856614"])[0]
    lon = (qs.get("long") or qs.get("lon") or qs.get("lng") or ["2.3522219"])[0]
    city = (qs.get("city") or ["Paris"])[0]

    headers = {
        "Content-Type": "application/json;charset=UTF-8",
        "Origin": "https://desk.wizi.eu",
        "Referer": "https://desk.wizi.eu/",
        "User-Agent": _HTTPX_UA,
    }

    listings: list[Listing] = []
    offset = 0
    page_size = 20

    try:
        async with AsyncSession(impersonate="chrome120", timeout=20, headers=headers) as session:
            while len(listings) < max_results:
                body = {
                    "furnished": 2, "offset": offset, "logement_type": 0,
                    "surfaceMin": 0, "surfaceMax": 500,
                    "transaction": 2,  # rent only
                    "positions": {"latitude": lat, "longitude": lon},
                }
                r = await session.post(
                    "https://app.wizi.eu/api/public/flats/search",
                    json=body,
                )
                if r.status_code != 200:
                    logger.warning("[WIZI] HTTP %s on offset %d", r.status_code, offset)
                    break
                items = r.json() or []
                if not isinstance(items, list) or not items:
                    break
                for it in items:
                    flat_id = str(it.get("id", ""))
                    if not flat_id:
                        continue
                    price = int(float(it.get("price") or 0)) or None
                    surface = it.get("surface")
                    surface = int(float(surface)) if surface not in (None, "", "0") else None
                    title = (it.get("title") or "").strip() or f"Bien {flat_id}"
                    images = []
                    for doc in it.get("documents") or []:
                        if doc.get("category") == "full_picture":
                            doc_id = doc.get("id")
                            ext = doc.get("extension", "jpeg")
                            if doc_id:
                                images.append(f"https://app.wizi.eu/api/public/documents/{doc_id}.{ext}")
                    pub = (it.get("publish_at") or it.get("created_at") or "").replace(" ", "T") or None
                    # Wizi /search omits postalCode (only /flats/{id} returns
                    # it). Title often includes "Paris 15ème" → derive zip; the
                    # enrich pass below upgrades it to the canonical postalCode
                    # when the detail call succeeds.
                    item_city = it.get("city") or city
                    init_loc = item_city
                    z = _zip_for_location(title) or _zip_for_location(item_city)
                    if z:
                        init_loc = f"{item_city}, {z}"
                    listings.append(Listing(
                        lbc_id=f"wizi_{flat_id}",
                        title=title[:200],
                        description=f"{item_city} - {surface or '?'}m²",
                        price=price,
                        location=init_loc,
                        seller_name="Wizi",
                        url=f"https://desk.wizi.eu/#/app/flat/{flat_id}",
                        seller_type_hint="agency",
                        source="wizi",
                        images=images,
                        surface=surface,
                        published_at=pub,
                    ))
                    if len(listings) >= max_results:
                        break
                if len(items) < page_size:
                    break
                offset += page_size
    except Exception as exc:
        logger.warning("[WIZI] scrape failed: %s", exc)

    # Phase 2: enrich descriptions from per-listing API (curl_cffi, ~80ms each
    # at concurrency=10). The /search endpoint doesn't include description;
    # only /flats/{id} does. Cheap because no HTML parse — pure JSON.
    if listings:
        from curl_cffi.requests import AsyncSession
        sem = asyncio.Semaphore(10)
        async def _enrich(lst):
            flat_id = lst.lbc_id.removeprefix("wizi_")
            if not flat_id.isdigit():
                return
            async with sem:
                try:
                    async with AsyncSession(impersonate="chrome120", timeout=15) as s:
                        r = await s.get(
                            f"https://app.wizi.eu/api/public/flats/{flat_id}",
                            headers={"Origin": "https://desk.wizi.eu",
                                     "Referer": "https://desk.wizi.eu/"},
                        )
                    if r.status_code != 200:
                        return
                    payload = r.json() or {}
                    desc = payload.get("description") or ""
                    if isinstance(desc, str) and len(desc) > 80:
                        lst.description = desc.replace("\r\n", "\n").strip()[:1500]
                    # Authoritative zip — overrides any title-derived guess.
                    pc = str(payload.get("postalCode") or "").strip()
                    if _re.fullmatch(r"\d{5}", pc):
                        det_city = (payload.get("city") or "").strip() or lst.location
                        # Replace existing zip if any; otherwise append.
                        loc_no_zip = _re.sub(r",?\s*\b\d{5}\b\s*", "", lst.location).strip(", ")
                        base = loc_no_zip or det_city
                        lst.location = f"{base}, {pc}"
                except Exception:
                    pass
        await asyncio.gather(*(_enrich(l) for l in listings), return_exceptions=True)

    logger.info("[WIZI] Parsed %d listings", len(listings))
    return listings


# ─── Inli (CDC Habitat — logement intermédiaire) ─────────────────────────────
_INLI_IDF_DEPTS: list[tuple[str, int]] = [
    ("paris", 75), ("seine-et-marne", 77), ("yvelines", 78), ("essonne", 91),
    ("hauts-de-seine", 92), ("seine-saint-denis", 93), ("val-de-marne", 94),
    ("val-d-oise", 95),
]
_INLI_CARD_RE = _re.compile(r'<div class="featured-item">(.*?)</a>\s*</div>', _re.DOTALL)
_INLI_HREF_RE = _re.compile(r'href="(/locations/offre/([a-z0-9-]+)/([A-Z0-9-]+))"')
_INLI_PRICE_RE = _re.compile(
    r'<span class="demi-condensed">([\d\s]+)\s*€\s*</span>\s*<span class="book-condensed">cc</span>'
)
_INLI_DETAILS_RE = _re.compile(
    r'<div class="featured-details"[^>]*>\s*<span>\s*(.*?)\s*</span>', _re.DOTALL,
)
_INLI_IMG_RE = _re.compile(r'<img[^>]+src="([^"]+)"[^>]*class="featured-image[^"]*"')
_INLI_SURFACE_RE = _re.compile(r"(\d+(?:[.,]\d+)?)\s*m[²2]")
_INLI_PIECES_RE = _re.compile(r"(\d+)\s*pi[èe]ces?", _re.IGNORECASE)


def _inli_card_to_listing(card_html: str) -> Optional[Listing]:
    href_m = _INLI_HREF_RE.search(card_html)
    if not href_m:
        return None
    href, slug_city, raw_id = href_m.group(1), href_m.group(2), href_m.group(3)
    url = "https://www.inli.fr" + href
    price_m = _INLI_PRICE_RE.search(card_html)
    if not price_m:
        return None
    try:
        price = int(price_m.group(1).replace(" ", ""))
    except ValueError:
        return None
    details_m = _INLI_DETAILS_RE.search(card_html)
    raw_details = ""
    if details_m:
        raw_details = _re.sub(r"<[^>]+>", " ", details_m.group(1))
        raw_details = _re.sub(r"\s+", " ", raw_details).strip()
    surface: Optional[int] = None
    if (sm := _INLI_SURFACE_RE.search(raw_details)):
        try: surface = int(round(float(sm.group(1).replace(",", "."))))
        except ValueError: pass
    typology = "Studio" if "Studio" in raw_details else None
    if (pm := _INLI_PIECES_RE.search(raw_details)):
        typology = f"{pm.group(1)} pièces"
    after_m2 = _re.split(r"m[²2]\s*", raw_details, maxsplit=1)
    location = (after_m2[1] if len(after_m2) > 1 else raw_details).strip()
    if not location:
        location = slug_city.replace("-", " ").title()
    location = _re.sub(r"\s+", " ", location)
    # Inli card text is "Paris 12eme" or a plain commune name — embed a zip so
    # is_critical_zone() can flag 77/78/91/95 results and dept-filters work.
    if (zip_code := _zip_for_location(location) or _zip_for_location(slug_city)):
        if zip_code not in location:
            location = f"{location}, {zip_code}"
    title_parts = [p for p in (typology, f"{surface} m²" if surface else None, location) if p]
    title = " · ".join(title_parts) or raw_details[:120] or "Logement Inli"
    images = [m.group(1) for m in _INLI_IMG_RE.finditer(card_html)
              if "placeholder" not in m.group(1)]
    return Listing(
        lbc_id=f"inli_{raw_id}", title=title[:200], description=raw_details,
        price=price, location=location[:120], seller_name="in'li", url=url,
        seller_type_hint="bailleur", source="inli", images=images,
        surface=surface, housing_type="appartement",
    )


async def _search_inli(search_url: str, max_results: int) -> list[Listing]:
    """Inli (CDC Habitat — logement intermédiaire IDF). Off-radar source.
    Paginates per-département. ~49 listings IDF ≤1100€ CC."""
    from curl_cffi.requests import AsyncSession
    PRICE_CAP = 1100
    PER_PAGE = 24
    MAX_PAGES = 12
    listings: list[Listing] = []
    seen: set[str] = set()
    async with AsyncSession(impersonate="chrome120", timeout=20) as session:
        async def _fetch(url):
            try:
                r = await session.get(url, allow_redirects=True)
                return r.text if r.status_code == 200 else None
            except Exception:
                return None
        async def _scrape_dept(name, num):
            base = f"https://www.inli.fr/locations/offres/{name}_d:{num}/"
            html1 = await _fetch(base)
            if not html1:
                return []
            cards1 = _INLI_CARD_RE.findall(html1)
            if len(cards1) < PER_PAGE:
                page_htmls = [html1]
            else:
                more = await asyncio.gather(*(_fetch(f"{base}?page={p}") for p in range(2, MAX_PAGES + 1)))
                page_htmls = [html1] + [h for h in more if h]
            out = []
            for h in page_htmls:
                cards = _INLI_CARD_RE.findall(h)
                if not cards: break
                for card in cards:
                    lst = _inli_card_to_listing(card)
                    if not lst or lst.lbc_id in seen: continue
                    seen.add(lst.lbc_id)
                    if lst.price and lst.price <= PRICE_CAP:
                        out.append(lst)
            return out
        per_dept = await asyncio.gather(*(_scrape_dept(n, d) for n, d in _INLI_IDF_DEPTS))
        for batch in per_dept:
            listings.extend(batch)
            if len(listings) >= max_results:
                break
    listings = listings[:max_results]
    logger.info("[INLI] Parsed %d listings ≤%d€ CC across IDF", len(listings), PRICE_CAP)
    return listings


# ─── Gens de Confiance (P2P trust network) ───────────────────────────────────
async def _search_gensdeconfiance(search_url: str, max_results: int) -> list[Listing]:
    """Off-radar P2P trust network. Server-renders 30 listings/page in a
    React-on-Rails JSON blob. Filter to IDF zip prefixes."""
    import json as _json
    base = search_url.rstrip("/")
    sep = "&" if "?" in base else "?"
    pages = [base] + [f"{base}{sep}page={n}" for n in range(2, 31)]
    htmls = await _fetch_pages_curl_cffi(pages)

    listings: list[Listing] = []
    seen: set[str] = set()
    idf_prefixes = ("75", "77", "78", "91", "92", "93", "94", "95")

    for html in htmls:
        if not html:
            continue
        m = _re.search(
            r'data-component-name="Search"[^>]*>(\{.*?\})</script>',
            html, _re.DOTALL,
        )
        if not m:
            continue
        try:
            blob = _json.loads(m.group(1))
        except _json.JSONDecodeError:
            continue
        pr = blob.get("preloadedResults")
        if isinstance(pr, str):
            try:
                pr = _json.loads(pr)
            except _json.JSONDecodeError:
                continue
        if not isinstance(pr, list):
            continue
        for r in pr:
            if r.get("category") != "realestate__rent":
                continue
            rid = str(r.get("id") or "")
            if not rid or rid in seen:
                continue
            zip_ = str(r.get("zip") or "")
            if zip_[:2] not in idf_prefixes:
                continue
            seen.add(rid)
            slug = r.get("slug") or ""
            attrs = r.get("attributes") or {}
            try:
                surface = int(attrs.get("nbSquareMeters")) if attrs.get("nbSquareMeters") else None
            except (TypeError, ValueError):
                surface = None
            try:
                price = int(r.get("price") or 0) or None
            except (TypeError, ValueError):
                price = None
            parts = []
            if attrs.get("nbPieces"): parts.append(f"{attrs['nbPieces']} pièce(s)")
            if surface: parts.append(f"{surface} m²")
            if attrs.get("furnished") is True or attrs.get("rentalFurnishings") == "furnished":
                parts.append("meublé")
            if attrs.get("propertyFloor") is not None: parts.append(f"étage {attrs['propertyFloor']}")
            if attrs.get("dpe"): parts.append(f"DPE {attrs['dpe']}")
            if r.get("rentalCharge"): parts.append(f"charges {r['rentalCharge']}€")
            equipments = attrs.get("equipments") or []
            if equipments: parts.append(", ".join(str(e) for e in equipments[:5]))
            description = (r.get("title") or "") + ". " + " · ".join(parts)
            url = f"https://www.gensdeconfiance.com/fr/annonce/{slug}" if slug else "https://www.gensdeconfiance.com/fr/recherche"
            pub = None
            if r.get("displayDate"):
                try:
                    from datetime import datetime as _dt2, timezone as _tz
                    pub = _dt2.fromtimestamp(int(r["displayDate"]), tz=_tz.utc).isoformat()
                except (TypeError, ValueError, OSError):
                    pub = None
            images = []
            if r.get("imageUrl"):
                images.append(r["imageUrl"])
            seller_hint = "particulier" if not r.get("pro") else "pro"
            listings.append(Listing(
                lbc_id=f"gdc_{rid}", title=(r.get("title") or "")[:200],
                description=description[:1500], price=price,
                location=f"{r.get('city') or ''} {zip_}".strip(),
                seller_name="Particulier" if not r.get("pro") else "Gens de Confiance",
                url=url, seller_type_hint=seller_hint, source="gensdeconfiance",
                images=images, surface=surface, published_at=pub,
            ))
            if len(listings) >= max_results:
                break
        if len(listings) >= max_results:
            break
    logger.info("[GENSDECONFIANCE] Parsed %d IDF rentals", len(listings))
    return listings


def _is_gensdeconfiance(url: str) -> bool:
    return "gensdeconfiance" in url


def _is_cdc_habitat(url: str) -> bool:
    return "cdc-habitat.fr" in url


def _is_fnaim(url: str) -> bool:
    return "fnaim.fr" in url


# ─── FNAIM (Fédération Nationale de l'Immobilier — 12k agences) ──────────────
_FNAIM_IDF_DEPT_SLUGS: list[str] = [
    "paris-75", "seine-et-marne-77", "yvelines-78", "essonne-91",
    "hauts-de-seine-92", "seine-saint-denis-93", "val-de-marne-94", "val-d-oise-95",
]
_FNAIM_PAGE_LIMIT = 9
_FNAIM_INTER_PAGE_DELAY = 0.6
_FNAIM_CARD_RE = _re.compile(r'<li class="item"><div class="itemInfo"[^>]*>(.*?)</li>', _re.DOTALL)
_FNAIM_HREF_RE = _re.compile(r'href="(/annonce-immobiliere/(\d+)/[^"]+\.htm)"')
_FNAIM_TITLE_RE = _re.compile(r'data-title="([^"]+)"')
_FNAIM_PRICE_RE = _re.compile(r'<p class="price">\s*([\d ]+)\s*&euro;')
_FNAIM_LOC_RE = _re.compile(r'<p class="picto lieu clear">\s*<a[^>]*>(.*?)</a>', _re.DOTALL)
_FNAIM_DESC_RE = _re.compile(r'<p class="description">\s*(.*?)\s*</p>', _re.DOTALL)
_FNAIM_AGENCY_RE = _re.compile(r'<div class="nom">.*?<b>([^<]+)</b>', _re.DOTALL)
_FNAIM_IMG_RE = _re.compile(r'<img[^>]*src="(https://imagesv2\.fnaim\.fr/[^"]+)"')
_FNAIM_SURFACE_RE = _re.compile(r"(\d+)\s*m[²2]")
_FNAIM_PIECES_RE = _re.compile(r"(\d+)\s*pi[èe]ces?", _re.IGNORECASE)


def _fnaim_card_to_listing(card_html: str) -> Optional[Listing]:
    import html as _html
    href_m = _FNAIM_HREF_RE.search(card_html)
    if not href_m: return None
    url_path, ad_id = href_m.group(1), href_m.group(2)
    price_m = _FNAIM_PRICE_RE.search(card_html)
    if not price_m: return None
    try: price = int(price_m.group(1).replace(" ", ""))
    except ValueError: return None
    title_m = _FNAIM_TITLE_RE.search(card_html)
    title = _html.unescape(title_m.group(1)).strip() if title_m else f"Annonce {ad_id}"
    loc_m = _FNAIM_LOC_RE.search(card_html)
    cp = ""
    city = ""
    location = ""
    if loc_m:
        raw = _re.sub(r"<[^>]+>", "|", loc_m.group(1))
        raw = _html.unescape(raw)
        parts = [p.strip() for p in raw.split("|") if p.strip()]
        if parts and parts[0].isdigit():
            cp = parts[0]
            city = parts[1].title() if len(parts) > 1 else ""
        else:
            city = parts[0].title() if parts else ""
        if city.lower().startswith("paris ") and cp.startswith("75") and len(cp) == 5:
            location = f"Paris {cp[-2:]}, {cp}"
        else:
            location = f"{city}, {cp}" if cp else city
    desc_m = _FNAIM_DESC_RE.search(card_html)
    description = ""
    if desc_m:
        desc_raw = _re.sub(r"<[^>]+>", " ", desc_m.group(1))
        desc_raw = _html.unescape(desc_raw)
        description = _re.sub(r"\s+", " ", desc_raw).strip()[:1500]
    agency_m = _FNAIM_AGENCY_RE.search(card_html)
    agency = _html.unescape(agency_m.group(1)).strip() if agency_m else "Agence FNAIM"
    images = []
    seen_imgs = set()
    for m in _FNAIM_IMG_RE.finditer(card_html):
        u = m.group(1)
        if u not in seen_imgs:
            seen_imgs.add(u)
            images.append(u)
    surface = None
    sm = _FNAIM_SURFACE_RE.search(title)
    if sm:
        try: surface = int(sm.group(1))
        except ValueError: pass
    housing_type = ""
    pm = _FNAIM_PIECES_RE.search(title)
    if pm:
        n = int(pm.group(1))
        housing_type = "studio" if n == 1 else f"T{min(n, 5)}" if n <= 5 else "T5+"
    return Listing(
        lbc_id=f"fnaim_{ad_id}", title=title[:240], description=description or title,
        price=price, location=(location or "Île-de-France")[:120],
        seller_name=agency[:120], url="https://www.fnaim.fr" + url_path,
        seller_type_hint="agence", source="fnaim", images=images,
        surface=surface, housing_type=housing_type,
    )


async def _search_fnaim(search_url: str, max_results: int) -> list[Listing]:
    """FNAIM — federated portal of 12k independent agencies. Off-radar.
    Sweeps 8 IDF départements in parallel sessions (server caps at 9 pages/query).
    ~1100 IDF listings ≤1100€."""
    from curl_cffi.requests import AsyncSession
    PRICE_CAP = 1100
    BASE = "https://www.fnaim.fr"
    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
    }
    listings = []
    seen = set()
    lock = asyncio.Lock()
    page2_link_re_tpl = (r'href="(/liste-annonces-immobilieres/'
                        r'18-location-appartement-{slug}-page-2\.htm[^"]+)"')

    async def _scrape_dept(slug):
        out = []
        base_url = f"{BASE}/liste-annonces-immobilieres/18-location-appartement-{slug}.htm"
        link_pattern = _re.compile(page2_link_re_tpl.format(slug=_re.escape(slug)))
        try:
            async with AsyncSession(impersonate="chrome120", timeout=25,
                                    max_redirects=5, headers=headers) as sess:
                r1 = await sess.get(base_url)
                if r1.status_code != 200 or len(r1.text) < 250_000:
                    logger.warning("[FNAIM] %s warm-up bad: %d / %d bytes", slug, r1.status_code, len(r1.text))
                    return out
                pages_html = [r1.text]
                tpl_m = link_pattern.search(r1.text)
                if tpl_m:
                    tpl = tpl_m.group(1).replace("&amp;", "&")
                    for p in range(2, _FNAIM_PAGE_LIMIT + 1):
                        url_path = _re.sub(r"-page-2\.htm", f"-page-{p}.htm", tpl)
                        url_path = _re.sub(r"([?&])ip=2(?=[&]|$)", rf"\g<1>ip={p}", url_path)
                        try:
                            r = await sess.get(BASE + url_path, headers={"Referer": base_url})
                        except Exception:
                            break
                        if r.status_code != 200 or len(r.text) < 250_000:
                            break
                        pages_html.append(r.text)
                        await asyncio.sleep(_FNAIM_INTER_PAGE_DELAY)
                for html in pages_html:
                    for card in _FNAIM_CARD_RE.findall(html):
                        lst = _fnaim_card_to_listing(card)
                        if not lst: continue
                        async with lock:
                            if lst.lbc_id in seen: continue
                            seen.add(lst.lbc_id)
                        if lst.price is not None and lst.price <= PRICE_CAP:
                            out.append(lst)
        except Exception as exc:
            logger.warning("[FNAIM] %s scrape failed: %s", slug, exc)
        return out

    per_dept = await asyncio.gather(*(_scrape_dept(slug) for slug in _FNAIM_IDF_DEPT_SLUGS))
    # Round-robin interleave so every département is represented even when
    # max_results is small. Iterating linearly used to drop 92-95 entirely
    # because 75+77 alone fill the quota.
    from itertools import zip_longest
    for tup in zip_longest(*per_dept):
        for lst in tup:
            if lst is None: continue
            listings.append(lst)
            if len(listings) >= max_results: break
        if len(listings) >= max_results: break
    listings = listings[:max_results]
    per_dept_counts = [len(b) for b in per_dept]
    logger.info("[FNAIM] Parsed %d listings ≤%d€/mois IDF (per-dept raw: %s)",
                len(listings), PRICE_CAP,
                dict(zip(_FNAIM_IDF_DEPT_SLUGS, per_dept_counts)))
    return listings


# ─── CDC Habitat (cdc-habitat.fr) ────────────────────────────────────────────
_CDC_HREF_RE = _re.compile(
    r'href="(https?://www\.cdc-habitat\.fr/annonces-immobilieres/location/'
    r'([^/"]+)/([^/"]+)/([^/"]+)/(\d+))"'
)
_CDC_PRICE_RE = _re.compile(r'<div class="price[^"]*">\s*([\d\s\., ]+)\s*€')
_CDC_LOC_RE = _re.compile(r'<div class="location[^"]*">\s*([^<]+?)\s*\(([0-9A-Z]+)\)\s*</div>')
_CDC_TYPE_RE = _re.compile(r'<div class="type[^"]*">\s*([^<]+?)\s*</div>')
_CDC_NOTES_RE = _re.compile(r'<div class="notes[^"]*">\s*([^<]+?)\s*</div>')
_CDC_H3_RE = _re.compile(r'<h3[^>]*>([\s\S]+?)</h3>')
_CDC_IMG_RE = _re.compile(r'<img\s+src="(https://referentiel-photos\.cdc-habitat\.fr/[^"]+)"')
_CDC_TIP_RE = _re.compile(r'tooltipBubble[\s\S]{0,400}?<p>([^<]+)</p>', _re.IGNORECASE)
_CDC_BANNER_RE = _re.compile(r'<div class="banner-band[^"]*">\s*([^<]+?)\s*</div>')
_CDC_SURFACE_RE = _re.compile(r"(\d+(?:[.,]\d+)?)\s*m[²2]")
_CDC_PIECES_RE = _re.compile(r"(\d+)\s*pi[èe]ces?", _re.IGNORECASE)
_CDC_PAGES_RE = _re.compile(r"/page-(\d+)")


def _cdc_category(tooltip: str) -> str:
    if not tooltip: return ""
    t = tooltip.lower()
    if "intermédiaire" in t or "intermediaire" in t: return "intermediaire"
    if "loyer libre" in t: return "libre"
    if "social" in t or ("plafonds" in t and "ressources" in t): return "social"
    return ""


def _cdc_card_to_listing(card_html: str) -> Optional[Listing]:
    href_m = _CDC_HREF_RE.search(card_html)
    if not href_m: return None
    full_url, _region, _dept_slug, ville_slug, raw_id = href_m.groups()
    price_m = _CDC_PRICE_RE.search(card_html)
    if not price_m: return None
    raw_price = price_m.group(1).replace(" ", "").replace(" ", "").replace(".", "").replace(",", ".")
    try: price = int(round(float(raw_price)))
    except ValueError: return None

    h3_m = _CDC_H3_RE.search(card_html)
    h3_clean = _re.sub(r"\s+", " ", _re.sub(r"<[^>]+>", " ", h3_m.group(1))).strip() if h3_m else ""
    typ_m = _CDC_TYPE_RE.search(card_html)
    housing_type = typ_m.group(1).strip() if typ_m else ""
    notes_m = _CDC_NOTES_RE.search(card_html)
    notes = notes_m.group(1).strip() if notes_m else ""
    loc_m = _CDC_LOC_RE.search(card_html)
    if loc_m:
        city = loc_m.group(1).strip().title()
        zip_ = loc_m.group(2).strip()
        location = f"{city} ({zip_})"
    else:
        location = ville_slug.replace("-", " ").title()

    surface = None
    if (sm := _CDC_SURFACE_RE.search(h3_clean)):
        try: surface = int(round(float(sm.group(1).replace(",", "."))))
        except ValueError: pass
    typology = None
    if (pm := _CDC_PIECES_RE.search(h3_clean)): typology = f"{pm.group(1)} pièces"
    elif "studio" in h3_clean.lower(): typology = "Studio"
    tip_m = _CDC_TIP_RE.search(card_html)
    category = _cdc_category(tip_m.group(1) if tip_m else "")
    banners = [b.strip() for b in _CDC_BANNER_RE.findall(card_html)]
    seen_img = set()
    images = []
    for m in _CDC_IMG_RE.finditer(card_html):
        u = m.group(1)
        if u not in seen_img:
            seen_img.add(u)
            images.append(u)

    title_parts = [p for p in (housing_type or None, typology, f"{surface} m²" if surface else None, location) if p]
    title = " · ".join(title_parts) or h3_clean[:120] or "Logement CDC Habitat"
    description_bits = [housing_type, typology, f"{surface} m²" if surface else "", notes, location] + banners
    description = " · ".join([d for d in description_bits if d]).strip()
    hint = "bailleur" if not category else f"bailleur:{category}"
    housing_norm = "appartement" if housing_type.lower().startswith("appart") else (
        "maison" if "maison" in housing_type.lower() else housing_type.lower())
    return Listing(
        lbc_id=f"cdc_{raw_id}", title=title[:200], description=description[:500],
        price=price, location=location[:120], seller_name="CDC Habitat",
        url=full_url, seller_type_hint=hint, source="cdc_habitat",
        images=images, surface=surface, housing_type=housing_norm,
    )


def _cdc_split_articles(html: str) -> list[str]:
    """Split into residenceCard articles, dropping JS template literals."""
    out = []
    parts = html.split('<article class="residenceCard"')
    for part in parts[1:]:
        end = part.find("</article>")
        if end == -1: continue
        body = part[:end]
        if "$(content).html()" in body:  # JS template, not real card
            continue
        out.append(body)
    return out


async def _search_cdc_habitat(search_url: str, max_results: int) -> list[Listing]:
    """CDC Habitat — public sister of Inli. ~44 IDF listings ≤1100€,
    62% intermediate (sweet spot alternant SNCF). Server-rendered, no anti-bot."""
    from curl_cffi.requests import AsyncSession
    PRICE_CAP = 1100
    MAX_PAGES = 30
    base = search_url.rstrip("/")
    listings = []
    seen = set()
    async with AsyncSession(impersonate="chrome120", timeout=25) as session:
        async def _fetch(url):
            try:
                r = await session.get(url, allow_redirects=True)
                if r.status_code != 200: return None
                if "/page-" in url and "/page-" not in str(r.url): return None
                return r.text
            except Exception:
                return None
        first = await _fetch(base)
        if not first:
            logger.warning("[CDC] page 1 fetch failed")
            return []
        page_nums = [int(n) for n in _CDC_PAGES_RE.findall(first)] or [1]
        last_page = min(max(max(page_nums), 1), MAX_PAGES)
        page_htmls = [first]
        if last_page >= 2:
            extra = await asyncio.gather(*(_fetch(f"{base}/page-{p}") for p in range(2, last_page + 1)))
            page_htmls.extend(extra)
        for html in page_htmls:
            if not html: continue
            for card in _cdc_split_articles(html):
                lst = _cdc_card_to_listing(card)
                if not lst or lst.lbc_id in seen: continue
                seen.add(lst.lbc_id)
                if lst.price and lst.price <= PRICE_CAP:
                    listings.append(lst)
                    if len(listings) >= max_results: break
            if len(listings) >= max_results: break
    listings = listings[:max_results]
    logger.info("[CDC] Parsed %d listings <=%d€ CC across IDF", len(listings), PRICE_CAP)
    return listings


async def _search_roomlala_with_playwright(search_url: str, max_results: int) -> list[Listing]:
    return await _search_via_generic(
        search_url, max_results,
        site="roomlala",
        base_url="https://www.roomlala.com",
        source="roomlala",
        prefix="rl",
        card_selectors=[r"property-card|room-card|listing-card", r"card|listing"],
    )


# ─── Laforêt (laforet.com) ─────────────────────────────────────────────────
#
# Laforêt's search HTML is rendered server-side (Symfony) but every result
# card carries `data-gtm-item-*` attributes used by their analytics layer:
# id, name, price, size, rooms-nb, city, zipcode, criteria. That gives us
# everything the spec asks for *without* hydrating the Live Components.
#
# Strategy:
#   1. Static fetch via curl_cffi (fast path) — works in probes (HTTP 200,
#      40 cards/page with full data attributes).
#   2. If 0 cards (e.g. WAF challenge), fall back to Playwright + stealth
#      with a 5-10s wait so any Live Component XHRs settle. Same parser.
#
# Search URL: /ville/location-appartement-paris-75000?filter[max]=1100
# Pagination: &page=N (40 cards per page).

_LAFORET_CARD_FIELDS = {
    "name":     _re.compile(r'data-gtm-item-name-param="([^"]+)"'),
    "city":     _re.compile(r'data-gtm-item-city-param="([^"]+)"'),
    "zip":      _re.compile(r'data-gtm-item-zipcode-param="([^"]+)"'),
    "price":    _re.compile(r'data-gtm-item-price-param="([^"]+)"'),
    "size":     _re.compile(r'data-gtm-item-size-param="([^"]+)"'),
    "rooms":    _re.compile(r'data-gtm-item-rooms-nb-param="([^"]+)"'),
    "criteria": _re.compile(r'data-gtm-item-criteria-param="([^"]+)"'),
}
_LAFORET_ID_RE = _re.compile(r'data-gtm-item-id-param="(\d+)"')


def _laforet_parse_html(html: str, max_results: int) -> list[Listing]:
    """Pull listings from a Laforêt search page using GTM data attributes."""
    listings: list[Listing] = []
    seen: set[str] = set()
    for m in _LAFORET_ID_RE.finditer(html):
        rid = m.group(1)
        if rid in seen:
            continue
        seen.add(rid)
        # Window covering the card's GTM block (attributes sit within ~2KB)
        chunk = html[max(0, m.start() - 200): m.start() + 2500]
        fields = {k: (r.search(chunk).group(1) if r.search(chunk) else "")
                  for k, r in _LAFORET_CARD_FIELDS.items()}
        href_m = _re.search(
            r'href="(https://www\.laforet\.com/agence-immobiliere/[^"]*-' + rid + r')"',
            chunk,
        )
        url = href_m.group(1) if href_m else f"https://www.laforet.com/?id={rid}"

        # Parse price (may carry decimals, e.g. "961.01")
        price = None
        if fields["price"]:
            try: price = int(float(fields["price"]))
            except ValueError: pass

        # Surface (e.g. "58.18")
        surface = None
        if fields["size"]:
            try: surface = int(float(fields["size"]))
            except ValueError: pass

        zipcode = fields["zip"]
        city    = (fields["city"] or "").title() or "Paris"
        # Always embed the 5-digit zip when present (was Paris-only before),
        # so non-75 listings stay visible to is_critical_zone() and
        # dashboard dept-prefix filters.
        if zipcode and len(zipcode) == 5:
            if zipcode.startswith("75"):
                location = f"{city} {zipcode[-2:]}, {zipcode}"
            else:
                location = f"{city}, {zipcode}"
        else:
            # Fallback: try the city table (rare — Laforêt usually fills zip).
            z = _zip_for_location(city)
            location = f"{city}, {z}" if z else city

        # Title — prefer the GTM item-name; enrich with criteria for clarity
        title = fields["name"] or f"Appartement {fields['rooms']} pièces"
        if fields["criteria"]:
            title = f"{title} — {fields['criteria']}"

        listings.append(Listing(
            lbc_id=f"lf_{rid}",
            title=title[:240],
            description=fields["criteria"] or "",
            price=price,
            location=location,
            seller_name="Laforêt",
            url=url,
            seller_type_hint="pro",
            source="laforet",
            surface=surface,
        ))
        if len(listings) >= max_results:
            break
    return listings


async def _search_laforet_with_playwright(search_url: str, max_results: int) -> list[Listing]:
    """Scrape Laforêt rentals.

    `search_url` is normalised to the Paris-rentals page if a generic URL
    is passed. Static HTML carries 40 cards/page with all needed fields
    in `data-gtm-item-*` attributes — no Live Component hydration needed
    in the common case. Falls back to Playwright + stealth if the static
    response is empty (WAF challenge).

    Important Laforêt quirk: passing `filter[max]=N` makes the backend
    ignore the city slug and return France-wide results. So we apply the
    1100 € budget cap client-side and use the unfiltered city URL.
    """
    from bs4 import BeautifulSoup  # noqa: F401 — kept for parity w/ siblings

    MAX_PRICE = 1100  # spec target

    # Default to Paris if a non-Laforêt URL is passed
    if not search_url or "laforet.com" not in search_url:
        search_url = "https://www.laforet.com/ville/location-appartement-paris-75000"

    # Strip any filter[max]/filter[min] from the URL so the city slug stays scoped
    search_url = _re.sub(r"[?&]filter%5B(?:max|min)%5D=\d+", "", search_url)
    search_url = _re.sub(r"[?&]filter\[(?:max|min)\]=\d+", "", search_url)
    search_url = search_url.rstrip("?&")

    sep = "&" if "?" in search_url else "?"
    pages = [search_url] + [f"{search_url}{sep}page={n}" for n in range(2, 6)]

    listings: list[Listing] = []
    seen: set[str] = set()

    # Fast path: curl_cffi static fetch
    htmls = await _fetch_pages_curl_cffi(pages)
    for html in htmls:
        if not html:
            continue
        for l in _laforet_parse_html(html, 1000):  # parse all, filter below
            if l.lbc_id in seen:
                continue
            seen.add(l.lbc_id)
            if l.price is not None and l.price > MAX_PRICE:
                continue
            listings.append(l)
            if len(listings) >= max_results:
                break
        if len(listings) >= max_results:
            break

    if listings:
        logger.info("[LAFORET] Parsed %d listings via static HTML (≤%d€)",
                    len(listings), MAX_PRICE)
        await _laforet_enrich_descriptions(listings)
        return listings

    # Fallback: Playwright + stealth (handles WAF / Live Component edge cases)
    logger.info("[LAFORET] Static fetch yielded 0 — falling back to Playwright")
    try:
        async with async_playwright() as pw:
            ctx = await pw.chromium.launch_persistent_context(
                user_data_dir=_user_data_dir("laforet"),
                headless=True,
                args=["--disable-blink-features=AutomationControlled", "--window-size=1280,800"],
                user_agent=_HTTPX_UA,
                viewport={"width": 1280, "height": 800},
                locale="fr-FR",
            )
            page = await ctx.new_page()
            await Stealth().apply_stealth_async(page)
            try:
                for url in pages[:3]:  # 3 pages max via PW (heavier)
                    await page.goto(url, wait_until="domcontentloaded", timeout=30_000)
                    await _handle_cookie_banner(page)
                    # Give Live Components 5-10s to hydrate (per spec)
                    await asyncio.sleep(random.uniform(5.0, 8.0))
                    html = await page.content()
                    for l in _laforet_parse_html(html, 1000):
                        if l.lbc_id in seen:
                            continue
                        seen.add(l.lbc_id)
                        if l.price is not None and l.price > MAX_PRICE:
                            continue
                        listings.append(l)
                    if len(listings) >= max_results:
                        break
            finally:
                await ctx.close()
    except Exception as exc:
        logger.warning("[LAFORET] Playwright fallback failed: %s", exc)

    logger.info("[LAFORET] Parsed %d listings (Playwright fallback)", len(listings))
    await _laforet_enrich_descriptions(listings)
    return listings


async def _laforet_enrich_descriptions(listings: list[Listing]) -> None:
    """Fetch each Laforêt detail page and replace the synthesized description
    with the full marketing copy (typically 500-1400 chars). curl_cffi only
    — no Camoufox needed. ~50ms/listing at concurrency=10."""
    if not listings:
        return
    import html as _htmllib
    from curl_cffi.requests import AsyncSession

    sec_re = _re.compile(r'id="section-description"(.*?)</section>', _re.S)
    prose_re = _re.compile(r'<div\s+class="prose"\s*>(.*?)</div>', _re.S)
    tag_re = _re.compile(r'<[^>]+>')

    sem = asyncio.Semaphore(10)
    async def _enrich(lst, session):
        async with sem:
            try:
                r = await session.get(lst.url, impersonate="chrome120", timeout=15)
                if r.status_code != 200:
                    return
                section = sec_re.search(r.text)
                if not section:
                    return
                prose = prose_re.search(section.group(1))
                if not prose:
                    return
                raw = prose.group(1).replace("<br />", "\n").replace("<br/>", "\n").replace("<br>", "\n")
                txt = tag_re.sub("", raw)
                txt = _htmllib.unescape(txt)
                txt = _re.sub(r"[ \t]+", " ", txt)
                txt = _re.sub(r"\n{3,}", "\n\n", txt).strip()
                if len(txt) > 80:
                    lst.description = txt[:1500]
                # Agency phone: <a href="tel:0155269393"> in every Laforêt detail page
                if (ph_m := _re.search(r'href="tel:(0\d{9})"', r.text)):
                    lst.phone = ph_m.group(1)
            except Exception:
                pass

    async with AsyncSession() as session:
        await asyncio.gather(*(_enrich(l, session) for l in listings), return_exceptions=True)


# ─── Guy Hoquet ──────────────────────────────────────────────────────────────
#
# Architecture: SPA-ish search page that hydrates listings via XHR to
#   GET /biens/result?templates[]=properties&p=N&filters[10][]=2
#                    &filters[20][]=<city-slug>&filters[40][]=<max-price>
# returning JSON {success, total, templates: {properties: <html-fragment>}}.
# Filter slot IDs (discovered via the search form's hidden inputs):
#   10 → type_transaction (1=Acheter, 2=Louer, 3=Loc.saisonnière, 5=Meublée)
#   20 → locations[] (slug from /biens/search-localization?q=…, e.g. paris-75056_c4)
#   40 → price_max
#
# The endpoint is fully cookieless when called with proper headers, so we use
# curl_cffi + chrome120 fingerprint (10× faster than Playwright). Playwright
# is the fallback when Cloudflare returns a challenge.

_GH_PARIS_SLUG = "paris-75056_c4"  # from /biens/search-localization?q=paris (id=41)
# 2026-05-05 — IDF region slug (location_type=1, region_code=11). Verified live
# against /biens/search-localization?q=ile-de-france. Returns mixed-IDF listings
# (92, 77, 91, 93, 94, 78, 95).
_GH_IDF_SLUG = "11_c1"
_GH_ITEM_RE = _re.compile(
    r'<div\s+class="[^"]*resultat-item[^"]*"\s+data-id="(\d+)">(.*?)</div>\s*</a>\s*</div>',
    _re.DOTALL,
)
_GH_HREF_RE = _re.compile(r'<a\s+href="(https://www\.guy-hoquet\.com/(?:location|achat-vente)/[^"]+)"')
_GH_NAME_RE = _re.compile(r'<span class="ttl property-name">\s*([^<\n]+?)\s*(?:<|$)', _re.DOTALL)
_GH_SURFACE_RE = _re.compile(r'(\d+(?:[\.,]\d+)?)\s*m(?:²|2|&sup2;)')
_GH_PRICE_RE = _re.compile(r'<div class="price">\s*([^<]+?)\s*</div>', _re.DOTALL)
_GH_CITY_RE = _re.compile(r'<div class="text-truncate"[^>]*title="([^"]+)"')
_GH_DESC_RE = _re.compile(r'<div class="description">\s*([^<]+?)\s*</div>', _re.DOTALL)
_GH_ALT_RE = _re.compile(r'<img[^>]+\balt="([^"]+)"')


def _gh_clean(s: str) -> str:
    """Collapse whitespace and decode the few HTML entities the API emits."""
    s = (s or "").replace("&nbsp;", " ").replace("&amp;", "&").replace("&#039;", "'")
    return _re.sub(r"\s+", " ", s).strip()


def _gh_parse_html(html_fragment: str, max_results: int) -> list[Listing]:
    """Parse the `templates.properties` HTML fragment into Listing objects."""
    listings: list[Listing] = []
    seen: set[str] = set()
    for m in _GH_ITEM_RE.finditer(html_fragment):
        rid = m.group(1)
        if rid in seen:
            continue
        seen.add(rid)
        chunk = m.group(2)

        href_m = _GH_HREF_RE.search(chunk)
        url = href_m.group(1) if href_m else f"https://www.guy-hoquet.com/?id={rid}"

        name = _gh_clean(_GH_NAME_RE.search(chunk).group(1)) if _GH_NAME_RE.search(chunk) else ""
        # The .property-name span contains both the type AND the surface ("Local commercial 21 m²")
        surf_m = _GH_SURFACE_RE.search(name) or _GH_SURFACE_RE.search(chunk)
        surface = None
        if surf_m:
            try: surface = int(float(surf_m.group(1).replace(",", ".")))
            except ValueError: pass

        price = None
        price_m = _GH_PRICE_RE.search(chunk)
        if price_m:
            digits = _re.sub(r"[^\d]", "", price_m.group(1))
            if digits:
                try: price = int(digits)
                except ValueError: pass

        city = _gh_clean(_GH_CITY_RE.search(chunk).group(1)) if _GH_CITY_RE.search(chunk) else "Paris"
        desc = _gh_clean(_GH_DESC_RE.search(chunk).group(1)) if _GH_DESC_RE.search(chunk) else ""

        # Title: prefer image alt (richer, e.g. "PARIS 13e - LOCAL COMMERCIAL 21 m² - EXCLUSIVITÉ"),
        # fall back to .property-name
        alt_m = _GH_ALT_RE.search(chunk)
        title = _gh_clean(alt_m.group(1)) if alt_m else name

        listings.append(Listing(
            lbc_id=f"gh_{rid}",
            title=(title or "Annonce Guy Hoquet")[:240],
            description=desc,
            price=price,
            location=city,
            seller_name="Guy Hoquet",
            url=url,
            seller_type_hint="pro",
            source="guyhoquet",
            surface=surface,
        ))
        if len(listings) >= max_results:
            break
    return listings


def _gh_build_api_url(page: int, *, location_slug: str = _GH_IDF_SLUG,
                     transaction: int = 2, price_max: int = 1100) -> str:
    """Build the JSON XHR URL the front-end calls when filters are applied."""
    return (
        "https://www.guy-hoquet.com/biens/result?"
        "templates%5B%5D=properties"
        f"&p={page}"
        f"&filters%5B10%5D%5B%5D={transaction}"
        f"&filters%5B20%5D%5B%5D={location_slug}"
        f"&filters%5B40%5D%5B%5D={price_max}"
    )


async def _gh_fetch_pages_curl_cffi(urls: list[str], timeout: int = 15) -> list[Optional[str]]:
    """Fetch the JSON XHRs and return the inner properties HTML for each.

    The endpoint requires X-Requested-With:XMLHttpRequest (otherwise it
    returns the full SPA shell HTML, not the JSON we want).
    """
    from curl_cffi.requests import AsyncSession
    headers = {
        "User-Agent": _HTTPX_UA,
        "Accept": "application/json,text/javascript,*/*;q=0.01",
        "X-Requested-With": "XMLHttpRequest",
        "Referer": "https://www.guy-hoquet.com/biens/result",
    }
    async with AsyncSession(impersonate="chrome120", timeout=timeout, headers=headers) as session:
        async def _one(url: str) -> Optional[str]:
            try:
                r = await session.get(url, allow_redirects=True)
                if r.status_code != 200:
                    return None
                data = r.json()
                if not data.get("success"):
                    return None
                return data.get("templates", {}).get("properties", "") or None
            except Exception as exc:
                logger.warning("[GUYHOQUET] curl_cffi fetch failed for %s: %s", url[:120], exc)
                return None
        return await asyncio.gather(*(_one(u) for u in urls))


async def _search_guyhoquet_with_playwright(search_url: str, max_results: int) -> list[Listing]:
    """Scrape Guy Hoquet rentals (Paris, ≤1100€ by default).

    `search_url` is best-effort: any Guy Hoquet URL is accepted but filters are
    re-applied via the JSON endpoint regardless (the public URLs don't carry
    filter state — it's session-bound). To target Paris rentals ≤1100€ pass
    any guy-hoquet.com URL or the canonical /annonces/location/paris/.

    Fast path: curl_cffi + chrome120 fingerprint hits /biens/result with the
    filter slots already encoded — no browser, no cookies, ~1s/page.
    Fallback: Playwright + stealth replays the same XHR through a real
    browser context if Cloudflare ever blocks the fast path.
    """
    MAX_PRICE = 1100  # spec target

    # We always paginate against the JSON endpoint regardless of input URL.
    pages = [_gh_build_api_url(p, price_max=MAX_PRICE) for p in range(1, 6)]

    listings: list[Listing] = []
    seen: set[str] = set()

    # Fast path: curl_cffi
    fragments = await _gh_fetch_pages_curl_cffi(pages)
    for frag in fragments:
        if not frag:
            continue
        for l in _gh_parse_html(frag, 1000):
            if l.lbc_id in seen:
                continue
            seen.add(l.lbc_id)
            if l.price is not None and l.price > MAX_PRICE:
                continue
            listings.append(l)
            if len(listings) >= max_results:
                break
        if len(listings) >= max_results:
            break

    if listings:
        logger.info("[GUYHOQUET] Parsed %d listings via JSON XHR (≤%d€)",
                    len(listings), MAX_PRICE)
        return listings

    # Fallback: Playwright + stealth
    logger.info("[GUYHOQUET] Fast path empty — falling back to Playwright")
    try:
        async with async_playwright() as pw:
            ctx = await pw.chromium.launch_persistent_context(
                user_data_dir=_user_data_dir("guyhoquet"),
                headless=True,
                args=["--disable-blink-features=AutomationControlled",
                      "--window-size=1280,800"],
                user_agent=_HTTPX_UA,
                viewport={"width": 1280, "height": 800},
                locale="fr-FR",
            )
            page = await ctx.new_page()
            await Stealth().apply_stealth_async(page)
            try:
                # Warm up: load the search page to set Laravel session cookies.
                await page.goto("https://www.guy-hoquet.com/biens/result",
                                wait_until="networkidle", timeout=45_000)
                await asyncio.sleep(1.5)
                # Replay each filtered XHR through the browser fetch (uses cookies).
                for url in pages:
                    try:
                        frag = await page.evaluate(
                            "async (u) => {"
                            "  const r = await fetch(u, {headers:{'X-Requested-With':'XMLHttpRequest'}});"
                            "  if (!r.ok) return null;"
                            "  const j = await r.json();"
                            "  return j && j.templates ? j.templates.properties : null;"
                            "}",
                            url,
                        )
                    except Exception as exc:
                        logger.warning("[GUYHOQUET] PW fetch failed: %s", exc)
                        continue
                    if not frag:
                        continue
                    for l in _gh_parse_html(frag, 1000):
                        if l.lbc_id in seen:
                            continue
                        seen.add(l.lbc_id)
                        if l.price is not None and l.price > MAX_PRICE:
                            continue
                        listings.append(l)
                    if len(listings) >= max_results:
                        break
            finally:
                await ctx.close()
    except Exception as exc:
        logger.warning("[GUYHOQUET] Playwright fallback failed: %s", exc)

    logger.info("[GUYHOQUET] Parsed %d listings (Playwright fallback)", len(listings))
    return listings


# ─── Listing quality + fraud filter ──────────────────────────────────────────

_SKIP_TITLE = _re.compile(
    r"sous.?loc|contre service|recherch|cherch|coloc|parking|garage|cave|"
    r"chambre\s+(à\s+lou|chez\s+|libre|dispo|meublée?)|"
    r"\d+\s*(nuit|jour|semaine|mois)\b|janvier|f[eé]vrier|mars|avril|mai|juin|"
    r"juillet|ao[uû]t|septembre|octobre|novembre|d[eé]cembre",
    _re.IGNORECASE,
)
_SKIP_DESC = _re.compile(
    r"sous.?loc|je cherche|je recherche|nous cherchons|en recherche d|"
    r"disponible (du|jusqu|pour \d)|location saisonni[èe]re|"
    r"courte dur[eé]e|quelques (jours|semaines|mois)",
    _re.IGNORECASE,
)


def is_real_offer(listing: Listing) -> bool:
    """Return True if the listing looks like a genuine long-term rental offer."""
    if not listing.seller_name or listing.seller_name == "xxx":
        return False
    if not listing.price or listing.price < 400:
        return False
    if _SKIP_TITLE.search(listing.title or ""):
        return False
    blob = (listing.title or "") + " " + (listing.description or "")
    if _SKIP_DESC.search(blob):
        return False
    return True


_FRAUD_KEYWORDS = _re.compile(
    r"western union|moneygram|virement avant visite|"
    r"clés? par courrier|remise des clés? à distance|"
    r"je suis à l.étranger|partant? à l.étranger|travail (à|en) l.étranger|"
    r"envoyer l.argent|wire transfer|arnaque|scam",
    _re.IGNORECASE,
)


def is_suspicious(listing: Listing) -> tuple[bool, str]:
    """Return (True, reason) if the listing shows fraud or spam signals."""
    blob = f"{listing.title} {listing.description}"

    m = _FRAUD_KEYWORDS.search(blob)
    if m:
        return True, f"Mot-clé suspect : '{m.group()}'"

    if listing.price and listing.price < 350:
        return True, f"Prix anormalement bas ({listing.price}€)"

    if listing.description and len(listing.description.strip()) < 20:
        return True, "Description quasi vide"

    if listing.title and len(listing.title) > 250:
        return True, "Titre anormalement long (spam)"

    return False, ""


# ─── Public API ───────────────────────────────────────────────────────────────

def _is_pap(url: str) -> bool:
    return "pap.fr" in url


def _is_bienici(url: str) -> bool:
    return "bienici.com" in url


def _is_logicimmo(url: str) -> bool:
    return "logic-immo.com" in url


def _is_studapart(url: str) -> bool:
    return "studapart.com" in url


def _is_parisattitude(url: str) -> bool:
    return "parisattitude.com" in url


def _is_lodgis(url: str) -> bool:
    return "lodgis.com" in url


def _is_immojeune(url: str) -> bool:
    return "immojeune.com" in url


def _is_locservice(url: str) -> bool:
    return "locservice.fr" in url


def _is_roomlala(url: str) -> bool:
    return "roomlala.com" in url


def _is_entreparticuliers(url: str) -> bool:
    return "entreparticuliers.com" in url


def _is_ladresse(url: str) -> bool:
    return "ladresse.com" in url


def _is_century21(url: str) -> bool:
    return "century21.fr" in url


def _is_wizi(url: str) -> bool:
    return "wizi.io" in url or "wizi.eu" in url


def _is_laforet(url: str) -> bool:
    return "laforet.com" in url


def _is_inli(url: str) -> bool:
    return "inli.fr" in url


def _is_guyhoquet(url: str) -> bool:
    return "guy-hoquet.com" in url or "guyhoquet.com" in url


# In-memory scrape result cache. Key = (search_url, max_results). Value =
# (timestamp, listings). TTL = 5 min so back-to-back /campagne calls in the
# same window don't re-hit the same sources. Saves ~30s on repeat campaigns.
_SCRAPE_CACHE: dict[tuple, tuple[float, list]] = {}
_SCRAPE_CACHE_TTL_SEC = 300  # 5 minutes


# ════════════════════════════════════════════════════════════════════════════
# LBC Sentinel — sub-60s API poller for change detection
# ════════════════════════════════════════════════════════════════════════════
# curl_cffi with safari17_0 fingerprint bypasses DataDome at low volume.
# Verified: 12 polls/min, 0 failures, 95-122ms latency.
# Auto-fallback to Camoufox (existing scrape) if 403/429.

from typing import Callable, Awaitable

try:
    from curl_cffi import requests as _ccffi
    _CCFFI_AVAILABLE = True
except ImportError:
    _CCFFI_AVAILABLE = False

_LBC_API_URL = "https://api.leboncoin.fr/finder/search"
_LBC_API_KEY = "ba0c2dad52b3ec"
_LBC_UAS = [
    "leboncoin/8.10.0.0.0 iOS/17.0",
    "leboncoin/8.10.5.0.0 iOS/17.4",
    "leboncoin/8.11.2.0.0 iOS/17.5",
    "leboncoin/8.11.0.0.0 iOS/17.4.1",
    "leboncoin/8.10.5.0.0 Android/14",
]
_lbc_sentinel_last_id: Optional[str] = None
_lbc_sentinel_banned_until: float = 0.0
_lbc_sentinel_consec_fails: int = 0


def _lbc_default_filters() -> dict:
    return {
        "category": {"id": "10"},
        "enums": {"real_estate_type": ["1", "2"], "furnished": ["1"]},
        "ranges": {"price": {"max": 1100}, "square": {"min": 25}},
        "location": {
            "locations": [
                {"locationType": "city", "label": city} for city in [
                    "Paris", "Boulogne-Billancourt", "Neuilly-sur-Seine",
                    "Levallois-Perret", "Clichy", "Issy-les-Moulineaux",
                    "Montrouge", "Malakoff", "Vanves", "Ivry-sur-Seine",
                    "Le Kremlin-Bicêtre", "Charenton-le-Pont", "Saint-Mandé",
                    "Vincennes", "Montreuil", "Bagnolet", "Pantin",
                    "Saint-Ouen", "Aubervilliers", "Alfortville",
                    "Maisons-Alfort", "Saint-Maur-des-Fossés",
                ]
            ]
        },
    }


async def _lbc_sentinel_poll() -> Optional[str]:
    """Returns latest list_id (str) or None on failure / cooldown."""
    global _lbc_sentinel_consec_fails, _lbc_sentinel_banned_until
    if not _CCFFI_AVAILABLE:
        return None
    if time.time() < _lbc_sentinel_banned_until:
        return None
    headers = {
        "api_key": _LBC_API_KEY,
        "User-Agent": random.choice(_LBC_UAS),
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Connection": "close",
    }
    body = {
        "limit": 1, "limit_alu": 1, "sort_by": "time", "sort_order": "desc",
        "filters": _lbc_default_filters(),
    }
    try:
        def _do():
            return _ccffi.post(_LBC_API_URL, headers=headers, json=body,
                              impersonate="safari17_0", timeout=10)
        r = await asyncio.to_thread(_do)
    except Exception as exc:
        _lbc_sentinel_consec_fails += 1
        if _lbc_sentinel_consec_fails >= 5:
            _lbc_sentinel_banned_until = time.time() + 30 * 60
            logger.error("[SENTINEL] 5 consec failures → 30-min cooldown")
        return None
    if r.status_code in (403, 429):
        _lbc_sentinel_consec_fails += 1
        is_dd = "captcha-delivery" in (r.text or "")[:200]
        logger.warning("[SENTINEL] %d (DataDome=%s) consec=%d", r.status_code, is_dd, _lbc_sentinel_consec_fails)
        if _lbc_sentinel_consec_fails >= 3:
            _lbc_sentinel_banned_until = time.time() + 60 * 60
            logger.error("[SENTINEL] flagged → 1h cooldown, fallback to Camoufox")
        return None
    if r.status_code != 200:
        return None
    _lbc_sentinel_consec_fails = 0
    try:
        ads = (r.json() or {}).get("ads") or []
    except Exception:
        return None
    if not ads:
        return None
    lid = ads[0].get("list_id")
    return str(lid) if lid is not None else None


async def _lbc_sentinel_loop(
    on_change: Callable[[str], Awaitable[None]],
    base_interval_sec: int = 60,
    jitter_sec: int = 15,
) -> None:
    """Background loop: poll every base_interval ± jitter, fire on_change(new_id)."""
    global _lbc_sentinel_last_id
    logger.info("[SENTINEL] starting LBC poller — %ds±%ds", base_interval_sec, jitter_sec)
    seed = await _lbc_sentinel_poll()
    if seed is not None:
        _lbc_sentinel_last_id = seed
        logger.info("[SENTINEL] seeded last_id=%s", seed)
    while True:
        try:
            wait = base_interval_sec + random.uniform(-jitter_sec, jitter_sec)
            await asyncio.sleep(max(15.0, wait))
            new_id = await _lbc_sentinel_poll()
            if new_id is None:
                continue
            if _lbc_sentinel_last_id is None:
                _lbc_sentinel_last_id = new_id
                continue
            if new_id != _lbc_sentinel_last_id:
                logger.info("[SENTINEL] CHANGE: %s → %s", _lbc_sentinel_last_id, new_id)
                _lbc_sentinel_last_id = new_id
                try:
                    await on_change(new_id)
                except Exception as exc:
                    logger.exception("[SENTINEL] on_change crashed: %s", exc)
        except asyncio.CancelledError:
            logger.info("[SENTINEL] cancelled")
            raise
        except Exception as exc:
            logger.exception("[SENTINEL] iteration crashed: %s", exc)
            await asyncio.sleep(30)


# ════════════════════════════════════════════════════════════════════════════
# PAP Sentinel — sub-60s SERP polling for change detection
# ════════════════════════════════════════════════════════════════════════════
# No native API/RSS. SERP HTML is the only viable source. curl_cffi chrome120
# bypasses Cloudflare at low rate. Set-diff against previous IDs detects new.
_PAP_SENTINEL_STATE: dict[str, set[str]] = {}
_PAP_SENTINEL_URL = "https://www.pap.fr/annonce/locations-paris-75-g439"
_PAP_ID_RE = _re.compile(r"-r(\d{6,10})")
_PAP_UAS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
]


async def _pap_sentinel_poll(state_key: str = "default") -> Optional[str]:
    """Returns the highest-numerical NEW listing ID, or None."""
    if not _CCFFI_AVAILABLE:
        return None
    headers = {
        "User-Agent": random.choice(_PAP_UAS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.5",
        "Cache-Control": "no-cache",
    }
    def _do():
        try:
            r = _ccffi.get(_PAP_SENTINEL_URL, headers=headers,
                          impersonate="chrome120", timeout=10)
            return r.text if r.status_code == 200 else None
        except Exception:
            return None
    html = await asyncio.to_thread(_do)
    if not html:
        return None
    seen_set: set[str] = set()
    ids: list[str] = []
    for m in _PAP_ID_RE.finditer(html):
        pid = m.group(1)
        if pid not in seen_set:
            seen_set.add(pid)
            ids.append(pid)
    if not ids:
        return None
    current = set(ids)
    previous = _PAP_SENTINEL_STATE.get(state_key)
    _PAP_SENTINEL_STATE[state_key] = current
    if previous is None:
        logger.info("[PAP-SENTINEL] seeded with %d IDs", len(current))
        return None
    new_ids = current - previous
    if not new_ids:
        return None
    newest = max(new_ids, key=lambda x: int(x))
    logger.info("[PAP-SENTINEL] %d new listing(s), newest=r%s", len(new_ids), newest)
    return newest


async def _pap_sentinel_loop(on_change: Callable[[str], Awaitable[None]],
                             interval_sec: int = 75) -> None:
    """PAP background sentinel loop, fires on_change(new_id) on detect."""
    logger.info("[PAP-SENTINEL] starting — %ds interval", interval_sec)
    await asyncio.sleep(random.uniform(0, 30))  # stagger
    while True:
        try:
            new_id = await _pap_sentinel_poll()
            if new_id:
                try: await on_change(new_id)
                except Exception as exc:
                    logger.exception("[PAP-SENTINEL] on_change crashed: %s", exc)
            await asyncio.sleep(interval_sec + random.uniform(-10, 10))
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.exception("[PAP-SENTINEL] iteration crashed: %s", exc)
            await asyncio.sleep(60)


async def search_listings(search_url: str, max_results: int = 50) -> list[Listing]:
    """Scrape a search page. Source auto-detected from URL.

    Supported: LBC, SeLoger, PAP, Bien'ici, Logic-Immo,
    Studapart, Paris Attitude, Lodgis, ImmoJeune, LocService, Roomlala.

    Caches results in-memory for 5 minutes — back-to-back campaigns reuse
    the previous fetch instead of re-scraping.
    """
    if config.MOCK_MODE:
        from mock_data import MOCK_LISTINGS
        logger.info("[MOCK] Returning %d fake listings", len(MOCK_LISTINGS))
        return MOCK_LISTINGS[:max_results]

    cache_key = (search_url, max_results)
    cached = _SCRAPE_CACHE.get(cache_key)
    if cached is not None:
        ts, listings = cached
        age = time.time() - ts
        if age < _SCRAPE_CACHE_TTL_SEC:
            logger.info("[CACHE HIT] %s — returning %d cached (age %.0fs)", search_url[:60], len(listings), age)
            return listings

    # Dispatch to per-source scraper, then cache result
    if _is_seloger(search_url):
        listings = await _search_seloger_with_playwright(search_url, max_results)
    elif _is_pap(search_url):
        listings = await _search_pap_with_playwright(search_url, max_results)
    elif _is_bienici(search_url):
        listings = await _search_bienici_with_playwright(search_url, max_results)
    elif _is_logicimmo(search_url):
        listings = await _search_logicimmo_with_playwright(search_url, max_results)
    elif _is_studapart(search_url):
        listings = await _search_studapart_with_playwright(search_url, max_results)
    elif _is_parisattitude(search_url):
        listings = await _search_parisattitude_with_playwright(search_url, max_results)
    elif _is_lodgis(search_url):
        listings = await _search_lodgis_with_playwright(search_url, max_results)
    elif _is_immojeune(search_url):
        listings = await _search_immojeune_with_playwright(search_url, max_results)
    elif _is_locservice(search_url):
        listings = await _search_locservice_with_playwright(search_url, max_results)
    elif _is_roomlala(search_url):
        listings = await _search_roomlala_with_playwright(search_url, max_results)
    elif _is_entreparticuliers(search_url):
        listings = await _search_entreparticuliers(search_url, max_results)
    elif _is_ladresse(search_url):
        listings = await _search_ladresse(search_url, max_results)
    elif _is_century21(search_url):
        listings = await _search_century21(search_url, max_results)
    elif _is_wizi(search_url):
        listings = await _search_wizi(search_url, max_results)
    elif _is_laforet(search_url):
        listings = await _search_laforet_with_playwright(search_url, max_results)
    elif _is_guyhoquet(search_url):
        listings = await _search_guyhoquet_with_playwright(search_url, max_results)
    elif _is_inli(search_url):
        listings = await _search_inli(search_url, max_results)
    elif _is_gensdeconfiance(search_url):
        listings = await _search_gensdeconfiance(search_url, max_results)
    elif _is_cdc_habitat(search_url):
        listings = await _search_cdc_habitat(search_url, max_results)
    elif _is_fnaim(search_url):
        listings = await _search_fnaim(search_url, max_results)
    elif config.USE_APIFY:
        logger.info("[APIFY] Scraping search (max %d): %s", max_results, search_url)
        items = await _run_actor(config.APIFY_SEARCH_ACTOR, {
            "searchUrl": search_url,
            "maxItems": max_results,
            "proxyConfiguration": {"useApifyProxy": True},
        })
        listings = [l for l in (_item_to_listing(i) for i in items) if l]
    else:
        listings = await _search_with_playwright(search_url, max_results)

    if listings:
        _SCRAPE_CACHE[cache_key] = (time.time(), listings)
    return listings


async def fetch_single_listing(url: str) -> Optional[Listing]:
    """Fetch a single listing. Source auto-detected from URL.

    Dispatch order:
      • SeLoger  → curl_cffi/Camoufox SSR parse
      • PAP      → curl_cffi + JSON-LD/regex (`_fetch_pap_single`)
      • Bien'ici → realEstateAd.json API (`_fetch_bienici_single`)
      • LBC (and Apify when enabled) → __NEXT_DATA__ via Playwright
    Returns None if the source-specific path failed; callers (e.g. /add) may
    then fall back to `_fetch_generic_minimal(url)` to still persist the URL.
    """
    if _is_seloger(url):
        return await _fetch_seloger_single_with_playwright(url)
    if _is_pap(url):
        return await _fetch_pap_single(url)
    if _is_bienici(url):
        return await _fetch_bienici_single(url)

    if config.USE_APIFY:
        logger.info("[APIFY] Fetching single listing: %s", url)
        items = await _run_actor(config.APIFY_SEARCH_ACTOR, {
            "startUrls": [{"url": url}],
            "maxItems": 1,
            "proxyConfiguration": {"useApifyProxy": True},
        })
        return _item_to_listing(items[0]) if items else None

    return await _fetch_single_with_playwright(url)

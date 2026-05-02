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
                headless=False,
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
    cam_html = await _fetch_html_with_camoufox(url, post_delay=(3.0, 5.0))
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

    # Price — check attributes array first (skip non-dict entries), then top-level
    price_raw = None
    for attr in _ensure_list(ad.get("attributes")):
        if not isinstance(attr, dict):
            continue
        if attr.get("key") == "price":
            values = attr.get("values")
            first_val = values[0] if isinstance(values, list) and values else None
            price_raw = attr.get("value_label") or first_val
            break
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

    return Listing(
        lbc_id=lbc_id,
        title=ad.get("subject") or ad.get("title") or "",
        description=ad.get("body") or "",
        price=_parse_price(price_raw),
        location=location,
        seller_name=seller_name,
        url=url,
        seller_type_hint=seller_type_hint,
        images=images,
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

def _seloger_ad_to_listing(ad: dict, url: str = "") -> Optional[Listing]:
    """Convert a SeLoger classifiedsData entry to a Listing.

    Structure (from classifiedsData in pageProps):
      id, url, location.address.{city, zipCode}, hardFacts.price.value,
      hardFacts.{title, keyfacts}, cardProvider.title (seller name),
      legacyTracking.id (numeric legacy id)
    """
    sl_id = str(ad.get("id") or ad.get("classifiedId") or "")
    if not sl_id:
        return None

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

    # Description: not available in search results (only on listing detail page)
    description = ad.get("description") or ad.get("descriptif") or ""

    # Extract photo URLs from gallery.images[].url
    gallery = ad.get("gallery") or {}
    images = [
        img.get("url", "")
        for img in (gallery.get("images") or [])
        if img.get("url")
    ]

    return Listing(
        lbc_id=listing_id,
        title=title,
        description=description,
        price=_parse_price(price_raw),
        location=location,
        seller_name=seller_name,
        url=url,
        seller_type_hint="pro",
        source="seloger",
        images=images,
    )


# ─── Playwright scrapers (free) ───────────────────────────────────────────────

async def _search_with_playwright(search_url: str, max_results: int) -> list[Listing]:
    logger.info("[PLAYWRIGHT] Scraping search page: %s", search_url)
    data = await _pw_get_next_data(search_url)
    if not data:
        raise RuntimeError(
            "Playwright could not fetch search page — DataDome blocked or page structure changed"
        )

    props = _ensure_dict(_dig(data, "props", "pageProps"))
    # LBC stores results under searchData.ads or directly ads. Either may be
    # missing or have a different shape; ensure list before iterating.
    ads = (
        _dig(props, "searchData", "ads")
        or props.get("ads")
        or []
    )
    ads = _ensure_list(ads)

    if not ads:
        logger.warning("No ads found in __NEXT_DATA__. pageProps keys: %s", list(props.keys()))

    listings = []
    for ad in ads[:max_results]:
        if not isinstance(ad, dict):
            logger.debug("Skipping non-dict ad entry: %r", type(ad).__name__)
            continue
        try:
            lbc_id = str(ad.get("list_id") or ad.get("id") or "")
            url = f"https://www.leboncoin.fr/ad/locations/{lbc_id}" if lbc_id else search_url
            listing = _ad_to_listing(ad, url)
            if listing:
                listings.append(listing)
        except Exception as exc:
            # One malformed listing must not kill the whole batch.
            logger.warning(
                "Skipping malformed LBC ad (%s): %s",
                ad.get("list_id") or ad.get("id") or "?",
                exc,
            )

    logger.info("[PLAYWRIGHT] Found %d listings", len(listings))
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

async def _pw_get_seloger_data(url: str) -> Optional[dict]:
    """
    Fetch SeLoger page using Camoufox (anti-detect Firefox) to bypass Datadome.
    Extracts listing data from window["__UFRN_FETCHER__"] in the SSR HTML.
    """
    from camoufox.async_api import AsyncCamoufox

    ssr_html: Optional[str] = None

    async with AsyncCamoufox(headless=False, locale=["fr-FR"], os="windows") as browser:
        page = await browser.new_page()

        async def on_response(r):
            nonlocal ssr_html
            ct = r.headers.get("content-type", "")
            if r.status == 200 and "text/html" in ct and "classified-search" in r.url:
                try:
                    ssr_html = await r.text()
                except Exception:
                    pass

        page.on("response", on_response)

        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=30_000)
            await _handle_cookie_banner(page)
            await asyncio.sleep(random.uniform(1.5, 2.5))
        finally:
            await page.close()

    if not ssr_html:
        logger.warning("[SELOGER] No HTML captured — Camoufox may have been blocked")
        return None

    # Extract window["__UFRN_FETCHER__"] — SeLoger's SSR data container
    m = _re.search(
        r'window\["__UFRN_FETCHER__"\]=JSON\.parse\("(.*?)"\);\s*</script>',
        ssr_html, _re.DOTALL
    )
    if not m:
        logger.warning("[SELOGER] __UFRN_FETCHER__ not found in page HTML")
        return None

    try:
        raw_str = m.group(1).encode("utf-8").decode("unicode_escape")
        fetcher = json.loads(raw_str)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        logger.warning("[SELOGER] Failed to parse __UFRN_FETCHER__: %s", exc)
        return None

    serp_raw = fetcher.get("data", {}).get("classified-serp-init-data")
    if not serp_raw or not isinstance(serp_raw, str) or not serp_raw.strip():
        logger.warning("[SELOGER] classified-serp-init-data is empty — still blocked")
        return None

    # SeLoger compresses the SERP data with LZString.compressToBase64
    try:
        import lzstring as _lzs
        decompressed = _lzs.LZString().decompressFromBase64(serp_raw)
        if not decompressed:
            raise ValueError("LZString decompression returned empty result")
        return {"_raw": json.loads(decompressed)}
    except Exception as exc:
        logger.warning("[SELOGER] Failed to decompress/parse serp data: %s", exc)
        return None


async def _search_seloger_with_playwright(search_url: str, max_results: int) -> list[Listing]:
    logger.info("[SELOGER] Scraping search page: %s", search_url)
    try:
        data = await _pw_get_seloger_data(search_url)
    except Exception as exc:
        logger.warning("[SELOGER] Scraping failed: %s", exc)
        return []
    if not data:
        logger.warning("[SELOGER] Could not retrieve data — blocked or URL format incorrect")
        logger.warning("[SELOGER] Tip: copy the search URL manually from your browser after filtering on seloger.com")
        return []

    raw = data.get("_raw") or data
    page_props = raw.get("pageProps") or {}

    # classifieds = ordered list of CPS IDs; classifiedsData = dict keyed by CPS ID
    cps_ids: list = page_props.get("classifieds") or []
    classified_map: dict = page_props.get("classifiedsData") or {}

    if not classified_map:
        logger.warning("[SELOGER] No classifiedsData found. pageProps keys: %s", list(page_props.keys())[:10])
        return []

    listings = []
    for cps_id in cps_ids[:max_results]:
        ad = classified_map.get(cps_id)
        if not ad or not isinstance(ad, dict):
            continue
        listing = _seloger_ad_to_listing(ad)
        if listing:
            listings.append(listing)

    logger.info("[SELOGER] Found %d listings (total available: %d)", len(listings), page_props.get("totalCount", 0))
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
    title_m = _re2.search(r"(\d+)\s*m²", text)
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
    )


async def _search_pap_with_playwright(search_url: str, max_results: int) -> list[Listing]:
    logger.info("[PAP] Scraping search page: %s", search_url)
    try:
        from bs4 import BeautifulSoup
        import re as _re2

        html: Optional[str] = None

        async with async_playwright() as pw:
            ctx = await pw.chromium.launch_persistent_context(
                user_data_dir=_user_data_dir("pap"),
                headless=False,
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
                await page.goto(search_url, wait_until="domcontentloaded", timeout=30_000)
                await _handle_cookie_banner(page)
                await asyncio.sleep(random.uniform(1.5, 2.5))
                html = await page.content()
            finally:
                await ctx.close()

        if not html:
            return []

        soup = BeautifulSoup(html, "html.parser")
        items = soup.find_all(class_=_re2.compile("search-list-item"))
        logger.info("[PAP] Found %d item elements", len(items))

        listings = []
        for item in items[:max_results]:
            listing = _parse_pap_listing(item)
            if listing:
                listings.append(listing)

        logger.info("[PAP] Parsed %d listings", len(listings))
        return listings

    except Exception as exc:
        logger.warning("[PAP] Scraping failed: %s", exc)
        return []


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
    )


async def _search_bienici_with_playwright(search_url: str, max_results: int) -> list[Listing]:
    logger.info("[BIENICI] Scraping search page: %s", search_url)

    # Try __NEXT_DATA__ first (Bien'ici uses Next.js)
    data = await _pw_get_next_data(search_url, site="bienici")
    if data:
        props = data.get("props", {}).get("pageProps", {})
        ads = (
            (props.get("searchResults") or {}).get("ads")
            or props.get("ads")
            or props.get("realEstateAds")
            or props.get("listings")
            or []
        )
        if ads:
            listings = [l for l in (_bienici_ad_to_listing(a) for a in ads[:max_results]) if l]
            logger.info("[BIENICI] Found %d listings via __NEXT_DATA__", len(listings))
            return listings
        logger.warning("[BIENICI] No ads in __NEXT_DATA__. pageProps keys: %s", list(props.keys())[:10])

    # Fallback: intercept the JSON API response (realEstateAds.json endpoint)
    captured: list[dict] = []

    async with async_playwright() as pw:
        ctx = await pw.chromium.launch_persistent_context(
            user_data_dir=_user_data_dir("bienici"),
            headless=False,
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

        async def _on_bienici_response(r):
            if ("realEstateAds" in r.url or "/search" in r.url) and r.status == 200:
                ct = r.headers.get("content-type", "")
                if "json" in ct:
                    try:
                        body = await r.json()
                        ads = body.get("realEstateAds") or body.get("ads") or []
                        captured.extend(ads)
                    except Exception:
                        pass

        page.on("response", _on_bienici_response)
        try:
            await page.goto(search_url, wait_until="networkidle", timeout=35_000)
            await _handle_cookie_banner(page)
            await asyncio.sleep(random.uniform(2.0, 3.0))
        finally:
            await ctx.close()

    if not captured:
        logger.warning("[BIENICI] Could not capture any listings")
        return []

    listings = [l for l in (_bienici_ad_to_listing(a) for a in captured[:max_results]) if l]
    logger.info("[BIENICI] Found %d listings via XHR intercept", len(listings))
    return listings


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
    sm = _re2.search(r"(\d+)\s*m²", text, _re2.IGNORECASE)
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
    )


async def _search_logicimmo_with_playwright(search_url: str, max_results: int) -> list[Listing]:
    logger.info("[LOGICIMMO] Scraping search page: %s", search_url)
    try:
        from bs4 import BeautifulSoup
        import re as _re2

        html: Optional[str] = None

        async with async_playwright() as pw:
            ctx = await pw.chromium.launch_persistent_context(
                user_data_dir=_user_data_dir("logicimmo"),
                headless=False,
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
                await page.goto(search_url, wait_until="domcontentloaded", timeout=30_000)
                await _handle_cookie_banner(page)
                await asyncio.sleep(random.uniform(2.0, 3.5))
                html = await page.content()
            finally:
                await ctx.close()

        if not html:
            return []

        soup = BeautifulSoup(html, "html.parser")
        items = (
            soup.find_all("article", class_=_re2.compile(r"listing|ad-result|property", _re2.I))
            or soup.find_all(attrs={"data-listing-id": True})
            or soup.find_all(class_=_re2.compile(r"listing-item|search-result-item|offer-card", _re2.I))
        )
        logger.info("[LOGICIMMO] Found %d card elements", len(items))

        listings = [l for l in (_logicimmo_item_to_listing(i) for i in items[:max_results]) if l]
        logger.info("[LOGICIMMO] Parsed %d listings", len(listings))
        return listings

    except Exception as exc:
        logger.warning("[LOGICIMMO] Scraping failed: %s", exc)
        return []


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
            headless=False,
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

    sm = _re2.search(r"(\d+)\s*m²", text, _re2.IGNORECASE)
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

async def _fetch_html_with_camoufox(url: str, post_delay: tuple[float, float] = (5.0, 8.0)) -> Optional[str]:
    """Fetch a page with Camoufox (anti-detect Firefox) — bypasses stealth-Playwright fingerprinting.

    Studapart serves an SEO landing page (no listings) to detected automation
    on the same URL where real browsers see ~48 listings. Camoufox masks the
    fingerprint enough to get the real content. Same approach we use for
    SeLoger (DataDome).
    """
    from camoufox.async_api import AsyncCamoufox
    async with AsyncCamoufox(headless=False, locale=["fr-FR"], os="windows") as browser:
        page = await browser.new_page()
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=30_000)
            await _handle_cookie_banner(page)
            await asyncio.sleep(random.uniform(*post_delay))
            return await page.content()
        except Exception as exc:
            logger.warning("Camoufox fetch failed for %s: %s", url, exc)
            return None
        finally:
            await page.close()


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


async def _search_studapart_with_playwright(search_url: str, max_results: int) -> list[Listing]:
    """Studapart scraper — uses Camoufox (anti-detect Firefox) because stealth
    Playwright gets fingerprinted and served an SEO landing page instead of
    real listings. ~48 cards per page; 200+ across paginated index.
    """
    logger.info("[STUDAPART] Scraping: %s", search_url)
    try:
        html = await _fetch_html_with_camoufox(search_url, post_delay=(5.0, 8.0))
        if not html:
            return []
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, "html.parser")
        cards = soup.find_all("a", class_="AccomodationBlock")
        logger.info("[STUDAPART] Found %d card elements", len(cards))
        listings = [
            l for l in (_studapart_card_to_listing(c) for c in cards[:max_results]) if l
        ]
        logger.info("[STUDAPART] Parsed %d listings", len(listings))
        return listings
    except Exception as exc:
        logger.warning("[STUDAPART] Scraping failed: %s", exc)
        return []


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
    )


async def _search_parisattitude_with_playwright(search_url: str, max_results: int) -> list[Listing]:
    """Site-specific parser — bypasses the generic Next.js path (PA is .NET/Quasar)."""
    logger.info("[PARISATTITUDE] Scraping: %s", search_url)
    try:
        html = await _fetch_html_with_stealth(search_url, "parisattitude", post_delay=(3.0, 5.0))
        if not html:
            return []
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, "html.parser")
        cards = soup.find_all("div", class_="accommodation-search-card")
        logger.info("[PARISATTITUDE] Found %d card elements", len(cards))
        listings = [
            l for l in (_parisattitude_card_to_listing(c) for c in cards[:max_results]) if l
        ]
        logger.info("[PARISATTITUDE] Parsed %d listings", len(listings))
        return listings
    except Exception as exc:
        logger.warning("[PARISATTITUDE] Scraping failed: %s", exc)
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
    sm = _re2.search(r"(\d+)\s*m²", text)
    surface = sm.group(1) if sm else ""

    # Location: "Paris X°" + neighborhood preceding €
    arr_m = _re2.search(r"Paris\s+(\d{1,2})\s*°?", text)
    location = ""
    if arr_m:
        # Try to find neighborhood — usually the segment right before "€"
        neighborhood_m = _re2.search(r"\|\s*([^|€]+?)\s*\|\s*€", text)
        if neighborhood_m:
            location = f"{neighborhood_m.group(1).strip()}, Paris {arr_m.group(1)}"
        else:
            location = f"Paris {arr_m.group(1)}"

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
    )


async def _search_lodgis_with_playwright(search_url: str, max_results: int) -> list[Listing]:
    """Lodgis scraper — Paris medium/long-term furnished. Uses Camoufox.
    Heads-up: typical inventory is 1000€+ so most results filter out."""
    logger.info("[LODGIS] Scraping: %s", search_url)
    try:
        html = await _fetch_html_with_camoufox(search_url, post_delay=(4.0, 6.0))
        if not html:
            return []
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, "html.parser")
        cards = soup.find_all("div", class_="card__appart")
        logger.info("[LODGIS] Found %d card elements", len(cards))
        listings = [
            l for l in (_lodgis_card_to_listing(c) for c in cards[:max_results]) if l
        ]
        logger.info("[LODGIS] Parsed %d listings", len(listings))
        return listings
    except Exception as exc:
        logger.warning("[LODGIS] Scraping failed: %s", exc)
        return []


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
    sm = _re2.search(r"(\d+)\s*m²\s*-\s*(\d+)\s*€", text)
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
    )


async def _search_immojeune_with_playwright(search_url: str, max_results: int) -> list[Listing]:
    """ImmoJeune scraper — uses Camoufox like Studapart since stealth Playwright
    gets fingerprinted and served the SEO landing version of the page.
    """
    logger.info("[IMMOJEUNE] Scraping: %s", search_url)
    try:
        html = await _fetch_html_with_camoufox(search_url, post_delay=(4.0, 6.0))
        if not html:
            return []
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, "html.parser")
        cards = soup.find_all("div", class_="card")
        logger.info("[IMMOJEUNE] Found %d card elements", len(cards))
        listings = [
            l for l in (_immojeune_card_to_listing(c) for c in cards[:max_results]) if l
        ]
        logger.info("[IMMOJEUNE] Parsed %d listings", len(listings))
        return listings
    except Exception as exc:
        logger.warning("[IMMOJEUNE] Scraping failed: %s", exc)
        return []


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

    # Title — first text up to the location marker "Paris XX (75XXX)"
    title_m = _re2.search(r"^(.+?)\s+Paris\s+\d{1,2}\s*\(", text)
    title = title_m.group(1).strip() if title_m else (text[:80] if text else "Appartement")

    location = ""
    loc_m = _re2.search(r"(Paris\s+\d{1,2})\s*\((\d{5})\)", text)
    if loc_m:
        location = f"{loc_m.group(1)}, {loc_m.group(2)}"
    else:
        zip_m = _re2.search(r"\b(\d{5})\b", text)
        if zip_m:
            location = zip_m.group(1)

    # Description — text after the price up to ~500 chars
    desc_m = _re2.search(r"€\s*/\s*mois\s*(.+?)$", text)
    description = desc_m.group(1).strip()[:500] if desc_m else ""

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
    )


async def _search_locservice_with_playwright(search_url: str, max_results: int) -> list[Listing]:
    """LocService scraper — owner-direct rentals, French market.
    Uses Camoufox for consistency with the rest of the Phase 2 fix; stealth
    Playwright also works on this site but Camoufox is more reliable."""
    logger.info("[LOCSERVICE] Scraping: %s", search_url)
    try:
        html = await _fetch_html_with_camoufox(search_url, post_delay=(4.0, 6.0))
        if not html:
            return []
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, "html.parser")
        cards = soup.find_all("li", class_="accommodation-ad")
        logger.info("[LOCSERVICE] Found %d card elements", len(cards))
        listings = [
            l for l in (_locservice_card_to_listing(c) for c in cards[:max_results]) if l
        ]
        logger.info("[LOCSERVICE] Parsed %d listings", len(listings))
        return listings
    except Exception as exc:
        logger.warning("[LOCSERVICE] Scraping failed: %s", exc)
        return []


async def _search_roomlala_with_playwright(search_url: str, max_results: int) -> list[Listing]:
    return await _search_via_generic(
        search_url, max_results,
        site="roomlala",
        base_url="https://www.roomlala.com",
        source="roomlala",
        prefix="rl",
        card_selectors=[r"property-card|room-card|listing-card", r"card|listing"],
    )


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


async def search_listings(search_url: str, max_results: int = 50) -> list[Listing]:
    """Scrape a search page. Source auto-detected from URL.

    Supported: LBC, SeLoger, PAP, Bien'ici, Logic-Immo,
    Studapart, Paris Attitude, Lodgis, ImmoJeune, LocService, Roomlala.
    """
    if config.MOCK_MODE:
        from mock_data import MOCK_LISTINGS
        logger.info("[MOCK] Returning %d fake listings", len(MOCK_LISTINGS))
        return MOCK_LISTINGS[:max_results]

    if _is_seloger(search_url):
        return await _search_seloger_with_playwright(search_url, max_results)

    if _is_pap(search_url):
        return await _search_pap_with_playwright(search_url, max_results)

    if _is_bienici(search_url):
        return await _search_bienici_with_playwright(search_url, max_results)

    if _is_logicimmo(search_url):
        return await _search_logicimmo_with_playwright(search_url, max_results)

    if _is_studapart(search_url):
        return await _search_studapart_with_playwright(search_url, max_results)

    if _is_parisattitude(search_url):
        return await _search_parisattitude_with_playwright(search_url, max_results)

    if _is_lodgis(search_url):
        return await _search_lodgis_with_playwright(search_url, max_results)

    if _is_immojeune(search_url):
        return await _search_immojeune_with_playwright(search_url, max_results)

    if _is_locservice(search_url):
        return await _search_locservice_with_playwright(search_url, max_results)

    if _is_roomlala(search_url):
        return await _search_roomlala_with_playwright(search_url, max_results)

    if config.USE_APIFY:
        logger.info("[APIFY] Scraping search (max %d): %s", max_results, search_url)
        items = await _run_actor(config.APIFY_SEARCH_ACTOR, {
            "searchUrl": search_url,
            "maxItems": max_results,
            "proxyConfiguration": {"useApifyProxy": True},
        })
        return [l for l in (_item_to_listing(i) for i in items) if l]

    return await _search_with_playwright(search_url, max_results)


async def fetch_single_listing(url: str) -> Optional[Listing]:
    """Fetch a single listing. Source auto-detected from URL."""
    if _is_seloger(url):
        return await _fetch_seloger_single_with_playwright(url)

    if config.USE_APIFY:
        logger.info("[APIFY] Fetching single listing: %s", url)
        items = await _run_actor(config.APIFY_SEARCH_ACTOR, {
            "startUrls": [{"url": url}],
            "maxItems": 1,
            "proxyConfiguration": {"useApifyProxy": True},
        })
        return _item_to_listing(items[0]) if items else None

    return await _fetch_single_with_playwright(url)

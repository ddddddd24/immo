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

# Persistent browser profile — keeps cookies/session across runs
_USER_DATA_DIR = str(Path("data/browser_profile"))


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


async def _pw_get_next_data(url: str) -> Optional[dict]:
    """
    Open *url* using a persistent Chromium profile with stealth enabled.
    Returns parsed __NEXT_DATA__ JSON or None.
    """
    Path(_USER_DATA_DIR).mkdir(parents=True, exist_ok=True)

    async with async_playwright() as pw:
        ctx = await pw.chromium.launch_persistent_context(
            user_data_dir=_USER_DATA_DIR,
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
            # Brief human-like pause before extracting
            await asyncio.sleep(random.uniform(1.5, 3.0))
            content = await page.content()
        finally:
            await ctx.close()

    match = _re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', content, _re.DOTALL)
    if not match:
        logger.warning("__NEXT_DATA__ not found — DataDome may have blocked the request (%s)", url)
        return None
    try:
        return json.loads(match.group(1))
    except json.JSONDecodeError:
        logger.warning("Failed to parse __NEXT_DATA__ JSON for %s", url)
        return None


# ─── Source detection ─────────────────────────────────────────────────────────

def _is_seloger(url: str) -> bool:
    return "seloger.com" in url


# ─── Listing normalisation ────────────────────────────────────────────────────

def _parse_price(raw) -> Optional[int]:
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        return int(raw)
    digits = "".join(c for c in str(raw) if c.isdigit())
    return int(digits) if digits else None


def _ad_to_listing(ad: dict, url: str = "") -> Optional[Listing]:
    """Convert a LBC ad dict (from __NEXT_DATA__) to a Listing."""
    lbc_id = str(ad.get("list_id") or ad.get("id") or "")
    if not lbc_id:
        return None

    if not url:
        url = f"https://www.leboncoin.fr/ad/locations/{lbc_id}"

    # Price — check attributes array first, then top-level price field
    price_raw = None
    for attr in (ad.get("attributes") or []):
        if attr.get("key") == "price":
            price_raw = attr.get("value_label") or attr.get("values", [None])[0]
            break
    if price_raw is None:
        p = ad.get("price", [None])
        price_raw = p[0] if isinstance(p, list) else p

    loc = ad.get("location") or {}
    location = ", ".join(filter(None, [loc.get("city"), loc.get("zipcode")]))

    owner = ad.get("owner") or {}
    seller_name = owner.get("name") or owner.get("store_name") or ""
    seller_type_hint = owner.get("type") or ""

    # Extract photo URLs (LBC stores them in images array)
    images = [
        img.get("url") or img.get("thumb_url") or ""
        for img in (ad.get("images", {}).get("urls_large") or ad.get("images", {}).get("urls") or [])
        if img
    ]
    images = [u for u in images if u]

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
        raise RuntimeError("Playwright could not fetch search page — DataDome blocked or page structure changed")

    props = data.get("props", {}).get("pageProps", {})
    # LBC stores results under searchData.ads or directly ads
    ads = (
        props.get("searchData", {}).get("ads")
        or props.get("ads")
        or []
    )

    if not ads:
        logger.warning("No ads found in __NEXT_DATA__. pageProps keys: %s", list(props.keys()))

    listings = []
    for ad in ads[:max_results]:
        lbc_id = str(ad.get("list_id") or ad.get("id") or "")
        url = f"https://www.leboncoin.fr/ad/locations/{lbc_id}" if lbc_id else search_url
        listing = _ad_to_listing(ad, url)
        if listing:
            listings.append(listing)

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
        price=price_raw,
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
                user_data_dir=_USER_DATA_DIR,
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

async def _run_actor(actor_id: str, input_payload: dict) -> list[dict]:
    headers = {"Content-Type": "application/json"}
    params = {"token": config.APIFY_API_KEY}
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            f"{_APIFY_BASE}/acts/{actor_id}/runs",
            json=input_payload, params=params, headers=headers,
        )
        resp.raise_for_status()
        run_id = resp.json()["data"]["id"]
        logger.info("Apify run started: actor=%s run_id=%s", actor_id, run_id)

        deadline = time.time() + _MAX_WAIT
        while time.time() < deadline:
            await asyncio.sleep(_POLL_INTERVAL)
            s = await client.get(f"{_APIFY_BASE}/actor-runs/{run_id}", params=params)
            s.raise_for_status()
            status = s.json()["data"]["status"]
            if status == "SUCCEEDED":
                break
            if status in ("FAILED", "TIMED-OUT", "ABORTED"):
                raise RuntimeError(f"Apify run {run_id} ended: {status}")
        else:
            raise TimeoutError(f"Apify run {run_id} timed out after {_MAX_WAIT}s")

        dataset_id = s.json()["data"]["defaultDatasetId"]
        items = await client.get(
            f"{_APIFY_BASE}/datasets/{dataset_id}/items",
            params={**params, "format": "json", "clean": "true"},
        )
        items.raise_for_status()
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
    data = await _pw_get_next_data(search_url)
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
            user_data_dir=_USER_DATA_DIR,
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
        price=price_raw,
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
                user_data_dir=_USER_DATA_DIR,
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


async def search_listings(search_url: str, max_results: int = 50) -> list[Listing]:
    """Scrape a search page. Source auto-detected from URL (LBC, SeLoger, PAP, Bien'ici, Logic-Immo)."""
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

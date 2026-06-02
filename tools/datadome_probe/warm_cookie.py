"""Phase 2 — warming de cookie DataDome via vrai navigateur (Camoufox).

Hypothèse à tester : un navigateur réel obtient un cookie `datadome` valide
(challenge JS résolu silencieusement), qu'on réinjecte ensuite dans curl_cffi
pour faire passer l'API mobile LBC et le SSR SeLoger.

Pronostic faible si l'IP est flaggée (le navigateur lui-même est challengé),
mais c'est le seul levier technique gratuit testable. NE TOUCHE PAS la prod.

Usage :
    python tools/datadome_probe/warm_cookie.py
"""
import asyncio
import sys
import time

try:
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
except Exception:
    pass

from curl_cffi import requests as ccffi

LBC_API_URL = "https://api.leboncoin.fr/finder/search"
LBC_API_KEY = "ba0c2dad52b3ec"
LBC_UA = "leboncoin/8.10.0.0.0 iOS/17.0"
LBC_FILTERS = {
    "category": {"id": "10"},
    "enums": {"real_estate_type": ["1", "2"], "furnished": ["1"]},
    "ranges": {"price": {"max": 1000}, "square": {"min": 25}},
    "location": {"locations": [
        {"locationType": "department", "department_id": "75", "label": "Paris"},
        {"locationType": "department", "department_id": "92", "label": "Hauts-de-Seine"},
        {"locationType": "department", "department_id": "93", "label": "Seine-Saint-Denis"},
        {"locationType": "department", "department_id": "94", "label": "Val-de-Marne"},
    ]},
}
SELOGER_URL = (
    "https://www.seloger.com/classified-search?distributionTypes=Rent"
    "&estateTypes=House,Apartment"
    "&locations=eyJwbGFjZUlkcyI6WyJTVFJURlI0NDA5MDQ1Il0sImR1cmF0aW9uIjoiNjAiLCJtb2RlIjoiVHJhbnNpdCJ9"
    "&priceMax=1000&projectTypes=Stock&spaceMin=25"
)


def p(t=""):
    print(t, flush=True)


async def warm(url: str, wait_s: float = 12.0):
    """Charge `url` dans Camoufox, renvoie (cookies_list, page_title, html_len, datadome_cookie)."""
    from camoufox.async_api import AsyncCamoufox
    async with AsyncCamoufox(headless=True, locale=["fr-FR"], os="windows") as browser:
        ctx = await browser.new_context(locale="fr-FR", viewport={"width": 1280, "height": 800})
        page = await ctx.new_page()
        t0 = time.time()
        challenged = False
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=45_000)
        except Exception as e:
            p(f"    goto exception après {time.time()-t0:.1f}s : {type(e).__name__}: {e}")
            challenged = True
        await asyncio.sleep(wait_s)  # laisser le challenge JS s'exécuter
        cookies = await ctx.cookies()
        try:
            html = await page.content()
        except Exception:
            html = ""
        title = ""
        try:
            title = await page.title()
        except Exception:
            pass
        dd = next((c for c in cookies if c["name"].lower() == "datadome"), None)
        p(f"    goto+wait = {time.time()-t0:.1f}s | titre={title!r} | html={len(html)} chars")
        p(f"    cookie datadome présent = {bool(dd)}"
          + (f" (val[:30]={dd['value'][:30]}…)" if dd else ""))
        if "captcha-delivery" in html or "geo.captcha" in html or "datadome" in html.lower():
            p("    ⚠️ page contient une signature DataDome (interstitielle/challenge)")
            challenged = True
        await browser.close() if hasattr(browser, "close") else None
        return cookies, title, len(html), dd, challenged


def cookies_to_jar(cookies):
    return {c["name"]: c["value"] for c in cookies}


async def test_lbc():
    p("\n══ LBC : warm www.leboncoin.fr puis replay API ══════════")
    cookies, title, hlen, dd, challenged = await warm("https://www.leboncoin.fr/recherche?category=10")
    if not dd:
        p("    → pas de cookie datadome exploitable, replay quand même avec tous les cookies")
    jar = cookies_to_jar(cookies)
    headers = {
        "api_key": LBC_API_KEY, "User-Agent": LBC_UA,
        "Accept": "application/json", "Content-Type": "application/json",
        "Connection": "close",
    }
    body = {"limit": 1, "limit_alu": 1, "sort_by": "time",
            "sort_order": "desc", "filters": LBC_FILTERS}
    try:
        r = ccffi.post(LBC_API_URL, headers=headers, json=body, cookies=jar,
                       impersonate="safari17_0", timeout=15)
        ads = []
        if r.status_code == 200:
            ads = (r.json() or {}).get("ads") or []
        p(f"    REPLAY API → status={r.status_code}  ads={len(ads)}")
        return r.status_code == 200 and len(ads) > 0
    except Exception as e:
        p(f"    REPLAY API exception : {e}")
        return False


async def test_seloger():
    p("\n══ SeLoger : warm SSR puis replay curl_cffi ═════════════")
    cookies, title, hlen, dd, challenged = await warm(SELOGER_URL)
    has_fetcher_in_browser = hlen > 50_000  # une vraie SERP fait des centaines de Ko
    p(f"    HTML navigateur volumineux (vraie SERP ?) = {has_fetcher_in_browser}")
    jar = cookies_to_jar(cookies)
    try:
        r = ccffi.get(SELOGER_URL, cookies=jar, impersonate="chrome120",
                      timeout=30, allow_redirects=True)
        has_fetcher = "__UFRN_FETCHER__" in (r.text or "")
        p(f"    REPLAY SSR → status={r.status_code}  __UFRN_FETCHER__={has_fetcher}  len={len(r.text or '')}")
        return r.status_code == 200 and has_fetcher
    except Exception as e:
        p(f"    REPLAY SSR exception : {e}")
        return False


async def main():
    p("=" * 60)
    p(" WARMING DE COOKIE DATADOME (Camoufox → curl_cffi)")
    p("=" * 60)
    lbc = await test_lbc()
    sel = await test_seloger()
    p("\n" + "=" * 60)
    p(" VERDICT WARMING")
    p("=" * 60)
    p(f"  LBC replay produit des annonces : {'OUI ✅' if lbc else 'NON ❌'}")
    p(f"  SeLoger replay produit la SERP  : {'OUI ✅' if sel else 'NON ❌'}")
    if not lbc and not sel:
        p("\n  → Le warming ne débloque rien : le navigateur lui-même est")
        p("    challengé sur cette IP. Confirme que le levier est l'IP, pas l'outil.")


if __name__ == "__main__":
    asyncio.run(main())

"""Enquête SeLoger : pourquoi 403/timeout sur une IP fraîche (LBC marche) ?
Teste : (A) handshake cookie curl_cffi + headers navigateur, (B) Camoufox avec
wait_until='commit' (hypothèse : le challenge DataDome tient le document ouvert,
comme Bien'ici, d'où le timeout en 'domcontentloaded')."""
import asyncio, os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
try: sys.stdout.reconfigure(encoding="utf-8")
except Exception: pass

URL = ("https://www.seloger.com/classified-search?distributionTypes=Rent"
       "&estateTypes=House,Apartment"
       "&locations=eyJwbGFjZUlkcyI6WyJTVFJURlI0NDA5MDQ1Il0sImR1cmF0aW9uIjoiNjAiLCJtb2RlIjoiVHJhbnNpdCJ9"
       "&priceMax=1000&projectTypes=Stock&spaceMin=25")
HOME = "https://www.seloger.com/"


def p(*a): print(*a, flush=True)


async def test_curl():
    from curl_cffi.requests import AsyncSession
    p("\n══ A) curl_cffi — handshake cookie + headers ══════════════════")
    # A1 : 2 GET séquentiels dans la même session (réutilise le cookie datadome)
    async with AsyncSession(impersonate="chrome120", timeout=25) as s:
        r1 = await s.get(URL, allow_redirects=True)
        dd = [c for c in s.cookies.keys() if "datadome" in c.lower()]
        p(f"  A1 get#1: {r1.status_code}  cookies datadome={dd}  fetcher={'__UFRN_FETCHER__' in r1.text}")
        r2 = await s.get(URL, allow_redirects=True)
        p(f"  A1 get#2 (même session): {r2.status_code}  fetcher={'__UFRN_FETCHER__' in r2.text}")
    # A2 : warm la home d'abord, puis la recherche
    async with AsyncSession(impersonate="chrome120", timeout=25) as s:
        rh = await s.get(HOME, allow_redirects=True)
        rs = await s.get(URL, allow_redirects=True)
        p(f"  A2 home={rh.status_code} → search={rs.status_code}  fetcher={'__UFRN_FETCHER__' in rs.text}")
    # A3 : headers navigateur complets
    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
        "Sec-Fetch-Dest": "document", "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none", "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
        "Referer": "https://www.seloger.com/",
    }
    async with AsyncSession(impersonate="chrome120", timeout=25, headers=headers) as s:
        r = await s.get(URL, allow_redirects=True)
        p(f"  A3 headers complets: {r.status_code}  fetcher={'__UFRN_FETCHER__' in r.text}")
    # A4 : autres fingerprints (chrome124 / safari17_0 marchaient en 4G)
    for imp in ("chrome124", "safari17_0", "chrome116", "edge99"):
        try:
            async with AsyncSession(impersonate=imp, timeout=25, headers=headers) as s:
                r = await s.get(URL, allow_redirects=True)
                p(f"  A4 imp={imp:10s}: {r.status_code}  fetcher={'__UFRN_FETCHER__' in r.text}")
        except Exception as e:
            p(f"  A4 imp={imp:10s}: EXC {str(e)[:50]}")


async def test_camoufox(wait_until):
    from camoufox.async_api import AsyncCamoufox
    p(f"\n══ B) Camoufox — wait_until='{wait_until}' ════════════════════")
    import time
    t0 = time.time()
    try:
        async with AsyncCamoufox(headless=True, locale=["fr-FR"], os="windows") as b:
            ctx = await b.new_context(locale="fr-FR", viewport={"width": 1280, "height": 800})
            page = await ctx.new_page()
            try:
                await page.goto(URL, wait_until=wait_until, timeout=35000)
                p(f"  goto OK @ +{time.time()-t0:.0f}s")
            except Exception as e:
                p(f"  goto EXC @ +{time.time()-t0:.0f}s : {type(e).__name__}: {str(e)[:60]}")
            await asyncio.sleep(10)  # laisser le challenge JS se résoudre
            title = await page.title()
            html = await page.content()
            has = "__UFRN_FETCHER__" in html
            chal = ("captcha" in html.lower() or "datadome" in html.lower() or "geo.captcha" in html.lower())
            p(f"  +{time.time()-t0:.0f}s | titre={title!r} | html={len(html)} | fetcher={has} | challenge={chal}")
            # cherche un iframe captcha
            frames = [f.url for f in page.frames]
            cap = [u for u in frames if "captcha" in u.lower() or "datadome" in u.lower()]
            if cap: p(f"  iframe(s) captcha: {cap}")
            await b.close() if hasattr(b, "close") else None
    except Exception as e:
        p(f"  Camoufox EXC: {type(e).__name__}: {str(e)[:80]}")


async def main():
    p("="*64); p(" ENQUÊTE SELOGER (IP actuelle)"); p("="*64)
    await test_curl()
    await test_camoufox("domcontentloaded")  # ce que le bot fait aujourd'hui
    await test_camoufox("commit")            # hypothèse fix
    p("\nFini.")


if __name__ == "__main__":
    asyncio.run(main())

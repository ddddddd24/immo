"""Mesure data SeLoger débloqué — à lancer SUR IP MOBILE (hotspot 4G).

Fait un scrape SeLoger COMPLET avec enrichissement détail normal (prod), et
mesure la conso réelle pour dimensionner un proxy mobile facturé au Go.

Donne : Mo sur le run (corps + estimation sur le fil), nb d'annonces, nb de
pages détail, et extrapolation Mo/jour & Mo/mois SUR LE FIL au rythme prod
(SeLoger = 1 scrape / 10 min = 144/jour).

⚠️ À lancer branché en 4G (sinon SeLoger = 403 sur l'IP Box). Le PC doit router
par la 4G (couper le WiFi/Ethernet Box). Vérifie l'IP avec probe.py d'abord.

Usage : python tools/datadome_probe/measure_seloger.py
"""
import asyncio, os, sys, time
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
try:
    sys.stdout.reconfigure(encoding="utf-8"); sys.stderr.reconfigure(encoding="utf-8")
except Exception:
    pass

# decomp = corps décompressé ; wire = octets sur le fil (compressés)
ACC = {"D": 0, "Wknown": 0, "Dofw": 0, "n": 0, "nosize": 0,
       "n_search": 0, "n_detail": 0, "n_other": 0,
       "B_search": 0, "B_detail": 0, "B_other": 0,
       "L_httpx": 0, "L_curl": 0, "L_pw": 0}


def _cat(url):
    u = (url or "").lower()
    if "classified-search" in u:
        return "search"
    elif "seloger.com" in u:
        return "detail"
    return "other"


def _rec(url, decomp, wire, layer):
    ACC["n"] += 1
    ACC["L_" + layer] += 1
    cat = _cat(url)
    ACC["n_" + cat] += 1
    # octets attribués : fil si connu, sinon corps (pour la ventilation)
    b = wire if wire is not None else (decomp if decomp is not None else 0)
    ACC["B_" + cat] += b
    if decomp is not None:
        ACC["D"] += decomp
    if wire is not None:
        ACC["Wknown"] += wire
        if decomp is not None:
            ACC["Dofw"] += decomp
    if decomp is None and wire is None:
        ACC["nosize"] += 1


def _cl(headers):
    try:
        v = headers.get("content-length") or headers.get("Content-Length")
        return int(v) if v is not None and str(v).isdigit() else None
    except Exception:
        return None


# httpx
import httpx
_os = httpx.AsyncClient.send
async def _send(self, request, *a, **k):
    resp = await _os(self, request, *a, **k)
    decomp = wire = None
    try: decomp = len(resp.content)
    except Exception: pass
    try: wire = int(resp.num_bytes_downloaded)
    except Exception: pass
    _rec(str(request.url), decomp, wire, "httpx")
    return resp
httpx.AsyncClient.send = _send

# curl_cffi
from curl_cffi import requests as _ccr
def _meter(r):
    decomp = wire = None
    try: decomp = len(r.content)
    except Exception: pass
    wire = _cl(getattr(r, "headers", {}) or {})
    _rec(str(getattr(r, "url", "")), decomp, wire, "curl")
for _m in ("get", "post"):
    if hasattr(_ccr, _m):
        _o = getattr(_ccr, _m)
        def _mk(o):
            def w(*a, **k):
                r = o(*a, **k)
                try: _meter(r)
                except Exception: pass
                return r
            return w
        setattr(_ccr, _m, _mk(_o))
try:
    from curl_cffi.requests import AsyncSession
    for _m in ("get", "post", "request"):
        if hasattr(AsyncSession, _m):
            _o = getattr(AsyncSession, _m)
            def _mka(o):
                async def w(self, *a, **k):
                    r = await o(self, *a, **k)
                    try: _meter(r)
                    except Exception: pass
                    return r
                return w
            setattr(AsyncSession, _m, _mka(_o))
except Exception as e:
    print("warn AsyncSession:", e)

# Playwright/Camoufox
try:
    from playwright.async_api import BrowserContext
    _onp = BrowserContext.new_page
    async def _np(self, *a, **k):
        page = await _onp(self, *a, **k)
        def _r(resp):
            try: _rec(resp.url, None, _cl(resp.headers), "pw")
            except Exception: pass
        try: page.on("response", _r)
        except Exception: pass
        return page
    BrowserContext.new_page = _np
except Exception as e:
    print("warn Playwright:", e)

import config
import scraper


def mb(n): return n / 1_000_000


async def main():
    print("=" * 70)
    print(" MESURE DATA SELOGER (à lancer SUR 4G)")
    print("=" * 70)
    # garde-fou : prévenir si on est sur l'IP Box flaggée
    try:
        ip = httpx.get("https://api.ipify.org", timeout=8).text.strip()
        print(f" IP courante : {ip}", "⚠️ IP BOX FLAGGÉE — branche la 4G !" if ip == "86.247.64.52" else "")
    except Exception:
        pass
    print(" Scrape SeLoger complet (max_results=500, enrichissement détail)…\n")

    t0 = time.time()
    try:
        res = await asyncio.wait_for(
            scraper.search_listings(config.DEFAULT_SEARCH_SELOGER_URL, max_results=500),
            timeout=600)
        n = len(res)
    except asyncio.TimeoutError:
        n = -1
    except Exception as e:
        print("ERREUR scrape:", type(e).__name__, str(e)[:80]); n = -2
    dt = time.time() - t0

    D = ACC["D"]
    ratio = (ACC["Wknown"] / ACC["Dofw"]) if ACC["Dofw"] else 0.25
    wire_est = D * ratio                       # estimation fil (corps × ratio gzip mesuré)
    wire_known = ACC["Wknown"]                 # fil mesuré directement (Content-Length présents)
    wire = max(wire_est, wire_known)           # on prend le plus prudent
    calls_day = 86400 / 600                     # SeLoger = 1 scrape / 10 min

    print("=" * 70)
    print(" RÉSULTAT DU RUN")
    print("=" * 70)
    print(f"  Annonces récupérées      : {n}")
    print(f"  Requêtes HTTP totales    : {ACC['n']}  (search={ACC['n_search']}, "
          f"détail={ACC['n_detail']}, autre={ACC['n_other']})")
    print(f"  Couche                   : curl_cffi={ACC['L_curl']}, httpx={ACC['L_httpx']}, "
          f"navigateur={ACC['L_pw']}" + ("  ⚠️ Camoufox a fired (curl_cffi a 403 ?)" if ACC['L_pw'] else "  ✓ tout curl_cffi"))
    print(f"  Durée du run             : {dt:.0f}s")
    print(f"  Ratio gzip mesuré        : {ratio:.2f}  (fil/corps)")
    print(f"  Corps décompressé        : {mb(D):.1f} Mo")
    print(f"  SUR LE FIL (≈ coût proxy): {mb(wire):.1f} Mo  ← LE CHIFFRE DU RUN")
    print("-" * 70)
    print(f"  EXTRAPOLATION 24/7 (144 scrapes/jour), CONFIG ACTUELLE (enrichit tout) :")
    print(f"     {mb(wire * calls_day):8.0f} Mo/jour  |  {mb(wire * calls_day) * 30 / 1000:6.1f} Go/mois sur le fil")

    # ── Ventilation + estimation APRÈS TRIM (enrichir seulement le nouveau) ──────
    b_search, b_detail = ACC["B_search"], ACC["B_detail"]
    per_detail = (b_detail / ACC["n_detail"]) if ACC["n_detail"] else 0
    print("\n" + "=" * 70)
    print(" APRÈS TRIM (search à chaque scrape + détail SEULEMENT sur le nouveau)")
    print("=" * 70)
    print(f"  Search par scrape (toujours payé) : {mb(b_search):.2f} Mo "
          f"({ACC['n_search']} req)")
    print(f"  Détail : {mb(b_detail):.2f} Mo pour {ACC['n_detail']} pages "
          f"→ {per_detail/1000:.0f} Ko/page détail")
    print(f"  → conso/scrape = search + (nb_nouvelles × {per_detail/1000:.0f} Ko)")
    print("-" * 70)
    print(f"  {'nouvelles/scrape':>16s} {'Mo/scrape':>10s} {'Go/mois (fil)':>14s}")
    for n_new in (0, 5, 10, 20, 30):
        per = b_search + n_new * per_detail
        print(f"  {n_new:>16d} {mb(per):10.2f} {mb(per)*calls_day*30/1000:14.1f}")
    print("-" * 70)
    print("  NB : en régime établi, nb_nouvelles/scrape est faible (annonces SeLoger")
    print("       ≤1000€ IDF en 10 min). 24/7 = plafond ; réel < (cache + pauses).")


if __name__ == "__main__":
    asyncio.run(main())

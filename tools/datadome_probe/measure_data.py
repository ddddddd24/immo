"""Mesure de la conso data du scraper, par source — pour dimensionner un proxy.

Isolé : n'importe PAS main.py (pas de lock instance), ne modifie pas le code prod.
Monkeypatch les 3 couches HTTP pour mesurer la taille des réponses :
  - httpx       (wrap AsyncClient.send → len(resp.content))     → ~15 sources + enrich.
  - curl_cffi   (post/get + AsyncSession.* → len(r.content))    → LBC, SeLoger, wizi…
  - Playwright/Camoufox (new_page → page.on("response"), CL)    → laforet, guyhoquet…

Métrique = taille du CORPS décompressé des réponses (octets). C'est un MAJORANT
du coût proxy réel : sur le fil, le gzip divise le texte (JSON/HTML) par ~3-6.
Donc coût proxy réel ≈ chiffre ci-dessous / ~4 pour les sources texte.
Lance un scrape réel par source (pagination + enrichissement inclus), puis
extrapole en /jour et /mois via les fréquences de polling 24/7.

Usage : python tools/datadome_probe/measure_data.py
"""
import asyncio
import os
import sys
import time
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

try:
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
except Exception:
    pass

USAGE = defaultdict(lambda: {"bytes": 0, "n": 0, "nosize": 0})
CURRENT = {"src": "?"}


def _add(src, nbytes):
    u = USAGE[src]
    u["n"] += 1
    if nbytes is None:
        u["nosize"] += 1
    else:
        u["bytes"] += nbytes


def _cl(headers):
    try:
        v = headers.get("content-length") or headers.get("Content-Length")
        return int(v) if v is not None and str(v).isdigit() else None
    except Exception:
        return None


# ── 1) httpx : wrap send → taille du corps décompressé ────────────────────────
import httpx

_orig_send = httpx.AsyncClient.send
async def _send(self, request, *a, **k):
    resp = await _orig_send(self, request, *a, **k)
    try:
        _add(CURRENT["src"], len(resp.content))      # corps décompressé
    except Exception:
        _add(CURRENT["src"], _cl(resp.headers))      # streaming → repli CL
    return resp
httpx.AsyncClient.send = _send

# ── 2) curl_cffi : len(r.content) ─────────────────────────────────────────────
from curl_cffi import requests as _ccr

def _meter_curl(r):
    try:
        _add(CURRENT["src"], len(r.content))
    except Exception:
        _add(CURRENT["src"], _cl(getattr(r, "headers", {}) or {}))

for _m in ("get", "post"):
    if hasattr(_ccr, _m):
        _o = getattr(_ccr, _m)
        def _mk_sync(o):
            def w(*a, **k):
                r = o(*a, **k)
                try: _meter_curl(r)
                except Exception: pass
                return r
            return w
        setattr(_ccr, _m, _mk_sync(_o))

try:
    from curl_cffi.requests import AsyncSession
    for _m in ("get", "post", "request"):
        if hasattr(AsyncSession, _m):
            _o = getattr(AsyncSession, _m)
            def _mk_async(o):
                async def w(self, *a, **k):
                    r = await o(self, *a, **k)
                    try: _meter_curl(r)
                    except Exception: pass
                    return r
                return w
            setattr(AsyncSession, _m, _mk_async(_o))
except Exception as e:
    print("warn: patch AsyncSession:", e)

# ── 3) Playwright/Camoufox : page.on("response") → Content-Length ──────────────
try:
    from playwright.async_api import BrowserContext
    _orig_newpage = BrowserContext.new_page
    async def _newpage(self, *a, **k):
        page = await _orig_newpage(self, *a, **k)
        def _on_resp(resp):
            try:
                _add(CURRENT["src"], _cl(resp.headers))
            except Exception:
                pass
        try:
            page.on("response", _on_resp)
        except Exception:
            pass
        return page
    BrowserContext.new_page = _newpage
except Exception as e:
    print("warn: patch Playwright new_page:", e)

import config
import scraper

SOURCES = [
    ("LBC",               config.DEFAULT_SEARCH_URL,               300),
    ("SeLoger",           config.DEFAULT_SEARCH_SELOGER_URL,       600),
    ("Logic-Immo",        config.DEFAULT_SEARCH_LOGICIMMO_URL,     600),
    ("PAP",               config.DEFAULT_SEARCH_PAP_URL,           120),
    ("Bien'ici",          config.DEFAULT_SEARCH_BIENICI_URL,       180),
    ("Studapart",         config.DEFAULT_SEARCH_STUDAPART_URL,     180),
    ("Paris Attitude",    config.DEFAULT_SEARCH_PARISATTITUDE_URL,  180),
    ("Lodgis",            config.DEFAULT_SEARCH_LODGIS_URL,        180),
    ("ImmoJeune",         config.DEFAULT_SEARCH_IMMOJEUNE_URL,     180),
    ("LocService",        config.DEFAULT_SEARCH_LOCSERVICE_URL,    180),
    ("EntreParticuliers", config.DEFAULT_SEARCH_ENTREPARTICULIERS_URL, 240),
    ("L'Adresse",         config.DEFAULT_SEARCH_LADRESSE_URL,      300),
    ("Century 21",        config.DEFAULT_SEARCH_CENTURY21_URL,     240),
    ("Wizi",              config.DEFAULT_SEARCH_WIZI_URL,          120),
    ("Laforêt",           config.DEFAULT_SEARCH_LAFORET_URL,       240),
    ("Guy Hoquet",        config.DEFAULT_SEARCH_GUYHOQUET_URL,     240),
    ("Inli",              config.DEFAULT_SEARCH_INLI_URL,          180),
    ("CDC Habitat",       config.DEFAULT_SEARCH_CDC_URL,           300),
    ("FNAIM",             config.DEFAULT_SEARCH_FNAIM_URL,         300),
]
SENTINELS = [("LBC-sentinel", 180), ("PAP-sentinel", 75)]


def mb(n):
    return n / 1_000_000


async def main():
    print("=" * 72)
    print(" MESURE CONSO DATA — scrape réel par source (IP Box actuelle)")
    print("=" * 72)
    for label, url, interval in SOURCES:
        if not url:
            print(f"  - {label:18s} : pas d'URL, skip")
            continue
        CURRENT["src"] = label
        t0 = time.time()
        try:
            res = await asyncio.wait_for(scraper.search_listings(url, max_results=500), timeout=150)
            n = len(res)
        except asyncio.TimeoutError:
            n = -1
        except Exception as e:
            print(f"  - {label:18s} : ERREUR {type(e).__name__}: {str(e)[:50]}")
            n = -2
        u = USAGE[label]
        print(f"  - {label:18s} : {mb(u['bytes']):7.2f} Mo | {u['n']:4d} req | "
              f"{u['nosize']:3d} sans taille | {n} annonces | {time.time()-t0:4.0f}s")

    CURRENT["src"] = "LBC-sentinel"
    try: await scraper._lbc_sentinel_poll()
    except Exception: pass
    CURRENT["src"] = "PAP-sentinel"
    try: await scraper._fetch_pages_httpx([config.DEFAULT_SEARCH_PAP_URL])
    except Exception: pass

    print("\n" + "=" * 72)
    print(" CONSO PAR SCRAPE + EXTRAPOLATION 24/7  (corps décompressé)")
    print("=" * 72)
    freq = {lbl: itv for lbl, _, itv in SOURCES}
    sfreq = {lbl: itv for lbl, itv in SENTINELS}
    rows, day_total = [], 0.0
    for lbl in [s[0] for s in SOURCES]:
        per, calls = USAGE[lbl]["bytes"], 86400 / freq[lbl]
        day = per * calls; day_total += day
        rows.append((lbl, per, calls, day))
    for lbl in [s[0] for s in SENTINELS]:
        per, calls = USAGE[lbl]["bytes"], 86400 / sfreq[lbl]
        day = per * calls; day_total += day
        rows.append((lbl + " (poll)", per, calls, day))

    rows.sort(key=lambda r: -r[3])
    print(f"  {'source':22s} {'Mo/scrape':>10s} {'scr/j':>7s} {'Mo/jour':>9s}")
    for lbl, per, calls, day in rows:
        print(f"  {lbl:22s} {mb(per):10.3f} {calls:7.0f} {mb(day):9.1f}")

    wire = day_total / 4.0  # estimation octets sur le fil (gzip ~/4)
    print("\n" + "-" * 72)
    print(f"  DÉCOMPRESSÉ : {mb(day_total):8.0f} Mo/jour  |  {mb(day_total)*30/1000:6.1f} Go/mois")
    print(f"  WIRE (~/4)  : {mb(wire):8.0f} Mo/jour  |  {mb(wire)*30/1000:6.1f} Go/mois  ← coût proxy estimé")
    print("-" * 72)
    print("  NB : 24/7 = plafond (réel < : pauses jeu + cache scrape).")
    print("  NB : LBC/SeLoger en 403 (backoff) sur l'IP Box → SOUS-ESTIMÉS ici.")


if __name__ == "__main__":
    asyncio.run(main())

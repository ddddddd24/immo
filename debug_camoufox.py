"""Debug Camoufox-based scrapers in isolation.

Runs ONE source at a time (pool size = 1, no concurrency) with verbose
step-by-step logs from `_fetch_html_with_camoufox`. Lets us see exactly
where each source spends its time and identify hangs vs slow sites vs
parser bugs.

Usage:
    python debug_camoufox.py            # all problematic sources
    python debug_camoufox.py seloger    # one source
"""
import asyncio
import logging
import sys
import time

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

import config
from scraper import init_camoufox_pool, shutdown_camoufox_pool, search_listings

SOURCES = {
    "seloger":   (config.DEFAULT_SEARCH_SELOGER_URL, "SeLoger"),
    "studapart": (config.DEFAULT_SEARCH_STUDAPART_URL, "Studapart"),
    "lodgis":    (config.DEFAULT_SEARCH_LODGIS_URL, "Lodgis"),
    "bienici":   (config.DEFAULT_SEARCH_BIENICI_URL, "Bien'ici"),
    "immojeune": (config.DEFAULT_SEARCH_IMMOJEUNE_URL, "ImmoJeune"),
    "locservice":(config.DEFAULT_SEARCH_LOCSERVICE_URL, "LocService"),
}

DEFAULT_TARGETS = ["seloger", "studapart", "lodgis", "bienici", "immojeune", "locservice"]


async def main() -> None:
    if len(sys.argv) > 1:
        keys = [sys.argv[1].lower()]
    else:
        keys = DEFAULT_TARGETS

    print(f"Initializing Camoufox pool (size=1)…", flush=True)
    n = await init_camoufox_pool(size=1)
    print(f"Pool ready: {n} browser(s)\n", flush=True)

    results_summary: list[tuple[str, float, int | str]] = []

    for key in keys:
        if key not in SOURCES:
            print(f"!! Unknown source: {key}")
            continue
        url, label = SOURCES[key]
        if not url:
            print(f"=== {label}: SKIPPED (empty URL) ===\n")
            continue
        print(f"\n{'='*70}\n=== {label}: {url[:80]}\n{'='*70}", flush=True)
        t0 = time.time()
        try:
            listings = await asyncio.wait_for(
                search_listings(url, max_results=10), timeout=240,
            )
            elapsed = time.time() - t0
            print(f"\n>>> {label}: {len(listings)} listings in {elapsed:.1f}s\n", flush=True)
            results_summary.append((label, elapsed, len(listings)))
        except asyncio.TimeoutError:
            elapsed = time.time() - t0
            print(f"\n>>> {label}: TIMEOUT after {elapsed:.1f}s\n", flush=True)
            results_summary.append((label, elapsed, "TIMEOUT"))
        except Exception as exc:
            elapsed = time.time() - t0
            print(f"\n>>> {label}: FAILED after {elapsed:.1f}s: {type(exc).__name__}: {exc}\n", flush=True)
            results_summary.append((label, elapsed, f"FAIL:{type(exc).__name__}"))

    print("\n" + "="*70)
    print("SUMMARY")
    print("="*70)
    for label, elapsed, outcome in results_summary:
        print(f"  {label:15s} : {elapsed:6.1f}s  →  {outcome}")

    await shutdown_camoufox_pool()
    print("\nPool shut down. Done.")


if __name__ == "__main__":
    asyncio.run(main())

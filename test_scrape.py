"""Quick test: scrape real LBC listings then simulate one."""
import asyncio
import os
import sys

# Force real scraper (bypass MOCK_MODE for this test)
os.environ["MOCK_MODE"] = "false"
os.environ.setdefault("ANTHROPIC_API_KEY", "mock_key")
os.environ.setdefault("APIFY_API_KEY", "mock_key")
os.environ.setdefault("LBC_EMAIL", "mock")
os.environ.setdefault("LBC_PASSWORD", "mock")

import config
import scraper

SEARCH_URL = (
    "https://www.leboncoin.fr/recherche"
    "?category=10"
    "&real_estate_type=1,2"
    "&price=min-800"
    "&furnished=1"
    "&locations=Paris,Saint-Maur-des-Foss%C3%A9s,Maisons-Alfort,Vincennes,Charenton-le-Pont"
)

async def main():
    print("\n🔍 Scraping LeBonCoin avec tes critères (un navigateur va s'ouvrir)...\n")
    try:
        listings = await scraper.search_listings(SEARCH_URL, max_results=10)
    except Exception as e:
        print(f"❌ Erreur scraping : {e}")
        sys.exit(1)

    if not listings:
        print("❌ Aucune annonce trouvée — DataDome a peut-être bloqué.")
        sys.exit(1)

    print(f"✅ {len(listings)} annonces trouvées :\n")
    for i, l in enumerate(listings, 1):
        price = f"{l.price}€" if l.price else "N/A"
        print(f"  [{i}] {l.title}")
        print(f"      📍 {l.location}  💰 {price}  👤 {l.seller_name or 'inconnu'}")
        print(f"      🔗 {l.url}")
        print()

    # Pick first listing and show mock simulation
    pick = listings[0]
    print("─" * 60)
    print(f"🎯 Simulation sur : {pick.title}")
    print("─" * 60)

    # Detect seller type with heuristic (free)
    config.MOCK_MODE = True  # re-enable mock for message generation (no Claude key)
    from agent import _detect_seller_type
    from mock_data import MOCK_MESSAGES
    seller_type = _detect_seller_type(pick)
    message = MOCK_MESSAGES[seller_type]

    tone = "Séduction / storytelling" if seller_type == "particulier" else "Professionnel / factuel"
    print(f"👤 Type détecté : {seller_type}")
    print(f"🎭 Ton choisi   : {tone}")
    print(f"\n📝 MESSAGE :\n{'─'*40}")
    print(message)
    print("─" * 40)

asyncio.run(main())

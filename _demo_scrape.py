# -*- coding: utf-8 -*-
"""Demo: scrape une vraie annonce LBC et génère le message Claude."""
import asyncio
import os
import sys

# Force UTF-8 output on Windows
sys.stdout.reconfigure(encoding="utf-8")

# Stub vars not needed for this demo
for k, v in {
    "TELEGRAM_BOT_TOKEN": "stub",
    "TELEGRAM_CHAT_ID":   "stub",
    "APIFY_API_KEY":      "stub",
    "LBC_EMAIL":          "stub@stub.com",
    "LBC_PASSWORD":       "stub",
    "ANTHROPIC_API_KEY":  "stub",
}.items():
    os.environ.setdefault(k, v)

# Real scraping, mock message generation (no API cost)
os.environ["MOCK_MODE"] = "false"
import config
import scraper
from scraper import is_real_offer
from agent import analyse_listing

SEARCH_URL = (
    "https://www.leboncoin.fr/recherche"
    "?category=10"
    "&real_estate_type=1,2"
    "&price=min-1000"
    "&furnished=1"
    "&square=25-max"
    "&locations=Paris,Boulogne-Billancourt,Neuilly-sur-Seine,Levallois-Perret,Clichy,"
    "Issy-les-Moulineaux,Montrouge,Malakoff,Vanves,Ivry-sur-Seine,"
    "Le%20Kremlin-Bic%C3%AAtre,Charenton-le-Pont,Saint-Mand%C3%A9,Vincennes,"
    "Montreuil,Bagnolet,Pantin,Saint-Ouen,Aubervilliers,"
    "Alfortville,Maisons-Alfort,Saint-Maur-des-Foss%C3%A9s"
)


async def main():
    print("🔍 Scraping LeBonCoin (navigateur va s'ouvrir)...\n")
    listings = await scraper.search_listings(SEARCH_URL, max_results=30)

    if not listings:
        print("❌ Aucune annonce trouvée — DataDome a peut-être bloqué.")
        return

    offers = [l for l in listings if is_real_offer(l)]
    print(f"✅ {len(listings)} annonces trouvées, {len(offers)} vraies offres\n")
    for i, l in enumerate(offers, 1):
        print(f"  [{i}] {l.title} — {l.location} — {l.price}€")
    print()

    if not offers:
        print("❌ Aucune vraie offre trouvée.")
        return

    # Fetch full details (description) for the first real offer
    first = offers[0]
    print(f"🔎 Récupération des détails de : {first.title}...")
    full = await scraper.fetch_single_listing(first.url)
    pick = full if full and full.description else first
    print("═" * 60)
    print(f"ANNONCE SÉLECTIONNÉE : {pick.title}")
    print(f"Lieu     : {pick.location}")
    print(f"Prix     : {pick.price}€/mois")
    print(f"Vendeur  : {pick.seller_name}")
    print(f"URL      : {pick.url}")
    print(f"\nDescription (400 premiers caractères) :")
    print((pick.description or "")[:400])
    print("═" * 60)

    # Switch to mock mode for message generation (no API cost)
    config.MOCK_MODE = True
    print("\n⚙️  Génération du message (mode template, sans API)...\n")
    result = await analyse_listing(pick)

    print(f"👤 Type détecté : {result.seller_type.upper()}")
    print(f"🎭 Ton choisi   : {result.tone}")
    print()
    print("📝 MESSAGE QUI SERAIT ENVOYÉ :")
    print("─" * 50)
    print(result.message)
    print("─" * 50)
    print(f"📊 Longueur : {len(result.message.split())} mots")


asyncio.run(main())

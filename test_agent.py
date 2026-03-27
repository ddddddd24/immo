"""
Simulation mode demo — runs agent.py against two fake listings
(one particulier, one agence) and prints the full formatted output.

Requires a real ANTHROPIC_API_KEY in .env.
Does NOT need Telegram, Apify, or Playwright credentials.
"""
import asyncio
import os
import sys

# ── minimal env stub so config.py doesn't crash on missing Telegram/Apify keys ──
_STUB_VARS = {
    "TELEGRAM_BOT_TOKEN": "stub",
    "TELEGRAM_CHAT_ID":   "stub",
    "APIFY_API_KEY":      "stub",
    "LBC_EMAIL":          "stub@stub.com",
    "LBC_PASSWORD":       "stub",
}
for k, v in _STUB_VARS.items():
    os.environ.setdefault(k, v)

# Now safe to import project modules
from agent import Listing, analyse_listing, format_simulation_text  # noqa: E402


# ─── Fake listings ────────────────────────────────────────────────────────────

FAKE_PARTICULIER = Listing(
    lbc_id="99999001",
    title="Studio meublé 28m² - Paris 11e - Balcon",
    description=(
        "Bonjour, je loue mon studio meublé de 28m² situé rue de la Roquette dans le 11e arrondissement. "
        "L'appartement est entièrement équipé : lit double, canapé, cuisine équipée, machine à laver. "
        "Il dispose d'un joli balcon avec vue dégagée. Idéal pour un jeune actif. "
        "Loyer 790€ charges comprises (eau, internet). Disponible à partir de septembre 2026. "
        "Caution = 1 mois. Pas de fumeurs, pas d'animaux svp. Contactez-moi pour une visite !"
    ),
    price=790,
    location="Paris 11e",
    seller_name="Marie Dupont",
    url="https://www.leboncoin.fr/annonces/99999001.htm",
    seller_type_hint="private",
)

FAKE_AGENCE = Listing(
    lbc_id="99999002",
    title="T2 meublé 35m² Maisons-Alfort - Proche métro",
    description=(
        "Agence Immo Plus vous propose ce T2 meublé de 35m² à Maisons-Alfort, "
        "à 5 min à pied du métro École Vétérinaire. "
        "Appartement en bon état : séjour, chambre séparée, cuisine équipée, salle de bain. "
        "Loyer 800€ hors charges. Charges locatives estimées à 80€/mois. "
        "Disponible immédiatement. Dossier solide exigé (3x le loyer). "
        "Visite sur rendez-vous uniquement. Agence Immo Plus - 01 23 45 67 89"
    ),
    price=800,
    location="Maisons-Alfort (94)",
    seller_name="Agence Immo Plus",
    url="https://www.leboncoin.fr/annonces/99999002.htm",
    seller_type_hint="pro",
)


# ─── Runner ───────────────────────────────────────────────────────────────────

async def run_simulation(listing: Listing, label: str) -> None:
    print(f"\n{'═' * 60}")
    print(f"  TEST : {label}")
    print(f"{'═' * 60}")

    result = await analyse_listing(listing)

    # Terminal-friendly version of the Telegram card
    print(f"\n🔍 ANALYSE ANNONCE")
    print(f"📍 {result.listing.title}")
    print(f"📍 {result.listing.location}")
    print(f"💰 {result.listing.price} €/mois")
    print(f"🔗 {result.listing.url}")
    print()
    type_emoji = "👤" if result.seller_type == "particulier" else "🏢"
    print(f"{type_emoji} Type détecté : {result.seller_type.upper()}")
    print(f"🎭 Ton choisi  : {result.tone}")
    print()
    print("📝 MESSAGE QUI SERAIT ENVOYÉ :")
    print("─" * 50)
    print(result.message)
    print("─" * 50)
    print()
    word_count = len(result.message.split())
    print(f"📊 Longueur : {word_count} mots")

    # Also show Telegram-formatted card
    print("\n--- Format Telegram (Markdown) ---")
    print(format_simulation_text(result))


async def main() -> None:
    print("=" * 60)
    print("  LeBonCoin Bot — Test simulation mode")
    print("  Deux annonces fictives : particulier + agence")
    print("=" * 60)

    await run_simulation(FAKE_PARTICULIER, "Particulier (studio Paris 11e)")
    await run_simulation(FAKE_AGENCE,      "Agence (T2 Maisons-Alfort)")

    print(f"\n{'=' * 60}")
    print("  ✅ Simulation terminée — imports OK, agent fonctionnel")
    print(f"{'=' * 60}\n")


if __name__ == "__main__":
    asyncio.run(main())

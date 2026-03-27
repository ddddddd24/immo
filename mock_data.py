"""Fake listings and pre-written messages used when MOCK_MODE=true."""
from agent import Listing

# ─── Fake listings ────────────────────────────────────────────────────────────

MOCK_LISTINGS = [
    Listing(
        lbc_id="mock_001",
        title="Studio meublé 28m² – Paris 11e – Balcon",
        description=(
            "Bonjour, je loue mon studio entièrement meublé de 28m² situé rue de la Roquette. "
            "Lit double, canapé, cuisine équipée, machine à laver. Joli balcon avec vue dégagée. "
            "Idéal jeune actif. Loyer 790€ charges comprises. Disponible septembre 2026."
        ),
        price=790,
        location="Paris 11e (75011)",
        seller_name="Marie Dupont",
        url="https://www.leboncoin.fr/annonces/mock_001.htm",
        seller_type_hint="private",
    ),
    Listing(
        lbc_id="mock_002",
        title="T2 meublé 35m² – Maisons-Alfort – Proche métro",
        description=(
            "Agence Immo Plus vous propose ce T2 meublé de 35m² à Maisons-Alfort. "
            "5 min à pied du métro École Vétérinaire. Séjour, chambre séparée, cuisine équipée. "
            "Loyer 800€ HC. Charges ~80€/mois. Dossier solide exigé (3x le loyer)."
        ),
        price=800,
        location="Maisons-Alfort (94700)",
        seller_name="Agence Immo Plus",
        url="https://www.leboncoin.fr/annonces/mock_002.htm",
        seller_type_hint="pro",
    ),
    Listing(
        lbc_id="mock_003",
        title="F1 meublé 26m² – Saint-Maur-des-Fossés",
        description=(
            "Je mets en location mon F1 meublé lumineux au 3e étage avec ascenseur. "
            "Parquet, double vitrage, cave. Quartier calme, commerces à 2 min. "
            "Loyer 750€ charges comprises (eau froide + internet). Libre début septembre."
        ),
        price=750,
        location="Saint-Maur-des-Fossés (94100)",
        seller_name="Pierre Martin",
        url="https://www.leboncoin.fr/annonces/mock_003.htm",
        seller_type_hint="private",
    ),
    Listing(
        lbc_id="mock_004",
        title="Studio 30m² meublé – Vincennes – Terrasse",
        description=(
            "Nexity Vincennes – Studio haut de gamme meublé, 30m², terrasse 8m². "
            "Immeuble récent, gardien, digicode, parking vélo. "
            "Loyer 795€ CC. Disponible immédiatement. "
            "Nous contacter pour constituer votre dossier."
        ),
        price=795,
        location="Vincennes (94300)",
        seller_name="Nexity Vincennes",
        url="https://www.leboncoin.fr/annonces/mock_004.htm",
        seller_type_hint="pro",
    ),
    Listing(
        lbc_id="mock_005",
        title="Appartement meublé 32m² – Charenton-le-Pont",
        description=(
            "Loue appartement meublé refait à neuf, 32m², 1er étage, clair. "
            "Cuisine ouverte équipée, salle de bain moderne, rangements. "
            "Loyer 780€ charges incluses. RER A Charenton à 5 min. "
            "Cherche locataire sérieux, disponible octobre 2026."
        ),
        price=780,
        location="Charenton-le-Pont (94220)",
        seller_name="Sophie Leblanc",
        url="https://www.leboncoin.fr/annonces/mock_005.htm",
        seller_type_hint="private",
    ),
]

# ─── Template-based mock messages (no API call, uses real listing data) ──────

def generate_mock_message(listing, seller_type: str) -> str:
    """Generate a personalized message from listing data without calling Claude."""
    title = listing.title or "votre bien"
    location = listing.location or "ce secteur"
    price = f"{listing.price} €" if listing.price else "le loyer indiqué"

    # Pick a specific detail to mention from the title/description
    desc = (listing.description or "").lower()
    title_lower = title.lower()
    if "balcon" in desc or "balcon" in title_lower:
        detail = "le balcon m'a particulièrement attiré"
    elif "terrasse" in desc or "terrasse" in title_lower:
        detail = "la terrasse est exactement ce que je cherche"
    elif "meublé" in desc or "meublé" in title_lower:
        detail = "le fait qu'il soit meublé correspond parfaitement à ma situation"
    elif "lumineux" in desc or "clair" in desc:
        detail = "l'appartement lumineux correspond bien à ce que je cherche"
    else:
        detail = f"la localisation à {location} m'intéresse vraiment"

    if seller_type == "particulier":
        return (
            "Bonjour,\n\n"
            f"Je m'appelle Illan, j'ai 26 ans et je suis alternant Product Owner chez SNCF Voyageurs. "
            f"Mes parents sont à la retraite à Maisons-Alfort et je cherche à leur laisser de l'espace "
            f"pour profiter sereinement de cette période.\n\n"
            f"En voyant votre annonce, {detail} — c'est exactement le type de logement que je recherche. "
            f"Je suis locataire sérieux, situation stable, dossier complet.\n\n"
            f"Le bien est-il toujours disponible ? Les charges sont-elles comprises dans les {price} ? "
            f"Et serait-il disponible à partir de septembre 2026 ?\n\n"
            "Seriez-vous disponible pour une visite ?\n\n"
            "Cordialement,\nIllan Krief"
        )
    else:
        return (
            "Bonjour,\n\n"
            f"Votre annonce m'intéresse — {detail}. Le bien est-il toujours disponible ?\n\n"
            "Je suis Illan Krief, 26 ans, alternant Product Owner chez SNCF Voyageurs "
            "(grande entreprise, situation stable). Je perçois 1 850 €/mois et mon dossier "
            "est complet et disponible immédiatement. Je souhaite emménager en septembre 2026, "
            "date à laquelle mon revenu sera complété par celui de ma compagne pacsée (~800 €/mois).\n\n"
            f"Pourriez-vous me préciser le montant des charges et confirmer que le bien serait "
            "disponible à cette période ? Je suis disponible pour une visite selon vos créneaux.\n\n"
            "Cordialement,\nIllan Krief"
        )


# Keep for backward compatibility with test_scrape.py
MOCK_MESSAGES = {
    "particulier": generate_mock_message(MOCK_LISTINGS[0], "particulier"),
    "agence":      generate_mock_message(MOCK_LISTINGS[1], "agence"),
}

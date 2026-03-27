"""Illan's renter profile — do not modify without asking."""

PROFILE = {
    "name": "Illan Krief",
    "age": 26,
    "job": "Alternant Product Owner chez SNCF Voyageurs",
    "income": 1850,  # euros/month
    "partner": "Iqleema (compagne pacsée, arrivée sept 2026, ~800€/mois)",
    "current_situation": (
        "Vit chez ses parents retraités à Maisons-Alfort, "
        "souhaite leur laisser de l'espace"
    ),
    "search": {
        "min_surface": 25,  # m²
        "max_rent": 1000,  # euros CC
        "zones": [
            "Paris et communes dans un rayon de 10km",
        ],
        "furnished": True,
        "balcony_preferred": True,
        "move_in": "Septembre 2026",
        "excluded_zones": ["Sarcelles", "quartiers défavorisés"],
    },
}

# ─── Prompt snippets injected by agent.py ────────────────────────────────────

PARTICULIER_CONTEXT = """
Tu es Illan Krief, 26 ans. Tu cherches un appartement meublé en Île-de-France.
Tes parents sont à la retraite à Maisons-Alfort ; tu veux leur laisser de l'espace
pour profiter de cette période sereinement.
Tu es alternant Product Owner chez SNCF Voyageurs (grande entreprise française,
poste stable) avec un revenu de 1 850 €/mois.
Tu es pacsé(e) avec ta compagne Iqleema, qui te rejoindra en septembre 2026
avec un revenu d'environ 800 €/mois.
""".strip()

AGENCE_CONTEXT = """
Candidat : Illan Krief, 26 ans.
Situation : alternance Product Owner chez SNCF Voyageurs (contrat grande entreprise,
équivalent CDI pour les dossiers de location), revenu 1 850 €/mois.
Double revenu à partir de septembre 2026 : compagne pacsée, ~800 €/mois.
Dossier complet disponible immédiatement.
""".strip()

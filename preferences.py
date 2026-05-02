"""Illan's apartment-hunt preferences — used by agent.score_listing.

Structured to be tweakable without touching code. Three layers of priority:

  DEALBREAKERS  → instant 0/10, no LLM call (saves tokens).
  MUST_HAVE     → score capped at 4/10 if absent.
  PREFERRED     → bonuses applied via LLM scoring with weight hints.

The LLM prompt is built dynamically from this file so any edit here
takes effect immediately on the next `/campagne` (no bot restart needed
for prompt changes — only env var changes need a restart).

EDIT THIS FILE to refine your scoring as you learn what you actually want
during the search. Common adjustments:
  - Move a feature from PREFERRED to MUST_HAVE if it becomes critical
  - Add a neighborhood to ZONES_AVOID if you visit one and don't like it
  - Adjust max_commute_minutes if you change job locations
"""

# ─── Hard constraints (instant rejection) ────────────────────────────────────

DEALBREAKERS_HOUSING_TYPE = {
    # We want a private apartment for Illan + Iqleema (sept 2026 onwards).
    # No shared situation with a third person.
    "coloc",
    "coliving",
    "chambre",
}

DEALBREAKERS_KEYWORDS = [
    # Title/description matches → instant 0/10 (case-insensitive substring).
    # Keep this list short — only put things that should NEVER be contacted.
    "sous-location",
    "sous loc",
    "courte durée",
    "courte duree",
    "saisonnier",
    "stage",
    "uniquement femme",
    "uniquement homme",
]

# Maximum number of co-residents (counts the user). 2 = Illan + Iqleema only.
MAX_RESIDENTS = 2


# ─── Must-have (heavy negative if absent) ────────────────────────────────────

MUST_HAVE = [
    # Empty for now — Illan didn't flag any absolute must. Lave-linge and
    # ascenseur are PREFERRED (below) but not blocking.
]


# ─── Preferred (positive, weighted 1=mild, 3=strong) ─────────────────────────

PREFERRED_FEATURES = [
    {"feature": "balcon",       "weight": 3, "synonyms": ["terrasse", "loggia"]},
    {"feature": "lave-linge",   "weight": 2, "synonyms": ["machine à laver", "lave linge"]},
    {"feature": "lumineux",     "weight": 2, "synonyms": ["clair", "ensoleillé", "exposition sud", "exposé sud"]},
    {"feature": "rénové",       "weight": 2, "synonyms": ["refait à neuf", "neuf", "refait"]},
    {"feature": "calme",        "weight": 2, "synonyms": ["sur cour", "rue calme", "tranquille"]},
    {"feature": "ascenseur",    "weight": 1, "synonyms": []},
    {"feature": "proche métro", "weight": 2, "synonyms": ["proche métro", "métro à pied", "à 2 min du métro", "5 min métro"]},
    {"feature": "cuisine équipée", "weight": 1, "synonyms": ["cuisine equipée", "cuisine américaine"]},
    {"feature": "meublé",       "weight": 1, "synonyms": ["meuble"]},  # Slight preference, non-blocking
    {"feature": "fibre",        "weight": 1, "synonyms": ["internet inclus", "wifi inclus"]},
]


# ─── Zones — Saint-Denis commute target (~1h max porte-à-porte) ──────────────

# Zones SAFE et bien connectées à Saint-Denis. Bonus de score si l'annonce
# est dans une de ces zones. Le LLM utilise ces noms pour matcher la
# location du listing (texte libre).
ZONES_PREFERRED = [
    # Paris arrondissements considérés sûrs et avec bon accès Saint-Denis
    {"zone": "Paris 13", "weight": 3, "match": ["paris 13", "75013", "place d'italie", "tolbiac"]},
    {"zone": "Paris 12", "weight": 3, "match": ["paris 12", "75012", "nation", "bercy", "ledru-rollin"]},
    {"zone": "Paris 11", "weight": 3, "match": ["paris 11", "75011", "république", "oberkampf", "bastille"]},
    {"zone": "Paris 15", "weight": 2, "match": ["paris 15", "75015", "convention", "vaugirard"]},
    {"zone": "Paris 14", "weight": 2, "match": ["paris 14", "75014", "denfert", "alésia"]},
    {"zone": "Paris 17", "weight": 2, "match": ["paris 17", "75017", "wagram", "monceau", "ternes"]},
    {"zone": "Paris 9", "weight": 2, "match": ["paris 9", "75009", "saint-georges", "trinité"]},
    {"zone": "Paris 10", "weight": 1, "match": ["paris 10", "75010", "gare de l'est", "gare du nord"]},
    {"zone": "Paris 5/6/7", "weight": 2, "match": ["paris 5", "paris 6", "paris 7", "75005", "75006", "75007"]},
    {"zone": "Paris 1/2/3/4", "weight": 2, "match": ["paris 1", "paris 2", "paris 3", "paris 4", "75001", "75002", "75003", "75004", "marais", "louvre"]},
    {"zone": "Paris 8", "weight": 2, "match": ["paris 8", "75008"]},
    {"zone": "Paris 16", "weight": 2, "match": ["paris 16", "75016"]},
    # 92 — Hauts-de-Seine (très safe, bon accès via ligne 13 / RER C / RER A)
    {"zone": "Boulogne-Billancourt", "weight": 2, "match": ["boulogne-billancourt", "92100"]},
    {"zone": "Issy-les-Moulineaux", "weight": 2, "match": ["issy-les-moulineaux", "92130"]},
    {"zone": "Levallois-Perret", "weight": 2, "match": ["levallois-perret", "92300"]},
    {"zone": "Neuilly-sur-Seine", "weight": 2, "match": ["neuilly-sur-seine", "92200"]},
    {"zone": "Clichy", "weight": 2, "match": ["clichy", "92110"]},  # ligne 13 directe → St-Denis
    {"zone": "Asnières", "weight": 2, "match": ["asnières", "92600"]},
    {"zone": "Courbevoie", "weight": 2, "match": ["courbevoie", "92400"]},
    {"zone": "Montrouge", "weight": 2, "match": ["montrouge", "92120"]},
    {"zone": "Vanves", "weight": 2, "match": ["vanves", "92170"]},
    {"zone": "Malakoff", "weight": 1, "match": ["malakoff", "92240"]},
    # 94 — Val-de-Marne (safe global, bon RER A / ligne 1 / RER C)
    {"zone": "Vincennes", "weight": 3, "match": ["vincennes", "94300"]},
    {"zone": "Saint-Mandé", "weight": 3, "match": ["saint-mandé", "94160"]},
    {"zone": "Charenton-le-Pont", "weight": 3, "match": ["charenton", "94220"]},
    {"zone": "Maisons-Alfort", "weight": 2, "match": ["maisons-alfort", "94700"]},
    {"zone": "Saint-Maur-des-Fossés", "weight": 2, "match": ["saint-maur", "94100"]},
    {"zone": "Alfortville", "weight": 2, "match": ["alfortville", "94140"]},
    {"zone": "Le Kremlin-Bicêtre", "weight": 2, "match": ["kremlin-bicêtre", "94270"]},
    {"zone": "Ivry-sur-Seine", "weight": 2, "match": ["ivry-sur-seine", "94200"]},
]

# Zones avec sécurité perçue plus faible OU mauvais commute. Pénalité de
# score si listing y tombe. Pas un dealbreaker — l'utilisateur peut quand
# même décider de visiter, juste un signal négatif.
ZONES_AVOID = [
    {"zone": "Paris 18 (parties Nord)", "weight": -2, "match": [
        "porte de clignancourt", "porte de la chapelle", "porte d'aubervilliers",
        "barbès", "marx dormoy", "la chapelle",
    ]},
    {"zone": "Paris 19 (parties Nord/Est)", "weight": -2, "match": [
        "porte de la villette", "porte de pantin", "stalingrad",
    ]},
    {"zone": "Paris 20 (parties Est)", "weight": -1, "match": [
        "porte de bagnolet", "porte de montreuil",
    ]},
    {"zone": "93 hors Saint-Denis centre", "weight": -2, "match": [
        "aubervilliers", "la courneuve", "sevran", "drancy", "stains",
        "bobigny", "epinay-sur-seine", "saint-ouen",  # Saint-Ouen on the fence
    ]},
    {"zone": "95 lointain", "weight": -3, "match": [
        "sarcelles", "garges", "argenteuil", "goussainville",
    ]},
]


# ─── Commute target ──────────────────────────────────────────────────────────

WORK_LOCATION = "Saint-Denis"
MAX_COMMUTE_MINUTES = 70   # 1h, with 1h10 acceptable


# ─── Couple status (impacts what types of listings make sense) ───────────────

LIVING_AS = "couple_from_2026_09"   # Illan solo until sept 2026, then 2-person


# ─── Negotiable budget cap ───────────────────────────────────────────────────

# Hard cap is in profile.search.max_rent (1000€). This is a soft cap — if
# something is exceptional, the LLM can recommend it slightly above.
SOFT_BUDGET_CAP = 1080


def build_prompt_block() -> str:
    """Return a compact French description of preferences for the LLM prompt."""
    pref_lines = []
    for p in PREFERRED_FEATURES:
        syns = " / ".join(p["synonyms"]) if p["synonyms"] else ""
        pref_lines.append(
            f"  - {p['feature']}{' (synonymes: ' + syns + ')' if syns else ''} (poids {p['weight']})"
        )
    zones_pref_lines = [
        f"  - {z['zone']} (poids +{z['weight']})" for z in ZONES_PREFERRED
    ]
    zones_avoid_lines = [
        f"  - {z['zone']} (pénalité {z['weight']})" for z in ZONES_AVOID
    ]
    return f"""
Profil :
- Couple (Illan + Iqleema dès sept 2026) — uniquement 2 personnes
- Travail à {WORK_LOCATION}, commute max ~{MAX_COMMUTE_MINUTES} min porte-à-porte
- Budget hard cap 1000€ CC, soft cap {SOFT_BUDGET_CAP}€ si exceptionnel

Caractéristiques préférées (avec poids) :
{chr(10).join(pref_lines)}

Quartiers préférés (bonus de score) :
{chr(10).join(zones_pref_lines)}

Quartiers à éviter (pénalité de score) :
{chr(10).join(zones_avoid_lines)}
""".strip()


def is_dealbreaker(*, housing_type: str, roommate_count, title: str, description: str) -> tuple[bool, str]:
    """Pre-filter check before any LLM call. Returns (yes, reason)."""
    if housing_type in DEALBREAKERS_HOUSING_TYPE:
        return True, f"type={housing_type} (Illan veut un appartement privé pour 2)"
    if roommate_count and roommate_count > MAX_RESIDENTS:
        return True, f"roommate_count={roommate_count} > {MAX_RESIDENTS}"
    blob = f"{title} {description}".lower()
    for kw in DEALBREAKERS_KEYWORDS:
        if kw.lower() in blob:
            return True, f"keyword={kw!r}"
    return False, ""

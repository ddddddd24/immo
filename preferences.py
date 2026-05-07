"""Illan's apartment-hunt preferences + scoring algorithm v2.

Score = 0.35·price/value + 0.30·zone + 0.25·commute + 0.10·features
applied AFTER hard dealbreakers (price>1050€, zone critique, étage>3 sans
ascenseur, dispo après sept 2026, coloc>2, etc.) which set score=0.

Edit DATA here (zones, features, weights, hard caps). The orchestration
(LLM call + final combination) lives in agent.score_listings_batch.
"""

import datetime as _dt
import re as _re_pref

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
    # Non-meublé / vide → Illan veut meublé. Match seulement si EXPLICITE.
    # ⚠️  Ne pas mettre "vide" tout court — trop de faux positifs.
    "non meublé",
    "non-meublé",
    "non meuble",
    "non-meuble",
    "loué vide",
    "loue vide",
    "location vide",
    "à louer vide",
    "a louer vide",
    "appartement vide",
    "non furnished",
    "unfurnished",
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


# ─── Hard price cap (above this → score=0, dealbreaker) ─────────────────────
HARD_PRICE_CAP = 1100  # euros CC. Hard rule: nothing above survives scoring.
SOFT_BUDGET_CAP = 1000  # Above this score is reduced but not zero.

# ─── Move-in date latest acceptable ──────────────────────────────────────────
# Available date AFTER this → dealbreaker. Available BEFORE is fine — they
# negotiate the start date. Format: YYYY-MM-DD.
MOVE_IN_DATE_LATEST = _dt.date(2026, 9, 30)


# ─── Critical avoid zones (hard dealbreaker, score=0) ────────────────────────
# Subset of ZONES_AVOID with weight ≤ -2 — too unsafe / too far to even score.
# 95 lointain (weight=-3) AND 93 hors St-Denis centre (weight=-2) per Illan.
CRITICAL_AVOID_KEYWORDS = [
    # 95 lointain
    "sarcelles", "garges", "argenteuil", "goussainville",
    # 93 hors Saint-Denis centre
    "aubervilliers", "la courneuve", "sevran", "drancy", "stains",
    "bobigny", "epinay-sur-seine", "épinay-sur-seine",
]


# ─── Commute mapping: minutes from zone → Saint-Denis ────────────────────────
# Used as deterministic shortcut; LLM only consulted for unknown zones.
COMMUTE_MAP = {
    # Paris arrondissements (zip codes)
    "75001": 30, "75002": 25, "75003": 25, "75004": 30, "75005": 40,
    "75006": 40, "75007": 45, "75008": 25, "75009": 20, "75010": 18,
    "75011": 28, "75012": 35, "75013": 45, "75014": 45, "75015": 45,
    "75016": 40, "75017": 22, "75018": 12, "75019": 22, "75020": 30,
    # 92 — Hauts-de-Seine (proche)
    "92100": 55, "92110": 25, "92120": 50, "92130": 50, "92170": 50,
    "92200": 28, "92240": 50, "92300": 22, "92400": 28, "92600": 30,
    # 93 — Saint-Denis et alentours (très proche)
    "93200": 5,   # Saint-Denis
    "93300": 12,  # Aubervilliers (DEALBREAKER mais mapping utile)
    "93800": 15,  # Épinay (DEALBREAKER)
    # 94 — Val-de-Marne (RER A/D, ~45-65 min)
    "94100": 60, "94120": 65, "94140": 55, "94160": 45, "94170": 60,
    "94200": 50, "94220": 45, "94270": 45, "94300": 40, "94340": 65,
    "94700": 55,
}


# ─── Subscore weights (must sum to 1.0) ──────────────────────────────────────
SCORE_WEIGHTS = {
    "price_value": 0.35,
    "zone":        0.30,
    "commute":     0.25,
    "features":    0.10,
}


# ─── Scoring helpers (deterministic, no LLM) ─────────────────────────────────

def is_critical_zone(*, location: str, title: str = "", description: str = "") -> tuple[bool, str]:
    """Return (True, zone_label) if listing is in a CRITICAL_AVOID zone.

    Two checks:
    1. Explicit city-name keywords (CRITICAL_AVOID_KEYWORDS).
    2. Zip-prefix check for far departments. 77 (Seine-et-Marne), 78 (Yvelines),
       91 (Essonne), 95 (Val-d'Oise) are all >50min from Saint-Denis by transit
       and not in our ZONES_PREFERRED. Any zip in those departments is a
       dealbreaker UNLESS the zip is explicitly whitelisted in COMMUTE_MAP
       (which would mean we accepted it as close enough).
    """
    blob = f"{location or ''} {title or ''} {description or ''}".lower()
    for kw in CRITICAL_AVOID_KEYWORDS:
        if kw in blob:
            return True, kw
    # Zip-prefix dealbreaker: location often "Ville, 77123"
    zm = _re_pref.search(r"\b(\d{5})\b", location or "")
    if zm:
        zip_code = zm.group(1)
        if zip_code[:2] in ("77", "78", "91", "95") and zip_code not in COMMUTE_MAP:
            dept_names = {"77": "Seine-et-Marne", "78": "Yvelines",
                          "91": "Essonne", "95": "Val-d'Oise"}
            return True, f"{dept_names[zip_code[:2]]} {zip_code} — trop loin de St-Denis"
    return False, ""


def price_value_score(price, surface) -> float:
    """0-10 from m²/€ ratio. 'Wow' = 25-30m² for 900-1000€ (ratio 0.025-0.033)."""
    if not price or not surface or price <= 0 or surface <= 0:
        return 5.0  # neutral when info missing
    ratio = surface / price  # m² per €
    # Linear: 0.018 → 4, 0.033 → 10
    score = 4 + (ratio - 0.018) * 400
    return max(0.0, min(10.0, score))


def zone_match_score(location: str) -> tuple[float, str]:
    """0-10 from preferences. Word-boundary matching to avoid 'paris 1' matching 'paris 19'."""
    if not location:
        return 5.0, "zone inconnue"
    blob = location.lower()
    def _matches(needle: str) -> bool:
        # Word-boundary check: needle must be surrounded by non-word chars (or string ends)
        pat = r"\b" + _re_pref.escape(needle.lower()) + r"\b"
        return _re_pref.search(pat, blob) is not None
    # Most specific match first (preferred)
    for z in ZONES_PREFERRED:
        for m in z.get("match", []):
            if _matches(m):
                return 5 + 5 * z["weight"] / 3, z["zone"]
    for z in ZONES_AVOID:
        for m in z.get("match", []):
            if _matches(m):
                return max(0.0, 5 + 5 * z["weight"] / 3), z["zone"]
    return 5.0, "zone neutre"


def commute_score_from_zip(location: str) -> tuple[float, int | None]:
    """0-10 from commute time to Saint-Denis. Returns (score, minutes_or_None).
    <30min=10, 30-40=8.5, 40-50=7, 50-60=5, 60-70=3.5, >70=1.5."""
    if not location:
        return 5.0, None
    m = _re_pref.search(r"\b(\d{5})\b", location)
    if not m or m.group(1) not in COMMUTE_MAP:
        return 5.0, None  # unknown — caller may use LLM estimate
    minutes = COMMUTE_MAP[m.group(1)]
    if minutes < 30: score = 10.0
    elif minutes < 40: score = 8.5
    elif minutes < 50: score = 7.0
    elif minutes < 60: score = 5.0
    elif minutes < 70: score = 3.5
    else: score = 1.5
    return score, minutes


def features_score_from_list(features_detected: list) -> float:
    """0-10. Base 5 + sum(weights) clamped. Features can be listed by
    canonical name OR synonyms — matched against PREFERRED_FEATURES."""
    bonus = 0
    for f in features_detected or []:
        f_lower = (f or "").lower().strip()
        for pf in PREFERRED_FEATURES:
            if (pf["feature"].lower() in f_lower
                or any(s.lower() in f_lower for s in pf.get("synonyms", []))):
                bonus += pf["weight"]
                break
    return max(0.0, min(10.0, 5.0 + bonus))


def combine_subscores(price_value: float, zone: float, commute: float, features: float) -> float:
    """Weighted average → 0.0-10.0 with 1 decimal."""
    raw = (
        SCORE_WEIGHTS["price_value"] * price_value
        + SCORE_WEIGHTS["zone"] * zone
        + SCORE_WEIGHTS["commute"] * commute
        + SCORE_WEIGHTS["features"] * features
    )
    return max(0.0, min(10.0, round(raw, 1)))


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

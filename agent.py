"""Claude AI logic: detect seller type, generate personalised contact message."""
import asyncio
import logging
import re
from dataclasses import dataclass, field
from typing import Literal

import config
from profile import PROFILE, PARTICULIER_CONTEXT, AGENCE_CONTEXT

logger = logging.getLogger(__name__)

SellerType = Literal["particulier", "agence"]

if not config.MOCK_MODE:
    import anthropic
    _client = anthropic.Anthropic(api_key=config.ANTHROPIC_API_KEY)
else:
    _client = None  # type: ignore[assignment]


# ─── Data classes ─────────────────────────────────────────────────────────────

@dataclass
class Listing:
    lbc_id: str
    title: str
    description: str
    price: int
    location: str
    seller_name: str
    url: str
    seller_type_hint: str = ""        # optional raw field from scraper
    source: str = "leboncoin"         # "leboncoin" | "seloger" | "pap"
    images: list = field(default_factory=list)  # photo URLs


@dataclass
class AnalysisResult:
    seller_type: SellerType
    tone: str
    message: str
    listing: Listing
    score: int = 0
    score_reason: str = ""


# ─── Seller type detection ────────────────────────────────────────────────────

_AGENCE_KEYWORDS = re.compile(
    r"\b(agence|immobilier|agence immobilière|cabinet|sarl|sas|sci|"
    r"groupe|transaction|patrimoine|résidence|property|realty|"
    r"nexity|orpi|century 21|laforêt|guy hoquet|foncia|era immobilier|"
    r"immo|notaire|promoteur|programme neuf)\b",
    re.IGNORECASE,
)


def _detect_seller_type(listing: Listing) -> SellerType:
    """Heuristic + Claude fallback to classify seller."""
    blob = " ".join([
        listing.seller_name,
        listing.title,
        listing.description[:500],
        listing.seller_type_hint,
    ])

    if _AGENCE_KEYWORDS.search(blob):
        return "agence"

    # In mock mode, default to particulier for anything the heuristic misses
    if config.MOCK_MODE:
        logger.debug("[MOCK] Seller type defaulting to 'particulier' for %s", listing.lbc_id)
        return "particulier"

    # Ask Claude when heuristic is ambiguous
    prompt = (
        f"Annonce LeBonCoin :\n"
        f"Vendeur: {listing.seller_name}\n"
        f"Titre: {listing.title}\n"
        f"Description (extrait): {listing.description[:300]}\n\n"
        "Est-ce que le vendeur est un particulier ou une agence immobilière ?\n"
        "Réponds UNIQUEMENT par 'particulier' ou 'agence'."
    )
    resp = _client.messages.create(
        model=config.CLAUDE_MODEL,
        max_tokens=10,
        messages=[{"role": "user", "content": prompt}],
    )
    answer = resp.content[0].text.strip().lower()
    return "agence" if "agence" in answer else "particulier"


# ─── Message generation ───────────────────────────────────────────────────────

_PARTICULIER_SYSTEM = """
Tu rédiges un message de contact pour une annonce de location sur LeBonCoin.
Le ton doit être chaleureux, humain, légèrement narratif — comme un vrai message
d'une personne ordinaire, pas un copier-coller générique.
Règles strictes :
- Maximum 150 mots
- Mentionne UN détail spécifique de l'annonce (ex: le balcon, la terrasse, la localisation, la surface) pour montrer que tu l'as vraiment lue
- Précise que tu cherches pour septembre 2026
- Pas de formule robotique ("Je me permets de vous contacter...")
- Intégrer naturellement ces 3 questions :
  1. Le bien est-il toujours disponible ?
  2. Les charges sont-elles comprises dans le prix ?
  3. Disponible à partir de septembre 2026 ?
- Ne PAS mentionner de salaire exact ni de chiffres financiers
- Langue : français
""".strip()

_AGENCE_SYSTEM = """
Tu rédiges un message de contact pour une agence immobilière.
Ton : professionnel mais naturel — pas un formulaire, pas une liste à puces.
Structure imposée :
1. Ouvrir sur l'intérêt pour le bien en mentionnant UN détail spécifique de l'annonce (surface, localisation, équipement...) + demander si toujours disponible (1 phrase)
2. Se présenter en prose fluide : prénom, âge, poste chez SNCF Voyageurs (grande entreprise, stable),
   revenu mensuel, dossier prêt immédiatement, souhaite emménager en septembre 2026,
   double revenu dès cette date (compagne pacsée)
3. Poser les questions pratiques : charges, confirmer disponibilité pour septembre 2026, visite possible ?
4. Signature simple
Règles :
- Maximum 120 mots
- Tout en prose, pas de tirets ni de bullet points
- Langue : français
""".strip()


def _build_particulier_prompt(listing: Listing) -> str:
    return (
        f"Contexte sur le locataire :\n{PARTICULIER_CONTEXT}\n\n"
        f"Annonce :\n"
        f"- Titre : {listing.title}\n"
        f"- Localisation : {listing.location}\n"
        f"- Loyer : {listing.price} €\n"
        f"- Description : {listing.description[:400]}\n\n"
        "Rédige le message de contact."
    )


def _build_agence_prompt(listing: Listing) -> str:
    return (
        f"Contexte sur le locataire :\n{AGENCE_CONTEXT}\n\n"
        f"Annonce :\n"
        f"- Titre : {listing.title}\n"
        f"- Localisation : {listing.location}\n"
        f"- Loyer : {listing.price} €\n"
        f"- Description : {listing.description[:400]}\n\n"
        "Rédige le message de contact professionnel."
    )


def _generate_message(listing: Listing, seller_type: SellerType) -> str:
    if config.MOCK_MODE:
        from mock_data import generate_mock_message
        logger.info("[MOCK] Returning personalized template message for seller_type=%s", seller_type)
        return generate_mock_message(listing, seller_type)

    if seller_type == "particulier":
        system = _PARTICULIER_SYSTEM
        user_prompt = _build_particulier_prompt(listing)
    else:
        system = _AGENCE_SYSTEM
        user_prompt = _build_agence_prompt(listing)

    resp = _client.messages.create(
        model=config.CLAUDE_MODEL,
        max_tokens=400,
        system=system,
        messages=[{"role": "user", "content": user_prompt}],
    )
    return resp.content[0].text.strip()


# ─── Scoring (optional, ENABLE_SCORING=true) ─────────────────────────────────

async def score_listing(listing: Listing) -> dict:
    """Rate a listing 1-10. Returns {"score": int, "reason": str}."""
    if config.MOCK_MODE or not config.ENABLE_SCORING:
        return {"score": 7, "reason": "mock score"}

    s = PROFILE["search"]
    prompt = (
        f"Note cette annonce de 1 à 10 pour ce locataire :\n"
        f"- Budget max : {s['max_rent']}€, surface min : {s['min_surface']}m²\n"
        f"- Zones préférées : Paris intra-muros et petite couronne proche\n\n"
        f"Annonce :\n"
        f"- Titre : {listing.title}\n"
        f"- Prix : {listing.price}€\n"
        f"- Localisation : {listing.location}\n"
        f"- Description : {listing.description[:300]}\n\n"
        "Réponds UNIQUEMENT sous cette forme (2 lignes) :\n"
        "SCORE: <chiffre 1-10>\n"
        "RAISON: <une phrase courte>"
    )
    resp = _client.messages.create(
        model=config.CLAUDE_MODEL,
        max_tokens=60,
        messages=[{"role": "user", "content": prompt}],
    )
    text = resp.content[0].text.strip()
    score = 5
    reason = ""
    for line in text.splitlines():
        if line.upper().startswith("SCORE:"):
            try:
                score = int(re.search(r"\d+", line).group())
            except Exception:
                pass
        elif line.upper().startswith("RAISON:"):
            reason = line.split(":", 1)[-1].strip()
    return {"score": score, "reason": reason}


# ─── Photo analysis (optional, ENABLE_PHOTO_ANALYSIS=true) ───────────────────

async def analyse_photos(image_urls: list) -> dict:
    """Analyse up to 3 photos with Claude Vision. Returns {"photo_score": int, "observations": str}."""
    if config.MOCK_MODE or not config.ENABLE_PHOTO_ANALYSIS:
        return {"photo_score": 7, "observations": "mock photo analysis"}

    if not image_urls:
        return {"photo_score": 5, "observations": "Pas de photos disponibles"}

    # Build content blocks for up to 3 images
    content = []
    for url in image_urls[:3]:
        content.append({
            "type": "image",
            "source": {"type": "url", "url": url},
        })
    content.append({
        "type": "text",
        "text": (
            "Analyse ces photos d'appartement. Note de 1 à 10 la qualité globale "
            "(meublé correct, propreté, luminosité, état général). "
            "Signale les red flags s'il y en a.\n"
            "Format de réponse :\n"
            "PHOTO_SCORE: <1-10>\n"
            "OBSERVATIONS: <une phrase>"
        ),
    })

    resp = _client.messages.create(
        model=config.CLAUDE_MODEL,
        max_tokens=80,
        messages=[{"role": "user", "content": content}],
    )
    text = resp.content[0].text.strip()
    photo_score = 5
    observations = ""
    for line in text.splitlines():
        if line.upper().startswith("PHOTO_SCORE:"):
            try:
                photo_score = int(re.search(r"\d+", line).group())
            except Exception:
                pass
        elif line.upper().startswith("OBSERVATIONS:"):
            observations = line.split(":", 1)[-1].strip()
    return {"photo_score": photo_score, "observations": observations}


# ─── Public API ───────────────────────────────────────────────────────────────

async def analyse_listing(listing: Listing) -> AnalysisResult:
    """Detect seller type, optionally score + analyse photos, generate message."""
    seller_type = _detect_seller_type(listing)
    tone = "Séduction / narratif" if seller_type == "particulier" else "Professionnel / factuel"
    message = _generate_message(listing, seller_type)

    # Optional scoring
    score = 0
    score_reason = ""
    if config.ENABLE_SCORING:
        score_data = await score_listing(listing)
        score = score_data["score"]
        score_reason = score_data["reason"]

    # Optional photo analysis (only if score is good enough)
    if config.ENABLE_PHOTO_ANALYSIS and listing.images and (not config.ENABLE_SCORING or score >= config.MIN_SCORE):
        photo_data = await analyse_photos(listing.images)
        if score_reason:
            score_reason += f" | Photos: {photo_data['observations']}"
        else:
            score_reason = f"Photos: {photo_data['observations']}"

    logger.info(
        "Listing %s → type=%s, tone=%s, score=%s, msg_len=%d",
        listing.lbc_id, seller_type, tone, score or "N/A", len(message),
    )
    return AnalysisResult(
        seller_type=seller_type,
        tone=tone,
        message=message,
        listing=listing,
        score=score,
        score_reason=score_reason,
    )


# ─── Intent classification (natural language → action) ───────────────────────

_INTENT_TOOLS = [
    {
        "name": "run_search",
        "description": "Lancer un scraping d'annonces LeBonCoin. Utiliser quand l'utilisateur veut chercher des appartements.",
        "input_schema": {
            "type": "object",
            "properties": {
                "url": {
                    "type": "string",
                    "description": "URL de recherche LeBonCoin (optionnel, utiliser l'URL par défaut si non précisée)",
                }
            },
            "required": [],
        },
    },
    {
        "name": "run_simulate",
        "description": "Analyser une annonce et générer le message qui serait envoyé, sans l'envoyer. Utiliser quand l'utilisateur envoie une URL LeBonCoin ou veut voir ce que le bot dirait.",
        "input_schema": {
            "type": "object",
            "properties": {
                "url": {
                    "type": "string",
                    "description": "URL de l'annonce LeBonCoin à analyser",
                }
            },
            "required": ["url"],
        },
    },
    {
        "name": "run_campagne",
        "description": "Lancer la campagne automatique complète : scraping + génération de messages + envoi.",
        "input_schema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "run_rapport",
        "description": "Afficher les statistiques du jour : annonces scrapées, messages envoyés, réponses reçues.",
        "input_schema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "run_stop",
        "description": "Arrêter la campagne en cours.",
        "input_schema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "run_settings",
        "description": "Afficher les critères de recherche actuels.",
        "input_schema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "reply",
        "description": "Répondre directement à l'utilisateur sans déclencher d'action. Utiliser pour les questions, les conversations, les explications.",
        "input_schema": {
            "type": "object",
            "properties": {
                "text": {
                    "type": "string",
                    "description": "La réponse à envoyer à l'utilisateur, en français",
                }
            },
            "required": ["text"],
        },
    },
]

_INTENT_SYSTEM = """
Tu es l'assistant du bot immobilier d'Illan Krief.
Illan cherche un appartement meublé en Île-de-France (max 800€, 25m²+, dispo sept 2026).
Ton rôle : comprendre ce qu'Illan veut faire et choisir l'outil approprié.
Réponds TOUJOURS en français.
Si le message contient une URL leboncoin.fr, utilise run_simulate avec cette URL.
""".strip()


def classify_intent(user_message: str) -> dict:
    """
    Classify a free-text user message into a bot action.
    Returns e.g. {"tool": "run_simulate", "url": "https://..."}
    or {"tool": "reply", "text": "..."}
    """
    if config.MOCK_MODE:
        # In mock mode, do a simple heuristic so we don't need a real API key
        msg = user_message.lower()
        if "leboncoin.fr" in msg:
            url = next((w for w in user_message.split() if "leboncoin.fr" in w), "")
            return {"tool": "run_simulate", "url": url}
        if any(w in msg for w in ["cherch", "search", "annonce", "scrape"]):
            return {"tool": "run_search"}
        if any(w in msg for w in ["campagne", "envoie", "lance", "envoyer"]):
            return {"tool": "run_campagne"}
        if any(w in msg for w in ["stat", "rapport", "aujourd", "bilan"]):
            return {"tool": "run_rapport"}
        if any(w in msg for w in ["stop", "arrête", "pause"]):
            return {"tool": "run_stop"}
        if any(w in msg for w in ["critère", "setting", "paramètre", "config"]):
            return {"tool": "run_settings"}
        return {"tool": "reply", "text": "Je suis en mode simulation (sans clé API). Envoie-moi une URL LeBonCoin ou tape une commande comme /search, /campagne, /rapport."}

    resp = _client.messages.create(
        model=config.CLAUDE_MODEL,
        max_tokens=200,
        system=_INTENT_SYSTEM,
        tools=_INTENT_TOOLS,
        tool_choice={"type": "any"},
        messages=[{"role": "user", "content": user_message}],
    )

    # Extract the tool use block
    for block in resp.content:
        if block.type == "tool_use":
            return {"tool": block.name, **block.input}

    return {"tool": "reply", "text": "Je n'ai pas compris. Envoie-moi une URL LeBonCoin ou décris ce que tu veux faire."}


def format_simulation_text(result: AnalysisResult) -> str:
    """Return the Telegram-formatted simulation card (no inline keyboard)."""
    type_emoji = "👤" if result.seller_type == "particulier" else "🏢"
    score_line = ""
    if result.score:
        score_line = f"⭐ Score : *{result.score}/10* — _{result.score_reason}_\n"
    return (
        f"🔍 *ANALYSE ANNONCE*\n\n"
        f"📍 {result.listing.title}\n"
        f"📍 {result.listing.location}\n"
        f"💰 {result.listing.price} €/mois\n"
        f"🔗 {result.listing.url}\n\n"
        f"{type_emoji} Type détecté : *{result.seller_type.capitalize()}*\n"
        f"🎭 Ton choisi : _{result.tone}_\n"
        f"{score_line}"
        f"\n📝 *MESSAGE QUI SERAIT ENVOYÉ :*\n"
        f"─────────────────────\n"
        f"{result.message}\n"
        f"─────────────────────"
    )

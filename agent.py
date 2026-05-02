"""Claude AI logic: detect seller type, generate personalised contact message."""
import asyncio
import logging
import re
import time
from dataclasses import dataclass, field
from typing import Any, Literal

import config
from profile import PROFILE, PARTICULIER_CONTEXT, AGENCE_CONTEXT

logger = logging.getLogger(__name__)

SellerType = Literal["particulier", "agence"]

if not config.MOCK_MODE:
    import anthropic
    if config.USE_DEEPSEEK:
        # DeepSeek exposes an Anthropic-compatible endpoint at this base URL,
        # so the same SDK + tool-use code paths work unchanged.
        _client = anthropic.Anthropic(
            api_key=config.DEEPSEEK_API_KEY,
            base_url="https://api.deepseek.com/anthropic",
        )
        logger.info("LLM provider: DeepSeek (model=%s)", config.CLAUDE_MODEL)
    else:
        _client = anthropic.Anthropic(api_key=config.ANTHROPIC_API_KEY)
        logger.info("LLM provider: Anthropic (model=%s)", config.CLAUDE_MODEL)
else:
    _client = None  # type: ignore[assignment]


def _first_text(resp) -> str:
    """Return the first text-block content from a Claude/DeepSeek response.

    DeepSeek V4 always prepends a 'thinking' block to responses on the
    Anthropic-compatible endpoint; Anthropic's own API doesn't (unless extended
    thinking is explicitly enabled). This helper handles both transparently.
    """
    for block in resp.content:
        if getattr(block, "type", None) == "text":
            return block.text
    return ""


def _call_claude(**kwargs) -> Any:
    """Invoke Claude (or DeepSeek via Anthropic-compatible endpoint) with retry.

    3 attempts, exponential backoff (2s, 4s, 8s). Retries on connection errors,
    rate limits, and 5xx upstream errors. Other errors propagate immediately.

    When USE_DEEPSEEK is on, auto-injects thinking={'type':'disabled'} so the
    response doesn't waste tokens on a chain-of-thought block we don't display.
    """
    if _client is None:
        raise RuntimeError("Claude client unavailable (MOCK_MODE or missing API key)")
    if config.USE_DEEPSEEK:
        kwargs.setdefault("thinking", {"type": "disabled"})
    last_exc: Exception | None = None
    for attempt in range(3):
        try:
            return _client.messages.create(**kwargs)
        except anthropic.APIConnectionError as exc:
            last_exc = exc
            logger.warning("Claude connection error (attempt %d/3): %s", attempt + 1, exc)
        except anthropic.RateLimitError as exc:
            last_exc = exc
            logger.warning("Claude rate-limited (attempt %d/3): %s", attempt + 1, exc)
        except anthropic.APIStatusError as exc:
            if getattr(exc, "status_code", 0) >= 500:
                last_exc = exc
                logger.warning("Claude 5xx (attempt %d/3): %s", attempt + 1, exc)
            else:
                raise
        if attempt < 2:
            time.sleep(2 * (2 ** attempt))
    assert last_exc is not None
    raise last_exc


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
    source: str = "leboncoin"         # "leboncoin" | "seloger" | "pap" ...
    images: list = field(default_factory=list)  # photo URLs
    surface: int | None = None        # square meters (m²), parsed from title/description


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
    resp = _call_claude(
        model=config.CLAUDE_MODEL,
        max_tokens=10,
        messages=[{"role": "user", "content": prompt}],
    )
    answer = _first_text(resp).strip().lower()
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

    resp = _call_claude(
        model=config.CLAUDE_MODEL,
        max_tokens=400,
        system=system,
        messages=[{"role": "user", "content": user_prompt}],
    )
    return _first_text(resp).strip()


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
    resp = _call_claude(
        model=config.CLAUDE_MODEL,
        max_tokens=60,
        messages=[{"role": "user", "content": prompt}],
    )
    text = _first_text(resp).strip()
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

    resp = _call_claude(
        model=config.CLAUDE_MODEL,
        max_tokens=80,
        messages=[{"role": "user", "content": content}],
    )
    text = _first_text(resp).strip()
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


# ─── Dossier pre-screening (optional, ENABLE_PRESCREENING=true) ──────────────

async def prescreen_listing(listing: Listing) -> dict:
    """Check profile compatibility with listing requirements.
    Returns {"eligible": bool, "note": str}.
    Always eligible in mock mode or when ENABLE_PRESCREENING=false.
    """
    if config.MOCK_MODE or not config.ENABLE_PRESCREENING:
        return {"eligible": True, "note": ""}

    s = PROFILE["search"]
    prompt = (
        f"Annonce de location :\n"
        f"- Titre : {listing.title}\n"
        f"- Prix : {listing.price}€/mois\n"
        f"- Description : {listing.description[:500]}\n\n"
        f"Profil du candidat :\n"
        f"- Alternant SNCF Voyageurs, 1 850€/mois net\n"
        f"- CDI SNCF confirmé septembre 2026, double revenu pacsé (~800€/mois supplémentaires)\n"
        f"- Emménagement souhaité : septembre 2026\n"
        f"- Budget max : {s['max_rent']}€ CC\n\n"
        "Si l'annonce mentionne des conditions (ratio salaire/loyer, type de contrat, "
        "date de disponibilité, garant requis, etc.), vérifie la compatibilité.\n"
        "Si aucune condition n'est mentionnée, considère le profil compatible.\n"
        "Réponds sur 2 lignes exactement :\n"
        "ELIGIBLE: oui|non\n"
        "NOTE: <raison si non éligible, sinon laisse vide>"
    )
    resp = _call_claude(
        model=config.CLAUDE_MODEL,
        max_tokens=80,
        messages=[{"role": "user", "content": prompt}],
    )
    text = _first_text(resp).strip()
    eligible = True
    note = ""
    for line in text.splitlines():
        if line.upper().startswith("ELIGIBLE:"):
            eligible = "non" not in line.lower()
        elif line.upper().startswith("NOTE:"):
            note = line.split(":", 1)[-1].strip()
    logger.info("Prescreening %s → eligible=%s note=%s", listing.lbc_id, eligible, note)
    return {"eligible": eligible, "note": note}


# ─── Intent classification (natural language → action) ───────────────────────

_INTENT_TOOLS = [
    {
        "name": "run_search",
        "description": "Lancer un scraping ponctuel d'UNE source pour tester ou voir les résultats bruts. Pour la recherche multi-sources complète préférer run_campagne.",
        "input_schema": {
            "type": "object",
            "properties": {
                "source": {
                    "type": "string",
                    "enum": [
                        "leboncoin", "seloger", "pap", "bienici", "logicimmo",
                        "studapart", "parisattitude", "lodgis", "immojeune", "locservice",
                    ],
                    "description": "Nom de la source à scraper. UTILISE TOUJOURS ce paramètre quand l'utilisateur nomme un site (ex: 'paris attitude' → 'parisattitude', 'seloger' → 'seloger'). N'INVENTE JAMAIS d'URL.",
                },
                "url": {
                    "type": "string",
                    "description": "URL EXACTE de recherche, UNIQUEMENT si l'utilisateur l'a explicitement collée dans son message. Sinon utilise `source`.",
                },
            },
            "required": [],
        },
    },
    {
        "name": "run_simulate",
        "description": "Analyser UNE annonce précise et générer le message qui serait envoyé, sans l'envoyer. Utiliser quand l'utilisateur envoie une URL d'annonce (depuis n'importe lequel des sites supportés) ou veut voir ce que le bot dirait sur un bien spécifique.",
        "input_schema": {
            "type": "object",
            "properties": {
                "url": {
                    "type": "string",
                    "description": "URL d'une annonce individuelle (leboncoin.fr, seloger.com, pap.fr, bienici.com, logic-immo.com, studapart.com, parisattitude.com, lodgis.com, immojeune.com, locservice.fr).",
                }
            },
            "required": ["url"],
        },
    },
    {
        "name": "run_campagne",
        "description": "Lancer la phase de PRÉPARATION d'une campagne : scraping + analyse + génération des messages personnalisés. AUCUN message n'est envoyé — les messages sont stockés en attente d'envoi (run_envoyer pour envoyer ensuite). Par défaut scrape toutes les sources. Si l'utilisateur précise un site (« lance la campagne pour paris attitude », « campagne studapart »), utiliser le paramètre `source` pour limiter à ce site.",
        "input_schema": {
            "type": "object",
            "properties": {
                "source": {
                    "type": "string",
                    "enum": [
                        "leboncoin", "seloger", "pap", "bienici", "logicimmo",
                        "studapart", "parisattitude", "lodgis", "immojeune", "locservice",
                    ],
                    "description": "Limiter la campagne à une seule source. Optionnel — si absent, toutes les sources sont scrapées.",
                },
            },
            "required": [],
        },
    },
    {
        "name": "run_envoyer",
        "description": "Demander la CONFIRMATION d'envoi des messages préparés. Cet outil n'envoie PAS directement — il affiche un récap et attend que l'utilisateur dise 'oui' / 'go' / 'confirme' ou tape /confirmer pour lancer l'envoi pour de vrai. Utiliser pour 'envoie les messages', 'fais l'envoi', 'contacte-les maintenant', etc.",
        "input_schema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "list_pending",
        "description": "Lister les VRAIES annonces actuellement en attente d'envoi (avec leurs URLs réelles depuis la base de données). À UTILISER OBLIGATOIREMENT quand l'utilisateur demande des URLs, des prix, des noms d'annonces préparées, ou 'donne-moi la liste', 'qu'as-tu préparé', 'montre-moi les URLs', 'liste des annonces prêtes', 'envoie-moi les liens'. NE JAMAIS répondre via reply avec des URLs ou détails inventés — utilise CET outil.",
        "input_schema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "list_recent",
        "description": "Lister les VRAIES annonces récemment scrapées en base (toutes sources, qu'elles soient préparées ou non). À UTILISER OBLIGATOIREMENT pour 'qu'as-tu trouvé en dernier', 'donne-moi les annonces récentes', 'montre-moi les dernières annonces', 'liste tout ce que t'as scrapé'. Toujours préférer cet outil à reply quand l'utilisateur veut voir des annonces concrètes.",
        "input_schema": {
            "type": "object",
            "properties": {
                "limit": {
                    "type": "number",
                    "description": "Nombre d'annonces à afficher (défaut 10, max 30).",
                },
            },
            "required": [],
        },
    },
    {
        "name": "query_listings",
        "description": (
            "Rapport custom sur les annonces en base avec filtres et tri configurables. "
            "À UTILISER quand l'utilisateur demande un rapport groupé/trié/filtré, ex: "
            "'groupe par site et trie par m²', 'montre-moi les studios sous 800€', "
            "'classe par surface', 'donne-moi tout ce qui est dans mon budget rangé "
            "par site', 'rapport complet', 'qu'est-ce qu'on a en base trié par...', "
            "'tout ce qui est en-dessous de X€'. C'est l'outil PUISSANT pour des "
            "questions de visualisation/analyse — préfère-le à list_recent dès que "
            "l'utilisateur veut un filtre ou tri custom."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "source": {
                    "type": "string",
                    "enum": [
                        "leboncoin", "seloger", "pap", "bienici", "logicimmo",
                        "studapart", "parisattitude", "lodgis", "immojeune", "locservice",
                    ],
                    "description": "Limiter à une source. Optionnel.",
                },
                "min_price": {"type": "number", "description": "Prix minimum en €. Optionnel."},
                "max_price": {"type": "number", "description": "Prix maximum en €. Optionnel."},
                "min_surface": {"type": "number", "description": "Surface minimum en m². Optionnel."},
                "max_surface": {"type": "number", "description": "Surface maximum en m². Optionnel."},
                "sort_by": {
                    "type": "string",
                    "enum": ["surface", "price", "recent", "score"],
                    "description": "Tri : 'surface' (desc, plus grand au plus petit), 'price' (asc), 'recent' (dernières scrapées), 'score' (desc). Défaut 'recent'.",
                },
                "group_by_source": {
                    "type": "boolean",
                    "description": "Si true, groupe les résultats par site (LBC, SeLoger, etc.). Défaut false.",
                },
                "limit": {
                    "type": "number",
                    "description": "Nombre max d'annonces (défaut 50, max 200).",
                },
            },
            "required": [],
        },
    },
    {
        "name": "run_rapport",
        "description": "Afficher les statistiques du jour : annonces scrapées, messages envoyés, réponses reçues. Utiliser pour 'rapport', 'stats', 'bilan', 'comment ça avance', etc.",
        "input_schema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "run_stop",
        "description": "Arrêter la campagne EN COURS d'exécution. Pour désactiver la campagne automatique récurrente, utiliser run_autostop.",
        "input_schema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "run_settings",
        "description": "Afficher les critères de recherche actuels (budget, surface, zones, etc.).",
        "input_schema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "run_autostart",
        "description": "Activer la campagne automatique récurrente toutes les N heures. Utiliser pour 'lance la campagne en boucle', 'tourne tous les X heures', etc.",
        "input_schema": {
            "type": "object",
            "properties": {
                "hours": {
                    "type": "number",
                    "description": "Intervalle en heures (défaut 3).",
                }
            },
            "required": [],
        },
    },
    {
        "name": "run_autostop",
        "description": "Arrêter la campagne automatique récurrente (désactive la boucle, contrairement à run_stop qui n'arrête que l'exécution courante).",
        "input_schema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "run_watch",
        "description": "Activer le mode veille : poll rapide toutes les N minutes pour chopper les nouvelles annonces dès qu'elles apparaissent et les contacter immédiatement. Utiliser pour 'mode veille', 'surveille', 'préviens-moi des nouveautés', 'temps réel', etc.",
        "input_schema": {
            "type": "object",
            "properties": {
                "minutes": {
                    "type": "number",
                    "description": "Intervalle de poll en minutes (défaut 15).",
                }
            },
            "required": [],
        },
    },
    {
        "name": "run_unwatch",
        "description": "Désactiver le mode veille (poll rapide). Utiliser pour 'arrête la veille', 'plus de surveillance', etc.",
        "input_schema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "run_visite",
        "description": "Enregistrer une visite planifiée pour un bien. Nécessite l'URL de l'annonce ET la date/heure du rendez-vous.",
        "input_schema": {
            "type": "object",
            "properties": {
                "url": {"type": "string", "description": "URL de l'annonce visitée."},
                "date": {"type": "string", "description": "Date et heure du rendez-vous en français libre, ex: 'Samedi 5 avril 10h'."},
            },
            "required": ["url", "date"],
        },
    },
    {
        "name": "run_visites",
        "description": "Afficher la liste des visites planifiées à venir.",
        "input_schema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "run_boite",
        "description": "Vérifier la boîte de réception LeBonCoin pour voir les nouvelles réponses des annonceurs. Utiliser pour 'check ma boîte', 'des réponses?', 'vérifie les messages reçus', etc.",
        "input_schema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "reply",
        "description": "Répondre directement à l'utilisateur sans déclencher d'action. Utiliser pour les salutations, questions générales, explications, ou si la demande ne correspond à aucun outil. La réponse doit être chaleureuse, en français, et donner envie de poursuivre la conversation.",
        "input_schema": {
            "type": "object",
            "properties": {
                "text": {
                    "type": "string",
                    "description": "La réponse à envoyer à l'utilisateur, en français, ton naturel et amical.",
                }
            },
            "required": ["text"],
        },
    },
]

_INTENT_SYSTEM = """
Tu es l'assistant conversationnel du bot immobilier d'Illan Krief.

Profil d'Illan :
- Cherche un appartement meublé en Île-de-France
- Budget max 1000€ CC, surface mini 25m², emménagement septembre 2026
- Alternant SNCF Voyageurs

Sources scrapées : LeBonCoin, SeLoger, PAP, Bien'ici, Logic-Immo, Studapart,
Paris Attitude, Lodgis, ImmoJeune, LocService.

Workflow en deux étapes (Illan préfère contrôler l'envoi explicitement) :
- run_campagne PRÉPARE les messages (scrape + analyse) sans rien envoyer
- run_envoyer ENVOIE effectivement les messages préparés (étape finale)
Distingue bien ces deux : « lance la recherche / prépare » → run_campagne ;
« envoie les messages / vas-y envoie » → run_envoyer.

Ton rôle : comprendre ce qu'Illan veut faire en langage naturel et choisir
l'outil approprié. Tu DOIS appeler exactement un outil par message.

RÈGLES CRITIQUES — à respecter absolument :

1. ANTI-HALLUCINATION : Si l'utilisateur demande des URLs, des prix exacts,
   des noms d'annonces, ou n'importe quel détail factuel sur les annonces
   en base, tu DOIS utiliser list_pending (annonces préparées) ou
   list_recent (annonces scrapées). N'invente JAMAIS d'URL ou de détail
   d'annonce dans le tool reply — tu n'as PAS accès à la base sans ces
   outils, donc toute URL inventée serait fausse.

2. URLs de recherche : N'INVENTE JAMAIS d'URL. Pour cibler un site précis
   avec run_search, utilise le paramètre `source` (ex: 'parisattitude',
   'studapart'). Utilise `url` UNIQUEMENT si l'utilisateur a collé une URL
   textuelle dans son message.

3. PORTÉE multi-source : si l'utilisateur dit « all / tous / toutes les
   sources / partout / tous les sites » → utilise OBLIGATOIREMENT
   run_campagne (sans paramètre source — ce qui scrape toutes les sources).
   Ne fais JAMAIS un run_search dans ce cas (run_search ne touche qu'UNE
   source).

4. RAPPORTS / TRI / GROUPEMENT : pour toute demande de visualisation
   personnalisée des annonces déjà en base — « groupe par site et trie
   par m² », « montre-moi les studios sous 800€ », « classe par
   surface », « rapport complet groupé » — utilise query_listings avec
   les bons paramètres (source / min_price / max_price / min_surface /
   max_surface / sort_by / group_by_source). C'est l'outil flexible.
   N'utilise list_recent que pour une simple liste plate sans filtre.

5. Filtres NON supportés (vraiment) — utilise reply pour expliquer :
   - Filtrer par date ("du jour", "aujourd'hui", "cette semaine",
     "dernière heure") — aucune source ne supporte ce filtre côté bot.
   - "Ignorer mon budget" pour la campagne (run_campagne / run_watch
     appliquent toujours le budget configuré).
   - Filtrage par arrondissement spécifique non configuré dans l'URL.
   Pour ces cas, dis clairement à Illan que la fonctionnalité n'est pas
   supportée et propose ce que le bot PEUT faire (ex: query_listings
   avec un max_price custom couvre 80% des "filtres budget custom").

6. Si le message contient une URL d'annonce individuelle (depuis n'importe
   quel site supporté), utilise run_simulate avec cette URL.

7. Si Illan dit bonjour, te remercie, plaisante, ou pose une question
   conversationnelle SANS demander de données factuelles ni d'action,
   utilise reply avec une réponse chaleureuse et naturelle en français.

8. Distinctions à respecter :
   - run_stop = arrêter la campagne en cours d'exécution
   - run_autostop = désactiver la campagne automatique récurrente
   - run_watch = mode veille rapide (intervalles en minutes)
   - run_autostart = campagne complète récurrente (intervalles en heures)

9. Si l'intention est ambiguë, choisis reply et demande une clarification.

Réponds TOUJOURS en français.
""".strip()


def classify_intent(user_message: str, history: list[dict] | None = None) -> dict:
    """
    Classify a free-text user message into a bot action.

    `history` is an optional list of prior {role, content} pairs so the LLM
    can answer follow-up questions ("qu'as-tu trouvé ?") coherently. Pairs
    must alternate user/assistant per Anthropic's API requirement.

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

    messages = list(history or [])
    messages.append({"role": "user", "content": user_message})
    resp = _call_claude(
        model=config.CLAUDE_MODEL,
        # 1024 leaves plenty of room for the `reply` tool's `text` param to
        # contain a full conversational answer in French. The 200-token cap
        # we used before was bounded by the tool_use JSON envelope (~150
        # tokens of structure overhead) and truncated replies mid-phrase.
        max_tokens=1024,
        system=_INTENT_SYSTEM,
        tools=_INTENT_TOOLS,
        tool_choice={"type": "any"},
        messages=messages,
    )

    # Extract the tool use block
    for block in resp.content:
        if block.type == "tool_use":
            return {"tool": block.name, **block.input}

    # Fallback: if the LLM replied with plain text instead of picking a tool
    # (DeepSeek does this for chitchat like "salut" / "merci"), surface the
    # text directly as a conversational reply rather than the canned fallback.
    text_reply = _first_text(resp).strip()
    if text_reply:
        return {"tool": "reply", "text": text_reply}

    return {"tool": "reply", "text": "Je n'ai pas compris. Envoie-moi une URL d'annonce ou décris ce que tu veux faire."}


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

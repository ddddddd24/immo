"""Claude AI logic: detect seller type, generate personalised contact message."""
import asyncio
import logging
import re
import time
from dataclasses import dataclass, field
from typing import Any, Literal, Optional

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
    housing_type: str = ""            # 'studio'|'T1'..'T5+'|'coloc'|'residence'|'coliving'|'chambre'
    roommate_count: int | None = None # only set for coloc/coliving when count is parseable
    published_at: str | None = None   # ISO-8601 date when listing was first posted on source site
    phone: str | None = None          # phone number, or "#blocked" if site policy hides it, or "" if listing has none
    available_from: str | None = None # YYYY-MM availability date extracted by LLM (None if not mentioned)


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
#
# Drafts are produced in 2 tones (particulier / agence). The agence tone
# branches further on the seller_name to distinguish "gros réseau" (Foncia,
# ORPI, Century 21, Laforêt, Guy Hoquet, Nestenn, FNAIM…) from independent
# agencies — the former want a tight dossier-first message, the latter
# appreciate more personalization on the bien itself.
#
# Anti-hallucination: we pre-extract a list of CONCRETE features that
# actually appear in the listing's title or description (balcon, métro,
# rénové, etc.), and pass it to the LLM with the instruction "tu peux
# mentionner UN détail parmi cette liste". This prevents the model from
# inventing features that aren't there.

_FEATURE_KEYWORDS = [
    # (needle to search in lowercased blob, human label to pass to LLM)
    ("balcon", "le balcon"),
    ("terrasse", "la terrasse"),
    ("loggia", "la loggia"),
    ("jardin", "le jardin"),
    ("très lumineux", "la luminosité"),
    ("lumineux", "la luminosité"),
    ("rénové", "la rénovation récente"),
    ("refait à neuf", "la rénovation récente"),
    ("calme", "le calme"),
    ("sans vis-à-vis", "l'absence de vis-à-vis"),
    ("parquet", "le parquet"),
    ("moulures", "les moulures"),
    ("haussmannien", "le style haussmannien"),
    ("baie vitrée", "la baie vitrée"),
    ("vue dégagée", "la vue dégagée"),
    ("vue sur", "la vue"),
    ("dernier étage", "le dernier étage"),
    ("ascenseur", "la présence d'un ascenseur"),
    ("cave", "la cave"),
    ("cuisine équipée", "la cuisine équipée"),
    ("cuisine ouverte", "la cuisine ouverte"),
    ("dressing", "le dressing"),
    ("placards", "les placards intégrés"),
    ("métro", "la proximité du métro"),
    (" rer ", "la proximité du RER"),
    ("gare", "la proximité de la gare"),
    ("tram", "la proximité du tram"),
    ("commerces", "la proximité des commerces"),
    ("écoles", "la proximité des écoles"),
    ("parc", "la proximité d'un parc"),
    ("résidence sécurisée", "la résidence sécurisée"),
    ("digicode", "la résidence sécurisée"),
    ("interphone", "la résidence sécurisée"),
]

_BIG_CHAIN_HINTS = (
    "orpi", "century 21", "century21", "foncia", "laforêt", "laforet",
    "guy hoquet", "nestenn", "stéphane plaza", "stephane plaza",
    "iad ", "safti", "era ", "l'adresse", "ladresse", "guy-hoquet",
    "fnaim", "particulier à particulier",
)


def _extract_listing_features(listing: Listing) -> list[str]:
    """Return concrete features actually present in the listing text. Caps
    at 6 to keep the prompt tight. Anti-hallucination guard."""
    blob = f" {(listing.title or '').lower()} {(listing.description or '').lower()} "
    found: list[str] = []
    seen: set[str] = set()
    for needle, label in _FEATURE_KEYWORDS:
        if needle in blob and label not in seen:
            seen.add(label)
            found.append(label)
            if len(found) >= 6:
                break
    return found


def _seller_size_hint(seller_name: str) -> str:
    """'gros_reseau' | 'indep' — branches the agence prompt for tone."""
    s = (seller_name or "").lower()
    return "gros_reseau" if any(h in s for h in _BIG_CHAIN_HINTS) else "indep"


def _listing_kind(listing: Listing) -> str:
    """Human-readable kind for the prompt context. Combines housing_type +
    surface heuristics so the LLM can adapt phrasing (un studio vs un T2
    appellent des justifications différentes pour un couple)."""
    ht = (getattr(listing, "housing_type", "") or "").lower()
    if "studio" in ht or "t1" in ht:
        return "studio/T1"
    if "t2" in ht:
        return "T2"
    if "t3" in ht or "t4" in ht or "t5" in ht:
        return "T3+"
    if "coloc" in ht or "coliving" in ht:
        return "colocation"
    if "chambre" in ht:
        return "chambre"
    # Fallback on surface
    surf = getattr(listing, "surface", None) or 0
    if surf and surf < 25:
        return "studio/T1"
    if surf and surf < 40:
        return "T2"
    return "appartement"


def _avail_hint(listing: Listing) -> str:
    """If the LLM already extracted a clean YYYY-MM availability date and it
    falls before our move-in target, give the model an explicit fact to
    weave in ('le bien est libre dès juin 2026, parfait')."""
    av = getattr(listing, "available_from", None)
    if not av or not isinstance(av, str) or len(av) < 7:
        return ""
    try:
        import datetime as _dt
        ym = av[:7]
        target = "2026-09"
        if ym <= target:
            mois_map = {"01":"janvier","02":"février","03":"mars","04":"avril",
                        "05":"mai","06":"juin","07":"juillet","08":"août",
                        "09":"septembre","10":"octobre","11":"novembre","12":"décembre"}
            label = f"{mois_map.get(ym[5:7],'')} {ym[:4]}".strip()
            return f"L'annonce indique une disponibilité dès {label}."
    except Exception:
        pass
    return ""


_PARTICULIER_SYSTEM = """
Tu rédiges un message de contact à un PROPRIÉTAIRE PARTICULIER (LeBonCoin,
PAP). Le destinataire est un humain ordinaire — pas une agence. Ton :
chaleureux, sincère, comme un voisin qui sonne pour visiter. PAS une
lettre de motivation.

═══ INTERDICTIONS STRICTES — toute occurrence est un échec ═══
1. NE COMMENCE JAMAIS PAR "Je me permets" sous aucune forme. JAMAIS.
2. Pas une seule des formules : "Pourriez-vous préciser", "envisager",
   "procurer", "Dans l'attente", "Veuillez agréer", "Auriez-vous
   l'obligeance".
3. AUCUN CHIFFRE INVENTÉ. Tu n'as PAS le droit d'écrire un montant de
   charges, de revenu, de loyer, ou tout autre chiffre financier qui ne
   serait pas explicitement fourni dans le contexte. Si tu veux demander
   les charges, formule "Les charges sont-elles comprises dans le loyer ?"
   et JAMAIS "les charges de XXX € sont-elles comprises dans le loyer de
   YYY €". Cette règle est non-négociable.
4. Pas de salaire, pas de revenu chiffré — particulier ne veut pas voir ça.

═══ RÈGLES POSITIVES ═══
- 90 à 130 mots, vouvoiement courtois mais détendu.
- Mentionne UN détail concret du bien — UNIQUEMENT depuis la liste
  'Features vérifiées' du contexte. Si vide → parle de la localisation ou
  de la surface (réelle, fournie en contexte).
- Mentionne SNCF (poste stable) + dossier solide avec garant Visale.
- GLI INVERSÉE : propose de prendre en charge TOI-MÊME le coût de
  l'assurance loyers impayés (GLI), pour que la garantie ne coûte rien au
  propriétaire. C'est un argument fort et différenciant. Si la description
  mentionne une GLI ou un organisme (Cautioneo, Garantme, assurance loyers
  impayés), mets-le EN AVANT et indique que tu présenteras un dossier déjà
  validé. Sinon, glisse-le naturellement comme un plus. Formule type :
  "je suis prêt à prendre en charge l'assurance loyers impayés de mon
  côté". Ne promets jamais de contourner les critères de l'assureur.
- Disponibilité FLEXIBLE : peut emménager dès juillet 2026 au plus tôt.
  S'adapter à la date de l'annonce ('avail_hint') — si le bien est libre
  tôt ou de suite, montrer qu'on peut emménager rapidement (c'est un atout).
  NE PAS répéter la date deux fois dans le message.
- 2-3 questions naturelles dans la prose : disponibilité, charges
  comprises, visite cette semaine.
- Préférer : "J'aimerais", "Le bien est-il toujours libre ?", "Une visite
  serait possible cette semaine ?", "Merci d'avance".
- Signature : juste "Illan" (sans nom de famille).
- Prose continue, pas de bullets, pas de markdown.
- Langue : français.
""".strip()

_AGENCE_SYSTEM = """
Tu rédiges un message de contact à une AGENCE IMMOBILIÈRE. Le destinataire
est un négociateur qui reçoit des dizaines de candidatures/jour — il doit
voir en 5 secondes que ton dossier est solide.

═══ INTERDICTIONS STRICTES — toute occurrence est un échec ═══
1. NE COMMENCE JAMAIS PAR "Je me permets" sous aucune forme. JAMAIS.
2. Pas une seule des formules : "Auriez-vous l'obligeance", "Dans
   l'attente", "Veuillez agréer", "Bien cordialement", "Je me permets de
   vous contacter".
3. AUCUN CHIFFRE INVENTÉ. Tu peux mentionner UNIQUEMENT les chiffres
   fournis explicitement dans le contexte : 1 850 €/mois (revenu candidat),
   ~800 €/mois (compagne), ~2 650 €/mois (ressources totales août 2026), et
   le loyer affiché de l'annonce. Tu n'as PAS le droit d'inventer un
   montant de charges. Si tu veux demander les charges, formule "Les
   charges sont-elles comprises dans le loyer ?" SANS supposer aucun
   montant. Cette règle est non-négociable.

═══ RÈGLES POSITIVES ═══
- 100 à 140 mots, vouvoiement pro mais HUMAIN.
- Prose fluide, PAS de bullets / tirets / numérotation.
- Structure : (a) intérêt sur le bien + UN détail concret (uniquement
  depuis 'Features vérifiées') ; (b) présentation : Illan Krief, 26 ans,
  alternant Product Owner chez SNCF Voyageurs (équivalent CDI pour les
  dossiers de location), 1 850 €/mois, garant Visale, dossier complet
  immédiatement disponible ; (c) compagne pacsée qui rejoint en août
  2026 (~800 €/mois → ressources totales ≈ 2 650 €/mois) ; (d) 2-3
  questions : confirmer dispo, charges comprises ou en sus, fenêtre de
  visite cette semaine.
- Disponibilité FLEXIBLE : emménagement possible dès juillet 2026 au plus
  tôt ; s'adapter à 'avail_hint' (si le bien est dispo de suite, signaler
  qu'on peut emménager rapidement — c'est un atout).
- 'seller_size' = 'gros_reseau' → concis + dossier-prêt. 'indep' → un peu
  plus chaleureux sur le bien.
- GLI INVERSÉE : propose que le candidat prenne en charge LUI-MÊME le coût
  de l'assurance loyers impayés (GLI), pour qu'elle ne coûte rien au
  bailleur — argument fort et différenciant. Si l'annonce mentionne une GLI
  ou un organisme (Cautioneo, Garantme, assurance loyers impayés), mets-le
  EN AVANT et précise que le dossier sera présenté déjà validé auprès de
  l'organisme demandé. Sinon, glisse-le comme un atout. Ne promets jamais
  de contourner les critères d'éligibilité de l'assureur.
- Préférer : "Merci par avance", "Bonne journée", "Cordialement".
- Signature : "Illan Krief".
- Langue : français.
""".strip()


def _build_particulier_prompt(listing: Listing, extra: str = "") -> str:
    features = _extract_listing_features(listing)
    kind = _listing_kind(listing)
    avail = _avail_hint(listing)
    feat_str = ("- " + "\n- ".join(features)) if features else "(aucune feature spécifique détectée — parle de la localisation)"
    return (
        f"Contexte sur le locataire :\n{PARTICULIER_CONTEXT}\n\n"
        f"Annonce :\n"
        f"- Type de bien : {kind}\n"
        f"- Titre : {listing.title}\n"
        f"- Localisation : {listing.location}\n"
        f"- Surface : {getattr(listing, 'surface', None) or '?'} m²\n"
        f"- Loyer : {listing.price} € (information interne, NE PAS chiffrer dans le message)\n"
        f"- Vendeur (prénom ou pseudo) : {listing.seller_name or '(inconnu)'}\n"
        f"- Description (extrait) : {(listing.description or '')[:500]}\n\n"
        f"Features vérifiées (utiliser UNE de la liste, ne pas inventer) :\n{feat_str}\n\n"
        + (f"avail_hint : {avail}\n\n" if avail else "")
        + (f"Élément personnel à intégrer naturellement SI c'est pertinent (sinon l'ignorer complètement, ne force rien) : {extra}\n\n" if extra else "")
        + "Rédige le message de contact en respectant TOUTES les règles du system."
    )


def _build_agence_prompt(listing: Listing, extra: str = "") -> str:
    features = _extract_listing_features(listing)
    kind = _listing_kind(listing)
    size = _seller_size_hint(listing.seller_name)
    avail = _avail_hint(listing)
    feat_str = ("- " + "\n- ".join(features)) if features else "(aucune feature spécifique détectée — parle de la localisation ou du type de bien)"
    return (
        f"Contexte sur le candidat :\n{AGENCE_CONTEXT}\n\n"
        f"Annonce :\n"
        f"- Type de bien : {kind}\n"
        f"- Titre : {listing.title}\n"
        f"- Localisation : {listing.location}\n"
        f"- Surface : {getattr(listing, 'surface', None) or '?'} m²\n"
        f"- Loyer : {listing.price} € (chiffre OK à intégrer si pertinent)\n"
        f"- Agence : {listing.seller_name or '(inconnue)'}\n"
        f"- Description (extrait) : {(listing.description or '')[:500]}\n\n"
        f"Features vérifiées (utiliser UNE de la liste, ne pas inventer) :\n{feat_str}\n\n"
        f"seller_size : {size}\n"
        + (f"avail_hint : {avail}\n\n" if avail else "\n")
        + (f"Élément personnel à intégrer naturellement SI c'est pertinent (sinon l'ignorer complètement, ne force rien) : {extra}\n\n" if extra else "")
        + "Rédige le message de contact en respectant TOUTES les règles du system."
    )


def _generate_message(listing: Listing, seller_type: SellerType, extra: str = "") -> str:
    if config.MOCK_MODE:
        from mock_data import generate_mock_message
        logger.info("[MOCK] Returning personalized template message for seller_type=%s", seller_type)
        return generate_mock_message(listing, seller_type)

    if seller_type == "particulier":
        system = _PARTICULIER_SYSTEM
        user_prompt = _build_particulier_prompt(listing, extra)
    else:
        system = _AGENCE_SYSTEM
        user_prompt = _build_agence_prompt(listing, extra)

    resp = _call_claude(
        model=config.CLAUDE_MODEL,
        max_tokens=400,
        system=system,
        messages=[{"role": "user", "content": user_prompt}],
    )
    return _first_text(resp).strip()


async def regenerate_message_with_tone(
    listing: "Listing", seller_type: "SellerType"
) -> str:
    """Public async wrapper around _generate_message. Used by the Telegram
    '🔄 Changer de ton' callback to force a tone (particulier ↔ agence)
    when the auto-detection picked the wrong one. Wrapped in asyncio.to_thread
    so the sync DeepSeek call doesn't block the bot event loop."""
    import asyncio as _aio
    return await _aio.to_thread(_generate_message, listing, seller_type)


# ─── Scoring (optional, ENABLE_SCORING=true) ─────────────────────────────────

async def score_listing(listing: Listing) -> dict:
    """Rate a listing 1-10 against Illan's structured preferences.

    Returns {"score": int, "reason": str}. Score 0 means a dealbreaker
    matched (no LLM call needed, saves tokens). 1-10 reflects how well
    the listing fits the preferences in preferences.py.
    """
    if config.MOCK_MODE or not config.ENABLE_SCORING:
        return {"score": 7, "reason": "mock score"}

    # Pre-filter: dealbreakers short-circuit without LLM call
    import preferences
    blocked, reason = preferences.is_dealbreaker(
        housing_type=getattr(listing, "housing_type", ""),
        roommate_count=getattr(listing, "roommate_count", None),
        title=listing.title or "",
        description=(listing.description or "")[:500],
    )
    if blocked:
        logger.info("Dealbreaker on %s: %s", listing.lbc_id, reason)
        return {"score": 0, "reason": f"dealbreaker: {reason}"}

    # Split the prompt: STABLE prefix (preferences + rules ~1000 tokens) goes
    # first with cache_control. DeepSeek auto-caches this prefix so repeated
    # scoring calls hit cache pricing ($0.014/M instead of $0.14/M = -90%).
    prefs_block = preferences.build_prompt_block()
    stable_prefix = (
        "Tu notes une annonce immobilière pour Illan de 1 à 10 selon SES préférences.\n\n"
        f"{prefs_block}\n\n"
        "Règles de notation :\n"
        "  • 9-10 = match excellent (zone préférée + plusieurs caractéristiques préférées)\n"
        "  • 7-8  = bon match (zone OK + au moins une caractéristique préférée)\n"
        "  • 5-6  = correct (rien de bloquant mais rien d'enthousiasmant)\n"
        "  • 3-4  = signaux négatifs (zone à éviter, ou caractéristiques manquantes)\n"
        "  • 1-2  = mauvais match (zone à éviter ET commute long ET 0 caractéristique préférée)\n\n"
        "Réponds STRICTEMENT sous cette forme (2 lignes max) :\n"
        "SCORE: <chiffre 1-10>\n"
        "RAISON: <une phrase concise mentionnant 2-3 facteurs concrets de l'annonce>"
    )
    listing_block = (
        "\n\nAnnonce à évaluer :\n"
        f"- Titre : {listing.title}\n"
        f"- Type : {getattr(listing, 'housing_type', '') or 'inconnu'}\n"
        f"- Prix : {listing.price}€\n"
        f"- Surface : {getattr(listing, 'surface', '') or '?'}m²\n"
        f"- Localisation : {listing.location}\n"
        f"- Description : {(listing.description or '')[:400]}"
    )
    resp = _call_claude(
        model=config.CLAUDE_MODEL,
        max_tokens=200,
        messages=[{
            "role": "user",
            "content": [
                # Stable prefix — cache_control hint for Anthropic-compat backends.
                # DeepSeek's auto-cache also picks up the identical prefix even
                # without this marker; the marker is belt-and-suspenders.
                {"type": "text", "text": stable_prefix, "cache_control": {"type": "ephemeral"}},
                {"type": "text", "text": listing_block},
            ],
        }],
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
    score = max(1, min(score, 10))  # clamp into [1, 10]
    return {"score": score, "reason": reason}


async def score_listings_batch(listings: list[Listing], batch_size: int = 5) -> list[dict]:
    """Score listings using v2 algo: 4 sub-scores (price/value, zone, commute,
    features) combined into 0-10 final, plus hard dealbreakers (price>1050€,
    critical zones, étage>3 sans ascenseur, dispo après sept 2026).

    Hybrid: price/value + zone match + zip-based commute computed by rules
    (free, deterministic). LLM extracts floor/elevator/available date/features
    from description in a single batched call.

    Returns list[{score, reason}] in input order. Reason includes subscore
    breakdown for debugging.
    """
    if config.MOCK_MODE or not config.ENABLE_SCORING:
        return [{"score": 7, "reason": "mock score"} for _ in listings]

    import preferences
    import json as _json
    import datetime as _dt

    results: list[Optional[dict]] = [None] * len(listings)

    def _zero(idx: int, reason: str) -> None:
        results[idx] = {"score": 0, "reason": f"❌ {reason}"}

    # ── Phase 1: Rule-based pre-filter (no LLM) ─────────────────────────────
    pending: list[tuple[int, Listing]] = []
    for i, lst in enumerate(listings):
        # Hard price cap
        if lst.price is not None and lst.price > preferences.HARD_PRICE_CAP:
            _zero(i, f"prix {lst.price}€ > {preferences.HARD_PRICE_CAP}€")
            continue
        # Original dealbreakers (housing_type, roommate_count, keywords)
        blocked, reason = preferences.is_dealbreaker(
            housing_type=getattr(lst, "housing_type", ""),
            roommate_count=getattr(lst, "roommate_count", None),
            title=lst.title or "",
            description=(lst.description or "")[:500],
        )
        if blocked:
            _zero(i, reason)
            continue
        # Critical avoid zones
        crit, kw = preferences.is_critical_zone(
            location=lst.location or "",
            title=lst.title or "",
            description=(lst.description or "")[:500],
        )
        if crit:
            _zero(i, f"zone critique: {kw}")
            continue
        pending.append((i, lst))

    if not pending:
        return [r or {"score": 5, "reason": ""} for r in results]

    # ── Phase 2: LLM batch — extract floor/lift/available_date/features/commute ──
    today_str = _dt.date.today().strftime("%Y-%m-%d")
    stable_prefix = (
        f"Tu analyses des annonces immo pour Illan (couple, emménagement cible "
        f"sept 2026, travail à Saint-Denis). Aujourd'hui : {today_str}.\n"
        "Extrais en JSON STRICT pour chaque annonce.\n"
        "Pour 'features', utilise UNIQUEMENT : balcon, terrasse, lave-linge, lumineux, "
        "rénové, calme, ascenseur, proche métro, cuisine équipée, meublé, fibre.\n"
        "Pour 'commute_min' : minutes vers Saint-Denis en transports publics (estimation honnête).\n"
        "\n"
        "Pour 'available' (date à laquelle l'appartement devient libre) :\n"
        "  Format : YYYY-MM-DD si le jour est mentionné, sinon YYYY-MM.\n"
        "Réfléchis comme un agent immo qui relit l'annonce ligne par ligne. La date "
        "n'est pas toujours formulée \"libre le XX\" — elle peut être DÉDUITE :\n"
        "  • \"Le locataire actuel part fin août\" → l'appart est libre dès septembre.\n"
        "  • \"Bail en cours jusqu'au 30/06/2026\" → libre 2026-07.\n"
        "  • \"Préavis de 3 mois déposé le 1er mai\" → libre 2026-08.\n"
        "  • \"Rentrée 2026\" / \"pour l'année universitaire 2026-2027\" → 2026-09.\n"
        "  • \"À partir de l'été\" sans année + on est en mai 2026 → 2026-07.\n"
        "  • \"libre de suite\" + annonce active → mois en cours.\n"
        "Pour les mois sans année explicite : choisis la PROCHAINE occurrence ≥ aujourd'hui "
        "(\"avril\" en 2026-05 = 2027-04, pas 2026-04).\n"
        "À NE PAS confondre avec la dispo : date d'ouverture d'une résidence neuve, "
        "date de rénovation/construction, date de fin de bail SANS info sur la suite, "
        "date de mise en ligne de l'annonce, date de visite.\n"
        "Si l'annonce ne donne aucun signal direct ou indirect sur la dispo → null. "
        "Mieux vaut null qu'une devinette.\n"
        f"Contrainte dure : jamais de date avant {today_str}.\n"
        "Pour 'floor' : numéro étage entier (null si non précisé). RDC = 0.\n"
        "Pour 'elevator' : true/false (null si non précisé).\n"
        "Pour 'apl_eligible' : true SI explicitement éligible (APL/ALS/aides), "
        "false SI explicitement NON-éligible (\"non éligible aux aides\", \"hors APL\", "
        "\"pas d'APL\", \"non conventionné\"), null SI silencieux.\n"
        "Pour 'unfurnished' : true SI explicitement non-meublé/loué vide, false SI meublé, "
        "null SI silencieux.\n"
        "Pour 'zone_safety' (note 0-10 de la sécurité/qualité du quartier mentionné dans 'Loc') :\n"
        "  9-10 : très safe, calme, BCBG (Neuilly, Vincennes, Saint-Mandé, Saint-Germain-en-Laye)\n"
        "  7-8  : safe, classe moyenne supérieure (Boulogne, Sèvres, Suresnes, Maisons-Alfort)\n"
        "  5-6  : moyen, mixte (bien dépendre du quartier exact)\n"
        "  3-4  : réputation difficile pour un couple sans enfants (zones nord-est 93, certains 95)\n"
        "  0-2  : zone clairement problématique\n"
        "  null SEULEMENT si tu ne reconnais pas la ville. Sinon réponds avec ton meilleur jugement, "
        "  même si c'est une banlieue moyennement connue — base-toi sur la réputation générale, "
        "  classe sociale, taux de criminalité, attractivité.\n"
        "Format réponse :\n"
        '{"items":[{"i":0,"floor":null|N,"elevator":null|true|false,'
        '"available":null|"YYYY-MM","apl_eligible":null|true|false,'
        '"unfurnished":null|true|false,"zone_safety":null|N,'
        '"commute_min":N,"features":["..."],"summary":"..."}]}'
    )

    async def _llm_batch(batch: list[tuple[int, Listing]]) -> dict:
        rows = []
        for idx, lst in batch:
            rows.append(
                f"i={idx}: Titre={(lst.title or '')[:100]}; "
                f"Prix={lst.price}€; Surface={getattr(lst, 'surface', None) or '?'}m²; "
                f"Loc={(lst.location or '')[:60]}; "
                f"Desc={(lst.description or '')[:1500]}"
            )
        try:
            # asyncio.to_thread → DeepSeek client is sync; without this the
            # event loop blocks per call and asyncio.gather serialises batches.
            resp = await asyncio.to_thread(
                _call_claude,
                model=config.CLAUDE_MODEL,
                max_tokens=300 * len(batch),
                messages=[{
                    "role": "user",
                    "content": [
                        {"type": "text", "text": stable_prefix, "cache_control": {"type": "ephemeral"}},
                        {"type": "text", "text": "Annonces:\n" + "\n".join(rows)},
                    ],
                }],
            )
            text = _first_text(resp).strip()
            text = re.sub(r"^```(?:json)?\s*|\s*```$", "", text, flags=re.MULTILINE)
            data = _json.loads(text)
            return {item["i"]: item for item in data.get("items", []) if "i" in item}
        except Exception as exc:
            logger.warning("LLM batch extract failed: %s", exc)
            return {}

    # Process batches in parallel (each = 1 LLM call for ~batch_size listings)
    batches = [pending[i:i + batch_size] for i in range(0, len(pending), batch_size)]
    llm_outputs = await asyncio.gather(*(_llm_batch(b) for b in batches))
    llm_data: dict = {}
    for d in llm_outputs:
        llm_data.update(d)

    # ── Phase 3: Combine rules + LLM data → final score ─────────────────────
    move_in_latest = preferences.MOVE_IN_DATE_LATEST.strftime("%Y-%m")
    # Discard any extracted date earlier than this — LLM hallucinations
    # ("fin avril" → 2025-04, "ouverture résidence 2025" etc.). One-month
    # grace before today to allow listings genuinely available now.
    today = _dt.date.today()
    earliest_avail = (today.replace(day=1) - _dt.timedelta(days=1)).strftime("%Y-%m")
    for idx, lst in pending:
        item = llm_data.get(idx, {}) or {}

        # Capture availability for ALL listings (even dealbroken ones) so the
        # dashboard can show "Libre" across the full table, not just scored rows.
        # Accept both YYYY-MM (7 chars) and YYYY-MM-DD (10 chars). Validate via
        # strptime — rejects "2026-13", "2026-09-31", malformed garbage.
        avail = item.get("available")
        avail_str = None
        if isinstance(avail, str):
            try:
                if len(avail) >= 10 and avail[4] == '-' and avail[7] == '-':
                    _dt.datetime.strptime(avail[:10], "%Y-%m-%d")
                    avail_str = avail[:10]
                elif len(avail) >= 7 and avail[4] == '-':
                    _dt.datetime.strptime(avail[:7], "%Y-%m")
                    avail_str = avail[:7]
            except (ValueError, IndexError):
                avail_str = None
        # Drop past-dated extractions — almost always year hallucinations.
        if avail_str and avail_str[:7] < earliest_avail:
            avail_str = None
        lst.available_from = avail_str

        # Late availability dealbreaker
        if avail_str is not None and avail_str[:7] > move_in_latest:
            _zero(idx, f"dispo {avail_str} > sept 2026")
            results[idx]["available_from"] = avail_str
            continue

        # Étage > 3 sans ascenseur dealbreaker.
        # Two tiers:
        #   floor > 3 + elev explicitly False  → drop (was the original rule)
        #   floor >= 5 + elev unknown          → also drop (conservative for
        #     top-of-Parisian-building cases where the listing/detail page
        #     omits ascenseur info — observed on a SeLoger 6/6 listing where
        #     the LLM had no signal and the listing escaped to score 9.2).
        floor = item.get("floor")
        elev = item.get("elevator")
        if isinstance(floor, int) and floor > 3 and elev is False:
            _zero(idx, f"étage {floor} sans ascenseur")
            continue
        if isinstance(floor, int) and floor >= 5 and elev is not True:
            _zero(idx, f"étage {floor}, ascenseur non confirmé")
            continue

        # Description-text safety net — catches "Nème étage sans ascenseur"
        # in free text when the LLM failed to structure floor/elevator (e.g.
        # if the detail-page enrichment timed out and only the basic search
        # data was passed in).
        _text_blob = f"{lst.title or ''} {lst.description or ''}"
        _no_asc_m = re.search(
            r"(\d+)\s*[èe]?(?:me|ème|er)?\s+étage[^.]{0,80}sans\s+ascenseur",
            _text_blob, re.IGNORECASE,
        )
        if _no_asc_m:
            try:
                _fl = int(_no_asc_m.group(1))
                if _fl > 3:
                    _zero(idx, f"étage {_fl} sans ascenseur (texte)")
                    continue
            except ValueError:
                pass

        # APL/aides eligibility dealbreaker — only if explicitly NOT eligible
        if item.get("apl_eligible") is False:
            _zero(idx, "non éligible aux aides (APL/ALS)")
            continue

        # Unfurnished dealbreaker — only if explicitly non-meublé
        if item.get("unfurnished") is True:
            _zero(idx, "non meublé (loué vide)")
            continue

        # Sub-scores
        pv = preferences.price_value_score(lst.price, getattr(lst, "surface", None))
        zs, zone_label = preferences.zone_match_score(lst.location or "")
        # Zone unknown to the curated list → ask the LLM what it thinks of the
        # specific commune. Falls back to a département-level baseline if the
        # LLM also has no opinion (75=6.0, 92=6.0, 93=4.0, 94=5.5, other=5.0).
        # The user-facing rule: "if quartier well-frequented note good, if
        # dangerous note less. Don't default to 5 just because we lack data."
        if zone_label == "zone neutre":
            llm_zs = item.get("zone_safety")
            if isinstance(llm_zs, (int, float)) and 0 <= llm_zs <= 10:
                zs = float(llm_zs)
                zone_label = (lst.location or "").split(",")[0].strip()[:30] or "?"
            else:
                _zip_m = re.search(r"\b(\d{5})\b", lst.location or "")
                _zip = _zip_m.group(1)[:2] if _zip_m else ""
                zs = {"75": 6.0, "92": 6.0, "93": 4.0, "94": 5.5}.get(_zip, 5.0)
                zone_label = f"dépt {_zip}" if _zip else "zone inconnue"
        cs, mins_known = preferences.commute_score_from_zip(lst.location or "")
        if mins_known is None:
            llm_min = item.get("commute_min")
            if isinstance(llm_min, (int, float)) and 0 < llm_min < 200:
                cs, mins_known = (
                    (10.0 if llm_min < 30 else
                     8.5 if llm_min < 40 else
                     7.0 if llm_min < 50 else
                     5.0 if llm_min < 60 else
                     3.5 if llm_min < 70 else
                     1.5),
                    int(llm_min),
                )
        fs = preferences.features_score_from_list(item.get("features") or [])

        final = preferences.combine_subscores(pv, zs, cs, fs)
        commute_str = f"{mins_known}min" if mins_known else "?"
        summary = (item.get("summary") or "")[:80].strip()
        results[idx] = {
            "score": final,
            "reason": (
                f"PV={pv:.1f} Z={zs:.1f}({zone_label[:18]}) "
                f"C={cs:.1f}({commute_str}) F={fs:.1f}"
                + (f" — {summary}" if summary else "")
            ),
            "available_from": avail_str,
        }

    return [r or {"score": 5, "reason": "score manquant"} for r in results]


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
        "name": "sync_sheet",
        "description": "Synchroniser la base d'annonces vers le Google Sheet (si configuré). Utilise pour 'sync sheet', 'mets à jour le tableur', 'pousse vers google sheets', 'synchronise', 'envoie au sheet'.",
        "input_schema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "score_all",
        "description": "Calculer (ou recalculer) le score 1-10 pour toutes les annonces qui n'en ont pas encore. À utiliser pour 'score tout', 'recalcule les scores', 'note toutes les annonces', 'fais un backfill des scores'. Coût ~$0.005/annonce.",
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

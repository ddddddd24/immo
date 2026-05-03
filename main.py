"""Telegram bot entry point — all commands handled here."""
import asyncio
import logging
import re
import sys
import time
from pathlib import Path
from typing import Any

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
)
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    ContextTypes,
    filters,
)
from telegram.constants import ParseMode
from telegram.request import HTTPXRequest

import config
import database
from agent import analyse_listing, format_simulation_text, classify_intent, Listing, score_listing, prescreen_listing
from scraper import search_listings, fetch_single_listing, is_real_offer, is_suspicious
from messenger import send_message_safe, check_inbox_lbc
from profile import PROFILE

# ─── Logging ──────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.FileHandler(config.LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)

# ─── Campaign state ───────────────────────────────────────────────────────────

_campaign_lock = asyncio.Lock()
_stop_requested = asyncio.Event()
_auto_task: asyncio.Task | None = None
_watch_task: asyncio.Task | None = None

# ─── Conversation memory (per chat_id) ────────────────────────────────────────
#
# DeepSeek classify_intent sees only one user message at a time. Without
# memory the bot would deny prior actions ("rien lancé encore" right after
# running a campaign). We keep the last few user/assistant turns per chat_id
# so the LLM can answer questions like "qu'as-tu trouvé ?" coherently.

_HISTORY: dict[int, list[dict]] = {}
_TURN_REPLIES: dict[int, list[str]] = {}
_HISTORY_MAX_PAIRS = 4  # last N (user, assistant) pairs


def _commit_turn(chat_id: int, user_text: str) -> None:
    h = _HISTORY.setdefault(chat_id, [])
    h.append({"role": "user", "content": user_text[:1500]})
    bot_replies = _TURN_REPLIES.pop(chat_id, [])
    bot_text = "\n\n".join(bot_replies)[:2000]
    if bot_text:
        h.append({"role": "assistant", "content": bot_text})
    max_msgs = _HISTORY_MAX_PAIRS * 2
    if len(h) > max_msgs:
        del h[: len(h) - max_msgs]


def _history_for(chat_id: int) -> list[dict]:
    return list(_HISTORY.get(chat_id, []))


# ─── TTL helpers for ctx.bot_data callback state ──────────────────────────────

_TTL_SECONDS = 1800  # 30 min — matches /simulate user wait window


def _set_ttl(data: dict, key: str, value: Any) -> None:
    data[key] = (value, time.time() + _TTL_SECONDS)


def _get_ttl(data: dict, key: str) -> Any:
    """Return the value if not expired, else None (and remove if expired)."""
    entry = data.get(key)
    if entry is None:
        return None
    value, expires_at = entry
    if time.time() >= expires_at:
        data.pop(key, None)
        return None
    return value


def _pop_ttl(data: dict, key: str) -> Any:
    """Pop and return the value if not expired, else None."""
    entry = data.pop(key, None)
    if entry is None:
        return None
    value, expires_at = entry
    if time.time() >= expires_at:
        return None
    return value


# ─── Helper ───────────────────────────────────────────────────────────────────

_MD_SPECIAL = re.compile(r"([_*\[\]`])")
_TELEGRAM_MSG_LIMIT = 3800  # safety margin under Telegram's 4096-char hard limit

# Hard pattern for any listing URL across the 11 supported sources.
# When DeepSeek invents a `reply` containing one of these, it's HALLUCINATED —
# the LLM has no DB access in the reply path. We strip them defensively.
_HALLUCINATED_URL_RE = re.compile(
    r"https?://(?:www\.|en[-_]us\.|fr[-_]fr\.)?"
    r"(?:leboncoin\.fr|seloger\.com|pap\.fr|bienici\.com|logic-immo\.com|"
    r"studapart\.com|parisattitude\.com|lodgis\.com|immojeune\.com|"
    r"locservice\.fr|roomlala\.com)\S+",
    re.IGNORECASE,
)


def _sanitize_reply_text(text: str) -> str:
    """Strip listing URLs from a `reply` tool output.

    The reply tool exists for conversational text — chitchat, explanations,
    clarifications. It has NO access to the SQLite DB. Any URL the LLM puts
    in a reply is fabricated from training data or conversation memory and
    will likely 404. We replace such URLs with a pointer to /rapport_complet
    or /pending so the user gets real data via a tool that queries the DB.
    """
    if not _HALLUCINATED_URL_RE.search(text):
        return text
    cleaned = _HALLUCINATED_URL_RE.sub("[URL invérifiée]", text)
    cleaned += (
        "\n\n⚠️ J'ai retiré des URLs que je ne peux pas vérifier — "
        "elles auraient été inventées. Pour les vraies URLs, tape "
        "`/rapport_complet` (groupé+trié) ou `/pending` (annonces préparées)."
    )
    return cleaned


def _escape_md(text: str | None) -> str:
    """Escape Telegram MARKDOWN special chars so dynamic content (titles,
    locations, seller names) doesn't break formatting. Returns "" for None."""
    if not text:
        return ""
    return _MD_SPECIAL.sub(r"\\\1", str(text))


def _chunk_for_telegram(text: str, max_len: int = _TELEGRAM_MSG_LIMIT) -> list[str]:
    """Split a long message at line boundaries so each piece fits Telegram's
    4096-char limit. Without this, /rapport_complet on 100+ listings used to
    silently fail to send and the user got nothing.
    """
    if len(text) <= max_len:
        return [text]
    chunks: list[str] = []
    current = ""
    for line in text.split("\n"):
        candidate = (current + "\n" + line) if current else line
        if len(candidate) > max_len:
            if current:
                chunks.append(current)
                current = line if len(line) <= max_len else line[:max_len]
            else:
                # Single line too long — hard-cut
                chunks.append(line[:max_len])
                current = line[max_len:]
        else:
            current = candidate
    if current:
        chunks.append(current)
    return chunks


async def _reply(update: Update, text: str, **kwargs) -> None:
    chat_id = update.effective_chat.id if update.effective_chat else None
    if chat_id is not None:
        _TURN_REPLIES.setdefault(chat_id, []).append(text)
    for part in _chunk_for_telegram(text):
        try:
            await update.effective_message.reply_text(
                part, parse_mode=ParseMode.MARKDOWN, **kwargs
            )
        except Exception as exc:
            # If MARKDOWN parsing fails (unbalanced *, _, [ in dynamic content
            # despite escaping), retry as plain text so the user sees something
            # rather than nothing.
            logger.warning("Markdown reply failed (%s) — retrying as plain text", exc)
            try:
                await update.effective_message.reply_text(part, **kwargs)
            except Exception as exc2:
                logger.error("Plain-text reply also failed: %s", exc2)


# ─── /start ───────────────────────────────────────────────────────────────────

async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    await _reply(
        update,
        "👋 *Bienvenue sur le bot immo d'Illan !*\n\n"
        "📡 Sources : LBC, SeLoger, PAP, Bien'ici, Logic-Immo,\n"
        "Studapart, Paris Attitude, Lodgis, ImmoJeune, LocService\n\n"
        "Commandes disponibles :\n"
        "• /search — Lancer un scraping\n"
        "• /simulate <url> — Simuler un message sans l'envoyer\n"
        "• /campagne [source] — Préparer la campagne (scrape + analyse, *sans envoi*)\n"
        "• /envoyer — Demander confirmation avant l'envoi des messages préparés\n"
        "• /confirmer — Confirmer et lancer l'envoi (ou tape « oui »)\n"
        "• /autostart [heures] — Campagne automatique (défaut : 3h)\n"
        "• /autostop — Arrêter la campagne automatique\n"
        "• /watch [min] — Mode veille : nouvelles annonces toutes les N min (défaut : 15)\n"
        "• /unwatch — Désactiver le mode veille\n"
        "• /rapport — Stats du jour\n"
        "• /settings — Critères de recherche\n"
        "• /visite <url> <date> — Enregistrer une visite\n"
        "• /visites — Liste des visites à venir\n"
        "• /boite — Vérifier les réponses LBC\n"
        "• /stop — Arrêter la campagne en cours",
    )


# ─── /settings ────────────────────────────────────────────────────────────────

async def cmd_settings(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    s = PROFILE["search"]
    zones = ", ".join(s["zones"])
    await _reply(
        update,
        "⚙️ *Critères de recherche*\n\n"
        f"📐 Surface min : {s['min_surface']} m²\n"
        f"💰 Loyer max : {s['max_rent']} € CC\n"
        f"📍 Zones : {zones}\n"
        f"🛋 Meublé : {'Oui' if s['furnished'] else 'Non'}\n"
        f"🌿 Balcon : {'Préféré' if s['balcony_preferred'] else 'Non'}\n"
        f"📅 Emménagement : {s['move_in']}\n"
        f"🚫 Exclusions : {', '.join(s['excluded_zones'])}",
    )


# ─── /search ──────────────────────────────────────────────────────────────────

async def cmd_search(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    url = " ".join(ctx.args) if ctx.args else config.DEFAULT_SEARCH_URL
    await _reply(update, f"🔍 Scraping en cours…\n`{url}`")
    try:
        listings = await search_listings(url, max_results=30)
    except Exception as exc:
        await _reply(update, f"❌ Erreur de scraping : `{exc}`")
        return

    if not listings:
        await _reply(update, "😕 Aucune annonce trouvée.")
        return

    # Persist new listings
    new_count = 0
    for lst in listings:
        listing_id = database.upsert_listing(
            lbc_id=lst.lbc_id,
            title=lst.title,
            price=lst.price,
            location=lst.location,
            seller_name=lst.seller_name,
            seller_type="",
            url=lst.url,
            source=lst.source,
            surface=lst.surface,
        )
        if not database.already_contacted(lst.lbc_id):
            new_count += 1

    await _reply(
        update,
        f"✅ {len(listings)} annonces scrapées, *{new_count}* nouvelles (non encore contactées).\n"
        f"_Brut : aucun message n'a été préparé._\n"
        f"Pour analyser et préparer des messages : lance /campagne (toutes sources) "
        f"ou « lance la campagne pour <site> » pour cibler une seule source.",
    )


# ─── /simulate ────────────────────────────────────────────────────────────────

async def cmd_simulate(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    if not ctx.args:
        await _reply(
            update,
            "Usage : `/simulate <url_leboncoin>`\n"
            "Exemple : `/simulate https://www.leboncoin.fr/annonces/1234567.htm`",
        )
        return

    url = ctx.args[0]
    await _reply(update, "⏳ Récupération de l'annonce…")

    try:
        listing = await fetch_single_listing(url)
    except Exception as exc:
        await _reply(update, f"❌ Impossible de récupérer l'annonce : `{exc}`")
        return

    if not listing:
        await _reply(update, "❌ Annonce introuvable ou format non reconnu.")
        return

    await _reply(update, "🤖 Analyse Claude en cours…")
    result = await analyse_listing(listing)
    text = format_simulation_text(result)

    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Envoyer", callback_data=f"send:{listing.lbc_id}"),
            InlineKeyboardButton("✏️ Modifier", callback_data=f"edit:{listing.lbc_id}"),
            InlineKeyboardButton("❌ Ignorer", callback_data=f"ignore:{listing.lbc_id}"),
        ]
    ])

    # Store result in bot_data so callback can access it (TTL guard against stale entries)
    _set_ttl(ctx.bot_data, f"sim:{listing.lbc_id}", result)

    await update.effective_message.reply_text(
        text,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=keyboard,
        disable_web_page_preview=True,
    )


# ─── Inline keyboard callbacks ────────────────────────────────────────────────

async def callback_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()

    action, lbc_id = query.data.split(":", 1)

    # ── /watch alert callbacks ──────────────────────────────────────────────
    if action == "watch_ignore":
        await query.edit_message_reply_markup(reply_markup=None)
        await query.message.reply_text(
            f"❌ Ignorée.",
        )
        return

    if action == "watch_prep":
        # User wants to prepare the message (analyse + create_contact in pending).
        # No actual send — that's still /envoyer + /confirmer.
        row = database.get_listing_by_lbc_id(lbc_id)
        if not row:
            await query.message.reply_text("❌ Annonce introuvable en base.")
            return
        if database.already_contacted(lbc_id):
            await query.edit_message_reply_markup(reply_markup=None)
            await query.message.reply_text("ℹ️ Déjà préparée — tape /pending pour la voir.")
            return
        # Build a minimal Listing object to pass to analyse_listing
        from agent import Listing
        lst = Listing(
            lbc_id=row["lbc_id"], title=row["title"] or "", description="",
            price=row["price"] or 0, location=row["location"] or "",
            seller_name=row["seller_name"] or "", url=row["url"] or "",
            source=row["source"] or "", surface=row["surface"],
        )
        await query.edit_message_reply_markup(reply_markup=None)
        await query.message.reply_text("🧠 Génération du message…")
        try:
            result = await analyse_listing(lst)
            listing_id = database.upsert_listing(
                lbc_id=lst.lbc_id, title=lst.title, price=lst.price,
                location=lst.location, seller_name=lst.seller_name,
                seller_type=result.seller_type, url=lst.url, source=lst.source,
                surface=lst.surface,
            )
            database.create_contact(listing_id, result.message)
            await query.message.reply_text(
                f"📝 Message préparé. Tape /envoyer puis /confirmer pour l'envoyer pour de vrai."
            )
        except Exception as exc:
            await query.message.reply_text(f"❌ Erreur génération : `{exc}`")
        return

    # ── /simulate callbacks ──────────────────────────────────────────────────
    result = _get_ttl(ctx.bot_data, f"sim:{lbc_id}")

    if action == "ignore":
        ctx.bot_data.pop(f"sim:{lbc_id}", None)
        await query.edit_message_reply_markup(reply_markup=None)
        await query.message.reply_text("❌ Annonce ignorée.")
        return

    if action == "edit":
        # Store pending-edit state so the next message from this user is used as the custom message
        _set_ttl(ctx.bot_data, f"pending_edit:{query.message.chat_id}", lbc_id)
        await query.edit_message_reply_markup(reply_markup=None)
        await query.message.reply_text(
            "✏️ *Mode édition activé.*\n"
            "Envoie ton message personnalisé et je l'enverrai directement.",
            parse_mode=ParseMode.MARKDOWN,
        )
        return

    if action == "send":
        if not result:
            await query.message.reply_text("❌ Session expirée, relancez /simulate.")
            return

        listing = result.listing
        listing_id = database.upsert_listing(
            lbc_id=listing.lbc_id,
            title=listing.title,
            price=listing.price,
            location=listing.location,
            seller_name=listing.seller_name,
            seller_type=result.seller_type,
            url=listing.url,
            source=listing.source,
            surface=listing.surface,
        )
        contact_id = database.create_contact(listing_id, result.message)

        await query.edit_message_reply_markup(reply_markup=None)
        await query.message.reply_text("📤 Envoi du message…")

        success = await send_message_safe(listing.url, result.message, contact_id)
        if success:
            await query.message.reply_text(f"✅ Message envoyé à : {listing.url}")
        else:
            await query.message.reply_text(
                "❌ Échec de l'envoi (voir logs). Vérifiez vos identifiants LBC."
            )


# ─── Deduplication helper ─────────────────────────────────────────────────────

def _deduplicate(listings: list) -> list:
    """Remove cross-platform duplicates using (price, normalised city, seller_name)."""
    seen: set = set()
    unique = []
    for lst in listings:
        city = (lst.location or "").split(",")[0].strip().lower()
        seller = (lst.seller_name or "").strip().lower()
        key = (lst.price, city, seller)
        if key not in seen:
            seen.add(key)
            unique.append(lst)
    return unique


_HOUSING_DEALBREAKERS = {"coloc", "coliving", "chambre"}


def _should_contact(listing) -> tuple[bool, str, str]:
    """Centralised eligibility check shared by /campagne and /watch.

    Returns (eligible, category, fr_reason). Categories are stable identifiers
    used by the campaign report to break down skipped listings:
    'qualité' / 'budget' / 'suspect' / 'déjà_préparée' / 'type_logement'.
    Order matters: cheapest checks first, DB hit last.
    """
    if not is_real_offer(listing):
        return False, "qualité", "annonce filtrée"
    htype = getattr(listing, "housing_type", "") or ""
    if htype in _HOUSING_DEALBREAKERS:
        return False, "type_logement", f"type {htype} (couple-only)"
    if listing.price and listing.price > PROFILE["search"]["max_rent"]:
        return False, "budget", f"{listing.price}€ > {PROFILE['search']['max_rent']}€"
    suspicious, reason = is_suspicious(listing)
    if suspicious:
        return False, "suspect", reason
    if database.already_contacted(listing.lbc_id):
        return False, "déjà_préparée", "contact déjà créé"
    return True, "", ""


# ─── Campaign core (shared by /campagne and auto-loop) ────────────────────────

async def _run_campaign_core(
    update: Update, ctx: ContextTypes.DEFAULT_TYPE, source: str | None = None
) -> None:
    """Run one full campaign cycle. Acquires _campaign_lock; honours _stop_requested.

    If `source` is provided (e.g. 'parisattitude'), restrict scraping to that one site.
    """
    async with _campaign_lock:
        _stop_requested.clear()
        await _run_campaign_body(update, ctx, source=source)


_SOURCE_LABELS = {
    "leboncoin":     "LBC",
    "seloger":       "SeLoger",
    "pap":           "PAP",
    "bienici":       "Bien'ici",
    "logicimmo":     "Logic-Immo",
    "studapart":     "Studapart",
    "parisattitude": "Paris Attitude",
    "lodgis":        "Lodgis",
    "immojeune":     "ImmoJeune",
    "locservice":    "LocService",
    "roomlala":      "Roomlala",
}


def _source_url(source: str) -> str:
    """Map a normalised source name to its configured search URL ('' if disabled)."""
    return {
        "leboncoin":     config.DEFAULT_SEARCH_URL,
        "seloger":       config.DEFAULT_SEARCH_SELOGER_URL,
        "pap":           config.DEFAULT_SEARCH_PAP_URL,
        "bienici":       config.DEFAULT_SEARCH_BIENICI_URL,
        "logicimmo":     config.DEFAULT_SEARCH_LOGICIMMO_URL,
        "studapart":     config.DEFAULT_SEARCH_STUDAPART_URL,
        "parisattitude": config.DEFAULT_SEARCH_PARISATTITUDE_URL,
        "lodgis":        config.DEFAULT_SEARCH_LODGIS_URL,
        "immojeune":     config.DEFAULT_SEARCH_IMMOJEUNE_URL,
        "locservice":    config.DEFAULT_SEARCH_LOCSERVICE_URL,
        "roomlala":      config.DEFAULT_SEARCH_ROOMLALA_URL,
    }.get(source, "")


def _campaign_sources(only: str | None = None) -> list[tuple[str, str]]:
    """Return [(url, label)] for each enabled source.

    If `only` is set, restrict to that single source (still subject to URL/creds).
    Empty URL skips a source.
    """
    if only:
        url = _source_url(only)
        if not url:
            return []
        return [(url, _SOURCE_LABELS.get(only, only))]

    sources: list[tuple[str, str]] = [
        (config.DEFAULT_SEARCH_URL, "LBC"),
        (config.DEFAULT_SEARCH_PAP_URL, "PAP"),
        (config.DEFAULT_SEARCH_BIENICI_URL, "Bien'ici"),
        (config.DEFAULT_SEARCH_LOGICIMMO_URL, "Logic-Immo"),
        (config.DEFAULT_SEARCH_STUDAPART_URL, "Studapart"),
        (config.DEFAULT_SEARCH_PARISATTITUDE_URL, "Paris Attitude"),
        (config.DEFAULT_SEARCH_LODGIS_URL, "Lodgis"),
        (config.DEFAULT_SEARCH_IMMOJEUNE_URL, "ImmoJeune"),
        (config.DEFAULT_SEARCH_LOCSERVICE_URL, "LocService"),
        (config.DEFAULT_SEARCH_ROOMLALA_URL, "Roomlala"),
    ]
    if config.SELOGER_EMAIL:
        sources.insert(1, (config.DEFAULT_SEARCH_SELOGER_URL, "SeLoger"))
    return [(url, label) for url, label in sources if url]


async def _run_campaign_body(
    update: Update, ctx: ContextTypes.DEFAULT_TYPE, source: str | None = None
) -> None:
    sources = _campaign_sources(only=source)
    if not sources:
        if source:
            await _reply(
                update,
                f"⚠️ Source `{source}` inconnue ou désactivée. "
                f"Sources disponibles : {', '.join(_SOURCE_LABELS.values())}.",
            )
        else:
            await _reply(update, "⚠️ Aucune source configurée.")
        return

    # ─── Live progress board ────────────────────────────────────────────────
    # Edit one Telegram message in place as each source completes, so the user
    # has visibility on progress instead of 5+ min of silence during scraping.
    title = (
        f"🚀 *Campagne {sources[0][1]} lancée*"
        if source else
        "🚀 *Campagne lancée*"
    )

    def _board(states: list[str]) -> str:
        return f"{title} — scraping en cours…\n\n" + "\n".join(states)

    states = [f"⏳ {label} : en attente" for _u, label in sources]
    try:
        status_msg = await update.effective_message.reply_text(
            _board(states), parse_mode=ParseMode.MARKDOWN,
        )
        chat_id = update.effective_chat.id
        if chat_id is not None:
            _TURN_REPLIES.setdefault(chat_id, []).append(_board(states))
    except Exception as exc:
        logger.warning("Could not send progress board (%s) — falling back", exc)
        status_msg = None

    async def _refresh_board() -> None:
        if status_msg is None:
            return
        try:
            await status_msg.edit_text(_board(states), parse_mode=ParseMode.MARKDOWN)
        except Exception:
            pass  # Telegram rate-limits edits; ignore non-critical failures

    # Per-source timeout. Camoufox cold-start (~15-30s) + page load (~30s) +
    # post_delay (~5s) easily exceeds 90s. Bien'ici has a 3-stage fallback
    # chain (Playwright → Camoufox → XHR intercept) that needs even more.
    _TIMEOUT_BY_LABEL = {
        # Pure Playwright + stealth — fast (~20-40s typical)
        "LBC": 90.0,
        "PAP": 90.0,
        "Logic-Immo": 90.0,
        # Camoufox-based — needs cold-start headroom
        "Studapart": 180.0,
        "Lodgis": 180.0,
        "ImmoJeune": 180.0,
        "LocService": 180.0,
        "Paris Attitude": 120.0,  # Playwright-only but heavy page
        # Bien'ici: now uses Camoufox + commit-strategy, ~17s typical
        "Bien'ici": 90.0,
        "SeLoger": 180.0,  # Camoufox + LZString decode
    }
    DEFAULT_TIMEOUT = 90.0

    from scraper import detect_housing_type

    async def _score_listings_parallel(items: list, concurrency: int = 8) -> None:
        """Score every listing in parallel, semaphore-bounded to avoid rate limits.

        Mutates each Listing in-place (sets .score and .score_reason) and
        persists to DB via set_listing_score so dashboard/Sheets see it.
        Failures are logged and skipped — don't block the campaign.
        """
        sem = asyncio.Semaphore(concurrency)
        n_done = [0]
        n_total = len(items)

        async def _one(lst):
            async with sem:
                try:
                    result = await score_listing(lst)
                    lst.score = result["score"]
                    lst.score_reason = result["reason"]
                    database.set_listing_score(
                        lst.lbc_id, result["score"], result["reason"],
                    )
                except Exception as exc:
                    logger.warning("Score failed for %s: %s", lst.lbc_id, exc)
                finally:
                    n_done[0] += 1

        await asyncio.gather(*(_one(l) for l in items))
        logger.info("Scored %d/%d listings", n_done[0], n_total)

    def _persist_batch(batch: list) -> int:
        """Upsert a batch of just-scraped listings to DB. Called per-source so
        partial campaigns (timeout / abort mid-flight) don't lose data."""
        n = 0
        for lst in batch:
            try:
                htype, n_room = detect_housing_type(lst.title or "", lst.description or "")
                database.upsert_listing(
                    lbc_id=lst.lbc_id, title=lst.title, price=lst.price,
                    location=lst.location, seller_name=lst.seller_name,
                    seller_type="", url=lst.url, source=lst.source,
                    surface=lst.surface, housing_type=htype, roommate_count=n_room,
                )
                n += 1
            except Exception as exc:
                logger.warning("Failed to persist listing %s: %s", lst.lbc_id, exc)
        return n

    # Open ONE Camoufox browser shared across all Camoufox-using sources.
    # Without this, Studapart + Lodgis + ImmoJeune + LocService + Bien'ici +
    # SeLoger all cold-start Firefox separately (5-6 × 30s = ~3 min just on
    # cold-starts). Shared browser cuts that to a single 30s cold-start.
    _CAMOUFOX_LABELS = {"Studapart", "Lodgis", "ImmoJeune", "LocService", "Bien'ici", "SeLoger"}
    needs_camoufox = any(label in _CAMOUFOX_LABELS for _, label in sources)
    camoufox_cm = None
    if needs_camoufox:
        try:
            from camoufox.async_api import AsyncCamoufox
            import scraper as _scraper_mod
            camoufox_cm = AsyncCamoufox(headless=False, locale=["fr-FR"], os="windows")
            _scraper_mod._SHARED_CAMOUFOX_BROWSER = await camoufox_cm.__aenter__()
            logger.info("Shared Camoufox browser opened for the campaign")
        except Exception as exc:
            logger.warning("Could not open shared Camoufox (%s) — sources will cold-start", exc)
            camoufox_cm = None

    per_source: list[tuple[str, int, list]] = []
    listings: list = []
    for i, (url, label) in enumerate(sources):
        states[i] = f"🔄 *{label}* : en cours…"
        await _refresh_board()
        timeout = _TIMEOUT_BY_LABEL.get(label, DEFAULT_TIMEOUT)
        t0 = time.time()
        try:
            results = await asyncio.wait_for(
                search_listings(url, max_results=25),
                timeout=timeout,
            )
        except asyncio.TimeoutError:
            elapsed = time.time() - t0
            logger.warning("%s scrape timed out after %.1fs (limit %.0fs)", label, elapsed, timeout)
            results = []
            states[i] = f"⏱ {label} : timeout (>{timeout:.0f}s)"
        except Exception as exc:
            logger.error("%s scrape failed: %s", label, exc)
            results = []
            states[i] = f"❌ {label} : échec ({type(exc).__name__})"
        else:
            n = len(results)
            elapsed = time.time() - t0
            logger.info("%s scrape done in %.1fs: %d listings", label, elapsed, n)
            if n == 0:
                states[i] = f"⚪ {label} : 0 annonce ({elapsed:.0f}s)"
            else:
                states[i] = f"✅ {label} : *{n}* annonce{'s' if n > 1 else ''} ({elapsed:.0f}s)"
        # Persist immediately — survives subsequent source timeouts / aborts
        if results:
            _persist_batch(results)
        await _refresh_board()
        per_source.append((label, len(results), results))
        listings.extend(results)

    # Close the shared Camoufox browser once all sources have run
    if camoufox_cm is not None:
        try:
            import scraper as _scraper_mod
            _scraper_mod._SHARED_CAMOUFOX_BROWSER = None
            await camoufox_cm.__aexit__(None, None, None)
            logger.info("Shared Camoufox browser closed")
        except Exception as exc:
            logger.warning("Failed to close shared Camoufox: %s", exc)

    listings = _deduplicate(listings)

    # Final scrape-phase update on the same message
    if status_msg is not None:
        final_board = (
            f"{title} — scraping terminé.\n\n" + "\n".join(states)
            + f"\n\n📋 *{len(listings)} annonces uniques* après dédup."
        )
        try:
            await status_msg.edit_text(final_board, parse_mode=ParseMode.MARKDOWN)
        except Exception:
            pass

    if not listings:
        await _reply(update, "😕 Aucune annonce à analyser.")
        return

    # Listings already persisted per-source above (see _persist_batch).
    # Now score EVERY listing in parallel (not just contact-eligible ones), so
    # the dashboard / Sheets / /rapport_complet show scores on all rows
    # including over-budget ones the user wants to browse.
    if config.ENABLE_SCORING:
        await _reply(update, f"🎯 Scoring de *{len(listings)}* annonces en parallèle…")
        await _score_listings_parallel(listings)

    await _reply(update, f"🧠 Analyse de *{len(listings)}* annonces en cours…")

    sent = 0  # count of messages PREPARED in this run
    skipped = {
        "budget": 0,
        "qualité": 0,
        "suspect": 0,
        "déjà_préparée": 0,
        "type_logement": 0,   # coloc/coliving/chambre — Illan veut un appart privé pour 2
        "dossier": 0,         # prescreen rejected (only if ENABLE_PRESCREENING)
        "score_bas": 0,       # score < MIN_SCORE (only if ENABLE_SCORING)
    }
    errors = 0

    for listing in listings:
        if _stop_requested.is_set():
            await _reply(update, "🛑 Campagne arrêtée manuellement.")
            break

        eligible, category, reason = _should_contact(listing)
        if not eligible:
            logger.info("Listing %s skipped (%s): %s", listing.lbc_id, category, reason)
            skipped[category] = skipped.get(category, 0) + 1
            continue

        # Optional dossier pre-screening
        if config.ENABLE_PRESCREENING:
            prescreen = await prescreen_listing(listing)
            if not prescreen["eligible"]:
                logger.info("Ineligible listing %s: %s", listing.lbc_id, prescreen["note"])
                skipped["dossier"] += 1
                await _reply(
                    update,
                    f"⚠️ Dossier incompatible → _{_escape_md(listing.title)}_ : _{_escape_md(prescreen['note'])}_",
                )
                continue

        try:
            # Score was computed upfront by _score_listings_parallel — read
            # from the listing in-memory (no extra LLM call here).
            score_result = None
            if config.ENABLE_SCORING:
                score_value = getattr(listing, "score", None)
                score_reason = getattr(listing, "score_reason", "") or ""
                if score_value is None:
                    # Defensive fallback — shouldn't normally happen
                    s = await score_listing(listing)
                    score_value = s["score"]
                    score_reason = s["reason"]
                score_result = {"score": score_value, "reason": score_reason}
                if score_value < config.MIN_SCORE:
                    skipped["score_bas"] += 1
                    logger.info(
                        "Listing %s skipped: score %d < %d (%s)",
                        listing.lbc_id, score_value, config.MIN_SCORE, score_reason,
                    )
                    continue

            result = await analyse_listing(listing)
            listing_id = database.upsert_listing(
                lbc_id=listing.lbc_id,
                title=listing.title,
                price=listing.price,
                location=listing.location,
                seller_name=listing.seller_name,
                seller_type=result.seller_type,
                url=listing.url,
                source=listing.source,
                surface=listing.surface,
            )
            # Persist score and fire high-interest alert
            if score_result is not None:
                database.set_listing_score(
                    listing.lbc_id, score_result["score"], score_result["reason"]
                )
                if score_result["score"] >= config.INTEREST_THRESHOLD:
                    await _reply(
                        update,
                        f"🔥 *ANNONCE INTÉRESSANTE* ⭐{score_result['score']}/10\n"
                        f"📍 _{_escape_md(listing.title)}_ ({_escape_md(listing.location)})\n"
                        f"💰 {listing.price}€/mois\n"
                        f"🔗 {listing.url}\n"
                        f"💡 _{score_result['reason']}_"
                    )

            # Prepare-only: persist as pending, no actual send
            database.create_contact(listing_id, result.message)
            sent += 1
            score_str = f" ⭐{score_result['score']}/10" if score_result else ""
            await _reply(
                update,
                f"📝 Préparé{score_str} → _{_escape_md(listing.title)}_ ({_escape_md(listing.location)})"
            )
        except Exception as exc:
            logger.error("Campaign error on %s: %s", listing.lbc_id, exc)
            errors += 1

    pending_total = database.count_pending_contacts()
    skip_label = {
        "budget": "hors budget",
        "qualité": "filtrées (qualité)",
        "suspect": "suspectes",
        "déjà_préparée": "déjà préparées",
        "type_logement": "coloc/coliving/chambre",
        "dossier": "dossier incompatible",
        "score_bas": f"score < {config.MIN_SCORE}",
    }
    skip_lines = [
        f"  • {n} {skip_label[k]}"
        for k, n in skipped.items() if n > 0
    ]
    skip_block = "\n".join(skip_lines) if skip_lines else "  • aucune"
    await _reply(
        update,
        f"🏁 *Campagne terminée — phase préparation*\n\n"
        f"📝 Messages préparés : *{sent}*\n"
        f"⏭ Annonces écartées :\n{skip_block}\n"
        f"❌ Erreurs : {errors}\n\n"
        f"📤 *{pending_total} message(s) en attente d'envoi*\n"
        f"Tape /envoyer (ou « envoie ») quand tu veux les envoyer.",
    )

    # Check for price drops on previously contacted listings
    drops = database.get_price_drops()
    if drops:
        lines = ["💸 *Baisses de prix détectées :*\n"]
        for d in drops[:5]:
            lines.append(
                f"• {d['title']} — {d['old_price']}€ → *{d['new_price']}€* "
                f"(-{d['old_price'] - d['new_price']}€)\n  {d['url']}"
            )
            database.clear_price_prev(d.get("lbc_id", ""))
        await _reply(update, "\n".join(lines))

    # Smart re-contact: listings never messaged that just dropped into budget
    uncontacted_drops = database.get_uncontacted_price_drops(PROFILE["search"]["max_rent"])
    if uncontacted_drops:
        await _reply(
            update,
            f"💸 *{len(uncontacted_drops)} annonce(s)* jamais contactée(s) sont passées sous budget — préparation en cours…"
        )
        for drop in uncontacted_drops:
            try:
                listing = await fetch_single_listing(drop["url"])
                if not listing:
                    continue
                suspicious, _ = is_suspicious(listing)
                if suspicious:
                    continue
                result = await analyse_listing(listing)
                listing_id = database.upsert_listing(
                    lbc_id=listing.lbc_id, title=listing.title, price=listing.price,
                    location=listing.location, seller_name=listing.seller_name,
                    seller_type=result.seller_type, url=listing.url, source=listing.source,
                )
                database.create_contact(listing_id, result.message)
                await _reply(
                    update,
                    f"💸📝 Préparé (baisse) → _{_escape_md(listing.title)}_ "
                    f"({drop['price_prev']}€ → *{drop['price']}€*)"
                )
            except Exception as exc:
                logger.error("Smart re-contact prep failed for %s: %s", drop["url"], exc)

    # Auto-sync to Google Sheets if configured
    if config.SYNC_AFTER_CAMPAIGN:
        try:
            import sheets_sync
            if sheets_sync.is_configured():
                summary = await asyncio.to_thread(sheets_sync.sync_listings)
                await _reply(
                    update,
                    f"📤 Sheets sync : {summary['updated']} màj, "
                    f"{summary['appended']} nouvelles."
                )
        except Exception as exc:
            logger.warning("Auto Sheets sync failed (non-fatal): %s", exc)

    # Check LBC inbox for new replies
    try:
        new_replies = await check_inbox_lbc()
        if new_replies:
            lines = [f"📬 *{len(new_replies)} nouvelle(s) réponse(s) LBC :*\n"]
            for r in new_replies[:5]:
                lines.append(f"• *{r['sender']}* : _{r['preview'][:80]}_\n  {r['thread_url']}")
            await _reply(update, "\n".join(lines))
    except Exception as exc:
        logger.warning("Inbox check failed: %s", exc)


# ─── /campagne ────────────────────────────────────────────────────────────────

async def cmd_campagne(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    if _campaign_lock.locked():
        await _reply(update, "⚠️ Une campagne est déjà en cours. Utilisez /stop pour l'arrêter.")
        return
    # Optional source filter via CLI: /campagne studapart
    source = (ctx.args[0].lower().strip() if ctx.args else None) or None
    await _run_campaign_core(update, ctx, source=source)


# ─── /envoyer (asks for confirmation) + /confirmer (actually sends) ──────────

_SEND_CONFIRM_KEY = "send_confirm"  # bot_data flag set by /envoyer, drained by /confirmer


async def cmd_envoyer(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Step 1 of send: confirm the user really wants to send.

    Sets a TTL flag in bot_data; user must run /confirmer (or reply 'oui')
    within 30 min to actually trigger the send.
    """
    pending = database.get_pending_contacts()
    if not pending:
        await _reply(update, "📭 Aucun message en attente d'envoi. Lance /campagne d'abord.")
        return

    _set_ttl(ctx.bot_data, _SEND_CONFIRM_KEY, len(pending))

    preview_lines = []
    for c in pending[:3]:
        preview_lines.append(f"  • _{_escape_md(c['title'])}_ ({_escape_md(c['location'])})")
    preview = "\n".join(preview_lines)
    if len(pending) > 3:
        preview += f"\n  … et {len(pending) - 3} autres"

    await _reply(
        update,
        f"📤 *Confirmation requise*\n\n"
        f"*{len(pending)} message(s)* prêts à être envoyés sur LeBonCoin / SeLoger :\n"
        f"{preview}\n\n"
        f"➡️ Tape /confirmer (ou « *oui* » / « *go* » / « *vas-y* ») pour lancer l'envoi.\n"
        f"➡️ Tape autre chose pour annuler.",
    )


async def cmd_confirmer(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Step 2 of send: drain the pending queue if user has confirmed."""
    confirmed = _pop_ttl(ctx.bot_data, _SEND_CONFIRM_KEY)
    if not confirmed:
        await _reply(
            update,
            "ℹ️ Aucun envoi en attente de confirmation. "
            "Lance /envoyer d'abord pour préparer l'envoi.",
        )
        return
    await _drain_pending_queue(update, ctx)


async def _drain_pending_queue(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Actually iterate pending contacts and send via Playwright."""
    pending = database.get_pending_contacts()
    if not pending:
        await _reply(update, "📭 La file est vide (peut-être déjà envoyée ?).")
        return

    await _reply(update, f"📤 Envoi de *{len(pending)}* message(s)…")

    sent = 0
    skipped_rate = 0
    errors = 0

    for c in pending:
        if _stop_requested.is_set():
            await _reply(update, "🛑 Envoi arrêté manuellement.")
            break

        if database.messages_sent_last_hour() >= config.MAX_MESSAGES_PER_HOUR:
            skipped_rate += 1
            await _reply(
                update,
                f"⏸ Limite horaire atteinte ({config.MAX_MESSAGES_PER_HOUR}/h). "
                "Pause 1 minute…",
            )
            await asyncio.sleep(60)
            continue

        try:
            success = await send_message_safe(c["url"], c["message"], c["contact_id"])
            if success:
                sent += 1
                await _reply(update, f"✉️ Envoyé → _{_escape_md(c['title'])}_ ({_escape_md(c['location'])})")
            else:
                errors += 1
        except Exception as exc:
            logger.error("Send failed for contact %s: %s", c["contact_id"], exc)
            errors += 1

        await asyncio.sleep(3)

    remaining = database.count_pending_contacts()
    await _reply(
        update,
        f"🏁 *Envoi terminé*\n\n"
        f"✉️ Envoyés : *{sent}*\n"
        f"⏸ Limite horaire (reportés) : {skipped_rate}\n"
        f"❌ Erreurs : {errors}\n"
        f"📤 Restants en attente : {remaining}",
    )


# ─── /pending (list pending contacts) + /recent (list recent listings) ───────

async def cmd_list_pending(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Surface the REAL pending-contact list from DB. Anti-hallucination tool."""
    pending = database.get_pending_contacts()
    if not pending:
        await _reply(update, "📭 Aucun message en attente d'envoi. Lance /campagne d'abord.")
        return
    lines = [f"📤 *{len(pending)} annonce(s) prête(s) à envoyer :*\n"]
    for i, c in enumerate(pending[:20], 1):
        title = _escape_md(c["title"] or "")[:60]
        location = _escape_md(c["location"] or "")[:35]
        lines.append(f"*{i}.* _{title}_ ({location})\n   {c['url']}")
    if len(pending) > 20:
        lines.append(f"\n_… et {len(pending) - 20} autres._")
    await _reply(update, "\n".join(lines))


async def cmd_list_recent(update: Update, ctx: ContextTypes.DEFAULT_TYPE, limit: int = 10) -> None:
    """Surface the most-recently-scraped listings from DB. Anti-hallucination tool."""
    limit = max(1, min(int(limit or 10), 30))
    listings = database.get_recent_listings(limit)
    if not listings:
        await _reply(update, "📭 Aucune annonce dans la base. Lance /campagne ou /search d'abord.")
        return
    lines = [f"📋 *{len(listings)} annonce(s) récente(s) en base :*\n"]
    for i, l in enumerate(listings, 1):
        title = _escape_md(l["title"] or "")[:60]
        location = _escape_md(l["location"] or "")[:30]
        price = f"{l['price']}€" if l["price"] else "?€"
        surface = f" · {l['surface']}m²" if l.get("surface") else ""
        score = f" ⭐{l['score']}/10" if l.get("score") else ""
        lines.append(f"*{i}.* _{title}_ ({location}) — *{price}*{surface}{score}\n   {l['url']}")
    await _reply(update, "\n".join(lines))


# ─── /rapport_complet — flexible custom report ─────────────────────────────────

_SORT_LABELS = {
    "surface": "surface (m² desc)",
    "price":   "prix (€ asc)",
    "score":   "score (desc)",
    "recent":  "récent (insertion desc)",
}


async def cmd_query(
    update: Update,
    ctx: ContextTypes.DEFAULT_TYPE,
    *,
    source: str | None = None,
    min_price: int | None = None,
    max_price: int | None = None,
    min_surface: int | None = None,
    max_surface: int | None = None,
    sort_by: str = "surface",
    group_by_source: bool = False,
    limit: int = 50,
) -> None:
    """Custom listing report from DB. Filters + sort + optional grouping by source."""
    try:
        listings = database.query_listings(
            source=source,
            min_price=min_price,
            max_price=max_price,
            min_surface=min_surface,
            max_surface=max_surface,
            sort_by=sort_by,
            limit=limit,
        )
    except ValueError as exc:
        await _reply(update, f"⚠️ Paramètre invalide : {exc}")
        return

    if not listings:
        await _reply(
            update,
            "📭 Aucune annonce ne matche ces filtres. "
            "Lance /campagne d'abord pour remplir la base.",
        )
        return

    # Header — describe the filters applied so the user knows what they got
    filters: list[str] = []
    if source:
        filters.append(f"source={source}")
    if min_price is not None:
        filters.append(f"≥{min_price}€")
    if max_price is not None:
        filters.append(f"≤{max_price}€")
    if min_surface is not None:
        filters.append(f"≥{min_surface}m²")
    if max_surface is not None:
        filters.append(f"≤{max_surface}m²")
    filter_str = " · ".join(filters) if filters else "aucun filtre"

    lines = [
        f"📊 *{len(listings)} annonce(s)* — _{filter_str}_, tri par "
        f"_{_SORT_LABELS.get(sort_by, sort_by)}_\n",
    ]

    def _fmt_listing(l: dict) -> str:
        title = _escape_md(l["title"] or "")[:55]
        location = _escape_md(l["location"] or "")[:25]
        price = f"*{l['price']}€*" if l["price"] else "?€"
        surface = f" · {l['surface']}m²" if l.get("surface") else ""
        score = f" ⭐{l['score']}/10" if l.get("score") else ""
        return f"  • {title} ({location}) — {price}{surface}{score}\n    {l['url']}"

    if group_by_source:
        from collections import defaultdict
        groups: dict[str, list[dict]] = defaultdict(list)
        for l in listings:
            groups[l["source"] or "?"].append(l)
        # Each group keeps the global sort order
        for src, items in sorted(groups.items()):
            lines.append(f"\n*— {_SOURCE_LABELS.get(src, src)} ({len(items)}) —*")
            for l in items:
                lines.append(_fmt_listing(l))
    else:
        for l in listings:
            lines.append(_fmt_listing(l))

    await _reply(update, "\n".join(lines))


# ─── /score_all — backfill scores for any un-scored listing ──────────────────

async def cmd_score_all(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Score every listing in DB that doesn't have a score yet.

    Useful after preferences.py changes (re-score with the new prompt)
    or to backfill listings persisted before scoring was enabled.
    """
    if not config.ENABLE_SCORING:
        await _reply(
            update,
            "⚠️ ENABLE_SCORING=false dans .env — active le scoring puis relance.",
        )
        return

    unscored = database.get_unscored_listings()
    if not unscored:
        await _reply(update, "✅ Toutes les annonces ont déjà un score.")
        return

    await _reply(
        update,
        f"🎯 Scoring de *{len(unscored)}* annonce(s) sans score (parallèle, semaphore=8)…\n"
        f"Coût estimé : ~{len(unscored) * 0.005:.2f}$ via DeepSeek.",
    )

    # Build minimal Listing-like objects for score_listing
    from agent import Listing
    items = []
    for r in unscored:
        items.append(Listing(
            lbc_id=r["lbc_id"], title=r["title"] or "", description="",
            price=r["price"] or 0, location=r["location"] or "",
            seller_name="", url=r["url"] or "",
            source=r["source"] or "", surface=r.get("surface"),
            housing_type=r.get("housing_type") or "",
            roommate_count=r.get("roommate_count"),
        ))

    sem = asyncio.Semaphore(8)
    n_done = [0]
    n_total = len(items)

    async def _one(lst):
        async with sem:
            try:
                result = await score_listing(lst)
                database.set_listing_score(
                    lst.lbc_id, result["score"], result["reason"],
                )
            except Exception as exc:
                logger.warning("Backfill score failed for %s: %s", lst.lbc_id, exc)
            finally:
                n_done[0] += 1

    await asyncio.gather(*(_one(l) for l in items))
    await _reply(
        update,
        f"✅ Backfill terminé : *{n_done[0]}/{n_total}* annonces scorées.",
    )


# ─── /sync — Google Sheets sync ──────────────────────────────────────────────

async def cmd_sync_sheet(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Push current DB listings to the configured Google Sheet."""
    import sheets_sync
    if not sheets_sync.is_configured():
        await _reply(
            update,
            "⚠️ Google Sheets sync n'est pas configuré.\n"
            "Renseigne `GOOGLE_SHEET_ID` et `GOOGLE_SERVICE_ACCOUNT_JSON` "
            "dans `.env`. Voir `sheets_sync.py` pour les étapes.",
        )
        return
    await _reply(update, "📤 Synchronisation Google Sheets en cours…")
    try:
        summary = await asyncio.to_thread(sheets_sync.sync_listings)
    except FileNotFoundError as exc:
        await _reply(update, f"⚠️ {exc}")
        return
    except Exception as exc:
        logger.error("Sheets sync failed: %s", exc)
        await _reply(update, f"❌ Erreur sync : `{exc}`")
        return
    await _reply(
        update,
        f"✅ Sync terminée : {summary['updated']} mises à jour, "
        f"{summary['appended']} ajouts, {summary['total']} annonces totales.",
    )


# ─── /rapport ─────────────────────────────────────────────────────────────────

async def cmd_rapport(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    stats = database.today_stats()
    rates = database.tone_response_rates()
    stale = database.stale_contacts_count(config.STALE_DAYS)
    s = PROFILE["search"]
    zones_short = ", ".join(s["zones"][:2]) + "…"

    rate_lines = []
    for tone_type, data in rates.items():
        emoji = "👤" if tone_type == "particulier" else "🏢"
        rate_lines.append(
            f"{emoji} {tone_type.capitalize()}: {data['rate']}% "
            f"({data['responded']}/{data['sent']} réponses)"
        )
    rates_text = "\n".join(rate_lines) if rate_lines else "Pas encore de données"

    await _reply(
        update,
        f"📊 *RAPPORT IMMOBILIER*\n\n"
        f"🎯 Critères : Appart meublé, {s['min_surface']}m²+, max {s['max_rent']}€, {zones_short}\n"
        f"📅 Période : aujourd'hui\n\n"
        f"📋 Annonces scrapées : {stats['scraped']}\n"
        f"✉️ Messages envoyés : {stats['sent']}\n"
        f"✅ Réponses positives : {stats['positive']}\n"
        f"❌ Réponses négatives : {stats['negative']}\n"
        f"🔇 Sans réponse : {stats['no_response']} "
        f"(dont *{stale}* fantômes depuis +{config.STALE_DAYS}j)\n\n"
        f"📈 *Taux de réponse par type :*\n{rates_text}\n\n"
        f"🤖 Continuer la campagne demain ?",
    )


# ─── /stop ────────────────────────────────────────────────────────────────────

async def cmd_stop(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    if _campaign_lock.locked():
        _stop_requested.set()
        await _reply(update, "🛑 Campagne arrêtée.")
    else:
        await _reply(update, "ℹ️ Aucune campagne en cours.")


# ─── /autostart ───────────────────────────────────────────────────────────────

async def cmd_autostart(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    global _auto_task
    if _auto_task and not _auto_task.done():
        await _reply(update, "⚠️ Campagne automatique déjà en cours. Utilise /autostop d'abord.")
        return

    try:
        interval_hours = float(ctx.args[0]) if ctx.args else 3.0
    except (ValueError, IndexError):
        interval_hours = 3.0

    await _reply(
        update,
        f"🤖 *Campagne automatique activée* — toutes les {interval_hours:.0f}h\n"
        "Utilise /autostop pour l'arrêter."
    )

    async def _auto_loop():
        while True:
            logger.info("Auto-campaign starting…")
            await _run_campaign_core(update, ctx)
            logger.info("Auto-campaign done, sleeping %.1fh", interval_hours)
            await asyncio.sleep(interval_hours * 3600)

    _auto_task = asyncio.create_task(_auto_loop())


# ─── /autostop ────────────────────────────────────────────────────────────────

async def cmd_autostop(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    global _auto_task
    if _auto_task and not _auto_task.done():
        _auto_task.cancel()
        _auto_task = None
        await _reply(update, "🛑 Campagne automatique arrêtée.")
    else:
        await _reply(update, "ℹ️ Aucune campagne automatique en cours.")


# ─── /visite ──────────────────────────────────────────────────────────────────

async def cmd_visite(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Register a visit appointment: /visite <url> <date/time>"""
    if not ctx.args or len(ctx.args) < 2:
        await _reply(
            update,
            "Usage : `/visite <url_annonce> <date>`\n"
            "Exemple : `/visite https://www.leboncoin.fr/... Samedi 5 avril 10h`"
        )
        return

    url = ctx.args[0]
    date_str = " ".join(ctx.args[1:])
    visit_id = database.save_visit(url=url, date_str=date_str)
    await _reply(
        update,
        f"📅 *Visite enregistrée #{visit_id}*\n"
        f"🔗 {url}\n"
        f"🗓 {date_str}\n\n"
        "Je t'enverrai un rappel la veille."
    )


# ─── /visites ─────────────────────────────────────────────────────────────────

async def cmd_visites(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """List upcoming visits."""
    visits = database.get_upcoming_visits()
    if not visits:
        await _reply(update, "📅 Aucune visite planifiée.")
        return

    lines = ["📅 *Visites à venir :*\n"]
    for v in visits:
        lines.append(f"• #{v['id']} — {v['date_str']}\n  {v['url']}")
    await _reply(update, "\n".join(lines))


# ─── /watch + /unwatch ────────────────────────────────────────────────────────

# ─── /watch — two-pool poller with score-filtered notifications ──────────────
#
# Replaces the previous auto-contact behaviour. New listings are scored,
# filtered by INTEREST_THRESHOLD, and surfaced as Telegram messages with
# inline buttons "📤 Préparer" / "❌ Ignorer". Nothing is sent without
# user click. Two pools run in parallel:
#   - Playwright sources (LBC, PAP, Logic-Immo, Paris Attitude): every 5 min
#   - Camoufox sources (Studapart, Lodgis, ImmoJeune, LocService, Bien'ici,
#     SeLoger): every 15 min (cold-start tax = ~30s)

_PLAYWRIGHT_WATCH_LABELS = {"LBC", "PAP", "Logic-Immo", "Paris Attitude"}
_CAMOUFOX_WATCH_LABELS = {
    "Studapart", "Lodgis", "ImmoJeune", "LocService", "Bien'ici", "SeLoger",
}
_watch_tasks: list[asyncio.Task] = []
_watch_seen: set[str] = set()  # lbc_ids already alerted on (in-memory)


async def _watch_pool_loop(
    update: Update,
    ctx: ContextTypes.DEFAULT_TYPE,
    labels: set[str],
    interval_min: int,
    pool_name: str,
) -> None:
    """One pool: poll a subset of sources every `interval_min`, score new
    listings, alert on score >= INTEREST_THRESHOLD."""
    import scraper as _scraper_mod
    import random as _random
    while True:
        try:
            logger.info("[WATCH %s] poll starting", pool_name)
            sources = [(u, l) for u, l in _campaign_sources() if l in labels]
            if not sources:
                logger.info("[WATCH %s] no sources active in this pool", pool_name)
                await asyncio.sleep(interval_min * 60)
                continue

            # Open shared Camoufox if this pool needs it
            cm_handle = None
            if any(l in _CAMOUFOX_WATCH_LABELS for _u, l in sources):
                try:
                    from camoufox.async_api import AsyncCamoufox
                    cm_handle = AsyncCamoufox(headless=False, locale=["fr-FR"], os="windows")
                    _scraper_mod._SHARED_CAMOUFOX_BROWSER = await cm_handle.__aenter__()
                except Exception as exc:
                    logger.warning("[WATCH %s] could not open shared Camoufox: %s", pool_name, exc)
                    cm_handle = None

            all_listings: list = []
            for url, label in sources:
                try:
                    timeout = _TIMEOUT_BY_LABEL.get(label, DEFAULT_TIMEOUT)
                    results = await asyncio.wait_for(
                        search_listings(url, max_results=20), timeout=timeout,
                    )
                    all_listings.extend(results)
                except asyncio.TimeoutError:
                    logger.warning("[WATCH %s] %s timed out", pool_name, label)
                except Exception as exc:
                    logger.warning("[WATCH %s] %s failed: %s", pool_name, label, exc)

            # Close shared Camoufox
            if cm_handle is not None:
                try:
                    _scraper_mod._SHARED_CAMOUFOX_BROWSER = None
                    await cm_handle.__aexit__(None, None, None)
                except Exception:
                    pass

            all_listings = _deduplicate(all_listings)

            # Persist all (browsing visibility) — same as campaign
            from scraper import detect_housing_type
            for lst in all_listings:
                try:
                    htype, n_room = detect_housing_type(lst.title or "", lst.description or "")
                    database.upsert_listing(
                        lbc_id=lst.lbc_id, title=lst.title, price=lst.price,
                        location=lst.location, seller_name=lst.seller_name,
                        seller_type="", url=lst.url, source=lst.source,
                        surface=lst.surface, housing_type=htype, roommate_count=n_room,
                    )
                except Exception:
                    pass

            # New = not yet alerted on AND passes _should_contact
            new = []
            for lst in all_listings:
                if lst.lbc_id in _watch_seen:
                    continue
                eligible, _cat, _reason = _should_contact(lst)
                if not eligible:
                    _watch_seen.add(lst.lbc_id)  # don't re-eval next poll
                    continue
                new.append(lst)

            if new and config.ENABLE_SCORING:
                # Score the new ones in parallel
                sem = asyncio.Semaphore(8)
                async def _score_one(lst):
                    async with sem:
                        try:
                            result = await score_listing(lst)
                            lst.score = result["score"]
                            lst.score_reason = result["reason"]
                            database.set_listing_score(
                                lst.lbc_id, result["score"], result["reason"],
                            )
                        except Exception as exc:
                            logger.warning("[WATCH %s] score failed for %s: %s", pool_name, lst.lbc_id, exc)
                await asyncio.gather(*(_score_one(l) for l in new))

            # Surface high-score listings
            alerted = 0
            for lst in new:
                _watch_seen.add(lst.lbc_id)
                score = getattr(lst, "score", None) or 0
                if score >= config.INTEREST_THRESHOLD:
                    await _send_watch_alert(update, lst)
                    alerted += 1

            if alerted:
                logger.info("[WATCH %s] alerted on %d/%d new", pool_name, alerted, len(new))
            elif new:
                logger.info("[WATCH %s] %d new but none above threshold %d", pool_name, len(new), config.INTEREST_THRESHOLD)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error("[WATCH %s] pool iteration crashed: %s", pool_name, exc)

        # Jittered sleep ±20% to avoid robotic regularity (DataDome detection)
        jitter = _random.uniform(0.8, 1.2)
        await asyncio.sleep(interval_min * 60 * jitter)


async def _send_watch_alert(update: Update, listing) -> None:
    """Build the inline-keyboard alert and ship it to Telegram."""
    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton("📤 Préparer", callback_data=f"watch_prep:{listing.lbc_id}"),
        InlineKeyboardButton("❌ Ignorer", callback_data=f"watch_ignore:{listing.lbc_id}"),
    ]])
    htype = getattr(listing, "housing_type", "") or "?"
    surface = getattr(listing, "surface", None)
    surface_str = f"{surface}m²" if surface else "?m²"
    score = getattr(listing, "score", None) or "?"
    reason = (getattr(listing, "score_reason", "") or "")[:200]
    msg = (
        f"🔥 *NOUVELLE ANNONCE* ⭐{score}/10\n\n"
        f"📍 _{_escape_md(listing.title)}_\n"
        f"🏷 {htype} · {surface_str} · *{listing.price}€/mois*\n"
        f"📍 {_escape_md(listing.location)}\n"
        f"💡 _{_escape_md(reason)}_\n\n"
        f"🔗 {listing.url}"
    )
    try:
        await update.effective_message.reply_text(
            msg, parse_mode=ParseMode.MARKDOWN,
            reply_markup=keyboard, disable_web_page_preview=True,
        )
    except Exception as exc:
        logger.warning("Watch alert send failed: %s", exc)


async def cmd_watch(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Start the two-pool watcher. Notifications, no auto-send."""
    global _watch_tasks
    if any(t and not t.done() for t in _watch_tasks):
        await _reply(update, "⚠️ Mode veille déjà actif. /unwatch pour l'arrêter.")
        return

    # Optional CLI args: /watch [fast_min] [slow_min]
    fast_min = 5
    slow_min = 15
    if ctx.args:
        try:
            fast_min = int(ctx.args[0])
            if len(ctx.args) > 1:
                slow_min = int(ctx.args[1])
        except (ValueError, TypeError):
            pass

    _watch_seen.clear()  # fresh start each /watch session
    await _reply(
        update,
        f"👁 *Mode veille activé*\n\n"
        f"🟢 Sources rapides (LBC, PAP, Logic-Immo, Paris Attitude) : "
        f"toutes les *{fast_min} min*\n"
        f"🟣 Sources Camoufox (Studapart, Lodgis, ImmoJeune, LocService, "
        f"Bien'ici, SeLoger) : toutes les *{slow_min} min*\n\n"
        f"🔔 Notification dès qu'une annonce a un score ≥ "
        f"*{config.INTEREST_THRESHOLD}/10*\n"
        f"📤 Tu confirmes la préparation via le bouton inline\n\n"
        f"`/unwatch` pour arrêter.",
    )

    _watch_tasks = [
        asyncio.create_task(_watch_pool_loop(
            update, ctx, _PLAYWRIGHT_WATCH_LABELS, fast_min, "fast",
        )),
        asyncio.create_task(_watch_pool_loop(
            update, ctx, _CAMOUFOX_WATCH_LABELS, slow_min, "slow",
        )),
    ]


async def cmd_unwatch(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    global _watch_tasks
    alive = [t for t in _watch_tasks if t and not t.done()]
    if not alive:
        await _reply(update, "ℹ️ Aucun mode veille actif.")
        return
    for t in alive:
        t.cancel()
    _watch_tasks = []
    await _reply(update, "👁 Mode veille désactivé.")


# ─── /boite ───────────────────────────────────────────────────────────────────

async def cmd_boite(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Check LBC inbox for new replies."""
    await _reply(update, "📬 Vérification de la boîte de réception LBC…")
    try:
        replies = await check_inbox_lbc()
        if not replies:
            await _reply(update, "📭 Aucune nouvelle réponse.")
            return
        lines = [f"📬 *{len(replies)} réponse(s) :*\n"]
        for r in replies[:10]:
            lines.append(f"• *{r['sender']}* : _{r['preview'][:100]}_\n  {r['thread_url']}")
        await _reply(update, "\n".join(lines))
    except Exception as exc:
        await _reply(update, f"❌ Erreur : `{exc}`")


# ─── Natural language chat handler ───────────────────────────────────────────

async def cmd_chat(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle any free-text message by classifying intent and routing to the right action."""
    user_text = update.effective_message.text
    chat_id = update.effective_message.chat_id
    logger.info("Chat message received: %s", user_text[:80])
    _TURN_REPLIES[chat_id] = []  # reset reply accumulator for this turn

    try:
        await _cmd_chat_inner(update, ctx, user_text, chat_id)
    finally:
        _commit_turn(chat_id, user_text)


async def _cmd_chat_inner(
    update: Update, ctx: ContextTypes.DEFAULT_TYPE, user_text: str, chat_id: int
) -> None:
    # ── Send-confirmation interception ─────────────────────────────────────────
    # If /envoyer was just invoked, the user's next message can be an affirmative
    # ("oui", "go", "vas-y", "confirme") to drain the queue, or anything else to
    # cancel the confirmation silently.
    if _get_ttl(ctx.bot_data, _SEND_CONFIRM_KEY) is not None:
        affirmative = re.search(
            r"\b(oui|ok|okay|go|vas[\s-]?y|confirme[rz]?|allez|envoie|c'est\s+bon)\b",
            user_text.strip().lower(),
        )
        if affirmative:
            _pop_ttl(ctx.bot_data, _SEND_CONFIRM_KEY)
            await _drain_pending_queue(update, ctx)
            return
        # User said something else → silently clear the flag and proceed normally
        _pop_ttl(ctx.bot_data, _SEND_CONFIRM_KEY)

    # ── Pending-edit interception ──────────────────────────────────────────────
    pending_key = f"pending_edit:{chat_id}"
    lbc_id = _pop_ttl(ctx.bot_data, pending_key)
    if lbc_id:
        result = _pop_ttl(ctx.bot_data, f"sim:{lbc_id}")
        if not result:
            await _reply(update, "❌ Session expirée, relancez /simulate.")
            return
        listing = result.listing
        custom_message = user_text.strip()
        listing_id = database.upsert_listing(
            lbc_id=listing.lbc_id,
            title=listing.title,
            price=listing.price,
            location=listing.location,
            seller_name=listing.seller_name,
            seller_type=result.seller_type,
            url=listing.url,
            source=listing.source,
            surface=listing.surface,
        )
        contact_id = database.create_contact(listing_id, custom_message)
        await _reply(update, "📤 Envoi du message modifié…")
        success = await send_message_safe(listing.url, custom_message, contact_id)
        if success:
            await _reply(update, f"✅ Message envoyé (édité) → {listing.url}")
        else:
            await _reply(update, "❌ Échec de l'envoi (voir logs). Vérifiez vos identifiants LBC.")
        return

    intent = classify_intent(user_text, history=_history_for(chat_id))
    tool = intent.get("tool")

    if tool == "run_search":
        source = (intent.get("source") or "").strip().lower()
        url = (intent.get("url") or "").strip()
        if source:
            url = _source_url(source)
            if not url:
                await _reply(
                    update,
                    f"⚠️ La source `{source}` n'est pas configurée (URL vide). "
                    "Utilise une autre source ou colle l'URL exacte.",
                )
                return
        if not url:
            url = config.DEFAULT_SEARCH_URL  # fallback to LBC
        ctx.args = [url] if url != config.DEFAULT_SEARCH_URL else []
        await cmd_search(update, ctx)

    elif tool == "run_simulate":
        url = intent.get("url", "").strip()
        if not url:
            await _reply(update, "🔗 Envoie-moi l'URL de l'annonce LeBonCoin à analyser.")
            return
        ctx.args = [url]
        await cmd_simulate(update, ctx)

    elif tool == "run_campagne":
        source = (intent.get("source") or "").strip().lower()
        ctx.args = [source] if source else []
        await cmd_campagne(update, ctx)

    elif tool == "run_envoyer":
        await cmd_envoyer(update, ctx)

    elif tool == "list_pending":
        await cmd_list_pending(update, ctx)

    elif tool == "list_recent":
        limit = intent.get("limit") or 10
        await cmd_list_recent(update, ctx, limit=int(limit))

    elif tool == "sync_sheet":
        await cmd_sync_sheet(update, ctx)

    elif tool == "score_all":
        await cmd_score_all(update, ctx)

    elif tool == "query_listings":
        await cmd_query(
            update, ctx,
            source=(intent.get("source") or "").strip().lower() or None,
            min_price=int(intent["min_price"]) if intent.get("min_price") is not None else None,
            max_price=int(intent["max_price"]) if intent.get("max_price") is not None else None,
            min_surface=int(intent["min_surface"]) if intent.get("min_surface") is not None else None,
            max_surface=int(intent["max_surface"]) if intent.get("max_surface") is not None else None,
            sort_by=(intent.get("sort_by") or "recent"),
            group_by_source=bool(intent.get("group_by_source", False)),
            limit=int(intent.get("limit") or 50),
        )

    elif tool == "run_rapport":
        await cmd_rapport(update, ctx)

    elif tool == "run_stop":
        await cmd_stop(update, ctx)

    elif tool == "run_settings":
        await cmd_settings(update, ctx)

    elif tool == "run_autostart":
        hours = intent.get("hours")
        ctx.args = [str(hours)] if hours else []
        await cmd_autostart(update, ctx)

    elif tool == "run_autostop":
        await cmd_autostop(update, ctx)

    elif tool == "run_watch":
        minutes = intent.get("minutes")
        ctx.args = [str(int(minutes))] if minutes else []
        await cmd_watch(update, ctx)

    elif tool == "run_unwatch":
        await cmd_unwatch(update, ctx)

    elif tool == "run_visite":
        url = (intent.get("url") or "").strip()
        date = (intent.get("date") or "").strip()
        if not url or not date:
            await _reply(update, "🔗 Donne-moi l'URL de l'annonce et la date/heure de la visite.")
            return
        ctx.args = [url, *date.split()]
        await cmd_visite(update, ctx)

    elif tool == "run_visites":
        await cmd_visites(update, ctx)

    elif tool == "run_boite":
        await cmd_boite(update, ctx)

    elif tool == "reply":
        raw = intent.get("text", "Je n'ai pas compris, peux-tu reformuler ?")
        await _reply(update, _sanitize_reply_text(raw))

    else:
        await _reply(update, "Je n'ai pas compris. Envoie-moi une URL d'annonce ou décris ce que tu veux faire.")


# ─── Error handler ────────────────────────────────────────────────────────────

async def error_handler(update: object, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    logger.error("Telegram error: %s", ctx.error, exc_info=ctx.error)
    if isinstance(update, Update) and update.effective_message:
        await update.effective_message.reply_text(
            f"⚠️ Une erreur inattendue s'est produite : `{ctx.error}`",
            parse_mode=ParseMode.MARKDOWN,
        )


# ─── Entry point ──────────────────────────────────────────────────────────────

def main() -> None:
    database.init_db()
    # Idempotent cleanup: remove any mock fixtures that may have been
    # persisted before the upsert guard existed.
    database.purge_mock_listings()

    # Use longer timeouts + respect system proxy (fixes Windows firewall/proxy issues)
    request = HTTPXRequest(
        connect_timeout=30,
        read_timeout=30,
        write_timeout=30,
        connection_pool_size=8,
    )
    app = Application.builder().token(config.TELEGRAM_BOT_TOKEN).request(request).build()

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("settings", cmd_settings))
    app.add_handler(CommandHandler("search", cmd_search))
    app.add_handler(CommandHandler("simulate", cmd_simulate))
    app.add_handler(CommandHandler("campagne", cmd_campagne))
    app.add_handler(CommandHandler("envoyer", cmd_envoyer))
    app.add_handler(CommandHandler("confirmer", cmd_confirmer))
    app.add_handler(CommandHandler("pending", cmd_list_pending))
    app.add_handler(CommandHandler("recent", cmd_list_recent))
    app.add_handler(CommandHandler("sync", cmd_sync_sheet))
    app.add_handler(CommandHandler("score_all", cmd_score_all))

    async def _cmd_rapport_complet(update, ctx):
        """Default /rapport_complet: groups by source, sorts by surface."""
        await cmd_query(update, ctx, group_by_source=True, sort_by="surface", limit=100)

    app.add_handler(CommandHandler("rapport_complet", _cmd_rapport_complet))
    app.add_handler(CommandHandler("rapport", cmd_rapport))
    app.add_handler(CommandHandler("stop", cmd_stop))
    app.add_handler(CommandHandler("autostart", cmd_autostart))
    app.add_handler(CommandHandler("autostop", cmd_autostop))
    app.add_handler(CommandHandler("visite", cmd_visite))
    app.add_handler(CommandHandler("visites", cmd_visites))
    app.add_handler(CommandHandler("boite", cmd_boite))
    app.add_handler(CommandHandler("watch", cmd_watch))
    app.add_handler(CommandHandler("unwatch", cmd_unwatch))
    app.add_handler(CallbackQueryHandler(callback_handler))
    # Natural language fallback — catches any non-command text message
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, cmd_chat))
    app.add_error_handler(error_handler)

    logger.info("Bot starting…")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()

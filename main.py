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

async def _reply(update: Update, text: str, **kwargs) -> None:
    chat_id = update.effective_chat.id if update.effective_chat else None
    if chat_id is not None:
        _TURN_REPLIES.setdefault(chat_id, []).append(text)
    await update.effective_message.reply_text(
        text, parse_mode=ParseMode.MARKDOWN, **kwargs
    )


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
        await _reply(update, f"❌ Erreur Apify : `{exc}`")
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


def _should_contact(listing) -> tuple[bool, str, str]:
    """Centralised eligibility check shared by /campagne and /watch.

    Returns (eligible, category, fr_reason). Categories are stable identifiers
    used by the campaign report to break down skipped listings:
    'qualité' / 'budget' / 'suspect' / 'déjà_préparée'.
    Order matters: cheapest checks first, DB hit last.
    """
    if not is_real_offer(listing):
        return False, "qualité", "annonce filtrée"
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

    if source:
        await _reply(
            update,
            f"🚀 Campagne *{sources[0][1]}* lancée ! Scraping en cours…",
        )
    else:
        await _reply(update, "🚀 Campagne lancée ! Scraping en cours…")

    per_source: list[tuple[str, int, list]] = []
    listings: list = []
    for url, label in sources:
        try:
            results = await search_listings(url, max_results=25)
        except Exception as exc:
            logger.error("%s scrape failed: %s", label, exc)
            results = []
        per_source.append((label, len(results), results))
        listings.extend(results)

    listings = _deduplicate(listings)

    if not listings:
        await _reply(update, "😕 Aucune annonce trouvée.")
        return

    breakdown = " + ".join(f"{n} {label}" for label, n, _ in per_source if n)
    await _reply(
        update,
        f"📋 {breakdown}\n→ *{len(listings)} uniques* — analyse en cours…"
    )

    sent = 0  # count of messages PREPARED in this run
    skipped = {"budget": 0, "qualité": 0, "suspect": 0, "déjà_préparée": 0}
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
                skipped_dup += 1
                await _reply(update, f"⚠️ Dossier incompatible → _{listing.title}_ : _{prescreen['note']}_")
                continue

        try:
            # Optional scoring gate
            score_result = None
            if config.ENABLE_SCORING:
                score_result = await score_listing(listing)
                if score_result["score"] < config.MIN_SCORE:
                    skipped_dup += 1
                    logger.info(
                        "Listing %s skipped: score %d < %d (%s)",
                        listing.lbc_id, score_result["score"], config.MIN_SCORE,
                        score_result["reason"],
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
                        f"📍 _{listing.title}_ ({listing.location})\n"
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
                f"📝 Préparé{score_str} → _{listing.title}_ ({listing.location})"
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
                    f"💸📝 Préparé (baisse) → _{listing.title}_ "
                    f"({drop['price_prev']}€ → *{drop['price']}€*)"
                )
            except Exception as exc:
                logger.error("Smart re-contact prep failed for %s: %s", drop["url"], exc)

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
        preview_lines.append(f"  • _{c['title']}_ ({c['location']})")
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
                await _reply(update, f"✉️ Envoyé → _{c['title']}_ ({c['location']})")
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

async def _fast_poll_loop(update: Update, ctx: ContextTypes.DEFAULT_TYPE, interval_min: int) -> None:
    """
    Lightweight background poller.
    Scrapes all sources every `interval_min` minutes and immediately contacts
    any listings that are new (not yet in DB and not already contacted).
    """
    while True:
        logger.info("[WATCH] Fast poll starting…")
        all_listings: list = []

        sources = _campaign_sources()

        for source_url, source_name in sources:
            try:
                results = await search_listings(source_url, max_results=20)
                all_listings.extend(results)
            except Exception as exc:
                logger.warning("[WATCH] %s scrape failed: %s", source_name, exc)

        all_listings = _deduplicate(all_listings)
        new = []
        for lst in all_listings:
            eligible, _cat, _reason = _should_contact(lst)
            if eligible:
                new.append(lst)

        if new:
            await _reply(update, f"🆕 *{len(new)} nouvelle(s) annonce(s)* détectée(s) — contact en cours…")
            sent = 0
            for listing in new:
                if database.messages_sent_last_hour() >= config.MAX_MESSAGES_PER_HOUR:
                    await _reply(update, f"⏸ Limite horaire atteinte ({config.MAX_MESSAGES_PER_HOUR}/h).")
                    break
                try:
                    result = await analyse_listing(listing)
                    listing_id = database.upsert_listing(
                        lbc_id=listing.lbc_id, title=listing.title, price=listing.price,
                        location=listing.location, seller_name=listing.seller_name,
                        seller_type=result.seller_type, url=listing.url, source=listing.source,
                    )
                    contact_id = database.create_contact(listing_id, result.message)
                    success = await send_message_safe(listing.url, result.message, contact_id)
                    if success:
                        sent += 1
                        await _reply(update, f"✉️ → _{listing.title}_ ({listing.location})")
                except Exception as exc:
                    logger.error("[WATCH] Error on %s: %s", listing.lbc_id, exc)
                await asyncio.sleep(2)

            if sent:
                await _reply(update, f"✅ {sent} message(s) envoyé(s).")

        logger.info("[WATCH] Done — next poll in %d min", interval_min)
        await asyncio.sleep(interval_min * 60)


async def cmd_watch(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    global _watch_task
    if _watch_task and not _watch_task.done():
        await _reply(update, "⚠️ Mode veille déjà actif. Utilise /unwatch pour l'arrêter.")
        return
    try:
        interval = int(ctx.args[0]) if ctx.args else config.FAST_POLL_INTERVAL_MIN
    except (ValueError, IndexError):
        interval = config.FAST_POLL_INTERVAL_MIN
    await _reply(
        update,
        f"👁 *Mode veille activé* — scraping toutes les {interval} min\n"
        "Nouvelles annonces contactées automatiquement.\n"
        "Utilise /unwatch pour désactiver."
    )
    _watch_task = asyncio.create_task(_fast_poll_loop(update, ctx, interval))


async def cmd_unwatch(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    global _watch_task
    if _watch_task and not _watch_task.done():
        _watch_task.cancel()
        _watch_task = None
        await _reply(update, "👁 Mode veille désactivé.")
    else:
        await _reply(update, "ℹ️ Aucun mode veille actif.")


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
        await _reply(update, intent.get("text", "Je n'ai pas compris, peux-tu reformuler ?"))

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

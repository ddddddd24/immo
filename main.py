"""Telegram bot entry point — all commands handled here."""
import asyncio
import logging
import os
import re
import sys
import time
from datetime import datetime
from contextlib import nullcontext
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
from agent import analyse_listing, format_simulation_text, classify_intent, Listing, score_listing, score_listings_batch, prescreen_listing
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


def _lower_priority() -> None:
    """Reduce process footprint so the bot doesn't disturb interactive apps.

    BELOW_NORMAL priority + CPU affinity to cores 0-3 (out of 12 on a 5600X).
    Child processes (Camoufox/Playwright/Firefox/Chromium) inherit both on
    Windows, so this propagates automatically.
    """
    try:
        import psutil
        p = psutil.Process(os.getpid())
        if sys.platform == "win32":
            p.nice(psutil.BELOW_NORMAL_PRIORITY_CLASS)
            ncpu = psutil.cpu_count(logical=True) or 4
            p.cpu_affinity(list(range(min(4, ncpu))))
        else:
            p.nice(10)
        logger.info("[hygiene] priority=BELOW_NORMAL, affinity=%s", p.cpu_affinity())
    except Exception as e:
        logger.warning("[hygiene] could not lower priority: %s", e)


_lower_priority()

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
        "📡 Sources : LBC, SeLoger, PAP, Bien'ici, Studapart,\n"
        "Paris Attitude, Lodgis, ImmoJeune, LocService\n\n"
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


# ─── /add — manually inject a listing the search/SERP missed ─────────────────

async def cmd_add(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Take any listing URL (LBC, PAP, SeLoger, Bien'ici, …), fetch the detail
    page, persist it, optionally score it, and ping a high-score notif.

    Use case: user spotted an annonce on a site whose SERP/filters didn't
    surface it. `/add <url>` makes it appear in the dashboard like any
    auto-scraped listing.
    """
    if not ctx.args:
        await _reply(
            update,
            "Usage : `/add <url>`\n"
            "Exemple : `/add https://www.pap.fr/annonces/appartement-...-r123456789`",
        )
        return

    url = ctx.args[0].strip()
    if not url.startswith(("http://", "https://")):
        await _reply(update, "❌ URL invalide (doit commencer par http(s)://).")
        return

    await _reply(update, "🔍 Récupération de l'annonce…")

    # 1. Fetch detail (source auto-detected)
    listing = None
    try:
        listing = await fetch_single_listing(url)
    except Exception as exc:
        logger.warning("[/add] fetch_single_listing crashed for %s: %s", url, exc)

    # 2. Generic fallback: persist the URL even if parse failed
    if listing is None:
        from scraper import _fetch_generic_minimal
        listing = _fetch_generic_minimal(url)
        await _reply(
            update,
            "⚠️ Impossible d'extraire les détails — annonce enregistrée avec l'URL seule.",
        )

    # 3. Detect housing type from title/description (matches campaign flow)
    from scraper import detect_housing_type
    htype, n_room = detect_housing_type(listing.title or "", listing.description or "")
    if htype:
        listing.housing_type = htype
    if n_room is not None:
        listing.roommate_count = n_room

    # 4. Persist via the batch upsert (one-row "batch")
    row = {
        "lbc_id": listing.lbc_id,
        "title": listing.title,
        "price": listing.price,
        "location": listing.location,
        "seller_name": listing.seller_name,
        "seller_type": "",
        "url": listing.url,
        "source": listing.source,
        "surface": listing.surface,
        "housing_type": getattr(listing, "housing_type", "") or "",
        "roommate_count": getattr(listing, "roommate_count", None),
        "published_at": getattr(listing, "published_at", None),
        "phone": getattr(listing, "phone", None),
        "description": listing.description or "",
        "available_from": getattr(listing, "available_from", None),
    }
    try:
        database.upsert_listings_batch([row])
    except Exception as exc:
        logger.error("[/add] upsert failed: %s", exc)
        await _reply(update, f"❌ Échec de l'enregistrement DB : `{exc}`")
        return

    # 4b. Cross-source dedup — flag this row as a duplicate of an existing
    # primary if price/surface/location match a listing already in DB.
    try:
        database.apply_dedup_for_batch([row])
    except Exception as exc:
        logger.warning("[/add] dedup pass failed for %s: %s", listing.lbc_id, exc)

    # 5. Optional scoring (DeepSeek) when enabled
    score: int | None = None
    reason: str = ""
    if config.ENABLE_SCORING and listing.price and listing.price > 0:
        try:
            result = await score_listing(listing)
            score = int(result.get("score", 0))
            reason = (result.get("reason") or "")[:200]
            database.set_listing_score(listing.lbc_id, score, reason)
        except Exception as exc:
            logger.warning("[/add] scoring failed for %s: %s", listing.lbc_id, exc)

    # 6. Build summary message
    surface_str = f"{listing.surface}m²" if listing.surface else "?m²"
    price_str = f"{listing.price}€" if listing.price else "?€"
    src_emoji = _SOURCE_EMOJI_PUSH.get(listing.source, "⚪")
    title_short = (listing.title or "")[:80]
    summary = (
        f"✅ *Ajoutée* {src_emoji}\n"
        f"📍 _{_escape_md(title_short)}_\n"
        f"🏷 {surface_str} · *{price_str}*"
    )
    if listing.location:
        summary += f"\n🌐 {_escape_md(listing.location)}"
    if score is not None:
        summary += f"\n⭐ score *{score}/10*"
        if reason:
            summary += f" — _{_escape_md(reason)}_"
    summary += f"\n🔗 {listing.url}"
    await _reply(update, summary)

    # 7. Push high-score notif (uses the same path as the campaign push alerts)
    if score is not None and score >= config.INTEREST_THRESHOLD:
        try:
            await _check_and_push_alerts([listing.lbc_id], ctx)
        except Exception as exc:
            logger.warning("[/add] push alert failed: %s", exc)


# ─── Inline keyboard callbacks ────────────────────────────────────────────────

async def callback_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()

    action, lbc_id = query.data.split(":", 1)

    # ── Push alert callbacks (mark called/rented) ──────────────────────────
    if action in ("called", "rented"):
        with database._conn() as conn:
            conn.execute(
                "UPDATE listings SET call_status=? WHERE lbc_id=?",
                (action, lbc_id),
            )
        emoji = "✅" if action == "called" else "❌"
        label = "Appelé" if action == "called" else "Loué"
        try:
            await query.edit_message_reply_markup(reply_markup=None)
            # Append a status line to the existing message body
            new_text = (query.message.text_html or query.message.text or "")
            new_text += f"\n\n{emoji} <b>{label}</b>"
            await query.edit_message_text(
                new_text, parse_mode=ParseMode.HTML, disable_web_page_preview=True,
            )
        except Exception:
            pass
        return

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
    update: Update, ctx: ContextTypes.DEFAULT_TYPE,
    source: str | None = None, tier: str = "all",
) -> None:
    """Run one full campaign cycle. Acquires _campaign_lock; honours _stop_requested.

    If `source` is provided, restrict to that one site.
    `tier`: "all" | "fast" | "slow" — used by 2-tier autostart.
    """
    async with _campaign_lock:
        _stop_requested.clear()
        await _run_campaign_body(update, ctx, source=source, tier=tier)


_SOURCE_LABELS = {
    "leboncoin":         "LBC",
    "seloger":           "SeLoger",
    "pap":               "PAP",
    "bienici":           "Bien'ici",
    "logicimmo":         "Logic-Immo",
    "studapart":         "Studapart",
    "parisattitude":     "Paris Attitude",
    "lodgis":            "Lodgis",
    "immojeune":         "ImmoJeune",
    "locservice":        "LocService",
    "roomlala":          "Roomlala",
    "entreparticuliers": "EntreParticuliers",
    "ladresse":          "L'Adresse",
    "century21":         "Century 21",
    "wizi":              "Wizi",
    "laforet":           "Laforêt",
    "guyhoquet":         "Guy Hoquet",
    "inli":              "Inli",
    "gensdeconfiance":   "Gens de Confiance",
    "cdc_habitat":       "CDC Habitat",
    "fnaim":             "FNAIM",
}


def _source_url(source: str) -> str:
    """Map a normalised source name to its configured search URL ('' if disabled)."""
    return {
        "leboncoin":         config.DEFAULT_SEARCH_URL,
        "seloger":           config.DEFAULT_SEARCH_SELOGER_URL,
        "pap":               config.DEFAULT_SEARCH_PAP_URL,
        "bienici":           config.DEFAULT_SEARCH_BIENICI_URL,
        "logicimmo":         config.DEFAULT_SEARCH_LOGICIMMO_URL,
        "studapart":         config.DEFAULT_SEARCH_STUDAPART_URL,
        "parisattitude":     config.DEFAULT_SEARCH_PARISATTITUDE_URL,
        "lodgis":            config.DEFAULT_SEARCH_LODGIS_URL,
        "immojeune":         config.DEFAULT_SEARCH_IMMOJEUNE_URL,
        "locservice":        config.DEFAULT_SEARCH_LOCSERVICE_URL,
        "roomlala":          config.DEFAULT_SEARCH_ROOMLALA_URL,
        "entreparticuliers": config.DEFAULT_SEARCH_ENTREPARTICULIERS_URL,
        "ladresse":          config.DEFAULT_SEARCH_LADRESSE_URL,
        "century21":         config.DEFAULT_SEARCH_CENTURY21_URL,
        "wizi":              config.DEFAULT_SEARCH_WIZI_URL,
        "laforet":           config.DEFAULT_SEARCH_LAFORET_URL,
        "guyhoquet":         config.DEFAULT_SEARCH_GUYHOQUET_URL,
        "inli":              config.DEFAULT_SEARCH_INLI_URL,
        "gensdeconfiance":   config.DEFAULT_SEARCH_GENSDECONFIANCE_URL,
        "cdc_habitat":       config.DEFAULT_SEARCH_CDC_URL,
        "fnaim":             config.DEFAULT_SEARCH_FNAIM_URL,
    }.get(source, "")


def _campaign_sources(only: str | None = None,
                      tier: str = "all") -> list[tuple[str, str]]:
    """Return [(url, label)] for each enabled source.

    `only`: restrict to that single source.
    `tier`: "all" (default), "fast" (httpx/curl_cffi only — sub-30s scrapes),
            or "slow" (Camoufox — 60-90s/source). Used by the 2-tier
            autostart so we can poll fast sources every 5 min and slow ones
            every 25 min.
    Empty URL skips a source.
    """
    if only:
        url = _source_url(only)
        if not url:
            return []
        return [(url, _SOURCE_LABELS.get(only, only))]

    sources: list[tuple[str, str]] = [
        (config.DEFAULT_SEARCH_URL, "LBC"),
        # SeLoger: scraping doesn't need creds (Camoufox parses the public
        # SERP). Sending DOES need SELOGER_EMAIL + PASSWORD — that gate is
        # in messenger.py:send_message which returns False with a warning
        # if creds are missing. So scrape unconditionally for browsing.
        (config.DEFAULT_SEARCH_SELOGER_URL, "SeLoger"),
        (config.DEFAULT_SEARCH_PAP_URL, "PAP"),
        (config.DEFAULT_SEARCH_BIENICI_URL, "Bien'ici"),
        (config.DEFAULT_SEARCH_LOGICIMMO_URL, "Logic-Immo"),
        (config.DEFAULT_SEARCH_STUDAPART_URL, "Studapart"),
        (config.DEFAULT_SEARCH_PARISATTITUDE_URL, "Paris Attitude"),
        (config.DEFAULT_SEARCH_LODGIS_URL, "Lodgis"),
        (config.DEFAULT_SEARCH_IMMOJEUNE_URL, "ImmoJeune"),
        (config.DEFAULT_SEARCH_LOCSERVICE_URL, "LocService"),
        (config.DEFAULT_SEARCH_ROOMLALA_URL, "Roomlala"),
        (config.DEFAULT_SEARCH_ENTREPARTICULIERS_URL, "EntreParticuliers"),
        (config.DEFAULT_SEARCH_LADRESSE_URL, "L'Adresse"),
        (config.DEFAULT_SEARCH_CENTURY21_URL, "Century 21"),
        (config.DEFAULT_SEARCH_WIZI_URL, "Wizi"),
        (config.DEFAULT_SEARCH_LAFORET_URL, "Laforêt"),
        (config.DEFAULT_SEARCH_GUYHOQUET_URL, "Guy Hoquet"),
        (config.DEFAULT_SEARCH_INLI_URL, "Inli"),
        (config.DEFAULT_SEARCH_GENSDECONFIANCE_URL, "Gens de Confiance"),
        (config.DEFAULT_SEARCH_CDC_URL, "CDC Habitat"),
        (config.DEFAULT_SEARCH_FNAIM_URL, "FNAIM"),
    ]
    # Tier classification — slow = Camoufox-based (cold-start expensive).
    _SLOW_LABELS = {"LBC", "SeLoger", "Logic-Immo"}
    if tier == "fast":
        sources = [s for s in sources if s[1] not in _SLOW_LABELS]
    elif tier == "slow":
        sources = [s for s in sources if s[1] in _SLOW_LABELS]
    return [(url, label) for url, label in sources if url]


# ─── Push alerts (instant Telegram notif on hot listings) ────────────────────

import preferences as _prefs_push
from html import escape as _html_escape
import re as _re_push

# Zones eligible for push alerts: weight-3 only (Paris 11/12/13, Vincennes,
# Saint-Mandé, Charenton). Anything else = too borderline for an instant ping.
_PUSH_HOT_ZONES = {
    z["zone"] for z in _prefs_push.ZONES_PREFERRED if z.get("weight", 0) >= 3
}

# Sources NOT covered by Jinka — less competition (5-15 candidates instead of
# 100-300). User's strategic edge: be one of the few people watching these.
# Lower score threshold for these so we get aggressive notifications.
_OFF_RADAR_SOURCES = {
    "studapart", "parisattitude", "lodgis", "immojeune", "locservice",
    "entreparticuliers", "ladresse", "century21", "wizi", "laforet",
    "guyhoquet", "kley", "inli", "icf", "actionlogement", "gensdeconfiance",
    "cdc_habitat", "fnaim",
}
_PUSH_MIN_SCORE_OFF_RADAR = 6.5  # Lower bar than _PUSH_MIN_SCORE for Jinka sources

# Per-campaign + rolling-window counters
_push_sent_this_campaign: int = 0
_push_send_times: list[float] = []

_SOURCE_EMOJI_PUSH = {
    "leboncoin": "🟠", "seloger": "🔵", "pap": "🟢", "bienici": "🟣",
    "logicimmo": "🟡", "studapart": "🎓", "parisattitude": "🗼",
    "lodgis": "🏛", "immojeune": "🧑‍🎓", "locservice": "🏠",
    "entreparticuliers": "🤝", "ladresse": "🏢", "century21": "21",
    "wizi": "🔑", "laforet": "🌳", "guyhoquet": "🎩",
}


def _push_reset_for_campaign() -> None:
    global _push_sent_this_campaign
    _push_sent_this_campaign = 0


def _push_rate_ok() -> bool:
    now = time.time()
    _push_send_times[:] = [t for t in _push_send_times if now - t < 60.0]
    if len(_push_send_times) >= config.PUSH_RATE_PER_MIN:
        return False
    if _push_sent_this_campaign >= config.PUSH_MAX_PER_CAMPAIGN:
        return False
    return True


def _format_avail_push(raw: str | None) -> str:
    if not raw:
        return "—"
    parts = raw.split("-")
    if len(parts) == 3:
        return f"{parts[2][:2]}/{parts[1]}/{parts[0]}"
    if len(parts) == 2:
        return f"{parts[1]}/{parts[0]}"
    return raw


def _build_call_script(listing: dict) -> str:
    """Generate a ready-to-read French call script for the listing.
    Adapts to source type (agence/particulier/résidence/default) and bakes in
    Illan's profile (alternant SNCF + Visale). <500 chars, single string."""
    AGENCIES = {"foncia", "century21", "laforet", "guyhoquet", "ladresse",
                "orpi", "nestenn", "stephaneplaza", "era"}
    PARTICULIERS = {"pap", "leboncoin", "locservice", "bienici", "lbc",
                    "entreparticuliers", "gensdeconfiance"}
    RESIDENCES = {"studapart", "immojeune", "kley", "studeasy", "lokaviz",
                  "studylease", "icf", "actionlogement", "inli"}

    src = (listing.get("source") or "").strip().lower()
    if src in AGENCIES: kind = "agence"
    elif src in RESIDENCES: kind = "residence"
    elif src in PARTICULIERS: kind = "particulier"
    else: kind = "default"

    price = listing.get("price")
    surface = listing.get("surface")
    location = (listing.get("location") or "").strip() or None
    housing = (listing.get("housing_type") or "").strip() or "logement"
    available = (listing.get("available_from") or "").strip() or None

    parts = [housing.lower()]
    if surface:
        try: parts.append(f"{int(float(surface))}m²")
        except Exception: pass
    if price:
        try: parts.append(f"à {int(float(price))}€ CC")
        except Exception: pass
    if location: parts.append(f"à {location}")
    descriptor = " ".join(parts)
    avail_tail = f" (dispo {available})" if available else ""

    if kind == "agence":
        script = (f"Bonjour, je vous appelle au sujet de votre {descriptor}{avail_tail}. "
                  "Est-il toujours disponible ? J'ai un dossier complet "
                  "(alternant SNCF, garant Visale) et je peux déposer ma candidature aujourd'hui.")
    elif kind == "particulier":
        script = (f"Bonjour, votre {descriptor}{avail_tail} m'intéresse beaucoup. "
                  "Toujours disponible ? Je suis alternant SNCF avec garant Visale, "
                  "dossier prêt — une visite cette semaine est possible ?")
    elif kind == "residence":
        script = (f"Bonjour, je vous appelle pour le {descriptor}{avail_tail}. "
                  "Je suis alternant SNCF (~1700€/mois) avec garant Visale. "
                  "Le logement est-il toujours disponible ?")
    else:
        script = (f"Bonjour, je vous appelle pour votre annonce {descriptor}{avail_tail}. "
                  "Toujours disponible ? Dossier solide (alternant SNCF + Visale), "
                  "je peux visiter dès aujourd'hui.")
    script = " ".join(script.split())
    if len(script) > 490:
        script = (f"Bonjour, je vous appelle pour votre annonce. Toujours disponible ? "
                  "Alternant SNCF + Visale, dossier prêt, visite aujourd'hui.")
    return script


def _build_push_html(row: dict) -> str:
    e = _html_escape
    src = (row.get("source") or "").lower()
    src_emoji = _SOURCE_EMOJI_PUSH.get(src, "⚪")
    htype = row.get("housing_type") or "logement"
    surface = row.get("surface")
    surf_str = f"{surface}m²" if surface else "?m²"
    score = row.get("score") or 0
    phone = (row.get("phone") or "").strip()
    tel_clean = _re_push.sub(r"[^\d+]", "", phone)
    off_radar = "💎 OFF-RADAR" if src in _OFF_RADAR_SOURCES else ""
    script = _build_call_script(row)
    return (
        f"🔥 <b>NOUVELLE ANNONCE — score {score}/10</b> {off_radar}\n\n"
        f"📍 <b>{e(row.get('location') or '?')}</b>\n"
        f"💰 {row.get('price') or '?'}€ • {surf_str}\n"
        f"🏠 {e(htype)} ({src_emoji})\n"
        f"🗝 Libre: {_format_avail_push(row.get('available_from'))}\n\n"
        f'📞 <a href="tel:{e(tel_clean)}">{e(phone)}</a>\n'
        f'🔗 <a href="{e(row.get("url") or "")}">Voir l\'annonce</a>\n\n'
        f"📝 <i>Script :</i>\n<code>{e(script)}</code>"
    )


async def _check_and_push_alerts(
    persisted_lbc_ids: list[str],
    ctx: ContextTypes.DEFAULT_TYPE,
) -> None:
    """Fire Telegram pushes for newly-persisted listings matching hot criteria.
    SQL gates: notified=0, score>=PUSH_MIN_SCORE, price<=PUSH_MAX_PRICE, real
    phone, fresh. Python gate: weight-3 zone. Rate-limited."""
    if not config.ENABLE_PUSH_ALERTS or not persisted_lbc_ids:
        return
    placeholders = ",".join("?" * len(persisted_lbc_ids))
    off_radar_placeholders = ",".join("?" * len(_OFF_RADAR_SOURCES))
    # Phase 1 push optim — allow push BEFORE LLM scoring lands:
    #   • Off-radar : push si score>=6.5 OR (score IS NULL AND price<=1100 AND
    #                 phone non-bloqué) — l'edge structurel justifie la fast-path
    #   • Jinka-covered : score>=PUSH_MIN_SCORE only (we wait for LLM here)
    # The score-NULL path is gated by basic price/phone sanity to avoid junk.
    sql = f"""
        SELECT lbc_id, title, price, location, surface, housing_type,
               source, url, phone, score, score_reason,
               published_at, available_from
          FROM listings
         WHERE lbc_id IN ({placeholders})
           AND notified = 0
           AND dedup_of IS NULL  -- skip cross-source duplicates: the primary already pushed
           AND (
                 (source IN ({off_radar_placeholders}) AND score >= ?)
              OR (source IN ({off_radar_placeholders}) AND score IS NULL
                    AND price IS NOT NULL AND price <= ?
                    AND phone IS NOT NULL AND phone NOT IN ('', '#blocked'))
              OR (source NOT IN ({off_radar_placeholders}) AND score >= ?)
               )
    """
    off_list = list(_OFF_RADAR_SOURCES)
    params = (list(persisted_lbc_ids)
              + off_list + [_PUSH_MIN_SCORE_OFF_RADAR]
              + off_list + [config.PUSH_MAX_PRICE]
              + off_list + [config.PUSH_MIN_SCORE])
    with database._conn() as conn:
        rows = [dict(r) for r in conn.execute(sql, params).fetchall()]
    if not rows:
        return

    # ── Fraud filter ────────────────────────────────────────────────────────
    # Push alerts are the loudest signal we send to Illan — never push a
    # listing that trips the scam-marker / payment-before-visit / price-
    # anomaly checks. Logs each skip so suspicious patterns are auditable.
    safe_rows: list[dict] = []
    for row in rows:
        # Hydrate description for fraud check (push SQL doesn't SELECT it).
        try:
            with database._conn() as conn:
                drow = conn.execute(
                    "SELECT description FROM listings WHERE lbc_id = ?",
                    (row["lbc_id"],),
                ).fetchone()
            row["description"] = (drow["description"] if drow else "") or ""
        except Exception:
            row["description"] = ""
        is_fraud, fraud_reason = database.is_suspicious_listing(row)
        if is_fraud:
            logger.warning(
                "[push] SKIP suspicious listing %s (%s) — %s",
                row["lbc_id"], row.get("source"), fraud_reason,
            )
            continue
        safe_rows.append(row)
    rows = safe_rows
    if not rows:
        return

    global _push_sent_this_campaign
    sent_ids: list[str] = []
    for row in rows:
        if not _push_rate_ok():
            logger.info("[push] rate limit hit — skipping remaining alerts")
            break
        try:
            kb = InlineKeyboardMarkup([[
                InlineKeyboardButton("✅ Appelé", callback_data=f"called:{row['lbc_id']}"),
                InlineKeyboardButton("❌ Loué", callback_data=f"rented:{row['lbc_id']}"),
            ]])
            await ctx.bot.send_message(
                chat_id=config.TELEGRAM_CHAT_ID,
                text=_build_push_html(row),
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=False,
                reply_markup=kb,
            )
            _push_send_times.append(time.time())
            _push_sent_this_campaign += 1
            sent_ids.append(row["lbc_id"])
            logger.info("[push] alert sent for %s (%s€, score=%s)",
                        row["lbc_id"], row["price"], row.get("score"))
        except Exception as exc:
            logger.warning("[push] send failed for %s: %s", row["lbc_id"], exc)

    if sent_ids:
        with database._conn() as conn:
            ph = ",".join("?" * len(sent_ids))
            conn.execute(
                f"UPDATE listings SET notified=1 WHERE lbc_id IN ({ph})", sent_ids,
            )


def _publish_static_dashboard() -> None:
    """Regenerate public/index.html + git commit/push to GitHub Pages.
    Sync function (subprocess), called via asyncio.to_thread."""
    import subprocess
    from pathlib import Path
    import generate_static
    generate_static.main()
    public_dir = Path("public")
    env = os.environ.copy()
    env.setdefault("GIT_AUTHOR_EMAIL", "dashboard@immo.local")
    env.setdefault("GIT_AUTHOR_NAME", "immo-dashboard")
    env.setdefault("GIT_COMMITTER_EMAIL", "dashboard@immo.local")
    env.setdefault("GIT_COMMITTER_NAME", "immo-dashboard")
    try:
        subprocess.run(["git", "add", "."], cwd=public_dir, check=True, env=env, capture_output=True)
        subprocess.run(
            ["git", "commit", "-m", f"update {datetime.utcnow().isoformat(timespec='seconds')}Z"],
            cwd=public_dir, env=env, capture_output=True,
        )
        subprocess.run(["git", "push", "origin", "HEAD"], cwd=public_dir,
                       check=True, env=env, capture_output=True, timeout=30)
    except Exception as exc:
        logger.warning("Static publish git step failed: %s", exc)


async def _run_campaign_body(
    update: Update, ctx: ContextTypes.DEFAULT_TYPE,
    source: str | None = None, tier: str = "all",
) -> None:
    sources = _campaign_sources(only=source, tier=tier)
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

    _push_reset_for_campaign()

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
        # Pure Playwright + stealth — fast (~20-40s typical, but LBC's
        # DataDome challenge can take 60-120s on a cold profile)
        "LBC": 180.0,
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
        # New agency / p2p sites — curl_cffi parallel, fast
        "EntreParticuliers": 60.0,
        "L'Adresse": 60.0,
        "Century 21": 60.0,
        "Wizi": 30.0,
        "Laforêt": 90.0,  # Playwright fallback if static fails
        "Guy Hoquet": 60.0,  # curl_cffi XHR — fast, Playwright fallback only on Cloudflare
        "Inli": 60.0,        # curl_cffi, fans out 8 IDF depts in parallel
        "Gens de Confiance": 60.0,  # curl_cffi, server-rendered Search blob
        "CDC Habitat": 60.0,  # curl_cffi, paginated server-rendered
        "FNAIM": 90.0,        # 8 départements parallel, 9 pages each, ~25-30s
    }
    DEFAULT_TIMEOUT = 90.0

    from scraper import detect_housing_type

    async def _score_listings_parallel(items: list, concurrency: int = 16) -> None:
        """Score listings in parallel, semaphore-bounded.

        Skips listings that already have a non-null score in DB — re-scoring
        the same listing wastes DeepSeek calls and time. Only NEW listings
        (or ones that previously failed to score) hit the API. Existing
        scores are read back into the Listing object so downstream filters
        see them.

        Mutates each Listing in-place (sets .score and .score_reason).
        Failures are logged and skipped — don't block the campaign.
        """
        # 1. Read existing scores from DB in one batch query.
        ids = [l.lbc_id for l in items if l.lbc_id]
        existing: dict[str, tuple[int, str]] = {}
        if ids:
            with database._conn() as conn:
                placeholders = ",".join("?" * len(ids))
                for row in conn.execute(
                    f"SELECT lbc_id, score, score_reason FROM listings "
                    f"WHERE lbc_id IN ({placeholders}) AND score IS NOT NULL",
                    ids,
                ).fetchall():
                    existing[row[0]] = (row[1], row[2] or "")

        # 2. Hydrate items that already have a score; collect the rest.
        # Skip listings priced > HARD_PRICE_CAP (1050€) — dealbreakers anyway,
        # so scoring them wastes DeepSeek calls. Listings 1000-1050€ DO get
        # scored (they show on dashboard, user wants notes for them).
        import preferences as _prefs
        cap = _prefs.HARD_PRICE_CAP
        to_score = []
        n_over_budget = 0
        for lst in items:
            if lst.lbc_id in existing:
                lst.score, lst.score_reason = existing[lst.lbc_id]
            elif lst.price is not None and lst.price > cap:
                n_over_budget += 1
            else:
                to_score.append(lst)

        if not to_score:
            logger.info(
                "Scored 0/%d (cached: %d, over-budget skipped: %d)",
                len(items), len(items) - n_over_budget, n_over_budget,
            )
            return

        # Batch-score: 5 listings per DeepSeek call → 5× faster + 5× cheaper.
        # score_listings_batch fans out groups in parallel internally; the
        # `concurrency` arg is no longer used here but kept for compatibility.
        try:
            batch_results = await score_listings_batch(to_score, batch_size=5)
            for lst, res in zip(to_score, batch_results):
                lst.score = res["score"]
                lst.score_reason = res["reason"]
                avail = res.get("available_from")
                if avail:
                    lst.available_from = avail
                try:
                    database.set_listing_score(
                        lst.lbc_id, res["score"], res["reason"],
                        available_from=avail,
                    )
                except Exception as exc:
                    logger.warning("Persist score failed for %s: %s", lst.lbc_id, exc)
            n_scored = len(batch_results)
        except Exception as exc:
            logger.warning("Batch scoring failed (%s) — falling back to per-listing", exc)
            n_scored = 0
            sem = asyncio.Semaphore(concurrency)
            async def _one(lst):
                nonlocal n_scored
                async with sem:
                    try:
                        result = await score_listing(lst)
                        lst.score = result["score"]
                        lst.score_reason = result["reason"]
                        database.set_listing_score(lst.lbc_id, result["score"], result["reason"])
                        n_scored += 1
                    except Exception as exc2:
                        logger.warning("Score failed for %s: %s", lst.lbc_id, exc2)
            await asyncio.gather(*(_one(l) for l in to_score))

        logger.info(
            "Scored %d new (batched) / %d cached / %d over-budget skipped (total %d)",
            n_scored, len(items) - len(to_score) - n_over_budget,
            n_over_budget, len(items),
        )

    def _persist_batch(batch: list) -> int:
        """Bulk-upsert a batch of just-scraped listings to DB in one transaction.
        Drops listings that fail is_real_offer (e.g. "Recherche location...",
        "Cherche logement", price <400€, suspect titles).
        """
        if not batch:
            return 0
        # Pre-filter: drop demand-side / suspect listings before persist
        from scraper import is_real_offer as _real
        clean = [l for l in batch if _real(l)]
        n_dropped = len(batch) - len(clean)
        if n_dropped > 0:
            logger.info("[persist] dropped %d non-offer/suspect listings", n_dropped)
        # Sites where phone is hidden by site policy (form/portal-only contact)
        _PHONE_BLOCKED_SOURCES = {"studapart", "locservice"}
        # When the source doesn't expose a real publication date, fall back to
        # the scrape time so the user has SOME idea of recency. The COALESCE
        # in upsert preserves the FIRST scrape time on subsequent /campagnes,
        # so this becomes "first time we saw this listing" — a decent estimate
        # of when it was posted. Marked with `scrape:` prefix so dashboard
        # shows ⏱ instead of 📅.
        _now_iso = datetime.utcnow().isoformat()
        rows = []
        for lst in clean:
            htype, n_room = detect_housing_type(lst.title or "", lst.description or "")
            phone = getattr(lst, "phone", None)
            if phone is None and lst.source in _PHONE_BLOCKED_SOURCES:
                phone = "#blocked"
            pub_at = getattr(lst, "published_at", None)
            if not pub_at:
                pub_at = f"scrape:{_now_iso}"
            rows.append({
                "lbc_id": lst.lbc_id, "title": lst.title, "price": lst.price,
                "location": lst.location, "seller_name": lst.seller_name,
                "seller_type": "", "url": lst.url, "source": lst.source,
                "surface": lst.surface, "housing_type": htype,
                "roommate_count": n_room,
                "published_at": pub_at,
                "phone": phone,
                "description": lst.description,
                "available_from": getattr(lst, "available_from", None),
            })
        try:
            n = database.upsert_listings_batch(rows)
            # Late-availability dealbreaker applied at persist time (no LLM
            # needed): structured available_from > 2026-09 → score=0, hidden
            # from dashboard. Catches PA/SeLoger/Lodgis/PAP without waiting
            # for scoring.
            late = [r["lbc_id"] for r in rows
                    if r.get("available_from")
                    and (r["available_from"] or "")[:7] > "2026-09"]
            if late:
                with database._conn() as conn:
                    placeholders = ",".join("?" * len(late))
                    conn.execute(
                        f"UPDATE listings SET score=0, score_reason=? "
                        f"WHERE lbc_id IN ({placeholders})",
                        ["❌ dispo > sept 2026"] + late,
                    )
                logger.info("[persist] flagged %d listings as dealbreakers (avail>2026-09)", len(late))
            # Activity check (generalized from PA): stamp seen_at for every
            # row we just persisted. Listings of this source that didn't
            # come back in the scrape keep their old seen_at and age out
            # via mark_stale_listings (24h cron in main()).
            try:
                by_source: dict[str, list[str]] = {}
                for r in rows:
                    by_source.setdefault(r["source"], []).append(r["lbc_id"])
                for src, ids in by_source.items():
                    database.mark_seen(src, ids)
            except Exception as exc:
                logger.warning("[persist] mark_seen failed: %s", exc)
            # Cross-source dedup at persist time. Stamps dedup_key on every
            # row, then flags later duplicates with dedup_of=<primary lbc_id>.
            # Earliest-seen variant wins.
            try:
                n_dups = database.apply_dedup_for_batch(rows)
                if n_dups > 0:
                    logger.info("[persist] flagged %d cross-source duplicates", n_dups)
            except Exception as exc:
                logger.warning("[persist] dedup pass failed: %s", exc)
            return n
        except Exception as exc:
            logger.warning("Bulk upsert failed (%s) — falling back to per-row", exc)
            n = 0
            for r in rows:
                try:
                    database.upsert_listing(**r)
                    n += 1
                except Exception as exc2:
                    logger.warning("Failed to persist %s: %s", r["lbc_id"], exc2)
            return n

    # Camoufox concurrency is bounded by the persistent browser pool
    # (scraper._CAMOUFOX_POOL, size=2). The semaphore here gates Camoufox
    # tasks BEFORE entering wait_for so queue waits don't eat the timeout
    # budget — each source gets its full scrape time. Playwright sources
    # each launch their own Chromium and stay fully parallel.
    _CAMOUFOX_LABELS = {"Studapart", "Lodgis", "ImmoJeune", "LocService", "Bien'ici", "SeLoger"}
    per_source: list[tuple[str, int, list]] = [("", 0, []) for _ in sources]
    refresh_lock = asyncio.Lock()
    camoufox_sem = asyncio.Semaphore(2)

    async def _scrape_one(i: int, url: str, label: str) -> None:
        slot = camoufox_sem if label in _CAMOUFOX_LABELS else nullcontext()
        async with slot:
            states[i] = f"🔄 *{label}* : en cours…"
            async with refresh_lock:
                await _refresh_board()
            timeout = _TIMEOUT_BY_LABEL.get(label, DEFAULT_TIMEOUT)
            t0 = time.time()
            results: list = []
            try:
                results = await asyncio.wait_for(search_listings(url, max_results=500), timeout=timeout)
            except asyncio.TimeoutError:
                states[i] = f"⏱ {label} : timeout (>{timeout:.0f}s)"
                logger.warning("%s scrape timed out after %.1fs", label, time.time() - t0)
            except Exception as exc:
                states[i] = f"❌ {label} : échec ({type(exc).__name__})"
                logger.error("%s scrape failed: %s", label, exc)
            else:
                elapsed, n = time.time() - t0, len(results)
                states[i] = (
                    f"⚪ {label} : 0 annonce ({elapsed:.0f}s)" if n == 0
                    else f"✅ {label} : *{n}* annonce{'s' if n > 1 else ''} ({elapsed:.0f}s)"
                )
                logger.info("%s scrape done in %.1fs: %d listings", label, elapsed, n)
            if results:
                n_persisted = _persist_batch(results)
                # Phase 1.2 — push immediately for this source (before other
                # sources finish scraping). Off-radar fast-path doesn't need
                # LLM score — push on price+phone+zone basic gates.
                if config.ENABLE_PUSH_ALERTS and n_persisted > 0:
                    try:
                        await _check_and_push_alerts(
                            [l.lbc_id for l in results if l.lbc_id], ctx,
                        )
                    except Exception as exc:
                        logger.warning("[push] per-source failed for %s: %s", label, exc)
            per_source[i] = (label, len(results), results)
            async with refresh_lock:
                await _refresh_board()

    t_parallel = time.time()
    await asyncio.gather(
        *(_scrape_one(i, url, label) for i, (url, label) in enumerate(sources)),
        return_exceptions=True,
    )
    logger.info("All %d sources finished in %.1fs (parallel)", len(sources), time.time() - t_parallel)
    listings: list = [lst for _label, _n, results in per_source for lst in results]
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
        import preferences as _prefs
        cap = _prefs.HARD_PRICE_CAP
        # Quick count: cached vs new vs over-budget (will be skipped)
        ids = [l.lbc_id for l in listings if l.lbc_id]
        n_cached = 0
        if ids:
            with database._conn() as conn:
                placeholders = ",".join("?" * len(ids))
                n_cached = conn.execute(
                    f"SELECT COUNT(*) FROM listings WHERE lbc_id IN ({placeholders}) "
                    f"AND score IS NOT NULL", ids,
                ).fetchone()[0]
        n_over = sum(1 for l in listings if l.price is not None and l.price > cap)
        n_new = len(listings) - n_cached - n_over
        if n_new > 0:
            await _reply(
                update,
                f"🎯 Scoring de *{n_new}* nouvelles annonces "
                f"({n_cached} en cache, {n_over} hors budget skipped)…"
            )
        else:
            await _reply(
                update,
                f"✅ Aucune nouvelle à scorer ({n_cached} en cache, {n_over} hors budget)."
            )
        await _score_listings_parallel(listings)
        # Push alerts: send Telegram notif for hot listings (after scoring so
        # score gate works). Idempotent via notified=1 flag in DB.
        if config.ENABLE_PUSH_ALERTS:
            try:
                await _check_and_push_alerts(
                    [l.lbc_id for l in listings if l.lbc_id], ctx,
                )
            except Exception as exc:
                logger.warning("[push] post-scoring alert pass failed: %s", exc)

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

            # High-interest alert (independent of contact prep)
            if score_result is not None and score_result["score"] >= config.INTEREST_THRESHOLD:
                await _reply(
                    update,
                    f"🔥 *ANNONCE INTÉRESSANTE* ⭐{score_result['score']}/10\n"
                    f"📍 _{_escape_md(listing.title)}_ ({_escape_md(listing.location)})\n"
                    f"💰 {listing.price}€/mois\n"
                    f"🔗 {listing.url}\n"
                    f"💡 _{score_result['reason']}_"
                )

            if config.ENABLE_CONTACT_PREP:
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
                # Prepare-only: persist as pending, no actual send
                database.create_contact(listing_id, result.message)
                sent += 1
                score_str = f" ⭐{score_result['score']}/10" if score_result else ""
                await _reply(
                    update,
                    f"📝 Préparé{score_str} → _{_escape_md(listing.title)}_ ({_escape_md(listing.location)})"
                )
            else:
                # Contact-prep disabled — just count eligible matches
                sent += 1
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
    # Activity check: prune listings that are now 404/dead on the source site.
    # Only check listings NOT in this campaign's results AND scraped > 1 day ago
    # (recent ones are likely still alive).
    try:
        scraped_ids = {l.lbc_id for l in listings}
        with database._conn() as conn:
            stale_rows = conn.execute(
                "SELECT lbc_id, url FROM listings "
                "WHERE lbc_id NOT IN ({}) AND datetime(scraped_at) < datetime('now', '-1 day') "
                "AND datetime(scraped_at) > datetime('now', '-7 days')"
                .format(",".join("?" * len(scraped_ids)) or "''"),
                list(scraped_ids),
            ).fetchall()
        if stale_rows:
            logger.info("[activity-check] checking %d not-recently-seen listings", len(stale_rows))
            import httpx as _httpx
            sem_chk = asyncio.Semaphore(20)
            dead = []
            async with _httpx.AsyncClient(headers={"User-Agent": "Mozilla/5.0 Chrome/120 Safari/537.36"},
                                          timeout=10, follow_redirects=True) as cc:
                async def _check(lbc_id, url):
                    if not url:
                        return
                    async with sem_chk:
                        try:
                            r = await cc.head(url)
                            if r.status_code in (404, 410, 451):
                                dead.append(lbc_id)
                        except Exception:
                            pass  # network errors → don't drop
                await asyncio.gather(*(_check(r["lbc_id"], r["url"]) for r in stale_rows))
            if dead:
                with database._conn() as conn:
                    conn.execute(
                        "DELETE FROM listings WHERE lbc_id IN ({})".format(",".join("?" * len(dead))),
                        dead,
                    )
                logger.info("[activity-check] deleted %d dead listings", len(dead))
                await _reply(update, f"🗑 Activity check : {len(dead)} annonces désactivées purgées.")
    except Exception as exc:
        logger.warning("Activity check failed (non-fatal): %s", exc)

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

    # Regen + push static dashboard to GitHub Pages
    try:
        import subprocess
        await asyncio.to_thread(_publish_static_dashboard)
        await _reply(update, "🌐 Dashboard public mis à jour sur GitHub Pages.")
    except Exception as exc:
        logger.warning("Static dashboard publish failed (non-fatal): %s", exc)

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


async def cmd_refresh(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Run /campagne then /score_all in sequence — full re-sync.

    Use after deploying scraper changes or when you want a complete refresh
    of the dashboard. /campagne re-scrapes (free, no LLM); /score_all runs
    LLM extraction on listings that need it (~$0.10-0.20).
    """
    if _campaign_lock.locked():
        await _reply(update, "⚠️ Une campagne est déjà en cours. Utilisez /stop puis relancez /refresh.")
        return
    await _reply(update, "🔄 *Refresh complet* lancé : campagne puis score_all.")
    await _run_campaign_core(update, ctx)
    if _stop_requested.is_set():
        await _reply(update, "🛑 Refresh interrompu par /stop.")
        return
    await _reply(update, "🧠 Campagne terminée. Lancement du score_all…")
    await cmd_score_all(update, ctx)
    await _reply(update, "✅ Refresh complet terminé.")


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

    # Cost: ~$0.000065/listing with batch 5-in-1 on DeepSeek (cache hits on prefix)
    await _reply(
        update,
        f"🎯 Scoring de *{len(unscored)}* annonce(s) sans score (batch 5-en-1)…\n"
        f"Coût estimé : ~${len(unscored) * 0.000065:.3f} via DeepSeek.",
    )

    # Build Listing objects, skip listings above hard price cap (>1050€ are
    # dealbreakers anyway via scoring v2 — wasted DeepSeek calls).
    from agent import Listing
    import preferences as _prefs
    cap = _prefs.HARD_PRICE_CAP
    items = []
    n_skip = 0
    for r in unscored:
        if r["price"] is not None and r["price"] > cap:
            n_skip += 1
            continue
        items.append(Listing(
            lbc_id=r["lbc_id"], title=r["title"] or "",
            description=r.get("description") or "",
            price=r["price"], location=r["location"] or "",
            seller_name="", url=r["url"] or "",
            source=r["source"] or "", surface=r.get("surface"),
            housing_type=r.get("housing_type") or "",
            roommate_count=r.get("roommate_count"),
        ))

    if not items:
        await _reply(update, f"✅ Aucune annonce ≤{cap}€ à scorer ({n_skip} skip >{cap}€).")
        return

    await _reply(
        update,
        f"🚀 Scoring batch 5-en-1 sur *{len(items)}* annonces ({n_skip} skip >{cap}€)…"
    )

    # Batch scoring: 5 listings per DeepSeek call, all batches in parallel
    results = await score_listings_batch(items, batch_size=5)
    n_done = 0
    n_avail = 0
    for lst, res in zip(items, results):
        try:
            avail = res.get("available_from")
            database.set_listing_score(
                lst.lbc_id, res["score"], res["reason"], available_from=avail,
            )
            n_done += 1
            if avail:
                n_avail += 1
        except Exception as exc:
            logger.warning("Persist score failed for %s: %s", lst.lbc_id, exc)

    await _reply(
        update,
        f"✅ Backfill terminé : *{n_done}/{len(items)}* annonces scorées "
        f"(*{n_avail}* avec date dispo) + *{n_skip}* hors budget skip.",
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
    """Start 2-tier auto-scraping.
    Fast tier (httpx/curl_cffi sources) every FAST minutes — sub-2.5min
    average detection on the off-radar sources where the user has structural edge.
    Slow tier (Camoufox sources: LBC/SeLoger/Logic-Immo) every SLOW minutes."""
    global _auto_task
    if _auto_task and not _auto_task.done():
        await _reply(update, "⚠️ Campagne automatique déjà en cours. Utilise /autostop d'abord.")
        return

    # Per-source independent loops — each source polls at its own optimal
    # interval (function of anti-bot risk + freshness need). Heavy DataDome
    # sources poll less; light API sources poll often. /autostart ignores
    # legacy fast/slow args.
    _SOURCE_INTERVAL_S = {
        # Camoufox / DataDome — keep slow to avoid IP rep degradation
        "LBC":               300,   # 5 min (DataDome)
        "SeLoger":           600,   # 10 min (DataDome)
        "Logic-Immo":        600,   # 10 min (DataDome)
        # API-based, very fast & cheap
        "Paris Attitude":    180,   # 3 min
        "Studapart":         180,
        "Bien'ici":          180,
        # curl_cffi static HTML — fast
        "Wizi":              120,   # 2 min
        "Inli":              180,
        "ImmoJeune":         180,
        "Lodgis":            180,
        "LocService":        180,
        "PAP":               120,
        "EntreParticuliers": 240,
        "L'Adresse":         300,
        "Century 21":        240,
        "Laforêt":           240,
        "Guy Hoquet":        240,
        "Gens de Confiance": 180,
        "CDC Habitat":       300,
        "FNAIM":             300,
    }
    DEFAULT_INTERVAL = 300

    sources_config = _campaign_sources()
    if not sources_config:
        await _reply(update, "⚠️ Aucune source configurée.")
        return

    await _reply(
        update,
        f"🤖 *Auto-campagne — {len(sources_config)} loops indépendants*\n"
        f"Chaque source poll à son rythme optimal (2-10 min).\n"
        f"Push immédiat dès qu'une nouvelle annonce est détectée.\n"
        "Utilise /autostop pour arrêter."
    )

    # Reverse map label → source key for _run_campaign_core(source=...)
    label_to_key = {v: k for k, v in _SOURCE_LABELS.items()}

    async def _source_loop(url: str, label: str):
        interval = _SOURCE_INTERVAL_S.get(label, DEFAULT_INTERVAL)
        source_key = label_to_key.get(label, label.lower())
        import random as _rand
        await asyncio.sleep(_rand.uniform(0, 30))
        while True:
            t0 = time.time()
            try:
                logger.info("[auto-loop] %s starting…", label)
                await _run_campaign_core(update, ctx, source=source_key)
                elapsed = time.time() - t0
                logger.info("[auto-loop] %s done in %.0fs, sleeping %ds", label, elapsed, interval)
            except Exception as exc:
                logger.warning("[auto-loop] %s crashed: %s", label, exc)
                elapsed = time.time() - t0
            sleep_for = max(0, interval * _rand.uniform(0.85, 1.15) - elapsed)
            await asyncio.sleep(sleep_for)

    async def _auto_loop():
        loops = [_source_loop(u, l) for u, l in sources_config]
        await asyncio.gather(*loops, return_exceptions=True)

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
    import random as _random
    while True:
        try:
            logger.info("[WATCH %s] poll starting", pool_name)
            sources = [(u, l) for u, l in _campaign_sources() if l in labels]
            if not sources:
                logger.info("[WATCH %s] no sources active in this pool", pool_name)
                await asyncio.sleep(interval_min * 60)
                continue

            # Camoufox sources reuse the bot-wide pool (scraper._CAMOUFOX_POOL)
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
    # LBC sentinel state — global so /sentinel commands can manage it
    _sentinel_task: asyncio.Task | None = None
    _sentinel_scrape_lock = asyncio.Lock()
    _sentinel_last_scrape_ts = [0.0]  # mutable container for closure
    _sentinel_bot_ref = [None]  # captures Application.bot at post_init

    async def _on_lbc_change(new_id: str) -> None:
        """Sentinel detected new LBC listing → trigger full scrape via campaign."""
        now = time.time()
        if now - _sentinel_last_scrape_ts[0] < 90:
            logger.info("[SENTINEL] change %s ignored — recent scrape (%.0fs ago)",
                        new_id, now - _sentinel_last_scrape_ts[0])
            return
        if _sentinel_scrape_lock.locked():
            logger.info("[SENTINEL] change %s skipped — scrape already running", new_id)
            return
        async with _sentinel_scrape_lock:
            _sentinel_last_scrape_ts[0] = time.time()
            logger.info("[SENTINEL] triggering LBC scrape (new_id=%s)", new_id)
            try:
                # Use _campaign_lock-protected core to feed the full pipeline
                # (persist + score + push). Pass dummy update.
                class _DummyUpdate:
                    effective_message = type("M", (), {"reply_text": lambda *a, **kw: None})()
                    effective_chat = type("C", (), {"id": int(config.TELEGRAM_CHAT_ID)})()
                class _DummyCtx:
                    bot = None
                # Easier path: just call search_listings + persist+push directly
                from scraper import search_listings, is_real_offer, detect_housing_type
                from datetime import datetime as _dt
                listings = await search_listings(config.DEFAULT_SEARCH_URL, max_results=30)
                clean = [l for l in listings if is_real_offer(l)]
                _now = _dt.utcnow().isoformat()
                rows = []
                for l in clean:
                    htype, n_room = detect_housing_type(l.title or "", l.description or "")
                    pub_at = getattr(l, "published_at", None) or f"scrape:{_now}"
                    rows.append({
                        "lbc_id": l.lbc_id, "title": l.title, "price": l.price,
                        "location": l.location, "seller_name": l.seller_name,
                        "seller_type": "", "url": l.url, "source": l.source,
                        "surface": l.surface, "housing_type": htype,
                        "roommate_count": n_room, "published_at": pub_at,
                        "phone": getattr(l, "phone", None),
                        "description": l.description,
                        "available_from": getattr(l, "available_from", None),
                    })
                n = database.upsert_listings_batch(rows)
                logger.info("[SENTINEL] persisted %d/%d LBC listings", n, len(clean))
                # Score newly-fetched listings (LLM batch) so push alerts can match
                # score>=7 criteria. Without this, sentinel-scraped listings have
                # score=None and never trigger a push.
                if config.ENABLE_SCORING and clean:
                    try:
                        import preferences as _prefs
                        cap = _prefs.HARD_PRICE_CAP
                        ids = [l.lbc_id for l in clean if l.lbc_id]
                        already = set()
                        if ids:
                            with database._conn() as conn:
                                placeholders = ",".join("?" * len(ids))
                                for r2 in conn.execute(
                                    f"SELECT lbc_id FROM listings WHERE lbc_id IN ({placeholders}) AND score IS NOT NULL",
                                    ids,
                                ).fetchall():
                                    already.add(r2[0])
                        to_score = [l for l in clean
                                   if l.lbc_id not in already
                                   and (l.price is None or l.price <= cap)]
                        if to_score:
                            from agent import score_listings_batch
                            results = await score_listings_batch(to_score, batch_size=5)
                            for lst, res in zip(to_score, results):
                                avail = res.get("available_from")
                                database.set_listing_score(
                                    lst.lbc_id, res["score"], res["reason"],
                                    available_from=avail,
                                )
                            logger.info("[SENTINEL] scored %d new LBC listings", len(to_score))
                    except Exception as exc:
                        logger.warning("[SENTINEL] scoring failed: %s", exc)
                # Trigger push for newly-persisted IDs
                if config.ENABLE_PUSH_ALERTS and n > 0 and _sentinel_bot_ref[0]:
                    fake_ctx = type("C", (), {"bot": _sentinel_bot_ref[0]})()
                    await _check_and_push_alerts([r["lbc_id"] for r in rows], fake_ctx)
            except Exception as exc:
                logger.warning("[SENTINEL] triggered scrape failed: %s", exc)

    async def _on_pap_change(new_id: str) -> None:
        """PAP sentinel detected new listing → trigger PAP scrape."""
        now = time.time()
        if now - _sentinel_last_scrape_ts[0] < 60:
            return
        if _sentinel_scrape_lock.locked():
            return
        async with _sentinel_scrape_lock:
            _sentinel_last_scrape_ts[0] = time.time()
            logger.info("[PAP-SENTINEL] triggering PAP scrape (new_id=r%s)", new_id)
            try:
                from scraper import search_listings, is_real_offer, detect_housing_type
                from datetime import datetime as _dt
                listings = await search_listings(config.DEFAULT_SEARCH_PAP_URL, max_results=30)
                clean = [l for l in listings if is_real_offer(l)]
                _now = _dt.utcnow().isoformat()
                rows = []
                for l in clean:
                    htype, n_room = detect_housing_type(l.title or "", l.description or "")
                    pub_at = getattr(l, "published_at", None) or f"scrape:{_now}"
                    rows.append({
                        "lbc_id": l.lbc_id, "title": l.title, "price": l.price,
                        "location": l.location, "seller_name": l.seller_name,
                        "seller_type": "", "url": l.url, "source": l.source,
                        "surface": l.surface, "housing_type": htype,
                        "roommate_count": n_room, "published_at": pub_at,
                        "phone": getattr(l, "phone", None),
                        "description": l.description,
                        "available_from": getattr(l, "available_from", None),
                    })
                n = database.upsert_listings_batch(rows)
                logger.info("[PAP-SENTINEL] persisted %d/%d", n, len(clean))
                if config.ENABLE_SCORING and clean:
                    try:
                        import preferences as _prefs
                        cap = _prefs.HARD_PRICE_CAP
                        ids = [l.lbc_id for l in clean if l.lbc_id]
                        already = set()
                        if ids:
                            with database._conn() as conn:
                                placeholders = ",".join("?" * len(ids))
                                for r2 in conn.execute(
                                    f"SELECT lbc_id FROM listings WHERE lbc_id IN ({placeholders}) AND score IS NOT NULL",
                                    ids,
                                ).fetchall():
                                    already.add(r2[0])
                        to_score = [l for l in clean
                                   if l.lbc_id not in already
                                   and (l.price is None or l.price <= cap)]
                        if to_score:
                            from agent import score_listings_batch
                            results = await score_listings_batch(to_score, batch_size=5)
                            for lst, res in zip(to_score, results):
                                avail = res.get("available_from")
                                database.set_listing_score(
                                    lst.lbc_id, res["score"], res["reason"],
                                    available_from=avail,
                                )
                            logger.info("[PAP-SENTINEL] scored %d", len(to_score))
                    except Exception as exc:
                        logger.warning("[PAP-SENTINEL] scoring failed: %s", exc)
                if config.ENABLE_PUSH_ALERTS and n > 0 and _sentinel_bot_ref[0]:
                    fake_ctx = type("C", (), {"bot": _sentinel_bot_ref[0]})()
                    await _check_and_push_alerts([r["lbc_id"] for r in rows], fake_ctx)
            except Exception as exc:
                logger.warning("[PAP-SENTINEL] scrape failed: %s", exc)

    _pap_sentinel_task = [None]  # mutable container

    async def _post_init(_app):
        import scraper as _scraper_mod
        await _scraper_mod.init_camoufox_pool(size=2)
        _sentinel_bot_ref[0] = _app.bot
        nonlocal _sentinel_task
        try:
            _sentinel_task = asyncio.create_task(
                _scraper_mod._lbc_sentinel_loop(_on_lbc_change)
            )
            logger.info("[SENTINEL] LBC sentinel loop started (60s±15s polling)")
        except Exception as exc:
            logger.warning("[SENTINEL] LBC failed to start: %s", exc)
        try:
            _pap_sentinel_task[0] = asyncio.create_task(
                _scraper_mod._pap_sentinel_loop(_on_pap_change)
            )
            logger.info("[SENTINEL] PAP sentinel loop started (75s polling)")
        except Exception as exc:
            logger.warning("[SENTINEL] PAP failed to start: %s", exc)

    async def _post_shutdown(_app):
        import scraper as _scraper_mod
        if _sentinel_task and not _sentinel_task.done():
            _sentinel_task.cancel()
            try: await _sentinel_task
            except asyncio.CancelledError: pass
        if _pap_sentinel_task[0] and not _pap_sentinel_task[0].done():
            _pap_sentinel_task[0].cancel()
            try: await _pap_sentinel_task[0]
            except asyncio.CancelledError: pass
        await _scraper_mod.shutdown_camoufox_pool()

    app = (
        Application.builder()
        .token(config.TELEGRAM_BOT_TOKEN)
        .request(request)
        .post_init(_post_init)
        .post_shutdown(_post_shutdown)
        .build()
    )

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("settings", cmd_settings))
    app.add_handler(CommandHandler("search", cmd_search))
    app.add_handler(CommandHandler("simulate", cmd_simulate))
    app.add_handler(CommandHandler("add", cmd_add))
    app.add_handler(CommandHandler("campagne", cmd_campagne))
    app.add_handler(CommandHandler("refresh", cmd_refresh))
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

    # ── Periodic activity check ──────────────────────────────────────────
    # Every hour, score=0 any listing whose last `seen_at` is >24h old.
    # This is the generalized version of the PA-only stale-clear at
    # scraper.py:2640. Hidden listings stay in the DB (we may want them for
    # price-history) but disappear from the dashboard.
    async def _stale_cleanup_job(ctx: ContextTypes.DEFAULT_TYPE) -> None:
        try:
            n = database.mark_stale_listings(hours=24)
            if n > 0:
                logger.info("[stale-cleanup] hid %d listings unseen for >24h", n)
        except Exception as exc:
            logger.warning("[stale-cleanup] failed: %s", exc)

    if app.job_queue is not None:
        # First run after 5 min so we don't fire mid-startup if the user
        # just relaunched. Then every hour.
        app.job_queue.run_repeating(
            _stale_cleanup_job, interval=3600, first=300, name="stale_cleanup",
        )
    else:
        logger.warning("[stale-cleanup] JobQueue unavailable — periodic check disabled")

    logger.info("Bot starting…")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()

"""Telegram bot entry point — all commands handled here."""
import asyncio
import logging
import sys
from pathlib import Path

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
from agent import analyse_listing, format_simulation_text, classify_intent, Listing, score_listing
from scraper import search_listings, fetch_single_listing, is_real_offer
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

_campaign_running = False
_auto_task: asyncio.Task | None = None


# ─── Helper ───────────────────────────────────────────────────────────────────

async def _reply(update: Update, text: str, **kwargs) -> None:
    await update.effective_message.reply_text(
        text, parse_mode=ParseMode.MARKDOWN, **kwargs
    )


# ─── /start ───────────────────────────────────────────────────────────────────

async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    await _reply(
        update,
        "👋 *Bienvenue sur le bot immo d'Illan !*\n\n"
        "Commandes disponibles :\n"
        "• /search — Lancer un scraping\n"
        "• /simulate <url> — Simuler un message sans l'envoyer\n"
        "• /campagne — Lancer la campagne complète\n"
        "• /autostart [heures] — Campagne automatique (défaut : 3h)\n"
        "• /autostop — Arrêter la campagne automatique\n"
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
        f"Lance /campagne pour envoyer les messages.",
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

    # Store result in bot_data so callback can access it
    ctx.bot_data[f"sim:{listing.lbc_id}"] = result

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
    result = ctx.bot_data.get(f"sim:{lbc_id}")

    if action == "ignore":
        await query.edit_message_reply_markup(reply_markup=None)
        await query.message.reply_text("❌ Annonce ignorée.")
        return

    if action == "edit":
        await query.message.reply_text(
            "✏️ *Modification manuelle non encore implémentée.*\n"
            "Utilisez /simulate à nouveau avec une URL différente.",
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


# ─── Campaign core (shared by /campagne and auto-loop) ────────────────────────

async def _run_campaign_core(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Run one full campaign cycle. Caller is responsible for _campaign_running flag."""
    global _campaign_running
    await _reply(update, "🚀 Campagne lancée ! Scraping en cours…")

    # Scrape LeBonCoin
    try:
        listings_lbc = await search_listings(config.DEFAULT_SEARCH_URL, max_results=25)
    except Exception as exc:
        logger.error("LBC scrape failed: %s", exc)
        listings_lbc = []

    # Scrape SeLoger (skip if no credentials)
    listings_sl: list = []
    if config.SELOGER_EMAIL:
        try:
            listings_sl = await search_listings(config.DEFAULT_SEARCH_SELOGER_URL, max_results=25)
        except Exception as exc:
            logger.error("SeLoger scrape failed: %s", exc)
    else:
        logger.info("SeLoger credentials not set — skipping SeLoger scrape")

    # Scrape PAP.fr
    listings_pap: list = []
    try:
        listings_pap = await search_listings(config.DEFAULT_SEARCH_PAP_URL, max_results=25)
    except Exception as exc:
        logger.error("PAP scrape failed: %s", exc)

    listings = listings_lbc + listings_sl + listings_pap
    # Deduplication: remove cross-platform duplicates by (price, location, seller_name)
    listings = _deduplicate(listings)

    if not listings:
        await _reply(update, "😕 Aucune annonce trouvée.")
        _campaign_running = False
        return

    await _reply(
        update,
        f"📋 {len(listings_lbc)} LBC + {len(listings_sl)} SeLoger + {len(listings_pap)} PAP "
        f"→ *{len(listings)} uniques* — analyse en cours…"
    )

    sent = 0
    skipped_dup = 0
    skipped_rate = 0
    errors = 0

    for listing in listings:
        if not _campaign_running:
            await _reply(update, "🛑 Campagne arrêtée manuellement.")
            break

        if not is_real_offer(listing):
            skipped_dup += 1
            continue

        # Hard budget ceiling (catches promoted listings from PAP that bypass URL filter)
        if listing.price and listing.price > PROFILE["search"]["max_rent"]:
            skipped_dup += 1
            continue

        if database.already_contacted(listing.lbc_id):
            skipped_dup += 1
            continue

        if database.messages_sent_last_hour() >= config.MAX_MESSAGES_PER_HOUR:
            skipped_rate += 1
            await _reply(update, f"⏸ Limite horaire atteinte ({config.MAX_MESSAGES_PER_HOUR}/h). Pause…")
            await asyncio.sleep(60)
            continue

        try:
            # Optional scoring gate
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
            contact_id = database.create_contact(listing_id, result.message)
            success = await send_message_safe(listing.url, result.message, contact_id)
            if success:
                sent += 1
                score_str = ""
                if config.ENABLE_SCORING:
                    score_str = f" ⭐{score_result['score']}/10"
                await _reply(
                    update,
                    f"✉️ Message envoyé{score_str} → _{listing.title}_ ({listing.location})"
                )
            else:
                errors += 1
        except Exception as exc:
            logger.error("Campaign error on %s: %s", listing.lbc_id, exc)
            errors += 1

        # Small delay between messages
        await asyncio.sleep(3)

    _campaign_running = False
    await _reply(
        update,
        f"🏁 *Campagne terminée*\n\n"
        f"✉️ Messages envoyés : {sent}\n"
        f"⏭ Déjà contactés / filtrés : {skipped_dup}\n"
        f"⏸ Limite horaire (ignorés) : {skipped_rate}\n"
        f"❌ Erreurs : {errors}",
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
    global _campaign_running
    if _campaign_running:
        await _reply(update, "⚠️ Une campagne est déjà en cours. Utilisez /stop pour l'arrêter.")
        return
    _campaign_running = True
    await _run_campaign_core(update, ctx)


# ─── /rapport ─────────────────────────────────────────────────────────────────

async def cmd_rapport(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    stats = database.today_stats()
    s = PROFILE["search"]
    zones_short = ", ".join(s["zones"][:2]) + "…"
    await _reply(
        update,
        f"📊 *LEBONCOIN UPDATE*\n\n"
        f"🎯 Critères : Appart meublé, {s['min_surface']}m²+, max {s['max_rent']}€, {zones_short}\n"
        f"📅 Période : aujourd'hui\n\n"
        f"📋 Annonces scrapées : {stats['scraped']}\n"
        f"✉️ Messages envoyés : {stats['sent']}\n"
        f"✅ Réponses positives : {stats['positive']}\n"
        f"❌ Réponses négatives : {stats['negative']}\n"
        f"🔇 Sans réponse : {stats['no_response']}\n\n"
        f"🤖 Continuer la campagne demain ?",
    )


# ─── /stop ────────────────────────────────────────────────────────────────────

async def cmd_stop(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    global _campaign_running
    if _campaign_running:
        _campaign_running = False
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
    logger.info("Chat message received: %s", user_text[:80])

    intent = classify_intent(user_text)
    tool = intent.get("tool")

    if tool == "run_search":
        url = intent.get("url") or config.DEFAULT_SEARCH_URL
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
        await cmd_campagne(update, ctx)

    elif tool == "run_rapport":
        await cmd_rapport(update, ctx)

    elif tool == "run_stop":
        await cmd_stop(update, ctx)

    elif tool == "run_settings":
        await cmd_settings(update, ctx)

    elif tool == "run_autostart":
        await cmd_autostart(update, ctx)

    elif tool == "run_autostop":
        await cmd_autostop(update, ctx)

    elif tool == "run_visites":
        await cmd_visites(update, ctx)

    elif tool == "reply":
        await _reply(update, intent.get("text", "Je n'ai pas compris, peux-tu reformuler ?"))

    else:
        await _reply(update, "Je n'ai pas compris. Envoie-moi une URL LeBonCoin ou décris ce que tu veux faire.")


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
    app.add_handler(CommandHandler("rapport", cmd_rapport))
    app.add_handler(CommandHandler("stop", cmd_stop))
    app.add_handler(CommandHandler("autostart", cmd_autostart))
    app.add_handler(CommandHandler("autostop", cmd_autostop))
    app.add_handler(CommandHandler("visite", cmd_visite))
    app.add_handler(CommandHandler("visites", cmd_visites))
    app.add_handler(CommandHandler("boite", cmd_boite))
    app.add_handler(CallbackQueryHandler(callback_handler))
    # Natural language fallback — catches any non-command text message
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, cmd_chat))
    app.add_error_handler(error_handler)

    logger.info("Bot starting…")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()

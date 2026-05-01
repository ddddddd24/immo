"""Send messages on LeBonCoin and SeLoger via Playwright (authenticated sessions).

Strategy:
  1. Launch a persistent Chromium context (headless by default).
  2. Log in once per session and reuse the cookies.
  3. Navigate to the listing URL, find the contact form, fill and submit.

Source is auto-detected from the listing URL.
Update _SELECTORS / _SELOGER_SELECTORS if either site redesigns their UI.
"""
import asyncio
import logging
import random
from pathlib import Path

from playwright.async_api import async_playwright, Page, BrowserContext, TimeoutError as PWTimeout

import config
import database

logger = logging.getLogger(__name__)

# ─── Auth state paths ──────────────────────────────────────────────────────────
_AUTH_STATE_PATH         = Path("data/lbc_auth.json")
_SELOGER_AUTH_STATE_PATH = Path("data/seloger_auth.json")

# ─── LeBonCoin selectors ───────────────────────────────────────────────────────
_SELECTORS = {
    "login_email":      'input[name="st_username"], input[type="email"]',
    "login_password":   'input[name="st_passwd"], input[type="password"]',
    "login_submit":     'button[type="submit"]',
    "contact_button":   'a[data-qa-id="adview_contact_button"], button[data-qa-id="adview_contact_button"]',
    "message_textarea": 'textarea[name="message"], textarea[placeholder*="message"], textarea',
    "send_button":      'button[type="submit"][data-qa-id="send_message"], button[type="submit"]',
}

# ─── SeLoger selectors ─────────────────────────────────────────────────────────
_SELOGER_SELECTORS = {
    "login_email":      'input[name="email"], input[type="email"]',
    "login_password":   'input[name="password"], input[type="password"]',
    "login_submit":     'button[type="submit"]',
    "contact_button":   (
        '[data-testid="contact-cta"], [data-testid*="contact-button"], '
        'button:has-text("Contacter"), a:has-text("Contacter"), '
        'button:has-text("Envoyer un message")'
    ),
    "message_textarea": 'textarea[name="message"], textarea[placeholder*="essage"], textarea',
    "send_button":      'button[type="submit"], button:has-text("Envoyer")',
}


# ─── LeBonCoin auth & send ────────────────────────────────────────────────────

async def _login_lbc(page: Page) -> None:
    logger.info("Logging in to LeBonCoin…")
    await page.goto("https://www.leboncoin.fr/compte/login", wait_until="networkidle")
    await page.fill(_SELECTORS["login_email"], config.LBC_EMAIL)
    await page.fill(_SELECTORS["login_password"], config.LBC_PASSWORD)
    await page.click(_SELECTORS["login_submit"])
    await page.wait_for_url("**/leboncoin.fr/**", timeout=15_000)
    logger.info("LBC login succeeded")


async def _send_on_page_lbc(page: Page, listing_url: str, message: str) -> None:
    await page.goto(listing_url, wait_until="domcontentloaded", timeout=30_000)
    try:
        await page.wait_for_selector(_SELECTORS["contact_button"], timeout=10_000)
        await page.click(_SELECTORS["contact_button"])
    except PWTimeout:
        raise RuntimeError("LBC contact button not found — listing may be expired or UI changed")
    await page.wait_for_selector(_SELECTORS["message_textarea"], timeout=10_000)
    await page.fill(_SELECTORS["message_textarea"], message)
    await page.click(_SELECTORS["send_button"])
    await page.wait_for_timeout(2_000)
    logger.info("LBC message submitted on %s", listing_url)


# ─── SeLoger auth & send ──────────────────────────────────────────────────────

async def _login_seloger(page: Page) -> None:
    logger.info("Logging in to SeLoger…")
    await page.goto("https://www.seloger.com/auth/signin", wait_until="networkidle")
    await page.fill(_SELOGER_SELECTORS["login_email"], config.SELOGER_EMAIL)
    await page.fill(_SELOGER_SELECTORS["login_password"], config.SELOGER_PASSWORD)
    await page.click(_SELOGER_SELECTORS["login_submit"])
    await page.wait_for_url("**/seloger.com/**", timeout=15_000)
    logger.info("SeLoger login succeeded")


async def _send_on_page_seloger(page: Page, listing_url: str, message: str) -> None:
    await page.goto(listing_url, wait_until="domcontentloaded", timeout=30_000)
    # Dismiss cookie banner if present
    try:
        btn = page.locator('#didomi-notice-agree-button, button:has-text("Accepter")')
        if await btn.first.is_visible(timeout=3_000):
            await btn.first.click()
    except Exception:
        pass
    try:
        await page.wait_for_selector(_SELOGER_SELECTORS["contact_button"], timeout=10_000)
        await page.click(_SELOGER_SELECTORS["contact_button"])
    except PWTimeout:
        raise RuntimeError("SeLoger contact button not found — listing may be expired or UI changed")
    await page.wait_for_selector(_SELOGER_SELECTORS["message_textarea"], timeout=10_000)
    await page.fill(_SELOGER_SELECTORS["message_textarea"], message)
    await page.click(_SELOGER_SELECTORS["send_button"])
    await page.wait_for_timeout(2_000)
    logger.info("SeLoger message submitted on %s", listing_url)


# ─── Public API ───────────────────────────────────────────────────────────────

async def send_message(listing_url: str, message: str, contact_id: int) -> bool:
    """
    Send *message* to the listing at *listing_url* (LBC or SeLoger, auto-detected).
    Updates the contacts table on success. Returns True on success, False on failure.
    """
    if config.MOCK_MODE:
        logger.info("[MOCK] Fake-sending message to %s (contact_id=%d)", listing_url, contact_id)
        database.mark_contact_sent(contact_id)
        return True

    # Rate-limit guard
    sent_last_hour = database.messages_sent_last_hour()
    if sent_last_hour >= config.MAX_MESSAGES_PER_HOUR:
        logger.warning(
            "Rate limit reached (%d/%d messages/hour). Skipping %s",
            sent_last_hour, config.MAX_MESSAGES_PER_HOUR, listing_url,
        )
        return False

    is_seloger = "seloger.com" in listing_url

    if is_seloger and not config.SELOGER_EMAIL:
        logger.warning("SeLoger credentials not set — skipping %s", listing_url)
        return False

    auth_path   = _SELOGER_AUTH_STATE_PATH if is_seloger else _AUTH_STATE_PATH
    login_fn    = _login_seloger            if is_seloger else _login_lbc
    send_fn     = _send_on_page_seloger     if is_seloger else _send_on_page_lbc

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        context = await browser.new_context(
            storage_state=str(auth_path) if auth_path.exists() else None
        )
        page = await context.new_page()
        try:
            last_exc: Exception | None = None
            for attempt in range(2):
                try:
                    await send_fn(page, listing_url, message)
                    last_exc = None
                    break
                except Exception as exc:
                    last_exc = exc
                    if attempt == 0:
                        logger.warning(
                            "Attempt %d failed (%s), re-authenticating…",
                            attempt + 1, exc,
                        )
                        auth_path.unlink(missing_ok=True)
                        await login_fn(page)
                        await context.storage_state(path=str(auth_path))
                        await asyncio.sleep(2 + random.random() * 3)
                    else:
                        logger.warning("Attempt %d failed (%s), giving up", attempt + 1, exc)
            if last_exc is not None:
                raise last_exc
        finally:
            await page.close()
            await browser.close()

    database.mark_contact_sent(contact_id)
    return True


async def send_message_safe(listing_url: str, message: str, contact_id: int) -> bool:
    """Wrapper that catches all exceptions and returns False instead of raising."""
    try:
        return await send_message(listing_url, message, contact_id)
    except Exception as exc:
        logger.error("send_message failed for %s: %s", listing_url, exc)
        return False


# ─── LBC inbox reading ────────────────────────────────────────────────────────

async def _read_lbc_inbox(page: Page) -> list[dict]:
    """
    Navigate to LBC inbox and scrape unread message threads.
    Returns list of {thread_url, sender, message_text, listing_url}.
    """
    await page.goto("https://www.leboncoin.fr/messages", wait_until="domcontentloaded", timeout=30_000)
    await page.wait_for_timeout(2_000)

    # Grab thread items from the inbox
    threads = []
    try:
        # Each thread is a link in the conversation list
        items = await page.query_selector_all('[data-qa-id="conversation_item"]')
        for item in items[:20]:
            try:
                link_el = await item.query_selector("a")
                href = await link_el.get_attribute("href") if link_el else None
                if not href:
                    continue

                # Check for unread badge
                unread_el = await item.query_selector('[data-qa-id="unread_badge"], .unread-badge, .badge-unread')
                is_unread = unread_el is not None

                # Sender name
                sender_el = await item.query_selector('[data-qa-id="conversation_sender"], .sender-name, .author')
                sender = (await sender_el.inner_text()).strip() if sender_el else ""

                # Preview text
                preview_el = await item.query_selector('[data-qa-id="conversation_preview"], .preview-text, .message-preview')
                preview = (await preview_el.inner_text()).strip() if preview_el else ""

                full_url = f"https://www.leboncoin.fr{href}" if href.startswith("/") else href
                threads.append({
                    "thread_url": full_url,
                    "sender": sender,
                    "preview": preview,
                    "is_unread": is_unread,
                })
            except Exception:
                continue
    except Exception as exc:
        logger.warning("LBC inbox scrape error: %s", exc)

    return threads


async def check_inbox_lbc() -> list[dict]:
    """
    Check LBC inbox for new replies to our contact messages.
    Returns list of new responses found.
    Returns empty list in MOCK_MODE or on error.
    """
    if config.MOCK_MODE:
        logger.info("[MOCK] Skipping inbox check")
        return []

    if not _AUTH_STATE_PATH.exists():
        logger.info("No LBC auth state — skipping inbox check")
        return []

    new_responses = []
    try:
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=True)
            context = await browser.new_context(
                storage_state=str(_AUTH_STATE_PATH)
            )
            page = await context.new_page()
            try:
                threads = await _read_lbc_inbox(page)
                logger.info("LBC inbox: %d threads found", len(threads))

                for thread in threads:
                    if thread.get("is_unread") and thread.get("preview"):
                        new_responses.append(thread)
            finally:
                await page.close()
                await browser.close()
    except Exception as exc:
        logger.error("check_inbox_lbc failed: %s", exc)

    return new_responses

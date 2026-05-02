"""Integration tests for the prepare → confirm → send flow with mocked deps."""
import asyncio
import types
import pytest


# ─── Fake Telegram Update + Context ──────────────────────────────────────────

class _FakeMessage:
    def __init__(self):
        self.replies: list[str] = []
        self.text = ""
        self.chat_id = 1

    async def reply_text(self, text, **kwargs):
        self.replies.append(text)


class _FakeChat:
    def __init__(self, chat_id=1):
        self.id = chat_id


class _FakeUpdate:
    def __init__(self, chat_id=1, text=""):
        self.effective_message = _FakeMessage()
        self.effective_message.text = text
        self.effective_message.chat_id = chat_id
        self.effective_chat = _FakeChat(chat_id)
        # back-references for clean access in assertions
        self.replies = self.effective_message.replies


class _FakeContext:
    def __init__(self):
        self.bot_data: dict = {}
        self.args: list = []


# ─── Fixture: assemble mocks for a campaign run ──────────────────────────────

@pytest.fixture
def campaign_env(tmp_db, monkeypatch, make_listing):
    """Reset campaign state + provide a configurable mock for search_listings."""
    import main

    # Reset module-level state between tests (asyncio.Lock, Event, etc.)
    main._stop_requested.clear()
    main._HISTORY.clear()
    main._TURN_REPLIES.clear()

    # Default scrape result: empty list. Tests override via env["listings"]
    env: dict = {"listings": []}

    async def fake_search(url, max_results=25):
        return env["listings"]

    monkeypatch.setattr(main, "search_listings", fake_search)

    # Mock analyse_listing → returns a synthetic AnalysisResult
    from agent import AnalysisResult

    async def fake_analyse(listing):
        return AnalysisResult(
            seller_type="particulier",
            tone="Test",
            message=f"Bonjour, je suis intéressé par {listing.title}. Cordialement.",
            listing=listing,
        )

    monkeypatch.setattr(main, "analyse_listing", fake_analyse)

    # Mock send_message_safe → record + return success/fail.
    # Mimic the real wrapper: mark the contact as sent on success.
    sent_log: list[tuple[str, str, int]] = []

    async def fake_send(url, message, contact_id):
        sent_log.append((url, message, contact_id))
        success = env.get("send_success", True)
        if success:
            tmp_db.mark_contact_sent(contact_id)
        return success

    monkeypatch.setattr(main, "send_message_safe", fake_send)
    env["sent_log"] = sent_log

    # Mock check_inbox_lbc to skip the network call at end of campaign
    async def fake_inbox():
        return []
    monkeypatch.setattr(main, "check_inbox_lbc", fake_inbox)

    return env


# ─── Helper: run an async coroutine in a fresh event loop ────────────────────

def _run(coro):
    return asyncio.run(coro)


# ─── /campagne — prepare-only ────────────────────────────────────────────────

def test_campaign_prepares_pending_contacts_no_send(campaign_env, tmp_db, make_listing):
    import main
    campaign_env["listings"] = [
        make_listing(lbc_id="lbc_1", price=850, location="Paris 11"),
        make_listing(lbc_id="lbc_2", price=920, location="Paris 18"),
    ]
    update = _FakeUpdate()
    _run(main._run_campaign_body(update, _FakeContext(), source="leboncoin"))

    # Both listings should be in DB as pending
    assert tmp_db.count_pending_contacts() == 2
    pending = tmp_db.get_pending_contacts()
    pending_urls = {c["url"] for c in pending}
    assert {"https://example.com/listing/1"} <= pending_urls or len(pending_urls) == 2

    # send_message_safe should NEVER be called by /campagne
    assert campaign_env["sent_log"] == []

    # Reply summary should mention "Préparé" and "en attente"
    full_text = " ".join(update.replies)
    assert "Préparé" in full_text or "préparation" in full_text
    assert "en attente" in full_text


def test_campaign_filters_over_budget(campaign_env, tmp_db, make_listing):
    """Regression: skipped_dup NameError used to crash here."""
    import main
    campaign_env["listings"] = [
        make_listing(lbc_id="exp_1", price=2500),
        make_listing(lbc_id="exp_2", price=3500),
        make_listing(lbc_id="exp_3", price=1800),
    ]
    update = _FakeUpdate()
    _run(main._run_campaign_body(update, _FakeContext(), source="leboncoin"))

    assert tmp_db.count_pending_contacts() == 0
    full_text = " ".join(update.replies)
    assert "hors budget" in full_text


def test_campaign_with_prescreening_does_not_crash(
    campaign_env, tmp_db, monkeypatch, make_listing
):
    """Regression: skipped_dup NameError fired when prescreen rejected."""
    import main, agent
    monkeypatch.setattr(main.config, "ENABLE_PRESCREENING", True)

    async def fake_prescreen(listing):
        return {"eligible": False, "note": "ratio salaire/loyer insuffisant"}

    monkeypatch.setattr(main, "prescreen_listing", fake_prescreen)

    campaign_env["listings"] = [make_listing(lbc_id="ps1", price=850)]
    update = _FakeUpdate()
    _run(main._run_campaign_body(update, _FakeContext(), source="leboncoin"))

    assert tmp_db.count_pending_contacts() == 0
    full_text = " ".join(update.replies)
    assert "dossier" in full_text.lower() or "incompatible" in full_text.lower()


def test_campaign_with_scoring_skips_low_score(
    campaign_env, tmp_db, monkeypatch, make_listing
):
    """Regression: skipped_dup NameError fired when score gate rejected."""
    import main
    monkeypatch.setattr(main.config, "ENABLE_SCORING", True)
    monkeypatch.setattr(main.config, "MIN_SCORE", 6)

    async def fake_score(listing):
        return {"score": 3, "reason": "trop loin du métro"}

    monkeypatch.setattr(main, "score_listing", fake_score)

    campaign_env["listings"] = [make_listing(lbc_id="lo1", price=850)]
    update = _FakeUpdate()
    _run(main._run_campaign_body(update, _FakeContext(), source="leboncoin"))

    assert tmp_db.count_pending_contacts() == 0
    full_text = " ".join(update.replies)
    assert "score" in full_text.lower()


def test_campaign_stop_requested_aborts(campaign_env, tmp_db, make_listing):
    import main
    main._stop_requested.set()
    campaign_env["listings"] = [
        make_listing(lbc_id=f"stop_{i}", price=850) for i in range(5)
    ]
    update = _FakeUpdate()
    _run(main._run_campaign_body(update, _FakeContext(), source="leboncoin"))

    assert tmp_db.count_pending_contacts() == 0
    full_text = " ".join(update.replies)
    assert "arrêtée" in full_text.lower() or "arrête" in full_text.lower()


# ─── /envoyer — confirmation prompt only ─────────────────────────────────────

def test_envoyer_with_no_pending_replies_helpful_message(campaign_env, tmp_db):
    import main
    update = _FakeUpdate()
    _run(main.cmd_envoyer(update, _FakeContext()))
    assert any("aucun message" in r.lower() for r in update.replies)


def test_envoyer_sets_confirm_flag_and_does_not_send(campaign_env, tmp_db, make_listing):
    import main
    # Seed one pending contact
    listing_id = tmp_db.upsert_listing(
        lbc_id="ev1", title="T", price=800, location="Paris",
        seller_name="X", seller_type="", url="http://ev1", source="leboncoin",
    )
    tmp_db.create_contact(listing_id, "msg")

    ctx = _FakeContext()
    update = _FakeUpdate()
    _run(main.cmd_envoyer(update, ctx))

    # Flag set, no actual send
    assert main._SEND_CONFIRM_KEY in ctx.bot_data
    assert campaign_env["sent_log"] == []
    full_text = " ".join(update.replies)
    assert "Confirmation requise" in full_text or "confirmer" in full_text.lower()


# ─── /confirmer — actually drains ────────────────────────────────────────────

def test_confirmer_without_flag_replies_helpful(campaign_env):
    import main
    update = _FakeUpdate()
    _run(main.cmd_confirmer(update, _FakeContext()))
    assert any("aucun envoi" in r.lower() for r in update.replies)


def test_confirmer_drains_pending_queue(campaign_env, tmp_db, make_listing):
    """Full flow: prepare → /envoyer → /confirmer → all sent."""
    import main
    # Seed two pending contacts
    for i, price in enumerate([800, 850]):
        lid = tmp_db.upsert_listing(
            lbc_id=f"co_{i}", title=f"T{i}", price=price, location="Paris",
            seller_name="X", seller_type="", url=f"http://co_{i}", source="leboncoin",
        )
        tmp_db.create_contact(lid, f"msg{i}")

    ctx = _FakeContext()
    # /envoyer sets the flag
    _run(main.cmd_envoyer(_FakeUpdate(), ctx))
    assert main._SEND_CONFIRM_KEY in ctx.bot_data

    # /confirmer drains
    update = _FakeUpdate()
    _run(main.cmd_confirmer(update, ctx))

    # Two messages sent, queue empty
    assert len(campaign_env["sent_log"]) == 2
    assert tmp_db.count_pending_contacts() == 0
    full_text = " ".join(update.replies)
    assert "Envoi terminé" in full_text or "envoyés" in full_text.lower()


# ─── /pending and /recent — anti-hallucination data tools ───────────────────

def test_list_pending_empty(campaign_env, tmp_db):
    import main
    update = _FakeUpdate()
    _run(main.cmd_list_pending(update, _FakeContext()))
    assert any("aucun message" in r.lower() for r in update.replies)


def test_list_pending_returns_real_urls(campaign_env, tmp_db):
    import main
    listing_id = tmp_db.upsert_listing(
        lbc_id="real_1", title="Studio Paris 11", price=800,
        location="Paris", seller_name="X", seller_type="", url="http://REAL_URL_1",
        source="leboncoin",
    )
    tmp_db.create_contact(listing_id, "msg")
    update = _FakeUpdate()
    _run(main.cmd_list_pending(update, _FakeContext()))
    full_text = " ".join(update.replies)
    # The reply must contain the EXACT url from DB, not an LLM hallucination
    assert "http://REAL_URL_1" in full_text


def test_list_recent_returns_db_rows(campaign_env, tmp_db):
    import main
    tmp_db.upsert_listing(
        lbc_id="rec_1", title="Studio", price=850, location="Paris",
        seller_name="X", seller_type="", url="http://REC_URL", source="leboncoin",
    )
    update = _FakeUpdate()
    _run(main.cmd_list_recent(update, _FakeContext(), limit=10))
    full_text = " ".join(update.replies)
    assert "http://REC_URL" in full_text


# ─── send-confirm short-circuit in cmd_chat ─────────────────────────────────

def test_affirmative_after_envoyer_drains_queue(campaign_env, tmp_db):
    """User says /envoyer then "oui" → drain without re-running classify_intent."""
    import main
    listing_id = tmp_db.upsert_listing(
        lbc_id="aff_1", title="T", price=800, location="Paris",
        seller_name="X", seller_type="", url="http://aff", source="leboncoin",
    )
    tmp_db.create_contact(listing_id, "msg")

    ctx = _FakeContext()
    _run(main.cmd_envoyer(_FakeUpdate(), ctx))
    assert main._SEND_CONFIRM_KEY in ctx.bot_data

    # Now user types "oui" — should drain immediately, no LLM call needed
    update = _FakeUpdate(text="oui")
    _run(main.cmd_chat(update, ctx))
    assert len(campaign_env["sent_log"]) == 1
    assert main._SEND_CONFIRM_KEY not in ctx.bot_data


def test_non_affirmative_after_envoyer_cancels_silently(campaign_env, tmp_db):
    """User says /envoyer then anything else → confirmation flag cleared, no send."""
    import main
    listing_id = tmp_db.upsert_listing(
        lbc_id="cn_1", title="T", price=800, location="Paris",
        seller_name="X", seller_type="", url="http://cn", source="leboncoin",
    )
    tmp_db.create_contact(listing_id, "msg")

    ctx = _FakeContext()
    _run(main.cmd_envoyer(_FakeUpdate(), ctx))

    # User changes mind: types "attends en fait"
    update = _FakeUpdate(text="attends en fait")
    _run(main.cmd_chat(update, ctx))

    assert campaign_env["sent_log"] == []
    assert main._SEND_CONFIRM_KEY not in ctx.bot_data
    # Pending queue still intact
    assert tmp_db.count_pending_contacts() == 1

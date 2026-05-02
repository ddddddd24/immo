"""Eligibility filter tests for `_should_contact` (categorized rejection)."""
import pytest


# Note: main._should_contact uses database.already_contacted, so these tests
# need a tmp_db fixture to provide a clean DB.

def test_should_contact_eligible(tmp_db, make_listing):
    import main
    listing = make_listing(lbc_id="ok1", price=850)
    eligible, category, reason = main._should_contact(listing)
    assert eligible is True
    assert category == ""


def test_should_contact_rejects_low_quality(tmp_db, make_listing):
    """Empty seller_name fails is_real_offer → category 'qualité'."""
    import main
    listing = make_listing(lbc_id="bad1", seller_name="")
    eligible, category, _ = main._should_contact(listing)
    assert eligible is False
    assert category == "qualité"


def test_should_contact_rejects_over_budget(tmp_db, make_listing):
    import main
    listing = make_listing(lbc_id="exp1", price=2500)
    eligible, category, reason = main._should_contact(listing)
    assert eligible is False
    assert category == "budget"
    assert "2500" in reason


def test_should_contact_rejects_suspicious(tmp_db, make_listing):
    import main
    listing = make_listing(
        lbc_id="sus1",
        price=850,
        description="Envoyez les arrhes par Western Union avant la visite.",
    )
    eligible, category, _ = main._should_contact(listing)
    assert eligible is False
    assert category == "suspect"


def test_should_contact_rejects_already_in_db(tmp_db, make_listing):
    """Pending contact = considered already prepared → 'déjà_préparée'."""
    import main
    listing = make_listing(lbc_id="dup1", price=850)
    listing_id = tmp_db.upsert_listing(
        lbc_id=listing.lbc_id, title=listing.title, price=listing.price,
        location=listing.location, seller_name=listing.seller_name,
        seller_type="", url=listing.url, source=listing.source,
    )
    tmp_db.create_contact(listing_id, "msg")
    eligible, category, _ = main._should_contact(listing)
    assert eligible is False
    assert category == "déjà_préparée"


def test_should_contact_rejects_skip_title(tmp_db, make_listing):
    """Title matching SKIP_TITLE pattern (sous-loc, coloc, etc.)."""
    import main
    listing = make_listing(lbc_id="skip1", title="Sous-location 3 mois", price=850)
    eligible, category, _ = main._should_contact(listing)
    assert eligible is False
    assert category == "qualité"


@pytest.mark.parametrize("housing_type", ["coloc", "coliving", "chambre"])
def test_should_contact_rejects_shared_housing(tmp_db, make_listing, housing_type):
    """Illan wants couple-only (Illan + Iqleema). No shared housing."""
    import main
    listing = make_listing(lbc_id=f"shared_{housing_type}", price=850)
    listing.housing_type = housing_type
    eligible, category, reason = main._should_contact(listing)
    assert eligible is False
    assert category == "type_logement"
    assert housing_type in reason


def test_should_contact_accepts_studio_t1_t2(tmp_db, make_listing):
    """Standard private apartment types are fine."""
    import main
    for ht in ["studio", "T1", "T2", "T3", "residence", ""]:
        listing = make_listing(lbc_id=f"private_{ht}", price=850)
        listing.housing_type = ht
        eligible, _, _ = main._should_contact(listing)
        assert eligible is True, f"{ht} should be eligible"


# ─── preferences.py dealbreakers ─────────────────────────────────────────────

def test_preferences_dealbreaker_coloc():
    import preferences
    blocked, reason = preferences.is_dealbreaker(
        housing_type="coloc", roommate_count=None,
        title="Coloc Paris 11", description="",
    )
    assert blocked
    assert "coloc" in reason


def test_preferences_dealbreaker_high_roommate_count():
    import preferences
    blocked, reason = preferences.is_dealbreaker(
        housing_type="", roommate_count=4,
        title="T5 spacieux", description="",
    )
    assert blocked
    assert "4" in reason


def test_preferences_dealbreaker_keyword():
    import preferences
    blocked, reason = preferences.is_dealbreaker(
        housing_type="studio", roommate_count=None,
        title="Studio sous-location 2 mois", description="",
    )
    assert blocked
    assert "sous-location" in reason


def test_preferences_no_dealbreaker_for_normal_listing():
    import preferences
    blocked, _ = preferences.is_dealbreaker(
        housing_type="studio", roommate_count=None,
        title="Studio meublé Paris 11", description="Beau studio calme",
    )
    assert blocked is False


def test_preferences_build_prompt_block_includes_zones():
    """Sanity check that the prompt-building helper produces something usable."""
    import preferences
    block = preferences.build_prompt_block()
    assert "Saint-Denis" in block          # work location
    assert "Vincennes" in block            # preferred zone
    assert "balcon" in block               # preferred feature
    assert "95 lointain" in block          # avoid zone label


# ─── _escape_md ──────────────────────────────────────────────────────────────

@pytest.mark.parametrize("inp, expected", [
    (None, ""),
    ("", ""),
    ("plain text", "plain text"),
    ("with *star*", r"with \*star\*"),
    ("under_score", r"under\_score"),
    ("[bracket]", r"\[bracket\]"),
    ("`code`", r"\`code\`"),
    ("T2 *parfait* — appart_du_coin", r"T2 \*parfait\* — appart\_du\_coin"),
])
def test_escape_md(inp, expected):
    import main
    assert main._escape_md(inp) == expected


# ─── source URL mapping ──────────────────────────────────────────────────────

def test_source_url_known():
    import main
    url = main._source_url("studapart")
    assert url and url.startswith("https://www.studapart.com")


def test_source_url_unknown_returns_empty():
    import main
    assert main._source_url("nonexistent") == ""


def test_campaign_sources_filters_disabled():
    import main
    # roomlala is disabled by default (empty URL)
    sources = main._campaign_sources()
    labels = [label for _url, label in sources]
    assert "Roomlala" not in labels


def test_campaign_sources_only_filters():
    import main
    sources = main._campaign_sources(only="parisattitude")
    assert len(sources) == 1
    assert sources[0][1] == "Paris Attitude"


def test_campaign_sources_only_disabled_returns_empty():
    import main
    assert main._campaign_sources(only="roomlala") == []


# ─── Anti-hallucination: URL stripping in reply ──────────────────────────────

def test_sanitize_reply_strips_lbc_url():
    """Regression: DeepSeek invented LBC URLs in a 'continue' reply."""
    import main
    fake = (
        "Voici la suite des annonces:\n"
        "1. Studio Paris 11 — 850€\n"
        "   https://www.leboncoin.fr/ad/locations/3189431190\n"
    )
    out = main._sanitize_reply_text(fake)
    assert "leboncoin.fr/ad/locations/3189431190" not in out
    assert "URL invérifiée" in out
    assert "rapport_complet" in out  # pointer to the real-data tool


def test_sanitize_reply_strips_multiple_sites():
    import main
    fake = (
        "https://www.studapart.com/fr/colocation/uxco-saint-ouen-71176\n"
        "https://www.seloger.com/annonces/locations/immeuble/montreuil-93/studio/230521529.htm\n"
        "https://en-us.roomlala.com/rent/FR-France/paris\n"
    )
    out = main._sanitize_reply_text(fake)
    for bad in ["studapart.com/fr/colocation", "seloger.com/annonces", "roomlala.com/rent"]:
        assert bad not in out


def test_sanitize_reply_passes_through_clean_text():
    """Non-listing URLs and plain text should not be touched."""
    import main
    clean = "Salut Illan ! 😊 Comment ça va ? Tape /rapport_complet pour la liste."
    assert main._sanitize_reply_text(clean) == clean


# ─── Telegram message chunking ────────────────────────────────────────────────

def test_chunk_short_text_stays_single():
    import main
    out = main._chunk_for_telegram("hello")
    assert out == ["hello"]


def test_chunk_long_text_splits_at_newlines():
    import main
    long_text = "\n".join([f"line {i:04d}" for i in range(2000)])
    parts = main._chunk_for_telegram(long_text, max_len=500)
    assert len(parts) > 1
    for p in parts:
        assert len(p) <= 500
    assert "\n".join(parts).replace("\n", "") == long_text.replace("\n", "")  # no data loss


def test_chunk_respects_default_telegram_limit():
    import main
    listing = "  • Studio Paris (Paris, 75011) — *850€* · 28m²\n    https://www.leboncoin.fr/ad/locations/3189431190\n"
    big = "📋 *Rapport*\n" + listing * 200
    parts = main._chunk_for_telegram(big)
    for p in parts:
        assert len(p) <= 3800

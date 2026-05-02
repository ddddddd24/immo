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

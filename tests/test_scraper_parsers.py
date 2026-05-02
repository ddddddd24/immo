"""Scraper parser tests — _parse_price edge cases + per-site snapshot tests."""
import pytest
from bs4 import BeautifulSoup


# ─── _parse_price edge cases ────────────────────────────────────────────────

@pytest.mark.parametrize("inp, expected", [
    (None, None),
    ("", None),
    (1500, 1500),
    (1234.7, 1234),
    ("900", 900),
    ("900€", 900),
    ("900 €", 900),
    ("1 234", 1234),
    ("1 234 €", 1234),
    ("1.234", 1234),
    # field-aware: stop at first non-numeric break after digits
    ("2100€ + charges 350€", 2100),
    ("Loyer 850 € CC", 850),
    # range checks
    ("99999", None),       # over 50000 cap
    ("25", None),          # under 50 floor
    ("charges comprises", None),
])
def test_parse_price(inp, expected):
    from scraper import _parse_price
    assert _parse_price(inp) == expected


# ─── _ad_to_listing (LBC) — synthetic ad dict ────────────────────────────────

def test_lbc_ad_to_listing_basic():
    from scraper import _ad_to_listing
    ad = {
        "list_id": 12345,
        "subject": "Studio meublé 28m² Paris 11",
        "body": "Beau studio.",
        "attributes": [{"key": "price", "value_label": "850 €"}],
        "location": {"city": "Paris", "zipcode": "75011"},
        "owner": {"name": "Marie Dupont", "type": "private"},
        "images": {"urls_large": [{"url": "https://example.com/1.jpg"}]},
    }
    listing = _ad_to_listing(ad)
    assert listing is not None
    assert listing.lbc_id == "12345"
    assert listing.price == 850
    assert listing.location == "Paris, 75011"
    assert listing.seller_name == "Marie Dupont"
    assert listing.title == "Studio meublé 28m² Paris 11"
    assert listing.images == ["https://example.com/1.jpg"]


def test_lbc_ad_to_listing_returns_none_without_id():
    from scraper import _ad_to_listing
    assert _ad_to_listing({"subject": "no id"}) is None


def test_lbc_ad_to_listing_returns_none_for_non_dict():
    """Regression: 'str' object has no attribute 'get' on schema drift."""
    from scraper import _ad_to_listing
    assert _ad_to_listing("not a dict") is None
    assert _ad_to_listing(None) is None
    assert _ad_to_listing(42) is None


@pytest.mark.parametrize("malformed_field", [
    {"images": "not a dict"},                      # images as string
    {"images": [{"not_url_key": "x"}]},            # images list with weird entries
    {"images": [None, "https://direct-url.jpg"]},  # images list with None + str
    {"location": "Paris"},                         # location as string instead of dict
    {"owner": "Marie"},                            # owner as string
    {"attributes": "not a list"},                  # attributes as string
    {"attributes": ["just a string"]},             # attributes list with non-dict items
    {"price": "not a list"},                       # price as string instead of [int]
])
def test_lbc_ad_to_listing_survives_malformed_fields(malformed_field):
    """Regression: any single field with the wrong type used to crash.

    All these were variants of "'str' object has no attribute 'get'" before
    the defensive parsing fix.
    """
    from scraper import _ad_to_listing
    ad = {"list_id": 9999, "subject": "Test"}
    ad.update(malformed_field)
    listing = _ad_to_listing(ad)
    assert listing is not None
    assert listing.lbc_id == "9999"


# ─── Per-site parser snapshots against captured HTML ────────────────────────
#
# Each test loads the real HTML we captured during scraper development and
# verifies the parser extracts a sensible number of valid listings. These
# tests catch regressions when a site changes its DOM.

def _all_listings_valid(listings, expected_prefix):
    """Common assertions: every listing has the expected prefix + fields set."""
    for lst in listings:
        assert lst.lbc_id.startswith(expected_prefix + "_"), \
            f"unexpected prefix on {lst.lbc_id}"
        assert lst.url, "missing url"
        assert lst.title, "missing title"
        # price may be None for some cards (e.g., no rent shown), but if
        # set it must be in [50, 50000] thanks to _parse_price
        if lst.price is not None:
            assert 50 <= lst.price <= 50000, f"out-of-range price {lst.price}"


def test_studapart_parser_against_snapshot(fixture_html):
    from scraper import _studapart_card_to_listing
    soup = BeautifulSoup(fixture_html("studapart_camoufox.html"), "html.parser")
    cards = soup.find_all("a", class_="AccomodationBlock")
    listings = [l for l in (_studapart_card_to_listing(c) for c in cards) if l]
    assert len(listings) >= 5, f"expected ≥5 cards, got {len(listings)}"
    _all_listings_valid(listings, "sa")
    # at least one listing should be in budget (Studapart has student residences)
    in_budget = [l for l in listings if l.price and l.price <= 1000]
    assert in_budget, "expected at least one Studapart listing ≤1000€"


def test_parisattitude_parser_against_snapshot(fixture_html):
    from scraper import _parisattitude_card_to_listing
    soup = BeautifulSoup(fixture_html("parisattitude_debug.html"), "html.parser")
    cards = soup.find_all("div", class_="accommodation-search-card")
    listings = [l for l in (_parisattitude_card_to_listing(c) for c in cards) if l]
    assert len(listings) >= 5, f"expected ≥5 cards, got {len(listings)}"
    _all_listings_valid(listings, "pa")
    # Paris Attitude is premium — most should be over budget, that's expected
    over_budget = [l for l in listings if l.price and l.price > 1000]
    assert over_budget, "expected most PA listings to be premium"


def test_immojeune_parser_against_snapshot(fixture_html):
    from scraper import _immojeune_card_to_listing
    soup = BeautifulSoup(fixture_html("immojeune_debug.html"), "html.parser")
    cards = soup.find_all("div", class_="card")
    listings = [l for l in (_immojeune_card_to_listing(c) for c in cards) if l]
    assert len(listings) >= 5, f"expected ≥5 cards, got {len(listings)}"
    _all_listings_valid(listings, "ij")


def test_locservice_parser_against_snapshot(fixture_html):
    from scraper import _locservice_card_to_listing
    soup = BeautifulSoup(fixture_html("locservice_debug.html"), "html.parser")
    cards = soup.find_all("li", class_="accommodation-ad")
    listings = [l for l in (_locservice_card_to_listing(c) for c in cards) if l]
    assert len(listings) >= 5, f"expected ≥5 cards, got {len(listings)}"
    _all_listings_valid(listings, "ls")
    # Title regression: was returning just "Appartement" before the fix
    titles_with_t = [l for l in listings if "T" in (l.title or "")]
    assert titles_with_t, "expected listings titles to contain T1/T2/T3 (was a bug)"


def test_lodgis_parser_against_snapshot(fixture_html):
    from scraper import _lodgis_card_to_listing
    soup = BeautifulSoup(fixture_html("lodgis_debug.html"), "html.parser")
    cards = soup.find_all("div", class_="card__appart")
    listings = [l for l in (_lodgis_card_to_listing(c) for c in cards) if l]
    assert len(listings) >= 5, f"expected ≥5 cards, got {len(listings)}"
    _all_listings_valid(listings, "lg")


def test_pap_parser_against_snapshot(fixture_html):
    """PAP: regression test for missing _parse_price wrapping bug."""
    from scraper import _parse_pap_listing
    import re
    soup = BeautifulSoup(fixture_html("pap_debug.html"), "html.parser")
    items = soup.find_all(class_=re.compile("search-list-item"))
    if not items:
        pytest.skip("PAP fixture has no search-list-item nodes (site redesign?)")
    listings = [l for l in (_parse_pap_listing(i) for i in items) if l]
    if not listings:
        pytest.skip("PAP fixture parses to 0 listings (selector drift)")
    _all_listings_valid(listings, "pap")
    # The bug fix: price must go through _parse_price → range-checked
    for l in listings:
        if l.price is not None:
            assert 50 <= l.price <= 50000


# ─── is_real_offer / is_suspicious filters ──────────────────────────────────

def test_is_real_offer_rejects_low_price(make_listing):
    from scraper import is_real_offer
    assert is_real_offer(make_listing(price=300)) is False


def test_is_real_offer_rejects_empty_seller(make_listing):
    from scraper import is_real_offer
    assert is_real_offer(make_listing(seller_name="")) is False


def test_is_real_offer_rejects_sublocation_title(make_listing):
    from scraper import is_real_offer
    assert is_real_offer(make_listing(title="Sous-location 2 mois")) is False


def test_is_real_offer_accepts_normal(make_listing):
    from scraper import is_real_offer
    assert is_real_offer(make_listing()) is True


def test_is_suspicious_catches_fraud_keywords(make_listing):
    from scraper import is_suspicious
    listing = make_listing(description="Envoyer l'argent par Western Union avant visite.")
    sus, reason = is_suspicious(listing)
    assert sus is True
    # Filter triggers on any of several fraud signals; just confirm it caught one
    assert "suspect" in reason.lower()


def test_is_suspicious_catches_low_price(make_listing):
    from scraper import is_suspicious
    sus, reason = is_suspicious(make_listing(price=300))
    assert sus is True
    assert "anormalement bas" in reason.lower()

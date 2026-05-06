"""DB layer tests — atomic upsert, FIFO pending queue, status enum, indexes."""
import sqlite3
import pytest


def _insert(db, **fields):
    """Tiny helper: upsert a listing with sensible defaults."""
    defaults = dict(
        lbc_id="t1",
        title="T",
        price=900,
        location="Paris",
        seller_name="X",
        seller_type="",
        url="http://x",
        source="leboncoin",
    )
    defaults.update(fields)
    return db.upsert_listing(**defaults)


# ─── upsert_listing ──────────────────────────────────────────────────────────

def test_upsert_returns_same_id_for_duplicate(tmp_db):
    id1 = _insert(tmp_db, lbc_id="dup")
    id2 = _insert(tmp_db, lbc_id="dup", title="T-bis")
    assert id1 == id2


def test_upsert_updates_metadata_in_place(tmp_db):
    _insert(tmp_db, lbc_id="meta", title="Old", location="Paris")
    _insert(tmp_db, lbc_id="meta", title="New", location="Lyon")
    row = tmp_db.get_listing_by_lbc_id("meta")
    assert row["title"] == "New"
    assert row["location"] == "Lyon"


def test_upsert_tracks_price_drop(tmp_db):
    _insert(tmp_db, lbc_id="drop", price=1000)
    _insert(tmp_db, lbc_id="drop", price=850)
    row = tmp_db.get_listing_by_lbc_id("drop")
    assert row["price"] == 850
    assert row["price_prev"] == 1000


def test_upsert_no_price_prev_on_increase(tmp_db):
    _insert(tmp_db, lbc_id="up", price=800)
    _insert(tmp_db, lbc_id="up", price=900)
    row = tmp_db.get_listing_by_lbc_id("up")
    assert row["price"] == 800  # price doesn't go up
    assert row["price_prev"] is None


# ─── pending contacts ────────────────────────────────────────────────────────

def test_get_pending_contacts_empty(tmp_db):
    assert tmp_db.get_pending_contacts() == []
    assert tmp_db.count_pending_contacts() == 0


def test_get_pending_contacts_fifo(tmp_db):
    id_a = _insert(tmp_db, lbc_id="a", url="http://a")
    id_b = _insert(tmp_db, lbc_id="b", url="http://b")
    id_c = _insert(tmp_db, lbc_id="c", url="http://c")
    cid_a = tmp_db.create_contact(id_a, "msg a")
    cid_b = tmp_db.create_contact(id_b, "msg b")
    cid_c = tmp_db.create_contact(id_c, "msg c")
    pending = tmp_db.get_pending_contacts()
    assert [c["contact_id"] for c in pending] == [cid_a, cid_b, cid_c]
    assert [c["url"] for c in pending] == ["http://a", "http://b", "http://c"]


def test_get_pending_contacts_skips_sent(tmp_db):
    id1 = _insert(tmp_db, lbc_id="x")
    cid = tmp_db.create_contact(id1, "msg")
    tmp_db.mark_contact_sent(cid)
    assert tmp_db.get_pending_contacts() == []


def test_count_pending_excludes_sent(tmp_db):
    a = _insert(tmp_db, lbc_id="a")
    b = _insert(tmp_db, lbc_id="b")
    cid_a = tmp_db.create_contact(a, "ma")
    tmp_db.create_contact(b, "mb")
    tmp_db.mark_contact_sent(cid_a)
    assert tmp_db.count_pending_contacts() == 1


# ─── already_contacted: pending counts as contacted ─────────────────────────

def test_already_contacted_includes_pending(tmp_db):
    """Critical: re-running /campagne shouldn't create dup pending rows."""
    id1 = _insert(tmp_db, lbc_id="z")
    tmp_db.create_contact(id1, "m")
    assert tmp_db.already_contacted("z") is True


def test_already_contacted_unknown_returns_false(tmp_db):
    assert tmp_db.already_contacted("never-seen") is False


# ─── status enum validation ──────────────────────────────────────────────────

def test_mark_contact_status_rejects_invalid(tmp_db):
    id1 = _insert(tmp_db)
    cid = tmp_db.create_contact(id1, "m")
    with pytest.raises(ValueError, match="Statut invalide"):
        tmp_db.mark_contact_status(cid, "BOGUS")


@pytest.mark.parametrize("status", ["pending", "sent", "responded", "positive", "negative"])
def test_mark_contact_status_accepts_valid(tmp_db, status):
    id1 = _insert(tmp_db)
    cid = tmp_db.create_contact(id1, "m")
    tmp_db.mark_contact_status(cid, status)  # must not raise


# ─── price drops include lbc_id (regression: was missing) ────────────────────

def test_get_price_drops_includes_lbc_id(tmp_db):
    id1 = _insert(tmp_db, lbc_id="drop_id_test", price=1000)
    cid = tmp_db.create_contact(id1, "m")
    tmp_db.mark_contact_sent(cid)
    _insert(tmp_db, lbc_id="drop_id_test", price=850)
    drops = tmp_db.get_price_drops()
    assert drops, "expected at least one drop"
    assert "lbc_id" in drops[0]
    assert drops[0]["lbc_id"] == "drop_id_test"


# ─── Fraud detection ─────────────────────────────────────────────────────────

def test_is_suspicious_listing_clean(tmp_db):
    """Normal listing — no markers, no price anomaly — must come back clean."""
    listing = {
        "title": "Studio meublé 25m² Paris 11",
        "description": "Bel appartement lumineux, charges comprises.",
        "price": 850,
        "surface": 25,
        "location": "75011 Paris",
    }
    is_fraud, reason = tmp_db.is_suspicious_listing(listing)
    assert is_fraud is False
    assert reason == ""


@pytest.mark.parametrize("marker,phrase", [
    ("WhatsApp",        "Contactez-moi par WhatsApp uniquement"),
    ("Dubaï",           "Je suis actuellement à Dubaï pour mon travail"),
    ("Western Union",   "Envoyez la caution par Western Union"),
    ("PayPal Friends",  "Paiement via PayPal Friends svp"),
    ("héritage",        "L'appartement vient d'un héritage familial"),
    ("voyage",          "Je suis en voyage d'affaires à l'étranger"),
])
def test_is_suspicious_listing_flags_scam_markers(tmp_db, marker, phrase):
    listing = {
        "title": "Bel appartement",
        "description": phrase,
        "price": 700, "surface": 30, "location": "75011 Paris",
    }
    is_fraud, reason = tmp_db.is_suspicious_listing(listing)
    assert is_fraud is True, f"expected fraud flag for '{phrase}'"
    assert "arnaque" in reason.lower()


def test_is_suspicious_listing_flags_payment_before_visit(tmp_db):
    listing = {
        "title": "Studio à louer",
        "description": "Veuillez verser la caution avant visite, je vous enverrai les clés par la poste.",
        "price": 700, "surface": 25, "location": "Paris",
    }
    is_fraud, reason = tmp_db.is_suspicious_listing(listing)
    assert is_fraud is True
    assert "avant visite" in reason.lower() or "paiement" in reason.lower()


def test_is_suspicious_listing_flags_price_anomaly(tmp_db):
    """Listing 50%+ below the zone+surface median triggers the fraud flag."""
    # Seed 6 comparables at ~30€/m² in 75011, surface ~25m²
    for i, (price, surf) in enumerate([(750, 25), (770, 24), (800, 26),
                                        (820, 27), (790, 25), (810, 26)]):
        _insert(tmp_db, lbc_id=f"comp{i}", price=price, location="75011 Paris")
        # Manually set surface (upsert_listing test helper signature has it)
    # _insert above doesn't populate surface — use direct upsert with surface
    for i, (price, surf) in enumerate([(750, 25), (770, 24), (800, 26),
                                        (820, 27), (790, 25), (810, 26)]):
        tmp_db.upsert_listing(
            lbc_id=f"comp{i}", title="T", price=price, location="75011 Paris",
            seller_name="X", seller_type="", url=f"http://comp/{i}",
            source="leboncoin", surface=surf,
        )
    # Suspect: same zone + surface but priced 350€ (≈14€/m², ~50% below median)
    listing = {
        "title": "Trop beau pour être vrai",
        "description": "Sans contexte particulier",
        "price": 350,
        "surface": 25,
        "location": "75011 Paris",
    }
    is_fraud, reason = tmp_db.is_suspicious_listing(listing)
    assert is_fraud is True
    assert "%" in reason and "marché" in reason


def test_is_suspicious_listing_skips_anomaly_when_few_comps(tmp_db):
    """Fewer than the comp threshold → no price-based flag (sample too small)."""
    tmp_db.upsert_listing(
        lbc_id="only_one", title="T", price=800, location="75011 Paris",
        seller_name="X", seller_type="", url="http://x", source="leboncoin",
        surface=25,
    )
    # Even priced absurdly low, with only 1 comp we don't trust the median
    listing = {
        "title": "Solo", "description": "rien",
        "price": 100, "surface": 25, "location": "75011 Paris",
    }
    is_fraud, reason = tmp_db.is_suspicious_listing(listing)
    assert is_fraud is False
    assert reason == ""


def test_is_suspicious_listing_handles_none(tmp_db):
    """Defensive: must not crash on None / missing fields."""
    assert tmp_db.is_suspicious_listing(None) == (False, "")
    assert tmp_db.is_suspicious_listing({}) == (False, "")
    assert tmp_db.is_suspicious_listing(
        {"title": "x", "description": "x", "price": None, "surface": None,
         "location": ""}
    ) == (False, "")


def test_clear_price_prev_silences_repeat(tmp_db):
    id1 = _insert(tmp_db, lbc_id="silence", price=1000)
    cid = tmp_db.create_contact(id1, "m")
    tmp_db.mark_contact_sent(cid)
    _insert(tmp_db, lbc_id="silence", price=900)
    assert len(tmp_db.get_price_drops()) == 1
    tmp_db.clear_price_prev("silence")
    assert tmp_db.get_price_drops() == []


# ─── score persistence + recent listings ────────────────────────────────────

def test_set_listing_score_persists(tmp_db):
    _insert(tmp_db, lbc_id="scored")
    tmp_db.set_listing_score("scored", 9, "great match")
    row = tmp_db.get_listing_by_lbc_id("scored")
    assert row["score"] == 9
    assert row["score_reason"] == "great match"


def test_get_recent_listings_orders_desc(tmp_db):
    _insert(tmp_db, lbc_id="r1")
    _insert(tmp_db, lbc_id="r2")
    _insert(tmp_db, lbc_id="r3")
    recent = tmp_db.get_recent_listings(limit=10)
    assert [r["lbc_id"] for r in recent] == ["r3", "r2", "r1"]


def test_get_recent_listings_respects_limit(tmp_db):
    for i in range(15):
        _insert(tmp_db, lbc_id=f"l{i}")
    assert len(tmp_db.get_recent_listings(limit=5)) == 5


# ─── indexes (regression: speeds up rate-limit + tone reports) ───────────────

def test_surface_persists_through_upsert(tmp_db):
    """Surface (m²) was added in the latest schema migration."""
    tmp_db.upsert_listing(
        lbc_id="surf1", title="Studio", price=900, location="Paris",
        seller_name="X", seller_type="", url="http://x", source="leboncoin",
        surface=28,
    )
    row = tmp_db.get_listing_by_lbc_id("surf1")
    assert row["surface"] == 28


def test_surface_not_overwritten_by_null_on_reupsert(tmp_db):
    """If a re-scrape doesn't see surface, the previous value should stick."""
    tmp_db.upsert_listing(
        lbc_id="surf2", title="T", price=900, location="Paris", seller_name="X",
        seller_type="", url="http://x", source="leboncoin", surface=30,
    )
    tmp_db.upsert_listing(  # re-upsert without surface
        lbc_id="surf2", title="T", price=900, location="Paris", seller_name="X",
        seller_type="", url="http://x", source="leboncoin", surface=None,
    )
    assert tmp_db.get_listing_by_lbc_id("surf2")["surface"] == 30


# ─── query_listings ──────────────────────────────────────────────────────────

def _seed(db, lbc_id, **fields):
    defaults = dict(title="T", price=900, location="Paris", seller_name="X",
                    seller_type="", url=f"http://{lbc_id}", source="leboncoin",
                    surface=None)
    defaults.update(fields)
    db.upsert_listing(lbc_id=lbc_id, **defaults)


def test_query_listings_no_filters_returns_all(tmp_db):
    for i in range(3):
        _seed(tmp_db, f"q{i}", price=800 + i)
    assert len(tmp_db.query_listings()) == 3


def test_query_listings_filter_max_price(tmp_db):
    _seed(tmp_db, "p1", price=500)
    _seed(tmp_db, "p2", price=1500)
    rows = tmp_db.query_listings(max_price=1000)
    assert [r["lbc_id"] for r in rows] == ["p1"]


def test_query_listings_filter_min_surface(tmp_db):
    _seed(tmp_db, "s1", surface=20)
    _seed(tmp_db, "s2", surface=30)
    _seed(tmp_db, "s3", surface=None)  # NULL surface should be excluded
    rows = tmp_db.query_listings(min_surface=25)
    assert {r["lbc_id"] for r in rows} == {"s2"}


def test_query_listings_filter_source(tmp_db):
    _seed(tmp_db, "x1", source="leboncoin")
    _seed(tmp_db, "x2", source="studapart")
    rows = tmp_db.query_listings(source="studapart")
    assert [r["lbc_id"] for r in rows] == ["x2"]


def test_query_listings_sort_by_surface_desc(tmp_db):
    _seed(tmp_db, "ss1", surface=20)
    _seed(tmp_db, "ss2", surface=50)
    _seed(tmp_db, "ss3", surface=35)
    rows = tmp_db.query_listings(sort_by="surface")
    assert [r["lbc_id"] for r in rows[:3]] == ["ss2", "ss3", "ss1"]


def test_query_listings_sort_by_price_asc(tmp_db):
    _seed(tmp_db, "pp1", price=900)
    _seed(tmp_db, "pp2", price=500)
    _seed(tmp_db, "pp3", price=700)
    rows = tmp_db.query_listings(sort_by="price")
    assert [r["lbc_id"] for r in rows[:3]] == ["pp2", "pp3", "pp1"]


def test_query_listings_invalid_sort_raises(tmp_db):
    with pytest.raises(ValueError, match="sort_by"):
        tmp_db.query_listings(sort_by="bogus")


def test_upsert_refuses_mock_listing_by_id(tmp_db):
    """Regression: mock_data.MOCK_LISTINGS used to leak into production DB."""
    rid = tmp_db.upsert_listing(
        lbc_id="mock_001", title="Fake", price=790, location="Paris",
        seller_name="X", seller_type="", url="https://www.leboncoin.fr/annonces/mock_001.htm",
        source="leboncoin",
    )
    assert rid == 0  # sentinel: not persisted
    assert tmp_db.get_listing_by_lbc_id("mock_001") is None


def test_upsert_refuses_mock_listing_by_url(tmp_db):
    rid = tmp_db.upsert_listing(
        lbc_id="real_42", title="Studio", price=900, location="Paris",
        seller_name="X", seller_type="", url="https://example.com/foo/mock/bar",
        source="leboncoin",
    )
    assert rid == 0


def test_purge_mock_listings_removes_existing(tmp_db):
    """Idempotent cleanup of any mock rows persisted before the guard."""
    import sqlite3, config
    # Bypass the upsert guard to seed legacy mock rows directly
    with sqlite3.connect(config.DB_PATH) as conn:
        conn.execute(
            "INSERT INTO listings (lbc_id, source, title, price, location, "
            "seller_name, seller_type, url, scraped_at) "
            "VALUES (?,?,?,?,?,?,?,?,?)",
            ("mock_001", "leboncoin", "Fake", 790, "Paris", "X", "",
             "https://www.leboncoin.fr/annonces/mock_001.htm", "2026-05-02"),
        )
        conn.execute(
            "INSERT INTO listings (lbc_id, source, title, price, location, "
            "seller_name, seller_type, url, scraped_at) "
            "VALUES (?,?,?,?,?,?,?,?,?)",
            ("real_42", "leboncoin", "Real", 900, "Paris", "X", "",
             "https://www.leboncoin.fr/ad/locations/12345", "2026-05-02"),
        )
        conn.commit()

    n = tmp_db.purge_mock_listings()
    assert n == 1
    assert tmp_db.get_listing_by_lbc_id("mock_001") is None
    assert tmp_db.get_listing_by_lbc_id("real_42") is not None
    # Idempotent: second call deletes nothing
    assert tmp_db.purge_mock_listings() == 0


def test_indexes_exist_after_init(tmp_db):
    import config
    con = sqlite3.connect(config.DB_PATH)
    indexes = {
        r[0] for r in con.execute(
            "SELECT name FROM sqlite_master WHERE type='index' "
            "AND name NOT LIKE 'sqlite_%'"
        ).fetchall()
    }
    con.close()
    assert "idx_contacts_listing" in indexes
    assert "idx_contacts_status_sent" in indexes
    assert "idx_responses_contact" in indexes
    # New indexes for the activity check + dedup paths
    assert "idx_listings_source_seen" in indexes
    assert "idx_listings_dedup_key" in indexes
    assert "idx_listings_dedup_of" in indexes


# ─── Activity check (cross-source) ──────────────────────────────────────────

def test_mark_seen_stamps_seen_at(tmp_db):
    _insert(tmp_db, lbc_id="alive_1", source="seloger")
    n = tmp_db.mark_seen("seloger", ["alive_1"])
    assert n == 1
    row = tmp_db.get_listing_by_lbc_id("alive_1")
    assert row["seen_at"] is not None


def test_mark_seen_only_touches_matching_source(tmp_db):
    """seen_at update is scoped by (source, lbc_id) to avoid colliding with
    a listing of the same lbc_id on a different source — it shouldn't happen
    in practice (lbc_id is UNIQUE) but the SQL is defensive."""
    _insert(tmp_db, lbc_id="x_lbc", source="leboncoin")
    n = tmp_db.mark_seen("seloger", ["x_lbc"])
    assert n == 0  # no row matches (source, lbc_id)
    row = tmp_db.get_listing_by_lbc_id("x_lbc")
    assert row["seen_at"] is None


def test_mark_seen_empty_list_is_noop(tmp_db):
    assert tmp_db.mark_seen("seloger", []) == 0


def test_mark_stale_listings_hides_after_24h(tmp_db):
    """Listings whose seen_at < now-24h get score=0 so the dashboard hides them."""
    _insert(tmp_db, lbc_id="stale_a")
    # Manually backdate seen_at to 25h ago
    with sqlite3.connect(_dbpath()) as conn:
        conn.execute(
            "UPDATE listings SET seen_at = datetime('now','-25 hours'), score = 7 "
            "WHERE lbc_id = ?", ("stale_a",),
        )
        conn.commit()
    n = tmp_db.mark_stale_listings(hours=24)
    assert n == 1
    row = tmp_db.get_listing_by_lbc_id("stale_a")
    assert row["score"] == 0
    assert row["score_reason"] == "❌ disparu de la source"


def test_mark_stale_listings_skips_recent(tmp_db):
    _insert(tmp_db, lbc_id="fresh_a")
    tmp_db.mark_seen("leboncoin", ["fresh_a"])  # stamps NOW
    with sqlite3.connect(_dbpath()) as conn:
        conn.execute("UPDATE listings SET score = 7 WHERE lbc_id = ?", ("fresh_a",))
        conn.commit()
    assert tmp_db.mark_stale_listings(hours=24) == 0
    row = tmp_db.get_listing_by_lbc_id("fresh_a")
    assert row["score"] == 7  # untouched


def test_mark_stale_listings_skips_never_seen(tmp_db):
    """Rows where seen_at IS NULL haven't been processed by the new code path
    yet (legacy data). Don't touch them — the next /campagne will stamp."""
    _insert(tmp_db, lbc_id="legacy")
    with sqlite3.connect(_dbpath()) as conn:
        conn.execute("UPDATE listings SET score = 7 WHERE lbc_id = ?", ("legacy",))
        conn.commit()
    assert tmp_db.mark_stale_listings(hours=24) == 0
    assert tmp_db.get_listing_by_lbc_id("legacy")["score"] == 7


def test_mark_stale_listings_idempotent(tmp_db):
    """Running it twice doesn't double-flag (score>0 filter skips already-hidden)."""
    _insert(tmp_db, lbc_id="stale_b")
    with sqlite3.connect(_dbpath()) as conn:
        conn.execute(
            "UPDATE listings SET seen_at = datetime('now','-25 hours'), score=7 "
            "WHERE lbc_id = ?", ("stale_b",),
        )
        conn.commit()
    assert tmp_db.mark_stale_listings(hours=24) == 1
    # Second pass: no rows updated
    assert tmp_db.mark_stale_listings(hours=24) == 0


def _dbpath():
    """Helper: return current config.DB_PATH (tmp_db monkeypatches it)."""
    import config
    return config.DB_PATH


# ─── Dedup helpers ──────────────────────────────────────────────────────────

def test_compute_dedup_key_basic(tmp_db):
    k = tmp_db.compute_dedup_key(800, 30, "Paris 75011")
    # zip extracted, price//50=16, surface//2=15
    assert k == "75011|16|15"


def test_compute_dedup_key_no_zip_falls_back_to_city(tmp_db):
    k = tmp_db.compute_dedup_key(800, 30, "Marseille")
    # First 8 chars of city, lowercased
    assert k == "marseill|16|15"


def test_compute_dedup_key_returns_none_on_missing_data(tmp_db):
    assert tmp_db.compute_dedup_key(None, 30, "Paris") is None
    assert tmp_db.compute_dedup_key(800, None, "Paris") is None
    assert tmp_db.compute_dedup_key(800, 30, "") is None


def test_title_similarity_identical_returns_zero(tmp_db):
    assert tmp_db.title_similarity("Studio 30m²", "Studio 30m²") == 0.0


def test_title_similarity_totally_different_high(tmp_db):
    assert tmp_db.title_similarity("ABC", "XYZ") == 1.0


def test_title_similarity_close_titles_below_threshold(tmp_db):
    """A LBC and SeLoger version of the same listing typically share most words."""
    s = tmp_db.title_similarity(
        "Beau studio 30m² Paris 11ème",
        "Beau studio 30 m² Paris 11e",
    )
    assert s < 0.30


def test_apply_dedup_for_batch_flags_cross_source_duplicate(tmp_db):
    """Same flat on LBC and SeLoger — SeLoger arrives second, flagged dup."""
    tmp_db.upsert_listings_batch([{
        "lbc_id": "lbc_1", "source": "leboncoin",
        "title": "Studio 30m² Paris 11", "price": 900, "surface": 30,
        "location": "Paris 75011", "url": "http://lbc/1",
    }])
    tmp_db.apply_dedup_for_batch([{
        "lbc_id": "lbc_1", "source": "leboncoin",
        "title": "Studio 30m² Paris 11", "price": 900, "surface": 30,
        "location": "Paris 75011",
    }])
    tmp_db.upsert_listings_batch([{
        "lbc_id": "sel_1", "source": "seloger",
        "title": "Studio 30 m² Paris 11e", "price": 905, "surface": 30,
        "location": "Paris 75011", "url": "http://sl/1",
    }])
    n = tmp_db.apply_dedup_for_batch([{
        "lbc_id": "sel_1", "source": "seloger",
        "title": "Studio 30 m² Paris 11e", "price": 905, "surface": 30,
        "location": "Paris 75011",
    }])
    assert n == 1
    row = tmp_db.get_listing_by_lbc_id("sel_1")
    assert row["dedup_of"] == "lbc_1"
    # Primary is unchanged
    assert tmp_db.get_listing_by_lbc_id("lbc_1")["dedup_of"] is None


def test_apply_dedup_respects_price_tolerance(tmp_db):
    """800 vs 805 (<5%) is a match; 800 vs 900 is not."""
    tmp_db.upsert_listings_batch([{
        "lbc_id": "p1", "source": "leboncoin",
        "title": "Studio Paris", "price": 800, "surface": 25,
        "location": "Paris 75011", "url": "http://x",
    }])
    tmp_db.apply_dedup_for_batch([{
        "lbc_id": "p1", "source": "leboncoin",
        "title": "Studio Paris", "price": 800, "surface": 25,
        "location": "Paris 75011",
    }])
    # Within 5%: should dedup
    tmp_db.upsert_listings_batch([{
        "lbc_id": "p2", "source": "seloger",
        "title": "Studio Paris", "price": 805, "surface": 25,
        "location": "Paris 75011", "url": "http://y",
    }])
    tmp_db.apply_dedup_for_batch([{
        "lbc_id": "p2", "source": "seloger",
        "title": "Studio Paris", "price": 805, "surface": 25,
        "location": "Paris 75011",
    }])
    assert tmp_db.get_listing_by_lbc_id("p2")["dedup_of"] == "p1"
    # Outside 5%: should NOT dedup
    tmp_db.upsert_listings_batch([{
        "lbc_id": "p3", "source": "pap",
        "title": "Studio Paris", "price": 900, "surface": 25,
        "location": "Paris 75011", "url": "http://z",
    }])
    tmp_db.apply_dedup_for_batch([{
        "lbc_id": "p3", "source": "pap",
        "title": "Studio Paris", "price": 900, "surface": 25,
        "location": "Paris 75011",
    }])
    assert tmp_db.get_listing_by_lbc_id("p3")["dedup_of"] is None


def test_apply_dedup_respects_surface_tolerance(tmp_db):
    """28 vs 30 (Δ=2) is a match; 28 vs 31 is not (Δ=3>2)."""
    tmp_db.upsert_listings_batch([{
        "lbc_id": "s1", "source": "leboncoin",
        "title": "Studio", "price": 900, "surface": 28,
        "location": "Paris 75011", "url": "http://x",
    }])
    tmp_db.apply_dedup_for_batch([{
        "lbc_id": "s1", "source": "leboncoin",
        "title": "Studio", "price": 900, "surface": 28,
        "location": "Paris 75011",
    }])
    tmp_db.upsert_listings_batch([{
        "lbc_id": "s2", "source": "seloger",
        "title": "Studio", "price": 900, "surface": 30,
        "location": "Paris 75011", "url": "http://y",
    }])
    tmp_db.apply_dedup_for_batch([{
        "lbc_id": "s2", "source": "seloger",
        "title": "Studio", "price": 900, "surface": 30,
        "location": "Paris 75011",
    }])
    assert tmp_db.get_listing_by_lbc_id("s2")["dedup_of"] == "s1"


def test_apply_dedup_skips_same_source(tmp_db):
    """Two LBC listings should never dedup against each other (intra-source
    handled by ON CONFLICT(lbc_id), not the cross-source helper)."""
    tmp_db.upsert_listings_batch([{
        "lbc_id": "ss1", "source": "leboncoin",
        "title": "Studio", "price": 900, "surface": 30,
        "location": "Paris 75011", "url": "http://x",
    }])
    tmp_db.apply_dedup_for_batch([{
        "lbc_id": "ss1", "source": "leboncoin",
        "title": "Studio", "price": 900, "surface": 30,
        "location": "Paris 75011",
    }])
    tmp_db.upsert_listings_batch([{
        "lbc_id": "ss2", "source": "leboncoin",
        "title": "Studio", "price": 905, "surface": 30,
        "location": "Paris 75011", "url": "http://y",
    }])
    tmp_db.apply_dedup_for_batch([{
        "lbc_id": "ss2", "source": "leboncoin",
        "title": "Studio", "price": 905, "surface": 30,
        "location": "Paris 75011",
    }])
    assert tmp_db.get_listing_by_lbc_id("ss2")["dedup_of"] is None


def test_apply_dedup_skips_dissimilar_titles(tmp_db):
    """Same price/surface/zip but different titles → not a duplicate."""
    tmp_db.upsert_listings_batch([{
        "lbc_id": "t1", "source": "leboncoin",
        "title": "Studio meublé Paris 11", "price": 900, "surface": 30,
        "location": "Paris 75011", "url": "http://x",
    }])
    tmp_db.apply_dedup_for_batch([{
        "lbc_id": "t1", "source": "leboncoin",
        "title": "Studio meublé Paris 11", "price": 900, "surface": 30,
        "location": "Paris 75011",
    }])
    tmp_db.upsert_listings_batch([{
        "lbc_id": "t2", "source": "seloger",
        "title": "Loft industriel ancien atelier", "price": 900, "surface": 30,
        "location": "Paris 75011", "url": "http://y",
    }])
    tmp_db.apply_dedup_for_batch([{
        "lbc_id": "t2", "source": "seloger",
        "title": "Loft industriel ancien atelier", "price": 900, "surface": 30,
        "location": "Paris 75011",
    }])
    assert tmp_db.get_listing_by_lbc_id("t2")["dedup_of"] is None


def test_apply_dedup_earliest_seen_wins(tmp_db):
    """When 3 sources list the same flat, all 2nd+ point at the 1st."""
    rows = [
        ("a", "leboncoin"), ("b", "seloger"), ("c", "pap"),
    ]
    for lid, src in rows:
        tmp_db.upsert_listings_batch([{
            "lbc_id": lid, "source": src,
            "title": "Studio meublé", "price": 900, "surface": 30,
            "location": "Paris 75011", "url": f"http://x/{lid}",
        }])
        tmp_db.apply_dedup_for_batch([{
            "lbc_id": lid, "source": src,
            "title": "Studio meublé", "price": 900, "surface": 30,
            "location": "Paris 75011",
        }])
    assert tmp_db.get_listing_by_lbc_id("a")["dedup_of"] is None
    assert tmp_db.get_listing_by_lbc_id("b")["dedup_of"] == "a"
    assert tmp_db.get_listing_by_lbc_id("c")["dedup_of"] == "a"


def test_is_duplicate_returns_true_for_flagged(tmp_db):
    tmp_db.upsert_listings_batch([{
        "lbc_id": "primary", "source": "leboncoin",
        "title": "Studio", "price": 900, "surface": 30,
        "location": "Paris 75011", "url": "http://x",
    }])
    tmp_db.apply_dedup_for_batch([{
        "lbc_id": "primary", "source": "leboncoin",
        "title": "Studio", "price": 900, "surface": 30,
        "location": "Paris 75011",
    }])
    tmp_db.upsert_listings_batch([{
        "lbc_id": "dup", "source": "seloger",
        "title": "Studio", "price": 900, "surface": 30,
        "location": "Paris 75011", "url": "http://y",
    }])
    tmp_db.apply_dedup_for_batch([{
        "lbc_id": "dup", "source": "seloger",
        "title": "Studio", "price": 900, "surface": 30,
        "location": "Paris 75011",
    }])
    assert tmp_db.is_duplicate("dup") is True
    assert tmp_db.is_duplicate("primary") is False


def test_is_duplicate_unknown_returns_false(tmp_db):
    assert tmp_db.is_duplicate("never-seen") is False

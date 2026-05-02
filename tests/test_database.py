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

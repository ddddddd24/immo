"""Tests for sheets_sync — mocks gspread so no live Google API calls."""
import types
import pytest


def test_is_configured_returns_false_when_id_missing(tmp_db, monkeypatch):
    import config
    import sheets_sync
    monkeypatch.setattr(config, "GOOGLE_SHEET_ID", "")
    monkeypatch.setattr(config, "GOOGLE_SERVICE_ACCOUNT_JSON", "doesnt-matter.json")
    assert sheets_sync.is_configured() is False


def test_is_configured_returns_false_when_json_missing(tmp_db, monkeypatch, tmp_path):
    import config
    import sheets_sync
    monkeypatch.setattr(config, "GOOGLE_SHEET_ID", "abc")
    monkeypatch.setattr(config, "GOOGLE_SERVICE_ACCOUNT_JSON", str(tmp_path / "missing.json"))
    assert sheets_sync.is_configured() is False


def test_is_configured_returns_true_when_both_set(tmp_db, monkeypatch, tmp_path):
    import config
    import sheets_sync
    fake_json = tmp_path / "sa.json"
    fake_json.write_text('{"type": "service_account"}')
    monkeypatch.setattr(config, "GOOGLE_SHEET_ID", "abc")
    monkeypatch.setattr(config, "GOOGLE_SERVICE_ACCOUNT_JSON", str(fake_json))
    assert sheets_sync.is_configured() is True


def test_build_row_maps_all_columns():
    import sheets_sync
    listing = {
        "lbc_id": "lbc_42",
        "source": "leboncoin",
        "title": "Studio 28m²",
        "price": 850,
        "surface": 28,
        "location": "Paris 11",
        "url": "https://www.leboncoin.fr/...",
        "scraped_at": "2026-05-02T15:00:00",
        "score": 8,
        "status": "pending",
    }
    row = sheets_sync._build_row(listing)
    assert row[0] == "lbc_42"        # A — id
    assert row[1] == "leboncoin"     # B — source
    assert row[2] == "Studio 28m²"   # C — title
    assert row[3] == 850             # D — price
    assert row[4] == 28              # E — surface
    assert row[5] == "Paris 11"      # F — location
    assert row[6].startswith("https://")  # G — url
    assert row[7] == "2026-05-02"    # H — scraped (truncated to date)
    assert row[8] == 8               # I — score
    assert row[9] == "pending"       # J — status
    # Sanity: row stops at J — user columns K+ aren't populated by the bot
    assert len(row) == 10


def test_build_row_handles_missing_fields():
    import sheets_sync
    listing = {"lbc_id": "x", "source": "leboncoin", "title": "T"}
    row = sheets_sync._build_row(listing)
    assert row[0] == "x"
    # All other columns coerce None → ""
    for cell in row[3:]:
        assert cell == "" or cell is None or isinstance(cell, (int, str))


def test_sync_listings_skips_when_no_listings(tmp_db, monkeypatch):
    """If DB is empty, don't even open the worksheet (no API calls)."""
    import sheets_sync

    called = {"open": 0}
    def _fake_open():
        called["open"] += 1
        raise AssertionError("should not have opened worksheet")
    monkeypatch.setattr(sheets_sync, "_open_worksheet", _fake_open)

    out = sheets_sync.sync_listings()
    assert out == {"updated": 0, "appended": 0, "total": 0}
    assert called["open"] == 0


class _FakeWorksheet:
    """Minimal gspread.Worksheet mock for sync_listings tests."""
    def __init__(self, existing_col_a=None, existing_first_row=None):
        self._col_a = existing_col_a or [""]  # row 0 = header placeholder
        self._first_row = existing_first_row or []
        self.batch_updates = []
        self.appended = []
        self.header_writes = []

    def row_values(self, n):
        if n == 1:
            return self._first_row
        return []

    def col_values(self, n):
        return self._col_a

    def update(self, rng, values, value_input_option="RAW"):
        self.header_writes.append((rng, values))

    def batch_update(self, batch, value_input_option="RAW"):
        self.batch_updates.extend(batch)

    def append_rows(self, rows, value_input_option="RAW"):
        self.appended.extend(rows)


def _seed_listing(db, lbc_id, **fields):
    defaults = dict(
        title="T", price=900, location="Paris", seller_name="X",
        seller_type="", url=f"http://{lbc_id}", source="leboncoin", surface=28,
    )
    defaults.update(fields)
    db.upsert_listing(lbc_id=lbc_id, **defaults)


def test_sync_listings_appends_new_rows(tmp_db, monkeypatch):
    """Empty sheet → all DB listings appended; headers written."""
    import sheets_sync
    _seed_listing(tmp_db, "lbc_1", price=850)
    _seed_listing(tmp_db, "lbc_2", price=950)

    fake_ws = _FakeWorksheet()
    monkeypatch.setattr(sheets_sync, "_open_worksheet", lambda: fake_ws)

    out = sheets_sync.sync_listings()
    assert out["appended"] == 2
    assert out["updated"] == 0
    # Headers written when sheet was empty
    assert fake_ws.header_writes, "headers should be initialised on empty sheet"


def test_sync_listings_updates_existing_by_lbc_id(tmp_db, monkeypatch):
    """Existing rows get UPDATED in place (preserve user columns past J)."""
    import sheets_sync
    _seed_listing(tmp_db, "exist_1", price=900)
    _seed_listing(tmp_db, "new_1", price=950)

    # Sheet pretends row 2 already has lbc_id 'exist_1'
    fake_ws = _FakeWorksheet(
        existing_col_a=["lbc_id", "exist_1"],  # header + one row
        existing_first_row=sheets_sync.HEADERS,
    )
    monkeypatch.setattr(sheets_sync, "_open_worksheet", lambda: fake_ws)

    out = sheets_sync.sync_listings()
    assert out["updated"] == 1     # exist_1 updated
    assert out["appended"] == 1    # new_1 appended
    # Verify the update targets row 2 (where exist_1 already lives)
    assert any("A2:J2" in upd["range"] for upd in fake_ws.batch_updates)


def test_sync_listings_uses_db_status_field(tmp_db, monkeypatch):
    """Joined contact status (pending/sent/...) lands in column J."""
    import sheets_sync
    listing_id = tmp_db.upsert_listing(
        lbc_id="status_test", title="T", price=900, location="Paris",
        seller_name="X", seller_type="", url="http://x", source="leboncoin",
    )
    cid = tmp_db.create_contact(listing_id, "msg")
    tmp_db.mark_contact_sent(cid)

    fake_ws = _FakeWorksheet()
    monkeypatch.setattr(sheets_sync, "_open_worksheet", lambda: fake_ws)
    sheets_sync.sync_listings()

    # New row appended → column J (index 9) should be 'sent'
    assert fake_ws.appended[0][9] == "sent"

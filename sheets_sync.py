"""Google Sheets bidirectional sync for the listings table.

Pushes scraped listings to a Google Sheet that the user can browse on
mobile/desktop, share with their partner/family, and annotate. Preserves
user-added columns (Notes, Visite prévue, etc.) on every sync.

Setup steps for the user (one-time):
1. Go to https://console.cloud.google.com/ → create a project (or pick one).
2. Enable the Google Sheets API for that project.
3. Create a Service Account → download its JSON key.
4. Save the JSON locally (e.g. data/google_service_account.json).
5. Create a Google Sheet, share it with the service-account's email
   (xxx@xxx.iam.gserviceaccount.com) as Editor.
6. Copy the Sheet ID from its URL: docs.google.com/spreadsheets/d/<ID>/edit
7. Set in .env:
       GOOGLE_SHEET_ID=<the id>
       GOOGLE_SERVICE_ACCOUNT_JSON=data/google_service_account.json

The bot's /sync command (or auto-sync at end of /campagne) writes to the
first worksheet. User-edited columns to the right of column J are preserved
across syncs.
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger(__name__)

# Schema: columns A..J are bot-managed, K+ is user-editable
HEADERS = [
    "lbc_id",       # A — stable id, used to upsert. Hide in Sheets UI if you want.
    "source",       # B
    "titre",        # C
    "prix (€)",     # D
    "m²",           # E
    "ville",        # F
    "url",          # G
    "scrapé le",    # H
    "score",        # I
    "statut",       # J — pending/sent/responded/etc.
    # User columns past column J are preserved across syncs:
    "notes",        # K (user)
    "visite",       # L (user)
]


def _client():
    """Lazy import + auth so the bot can boot without gspread if disabled."""
    import gspread
    from google.oauth2.service_account import Credentials
    import config

    json_path = Path(config.GOOGLE_SERVICE_ACCOUNT_JSON)
    if not json_path.exists():
        raise FileNotFoundError(
            f"Service-account JSON not found at {json_path}. "
            "See sheets_sync.py module docstring for setup steps."
        )
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive.file",
    ]
    creds = Credentials.from_service_account_file(str(json_path), scopes=scopes)
    return gspread.authorize(creds)


def _open_worksheet():
    import config
    gc = _client()
    sh = gc.open_by_key(config.GOOGLE_SHEET_ID)
    # First worksheet by default
    return sh.sheet1


def is_configured() -> bool:
    """Check whether all the env vars are populated AND service account file exists."""
    import config
    if not getattr(config, "GOOGLE_SHEET_ID", "") or not getattr(config, "GOOGLE_SERVICE_ACCOUNT_JSON", ""):
        return False
    return Path(config.GOOGLE_SERVICE_ACCOUNT_JSON).exists()


def _ensure_headers(ws) -> None:
    """Write headers to row 1 if the sheet is empty."""
    first_row = ws.row_values(1)
    if not first_row:
        ws.update("A1", [HEADERS], value_input_option="RAW")
        logger.info("Initialised Sheets headers")
    elif first_row[:len(HEADERS)] != HEADERS:
        # Update only the bot-managed headers, leave user-added headers past column L
        ws.update("A1:L1", [HEADERS], value_input_option="RAW")
        logger.info("Refreshed Sheets headers")


def _build_row(listing: dict) -> list:
    """Map a DB listing dict to a sheet row (columns A..J only — K+ is user)."""
    return [
        listing.get("lbc_id") or "",
        listing.get("source") or "",
        listing.get("title") or "",
        listing.get("price") or "",
        listing.get("surface") or "",
        listing.get("location") or "",
        listing.get("url") or "",
        (listing.get("scraped_at") or "")[:10],
        listing.get("score") or "",
        listing.get("status") or "",
    ]


def sync_listings(limit: int = 500) -> dict:
    """Push the latest `limit` listings (with status) from DB to the sheet.

    Strategy: read the existing sheet to map lbc_id → row index. For each
    listing in the DB, either UPDATE its row (cols A..J) or APPEND a new row.
    User-added columns past J are never touched.

    Returns {"updated": N, "appended": M, "total": T}.
    """
    import database

    # Pull DB rows with the latest contact status joined in
    rows = database._query if False else None  # noqa: keep import context
    import sqlite3, config
    conn = sqlite3.connect(config.DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        listings = [dict(r) for r in conn.execute(f"""
            SELECT l.lbc_id, l.source, l.title, l.price, l.surface, l.location,
                   l.url, l.scraped_at, l.score,
                   (SELECT c.status FROM contacts c WHERE c.listing_id = l.id
                    ORDER BY c.id DESC LIMIT 1) AS status
            FROM listings l
            ORDER BY l.id DESC
            LIMIT ?
        """, (limit,)).fetchall()]
    finally:
        conn.close()

    if not listings:
        logger.info("No listings to sync")
        return {"updated": 0, "appended": 0, "total": 0}

    ws = _open_worksheet()
    _ensure_headers(ws)

    # Build lbc_id -> sheet row index from existing sheet (column A)
    col_a = ws.col_values(1)  # includes header row at index 0
    existing: dict[str, int] = {}
    for i, lbc_id in enumerate(col_a):
        if i == 0:  # skip header
            continue
        if lbc_id:
            existing[lbc_id] = i + 1  # 1-indexed sheet row

    updates: list[tuple[str, list]] = []  # (range, values)
    new_rows: list[list] = []

    for l in listings:
        row_data = _build_row(l)
        lbc_id = l.get("lbc_id")
        if not lbc_id:
            continue
        if lbc_id in existing:
            row_idx = existing[lbc_id]
            updates.append((f"A{row_idx}:J{row_idx}", [row_data]))
        else:
            new_rows.append(row_data)

    # Batch updates
    if updates:
        ws.batch_update([
            {"range": rng, "values": vals} for rng, vals in updates
        ], value_input_option="RAW")
    if new_rows:
        # Append at the end — user-edited columns K+ on existing rows untouched
        ws.append_rows(new_rows, value_input_option="RAW")

    summary = {
        "updated": len(updates),
        "appended": len(new_rows),
        "total": len(listings),
    }
    logger.info("Sheets sync done: %s", summary)
    return summary

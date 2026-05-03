"""SQLite persistence layer — listings, contacts, responses."""
import sqlite3
import logging
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Optional

import config

logger = logging.getLogger(__name__)


# ─── Setup ────────────────────────────────────────────────────────────────────

def purge_mock_listings() -> int:
    """One-shot cleanup: drop any mock_data.py fixtures that polluted the
    production DB before the upsert guard was in place. Returns count deleted.

    Safe to run at every startup: idempotent, logs nothing if no rows match.
    """
    with _conn() as conn:
        # Cascade-delete contacts first (foreign key)
        conn.execute(
            """DELETE FROM contacts WHERE listing_id IN (
                   SELECT id FROM listings
                   WHERE lbc_id LIKE 'mock_%' OR url LIKE '%/mock%'
               )"""
        )
        cur = conn.execute(
            "DELETE FROM listings WHERE lbc_id LIKE 'mock_%' OR url LIKE '%/mock%'"
        )
        n = cur.rowcount
    if n > 0:
        logger.info("Purged %d mock listing(s) from production DB", n)
    return n


def init_db() -> None:
    """Create tables if they don't exist."""
    Path(config.DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    with _conn() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS listings (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                lbc_id      TEXT    UNIQUE NOT NULL,
                source      TEXT    NOT NULL DEFAULT 'leboncoin',
                title       TEXT,
                price       INTEGER,
                location    TEXT,
                seller_name TEXT,
                seller_type TEXT,
                url         TEXT,
                scraped_at  TEXT    NOT NULL
            );

            CREATE TABLE IF NOT EXISTS contacts (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                listing_id   INTEGER NOT NULL REFERENCES listings(id),
                message_sent TEXT,
                sent_at      TEXT,
                status       TEXT    NOT NULL DEFAULT 'pending'
                                     CHECK(status IN ('pending','sent','responded','positive','negative'))
            );

            CREATE TABLE IF NOT EXISTS responses (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                contact_id    INTEGER NOT NULL REFERENCES contacts(id),
                response_text TEXT,
                received_at   TEXT    NOT NULL,
                sentiment     TEXT    CHECK(sentiment IN ('positive','negative','neutral'))
            );
            CREATE TABLE IF NOT EXISTS visits (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                url         TEXT NOT NULL,
                date_str    TEXT NOT NULL,
                created_at  TEXT NOT NULL,
                done        INTEGER NOT NULL DEFAULT 0
            );

            CREATE INDEX IF NOT EXISTS idx_contacts_listing
                ON contacts(listing_id);
            CREATE INDEX IF NOT EXISTS idx_contacts_status_sent
                ON contacts(status, sent_at);
            CREATE INDEX IF NOT EXISTS idx_responses_contact
                ON responses(contact_id);
        """)
    # Migrate existing DBs: add missing columns
    with _conn() as conn:
        cols = [r[1] for r in conn.execute("PRAGMA table_info(listings)").fetchall()]
        if "source" not in cols:
            conn.execute("ALTER TABLE listings ADD COLUMN source TEXT NOT NULL DEFAULT 'leboncoin'")
            logger.info("Migrated listings table: added source column")
        if "price_prev" not in cols:
            conn.execute("ALTER TABLE listings ADD COLUMN price_prev INTEGER")
            logger.info("Migrated listings table: added price_prev column")
        if "score" not in cols:
            conn.execute("ALTER TABLE listings ADD COLUMN score INTEGER")
            logger.info("Migrated listings table: added score column")
        if "score_reason" not in cols:
            conn.execute("ALTER TABLE listings ADD COLUMN score_reason TEXT")
            logger.info("Migrated listings table: added score_reason column")
        if "surface" not in cols:
            conn.execute("ALTER TABLE listings ADD COLUMN surface INTEGER")
            logger.info("Migrated listings table: added surface column (m²)")
        if "housing_type" not in cols:
            conn.execute("ALTER TABLE listings ADD COLUMN housing_type TEXT")
            logger.info("Migrated listings table: added housing_type column")
        if "roommate_count" not in cols:
            conn.execute("ALTER TABLE listings ADD COLUMN roommate_count INTEGER")
            logger.info("Migrated listings table: added roommate_count column")

    logger.info("Database initialised at %s", config.DB_PATH)


@contextmanager
def _conn():
    conn = sqlite3.connect(config.DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ─── Listings ─────────────────────────────────────────────────────────────────

_MOCK_LBC_ID_PREFIX = "mock_"
_MOCK_URL_HINT = "/mock"


def _is_mock_listing(lbc_id: str, url: str) -> bool:
    """Detect mock_data.py fixtures so they never pollute production DB.

    mock_data.MOCK_LISTINGS uses lbc_id 'mock_001' .. 'mock_NNN' and URLs
    matching '...annonces/mock_NNN.htm'. Real LBC IDs are pure digits.
    """
    return (
        lbc_id.startswith(_MOCK_LBC_ID_PREFIX)
        or _MOCK_URL_HINT in (url or "")
    )


def upsert_listing(
    lbc_id: str,
    title: str,
    price: Optional[int],
    location: str,
    seller_name: str,
    seller_type: str,
    url: str,
    source: str = "leboncoin",
    surface: Optional[int] = None,
    housing_type: str = "",
    roommate_count: Optional[int] = None,
) -> int:
    """Insert listing or update in place. Tracks downward price changes. Returns row id.

    Atomic via ON CONFLICT...DO UPDATE so concurrent /watch + /campagne calls
    can't race on the SELECT-then-INSERT pattern. Updates surface in place
    if the scraper extracted a non-null m² value.

    Refuses to persist mock_data.py fixtures — those are for UI smoke tests
    only and would surface as fake listings to the user later.
    """
    if _is_mock_listing(lbc_id, url):
        logger.debug("Refusing to persist mock listing %s — production DB only", lbc_id)
        # Return a sentinel id 0 so callers don't crash; real id is never 0
        # (AUTOINCREMENT starts at 1).
        return 0
    with _conn() as conn:
        cur = conn.execute(
            """INSERT INTO listings
               (lbc_id, source, title, price, location, seller_name, seller_type, url, scraped_at, surface, housing_type, roommate_count)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(lbc_id) DO UPDATE SET
                 title          = excluded.title,
                 location       = excluded.location,
                 seller_name    = excluded.seller_name,
                 seller_type    = excluded.seller_type,
                 url            = excluded.url,
                 surface        = COALESCE(excluded.surface, listings.surface),
                 housing_type   = COALESCE(NULLIF(excluded.housing_type, ''), listings.housing_type),
                 roommate_count = COALESCE(excluded.roommate_count, listings.roommate_count),
                 price_prev  = CASE
                     WHEN excluded.price IS NOT NULL
                          AND listings.price IS NOT NULL
                          AND excluded.price < listings.price
                     THEN listings.price
                     ELSE listings.price_prev
                 END,
                 price       = CASE
                     WHEN excluded.price IS NOT NULL
                          AND listings.price IS NOT NULL
                          AND excluded.price < listings.price
                     THEN excluded.price
                     ELSE listings.price
                 END
               RETURNING id""",
            (lbc_id, source, title, price, location, seller_name, seller_type, url,
             datetime.utcnow().isoformat(), surface, housing_type or "", roommate_count),
        )
        return cur.fetchone()[0]


def already_contacted(lbc_id: str) -> bool:
    """Return True if we have any contact row for this listing.

    With the prepare/send split, a pending contact (message ready but not yet
    sent) also blocks re-preparation — re-running /campagne won't recreate
    duplicate pending rows.
    """
    with _conn() as conn:
        cur = conn.execute(
            """SELECT c.id FROM contacts c
               JOIN listings l ON l.id = c.listing_id
               WHERE l.lbc_id = ?
               LIMIT 1""",
            (lbc_id,),
        )
        return cur.fetchone() is not None


def get_pending_contacts() -> list[dict]:
    """Return contacts prepared but not yet sent (status='pending').

    Each row: {contact_id, url, message, title, location}. Ordered by
    creation order (oldest first) so /envoyer drains FIFO.
    """
    with _conn() as conn:
        rows = conn.execute(
            """SELECT c.id           AS contact_id,
                      l.url          AS url,
                      c.message_sent AS message,
                      l.title        AS title,
                      l.location     AS location
               FROM contacts c
               JOIN listings l ON l.id = c.listing_id
               WHERE c.status = 'pending'
               ORDER BY c.id ASC"""
        ).fetchall()
    return [dict(r) for r in rows]


def count_pending_contacts() -> int:
    with _conn() as conn:
        return conn.execute(
            "SELECT COUNT(*) AS n FROM contacts WHERE status = 'pending'"
        ).fetchone()["n"]


def get_recent_listings(limit: int = 10) -> list[dict]:
    """Return the most recently scraped listings (insertion order desc).

    Used by the NL `list_recent` tool to give the user a real, grounded
    answer when they ask 'qu'as-tu trouvé ?' — bypasses the LLM's tendency
    to hallucinate URLs from memory.
    """
    with _conn() as conn:
        rows = conn.execute(
            """SELECT lbc_id, source, title, price, location, url, scraped_at,
                      score, surface, housing_type, roommate_count
               FROM listings
               ORDER BY id DESC
               LIMIT ?""",
            (limit,),
        ).fetchall()
    return [dict(r) for r in rows]


_VALID_SORT = {"surface", "price", "recent", "score"}


def query_listings(
    *,
    source: Optional[str] = None,
    min_price: Optional[int] = None,
    max_price: Optional[int] = None,
    min_surface: Optional[int] = None,
    max_surface: Optional[int] = None,
    sort_by: str = "recent",
    limit: int = 100,
) -> list[dict]:
    """Polyvalent listing query for the NL `query_listings` / /rapport tool.

    Filters by source / price range / surface range; sorts by surface
    (descending), price (ascending), recent (insertion order desc), or
    score (descending). All filters are optional. Returns up to `limit`
    rows with full listing fields including surface.
    """
    if sort_by not in _VALID_SORT:
        raise ValueError(f"sort_by must be one of {sorted(_VALID_SORT)}")
    where = []
    params: list = []
    if source:
        where.append("source = ?")
        params.append(source)
    if min_price is not None:
        where.append("price >= ?")
        params.append(min_price)
    if max_price is not None:
        where.append("(price IS NOT NULL AND price <= ?)")
        params.append(max_price)
    if min_surface is not None:
        where.append("(surface IS NOT NULL AND surface >= ?)")
        params.append(min_surface)
    if max_surface is not None:
        where.append("(surface IS NOT NULL AND surface <= ?)")
        params.append(max_surface)
    where_sql = " WHERE " + " AND ".join(where) if where else ""

    order_sql = {
        "surface": "ORDER BY surface DESC NULLS LAST, id DESC",
        "price":   "ORDER BY price ASC NULLS LAST, id DESC",
        "score":   "ORDER BY score DESC NULLS LAST, id DESC",
        "recent":  "ORDER BY id DESC",
    }[sort_by]
    sql = (
        "SELECT lbc_id, source, title, price, location, url, scraped_at, "
        "score, surface, housing_type, roommate_count "
        f"FROM listings{where_sql} {order_sql} LIMIT ?"
    )
    params.append(limit)
    with _conn() as conn:
        rows = conn.execute(sql, tuple(params)).fetchall()
    return [dict(r) for r in rows]


def get_listing_by_lbc_id(lbc_id: str) -> Optional[sqlite3.Row]:
    with _conn() as conn:
        return conn.execute(
            "SELECT * FROM listings WHERE lbc_id = ?", (lbc_id,)
        ).fetchone()


def set_listing_score(lbc_id: str, score: int, reason: str) -> None:
    """Record Claude's score (1–10) and short reason for a listing."""
    with _conn() as conn:
        conn.execute(
            "UPDATE listings SET score = ?, score_reason = ? WHERE lbc_id = ?",
            (score, reason, lbc_id),
        )


def get_unscored_listings(limit: Optional[int] = None) -> list[dict]:
    """Return listings whose score is NULL — for the backfill / score_all flow."""
    sql = """SELECT lbc_id, source, title, price, location, url, surface,
                    housing_type, roommate_count
             FROM listings
             WHERE score IS NULL
             ORDER BY id DESC"""
    if limit is not None:
        sql += " LIMIT ?"
        params: tuple = (limit,)
    else:
        params = ()
    with _conn() as conn:
        rows = conn.execute(sql, params).fetchall()
    return [dict(r) for r in rows]


# ─── Contacts ─────────────────────────────────────────────────────────────────

def create_contact(listing_id: int, message: str) -> int:
    with _conn() as conn:
        cur = conn.execute(
            "INSERT INTO contacts (listing_id, message_sent, status) VALUES (?,?,'pending')",
            (listing_id, message),
        )
        return cur.lastrowid


def mark_contact_sent(contact_id: int) -> None:
    with _conn() as conn:
        conn.execute(
            "UPDATE contacts SET status='sent', sent_at=? WHERE id=?",
            (datetime.utcnow().isoformat(), contact_id),
        )


_VALID_STATUS = {"pending", "sent", "responded", "positive", "negative"}


def mark_contact_status(contact_id: int, status: str) -> None:
    if status not in _VALID_STATUS:
        raise ValueError(
            f"Statut invalide: {status!r} (valeurs autorisées : {sorted(_VALID_STATUS)})"
        )
    with _conn() as conn:
        conn.execute(
            "UPDATE contacts SET status=? WHERE id=?", (status, contact_id)
        )


def messages_sent_last_hour() -> int:
    """Count messages sent in the past 60 minutes (rate-limiting)."""
    with _conn() as conn:
        cur = conn.execute(
            """SELECT COUNT(*) as n FROM contacts
               WHERE status='sent'
               AND sent_at >= datetime('now', '-1 hour')"""
        )
        return cur.fetchone()["n"]


# ─── Responses ────────────────────────────────────────────────────────────────

def save_response(contact_id: int, text: str, sentiment: str) -> None:
    with _conn() as conn:
        conn.execute(
            "INSERT INTO responses (contact_id, response_text, received_at, sentiment) VALUES (?,?,?,?)",
            (contact_id, text, datetime.utcnow().isoformat(), sentiment),
        )
        conn.execute(
            "UPDATE contacts SET status='responded' WHERE id=?", (contact_id,)
        )


# ─── Stats ────────────────────────────────────────────────────────────────────

def today_stats() -> dict:
    with _conn() as conn:
        scraped = conn.execute(
            "SELECT COUNT(*) as n FROM listings WHERE scraped_at >= date('now')"
        ).fetchone()["n"]

        sent = conn.execute(
            "SELECT COUNT(*) as n FROM contacts WHERE status='sent' AND sent_at >= date('now')"
        ).fetchone()["n"]

        positive = conn.execute(
            """SELECT COUNT(*) as n FROM responses r
               JOIN contacts c ON c.id = r.contact_id
               WHERE r.sentiment='positive' AND r.received_at >= date('now')"""
        ).fetchone()["n"]

        negative = conn.execute(
            """SELECT COUNT(*) as n FROM responses r
               JOIN contacts c ON c.id = r.contact_id
               WHERE r.sentiment='negative' AND r.received_at >= date('now')"""
        ).fetchone()["n"]

        no_response = sent - conn.execute(
            """SELECT COUNT(DISTINCT c.id) as n FROM contacts c
               JOIN responses r ON r.contact_id = c.id
               WHERE c.status='sent' AND c.sent_at >= date('now')"""
        ).fetchone()["n"]

    return {
        "scraped": scraped,
        "sent": sent,
        "positive": positive,
        "negative": negative,
        "no_response": max(no_response, 0),
    }


# ─── Price drops ──────────────────────────────────────────────────────────────

def get_price_drops() -> list[dict]:
    """Return listings we've contacted where price dropped since last scrape."""
    with _conn() as conn:
        rows = conn.execute(
            """SELECT l.lbc_id, l.title, l.url,
                      l.price as new_price, l.price_prev as old_price
               FROM listings l
               JOIN contacts c ON c.listing_id = l.id
               WHERE l.price_prev IS NOT NULL AND l.price_prev > l.price
               AND c.status = 'sent'
               ORDER BY (l.price_prev - l.price) DESC
               LIMIT 10"""
        ).fetchall()
    return [dict(r) for r in rows]


def clear_price_prev(lbc_id: str) -> None:
    """Reset price_prev after alerting (avoid repeat notifications)."""
    with _conn() as conn:
        conn.execute("UPDATE listings SET price_prev = NULL WHERE lbc_id = ?", (lbc_id,))


# ─── Visits ───────────────────────────────────────────────────────────────────

def save_visit(url: str, date_str: str) -> int:
    """Save a visit appointment. Returns visit id."""
    with _conn() as conn:
        cur = conn.execute(
            "INSERT INTO visits (url, date_str, created_at) VALUES (?,?,?)",
            (url, date_str, datetime.utcnow().isoformat()),
        )
        return cur.lastrowid


def get_upcoming_visits() -> list[dict]:
    with _conn() as conn:
        rows = conn.execute(
            "SELECT id, url, date_str FROM visits WHERE done = 0 ORDER BY created_at"
        ).fetchall()
    return [dict(r) for r in rows]


def mark_visit_done(visit_id: int) -> None:
    with _conn() as conn:
        conn.execute("UPDATE visits SET done = 1 WHERE id = ?", (visit_id,))


# ─── Price-drop re-contact ────────────────────────────────────────────────────

def get_uncontacted_price_drops(max_price: int) -> list[dict]:
    """Listings in DB (never contacted) whose price just dropped to <= max_price."""
    with _conn() as conn:
        rows = conn.execute(
            """SELECT l.lbc_id, l.title, l.url, l.price, l.price_prev,
                      l.location, l.seller_name, l.source, l.seller_type
               FROM listings l
               WHERE l.price_prev IS NOT NULL
               AND l.price_prev > l.price
               AND l.price <= ?
               AND l.id NOT IN (SELECT listing_id FROM contacts)
               ORDER BY (l.price_prev - l.price) DESC
               LIMIT 10""",
            (max_price,),
        ).fetchall()
    return [dict(r) for r in rows]


# ─── Response rate stats ──────────────────────────────────────────────────────

def tone_response_rates() -> dict:
    """Response rates by seller_type → {type: {sent, responded, rate}}."""
    with _conn() as conn:
        rows = conn.execute(
            """SELECT l.seller_type,
                      COUNT(DISTINCT c.id)           AS sent,
                      COUNT(DISTINCT r.contact_id)   AS responded
               FROM contacts c
               JOIN listings l ON l.id = c.listing_id
               LEFT JOIN responses r ON r.contact_id = c.id
               WHERE c.status IN ('sent', 'responded', 'positive', 'negative')
               GROUP BY l.seller_type"""
        ).fetchall()
    result = {}
    for row in rows:
        st = row["seller_type"] or "inconnu"
        sent = row["sent"]
        responded = row["responded"]
        result[st] = {
            "sent": sent,
            "responded": responded,
            "rate": round(responded / sent * 100) if sent else 0,
        }
    return result


def stale_contacts_count(days: int = 5) -> int:
    """Count sent contacts with no reply older than `days` days."""
    with _conn() as conn:
        return conn.execute(
            """SELECT COUNT(*) AS n FROM contacts
               WHERE status = 'sent'
               AND sent_at <= datetime('now', ?)
               AND id NOT IN (SELECT DISTINCT contact_id FROM responses)""",
            (f"-{days} days",),
        ).fetchone()["n"]


# ─── Contacts for response reading ───────────────────────────────────────────

def get_sent_contacts_without_response() -> list[dict]:
    """Return all contacts that were sent but have no response yet."""
    with _conn() as conn:
        rows = conn.execute(
            """SELECT c.id as contact_id, l.url, l.lbc_id, l.source, c.sent_at
               FROM contacts c
               JOIN listings l ON l.id = c.listing_id
               WHERE c.status = 'sent'
               ORDER BY c.sent_at DESC"""
        ).fetchall()
    return [dict(r) for r in rows]

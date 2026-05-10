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
    # Switch to WAL once — it's a database-level setting that persists. WAL
    # lets readers (dashboard) and writers (bot) operate concurrently without
    # blocking each other.
    with _conn() as conn:
        conn.execute("PRAGMA journal_mode = WAL")
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

            CREATE TABLE IF NOT EXISTS system_metrics (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                ts              INTEGER NOT NULL,
                bot_ram_mb      REAL    NOT NULL,
                children_ram_mb REAL    NOT NULL,
                total_ram_mb    REAL    NOT NULL,
                cpu_percent     REAL    NOT NULL,
                warm_contexts   INTEGER NOT NULL,
                children_count  INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_system_metrics_ts
                ON system_metrics(ts);

            CREATE TABLE IF NOT EXISTS bot_state (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
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
        if "published_at" not in cols:
            conn.execute("ALTER TABLE listings ADD COLUMN published_at TEXT")
            logger.info("Migrated listings table: added published_at column (date listing was posted on the source site)")
        if "phone" not in cols:
            conn.execute("ALTER TABLE listings ADD COLUMN phone TEXT")
            logger.info("Migrated listings table: added phone column (#blocked=site policy, ''=listing has none, ...=number)")
        if "available_from" not in cols:
            conn.execute("ALTER TABLE listings ADD COLUMN available_from TEXT")
            logger.info("Migrated listings table: added available_from column (YYYY-MM date listing becomes available)")
        if "description" not in cols:
            conn.execute("ALTER TABLE listings ADD COLUMN description TEXT")
            logger.info("Migrated listings table: added description column (used by /score_all to backfill LLM extraction)")
        if "availability_extracted" not in cols:
            conn.execute("ALTER TABLE listings ADD COLUMN availability_extracted INTEGER DEFAULT 0")
            logger.info("Migrated listings table: added availability_extracted flag (1 once LLM has tried; prevents token waste on /score_all loops)")
        if "notified" not in cols:
            conn.execute("ALTER TABLE listings ADD COLUMN notified INTEGER NOT NULL DEFAULT 0")
            logger.info("Migrated listings table: added notified flag (1 = push alert sent; prevents duplicate Telegram pings)")
        if "call_status" not in cols:
            conn.execute("ALTER TABLE listings ADD COLUMN call_status TEXT")
            logger.info("Migrated listings table: added call_status (set via push inline buttons: 'called', 'rented', 'skipped')")
        if "seen_at" not in cols:
            conn.execute("ALTER TABLE listings ADD COLUMN seen_at TEXT")
            logger.info("Migrated listings table: added seen_at (last time listing appeared in a /campagne for its source — drives 24h activity check)")
        if "dedup_of" not in cols:
            conn.execute("ALTER TABLE listings ADD COLUMN dedup_of TEXT")
            logger.info("Migrated listings table: added dedup_of (lbc_id of the primary cross-source variant — NULL for primaries)")
        if "dedup_key" not in cols:
            conn.execute("ALTER TABLE listings ADD COLUMN dedup_key TEXT")
            logger.info("Migrated listings table: added dedup_key (coarse fingerprint zip|price_bucket|surface_bucket — speeds cross-source candidate lookup)")

        # Indexes that the activity check + dedup queries lean on.
        # Idempotent so they run safely on every startup.
        conn.execute("CREATE INDEX IF NOT EXISTS idx_listings_source_seen ON listings(source, seen_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_listings_dedup_key ON listings(dedup_key)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_listings_dedup_of ON listings(dedup_of)")

    logger.info("Database initialised at %s", config.DB_PATH)


@contextmanager
def _conn():
    conn = sqlite3.connect(config.DB_PATH, timeout=30.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    # synchronous=NORMAL is safe with WAL and ~3x faster on writes.
    # busy_timeout lets the dashboard and bot share the DB without spurious
    # "database is locked" errors when a write collides with a read.
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.execute("PRAGMA busy_timeout = 5000")
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
    published_at: Optional[str] = None,
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
               (lbc_id, source, title, price, location, seller_name, seller_type, url, scraped_at, surface, housing_type, roommate_count, published_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(lbc_id) DO UPDATE SET
                 source         = excluded.source,
                 title          = excluded.title,
                 location       = excluded.location,
                 seller_name    = excluded.seller_name,
                 seller_type    = excluded.seller_type,
                 url            = excluded.url,
                 surface        = COALESCE(excluded.surface, listings.surface),
                 housing_type   = COALESCE(NULLIF(excluded.housing_type, ''), listings.housing_type),
                 roommate_count = COALESCE(excluded.roommate_count, listings.roommate_count),
                 published_at   = COALESCE(excluded.published_at, listings.published_at),
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
             datetime.utcnow().isoformat(), surface, housing_type or "", roommate_count, published_at),
        )
        return cur.fetchone()[0]


def upsert_listings_batch(rows: list) -> int:
    """Bulk upsert. `rows` is a list of dicts with the same keys as
    upsert_listing kwargs. Wraps N upserts in a single transaction —
    ~50× faster than calling upsert_listing in a loop for large batches.
    Skips mock listings silently. Returns number of rows persisted.
    """
    if not rows:
        return 0
    payload = []
    now = datetime.utcnow().isoformat()
    for r in rows:
        if _is_mock_listing(r["lbc_id"], r.get("url", "")):
            continue
        payload.append((
            r["lbc_id"], r.get("source", "leboncoin"), r.get("title", ""),
            r.get("price"), r.get("location", ""), r.get("seller_name", ""),
            r.get("seller_type", ""), r.get("url", ""), now,
            r.get("surface"), r.get("housing_type") or "", r.get("roommate_count"),
            r.get("published_at"), r.get("phone"),
            (r.get("description") or "")[:1000],  # truncate to avoid DB bloat
            r.get("available_from"),
        ))
    if not payload:
        return 0
    with _conn() as conn:
        conn.executemany(
            """INSERT INTO listings
               (lbc_id, source, title, price, location, seller_name, seller_type, url, scraped_at, surface, housing_type, roommate_count, published_at, phone, description, available_from)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(lbc_id) DO UPDATE SET
                 source         = excluded.source,
                 title          = excluded.title,
                 location       = excluded.location,
                 seller_name    = excluded.seller_name,
                 seller_type    = excluded.seller_type,
                 url            = excluded.url,
                 surface        = COALESCE(excluded.surface, listings.surface),
                 housing_type   = COALESCE(NULLIF(excluded.housing_type, ''), listings.housing_type),
                 roommate_count = COALESCE(excluded.roommate_count, listings.roommate_count),
                 published_at   = COALESCE(excluded.published_at, listings.published_at),
                 phone          = COALESCE(excluded.phone, listings.phone),
                 description    = COALESCE(NULLIF(excluded.description, ''), listings.description),
                 -- Clear the LLM-attempted flag when a NEW description arrives
                 -- (description was empty or different before). Lets /score_all
                 -- retry availability extraction with the freshly-enriched text.
                 availability_extracted = CASE
                     WHEN excluded.description IS NOT NULL
                          AND excluded.description != ''
                          AND COALESCE(listings.description, '') != excluded.description
                     THEN 0
                     ELSE listings.availability_extracted
                 END,
                 available_from = COALESCE(excluded.available_from, listings.available_from),
                 price_prev     = CASE
                     WHEN excluded.price IS NOT NULL
                          AND listings.price IS NOT NULL
                          AND excluded.price < listings.price
                     THEN listings.price
                     ELSE listings.price_prev
                 END,
                 price          = CASE
                     WHEN excluded.price IS NOT NULL
                          AND listings.price IS NOT NULL
                          AND excluded.price < listings.price
                     THEN excluded.price
                     ELSE listings.price
                 END""",
            payload,
        )
    return len(payload)


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


def set_listing_score(lbc_id: str, score: int, reason: str,
                      available_from: Optional[str] = None) -> None:
    """Record Claude's score (1–10), short reason, and optional availability date.

    Sets availability_extracted=1 unconditionally — once the LLM has been asked,
    we don't ask again until /campagne brings a fresh description (which clears
    the flag elsewhere, or the listing goes through the structured path).
    Prevents token waste loops on listings whose LLM truly returned null.

    COALESCE preserves any prior non-null available_from to avoid clobbering
    structured-source data with a later silent LLM pass.
    """
    with _conn() as conn:
        conn.execute(
            """UPDATE listings
               SET score = ?,
                   score_reason = ?,
                   available_from = COALESCE(?, available_from),
                   availability_extracted = 1
               WHERE lbc_id = ?""",
            (score, reason, available_from, lbc_id),
        )


def get_unscored_listings(limit: Optional[int] = None) -> list[dict]:
    """Return listings needing LLM extraction. Two cases:
    1. No score yet (`score IS NULL`).
    2. Scored, but available_from is NULL AND we never tried LLM extraction
       (`availability_extracted=0`) AND we have a description to extract from.

    Crucial: case (2) skips listings where the LLM was already asked and
    returned null. Without `availability_extracted` we'd re-burn tokens on
    every /score_all asking the same question with the same input.
    """
    sql = """SELECT lbc_id, source, title, price, location, url, surface,
                    housing_type, roommate_count, description
             FROM listings
             WHERE score IS NULL
                OR (available_from IS NULL
                    AND availability_extracted = 0
                    AND description IS NOT NULL
                    AND description != '')
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


# ─── Fraud detection ──────────────────────────────────────────────────────────

import re as _re_fraud

# Markers commonly seen in immobilier scams (Dubaï, Western Union, "frais de
# dossier" demanded before visit, fake heritage stories, off-platform shift to
# WhatsApp). Compiled once at module load — case-insensitive, accent-tolerant.
# We match on raw text after lowercasing; word-boundary kept loose because
# scammers often inject spaces/punctuation ("West-ern  Union").
_SCAM_MARKERS = (
    ("whatsapp",            r"whats?\s*app"),
    ("dubai",               r"duba[iï]"),
    ("etranger",            r"(?:à|a)\s+l[''’]?\s*(?:é|e)tranger"),
    ("frais_dossier",       r"frais\s+de\s+dossier"),
    ("western_union",       r"western\s*union"),
    ("paypal_friends",      r"paypal\s+friends?"),
    ("voyage_affaires",     r"voyage\s+d[''’]?\s*affaires?"),
    ("heritage",            r"h[ée]ritage"),
)
_SCAM_PATTERNS = [(label, _re_fraud.compile(rx, _re_fraud.IGNORECASE))
                  for label, rx in _SCAM_MARKERS]

# "Payment before visit" — fraudsters demand a deposit / send keys / sign
# before any in-person visit. Captured separately for a clearer reason string.
_PAYMENT_BEFORE_VISIT = _re_fraud.compile(
    r"(?:"
    r"caution\s+avant\s+visite"
    r"|paiement\s+avant\s+visite"
    r"|verser\s+(?:la\s+)?caution\s+avant"
    r"|envoyer\s+(?:l[''’]?\s*argent|les\s+fonds)\s+avant"
    r"|acompte\s+avant\s+(?:la\s+)?visite"
    r"|(?:les\s+)?cl[ée]s\s+(?:par\s+|en\s+)?(?:la\s+)?poste"
    r"|virement\s+avant\s+(?:la\s+)?visite"
    r")",
    _re_fraud.IGNORECASE,
)

# Threshold: a listing is flagged as suspiciously cheap when its price is
# at least 50 % below the median €/m² of comparable listings (same broad
# zone + similar surface ±25 %). Need ≥5 comps to trust the median — under
# that, sample is too small and we silently skip the price check.
_PRICE_ANOMALY_RATIO = 0.50  # 50 % under median = flag
_PRICE_ANOMALY_MIN_COMPS = 5
_SURFACE_TOLERANCE = 0.25    # ±25 % of the listing's surface


def _zone_key(location: str) -> str:
    """Best-effort zone fingerprint for median calc.

    Uses the 5-digit ZIP if present (most precise), else the first 8 chars
    of the city name lowercased. Mirrors the dedup key used in dashboard.py
    so the comparison cohort matches the user's mental model of "same area".
    """
    if not location:
        return ""
    m = _re_fraud.search(r"\b(\d{5})\b", location)
    if m:
        return m.group(1)
    return location.strip()[:8].lower()


def is_suspicious_listing(listing) -> tuple[bool, str]:
    """Return (is_fraud, reason) for a listing.

    Accepts a dict or sqlite3.Row. Checks, in order:
      1. Scam-marker keywords in title/description (WhatsApp, Dubaï,
         à l'étranger, Western Union, etc.).
      2. "Payment-before-visit" phrasing (caution/virement avant visite,
         clés par la poste...).
      3. Price anomalously low: ≥ 50 % under the median €/m² of comparable
         listings in the same zone (5-digit ZIP) and surface band (±25 %).
         Requires ≥ 5 comparables; otherwise skipped silently.

    Returns (False, "") for clean listings. The reason string is always
    short and user-facing — used directly in the dashboard badge tooltip.
    """
    if listing is None:
        return (False, "")
    # Normalise input — sqlite3.Row + dict both work via __getitem__, but Row
    # raises IndexError on missing keys (no .get()). Wrap once.
    def _g(key, default=""):
        try:
            v = listing[key]
        except (KeyError, IndexError, TypeError):
            v = default
        return v if v is not None else default

    title = str(_g("title", "") or "")
    description = str(_g("description", "") or "")
    blob = f"{title}\n{description}"

    # 1. Scam markers
    hits: list[str] = []
    for label, rx in _SCAM_PATTERNS:
        if rx.search(blob):
            # Friendly French label for the badge tooltip
            hits.append({
                "whatsapp":        "WhatsApp",
                "dubai":           "Dubaï",
                "etranger":        "à l'étranger",
                "frais_dossier":   "frais de dossier",
                "western_union":   "Western Union",
                "paypal_friends":  "PayPal Friends",
                "voyage_affaires": "voyage d'affaires",
                "heritage":        "héritage",
            }[label])
    if hits:
        # Only show first 2 markers in reason — avoid 80-char tooltip
        joined = " + ".join(hits[:2])
        return (True, f"Mots-clés arnaque : {joined}")

    # 2. Payment-before-visit
    if _PAYMENT_BEFORE_VISIT.search(blob):
        return (True, "Paiement exigé avant visite")

    # 3. Price anomaly vs. zone+surface median
    price = _g("price", None)
    surface = _g("surface", None)
    location = _g("location", "")
    if price and surface and location:
        try:
            price_i = int(price)
            surf_i = int(surface)
        except (TypeError, ValueError):
            return (False, "")
        if price_i > 0 and surf_i > 0:
            zk = _zone_key(location)
            if zk:
                surf_low = max(1, int(surf_i * (1 - _SURFACE_TOLERANCE)))
                surf_high = int(surf_i * (1 + _SURFACE_TOLERANCE)) + 1
                # Median €/m² for the cohort. We compute via SQL window with
                # a CTE — sqlite has had window functions since 3.25 (2018),
                # safe to assume on any modern Python.
                with _conn() as conn:
                    rows = conn.execute(
                        """SELECT price * 1.0 / surface AS ppm
                           FROM listings
                           WHERE price IS NOT NULL AND price > 0
                             AND surface IS NOT NULL AND surface > 0
                             AND surface BETWEEN ? AND ?
                             AND (
                                 location LIKE ?
                              OR substr(lower(location), 1, 8) = ?
                             )""",
                        (surf_low, surf_high, f"%{zk}%", zk),
                    ).fetchall()
                ppms = sorted(r["ppm"] for r in rows if r["ppm"])
                # Drop the listing's own row if present (avoid biasing median
                # toward the very listing we're judging).
                # NB: sqlite returns floats; equality-on-floats is fragile,
                # but worst case we don't drop — tolerable since N≥5.
                own = price_i / surf_i if surf_i else None
                if own is not None and own in ppms:
                    ppms.remove(own)
                if len(ppms) >= _PRICE_ANOMALY_MIN_COMPS:
                    mid = ppms[len(ppms) // 2]
                    if mid > 0:
                        ratio = (price_i / surf_i) / mid
                        if ratio <= (1 - _PRICE_ANOMALY_RATIO):
                            pct = int(round((1 - ratio) * 100))
                            return (True, f"Prix {pct}% sous le marché")

    return (False, "")


# ─── Activity check (cross-source) ────────────────────────────────────────────

def mark_seen(source: str, lbc_ids: list) -> int:
    """Stamp `seen_at = now` for every listing in (source, lbc_ids).

    Called right after `_persist_batch` finishes for a given source. Listings
    of `source` that are NOT in `lbc_ids` keep their old `seen_at`, so they
    age out and are caught by `mark_stale_listings` once they cross 24h.
    Returns rows updated.
    """
    if not lbc_ids:
        return 0
    now = datetime.utcnow().isoformat()
    placeholders = ",".join("?" * len(lbc_ids))
    with _conn() as conn:
        cur = conn.execute(
            f"UPDATE listings SET seen_at = ? "
            f"WHERE source = ? AND lbc_id IN ({placeholders})",
            (now, source, *lbc_ids),
        )
        return cur.rowcount


def mark_stale_listings(hours: int = 24) -> int:
    """Soft-delete listings that haven't been seen in any /campagne for `hours`.

    Sets `score = 0`, `score_reason = "❌ disparu de la source"` so the
    dashboard hides them automatically (it filters out score=0). We only
    touch rows with `score > 0` to avoid re-flagging dealbreakers (which
    already have score=0 with a more informative reason) and we skip rows
    where `seen_at IS NULL` (legacy: never seen in a post-migration scrape,
    so we can't tell yet — they'll be stamped on the next /campagne).

    Idempotent: if seen_at hasn't moved since last check, the row is already
    score=0 and the WHERE clause filters it out. Returns rows hidden.
    """
    cutoff = f"-{int(hours)} hours"
    with _conn() as conn:
        cur = conn.execute(
            """UPDATE listings
                  SET score = 0,
                      score_reason = '❌ disparu de la source'
                WHERE seen_at IS NOT NULL
                  AND seen_at < datetime('now', ?)
                  AND (score IS NULL OR score > 0)""",
            (cutoff,),
        )
        return cur.rowcount


# ─── Cross-source dedup ───────────────────────────────────────────────────────

def _normalise_zip_or_city(location: Optional[str]) -> str:
    """Same convention as dashboard.py used to fingerprint a listing.
    Prefers a 5-digit postal code; otherwise falls back to first 8 chars
    of the city name lowercased — matches the previous dedup behaviour.
    """
    if not location:
        return ""
    import re as _re
    m = _re.search(r"\b(\d{5})\b", location)
    if m:
        return m.group(1)
    return location[:8].lower().strip()


def compute_dedup_key(price: Optional[int], surface: Optional[int],
                      location: Optional[str]) -> Optional[str]:
    """Coarse bucket key — `<zip>|<price//50>|<surface//2>`.

    Used as a SQL pre-filter to find candidate cross-source duplicates
    cheaply. The actual ±5% / ±2 / Levenshtein checks happen in Python on
    the small candidate set returned by the query.

    Returns None when we don't have enough info to fingerprint reliably.
    """
    if price is None or surface is None:
        return None
    z = _normalise_zip_or_city(location)
    if not z:
        return None
    return f"{z}|{int(price) // 50}|{int(surface) // 2}"


def _levenshtein(a: str, b: str) -> int:
    """Iterative Levenshtein — pure stdlib, O(len(a)*len(b)) time, O(len(b)) space.
    Good enough for short titles (< 200 chars) which is all we ever feed it.
    """
    if a == b:
        return 0
    if not a:
        return len(b)
    if not b:
        return len(a)
    prev = list(range(len(b) + 1))
    for i, ca in enumerate(a, 1):
        curr = [i]
        for j, cb in enumerate(b, 1):
            cost = 0 if ca == cb else 1
            curr.append(min(curr[-1] + 1, prev[j] + 1, prev[j - 1] + cost))
        prev = curr
    return prev[-1]


def title_similarity(a: str, b: str) -> float:
    """Normalised Levenshtein distance ∈ [0,1] — 0 = identical, 1 = totally different.
    Lowercases both sides; empty inputs return 1.0 (treated as "no signal").
    """
    a = (a or "").strip().lower()
    b = (b or "").strip().lower()
    if not a or not b:
        return 1.0
    n = max(len(a), len(b))
    return _levenshtein(a, b) / n


def find_dedup_primary(
    *,
    lbc_id: str,
    source: str,
    price: Optional[int],
    surface: Optional[int],
    location: Optional[str],
    title: Optional[str],
    price_tol_pct: float = 0.05,
    surface_tol: int = 2,
    title_tol: float = 0.30,
) -> Optional[str]:
    """Locate the earliest-seen cross-source primary that matches this listing.

    Match criteria (all must hold):
      • Different source (intra-source dups are handled by ON CONFLICT(lbc_id))
      • |price_a - price_b| / min(price_a, price_b) ≤ 5%
      • |surface_a - surface_b| ≤ 2 m²
      • Same zip-or-city slug
      • Levenshtein(title_a, title_b) / max(len) < 30%
      • Candidate is itself a primary (dedup_of IS NULL) — so we never chain.

    Returns the lbc_id of the primary, or None if no match. The candidate
    SQL is bucketed by `dedup_key` (±1 bucket on price and surface) so even
    on a 100k-row DB we only Levenshtein a handful of titles.
    """
    if price is None or surface is None or not title:
        return None
    z = _normalise_zip_or_city(location)
    if not z:
        return None
    p_bucket = int(price) // 50
    s_bucket = int(surface) // 2
    # Generate the (zip|p|s) keys we want to scan: ±1 bucket on each axis.
    candidate_keys = [
        f"{z}|{p}|{s}"
        for p in (p_bucket - 1, p_bucket, p_bucket + 1)
        for s in (s_bucket - 1, s_bucket, s_bucket + 1)
    ]
    placeholders = ",".join("?" * len(candidate_keys))
    price_tol_abs = max(int(round(price * price_tol_pct)), 5)  # ≥5€ floor
    with _conn() as conn:
        rows = conn.execute(
            f"""SELECT lbc_id, source, price, surface, title, scraped_at
                  FROM listings
                 WHERE dedup_key IN ({placeholders})
                   AND dedup_of IS NULL
                   AND lbc_id != ?
                   AND source != ?
                   AND price IS NOT NULL AND surface IS NOT NULL
                   AND ABS(price - ?) <= ?
                   AND ABS(surface - ?) <= ?
                 ORDER BY id ASC
                 LIMIT 20""",
            (*candidate_keys, lbc_id, source,
             price, price_tol_abs, surface, surface_tol),
        ).fetchall()
    for r in rows:
        # ±5% relative price tol (the SQL already used absolute bound, but the
        # spec is relative — re-check on the raw values).
        p1, p2 = price, r["price"]
        denom = min(p1, p2) or 1
        if abs(p1 - p2) / denom > price_tol_pct:
            continue
        if title_similarity(title, r["title"] or "") >= title_tol:
            continue
        return r["lbc_id"]
    return None


def apply_dedup_for_batch(rows: list) -> int:
    """Walk freshly-persisted rows and stamp `dedup_of`/`dedup_key` columns.

    `rows` is the same payload list passed to upsert_listings_batch (dicts
    with keys: lbc_id, source, price, surface, location, title). We do this
    AFTER the bulk upsert so the new rows' dedup_key is visible to peers
    inside the same batch (earliest-seen wins — and ties break by id ASC).

    Idempotent: re-running on the same rows is safe — `find_dedup_primary`
    skips the row's own lbc_id, and we never overwrite an existing
    dedup_of. Returns count of rows stamped as duplicates.
    """
    if not rows:
        return 0
    n_dups = 0
    # First pass: write dedup_key for every row (cheap, 1 UPDATE per row).
    # Second pass: detect duplicates against earlier primaries.
    with _conn() as conn:
        for r in rows:
            key = compute_dedup_key(r.get("price"), r.get("surface"),
                                    r.get("location"))
            if key is None:
                continue
            conn.execute(
                "UPDATE listings SET dedup_key = ? "
                "WHERE lbc_id = ? AND (dedup_key IS NULL OR dedup_key != ?)",
                (key, r["lbc_id"], key),
            )
    for r in rows:
        # Skip rows already flagged in a previous run.
        existing = get_listing_by_lbc_id(r["lbc_id"])
        if existing is None or existing["dedup_of"] is not None:
            continue
        primary = find_dedup_primary(
            lbc_id=r["lbc_id"],
            source=r.get("source", ""),
            price=r.get("price"),
            surface=r.get("surface"),
            location=r.get("location"),
            title=r.get("title"),
        )
        if primary:
            with _conn() as conn:
                conn.execute(
                    "UPDATE listings SET dedup_of = ? WHERE lbc_id = ? "
                    "AND dedup_of IS NULL",
                    (primary, r["lbc_id"]),
                )
            n_dups += 1
            logger.info("[dedup] %s flagged as duplicate of %s",
                        r["lbc_id"], primary)
    return n_dups


def is_duplicate(lbc_id: str) -> bool:
    """Return True if this listing was flagged as a cross-source duplicate.
    Used by the push-alert path to skip re-notifying about the same flat.
    """
    with _conn() as conn:
        row = conn.execute(
            "SELECT dedup_of FROM listings WHERE lbc_id = ?", (lbc_id,)
        ).fetchone()
    return bool(row and row["dedup_of"])


# ─── System metrics ───────────────────────────────────────────────────────────

def record_system_metrics(
    bot_ram_mb: float,
    children_ram_mb: float,
    cpu_percent: float,
    warm_contexts: int,
    children_count: int,
    retention_days: int = 7,
) -> None:
    """Persist a single (timestamped) snapshot and prune entries older than
    `retention_days`. Called from the bot's JobQueue every 5 minutes.
    """
    import time as _time
    now = int(_time.time())
    cutoff = now - retention_days * 86400
    total = bot_ram_mb + children_ram_mb
    with _conn() as conn:
        conn.execute(
            """INSERT INTO system_metrics
               (ts, bot_ram_mb, children_ram_mb, total_ram_mb,
                cpu_percent, warm_contexts, children_count)
               VALUES (?,?,?,?,?,?,?)""",
            (now, bot_ram_mb, children_ram_mb, total, cpu_percent,
             warm_contexts, children_count),
        )
        conn.execute("DELETE FROM system_metrics WHERE ts < ?", (cutoff,))


def get_system_metrics(hours: int = 24) -> list[dict]:
    """Return all snapshots from the last `hours` ordered oldest -> newest."""
    import time as _time
    cutoff = int(_time.time()) - hours * 3600
    with _conn() as conn:
        rows = conn.execute(
            """SELECT ts, bot_ram_mb, children_ram_mb, total_ram_mb,
                      cpu_percent, warm_contexts, children_count
               FROM system_metrics
               WHERE ts >= ?
               ORDER BY ts ASC""",
            (cutoff,),
        ).fetchall()
    return [dict(r) for r in rows]


# ─── Bot state (key/value) ───────────────────────────────────────────────────
# Survives restarts. Used to persist /autostart so it resumes on reboot.

def set_state(key: str, value: str) -> None:
    with _conn() as conn:
        conn.execute(
            "INSERT INTO bot_state (key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, value),
        )


def get_state(key: str, default: Optional[str] = None) -> Optional[str]:
    with _conn() as conn:
        row = conn.execute(
            "SELECT value FROM bot_state WHERE key = ?", (key,)
        ).fetchone()
    return row["value"] if row else default

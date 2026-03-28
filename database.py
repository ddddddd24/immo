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

def upsert_listing(
    lbc_id: str,
    title: str,
    price: Optional[int],
    location: str,
    seller_name: str,
    seller_type: str,
    url: str,
    source: str = "leboncoin",
) -> int:
    """Insert listing or return existing id. Tracks price changes. Returns row id."""
    with _conn() as conn:
        cur = conn.execute(
            "SELECT id, price FROM listings WHERE lbc_id = ?", (lbc_id,)
        )
        row = cur.fetchone()
        if row:
            # Track price drop: save old price if current price changed downward
            if price and row["price"] and price < row["price"]:
                conn.execute(
                    "UPDATE listings SET price_prev = ?, price = ? WHERE id = ?",
                    (row["price"], price, row["id"]),
                )
            return row["id"]
        cur = conn.execute(
            """INSERT INTO listings
               (lbc_id, source, title, price, location, seller_name, seller_type, url, scraped_at)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            (lbc_id, source, title, price, location, seller_name, seller_type, url,
             datetime.utcnow().isoformat()),
        )
        return cur.lastrowid


def already_contacted(lbc_id: str) -> bool:
    """Return True if we've already sent a message for this listing."""
    with _conn() as conn:
        cur = conn.execute(
            """SELECT c.id FROM contacts c
               JOIN listings l ON l.id = c.listing_id
               WHERE l.lbc_id = ? AND c.status != 'pending'
               LIMIT 1""",
            (lbc_id,),
        )
        return cur.fetchone() is not None


def get_listing_by_lbc_id(lbc_id: str) -> Optional[sqlite3.Row]:
    with _conn() as conn:
        return conn.execute(
            "SELECT * FROM listings WHERE lbc_id = ?", (lbc_id,)
        ).fetchone()


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


def mark_contact_status(contact_id: int, status: str) -> None:
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
            """SELECT l.title, l.url, l.price as new_price, l.price_prev as old_price
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

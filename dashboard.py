"""Simple local web dashboard — run with: python dashboard.py
Serves on http://localhost:5000
No extra dependencies beyond what's already installed (uses http.server).
"""
import json
import sqlite3
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import urlparse

import config

_DB = config.DB_PATH


def _query(sql: str, params=()) -> list[dict]:
    conn = sqlite3.connect(_DB)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(sql, params).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def _render_dashboard() -> str:
    contacts = _query("""
        SELECT c.id, c.status, c.sent_at, c.message_sent,
               l.title, l.price, l.location, l.url, l.source
        FROM contacts c
        JOIN listings l ON l.id = c.listing_id
        ORDER BY c.sent_at DESC
        LIMIT 100
    """)

    stats = _query("""
        SELECT
            COUNT(*) as total_scraped,
            (SELECT COUNT(*) FROM contacts WHERE status='sent') as total_sent,
            (SELECT COUNT(*) FROM contacts WHERE status='responded') as total_responded,
            (SELECT COUNT(*) FROM responses WHERE sentiment='positive') as total_positive,
            (SELECT COUNT(*) FROM visits WHERE done=0) as upcoming_visits
        FROM listings
    """)[0]

    visits = _query("SELECT * FROM visits WHERE done=0 ORDER BY created_at DESC")

    source_counts = _query("""
        SELECT source, COUNT(*) as n FROM listings GROUP BY source ORDER BY n DESC
    """)

    status_colors = {
        "sent": "#3b82f6",
        "pending": "#9ca3af",
        "responded": "#f59e0b",
        "positive": "#10b981",
        "negative": "#ef4444",
    }
    source_emoji = {"leboncoin": "🟠", "seloger": "🔵", "pap": "🟢"}

    rows_html = ""
    for c in contacts:
        color = status_colors.get(c["status"], "#9ca3af")
        src_emoji = source_emoji.get(c["source"], "⚪")
        msg_preview = (c["message_sent"] or "")[:120].replace("<", "&lt;").replace(">", "&gt;")
        rows_html += f"""
        <tr>
            <td>{src_emoji} <a href="{c['url']}" target="_blank">{(c['title'] or '')[:50]}</a></td>
            <td>{c['location'] or ''}</td>
            <td>{c['price'] or ''}€</td>
            <td><span class="badge" style="background:{color}">{c['status']}</span></td>
            <td>{(c['sent_at'] or '')[:16]}</td>
            <td class="msg-preview" title="{msg_preview}">{msg_preview[:60]}…</td>
        </tr>"""

    visits_html = ""
    for v in visits:
        visits_html += f"<li>📅 {v['date_str']} — <a href='{v['url']}' target='_blank'>{v['url'][:60]}</a></li>"
    if not visits_html:
        visits_html = "<li>Aucune visite planifiée</li>"

    sources_html = " | ".join(
        f"{source_emoji.get(s['source'], '⚪')} {s['source']}: <b>{s['n']}</b>"
        for s in source_counts
    )

    return f"""<!DOCTYPE html>
<html lang="fr">
<head>
<meta charset="UTF-8">
<meta http-equiv="refresh" content="60">
<title>🏠 Dashboard Immo</title>
<style>
  body {{ font-family: -apple-system, sans-serif; margin: 0; background: #0f172a; color: #e2e8f0; }}
  .header {{ background: #1e293b; padding: 20px 32px; border-bottom: 1px solid #334155; }}
  .header h1 {{ margin: 0; font-size: 1.5rem; }}
  .content {{ padding: 24px 32px; }}
  .stats {{ display: flex; gap: 16px; flex-wrap: wrap; margin-bottom: 24px; }}
  .stat-card {{ background: #1e293b; border-radius: 12px; padding: 16px 24px; min-width: 140px; }}
  .stat-card .label {{ font-size: 0.75rem; color: #94a3b8; text-transform: uppercase; letter-spacing: 0.05em; }}
  .stat-card .value {{ font-size: 2rem; font-weight: 700; margin-top: 4px; }}
  .section {{ background: #1e293b; border-radius: 12px; padding: 20px; margin-bottom: 24px; }}
  .section h2 {{ margin: 0 0 16px; font-size: 1rem; color: #94a3b8; text-transform: uppercase; letter-spacing: 0.05em; }}
  table {{ width: 100%; border-collapse: collapse; font-size: 0.875rem; }}
  th {{ text-align: left; padding: 8px 12px; color: #64748b; font-weight: 500; border-bottom: 1px solid #334155; }}
  td {{ padding: 8px 12px; border-bottom: 1px solid #1e293b; vertical-align: top; }}
  tr:hover td {{ background: #263348; }}
  a {{ color: #60a5fa; text-decoration: none; }}
  a:hover {{ text-decoration: underline; }}
  .badge {{ display: inline-block; padding: 2px 8px; border-radius: 9999px; font-size: 0.75rem; color: white; }}
  .msg-preview {{ color: #94a3b8; font-size: 0.8rem; cursor: help; }}
  ul {{ margin: 0; padding-left: 20px; }}
  li {{ margin: 6px 0; }}
  .sources {{ color: #94a3b8; font-size: 0.85rem; margin-bottom: 12px; }}
</style>
</head>
<body>
<div class="header">
  <h1>🏠 Dashboard Recherche Appartement — Illan</h1>
  <div style="color:#64748b;font-size:0.8rem;margin-top:4px">Mise à jour automatique toutes les 60s</div>
</div>
<div class="content">
  <div class="stats">
    <div class="stat-card"><div class="label">Annonces scrapées</div><div class="value">{stats['total_scraped']}</div></div>
    <div class="stat-card"><div class="label">Messages envoyés</div><div class="value" style="color:#3b82f6">{stats['total_sent']}</div></div>
    <div class="stat-card"><div class="label">Réponses reçues</div><div class="value" style="color:#f59e0b">{stats['total_responded']}</div></div>
    <div class="stat-card"><div class="label">Réponses positives</div><div class="value" style="color:#10b981">{stats['total_positive']}</div></div>
    <div class="stat-card"><div class="label">Visites à venir</div><div class="value" style="color:#a78bfa">{stats['upcoming_visits']}</div></div>
  </div>

  <div class="section">
    <h2>📅 Visites planifiées</h2>
    <ul>{visits_html}</ul>
  </div>

  <div class="section">
    <h2>📋 Derniers contacts</h2>
    <div class="sources">{sources_html}</div>
    <table>
      <thead><tr><th>Annonce</th><th>Ville</th><th>Prix</th><th>Statut</th><th>Envoyé le</th><th>Message</th></tr></thead>
      <tbody>{rows_html}</tbody>
    </table>
  </div>
</div>
</body>
</html>"""


class _Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):  # silence default access logs
        pass

    def do_GET(self):
        path = urlparse(self.path).path
        if path == "/" or path == "/dashboard":
            body = _render_dashboard().encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        elif path == "/api/stats":
            data = _query("""
                SELECT
                    (SELECT COUNT(*) FROM listings) as listings,
                    (SELECT COUNT(*) FROM contacts WHERE status='sent') as sent,
                    (SELECT COUNT(*) FROM responses WHERE sentiment='positive') as positive
            """)[0]
            body = json.dumps(data).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(body)
        else:
            self.send_response(404)
            self.end_headers()


def run(port: int = 5000):
    server = HTTPServer(("localhost", port), _Handler)
    print(f"Dashboard running at http://localhost:{port}")
    print("Press Ctrl+C to stop.")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    run()

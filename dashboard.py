"""Local web dashboard — run with: python dashboard.py
Serves on http://localhost:5000

Stdlib-only (http.server) — no Flask/FastAPI dep.

Two tabs:
  /              → Listings browser (all scraped, sortable, filterable)
  /contacts      → Contacts/messages tab (preserved from original)
"""
import json
import sqlite3
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import urlparse

import config

_DB = config.DB_PATH


SOURCE_EMOJI = {
    "leboncoin":     "🟠",
    "seloger":       "🔵",
    "pap":           "🟢",
    "bienici":       "🟣",
    "logicimmo":     "🟡",
    "studapart":     "🎓",
    "parisattitude": "🗼",
    "lodgis":        "🏛",
    "immojeune":     "🧑‍🎓",
    "locservice":    "🏠",
    "roomlala":      "🛏",
}
STATUS_COLOR = {
    "sent":      "#3b82f6",
    "pending":   "#9ca3af",
    "responded": "#f59e0b",
    "positive":  "#10b981",
    "negative":  "#ef4444",
}


def _query(sql: str, params=()) -> list[dict]:
    conn = sqlite3.connect(_DB)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(sql, params).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def _esc(s) -> str:
    if s is None:
        return ""
    return str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")


def _stats() -> dict:
    return _query("""
        SELECT
            (SELECT COUNT(*) FROM listings) as listings,
            (SELECT COUNT(*) FROM contacts WHERE status='pending') as pending,
            (SELECT COUNT(*) FROM contacts WHERE status='sent') as sent,
            (SELECT COUNT(*) FROM contacts WHERE status='responded') as responded,
            (SELECT COUNT(*) FROM responses WHERE sentiment='positive') as positive,
            (SELECT COUNT(*) FROM visits WHERE done=0) as visits
    """)[0]


# ─── /  — listings browser ────────────────────────────────────────────────────

def _render_listings() -> str:
    listings = _query("""
        SELECT l.lbc_id, l.source, l.title, l.price, l.surface, l.location,
               l.url, l.scraped_at, l.score, l.score_reason,
               (SELECT c.status FROM contacts c WHERE c.listing_id = l.id ORDER BY c.id DESC LIMIT 1) as status
        FROM listings l
        ORDER BY l.id DESC
        LIMIT 500
    """)
    s = _stats()
    sources = sorted({l["source"] for l in listings if l["source"]})

    # Build the rows once on the server; client-side JS handles sort/filter.
    row_data = []
    for l in listings:
        row_data.append({
            "id": l["lbc_id"],
            "source": l["source"] or "",
            "title": l["title"] or "",
            "price": l["price"],
            "surface": l["surface"],
            "location": l["location"] or "",
            "url": l["url"] or "",
            "score": l["score"],
            "score_reason": l["score_reason"] or "",
            "status": l["status"] or "",
            "scraped": (l["scraped_at"] or "")[:10],
        })

    rows_json = json.dumps(row_data)
    sources_options = "".join(f'<option value="{_esc(s)}">{_esc(s)}</option>' for s in sources)
    source_filter = ['<option value="">Toutes</option>'] + [
        f'<option value="{_esc(s)}">{SOURCE_EMOJI.get(s, "⚪")} {_esc(s)}</option>'
        for s in sources
    ]

    return f"""<!DOCTYPE html>
<html lang="fr">
<head>
<meta charset="UTF-8">
<meta http-equiv="refresh" content="120">
<title>🏠 Annonces — Dashboard Immo</title>
<style>
  * {{ box-sizing: border-box; }}
  body {{ font-family: -apple-system, BlinkMacSystemFont, sans-serif; margin: 0; background: #0f172a; color: #e2e8f0; }}
  .header {{ background: #1e293b; padding: 16px 32px; border-bottom: 1px solid #334155; display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 12px; }}
  .header h1 {{ margin: 0; font-size: 1.4rem; }}
  .nav {{ display: flex; gap: 8px; }}
  .nav a {{ padding: 6px 12px; background: #0f172a; border-radius: 8px; color: #94a3b8; text-decoration: none; font-size: 0.85rem; border: 1px solid #334155; }}
  .nav a:hover, .nav a.active {{ background: #2563eb; color: white; border-color: #2563eb; }}
  .content {{ padding: 20px 32px; }}
  .stats {{ display: flex; gap: 12px; flex-wrap: wrap; margin-bottom: 16px; }}
  .stat {{ background: #1e293b; border-radius: 10px; padding: 10px 16px; }}
  .stat .label {{ font-size: 0.7rem; color: #94a3b8; text-transform: uppercase; letter-spacing: 0.05em; }}
  .stat .value {{ font-size: 1.5rem; font-weight: 700; margin-top: 2px; }}
  .filters {{ background: #1e293b; border-radius: 10px; padding: 14px 18px; margin-bottom: 16px; display: flex; gap: 12px; flex-wrap: wrap; align-items: center; }}
  .filters label {{ font-size: 0.8rem; color: #94a3b8; }}
  .filters input, .filters select {{ background: #0f172a; color: #e2e8f0; border: 1px solid #334155; border-radius: 6px; padding: 5px 10px; font-size: 0.85rem; }}
  .filters input[type="number"] {{ width: 90px; }}
  .filters select {{ min-width: 160px; }}
  .filter-count {{ margin-left: auto; color: #94a3b8; font-size: 0.85rem; }}
  table {{ width: 100%; border-collapse: collapse; font-size: 0.85rem; background: #1e293b; border-radius: 10px; overflow: hidden; }}
  th {{ text-align: left; padding: 10px 12px; color: #cbd5e1; font-weight: 600; background: #0f172a; cursor: pointer; user-select: none; border-bottom: 2px solid #334155; }}
  th:hover {{ background: #1e293b; }}
  th .sort-arrow {{ display: inline-block; width: 10px; color: #475569; }}
  th.asc .sort-arrow::after {{ content: "▲"; color: #3b82f6; }}
  th.desc .sort-arrow::after {{ content: "▼"; color: #3b82f6; }}
  td {{ padding: 8px 12px; border-bottom: 1px solid #263348; vertical-align: top; }}
  tr:hover td {{ background: #263348; }}
  a {{ color: #60a5fa; text-decoration: none; }}
  a:hover {{ text-decoration: underline; }}
  .badge {{ display: inline-block; padding: 2px 8px; border-radius: 9999px; font-size: 0.7rem; color: white; }}
  .src {{ font-size: 0.75rem; color: #94a3b8; }}
  .score {{ display: inline-block; padding: 2px 6px; border-radius: 6px; background: #fbbf24; color: #1f2937; font-weight: 600; font-size: 0.75rem; }}
  .empty {{ padding: 40px; text-align: center; color: #64748b; }}
</style>
</head>
<body>
<div class="header">
  <h1>🏠 Annonces — {len(row_data)} en base</h1>
  <div class="nav">
    <a href="/" class="active">📋 Annonces</a>
    <a href="/contacts">✉️ Contacts</a>
  </div>
</div>
<div class="content">
  <div class="stats">
    <div class="stat"><div class="label">Total</div><div class="value">{s['listings']}</div></div>
    <div class="stat"><div class="label">En attente</div><div class="value" style="color:#9ca3af">{s['pending']}</div></div>
    <div class="stat"><div class="label">Envoyés</div><div class="value" style="color:#3b82f6">{s['sent']}</div></div>
    <div class="stat"><div class="label">Réponses</div><div class="value" style="color:#f59e0b">{s['responded']}</div></div>
    <div class="stat"><div class="label">Positives</div><div class="value" style="color:#10b981">{s['positive']}</div></div>
    <div class="stat"><div class="label">Visites</div><div class="value" style="color:#a78bfa">{s['visits']}</div></div>
  </div>

  <div class="filters">
    <label>Source <select id="f-source">{"".join(source_filter)}</select></label>
    <label>Prix max <input id="f-maxprice" type="number" placeholder="1000" /></label>
    <label>m² min <input id="f-minsurface" type="number" placeholder="20" /></label>
    <label>Recherche <input id="f-search" type="text" placeholder="titre / ville…" style="width:180px" /></label>
    <span class="filter-count" id="count">{len(row_data)} annonces</span>
  </div>

  <table id="tbl">
    <thead>
      <tr>
        <th data-sort="source">Source <span class="sort-arrow"></span></th>
        <th data-sort="title">Annonce <span class="sort-arrow"></span></th>
        <th data-sort="location">Ville <span class="sort-arrow"></span></th>
        <th data-sort="price" class="num">Prix <span class="sort-arrow"></span></th>
        <th data-sort="surface" class="num">m² <span class="sort-arrow"></span></th>
        <th data-sort="score" class="num">Score <span class="sort-arrow"></span></th>
        <th data-sort="status">Statut <span class="sort-arrow"></span></th>
        <th data-sort="scraped">Scrapé <span class="sort-arrow"></span></th>
      </tr>
    </thead>
    <tbody id="tbody"></tbody>
  </table>
</div>

<script>
  const ROWS = {rows_json};
  const SOURCE_EMOJI = {json.dumps(SOURCE_EMOJI)};
  const STATUS_COLOR = {json.dumps(STATUS_COLOR)};

  let sortKey = "scraped";
  let sortDir = -1;  // -1 desc, 1 asc

  function render() {{
    const src = document.getElementById('f-source').value;
    const maxPrice = parseInt(document.getElementById('f-maxprice').value) || null;
    const minSurface = parseInt(document.getElementById('f-minsurface').value) || null;
    const search = (document.getElementById('f-search').value || '').toLowerCase();

    let rows = ROWS.filter(r => {{
      if (src && r.source !== src) return false;
      if (maxPrice && (r.price === null || r.price > maxPrice)) return false;
      if (minSurface && (r.surface === null || r.surface < minSurface)) return false;
      if (search) {{
        const blob = (r.title + ' ' + r.location).toLowerCase();
        if (!blob.includes(search)) return false;
      }}
      return true;
    }});

    rows.sort((a, b) => {{
      const va = a[sortKey], vb = b[sortKey];
      if (va === null && vb === null) return 0;
      if (va === null) return 1;
      if (vb === null) return -1;
      if (typeof va === 'number') return (va - vb) * sortDir;
      return String(va).localeCompare(String(vb)) * sortDir;
    }});

    document.getElementById('count').textContent = rows.length + ' annonces';

    const escape = s => String(s || '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');

    const tbody = document.getElementById('tbody');
    tbody.innerHTML = rows.map(r => {{
      const emoji = SOURCE_EMOJI[r.source] || '⚪';
      const statusBadge = r.status
        ? `<span class="badge" style="background:${{STATUS_COLOR[r.status] || '#475569'}}">${{r.status}}</span>`
        : '<span style="color:#475569">—</span>';
      const score = r.score
        ? `<span class="score" title="${{escape(r.score_reason)}}">${{r.score}}/10</span>`
        : '<span style="color:#475569">—</span>';
      const surface = r.surface ? r.surface + 'm²' : '<span style="color:#475569">—</span>';
      const price = r.price ? r.price + '€' : '<span style="color:#475569">—</span>';
      return `<tr>
        <td><span class="src">${{emoji}} ${{escape(r.source)}}</span></td>
        <td><a href="${{escape(r.url)}}" target="_blank">${{escape(r.title.slice(0, 60))}}</a></td>
        <td>${{escape(r.location.slice(0, 30))}}</td>
        <td class="num"><b>${{price}}</b></td>
        <td class="num">${{surface}}</td>
        <td class="num">${{score}}</td>
        <td>${{statusBadge}}</td>
        <td style="color:#64748b;font-size:0.75rem">${{escape(r.scraped)}}</td>
      </tr>`;
    }}).join('') || '<tr><td colspan="8" class="empty">Aucune annonce ne matche les filtres.</td></tr>';

    document.querySelectorAll('th').forEach(th => {{
      th.classList.remove('asc', 'desc');
      if (th.dataset.sort === sortKey) th.classList.add(sortDir === 1 ? 'asc' : 'desc');
    }});
  }}

  document.querySelectorAll('th[data-sort]').forEach(th => {{
    th.addEventListener('click', () => {{
      const key = th.dataset.sort;
      if (sortKey === key) sortDir = -sortDir;
      else {{ sortKey = key; sortDir = (key === 'price' ? 1 : -1); }}
      render();
    }});
  }});
  ['f-source', 'f-maxprice', 'f-minsurface', 'f-search'].forEach(id => {{
    document.getElementById(id).addEventListener('input', render);
  }});

  render();
</script>
</body>
</html>"""


# ─── /contacts — preserved from original (messages tab) ──────────────────────

def _render_contacts() -> str:
    contacts = _query("""
        SELECT c.id, c.status, c.sent_at, c.message_sent,
               l.title, l.price, l.location, l.url, l.source
        FROM contacts c
        JOIN listings l ON l.id = c.listing_id
        ORDER BY c.id DESC
        LIMIT 200
    """)
    visits = _query("SELECT * FROM visits WHERE done=0 ORDER BY created_at DESC")
    s = _stats()

    rows_html = ""
    for c in contacts:
        color = STATUS_COLOR.get(c["status"], "#9ca3af")
        emoji = SOURCE_EMOJI.get(c["source"], "⚪")
        msg = _esc((c["message_sent"] or "")[:160])
        rows_html += f"""
        <tr>
            <td>{emoji} <a href="{_esc(c['url'])}" target="_blank">{_esc((c['title'] or '')[:50])}</a></td>
            <td>{_esc(c['location'] or '')}</td>
            <td>{c['price'] or ''}€</td>
            <td><span class="badge" style="background:{color}">{_esc(c['status'])}</span></td>
            <td>{_esc((c['sent_at'] or '')[:16])}</td>
            <td class="msg-preview" title="{msg}">{msg[:80]}…</td>
        </tr>"""
    if not contacts:
        rows_html = '<tr><td colspan="6" class="empty">Aucun contact préparé. Lance /campagne dans Telegram.</td></tr>'

    visits_html = ""
    for v in visits:
        visits_html += f"<li>📅 {_esc(v['date_str'])} — <a href='{_esc(v['url'])}' target='_blank'>{_esc(v['url'][:60])}</a></li>"
    if not visits_html:
        visits_html = "<li style='color:#64748b'>Aucune visite planifiée</li>"

    return f"""<!DOCTYPE html>
<html lang="fr">
<head>
<meta charset="UTF-8">
<meta http-equiv="refresh" content="60">
<title>✉️ Contacts — Dashboard Immo</title>
<style>
  body {{ font-family: -apple-system, sans-serif; margin: 0; background: #0f172a; color: #e2e8f0; }}
  .header {{ background: #1e293b; padding: 16px 32px; border-bottom: 1px solid #334155; display: flex; justify-content: space-between; align-items: center; }}
  .header h1 {{ margin: 0; font-size: 1.4rem; }}
  .nav {{ display: flex; gap: 8px; }}
  .nav a {{ padding: 6px 12px; background: #0f172a; border-radius: 8px; color: #94a3b8; text-decoration: none; font-size: 0.85rem; border: 1px solid #334155; }}
  .nav a.active {{ background: #2563eb; color: white; border-color: #2563eb; }}
  .content {{ padding: 20px 32px; }}
  .section {{ background: #1e293b; border-radius: 10px; padding: 20px; margin-bottom: 16px; }}
  .section h2 {{ margin: 0 0 12px; font-size: 0.9rem; color: #94a3b8; text-transform: uppercase; letter-spacing: 0.05em; }}
  table {{ width: 100%; border-collapse: collapse; font-size: 0.85rem; }}
  th {{ text-align: left; padding: 8px 12px; color: #94a3b8; font-weight: 500; border-bottom: 1px solid #334155; }}
  td {{ padding: 8px 12px; border-bottom: 1px solid #263348; }}
  tr:hover td {{ background: #263348; }}
  a {{ color: #60a5fa; text-decoration: none; }} a:hover {{ text-decoration: underline; }}
  .badge {{ display: inline-block; padding: 2px 8px; border-radius: 9999px; font-size: 0.7rem; color: white; }}
  .msg-preview {{ color: #94a3b8; font-size: 0.78rem; cursor: help; max-width: 360px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }}
  ul {{ margin: 0; padding-left: 20px; }} li {{ margin: 6px 0; }}
  .empty {{ padding: 30px; text-align: center; color: #64748b; }}
</style>
</head>
<body>
<div class="header">
  <h1>✉️ Contacts ({len(contacts)})</h1>
  <div class="nav">
    <a href="/">📋 Annonces</a>
    <a href="/contacts" class="active">✉️ Contacts</a>
  </div>
</div>
<div class="content">
  <div class="section">
    <h2>📅 Visites planifiées ({s['visits']})</h2>
    <ul>{visits_html}</ul>
  </div>
  <div class="section">
    <h2>Messages préparés / envoyés</h2>
    <table>
      <thead><tr><th>Annonce</th><th>Ville</th><th>Prix</th><th>Statut</th><th>Sent at</th><th>Message</th></tr></thead>
      <tbody>{rows_html}</tbody>
    </table>
  </div>
</div>
</body>
</html>"""


# ─── HTTP handler ────────────────────────────────────────────────────────────

class _Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        pass

    def do_GET(self):
        path = urlparse(self.path).path
        if path in ("/", "/dashboard"):
            self._send_html(_render_listings())
        elif path == "/contacts":
            self._send_html(_render_contacts())
        elif path == "/api/stats":
            self._send_json(_stats())
        elif path == "/api/listings":
            data = _query("""
                SELECT lbc_id, source, title, price, surface, location, url,
                       scraped_at, score
                FROM listings ORDER BY id DESC LIMIT 1000
            """)
            self._send_json(data)
        else:
            self.send_response(404)
            self.end_headers()

    def _send_html(self, body: str):
        b = body.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(b)))
        self.end_headers()
        self.wfile.write(b)

    def _send_json(self, data):
        b = json.dumps(data, default=str).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(b)))
        self.end_headers()
        self.wfile.write(b)


def run(port: int = 5000):
    server = HTTPServer(("localhost", port), _Handler)
    print(f"Dashboard running at http://localhost:{port}")
    print("  /          → annonces (filtres + tri)")
    print("  /contacts  → contacts + visites")
    print("Press Ctrl+C to stop.")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    run()

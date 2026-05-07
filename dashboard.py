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
    "leboncoin":         "🟠",
    "seloger":           "🔵",
    "pap":               "🟢",
    "bienici":           "🟣",
    "logicimmo":         "🟡",
    "studapart":         "🎓",
    "parisattitude":     "🗼",
    "lodgis":            "🏛",
    "immojeune":         "🧑‍🎓",
    "locservice":        "🏠",
    "roomlala":          "🛏",
    "entreparticuliers": "🤝",
    "ladresse":          "🏢",
    "century21":         "21",
    "wizi":              "🔑",
    "laforet":           "🌳",
    "guyhoquet":         "🎩",
    "inli":              "🏗",
    "gensdeconfiance":   "🤝",
    "cdc_habitat":       "🏛️",
    "fnaim":             "🏘",
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
    # Filtres adaptés au profil Illan + Iqleema (couple, max 1100€) :
    #  - prix ≤ 1100€
    #  - exclure coloc/coliving avec >2 occupants
    #  - exclure résidence étudiante / chambre / coliving (type)
    raw_listings = _query("""
        SELECT l.lbc_id, l.source, l.title, l.price, l.surface, l.location,
               l.url, l.scraped_at, l.published_at, l.phone, l.score, l.score_reason,
               l.housing_type, l.roommate_count, l.available_from, l.dedup_of
        FROM listings l
        WHERE l.price IS NOT NULL AND l.price <= 1100
          AND l.housing_type NOT IN ('coliving', 'chambre', 'residence')
          AND (
            l.housing_type != 'coloc'
            OR (l.roommate_count IS NOT NULL AND l.roommate_count <= 2)
          )
          -- Hide dealbreakers (score=0). NULL = not yet scored, keep visible.
          AND (l.score IS NULL OR l.score > 0)
          -- Cross-source dedup: only show primaries
          AND (l.dedup_of IS NULL OR l.dedup_of = '')
        ORDER BY l.id DESC
        LIMIT 10000
    """)

    # SQL dedup_of already handles cross-source dedup.
    listings = list(raw_listings)
    s = _stats()
    sources = sorted({l["source"] for l in listings if l["source"]})
    housing_types = sorted({l["housing_type"] for l in listings if l["housing_type"]})

    # Build the rows once on the server; client-side JS handles sort/filter.
    row_data = []
    for l in listings:
        ht = l["housing_type"] or ""
        if ht in ("coloc", "coliving") and l["roommate_count"]:
            ht_display = f"{ht} {l['roommate_count']}p"
        else:
            ht_display = ht
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
            "published": ((l["published_at"] or "")[7:17] if (l["published_at"] or "").startswith("scrape:") else (l["published_at"] or "")[:10]),
            "published_is_scrape": (l["published_at"] or "").startswith("scrape:"),
            "phone": l["phone"],
            "available_from": l["available_from"] or "",
            "housing_type": ht,
            "housing_display": ht_display,
        })

    rows_json = json.dumps(row_data)
    source_filter = ['<option value="">Toutes</option>'] + [
        f'<option value="{_esc(s)}">{SOURCE_EMOJI.get(s, "⚪")} {_esc(s)}</option>'
        for s in sources
    ]
    type_filter = ['<option value="">Tous types</option>'] + [
        f'<option value="{_esc(t)}">{_esc(t)}</option>' for t in housing_types
    ]

    return f"""<!DOCTYPE html>
<html lang="fr">
<head>
<meta charset="UTF-8">
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
    <a href="index.html" class="active">📋 Annonces</a>
    <a href="contacts.html">✉️ Contacts</a>
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
    <label>Type <select id="f-type">{"".join(type_filter)}</select></label>
    <label>Prix max <input id="f-maxprice" type="number" placeholder="1000" /></label>
    <label>m² min <input id="f-minsurface" type="number" placeholder="20" /></label>
    <label>Recherche <input id="f-search" type="text" placeholder="titre / ville…" style="width:180px" /></label>
    <label><input id="f-phone-only" type="checkbox" /> 📞 Avec tél seulement</label>
    <span class="filter-count" id="count">{len(row_data)} annonces</span>
  </div>

  <table id="tbl">
    <thead>
      <tr>
        <th data-sort="source">Source <span class="sort-arrow"></span></th>
        <th data-sort="title">Annonce <span class="sort-arrow"></span></th>
        <th data-sort="housing_type">Type <span class="sort-arrow"></span></th>
        <th data-sort="location">Ville <span class="sort-arrow"></span></th>
        <th data-sort="price" class="num">Prix <span class="sort-arrow"></span></th>
        <th data-sort="surface" class="num">m² <span class="sort-arrow"></span></th>
        <th data-sort="score" class="num">Score <span class="sort-arrow"></span></th>
        <th data-sort="published">Publié <span class="sort-arrow"></span></th>
        <th data-sort="available_from">🗝 Libre <span class="sort-arrow"></span></th>
        <th data-sort="phone">📞 <span class="sort-arrow"></span></th>
      </tr>
    </thead>
    <tbody id="tbody"></tbody>
  </table>
</div>

<script>
  const ROWS = {rows_json};
  const SOURCE_EMOJI = {json.dumps(SOURCE_EMOJI)};
  const OFF_RADAR_SOURCES = new Set(['studapart','parisattitude','lodgis','immojeune','locservice','entreparticuliers','ladresse','century21','wizi','laforet','guyhoquet','kley','inli','icf','actionlogement','gensdeconfiance','cdc_habitat','fnaim']);

  let sortKey = "published";
  let sortDir = -1;

  function render() {{
    const src = document.getElementById('f-source').value;
    const type = document.getElementById('f-type').value;
    const maxPrice = parseInt(document.getElementById('f-maxprice').value) || null;
    const minSurface = parseInt(document.getElementById('f-minsurface').value) || null;
    const search = (document.getElementById('f-search').value || '').toLowerCase();
    const phoneOnly = document.getElementById('f-phone-only').checked;

    let rows = ROWS.filter(r => {{
      if (src && r.source !== src) return false;
      if (type && r.housing_type !== type) return false;
      if (maxPrice && (r.price === null || r.price > maxPrice)) return false;
      if (minSurface && (r.surface === null || r.surface < minSurface)) return false;
      if (phoneOnly) {{
        if (!r.phone || r.phone === '#blocked') return false;
      }}
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
    // YYYY-MM-DD → DD/MM/YYYY ; YYYY-MM → MM/YYYY
    const formatAvail = s => {{
      if (!s) return '';
      const m1 = /^(\d{{4}})-(\d{{2}})-(\d{{2}})$/.exec(s);
      if (m1) return `${{m1[3]}}/${{m1[2]}}/${{m1[1]}}`;
      const m2 = /^(\d{{4}})-(\d{{2}})$/.exec(s);
      if (m2) return `${{m2[2]}}/${{m2[1]}}`;
      return s;
    }};

    // Score tooltip parser: "PV=X Z=Y(zone) C=Z(min) F=W — note" → labeled multiline
    const buildScoreTooltip = (reason, finalScore) => {{
      if (!reason) return '';
      const m = /PV=([\d.]+)\s+Z=([\d.]+)(?:\(([^)]+)\))?\s+C=([\d.]+)(?:\(([^)]+)\))?\s+F=([\d.]+)(?:\s+—\s+(.+))?/.exec(reason);
      if (!m) return reason;
      const [_, pv, zs, zone, cs, commute, fs, note] = m;
      const lines = [
        `📊 Détail score ${{finalScore}}/10:`, '',
        `💰 Prix/Valeur (35%): ${{pv}}/10`,
        `📍 Zone (30%): ${{zs}}/10` + (zone ? ` — ${{zone}}` : ''),
        `🚇 Trajet (25%): ${{cs}}/10` + (commute ? ` — ${{commute}}` : ''),
        `✨ Features (10%): ${{fs}}/10`,
      ];
      if (note) lines.push('', `📝 ${{note}}`);
      return lines.join(String.fromCharCode(10));
    }};

    const tbody = document.getElementById('tbody');
    tbody.innerHTML = rows.map(r => {{
      const emoji = SOURCE_EMOJI[r.source] || '⚪';
      const score = r.score
        ? `<span class="score" title="${{escape(buildScoreTooltip(r.score_reason, r.score))}}">${{r.score}}/10</span>`
        : '<span style="color:#475569">—</span>';
      const surface = r.surface ? r.surface + 'm²' : '<span style="color:#475569">—</span>';
      const price = r.price ? r.price + '€' : '<span style="color:#475569">—</span>';
      const typeBadge = r.housing_display
        ? `<span style="font-size:0.75rem;color:#cbd5e1;background:#334155;padding:2px 6px;border-radius:6px">${{escape(r.housing_display)}}</span>`
        : '<span style="color:#475569">—</span>';
      // 🔥 NEW: published_at < 6h ago
      const pubMs = r.published ? new Date(r.published.length > 7 ? r.published : r.published + '-01').getTime() : 0;
      const isNew = pubMs > Date.now() - 6 * 3600 * 1000;
      const newBadge = isNew ? '<span style="background:#ef4444;color:white;padding:1px 5px;border-radius:4px;font-size:0.7rem;font-weight:600;margin-right:4px">🔥 NEW</span>' : '';
      // 💎 OFF-RADAR: source not covered by Jinka
      const offRadarBadge = OFF_RADAR_SOURCES.has(r.source) ? '<span style="background:#8b5cf6;color:white;padding:1px 5px;border-radius:4px;font-size:0.7rem;font-weight:600;margin-right:4px" title="Source hors Jinka — moins de concurrence">💎</span>' : '';
      // tel: clickable phone (strip non-digits for href)
      const phoneCell = r.phone === '#blocked'
        ? '🚫'
        : (r.phone === '' || r.phone === null
            ? '⚪'
            : `<a href="tel:${{escape(r.phone.replace(/[^+\\d]/g, ''))}}" style="color:#22c55e;text-decoration:none;font-weight:600">📞 ${{escape(r.phone)}}</a>`);
      return `<tr>
        <td><span class="src">${{emoji}} ${{escape(r.source)}}</span></td>
        <td>${{newBadge}}${{offRadarBadge}}<a href="${{escape(r.url)}}" target="_blank">${{escape(r.title.slice(0, 60))}}</a></td>
        <td>${{typeBadge}}</td>
        <td>${{escape(r.location.slice(0, 30))}}</td>
        <td class="num"><b>${{price}}</b></td>
        <td class="num">${{surface}}</td>
        <td class="num">${{score}}</td>
        <td style="color:#64748b;font-size:0.75rem" title="${{r.published_is_scrape ? 'Date de scraping (estimation)' : 'Date de publication réelle'}}">${{r.published ? (r.published_is_scrape ? '⏱ ' : '📅 ') + escape(r.published) : ''}}</td>
        <td style="font-size:0.75rem" title="${{r.available_from ? 'Disponible à partir de ' + escape(formatAvail(r.available_from)) : 'Date de disponibilité non précisée'}}">${{r.available_from ? '🗝 ' + escape(formatAvail(r.available_from)) : '<span style=\"color:#475569\">—</span>'}}</td>
        <td title="${{r.phone === '#blocked' ? 'Site ne partage pas le téléphone' : (r.phone === '' || r.phone === null ? 'Pas de téléphone' : 'Tap pour appeler')}}">${{phoneCell}}</td>
      </tr>`;
    }}).join('') || '<tr><td colspan="10" class="empty">Aucune annonce ne matche les filtres.</td></tr>';

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
  ['f-source', 'f-type', 'f-maxprice', 'f-minsurface', 'f-search'].forEach(id => {{
    document.getElementById(id).addEventListener('input', render);
  }});
  document.getElementById('f-phone-only').addEventListener('change', render);

  render();
</script>
</body>
</html>"""


# ─── /contacts — preserved from original (messages tab) ──────────────────────


def _render_listings_mobile() -> str:
    # Filtres adaptés au profil Illan + Iqleema (couple, max 1100€) :
    #  - prix ≤ 1100€
    #  - exclure coloc/coliving avec >2 occupants
    #  - exclure résidence étudiante / chambre / coliving (type)
    # Cross-source dedup is applied at PERSIST time (see
    # database.apply_dedup_for_batch) — duplicates carry `dedup_of != NULL`
    # and are filtered out by the SQL clause below. Earliest-seen variant
    # wins so we don't churn the user's notif feed when SeLoger republishes
    # the same flat that LBC posted yesterday.
    raw_listings = _query("""
        SELECT l.lbc_id, l.source, l.title, l.price, l.surface, l.location,
               l.url, l.scraped_at, l.published_at, l.phone, l.score, l.score_reason,
               l.housing_type, l.roommate_count, l.available_from, l.description,
               (SELECT c.status FROM contacts c WHERE c.listing_id = l.id ORDER BY c.id DESC LIMIT 1) as status
        FROM listings l
        WHERE l.price IS NOT NULL AND l.price <= 1100
          AND l.housing_type NOT IN ('coliving', 'chambre', 'residence')
          AND (
            l.housing_type != 'coloc'
            OR (l.roommate_count IS NOT NULL AND l.roommate_count <= 2)
          )
          -- Hide dealbreakers (score=0). NULL = not yet scored, keep visible.
          AND (l.score IS NULL OR l.score > 0)
          -- Hide cross-source duplicates; the primary is shown instead.
          AND l.dedup_of IS NULL
        ORDER BY l.id DESC
        LIMIT 10000
    """)
    listings = list(raw_listings)
    s = _stats()
    sources = sorted({l["source"] for l in listings if l["source"]})
    housing_types = sorted({l["housing_type"] for l in listings if l["housing_type"]})

    # Build the rows once on the server; client-side JS handles sort/filter.
    # Fraud detection runs server-side: it needs DB access to compute the
    # zone+surface median for the price-anomaly check.
    import database as _db_fraud
    row_data = []
    for l in listings:
        ht = l["housing_type"] or ""
        if ht in ("coloc", "coliving") and l["roommate_count"]:
            ht_display = f"{ht} {l['roommate_count']}p"
        else:
            ht_display = ht
        try:
            is_fraud, fraud_reason = _db_fraud.is_suspicious_listing(l)
        except Exception:
            # Defensive: fraud detection must never crash the dashboard render.
            is_fraud, fraud_reason = (False, "")
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
            "published": ((l["published_at"] or "")[7:17] if (l["published_at"] or "").startswith("scrape:") else (l["published_at"] or "")[:10]),
            "published_is_scrape": (l["published_at"] or "").startswith("scrape:"),
            "phone": l["phone"],  # None=unknown, ""=no phone, "#blocked"=site policy, else number
            "available_from": l["available_from"] or "",  # YYYY-MM, "" if unknown
            "housing_type": ht,
            "housing_display": ht_display,
            "is_fraud": is_fraud,
            "fraud_reason": fraud_reason,
        })

    rows_json = json.dumps(row_data)
    source_options = ['<option value="">Toutes</option>'] + [
        f'<option value="{_esc(s)}">{SOURCE_EMOJI.get(s, "⚪")} {_esc(s)}</option>'
        for s in sources
    ]
    type_options = ['<option value="">Tous types</option>'] + [
        f'<option value="{_esc(t)}">{_esc(t)}</option>' for t in housing_types
    ]

    # Find latest scraped_at to compute "Dernière /campagne il y a X"
    last_scrape_row = _query("SELECT MAX(scraped_at) AS m FROM listings")
    last_scrape = (last_scrape_row[0]["m"] or "") if last_scrape_row else ""

    return f"""<!DOCTYPE html>
<html lang="fr">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover, user-scalable=no">
<meta name="theme-color" content="#0a0a0f">
<meta name="apple-mobile-web-app-capable" content="yes">
<meta name="apple-mobile-web-app-status-bar-style" content="black-translucent">
<link rel="manifest" href="manifest.json">
<link rel="apple-touch-icon" href="icon-192.png">
<title>🏠 Annonces — Immo</title>
<style>
  /* ─── RESET & TOKENS ──────────────────────────────────────────────── */
  *, *::before, *::after {{ box-sizing: border-box; -webkit-tap-highlight-color: transparent; }}
  :root {{
    --bg:        #0a0a0f;
    --bg-card:   #14141c;
    --bg-elev:   #1c1c26;
    --bg-input:  #0f0f17;
    --border:    #2a2a37;
    --text:      #f1f5f9;
    --text-dim:  #94a3b8;
    --text-mute: #64748b;
    --accent:    #22c55e;
    --accent-d:  #15803d;
    --danger:    #ef4444;
    --warn:      #f59e0b;
    --info:      #3b82f6;
    --purple:    #8b5cf6;
    --safe-top:    env(safe-area-inset-top, 0px);
    --safe-bot:    env(safe-area-inset-bottom, 0px);
  }}
  html, body {{
    margin: 0; padding: 0; background: var(--bg); color: var(--text);
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", sans-serif;
    font-size: 16px; line-height: 1.4; overscroll-behavior-y: contain;
    -webkit-font-smoothing: antialiased;
  }}
  body {{
    padding-top: var(--safe-top);
    padding-bottom: calc(72px + var(--safe-bot));  /* room for bottom nav */
    min-height: 100vh;
  }}
  a {{ color: inherit; text-decoration: none; }}
  button {{ font: inherit; color: inherit; background: none; border: none; cursor: pointer; }}

  /* ─── TOP STATUS BAR ─────────────────────────────────────────────── */
  .topbar {{
    position: sticky; top: 0; z-index: 50;
    background: rgba(10,10,15,0.95);
    backdrop-filter: saturate(180%) blur(12px);
    -webkit-backdrop-filter: saturate(180%) blur(12px);
    border-bottom: 1px solid var(--border);
    padding: 12px 16px 10px;
    padding-top: calc(12px + var(--safe-top));
  }}
  .topbar-row {{ display: flex; align-items: center; justify-content: space-between; gap: 12px; }}
  .topbar h1 {{ margin: 0; font-size: 18px; font-weight: 700; letter-spacing: -0.01em; }}
  .topbar-count {{ font-size: 13px; color: var(--text-dim); font-weight: 500; }}
  .status-banner {{
    margin-top: 8px; font-size: 12px; color: var(--text-dim);
    display: flex; align-items: center; gap: 6px;
  }}
  .status-banner .dot {{
    width: 8px; height: 8px; border-radius: 50%;
    background: var(--accent); box-shadow: 0 0 8px var(--accent);
  }}

  /* ─── PULL-TO-REFRESH ─────────────────────────────────────────────── */
  .ptr {{
    position: fixed; top: var(--safe-top); left: 0; right: 0; height: 60px;
    display: flex; align-items: center; justify-content: center;
    color: var(--accent); font-size: 13px; font-weight: 600;
    transform: translateY(-100%); transition: transform 0.2s ease;
    z-index: 40; pointer-events: none;
  }}
  .ptr.pulling {{ transform: translateY(0); }}
  .ptr-icon {{
    display: inline-block; margin-right: 8px;
    transition: transform 0.2s ease;
  }}
  .ptr.ready .ptr-icon {{ transform: rotate(180deg); }}
  .ptr.refreshing .ptr-icon {{ animation: spin 1s linear infinite; }}
  @keyframes spin {{ to {{ transform: rotate(360deg); }} }}

  /* ─── CONTROL BAR (sort + filter button) ──────────────────────────── */
  .controls {{
    padding: 12px 16px; display: flex; gap: 10px; align-items: center;
    background: var(--bg);
  }}
  .seg {{
    display: flex; flex: 1; background: var(--bg-elev); border-radius: 10px;
    padding: 3px; border: 1px solid var(--border);
  }}
  .seg button {{
    flex: 1; padding: 9px 4px; font-size: 13px; font-weight: 600;
    color: var(--text-dim); border-radius: 7px; min-height: 36px;
    transition: background 0.15s, color 0.15s;
  }}
  .seg button.active {{
    background: var(--bg-card); color: var(--text);
    box-shadow: 0 1px 2px rgba(0,0,0,0.3);
  }}
  .filter-btn {{
    flex-shrink: 0; padding: 9px 14px; min-height: 40px; min-width: 44px;
    background: var(--bg-elev); border: 1px solid var(--border);
    border-radius: 10px; font-size: 13px; font-weight: 600; color: var(--text);
    display: flex; align-items: center; gap: 6px;
  }}
  .filter-btn .badge-pill {{
    background: var(--accent); color: #052e13;
    font-size: 11px; font-weight: 700; padding: 1px 6px; border-radius: 9999px;
    min-width: 18px; text-align: center;
  }}

  /* ─── FEED ────────────────────────────────────────────────────────── */
  .feed {{
    padding: 4px 16px 16px; display: flex; flex-direction: column; gap: 12px;
  }}

  /* ─── CARD ────────────────────────────────────────────────────────── */
  .card {{
    background: var(--bg-card); border: 1px solid var(--border);
    border-radius: 16px; overflow: hidden;
    display: flex; flex-direction: column;
    transition: transform 0.1s ease;
  }}
  .card:active {{ transform: scale(0.995); }}
  .card-photo {{
    height: 160px; background: linear-gradient(135deg, #1e293b, #0f172a);
    display: flex; align-items: center; justify-content: center;
    font-size: 56px; position: relative; flex-shrink: 0;
  }}
  .card-badges {{
    position: absolute; top: 10px; left: 10px; right: 10px;
    display: flex; flex-wrap: wrap; gap: 6px;
  }}
  .badge {{
    display: inline-flex; align-items: center; gap: 4px;
    padding: 4px 9px; border-radius: 9999px;
    font-size: 11px; font-weight: 700; letter-spacing: 0.02em;
    backdrop-filter: blur(4px);
  }}
  .badge-new {{ background: rgba(239,68,68,0.92); color: white; }}
  .badge-radar {{ background: rgba(139,92,246,0.92); color: white; }}
  .badge-zone {{ background: rgba(20,20,28,0.85); color: var(--text); border: 1px solid rgba(148,163,184,0.3); }}
  /* SCAM SUSPECT — red, animated pulse, top of badge stack */
  .badge-scam {{
    background: rgba(220,38,38,0.95); color: white;
    border: 1px solid rgba(255,255,255,0.4);
    box-shadow: 0 0 0 0 rgba(239,68,68,0.55);
    animation: scam-pulse 1.6s infinite;
  }}
  @keyframes scam-pulse {{
    0%   {{ box-shadow: 0 0 0 0 rgba(239,68,68,0.55); }}
    70%  {{ box-shadow: 0 0 0 8px rgba(239,68,68,0); }}
    100% {{ box-shadow: 0 0 0 0 rgba(239,68,68,0); }}
  }}
  /* Best-time-to-call badge — sits next to the phone button */
  .call-time {{
    display: inline-flex; align-items: center; gap: 4px;
    padding: 4px 9px; border-radius: 9999px;
    font-size: 11px; font-weight: 700; letter-spacing: 0.02em;
    margin-bottom: 8px;
    border: 1px solid var(--border);
  }}
  .call-time[data-level="ok"]   {{ background: rgba(34,197,94,0.15);  color: #4ade80; border-color: rgba(34,197,94,0.4); }}
  .call-time[data-level="meh"]  {{ background: rgba(245,158,11,0.15); color: #fbbf24; border-color: rgba(245,158,11,0.4); }}
  .call-time[data-level="bad"]  {{ background: rgba(239,68,68,0.15);  color: #f87171; border-color: rgba(239,68,68,0.4); }}

  .card-body {{ padding: 14px 16px 12px; }}
  .card-source {{
    font-size: 11px; color: var(--text-dim); text-transform: uppercase;
    letter-spacing: 0.06em; font-weight: 600; margin-bottom: 4px;
    display: flex; align-items: center; gap: 6px;
  }}
  .card-title {{
    font-size: 15px; font-weight: 600; line-height: 1.3;
    margin: 0 0 10px; color: var(--text);
    display: -webkit-box; -webkit-line-clamp: 2; -webkit-box-orient: vertical;
    overflow: hidden;
  }}
  .card-stats {{
    display: flex; align-items: baseline; gap: 14px; margin-bottom: 10px;
    flex-wrap: wrap;
  }}
  .card-price {{
    font-size: 26px; font-weight: 800; color: var(--text); letter-spacing: -0.02em;
  }}
  .card-price .unit {{ font-size: 14px; font-weight: 600; color: var(--text-dim); }}
  .card-surface {{
    font-size: 16px; font-weight: 600; color: var(--text-dim);
  }}
  .card-type {{
    font-size: 11px; padding: 3px 8px; border-radius: 6px;
    background: var(--bg-elev); color: var(--text-dim);
    border: 1px solid var(--border); font-weight: 600;
  }}
  .card-meta {{
    display: flex; gap: 12px; font-size: 12px; color: var(--text-mute);
    margin-bottom: 12px; flex-wrap: wrap;
  }}
  .card-meta span {{ display: inline-flex; align-items: center; gap: 4px; }}

  /* Score chip — taps to expand */
  .score-chip {{
    display: inline-flex; align-items: center; gap: 6px;
    padding: 6px 11px; border-radius: 10px;
    background: linear-gradient(135deg, #f59e0b, #d97706);
    color: #1f1300; font-weight: 700; font-size: 13px;
    margin-bottom: 10px; min-height: 32px; cursor: pointer;
    border: none;
  }}
  .score-chip[data-grade="high"] {{ background: linear-gradient(135deg, #22c55e, #15803d); color: #052e13; }}
  .score-chip[data-grade="mid"]  {{ background: linear-gradient(135deg, #f59e0b, #d97706); color: #2a1a00; }}
  .score-chip[data-grade="low"]  {{ background: linear-gradient(135deg, #64748b, #475569); color: #f1f5f9; }}
  .score-chip .chev {{ font-size: 10px; opacity: 0.7; transition: transform 0.15s; }}
  .score-chip.open .chev {{ transform: rotate(180deg); }}
  .score-detail {{
    display: none; margin: 0 0 12px; padding: 12px;
    background: var(--bg-input); border: 1px solid var(--border);
    border-radius: 10px; font-size: 12px; color: var(--text-dim);
    font-family: ui-monospace, "SF Mono", Menlo, Consolas, monospace;
    white-space: pre-wrap; line-height: 1.6;
  }}
  .score-detail.open {{ display: block; }}

  /* Action buttons */
  .card-actions {{ display: flex; gap: 8px; }}
  .btn {{
    flex: 1; min-height: 48px; border-radius: 12px; font-size: 15px;
    font-weight: 700; display: flex; align-items: center; justify-content: center;
    gap: 8px; transition: opacity 0.15s, transform 0.1s;
  }}
  .btn:active {{ transform: scale(0.97); }}
  .btn-primary {{
    background: var(--accent); color: #042e13;
    box-shadow: 0 1px 0 rgba(255,255,255,0.08) inset, 0 4px 12px rgba(34,197,94,0.25);
  }}
  .btn-primary[data-disabled="true"] {{
    background: var(--bg-elev); color: var(--text-mute); box-shadow: none;
    pointer-events: none;
  }}
  .btn-secondary {{
    background: var(--bg-elev); color: var(--text);
    border: 1px solid var(--border);
  }}

  /* Empty / skeleton */
  .empty {{
    padding: 60px 20px; text-align: center; color: var(--text-mute);
    font-size: 14px;
  }}
  .skeleton-card {{
    background: var(--bg-card); border: 1px solid var(--border);
    border-radius: 16px; height: 320px;
    background: linear-gradient(110deg, var(--bg-card) 30%, var(--bg-elev) 50%, var(--bg-card) 70%);
    background-size: 200% 100%;
    animation: shimmer 1.4s linear infinite;
  }}
  @keyframes shimmer {{ to {{ background-position: -200% 0; }} }}

  /* ─── BOTTOM NAV ─────────────────────────────────────────────────── */
  .bottom-nav {{
    position: fixed; bottom: 0; left: 0; right: 0; z-index: 60;
    background: rgba(10,10,15,0.96);
    backdrop-filter: saturate(180%) blur(20px);
    -webkit-backdrop-filter: saturate(180%) blur(20px);
    border-top: 1px solid var(--border);
    padding: 6px 0 calc(6px + var(--safe-bot));
    display: grid; grid-template-columns: repeat(3, 1fr);
  }}
  .bottom-nav a {{
    display: flex; flex-direction: column; align-items: center; gap: 2px;
    padding: 8px 4px; min-height: 56px; color: var(--text-mute);
    font-size: 11px; font-weight: 600;
  }}
  .bottom-nav a.active {{ color: var(--accent); }}
  .bottom-nav .icon {{ font-size: 22px; line-height: 1; }}

  /* ─── BOTTOM SHEET (filters) ──────────────────────────────────────── */
  .scrim {{
    position: fixed; inset: 0; background: rgba(0,0,0,0.55);
    z-index: 70; opacity: 0; pointer-events: none;
    transition: opacity 0.2s ease;
  }}
  .scrim.open {{ opacity: 1; pointer-events: auto; }}
  .sheet {{
    position: fixed; left: 0; right: 0; bottom: 0; z-index: 80;
    background: var(--bg-card); border-radius: 20px 20px 0 0;
    border-top: 1px solid var(--border);
    padding: 8px 20px calc(20px + var(--safe-bot));
    transform: translateY(100%); transition: transform 0.25s cubic-bezier(0.32, 0.72, 0, 1);
    max-height: 85vh; overflow-y: auto;
  }}
  .sheet.open {{ transform: translateY(0); }}
  .sheet-handle {{
    width: 40px; height: 5px; background: var(--border); border-radius: 9999px;
    margin: 0 auto 14px; cursor: grab;
  }}
  .sheet h2 {{
    margin: 0 0 16px; font-size: 17px; font-weight: 700;
    display: flex; justify-content: space-between; align-items: center;
  }}
  .sheet h2 button {{
    font-size: 13px; font-weight: 600; color: var(--accent);
    padding: 8px 4px; min-height: 40px;
  }}
  .field {{ margin-bottom: 16px; }}
  .field label {{
    display: block; font-size: 12px; color: var(--text-dim);
    font-weight: 600; text-transform: uppercase; letter-spacing: 0.05em;
    margin-bottom: 6px;
  }}
  .field input, .field select {{
    width: 100%; min-height: 48px; font-size: 16px;
    background: var(--bg-input); color: var(--text);
    border: 1px solid var(--border); border-radius: 10px;
    padding: 10px 14px;
  }}
  .field input:focus, .field select:focus {{
    outline: none; border-color: var(--accent);
  }}
  .field-row {{ display: grid; grid-template-columns: 1fr 1fr; gap: 12px; }}
  .toggle-field {{
    display: flex; align-items: center; justify-content: space-between;
    padding: 12px 14px; background: var(--bg-input); border: 1px solid var(--border);
    border-radius: 10px; min-height: 52px;
  }}
  .toggle-field label {{ margin: 0; text-transform: none; letter-spacing: 0; font-size: 15px; color: var(--text); }}
  /* iOS-style switch */
  .switch {{ position: relative; width: 50px; height: 30px; flex-shrink: 0; }}
  .switch input {{ opacity: 0; width: 0; height: 0; }}
  .switch .slider {{
    position: absolute; inset: 0; background: var(--border);
    border-radius: 9999px; transition: background 0.2s;
  }}
  .switch .slider::before {{
    content: ""; position: absolute; left: 3px; top: 3px;
    width: 24px; height: 24px; background: white; border-radius: 50%;
    transition: transform 0.2s;
  }}
  .switch input:checked + .slider {{ background: var(--accent); }}
  .switch input:checked + .slider::before {{ transform: translateX(20px); }}

  .sheet-actions {{
    display: flex; gap: 10px; margin-top: 8px;
    position: sticky; bottom: 0; background: var(--bg-card); padding-top: 12px;
  }}
  .sheet-actions .btn {{ min-height: 50px; }}

  /* ─── DESKTOP FALLBACK (≥768px) ──────────────────────────────────── */
  @media (min-width: 768px) {{
    body {{ padding-bottom: 24px; }}
    .topbar {{ padding: 18px 32px 14px; }}
    .topbar h1 {{ font-size: 22px; }}
    .controls {{ padding: 16px 32px; max-width: 1200px; margin: 0 auto; }}
    .feed {{
      padding: 8px 32px 32px; max-width: 1200px; margin: 0 auto;
      display: grid; grid-template-columns: repeat(auto-fill, minmax(340px, 1fr));
      gap: 16px;
    }}
    .bottom-nav {{
      position: sticky; top: 0; bottom: auto;
      max-width: 1200px; margin: 0 auto;
      grid-template-columns: repeat(3, max-content); justify-content: flex-start;
      gap: 8px; background: transparent; backdrop-filter: none; border: none;
      padding: 0 32px; display: none;  /* desktop uses topbar nav instead */
    }}
  }}
</style>
</head>
<body>

<!-- Pull-to-refresh indicator -->
<div class="ptr" id="ptr">
  <span class="ptr-icon">↓</span>
  <span class="ptr-label">Tirer pour rafraîchir</span>
</div>

<header class="topbar">
  <div class="topbar-row">
    <h1>🏠 Annonces</h1>
    <div class="topbar-count" id="count">{len(row_data)} résultats</div>
  </div>
  <div class="status-banner" id="status-banner">
    <span class="dot"></span>
    <span id="status-text">Dernière /campagne : —</span>
  </div>
</header>

<!-- Sort + filter button -->
<div class="controls">
  <div class="seg" role="tablist" aria-label="Trier par">
    <button data-sort="published" class="active">📅 Publié</button>
    <button data-sort="price">💰 Prix</button>
    <button data-sort="score">⭐ Score</button>
  </div>
  <button class="filter-btn" id="open-filters" aria-label="Ouvrir les filtres">
    ⚙ Filtres <span class="badge-pill" id="active-filters" style="display:none">0</span>
  </button>
</div>

<!-- Feed -->
<main class="feed" id="feed"></main>

<!-- Bottom nav -->
<nav class="bottom-nav">
  <a href="index.html" class="active"><span class="icon">📋</span><span>Annonces</span></a>
  <a href="contacts.html"><span class="icon">✉️</span><span>Contacts</span></a>
  <a href="#" id="nav-stats"><span class="icon">📊</span><span>Stats</span></a>
</nav>

<!-- Bottom sheet: filters -->
<div class="scrim" id="scrim"></div>
<aside class="sheet" id="sheet" aria-modal="true" role="dialog" aria-label="Filtres">
  <div class="sheet-handle"></div>
  <h2>Filtres <button id="reset-filters" type="button">Réinitialiser</button></h2>

  <div class="field">
    <label for="f-source">Source</label>
    <select id="f-source">{"".join(source_options)}</select>
  </div>
  <div class="field">
    <label for="f-type">Type de logement</label>
    <select id="f-type">{"".join(type_options)}</select>
  </div>
  <div class="field-row">
    <div class="field">
      <label for="f-maxprice">Prix max (€)</label>
      <input id="f-maxprice" type="number" inputmode="numeric" pattern="[0-9]*" placeholder="1100" />
    </div>
    <div class="field">
      <label for="f-minsurface">m² min</label>
      <input id="f-minsurface" type="number" inputmode="numeric" pattern="[0-9]*" placeholder="20" />
    </div>
  </div>
  <div class="field">
    <label for="f-search">Recherche</label>
    <input id="f-search" type="search" placeholder="titre, ville, mot-clé…" />
  </div>
  <div class="field">
    <div class="toggle-field">
      <label for="f-phone-only">📞 Uniquement avec téléphone</label>
      <span class="switch"><input id="f-phone-only" type="checkbox" /><span class="slider"></span></span>
    </div>
  </div>

  <div class="sheet-actions">
    <button class="btn btn-secondary" id="close-filters" type="button">Annuler</button>
    <button class="btn btn-primary" id="apply-filters" type="button">Voir les annonces</button>
  </div>
</aside>

<!-- Stats sheet (lightweight) -->
<aside class="sheet" id="stats-sheet" aria-modal="true" role="dialog" aria-label="Statistiques">
  <div class="sheet-handle"></div>
  <h2>📊 Statistiques</h2>
  <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:14px">
    <div style="background:var(--bg-input);padding:14px;border-radius:12px;border:1px solid var(--border)">
      <div style="font-size:11px;color:var(--text-dim);text-transform:uppercase;letter-spacing:0.05em">Total</div>
      <div style="font-size:24px;font-weight:800;margin-top:4px">{s['listings']}</div>
    </div>
    <div style="background:var(--bg-input);padding:14px;border-radius:12px;border:1px solid var(--border)">
      <div style="font-size:11px;color:var(--text-dim);text-transform:uppercase;letter-spacing:0.05em">En attente</div>
      <div style="font-size:24px;font-weight:800;margin-top:4px;color:#9ca3af">{s['pending']}</div>
    </div>
    <div style="background:var(--bg-input);padding:14px;border-radius:12px;border:1px solid var(--border)">
      <div style="font-size:11px;color:var(--text-dim);text-transform:uppercase;letter-spacing:0.05em">Envoyés</div>
      <div style="font-size:24px;font-weight:800;margin-top:4px;color:#3b82f6">{s['sent']}</div>
    </div>
    <div style="background:var(--bg-input);padding:14px;border-radius:12px;border:1px solid var(--border)">
      <div style="font-size:11px;color:var(--text-dim);text-transform:uppercase;letter-spacing:0.05em">Réponses</div>
      <div style="font-size:24px;font-weight:800;margin-top:4px;color:#f59e0b">{s['responded']}</div>
    </div>
    <div style="background:var(--bg-input);padding:14px;border-radius:12px;border:1px solid var(--border)">
      <div style="font-size:11px;color:var(--text-dim);text-transform:uppercase;letter-spacing:0.05em">Positives</div>
      <div style="font-size:24px;font-weight:800;margin-top:4px;color:#10b981">{s['positive']}</div>
    </div>
    <div style="background:var(--bg-input);padding:14px;border-radius:12px;border:1px solid var(--border)">
      <div style="font-size:11px;color:var(--text-dim);text-transform:uppercase;letter-spacing:0.05em">Visites</div>
      <div style="font-size:24px;font-weight:800;margin-top:4px;color:#a78bfa">{s['visits']}</div>
    </div>
  </div>
  <div class="sheet-actions">
    <button class="btn btn-primary" id="close-stats" type="button" style="flex:1">Fermer</button>
  </div>
</aside>

<script>
  const ROWS = {rows_json};
  const SOURCE_EMOJI = {json.dumps(SOURCE_EMOJI)};
  const LAST_SCRAPE = {json.dumps(last_scrape)};
  const OFF_RADAR = new Set(['studapart','parisattitude','lodgis','immojeune','locservice','entreparticuliers','ladresse','century21','wizi','laforet','guyhoquet','kley','inli','icf','actionlogement','gensdeconfiance','cdc_habitat','fnaim']);

  // ─── State ────────────────────────────────────────────────────────
  const state = {{
    sortKey: 'published',
    sortDir: -1,  // -1 desc, +1 asc
    filters: {{ source: '', type: '', maxprice: '', minsurface: '', search: '', phoneOnly: false }},
  }};

  // ─── Helpers ──────────────────────────────────────────────────────
  const $ = id => document.getElementById(id);
  const escape = s => String(s == null ? '' : s)
    .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;').replace(/'/g, '&#39;');

  const formatAvail = s => {{
    if (!s) return '';
    let m = /^(\\d{{4}})-(\\d{{2}})-(\\d{{2}})$/.exec(s);
    if (m) return m[3] + '/' + m[2] + '/' + m[1];
    m = /^(\\d{{4}})-(\\d{{2}})$/.exec(s);
    if (m) return m[2] + '/' + m[1];
    return s;
  }};

  const timeAgo = iso => {{
    if (!iso) return '—';
    const t = new Date(iso.replace(' ', 'T')).getTime();
    if (isNaN(t)) return '—';
    const sec = Math.max(1, Math.floor((Date.now() - t) / 1000));
    if (sec < 60) return 'à l\\'instant';
    if (sec < 3600) return 'il y a ' + Math.floor(sec / 60) + ' min';
    if (sec < 86400) return 'il y a ' + Math.floor(sec / 3600) + 'h';
    return 'il y a ' + Math.floor(sec / 86400) + 'j';
  }};

  const scoreGrade = n => n >= 7 ? 'high' : n >= 5 ? 'mid' : 'low';

  // ─── Best time to call (per source-type) ─────────────────────────────
  // Pure JS, time-of-day aware, uses local timezone via Date getters.
  // Returns {{ level: 'ok'|'meh'|'bad', label, emoji }}.
  // Sources fall into 3 buckets:
  //   particuliers (LBC, PAP, LocService, ...) → 19h-21h sem, 10h-12h WE
  //   agences (FNAIM, Foncia, Century 21, ...) → 10h-12h, 14h-17h sem
  //   résidences (Studapart, ImmoJeune, Kley, ...) → 9h-12h, 14h-18h sem
  const CALL_BUCKETS = {{
    particulier: new Set(['leboncoin','pap','locservice','entreparticuliers',
                          'gensdeconfiance','bienici']),
    agence:      new Set(['fnaim','foncia','century21','laforet','guyhoquet',
                          'ladresse','seloger','logicimmo','wizi']),
    residence:   new Set(['studapart','immojeune','kley','inli','cdc_habitat',
                          'lodgis','parisattitude','icf','actionlogement']),
  }};

  function _bucketFor(source) {{
    const s = (source || '').toLowerCase();
    if (CALL_BUCKETS.particulier.has(s)) return 'particulier';
    if (CALL_BUCKETS.agence.has(s))      return 'agence';
    if (CALL_BUCKETS.residence.has(s))   return 'residence';
    return 'particulier';  // safest default — most permissive evening window
  }}

  // Window helper: returns 'ok' if hour ∈ [start, end), else 'no'.
  // (start, end are integers; we use a fractional hour: hour + minute/60.)
  function _inRange(h, start, end) {{ return h >= start && h < end; }}

  function bestTimeToCall(source, now) {{
    const d = (now == null) ? new Date() : new Date(now);
    const dow = d.getDay();           // 0 = Sunday, 6 = Saturday
    const isWeekend = (dow === 0 || dow === 6);
    const h = d.getHours() + d.getMinutes() / 60;
    const bucket = _bucketFor(source);

    // Each bucket defines OK windows + MEH (adjacent ±1h) windows.
    let ok = false, meh = false;
    if (bucket === 'particulier') {{
      if (isWeekend) {{
        ok  = _inRange(h, 10, 12);
        meh = _inRange(h, 9, 10) || _inRange(h, 12, 13);
      }} else {{
        ok  = _inRange(h, 19, 21);
        meh = _inRange(h, 18, 19) || _inRange(h, 21, 22);
      }}
    }} else if (bucket === 'agence') {{
      if (isWeekend) {{
        ok = false; meh = false;       // agences fermées le week-end
      }} else {{
        ok  = _inRange(h, 10, 12) || _inRange(h, 14, 17);
        meh = _inRange(h, 9, 10) || _inRange(h, 12, 14) || _inRange(h, 17, 18);
      }}
    }} else {{ // residence
      if (isWeekend) {{
        ok = false; meh = _inRange(h, 10, 12);
      }} else {{
        ok  = _inRange(h, 9, 12) || _inRange(h, 14, 18);
        meh = _inRange(h, 8, 9) || _inRange(h, 12, 14) || _inRange(h, 18, 19);
      }}
    }}

    if (ok)  return {{ level: 'ok',  label: 'Bon moment',         emoji: '🟢' }};
    if (meh) return {{ level: 'meh', label: 'Acceptable',         emoji: '🟡' }};
    return       {{ level: 'bad', label: h < 9 ? 'Trop tôt' : 'Trop tard', emoji: '🔴' }};
  }}

  const buildScoreDetail = (reason, score) => {{
    if (!reason) return 'Score: ' + score + '/10';
    const m = /PV=([\\d.]+)\\s+Z=([\\d.]+)(?:\\(([^)]+)\\))?\\s+C=([\\d.]+)(?:\\(([^)]+)\\))?\\s+F=([\\d.]+)(?:\\s+—\\s+(.+))?/.exec(reason);
    if (!m) return reason;
    const pv=m[1], zs=m[2], zone=m[3], cs=m[4], commute=m[5], fs=m[6], note=m[7];
    const lines = [
      '📊 Score ' + score + '/10',
      '',
      '💰 Prix/Valeur (35%) : ' + pv + '/10',
      '📍 Zone (30%)        : ' + zs + '/10' + (zone ? ' — ' + zone : ''),
      '🚇 Trajet (25%)      : ' + cs + '/10' + (commute ? ' — ' + commute : ''),
      '✨ Features (10%)    : ' + fs + '/10',
    ];
    if (note) {{ lines.push('', '📝 ' + note); }}
    return lines.join('\\n');
  }};

  // ─── Status banner ────────────────────────────────────────────────
  $('status-text').textContent = 'Dernière /campagne ' + timeAgo(LAST_SCRAPE);

  // ─── Render ───────────────────────────────────────────────────────
  function compare(a, b, key, dir) {{
    const va = a[key], vb = b[key];
    if (va == null && vb == null) return 0;
    if (va == null) return 1;
    if (vb == null) return -1;
    if (typeof va === 'number') return (va - vb) * dir;
    return String(va).localeCompare(String(vb)) * dir;
  }}

  function applyFilters(rows) {{
    const f = state.filters;
    return rows.filter(r => {{
      if (f.source && r.source !== f.source) return false;
      if (f.type && r.housing_type !== f.type) return false;
      const maxP = parseInt(f.maxprice) || null;
      if (maxP && (r.price == null || r.price > maxP)) return false;
      const minS = parseInt(f.minsurface) || null;
      if (minS && (r.surface == null || r.surface < minS)) return false;
      if (f.phoneOnly && (!r.phone || r.phone === '#blocked')) return false;
      if (f.search) {{
        const blob = (r.title + ' ' + r.location).toLowerCase();
        if (!blob.includes(f.search.toLowerCase())) return false;
      }}
      return true;
    }});
  }}

  function activeFilterCount() {{
    const f = state.filters;
    let n = 0;
    if (f.source) n++;
    if (f.type) n++;
    if (f.maxprice) n++;
    if (f.minsurface) n++;
    if (f.search) n++;
    if (f.phoneOnly) n++;
    return n;
  }}

  function cardHtml(r) {{
    const emoji = SOURCE_EMOJI[r.source] || '⚪';

    // Badges (NEW / OFF-RADAR / zone)
    const pubMs = r.published ? new Date(r.published.length > 7 ? r.published : r.published + '-01').getTime() : 0;
    const isNew = pubMs > Date.now() - 6 * 3600 * 1000;
    const isOffRadar = OFF_RADAR.has(r.source);
    const zoneText = (r.location || '').slice(0, 24);

    let badges = '';
    // SCAM SUSPECT first — it's the most important signal, must be visible
    // before the user even reads the title.
    if (r.is_fraud) {{
      const reason = escape(r.fraud_reason || 'Annonce suspecte');
      badges += '<span class="badge badge-scam" title="' + reason + '">⚠️ SCAM SUSPECT</span>';
    }}
    if (isNew) badges += '<span class="badge badge-new">🔥 NEW</span>';
    if (isOffRadar) badges += '<span class="badge badge-radar">💎 OFF-RADAR</span>';
    if (zoneText) badges += '<span class="badge badge-zone">📍 ' + escape(zoneText) + '</span>';

    // Score chip
    let scoreHtml = '';
    if (r.score) {{
      const grade = scoreGrade(r.score);
      const detail = buildScoreDetail(r.score_reason, r.score);
      scoreHtml = '<button type="button" class="score-chip" data-grade="' + grade + '" data-toggle="score">' +
                  '⭐ ' + r.score + '/10 <span class="chev">▼</span></button>' +
                  '<pre class="score-detail">' + escape(detail) + '</pre>';
    }}

    // Price + surface
    const priceStr = r.price ? r.price + '<span class="unit">€</span>' : '<span class="unit">— €</span>';
    const surfStr = r.surface ? r.surface + ' m²' : '— m²';
    const typeStr = r.housing_display ? '<span class="card-type">' + escape(r.housing_display) + '</span>' : '';

    // Meta line
    const pubLabel = r.published
      ? (r.published_is_scrape ? '⏱ ' : '📅 ') + escape(r.published)
      : '—';
    const availLabel = r.available_from ? '🗝 ' + escape(formatAvail(r.available_from)) : '';

    // Phone button + best-time-to-call badge
    let phoneBtn;
    let callTimeBadge = '';
    if (r.phone === '#blocked') {{
      phoneBtn = '<button class="btn btn-primary" data-disabled="true">🚫 Tél bloqué</button>';
    }} else if (!r.phone) {{
      phoneBtn = '<button class="btn btn-primary" data-disabled="true">📞 Pas de tél</button>';
    }} else {{
      const cleanTel = r.phone.replace(/[^+\\d]/g, '');
      phoneBtn = '<a class="btn btn-primary" href="tel:' + escape(cleanTel) + '">📞 ' + escape(r.phone) + '</a>';
      // Compute best-time-to-call only when we actually have a number — it
      // makes no sense to show "Bon moment" for a card with a disabled phone.
      const ct = bestTimeToCall(r.source);
      callTimeBadge = '<div class="call-time" data-level="' + ct.level + '">' +
                      ct.emoji + ' ' + escape(ct.label) + '</div>';
    }}

    return (
      '<article class="card">' +
        '<div class="card-photo">' +
          '<span>' + emoji + '</span>' +
          (badges ? '<div class="card-badges">' + badges + '</div>' : '') +
        '</div>' +
        '<div class="card-body">' +
          '<div class="card-source">' + emoji + ' ' + escape(r.source) + '</div>' +
          '<h3 class="card-title">' + escape((r.title || '').slice(0, 120)) + '</h3>' +
          '<div class="card-stats">' +
            '<span class="card-price">' + priceStr + '</span>' +
            '<span class="card-surface">' + surfStr + '</span>' +
            typeStr +
          '</div>' +
          (scoreHtml || '') +
          '<div class="card-meta">' +
            '<span>' + pubLabel + '</span>' +
            (availLabel ? '<span>' + availLabel + '</span>' : '') +
          '</div>' +
          callTimeBadge +
          '<div class="card-actions">' +
            phoneBtn +
            (r.url ? '<a class="btn btn-secondary" href="' + escape(r.url) + '" target="_blank" rel="noopener">Voir →</a>' : '') +
          '</div>' +
        '</div>' +
      '</article>'
    );
  }}

  function render() {{
    const filtered = applyFilters(ROWS).slice();
    filtered.sort((a, b) => compare(a, b, state.sortKey, state.sortDir));

    $('count').textContent = filtered.length + ' résultat' + (filtered.length === 1 ? '' : 's');

    const feed = $('feed');
    if (!filtered.length) {{
      feed.innerHTML = '<div class="empty">Aucune annonce ne correspond.<br><br>Essaye d\\'ajuster les filtres.</div>';
    }} else {{
      feed.innerHTML = filtered.map(cardHtml).join('');
    }}

    // Active filter pill
    const n = activeFilterCount();
    const pill = $('active-filters');
    if (n > 0) {{ pill.style.display = 'inline-block'; pill.textContent = n; }}
    else {{ pill.style.display = 'none'; }}

    // Sort segment active state
    document.querySelectorAll('.seg button').forEach(b => {{
      b.classList.toggle('active', b.dataset.sort === state.sortKey);
    }});
  }}

  // ─── Sort segment control ─────────────────────────────────────────
  document.querySelectorAll('.seg button').forEach(btn => {{
    btn.addEventListener('click', () => {{
      const k = btn.dataset.sort;
      if (state.sortKey === k) {{
        state.sortDir = -state.sortDir;
      }} else {{
        state.sortKey = k;
        state.sortDir = (k === 'price') ? 1 : -1;  // cheapest first / newest first / best first
      }}
      render();
    }});
  }});

  // ─── Score chip expansion (event delegation) ──────────────────────
  $('feed').addEventListener('click', e => {{
    const chip = e.target.closest('[data-toggle="score"]');
    if (!chip) return;
    e.preventDefault();
    chip.classList.toggle('open');
    const detail = chip.nextElementSibling;
    if (detail && detail.classList.contains('score-detail')) detail.classList.toggle('open');
  }});

  // ─── Filter sheet ─────────────────────────────────────────────────
  const sheet = $('sheet');
  const scrim = $('scrim');
  const statsSheet = $('stats-sheet');

  function openSheet(el) {{
    el.classList.add('open');
    scrim.classList.add('open');
    document.body.style.overflow = 'hidden';
  }}
  function closeAllSheets() {{
    sheet.classList.remove('open');
    statsSheet.classList.remove('open');
    scrim.classList.remove('open');
    document.body.style.overflow = '';
  }}

  $('open-filters').addEventListener('click', () => {{
    // Sync inputs with state
    $('f-source').value = state.filters.source;
    $('f-type').value = state.filters.type;
    $('f-maxprice').value = state.filters.maxprice;
    $('f-minsurface').value = state.filters.minsurface;
    $('f-search').value = state.filters.search;
    $('f-phone-only').checked = state.filters.phoneOnly;
    openSheet(sheet);
  }});
  $('close-filters').addEventListener('click', closeAllSheets);
  $('close-stats').addEventListener('click', closeAllSheets);
  scrim.addEventListener('click', closeAllSheets);

  $('apply-filters').addEventListener('click', () => {{
    state.filters.source = $('f-source').value;
    state.filters.type = $('f-type').value;
    state.filters.maxprice = $('f-maxprice').value;
    state.filters.minsurface = $('f-minsurface').value;
    state.filters.search = $('f-search').value;
    state.filters.phoneOnly = $('f-phone-only').checked;
    closeAllSheets();
    render();
  }});

  $('reset-filters').addEventListener('click', () => {{
    state.filters = {{ source: '', type: '', maxprice: '', minsurface: '', search: '', phoneOnly: false }};
    $('f-source').value = '';
    $('f-type').value = '';
    $('f-maxprice').value = '';
    $('f-minsurface').value = '';
    $('f-search').value = '';
    $('f-phone-only').checked = false;
    render();
  }});

  // Live search (no need to apply)
  $('f-search').addEventListener('input', e => {{
    state.filters.search = e.target.value;
    render();
  }});

  // ─── Stats nav ────────────────────────────────────────────────────
  $('nav-stats').addEventListener('click', e => {{
    e.preventDefault();
    openSheet(statsSheet);
  }});

  // ─── Pull-to-refresh ──────────────────────────────────────────────
  // Triggers a hard reload (server re-renders fresh data).
  // Only activates when the page is scrolled to the very top.
  (() => {{
    const ptr = $('ptr');
    const ptrLabel = ptr.querySelector('.ptr-label');
    let startY = null, currentY = 0, pulling = false;
    const THRESHOLD = 70;

    document.addEventListener('touchstart', e => {{
      if (window.scrollY > 0) return;
      if (sheet.classList.contains('open') || statsSheet.classList.contains('open')) return;
      startY = e.touches[0].clientY;
      pulling = false;
    }}, {{ passive: true }});

    document.addEventListener('touchmove', e => {{
      if (startY == null) return;
      currentY = e.touches[0].clientY - startY;
      if (currentY > 10 && window.scrollY <= 0) {{
        pulling = true;
        ptr.classList.add('pulling');
        if (currentY > THRESHOLD) {{
          ptr.classList.add('ready');
          ptrLabel.textContent = 'Relâcher pour rafraîchir';
        }} else {{
          ptr.classList.remove('ready');
          ptrLabel.textContent = 'Tirer pour rafraîchir';
        }}
      }}
    }}, {{ passive: true }});

    document.addEventListener('touchend', () => {{
      if (!pulling) {{ startY = null; return; }}
      if (currentY > THRESHOLD) {{
        ptr.classList.add('refreshing');
        ptrLabel.textContent = 'Rafraîchissement…';
        // Show skeletons while reloading
        $('feed').innerHTML = '<div class="skeleton-card"></div><div class="skeleton-card"></div><div class="skeleton-card"></div>';
        setTimeout(() => location.reload(), 350);
      }} else {{
        ptr.classList.remove('pulling', 'ready');
      }}
      startY = null; currentY = 0; pulling = false;
    }}, {{ passive: true }});
  }})();

  // ─── PWA service worker ───────────────────────────────────────────
  if ('serviceWorker' in navigator) {{
    window.addEventListener('load', () => {{
      navigator.serviceWorker.register('sw.js').catch(() => {{}});
    }});
  }}

  // ─── Initial render ───────────────────────────────────────────────
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
    <a href="index.html">📋 Annonces</a>
    <a href="contacts.html" class="active">✉️ Contacts</a>
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
        if path in ("/", "/dashboard", "/index.html"):
            self._send_html(_render_listings())
        elif path in ("/contacts", "/contacts.html"):
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

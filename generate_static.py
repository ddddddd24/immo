"""Génère une version HTML statique du dashboard pour hébergement
GitHub Pages (24/7 même PC éteint).

Usage:
    python generate_static.py
    → écrit ./public/index.html

Pour auto-update, lancer périodiquement (ex: toutes les 15 min via tâche
planifiée Windows ou cron) puis git add/commit/push.
"""
import os
import sys
from pathlib import Path

# Reuse the existing dashboard rendering logic — it already produces a
# self-contained HTML page (data + JS inline, all client-side filters).
import dashboard


def main() -> None:
    out_dir = Path("public")
    out_dir.mkdir(parents=True, exist_ok=True)
    html = dashboard._render_listings()
    (out_dir / "index.html").write_text(html, encoding="utf-8")

    # Also drop a contacts page (less critical, but useful)
    try:
        contacts_html = dashboard._render_contacts()
        (out_dir / "contacts.html").write_text(contacts_html, encoding="utf-8")
    except Exception:
        pass

    print(f"OK → public/index.html ({(out_dir / 'index.html').stat().st_size // 1024} KB)")


if __name__ == "__main__":
    main()

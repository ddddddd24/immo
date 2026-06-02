# LeBonCoin Bot — Build Plan

## Status: 🚧 Phase 2 in progress (2026-04-30)

Phase 1 (initial bot) shipped 2026-03-24. Phase 1.5 (12-bug fix pass) shipped 2026-04-30.

---

## 2026-06-02 — LBC + SeLoger morts (DataDome) — INVESTIGATION

**Verdict : IP résidentielle flaggée par DataDome, pas un problème de code/fingerprint.**

- [x] Sonde isolée `tools/datadome_probe/probe.py` — 8/8 variantes LBC + 3/3 SeLoger en 403, tous fingerprints. IP = 86.247.64.52 Orange AS3215.
- [x] Test warming cookie `tools/datadome_probe/warm_cookie.py` — Camoufox reçoit l'interstitielle, replay reste 403. Échec attendu et confirmé.
- [x] `scraper.py` NON modifié (aucun bypass technique gratuit ne marche).
- [x] lessons.md mis à jour (dead-ends + leviers réels).

### Review
- Cause racine = réputation IP (24/7 × 3 semaines + projet voisin "rendement").
- Aucun changement de code ne récupère ça. Leviers : (1) refresh IP Orange [gratuit, recommandé], (2) cooldown N jours [gratuit, lent, nécessite stopper "rendement"], (3) proxy résidentiel/mobile FR [payant, accord requis].
- ⚠️ Oracle Cloud (`oracle_retry.py`) inutile ici : ASN datacenter = bloqué d'emblée.
- Scripts de sonde conservés dans `tools/datadome_probe/` pour re-vérifier après refresh IP.

### Throttle préparé (s'applique au prochain redémarrage)
- [x] Sentinel LBC 60s±15s → **180s±30s** (~1440 → ~480 polls/jour). Tunable dans main.py (`_LBC_SENTINEL_INTERVAL_S`), défaut aligné dans scraper.py `_lbc_sentinel_loop`. Backups `*.bak-20260602`. py_compile OK.
- But : éviter de re-griller la nouvelle IP. Décision speed/sécurité : 180s garde une détection < 3 min ; remonter à 60s = re-flag probable.

### Reste à faire après changement d'IP (ce soir)
- [ ] Relancer `tools/datadome_probe/probe.py` → confirmer nouvelle IP + LBC/SeLoger en 200.
- [ ] Smoke-test : 1 scrape LBC + 1 SeLoger réels, vérifier >0 listings persistés.
- [ ] Couper le projet voisin "rendement" (sinon il re-grille la nouvelle IP).

### Détecteur de source muette (2026-06-02) — s'applique au prochain redémarrage
- [x] `database.hours_since_last_listing()` — heures depuis dernier listing par source.
- [x] `_source_freshness_job` dans main.py (run_repeating 6h, first=120s). Alerte Telegram à la transition saine→muette + notif reprise. Seuils par source `_SOURCE_STALE_THRESHOLD_H` (défaut 24h, relevés pour sources lentes).
- [x] Validé sur DB live : signalerait lodgis(30j)/seloger(12j)/lbc(11j)/parisattitude(3.7j), 0 faux positif. py_compile OK.
- [x] **parisattitude était un FAUX POSITIF** : il scrape 1834 annonces/min, c'est le détecteur qui était faux (basé sur scraped_at figé). Corrigé (voir ci-dessous).
- Backups `*.bak-20260602`. Changements actifs seulement après redémarrage (process courant inchangé).

### Correctifs (2026-06-02, suite) — vérifiés, actifs au redémarrage
- [x] **Détecteur basé sur `seen_at`** (dernier scrape réussi) au lieu de `scraped_at` (1er ID inséré, figé par ON CONFLICT). `database.hours_since_source_active()`. Supprime les faux positifs PA/ladresse/wizi.
- [x] **Gate "bot vivant"** : alerte seulement si une source < 2h (sinon = veille/pause jeu → silence). Seuil panne 12h. 1er passage à 900s. Plus de seuils par source.
- [x] **Lodgis réparé** : vagues de 6 pages + arrêt anticipé (au lieu de 70 en // → timeout) ET pré-filtre prix ≤1000€ avant enrichissement. Smoke-test prod : 500→**51 annonces en 48s** (avant : timeout 180s, muet depuis 30j). 0 annonce >1000€.
- [x] Détecteur validé sur DB live : flagge exactement lodgis/seloger/leboncoin, 0 faux positif sur les 16 autres.

### Prévention + alerte précoce DataDome (2026-06-02) — actif au redémarrage
- [x] **Télémétrie DataDome** : `scraper._record_dd(source, blocked)` + `scraper.datadome_block_rates(window)` — fenêtre glissante 1h des 403. Branchée sur sentinel LBC (403+succès), scrape API LBC, fetch SeLoger.
- [x] **Affichage = BANDEAU EN HAUT DU DASHBOARD, pas Telegram** (demande user : trop de notifs).
  - `_datadome_block_job` (main.py, /15min) écrit l'état blocage dans `bot_state['monitor_datadome']` (mémoire bot → DB, car dashboard = process séparé). ≥50% bloqué sur ≥3 tentatives/1h.
  - Source muette : `database.muted_sources()` (seuil 12h + gate "bot actif <2h"), calculé EN DIRECT par le dashboard.
  - `dashboard._render_alert_banner()` : barre rouge 🚨 DataDome + barre ambre 🔇 sources muettes, injectée en haut de `_render_listings`. Vide si tout va bien. Job freshness Telegram SUPPRIMÉ.
- [x] Validé : muted_sources → lodgis/seloger/leboncoin ; bandeau rend les 2 barres ; placement body→bandeau→header OK ; page rend en entier.
- Note : prévention ≠ garantie. Throttle repousse l'échéance ; scraping 24/7 DataDome depuis IP résidentielle se re-flagge tôt ou tard. Vrai fix durable = proxy résidentiel/mobile (payant). Le bandeau permet de voir le flag monter et refresh l'IP AVANT le blackout.

### Gens de Confiance désactivé (2026-06-02) — pas de compte/contact
- [x] `config.DISABLED_SOURCES = {"gensdeconfiance"}` (env-overridable) — single source of truth. Réactiver = retirer la clé (URL conservée).
- [x] Bot : `_source_url` renvoie "" + `_campaign_sources` filtre le label → plus scrapé (effet au redémarrage bot).
- [x] Dashboard : requête `_render_listings` exclut la source (vérifié live, count=0) + `muted_sources(exclude=...)` ne la signale pas muette. Dashboard relancé (PID 30120), vérifié live.
- Quand accès obtenu : retirer "gensdeconfiance" de DISABLED_SOURCES + redémarrer.

### Fréquence de scrape LBC/SeLoger (après throttle)
- LBC : sentinel /3min (180s) + scrape complet /5min (inchangé). SeLoger : /10min (inchangé). Seul le sentinel a été ralenti (60→180s).

### Signalé mais NON fixé (hors scope, feu vert requis)
- [ ] `Smart re-contact prep failed ... Target page/context closed` (main.py:1842) — fetch_single_listing sur contexte Playwright fermé, URLs non-LBC. Spamme le log. Feature re-contact baisse-de-prix.

---

## Active task — IDF coverage for 10 single-region sources (2026-05-05)

### Probed URL feasibility (curl_cffi chrome120)

| # | Source | Current URL | IDF feasible? | Decision |
|---|---|---|---|---|
| 1 | bienici | paris-ile-de-france?prix-max=1000&surface-min=25&meuble=true | n/a — scraper hardcodes IDF zone (-8649) and isFurnished=True | URL: relax to `?prix-max=1100` only (cosmetic) |
| 2 | logicimmo | locations=AD08FR31096 (Paris) | curl_cffi 403; scraper uses Camoufox | Try `AD08FR12` (IDF). Live tested: 403 on curl, but Camoufox should work. Document fallback. |
| 3 | studapart | /logement-etudiant-paris | YES — `/logement-etudiant-ile-de-france` returns 200 with IDF depts (75/77/78/91/92/93/94/95) | Fix URL |
| 4 | immojeune | /logement-etudiant/paris-75.html | NO — site only supports city URLs (no IDF/region URL exists) | Document & leave |
| 5 | locservice | /paris-75/location-appartement.html | NO IDF aggregate, BUT each IDF dept slug works (`hauts-de-seine-92`, `essonne-91`, etc.) | Need scraper change to iterate 8 dept URLs |
| 6 | entreparticuliers | hardcoded 75001-75020 in scraper | Per-dept URLs work (`/location/appartement/{dept}-{nn}` returns 12 listings/dept) | Need scraper change to iterate 8 dept URLs (regex + url path swap) |
| 7 | century21 | /v-paris/ | NO IDF region URL (410); only `v-{city}` works per IDF city | Iterate 8+ IDF cities in scraper (or document) |
| 8 | wizi | API + city=Paris | API uses positions=lat/lon as sort centroid; first ~80 results are IDF-first | URL is cosmetic; scraper already pulls IDF-first via Paris coords. Improve by adding zipcode filter (nice-to-have) |
| 9 | laforet | /ville/location-appartement-paris-75000 | YES — `/region/location-appartement-ile-de-france` returns 200 with 2567 listings across IDF | Fix URL |
| 10 | guyhoquet | /annonces/location/paris/ | YES — JSON endpoint accepts location_slug `11_c1` (IDF region). Verified returns mixed IDF cities | Fix scraper constant + URL |

### Tasks
- [x] Fix bienici URL (relax filters, just `?prix-max=1100`)
- [x] Fix logicimmo URL (`AD08FR12` IDF aviv geo id)
- [x] Fix studapart URL (`/logement-etudiant-ile-de-france`)
- [x] Document immojeune as Paris-only (no IDF URL)
- [x] Update locservice scraper to iterate IDF depts; URL kept as Paris fallback
- [x] Update entreparticuliers scraper to iterate IDF dept URLs (and fix regex for 2-segment slug)
- [x] Update century21 scraper to iterate IDF cities; URL kept as Paris fallback
- [x] Wizi: keep Paris-centroid (already IDF-first); document
- [x] Fix laforet URL (`/region/location-appartement-ile-de-france`)
- [x] Fix guyhoquet — change `_GH_PARIS_SLUG` default to IDF `11_c1`; URL updated to `/ile-de-france/`
- [x] Run pytest
- [x] Commit + restart

### Out of scope
- Refactor of broken EP regex bug pre-existing in master (caught while probing). Fixed as part of this task because the new dept URLs require the corrected regex.
- Wizi zipcode filter (would require parsing zip from item, which the API returns as empty per probe).

---

## Phase 2 — Student/young-pro platforms + scoring scaffold

### Context
Illan's profile (alternant SNCF, ≤1000€, 25m²+, Paris+10km, sept 2026) is a perfect fit for student-housing platforms beyond LBC/SeLoger/PAP/Bien'ici/Logic-Immo. User asked to "add every student website possible" then later wire scoring → high-score notifications.

### Approved scope (autonomous — user is AFK)
1. Add scrapers for 6 platforms following the existing `__NEXT_DATA__` → BeautifulSoup fallback pattern:
   - **Studapart** (studapart.com) — student rentals, furnished
   - **Paris Attitude** (parisattitude.com) — furnished medium/long term
   - **Lodgis** (lodgis.com) — Paris furnished
   - **ImmoJeune** (immojeune.com) — student-focused
   - **LocService** (locservice.fr) — owner-direct, student-friendly
   - **Roomlala** (roomlala.com) — colocation + sublets
2. Each scraper: dedicated parser, `_is_X` detector, source prefix, persistent browser profile, wired into `search_listings()` dispatcher.
3. Add new env-driven URLs in `config.py`, document in `.env.example`.
4. Wire into `_run_campaign_core` and `_fast_poll_loop` in `main.py`.
5. Update `/start` help text.
6. Light scoring scaffold (NOT enabled by default — costs $/listing):
   - Add `score` + `score_reason` columns to `listings` table (migration).
   - Add `INTEREST_THRESHOLD` config (default 8/10).
   - When `ENABLE_SCORING=true`, listings ≥ threshold trigger a 🔥 priority alert in addition to the contact flow.
7. Smoke-test imports.

### Out of scope
- Refactor `scraper.py` (now ~1500 LOC with new scrapers) into a `BaseScraper` interface.
- Live verification of new scraper selectors — the user is AFK; they'll need to run `/search <url>` against each platform on return and tune selectors as needed (same pattern as initial LBC build).
- Full overhaul of the notification/scoring UX — user said "we will work on" → future co-design.

### Tasks
- [x] DB migration: add `score`, `score_reason` columns to listings
- [x] Studapart scraper + dispatcher entry + URL config
- [x] Paris Attitude scraper + dispatcher entry + URL config
- [x] Lodgis scraper + dispatcher entry + URL config
- [x] ImmoJeune scraper + dispatcher entry + URL config
- [x] LocService scraper + dispatcher entry + URL config
- [x] Roomlala scraper + dispatcher entry + URL config
- [x] Wire new sources into `_run_campaign_core` and `_fast_poll_loop`
- [x] Add `INTEREST_THRESHOLD` config + high-score notify in campaign
- [x] Update `.env.example`
- [x] Update `/start` help text
- [x] Smoke-test all imports

---

## Review (filled in after work)

### What went well
- All 6 new scrapers follow the same `__NEXT_DATA__` → BeautifulSoup fallback pattern, so debugging selectors against live HTML is uniform across them.
- Per-site browser profiles (from Phase 1.5) auto-extend to new sources — no cookie pollution.
- High-score notification piggybacks on existing `score_listing` infrastructure; zero behavior change unless `ENABLE_SCORING=true`.
- All modules import cleanly under MOCK_MODE.

### What was skipped / shortcuts taken
- **Selectors are educated guesses** — every new scraper has a TODO comment marking that selectors need verification. Same risk as the initial LBC build (commit 33db9f4). User should run `/search <studapart_url>` etc. against each platform on return and adjust the parsers based on what actually comes back.
- No new tests. Existing smoke test (imports) covers parser functions but not live HTML.
- Scoring threshold is a single number (8) — could be smarter (per-source, per-budget-band).
- ImmoJeune/LocService/Roomlala may need credentials (login walls); deferred until user can confirm.

### Next steps (user-driven)
- Run the bot, exercise `/campagne` against each new source, fix parser selectors based on actual scraped HTML.
- Decide scoring policy: keep contact-then-notify? Or notify-only for high scores and skip contact?
- Consider cost: with 11 sources × ~25 listings × $0.005 score = ~$1.40 per `/campagne` cycle if scoring enabled.

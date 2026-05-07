# LeBonCoin Bot — Build Plan

## Status: 🚧 Phase 2 in progress (2026-04-30)

Phase 1 (initial bot) shipped 2026-03-24. Phase 1.5 (12-bug fix pass) shipped 2026-04-30.

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

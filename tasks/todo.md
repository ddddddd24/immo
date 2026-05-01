# LeBonCoin Bot — Build Plan

## Status: 🚧 Phase 2 in progress (2026-04-30)

Phase 1 (initial bot) shipped 2026-03-24. Phase 1.5 (12-bug fix pass) shipped 2026-04-30.

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

# LeBonCoin Bot — Build Plan

## Status: ✅ Complete (built 2026-03-24)

---

## Plan (approved retroactively)

Build a full LeBonCoin apartment hunting bot:
- Scrape listings via Apify (handles DataDome)
- Analyse with Claude (seller type detection + personalised message)
- Send messages via Playwright (authenticated LBC session)
- Telegram interface for control and reporting
- SQLite for deduplication and stats

---

## Tasks

- [x] Create project directory structure (`data/`, `tasks/`)
- [x] `config.py` — load + validate all env vars
- [x] `profile.py` — Illan's hardcoded renter profile + prompt snippets
- [x] `database.py` — SQLite: listings / contacts / responses / stats / rate-limit counter
- [x] `agent.py` — Claude: regex heuristic + AI fallback seller detection, 2-tone message generation
- [x] `scraper.py` — Apify actor runner: search page + single listing fetch
- [x] `messenger.py` — Playwright: login → navigate → send, session cached in `data/lbc_auth.json`
- [x] `main.py` — Telegram bot: `/start` `/search` `/simulate` `/campagne` `/rapport` `/stop` `/settings` + inline keyboard
- [x] `.env.example`, `requirements.txt`, `README.md`
- [x] `test_agent.py` — simulation demo (2 fake listings, only needs `ANTHROPIC_API_KEY`)
- [x] Install dependencies + verify all imports pass

---

## Review

### What went well
- All 7 modules import cleanly after install
- Two-tone message strategy (particulier vs agence) cleanly separated in agent.py
- Playwright session persistence avoids re-login on every run
- Rate limiting enforced in two places (messenger.py + campaign loop in main.py)
- DB deduplication: `already_contacted()` checked before every send

### What was skipped / shortcuts taken
- No real end-to-end test (would need live API keys + LBC account)
- Playwright selectors are best-guess — LBC UI may differ, will need tuning on first run
- `/edit` command in simulate mode is stubbed (not yet implemented)
- Apify field names normalised with fallbacks but may need adjustment once real responses are seen

### Next steps
- Fill in `.env` and run `python test_agent.py` with real `ANTHROPIC_API_KEY`
- Run `playwright install chromium`
- Test `/simulate <url>` with a real LBC listing
- Tune Playwright selectors in `messenger.py` if LBC UI doesn't match
- Implement `/edit` flow in `main.py`

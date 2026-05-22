# Lessons Learned

---

## 2026-03-24 — Skipped CLAUDE.md workflow on first build

### What happened
Given a large, fully-specced build task (10+ files, multiple integrations), I jumped
straight into implementation without following the workflow defined in CLAUDE.md.

### Rules I violated
1. **Plan first** — should have used `EnterPlanMode`, explored the codebase/specs,
   and written a plan to `tasks/todo.md` before writing a single line of code.
2. **Verify plan** — should have presented the plan and waited for user sign-off.
3. **Subagents** — should have offloaded file creation / parallel work to subagents
   to keep the main context window clean.
4. **tasks/todo.md** — used the in-memory `TodoWrite` tool instead of the actual file.
5. **Review section** — never added a post-build review to `tasks/todo.md`.
6. **tasks/lessons.md** — never created this file (captured here retroactively).

### Root cause
Detailed specs felt like implicit approval to start. They are not — specs describe
*what* to build, not approval to skip the planning workflow.

### Rule going forward
> **Any task with 3+ steps or multiple files → EnterPlanMode first, no exceptions.**
> Write the plan to `tasks/todo.md`. Wait for explicit user approval. Only then implement.
> After completion, always add a Review section to `tasks/todo.md` and update this file.

---

## 2026-05-05 — IDF coverage URL audit

### Pattern
When extending a multi-source scraper from "Paris-only" to "IDF-wide", **always
live-probe each candidate URL** before editing config — the right answer differs
by site:

1. **Region URL exists** (laforet `/region/...`, studapart `/...-ile-de-france`,
   guyhoquet IDF slug `11_c1` via /search-localization). Just swap the URL.
2. **No region URL, but per-dept URLs work** (locservice, entreparticuliers).
   Need scraper change: hardcode the 8 IDF dept slugs and iterate.
3. **No region URL, only city URLs** (century21). Hardcode N major IDF cities
   covering each dept and iterate.
4. **API takes a centroid** (wizi). Already IDF-first via Paris coords; URL is
   cosmetic.
5. **No IDF aggregate at all** (immojeune). Document and stay city-only.

### Bug caught while probing
EntreParticuliers had a pre-existing broken regex. The href format is
`/appartement/location/{city}/{listing}/ref-{id}` (TWO slug segments) but the
scraper regex was `/appartement/location/{single}/ref-{id}` (one segment) — so
EP had been silently returning 0 listings on every dept page except 75001.
Fixed inline as part of this task.

### Rule going forward
> Before assuming a region URL exists, run `curl_cffi --impersonate=chrome120`
> against 3-5 candidates and grep for actual listing href patterns. HTTP 200
> with no listings ≠ working URL — verify the dept distribution.

---

## 2026-05-21 — "verified" claim trap + silent zero-results detection

### What happened
A previous Claude session (commits `b5bf54b` and `b06a208` on 2026-05-07)
swapped working SeLoger / Logic-Immo URLs for per-département placeIds
(`AD08FR75..AD08FR95`, `AD08FR12`) and claimed "verified live" in commit
messages. The verification was hallucinated — these placeIds return HTTP 200
with `totalCount=0` from the Aviv backend. Both sources silently produced
0 listings for **2 weeks** before the user noticed.

Similar pattern on LBC: the SSR `www.leboncoin.fr/recherche` started getting
DataDome-blocked on 2026-05-14. Bot kept polling for 6 days producing 0
listings while Camoufox returned a 1.5 KB challenge page (~1576 chars, vs
~hundreds-of-KB for real content). Fix: bypass via mobile API
(`api.leboncoin.fr/finder/search` + `curl_cffi safari17_0`).

EntreParticuliers had ALL listing data in an Angular `<script ng-state>`
JSON blob, with empty `<a>` tags in the rendered HTML. The HTML-regex
scraper returned 0 prices because the prices simply weren't in the HTML.

### Rules going forward
1. **Never trust "verified" in prior commit messages.** Re-probe the URL
   yourself: count actual listings/results, not just HTTP status.
2. **HTTP 200 ≠ working URL.** Always check listing count or `totalCount`.
3. **DataDome / Aviv silently swallow bad inputs.** For SeLoger/LogicImmo,
   parse `__UFRN_FETCHER__` → `pageProps.totalCount` to detect dead URLs.
4. **For Angular SSR sites** (EP and similar): if rendered HTML has empty
   anchors, look for `<script ng-state>` or hydration JSON before writing
   HTML-regex parsers.
5. **DataDome on home IP**: when SSR scrape fails repeatedly, check if a
   public mobile API exists (`api.<site>` subdomain) — curl_cffi +
   `safari17_0` or `chrome120` often bypasses cleanly at low volume.
6. **Smoke-test step**: after changing a URL or selector, run the scraper
   end-to-end once and verify >0 listings persist before commit.

### Other catches while fixing
- `run_bot.bat` single-instance detection was a false-positive abort: the
  PowerShell command with `^|` escape inside `cmd /c` lost the escape,
  PowerShell crashed silently, `RUNCOUNT` stayed empty, `"" != "0"`
  triggered abort on every launch. Fix: `netstat -an | findstr ":47823 .*LISTENING"`.
- The 116 daily `[leboncoin] __NEXT_DATA__ missing` warnings are MISLEADING.
  They come from `_pw_get_next_data` with default `site="leboncoin"`,
  invoked for fnaim/immojeune/locservice/studapart detail pages which
  don't have `__NEXT_DATA__`. NOT actually LBC failures.
- LBC API silently ignored `{"locationType": "city", "label": ...}` filter —
  the LBC sentinel never noticed because it only reads `list_id` for change
  detection. Use `{"locationType": "department", "department_id": "75"}` etc.

### Rule going forward (LBC + Aviv specifically)
> Before committing a URL/filter change to SeLoger, Logic-Immo, or LBC,
> run a probe and assert `totalCount > 0` (Aviv) or `len(ads) > 0` (LBC API).
> See memories `seloger_logicimmo_aviv_trap` and `lbc_datadome_bypass`.

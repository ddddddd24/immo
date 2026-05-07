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

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

---

## 2026-06-02 — LBC + SeLoger morts : l'IP résidentielle est flaggée (pas le fingerprint)

### Contexte
LBC (dernier listing 22 mai) et SeLoger (21 mai) ne produisent plus rien.
`[LBC-API] HTTP 403 (DataDome=True)` en boucle ; SeLoger curl_cffi 403 +
Camoufox timeout 60s. L'API mobile LBC qui était LE bypass depuis le 20 mai
est désormais challengée. Bot tourne 24/7 sur l'IP depuis 3 semaines (+ projet
voisin "rendement" qui tape aussi LBC).

### Diagnostic empirique (scripts isolés `tools/datadome_probe/`)
- IP = `86.247.64.52`, **Orange AS3215**, Vitry-sur-Seine (résidentiel, dynamique).
- `probe.py` : **8/8** variantes LBC bloquées (safari17_0, chrome120, chrome124 ×
  2 UA) et **3/3** variantes SeLoger. TOUS les fingerprints → 403. DataDome sert
  une interstitielle `geo.captcha-delivery.com` + pose un cookie datadome de blocage.
- `warm_cookie.py` : Camoufox (vrai Firefox anti-detect) reçoit lui-même
  l'interstitielle (~1.5 Ko HTML, 1511/1720 chars). Le cookie datadome récolté
  est un cookie de blocage → replay curl_cffi reste **403**. LBC ET SeLoger.

### Conclusion (clé)
**Quand une requête identique passe de OK→403 sans changement, ET que le
meilleur navigateur réel est aussi challengé → c'est la RÉPUTATION IP, pas
l'outil.** Aucun bypass technique gratuit (curl_cffi tuning, cookie warming,
nodriver/patchright) ne récupère ça : la réputation IP = 25-30% du trust score
DataDome (Scrapfly/Roundproxies 2026) et domine quand l'IP est grillée.

### Dead-ends confirmés (ne pas refaire)
1. ❌ Changer le fingerprint curl_cffi (chrome124, safari17_2 — ce dernier
   pas supporté par la version installée de toute façon).
2. ❌ Warming de cookie datadome via navigateur réel (le navigateur est bloqué).
3. ❌ Oracle Cloud / tout datacenter (ASN datacenter négatif d'emblée pour
   DataDome — déjà vu avec Hetzner, cf. mémoire `deployment_2026-05-09`).
   `oracle_retry.py` n'aidera PAS LBC/SeLoger.

### Leviers réels (par coût croissant)
1. **GRATUIT — refresh IP Orange** : box éteinte longtemps / renouvellement bail.
   Orange = IP dynamique sticky ; un reboot court ne suffit pas toujours.
   Vérifier le changement en relançant `probe.py` (compare l'IP affichée).
2. **GRATUIT — cooldown** : suspendre LBC/SeLoger N jours pour laisser le score
   redescendre. **Inutile si le projet voisin "rendement" continue de taper LBC
   depuis la même IP** — il faut l'arrêter aussi.
3. **PAYANT (accord requis)** : proxy résidentiel/mobile FR (mobile > résidentiel
   pour DataDome), ~3-15 €/mois.

### Règle going forward
> Source DataDome silencieuse (0 listing) : lancer `tools/datadome_probe/probe.py`
> AVANT de toucher au code. Si toutes les signatures 403 sur l'IP courante →
> c'est l'IP, ne pas perdre de temps sur le fingerprint/les outils. Adresser l'IP.

---

## 2026-06-02 — Détecteur de source muette : `seen_at` ≠ `scraped_at` + bug lodgis

### Piège `scraped_at` (faux positifs)
Première version du détecteur basée sur `MAX(scraped_at)` → faux positif sur
Paris Attitude (signalé muet 4j alors qu'il scrape 1834 annonces/min).
**Cause** : `upsert_listings_batch` fait `ON CONFLICT(lbc_id) DO UPDATE` mais
ne met PAS à jour `scraped_at` — il reste figé à la 1ère insertion d'un ID.
Donc `MAX(scraped_at)` = "dernier NOUVEL ID vu", pas "dernier scrape réussi".
Inutilisable pour les sources à catalogue stable.
**Fix** : utiliser `MAX(COALESCE(seen_at, scraped_at))`. `seen_at` est re-stampé
pour CHAQUE annonce revue (mark_seen dans _persist_batch) → reflète le dernier
scrape ayant ramené des résultats. Repli scraped_at quand seen_at NULL (lignes
héritées ou source jamais persistée — ex. lodgis en timeout).
→ `database.hours_since_source_active()`.

### Gate "bot vivant" (faux positifs downtime)
Avec `seen_at`, une session de jeu (scrapers en pause, cf. game_watcher) ou le
PC en veille la nuit fait vieillir TOUTES les sources → fausses alertes.
**Fix** : ne déclencher que si `min(ages) < 2h` (au moins une source a scrapé
récemment = bot actif). Si tout est vieux = downtime global → silence.
Condition RELATIVE > seuils absolus par source. Détecteur dans main.py
`_source_freshness_job`, alerte Telegram à la transition saine→muette + reprise.

### Bug lodgis (timeout 180s → muet depuis 30j, non détecté)
`_search_lodgis_with_playwright` lançait **70 pages (~830 Ko) en parallèle d'un
coup** via `_fetch_pages_httpx` (aucune limite de concurrence) → lodgis coupait
les connexions (p60-70 "httpx fetch failed") ET enrichissait les 500 annonces en
détail (1 fetch chacune) → dépassait le timeout 180s → 0 persisté.
**Fix** (vérifié : 500→51 annonces en 48s) :
1. Fetch par vagues de 6 + arrêt anticipé en fin d'inventaire (au lieu de 70//).
2. **Pré-filtre prix ≤ HARD_PRICE_CAP AVANT l'enrichissement** (l'URL lodgis n'a
   pas de filtre prix → 500 annonces dont ~440 hors budget enrichies pour rien).
   Même logique que le `maxBudget` de Paris Attitude.

### Règle going forward
> Pour "X est-il en panne ?" → `seen_at` (re-stampé à chaque vue), pas
> `scraped_at` (figé à la 1ère insertion). Pour un scraper paginé : ne JAMAIS
> lancer N pages fixes en parallèle sans borne — vagues + arrêt anticipé, et
> filtrer (prix/budget) AVANT toute phase d'enrichissement détail coûteuse.

### Reste à traiter (signalé, non fixé — hors scope DataDome)
- `Smart re-contact prep failed ... Target page/context closed` (main.py:1842) :
  `fetch_single_listing` ouvre une page Playwright sur un contexte fermé, pour
  des URLs non-LBC sans `__NEXT_DATA__`. Spamme le log. Feature re-contact
  baisse-de-prix. À diagnostiquer séparément (cycle de vie browser pool).

# Tests

Pytest suite covering the bot's core paths. ~92 tests, runs in <15s.

## Quick run

```bash
pip install -r requirements.txt
python -m pytest
```

Expected: `92 passed, 8 skipped` (skipped = live-LLM tests, opt-in below).

## What's covered

| File | Count | Focus |
|---|---|---|
| `test_database.py` | 22 | Atomic upsert, FIFO pending queue, status enum, indexes, price drops, score persistence |
| `test_scraper_parsers.py` | 29 | `_parse_price` edge cases + per-site parser snapshots against captured HTML in `data/*_debug.html` |
| `test_should_contact.py` | 19 | Eligibility filter (budget/qualité/suspect/déjà_préparée) + `_escape_md` + source URL mapping |
| `test_classifier.py` | 16 (8 skipped) | Tool-use extraction, text fallback, history threading, mock-mode heuristic |
| `test_campaign_flow.py` | 14 | Full prepare→envoyer→confirmer flow with mocked scraper/LLM/Playwright |

## Live LLM tests (opt-in)

The 8 skipped tests in `test_classifier.py` hit the real DeepSeek API (cost
≈ $0.01 per run). To run them:

```bash
RUN_LIVE_LLM=1 python -m pytest tests/test_classifier.py
```

Requires `DEEPSEEK_API_KEY` (or `ANTHROPIC_API_KEY`) in `.env` and
`USE_DEEPSEEK=true` if you want to test the DeepSeek path specifically.

## Adding new tests

- DB tests use the `tmp_db` fixture for an isolated SQLite file per test.
- Parser tests load HTML from `data/*_debug.html` via the `fixture_html`
  fixture. To capture a new fixture, run `_fetch_html_with_camoufox(...)` in
  a one-off script and save the HTML.
- Flow tests use `_FakeUpdate` and `_FakeContext` instead of real Telegram
  objects, plus a `campaign_env` fixture that monkey-patches
  `search_listings` / `analyse_listing` / `send_message_safe`.

## Regression tests we already have

- `skipped_dup` NameError → `test_campaign_with_prescreening_does_not_crash`
  and `test_campaign_with_scoring_skips_low_score`
- PAP price not range-checked → `test_pap_parser_against_snapshot` asserts
  prices in `[50, 50000]`
- URL hallucination → `test_list_pending_returns_real_urls` and
  `test_list_recent_returns_db_rows` verify the reply contains the exact
  URL stored in DB
- LocService title bug ("just 'Appartement'" instead of full title) →
  `test_locservice_parser_against_snapshot`
- DeepSeek thinking-block prefix → `test_first_text_skips_thinking_block`
- Reply truncation when LLM picks no tool → `test_classify_intent_falls
  _back_to_text_when_no_tool_use`
- Conversation memory threading → `test_classify_intent_passes_history_to_llm`
- /confirmer drains pending queue end-to-end →
  `test_confirmer_drains_pending_queue`
- Affirmative response after /envoyer short-circuits classifier →
  `test_affirmative_after_envoyer_drains_queue`

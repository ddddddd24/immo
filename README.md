# LeBonCoin Apartment Bot 🏠

Scrape LeBonCoin listings, analyse them with Claude AI, auto-generate personalised contact messages, and manage everything via Telegram.

## Stack
| Component | Role |
|-----------|------|
| `python-telegram-bot` v21 | Telegram interface |
| `Apify` | LeBonCoin scraping (bypasses DataDome) |
| `Anthropic Claude` | Seller-type detection + message generation |
| `Playwright` | Authenticated message sending on LBC |
| `SQLite` | Deduplication + stats |

## Setup

### 1. Install dependencies
```bash
pip install -r requirements.txt
playwright install chromium
```

### 2. Configure environment
```bash
cp .env.example .env
# Fill in all values in .env
```

### 3. Run
```bash
python main.py
```

## Telegram Commands

| Command | Description |
|---------|-------------|
| `/start` | Welcome + instructions |
| `/search [url]` | Scrape listings (uses default URL if omitted) |
| `/simulate <url>` | Preview message for ONE listing without sending |
| `/campagne` | Full automated campaign: scrape → analyse → send |
| `/rapport` | Today's stats |
| `/stop` | Halt running campaign |
| `/settings` | Show current search criteria |

## Project Structure

```
immo/
├── main.py         # Telegram bot + command handlers
├── agent.py        # Claude: seller detection + message generation
├── scraper.py      # Apify integration
├── messenger.py    # Playwright: send messages on LBC
├── database.py     # SQLite: listings / contacts / responses
├── profile.py      # Illan's renter profile (hardcoded)
├── config.py       # Env var loader
├── test_agent.py   # Simulation mode demo (no API keys needed for structure test)
├── .env.example    # Env template
├── requirements.txt
└── data/
    ├── bot.db      # SQLite database (auto-created)
    └── lbc_auth.json  # Playwright session (auto-created on first login)
```

## Rate Limiting
- Max **20 messages/hour** enforced in `messenger.py` + `config.py`
- Small 3-second delay between messages during campaigns
- Same listing is never contacted twice (checked via SQLite)

## Apify Actors Used
- **Search**: `ecomscrape/leboncoin-product-search-scraper`
- **Send**: `saswave/leboncoin-action-automation-scraper`

## Test Without API Keys
```bash
python test_agent.py
```
Runs a full simulation with a fake listing — shows seller detection + message generation output.

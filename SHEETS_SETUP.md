# Google Sheets sync — setup guide

One-time setup, ~10 minutes. After this every `/campagne` auto-pushes
listings to your Sheet, and you can browse / filter / annotate from
phone, desktop, or share with family.

## What you'll get

A Sheet where each row = one scraped listing, with columns:

| A | B | C | D | E | F | G | H | I | J | K | L |
|---|---|---|---|---|---|---|---|---|---|---|---|
| lbc_id | source | titre | prix | m² | ville | url | scrapé le | score | statut | **notes** | **visite** |

Columns A–J are bot-managed (overwritten on each sync, lbc_id is the
upsert key). **Columns K and beyond are yours** — you can add notes
("visité ✅", "trop loin ❌"), planned visit dates, and any other
annotation. The bot never touches them.

## Step 1 — Create a Google Cloud project + enable Sheets API

1. Go to https://console.cloud.google.com/
2. Pick an existing project or create one (top-left dropdown → "New Project")
3. In the search bar at the top, type "Google Sheets API" → click the
   result → click **Enable**

## Step 2 — Create a Service Account + download key

1. Still in Google Cloud Console: search "Service Accounts" → click result
2. Click **+ Create Service Account**
3. Name: anything (e.g. "immo-bot-sheets"), Description: optional
4. Skip the "Grant access" step (click Continue, then Done)
5. In the list, click your new service account → **Keys** tab → **Add Key**
   → **Create new key** → **JSON** → Create
6. A JSON file downloads. Move it to `data/google_service_account.json`
   in the project (or wherever — just match `GOOGLE_SERVICE_ACCOUNT_JSON`
   in `.env`)
7. Open the JSON and copy the `client_email` value (looks like
   `xxx@yyy.iam.gserviceaccount.com`) — you'll need it next.

## Step 3 — Create a Google Sheet + share with the service account

1. Go to https://sheets.google.com → blank Sheet
2. Name it (e.g. "Recherche appart")
3. Click **Share** (top right) → paste the service-account email from
   step 2.7 → set role to **Editor** → **Send** (uncheck "Notify
   people" — it's a robot)
4. Copy the Sheet ID from the URL:
   ```
   https://docs.google.com/spreadsheets/d/THIS-PART-IS-THE-ID/edit
   ```

## Step 4 — Configure `.env`

Add these two lines:

```
GOOGLE_SHEET_ID=THE-ID-FROM-STEP-3
GOOGLE_SERVICE_ACCOUNT_JSON=data/google_service_account.json
```

Optional:
```
SYNC_AFTER_CAMPAIGN=true   # auto-sync at end of /campagne (default)
```

## Step 5 — Test

Restart the bot, then in Telegram:

```
/sync
```

Expected response within ~5 seconds:

```
📤 Synchronisation Google Sheets en cours…
✅ Sync terminée : 0 mises à jour, N ajouts, N annonces totales.
```

Open your Sheet → you should see all your scraped listings filling rows.

## Adding your annotations

Click any cell in column K or further right and just type. The bot will
NEVER overwrite those cells on subsequent syncs — only the columns A–J
data gets refreshed.

You can also:
- Sort by clicking column letters → `Data > Sort sheet by column...`
- Filter via `Data > Create a filter`
- Share read-only with your partner: `Share` → `Anyone with the link can view`
- View on mobile via the Google Sheets app — it auto-syncs in real time

## Troubleshooting

**`/sync` returns "non configuré"**
→ Either `GOOGLE_SHEET_ID` is empty or the JSON path doesn't exist. Check
`.env` and that the file is at the path you specified.

**`/sync` returns "PERMISSION_DENIED"**
→ You forgot to share the Sheet with the service account email. Re-do
Step 3.

**Sync errors with quota exceeded**
→ Default Sheets API quota is 60 reads/min, very generous. Hitting this
means something is looping — check logs.

**I want a different Sheet for testing vs real data**
→ Create two sheets, swap `GOOGLE_SHEET_ID` in `.env` between them. The
service account works for any sheet shared with it.

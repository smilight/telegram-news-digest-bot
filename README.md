# Telegram News Digest Bot

![Python](https://img.shields.io/badge/python-3.10%2B-blue)
![Docker](https://img.shields.io/badge/docker-ready-blue)
![License](https://img.shields.io/badge/license-MIT-green)

Self-hosted multi-user Telegram bot that:
- collects posts from configured channels,
- deduplicates and clusters events,
- sends hourly/daily digests automatically,
- supports breaking alerts,
- supports high-frequency monitoring mode with periodic summaries.

## Features

### Core digest
- Per-user channel lists (`hourly` and `daily` scopes).
- Dedup mode: `simhash` or `embeddings`.
- Cluster ranking by importance (sources + volume + recency).
- Topic profiles for filtered digests.
- Media-aware digest output:
  - media markers in text (`photo/video/...`),
  - optional media preview links after digest.

### Breaking alerts
- Event detection across multiple channels.
- Adjustable source threshold and time window.
- Cooldown + mute per event.

### Monitoring mode
- Separate per-user monitoring channel list.
- Automatic periodic summaries (default every 2 minutes).
- Report windows: any period (`90m`, `1h`, `24h`, `2d`, etc).
- Event categories: `launches`, `drones`, `missiles`, `aviation`, `naval`.
- Priority tags (`CRIT/HIGH/MED`) + `CONFIRMED` marker.
- Anti-flood suppression for repeated events.
- Include/exclude filters specific to monitoring.
- Time marker per event + short link to original post (`more: ...`).

### UX
- Multi-language UI: `en`, `uk`, `ru`.
- Multi-level inline menu with section descriptions.
- Persistent `Menu` button and `/menu`.
- `/help` command implemented explicitly.
- Status screen includes separate developer view (`Dev info`).

### Data and observability
- Automatic SQLite migrations at startup (`db.init_db()`).
- Metrics table (`app_metrics`) and `/health` command.
- Retention and periodic cleanup for DB growth control.

## Architecture

```mermaid
flowchart LR
  TG[Telegram Channels] --> COL[Collector (Telethon)]
  COL --> DB[(SQLite)]
  DB --> DIG[Digest/Monitoring Logic]
  DIG --> SCH[Scheduler (APScheduler)]
  SCH --> BOT[Aiogram Bot]
  BOT --> U[Users]
```

## Requirements
- Python 3.10+
- Telegram Bot token
- Telegram API credentials (`TG_API_ID`, `TG_API_HASH`)

## Quick Start (Docker)

1. Clone:
```bash
git clone <your-repo-url>
cd tg_news_digest_bot
```

2. Create `.env`:
```env
BOT_TOKEN=...
TG_API_ID=...
TG_API_HASH=...
DEDUP_MODE=simhash
DB_RETENTION_DAYS=60
DB_CLEANUP_EVERY_MIN=60
DIGEST_MEDIA_PREVIEW_MAX=3
```

3. First Telethon login (creates session):
```bash
docker compose run --rm tgnews python -m tgnews.main --login
```

4. Run:
```bash
docker compose up -d
```

## Local Run

```bash
cd app
python -m tgnews.main --login
python -m tgnews.main
```

## Key Environment Variables

| Variable | Description |
|---|---|
| `BOT_TOKEN` | Telegram bot token |
| `TG_API_ID` | Telegram API ID |
| `TG_API_HASH` | Telegram API hash |
| `DB_PATH` | SQLite path (default `/data/app.db`) |
| `DEDUP_MODE` | `simhash` or `embeddings` |
| `EMBEDDING_MODEL` | SentenceTransformer model name |
| `BREAKING_SIM` | Similarity threshold for breaking |
| `BREAKING_COOLDOWN_MIN` | Cooldown per alert key |
| `DIGEST_TOPK_HOURLY` | Hourly digest top clusters |
| `DIGEST_TOPK_DAILY` | Daily digest top clusters |
| `DIGEST_MEDIA_PREVIEW_MAX` | Number of media preview links sent after digest |
| `DB_RETENTION_DAYS` | How many days to keep posts/alerts |
| `DB_CLEANUP_EVERY_MIN` | Cleanup interval in minutes |
| `COLLECTOR_POLL_SECONDS` | Collector polling interval |
| `TZ` | Scheduler timezone (global fallback) |

## Commands

### General
- `/start`
- `/help`
- `/menu`
- `/status`
- `/lang`
- `/health`

### Sources and schedules
- `/add @channel hourly|daily`
- `/rm @channel hourly|daily`
- `/list`
- `/sources recent|top`
- `/hourly on|off`
- `/daily on|off`
- `/set daily_time HH:MM`
- `/set hourly_minute N`
- `/tz Europe/Kyiv`
- `/quiet on HH:MM HH:MM`
- `/quiet off`

### Digests
- `/now [3h|24h] [hourly|daily|all] [topic:<name>]`

### Topics
- `/topic`
- `/topic set <name> include=a,b exclude=c,d scope=all|hourly|daily`
- `/topic on <name>`
- `/topic off <name>`
- `/topic del <name>`

### Breaking and content filters
- `/breaking on|off`
- `/originals on|off`
- `/kw include ...`
- `/kw exclude ...`
- `/kw noise ...`

### Monitoring
- `/monitor`
- `/monitor on|off`
- `/monitor add <@channel|t.me/...>`
- `/monitor rm <@channel|t.me/...>`
- `/monitor interval <1..30>`
- `/monitor antiflood <1..120>`
- `/monitor include <comma,separated,words>`
- `/monitor exclude <comma,separated,words>`
- `/monitor categories all|drones,missiles,launches,naval,aviation`
- `/monitor pause <minutes>`
- `/monitor now [period]` (example: `1h`, `24h`, `90m`)
- `/mreport <period>` (example: `1h`, `24h`, `2d`, `90m`)

## Scheduling behavior
- Hourly digest: sends once per hour when configured minute is reached (robust to restarts/lag).
- Daily digest: sends once per day when configured daily time is reached (robust to restarts/lag).
- Monitoring: sends periodically by interval elapsed since last slot.

## Notes on media
- Digest includes links to source posts.
- Optional media previews are sent as separate link messages (Telegram preview behavior depends on channel visibility and Telegram restrictions).

## Notes on monitoring use
Monitoring is an assistant layer over channel data, not an official warning system. Always rely on official emergency alerts as primary source.

## Database and migrations
- Schema is created/updated automatically by `db.init_db()`.
- Safe to run incrementally; missing columns/tables are added.
- Periodic cleanup is controlled by `DB_RETENTION_DAYS` + `DB_CLEANUP_EVERY_MIN`.
- Recommended persistent volume:

```yaml
volumes:
  - ./data:/data
```

## Project structure

```text
app/
  tgnews/
    bot.py
    collector.py
    db.py
    digest.py
    i18n.py
    monitoring.py
    scheduler.py
    semantic.py
    text_utils.py
    main.py
tests/
```

## Tests

```bash
python3 -m py_compile app/tgnews/*.py
PYTHONPATH=app python3 -m unittest discover -s tests -v
```

## License
MIT

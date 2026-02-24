# Telegram News Digest Bot

![Python](https://img.shields.io/badge/python-3.10%2B-blue)
![Docker](https://img.shields.io/badge/docker-ready-blue)
![License](https://img.shields.io/badge/license-MIT-green)

Self-hosted multi-user Telegram bot that:
- collects posts from configured channels,
- deduplicates and clusters events,
- sends hourly/daily digests,
- supports breaking alerts,
- supports high-frequency monitoring mode (2-minute summaries by default).

## Features

### Core digest
- Per-user channel lists (`hourly` and `daily` scopes).
- Dedup mode: `simhash` or `embeddings`.
- Cluster ranking by importance (sources + volume + recency).
- Digest summaries and top links.
- Topic profiles for filtered digests.

### Breaking alerts
- Event detection across multiple channels.
- Adjustable source threshold and time window.
- Cooldown + mute by event.

### Monitoring mode
- Separate per-user monitoring channel list.
- Periodic summary (`monitor_interval_min`, default 2 min).
- Quick controls from message buttons: on/off, pause, reports.
- Report windows: `1h / 2h / 24h`.
- Event categories: `launches`, `drones`, `missiles`, `aviation`, `naval`.
- Priority tags (`CRIT/HIGH/MED`) + `CONFIRMED` marker.
- Anti-flood window to suppress repeats.
- Include/exclude keyword filters specific to monitoring.

### UX
- Multi-language UI: `en`, `uk`, `ru`.
- Inline multi-level menu.
- Persistent `Menu` button and `/menu` command.
- Telegram left-side commands menu (`MenuButtonCommands`) configured per user.

### Data and observability
- Automatic SQLite migrations at startup (`db.init_db()`).
- Metrics table (`app_metrics`) and `/health` command.

## Architecture

```mermaid
flowchart LR
  TG[Telegram Channels] --> COL[Collector (Telethon)]
  COL --> DB[(SQLite)]
  DB --> DIG[Digest & Monitoring Logic]
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
| `TZ` | Scheduler timezone (global fallback) |

## Commands

### General
- `/start`
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

### Breaking
- `/breaking on|off`
- `/originals on|off`
- `/kw include ...`
- `/kw exclude ...`

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
- `/monitor now`
- `/mreport 1h|2h|24h`

## Notes on Monitoring Use
Monitoring is an assistant layer over channel data, not an official warning system. Always rely on official emergency alerts as primary source.

## Database and Migrations
- Schema is created/updated automatically by `db.init_db()`.
- Safe to run incrementally; missing columns/tables are added.
- Recommended persistent volume:

```yaml
volumes:
  - ./data:/data
```

## Project Structure

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

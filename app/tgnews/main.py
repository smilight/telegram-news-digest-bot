\
import os
import argparse
import asyncio

from . import db
from .collector import Collector
from .bot import make_bot_and_dp
from .scheduler import setup_scheduler
from .tz_utils import canonical_tz_name

def parse_args():
  ap = argparse.ArgumentParser()
  ap.add_argument("--login", action="store_true", help="Run interactive Telethon login and exit.")
  return ap.parse_args()

async def run_login_only():
  db.init_db()
  db.backfill_user_timezone(canonical_tz_name(os.getenv("TZ", "UTC"), fallback_to_env=False))
  c = Collector()
  await c.ensure_login_interactive()
  print("✅ Telethon login OK. Session saved. Now run: docker compose up -d")
  await c.client.disconnect()

async def main():
  args = parse_args()

  for key in ("TG_API_ID", "TG_API_HASH", "BOT_TOKEN"):
    if not os.getenv(key):
      raise SystemExit(f"Missing env var {key}. Fill .env and run docker compose up.")

  db.init_db()
  db.backfill_user_timezone(canonical_tz_name(os.getenv("TZ", "UTC"), fallback_to_env=False))

  if args.login:
    await run_login_only()
    return

  bot, dp = make_bot_and_dp()

  stop_event = asyncio.Event()
  collector = Collector()

  scheduler = setup_scheduler(bot)
  scheduler.start()

  collector_task = asyncio.create_task(collector.loop(stop_event))
  bot_task = asyncio.create_task(dp.start_polling(bot))

  try:
    await asyncio.gather(collector_task, bot_task)
  finally:
    stop_event.set()
    scheduler.shutdown(wait=False)

if __name__ == "__main__":
  asyncio.run(main())

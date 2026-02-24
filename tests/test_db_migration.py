import importlib
import os
import sqlite3
import tempfile
import unittest


class TestDbMigration(unittest.TestCase):
  def test_init_db_adds_new_columns(self):
    with tempfile.TemporaryDirectory() as td:
      db_path = os.path.join(td, 'app.db')
      conn = sqlite3.connect(db_path)
      conn.executescript('''
        CREATE TABLE users (
          user_id INTEGER PRIMARY KEY,
          created_at TEXT NOT NULL DEFAULT (datetime('now')),
          hourly_enabled INTEGER NOT NULL DEFAULT 0,
          daily_enabled INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE user_channels (
          user_id INTEGER NOT NULL,
          username TEXT NOT NULL,
          scope TEXT NOT NULL DEFAULT 'daily'
        );
        CREATE TABLE posts (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          channel_username TEXT NOT NULL,
          msg_id INTEGER NOT NULL,
          date_utc TEXT NOT NULL,
          text TEXT NOT NULL,
          link TEXT NOT NULL,
          norm_hash TEXT NOT NULL,
          simhash INTEGER NOT NULL
        );
      ''')
      conn.commit()
      conn.close()

      os.environ['DB_PATH'] = db_path
      from tgnews import db as db_module
      importlib.reload(db_module)
      db_module.init_db()

      conn = sqlite3.connect(db_path)
      users_cols = [r[1] for r in conn.execute("PRAGMA table_info(users)").fetchall()]
      posts_cols = [r[1] for r in conn.execute("PRAGMA table_info(posts)").fetchall()]
      conn.close()

      self.assertIn('timezone', users_cols)
      self.assertIn('quiet_hours_enabled', users_cols)
      self.assertIn('is_forward', posts_cols)
      self.assertIn('url_hash', posts_cols)


if __name__ == '__main__':
  unittest.main()

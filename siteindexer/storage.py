from __future__ import annotations

import json
import sqlite3
import time
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Optional

_SOURCE_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_-]{2,32}$")

def validate_source_name(source_name: str) -> None:
    if not _SOURCE_RE.match(source_name or ""):
        raise ValueError("source_name must match ^[A-Za-z][A-Za-z0-9_-]{2,32}$")

SCHEMA = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS index_runs (
  id TEXT PRIMARY KEY,
  source_name TEXT NOT NULL,
  started_at INTEGER NOT NULL,
  finished_at INTEGER,
  root_url TEXT NOT NULL,
  scope_json TEXT NOT NULL,
  max_pages INTEGER NOT NULL,
  stats_json TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS pages (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  source_name TEXT NOT NULL,
  url TEXT NOT NULL,
  title TEXT,
  fetched_at INTEGER NOT NULL,
  status_code INTEGER,
  content_text TEXT,
  UNIQUE(source_name, url)
);

CREATE INDEX IF NOT EXISTS idx_pages_source ON pages(source_name);
CREATE INDEX IF NOT EXISTS idx_pages_source_fetched ON pages(source_name, fetched_at);

CREATE TABLE IF NOT EXISTS chunks (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  page_id INTEGER NOT NULL,
  chunk_index INTEGER NOT NULL,
  heading_path TEXT,
  text TEXT NOT NULL,
  FOREIGN KEY(page_id) REFERENCES pages(id)
);

CREATE VIRTUAL TABLE IF NOT EXISTS chunks_fts USING fts5(
  text,
  heading_path,
  url UNINDEXED,
  title UNINDEXED,
  source_name UNINDEXED
);

CREATE TABLE IF NOT EXISTS plans (
  plan_id TEXT PRIMARY KEY,
  source_name TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  root_url TEXT NOT NULL,
  scope_json TEXT NOT NULL,
  max_pages INTEGER NOT NULL,
  include_json TEXT NOT NULL,
  exclude_json TEXT NOT NULL,
  urls_json TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_plans_source_name ON plans(source_name);
"""



@dataclass(frozen=True)
class PageRow:
  id: int
  url: str
  title: Optional[str]
  fetched_at: int
  status_code: Optional[int]
  content_text: Optional[str]


class Storage:
  def __init__(self, db_path: str | Path):
    self.db_path = str(db_path)
    self._init_db()

  def _connect(self) -> sqlite3.Connection:
    conn = sqlite3.connect(self.db_path)
    conn.row_factory = sqlite3.Row
    return conn

  def _init_db(self) -> None:
    Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
    with self._connect() as conn:
      conn.executescript(SCHEMA)

  # ---------- Plans ----------
  def save_plan(
    self,
    plan_id: str,
    source_name: str,
    root_url: str,
    scope: dict[str, Any],
    max_pages: int,
    include: list[str],
    exclude: list[str],
    urls: list[str],
  ) -> None:
    validate_source_name(source_name)
    now = int(time.time())
    with self._connect() as conn:
      conn.execute(
        """
        INSERT OR REPLACE INTO plans(plan_id, source_name, created_at, root_url, scope_json, max_pages, include_json, exclude_json, urls_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
          plan_id,
          source_name,
          now,
          root_url,
          json.dumps(scope),
          int(max_pages),
          json.dumps(include),
          json.dumps(exclude),
          json.dumps(urls),
        ),
      )

  def load_plan(self, plan_id: str) -> dict[str, Any] | None:
    with self._connect() as conn:
      row = conn.execute("SELECT * FROM plans WHERE plan_id = ?", (plan_id,)).fetchone()
      if not row:
        return None
      return {
        "plan_id": row["plan_id"],
        "source_name": row["source_name"],
        "created_at": row["created_at"],
        "root_url": row["root_url"],
        "scope": json.loads(row["scope_json"]),
        "max_pages": row["max_pages"],
        "include": json.loads(row["include_json"]),
        "exclude": json.loads(row["exclude_json"]),
        "urls": json.loads(row["urls_json"]),
      }

  # ---------- Pages / Chunks ----------
  def upsert_page(self, source_name: str, url: str, title: str | None, status_code: int | None, content_text: str | None) -> int:
    validate_source_name(source_name)
    now = int(time.time())
    with self._connect() as conn:
      conn.execute(
        """
        INSERT INTO pages(source_name, url, title, fetched_at, status_code, content_text)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(source_name, url) DO UPDATE SET
          title=excluded.title,
          fetched_at=excluded.fetched_at,
          status_code=excluded.status_code,
          content_text=excluded.content_text
        """,
        (source_name, url, title, now, status_code, content_text),
      )
      page_id = conn.execute("SELECT id FROM pages WHERE source_name = ? AND url = ?", (source_name, url)).fetchone()["id"]
      return int(page_id)

  def replace_chunks(self, page_id: int, chunks: Iterable[tuple[int, str | None, str]]) -> None:
    with self._connect() as conn:
      # Find old chunk ids for this page so we can remove their FTS rows.
      old_ids = [
        int(r["id"])
        for r in conn.execute("SELECT id FROM chunks WHERE page_id = ?", (page_id,)).fetchall()
      ]
      if old_ids:
        qmarks = ",".join(["?"] * len(old_ids))
        conn.execute(f"DELETE FROM chunks_fts WHERE rowid IN ({qmarks})", old_ids)

      # Remove old chunks
      conn.execute("DELETE FROM chunks WHERE page_id = ?", (page_id,))

      # Grab page metadata for denormalized FTS fields
      prow = conn.execute("SELECT source_name, url, title FROM pages WHERE id = ?", (page_id,)).fetchone()
      page_source = prow["source_name"] if prow else None
      page_url = prow["url"] if prow else None
      page_title = prow["title"] if prow else None

      # Insert new chunks and mirror into FTS with rowid = chunk_id
      for (idx, heading, text) in chunks:
        cur = conn.execute(
          "INSERT INTO chunks(page_id, chunk_index, heading_path, text) VALUES (?, ?, ?, ?)",
          (page_id, idx, heading, text),
        )
        chunk_id = int(cur.lastrowid)

        conn.execute(
          "INSERT INTO chunks_fts(rowid, text, heading_path, url, title, source_name) VALUES (?, ?, ?, ?, ?, ?)",
          (chunk_id, text, heading, page_url, page_title, page_source),
        )

  def get_page(self, source_name: str, url: str) -> PageRow | None:
    validate_source_name(source_name)
    with self._connect() as conn:
      row = conn.execute("SELECT * FROM pages WHERE source_name = ? AND url = ?", (source_name, url)).fetchone()
      if not row:
        return None
      return PageRow(
        id=int(row["id"]),
        url=row["url"],
        title=row["title"],
        fetched_at=int(row["fetched_at"]),
        status_code=row["status_code"],
        content_text=row["content_text"],
      )

  def search_chunks(self, query: str, top_k: int = 5, source_name: str | None = None) -> list[dict[str, Any]]:
    # FTS5 query syntax is token-based; quoting makes it phrase search.
    # We will pass through what the caller gives us.
    if source_name:
      validate_source_name(source_name)
    
    with self._connect() as conn:
      try:
        if source_name:
          # Filter by source_name using the denormalized field in FTS
          rows = conn.execute(
            """
            SELECT
              c.id AS chunk_id,
              c.text AS chunk_text,
              c.heading_path AS heading_path,
              p.source_name AS source_name,
              p.url AS url,
              p.title AS title,
              p.fetched_at AS fetched_at,
              bm25(chunks_fts) AS score
            FROM chunks_fts
            JOIN chunks c ON c.id = chunks_fts.rowid
            JOIN pages p ON p.id = c.page_id
            WHERE chunks_fts MATCH ? AND p.source_name = ?
            ORDER BY score ASC
            LIMIT ?
            """,
            (query, source_name, int(top_k)),
          ).fetchall()
        else:
          rows = conn.execute(
            """
            SELECT
              c.id AS chunk_id,
              c.text AS chunk_text,
              c.heading_path AS heading_path,
              p.source_name AS source_name,
              p.url AS url,
              p.title AS title,
              p.fetched_at AS fetched_at,
              bm25(chunks_fts) AS score
            FROM chunks_fts
            JOIN chunks c ON c.id = chunks_fts.rowid
            JOIN pages p ON p.id = c.page_id
            WHERE chunks_fts MATCH ?
            ORDER BY score ASC
            LIMIT ?
            """,
            (query, int(top_k)),
          ).fetchall()
      except sqlite3.OperationalError:
        # Fallback if FTS5 isn't available for some reason
        q = f"%{query}%"
        if source_name:
          rows = conn.execute(
            """
            SELECT
              c.id AS chunk_id,
              c.text AS chunk_text,
              c.heading_path AS heading_path,
              p.source_name AS source_name,
              p.url AS url,
              p.title AS title,
              p.fetched_at AS fetched_at,
              0.0 AS score
            FROM chunks c
            JOIN pages p ON p.id = c.page_id
            WHERE c.text LIKE ? AND p.source_name = ?
            ORDER BY p.fetched_at DESC
            LIMIT ?
            """,
            (q, source_name, int(top_k)),
          ).fetchall()
        else:
          rows = conn.execute(
            """
            SELECT
              c.id AS chunk_id,
              c.text AS chunk_text,
              c.heading_path AS heading_path,
              p.source_name AS source_name,
              p.url AS url,
              p.title AS title,
              p.fetched_at AS fetched_at,
              0.0 AS score
            FROM chunks c
            JOIN pages p ON p.id = c.page_id
            WHERE c.text LIKE ?
            ORDER BY p.fetched_at DESC
            LIMIT ?
            """,
            (q, int(top_k)),
          ).fetchall()

      return [
        {
          "chunk_id": int(r["chunk_id"]),
          "text": r["chunk_text"],
          "heading_path": r["heading_path"],
          "source_name": r["source_name"],
          "url": r["url"],
          "title": r["title"],
          "fetched_at": int(r["fetched_at"]),
          "score": float(r["score"]),
        }
        for r in rows
      ]

  # ---------- Utility / Inspection ----------
  def list_tables(self) -> list[str]:
    """List all tables in the database."""
    with self._connect() as conn:
      rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
      ).fetchall()
      return [r["name"] for r in rows]

  def describe_table(self, table: str, limit: int = 5) -> dict[str, Any]:
    """Return table schema and sample rows."""
    with self._connect() as conn:
      # Get column info
      columns = conn.execute(f"PRAGMA table_info({table})").fetchall()
      col_info = [{"name": c["name"], "type": c["type"], "notnull": bool(c["notnull"])} for c in columns]
      
      # Get row count
      count_row = conn.execute(f"SELECT COUNT(*) as cnt FROM {table}").fetchone()
      row_count = int(count_row["cnt"]) if count_row else 0
      
      # Get sample rows
      sample_rows = conn.execute(f"SELECT * FROM {table} LIMIT ?", (limit,)).fetchall()
      samples = [dict(r) for r in sample_rows]
      
      return {
        "table": table,
        "columns": col_info,
        "row_count": row_count,
        "sample_rows": samples,
      }

  def list_sources(self) -> list[dict[str, Any]]:
    """List all indexed sources with stats."""
    with self._connect() as conn:
      rows = conn.execute(
        """
        SELECT 
          source_name,
          COUNT(*) as page_count,
          MIN(fetched_at) as first_fetched,
          MAX(fetched_at) as last_fetched
        FROM pages
        GROUP BY source_name
        ORDER BY source_name
        """
      ).fetchall()
      return [
        {
          "source_name": r["source_name"],
          "page_count": int(r["page_count"]),
          "first_fetched": int(r["first_fetched"]),
          "last_fetched": int(r["last_fetched"]),
        }
        for r in rows
      ]

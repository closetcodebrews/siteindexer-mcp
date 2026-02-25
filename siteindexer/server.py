from __future__ import annotations

import logging
import os
import time
import uuid
from typing import Any

import httpx
import trafilatura
from mcp.server.fastmcp import FastMCP

from .chunking import chunk_text
from .crawl import Scope, crawl_plan, fetch_html
from .storage import Storage

# IMPORTANT: For stdio servers, never write to stdout; use logging to stderr. :contentReference[oaicite:2]{index=2}
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("siteindexer")

mcp = FastMCP("siteindexer")

DEFAULT_DB = os.environ.get("SITEINDEXER_DB", os.path.join(".siteindexer", "siteindexer.db"))
storage = Storage(DEFAULT_DB)


def _new_id(prefix: str) -> str:
  return f"{prefix}_{uuid.uuid4().hex[:12]}"


@mcp.tool()
async def plan_index(
  source_name: str,
  url: str,
  scope_mode: str = "subpath",
  max_pages: int = 25,
  include: list[str] | None = None,
  exclude: list[str] | None = None,
) -> dict[str, Any]:
  """
  Plan an index run without fetching full content.
  Returns the exact list of URLs the server intends to fetch.
  
  Args:
    source_name: A unique identifier for this documentation source (e.g. "mcp-docs", "react-docs").
                 Must be 3-32 chars, start with letter, contain only letters/numbers/underscores/hyphens.
    url: The root URL to start indexing from.
    scope_mode: "page" (single page), "subpath" (pages under this path), or "domain" (entire domain).
    max_pages: Maximum number of pages to index.
    include: Optional list of regex patterns - only URLs matching these will be included.
    exclude: Optional list of regex patterns - URLs matching these will be excluded.
  """
  include = include or []
  exclude = exclude or []
  scope = Scope(mode=scope_mode, root_url=url)

  planned_urls = await crawl_plan(url, scope, max_pages, include, exclude)
  plan_id = _new_id("plan")

  storage.save_plan(
    plan_id=plan_id,
    source_name=source_name,
    root_url=url,
    scope={"mode": scope_mode, "root_url": url},
    max_pages=max_pages,
    include=include,
    exclude=exclude,
    urls=planned_urls,
  )

  return {
    "plan_id": plan_id,
    "source_name": source_name,
    "root_url": url,
    "scope_mode": scope_mode,
    "max_pages": max_pages,
    "include": include,
    "exclude": exclude,
    "url_count": len(planned_urls),
    "urls": planned_urls,
    "note": "Approval gate: review URLs, then call run_index(plan_id).",
  }


@mcp.tool()
async def run_index(plan_id: str) -> dict[str, Any]:
  """
  Execute an approved plan: fetch -> extract -> chunk -> store.
  """
  plan = storage.load_plan(plan_id)
  if not plan:
    return {"ok": False, "error": f"Unknown plan_id: {plan_id}"}

  source_name: str = plan["source_name"]
  urls: list[str] = plan["urls"]
  started = int(time.time())
  fetched = 0
  stored_pages = 0
  stored_chunks = 0
  failures: list[dict[str, Any]] = []

  async with httpx.AsyncClient(headers={"User-Agent": "siteindexer/0.1"}) as client:
    for u in urls:
      status, html = await fetch_html(client, u)
      fetched += 1

      if not html:
        failures.append({"url": u, "status_code": status, "error": "No HTML fetched"})
        storage.upsert_page(source_name=source_name, url=u, title=None, status_code=status, content_text=None)
        continue

      extracted = trafilatura.extract(html, include_comments=False, include_tables=True)
      extracted = (extracted or "").strip()
      title = trafilatura.extract_metadata(html).title if trafilatura.extract_metadata(html) else None

      page_id = storage.upsert_page(source_name=source_name, url=u, title=title, status_code=status, content_text=extracted)

      chunks = chunk_text(extracted)
      storage.replace_chunks(
        page_id,
        [(c.index, c.heading_path, c.text) for c in chunks],
      )
      stored_pages += 1
      stored_chunks += len(chunks)

  finished = int(time.time())
  return {
    "ok": True,
    "plan_id": plan_id,
    "source_name": source_name,
    "started_at": started,
    "finished_at": finished,
    "duration_sec": finished - started,
    "urls_planned": len(urls),
    "urls_fetched": fetched,
    "pages_stored": stored_pages,
    "chunks_stored": stored_chunks,
    "failures": failures[:20],
    "next": "Use search(query) to retrieve passages with citations.",
  }

@mcp.tool()
async def search(query: str, top_k: int = 5, source_name: str | None = None) -> dict[str, Any]:
  """
  Search indexed documentation using full-text search (BM25).
  
  Args:
    query: The search query string.
    top_k: Maximum number of results to return.
    source_name: Optional - filter results to a specific source. If not provided, searches all sources.
  """
  hits = storage.search_chunks(query=query, top_k=top_k, source_name=source_name)
  return {
    "query": query,
    "top_k": top_k,
    "source_name": source_name,
    "hits": hits,
  }

@mcp.tool()
async def get_page(source_name: str, url: str) -> dict[str, Any]:
  """
  Return stored page content (not live fetch).
  
  Args:
    source_name: The source identifier to look up the page in.
    url: The URL of the page to retrieve.
  """
  row = storage.get_page(source_name, url)
  if not row:
    return {"ok": False, "error": "Not indexed", "source_name": source_name, "url": url}
  return {
    "ok": True,
    "source_name": source_name,
    "url": row.url,
    "title": row.title,
    "fetched_at": row.fetched_at,
    "status_code": row.status_code,
    "content_text": row.content_text,
  }


@mcp.tool()
async def refresh(source_name: str, url: str, scope_mode: str = "subpath", max_pages: int = 25) -> dict[str, Any]:
  """
  Convenience: plan + run in one call (you can keep this disabled if you prefer strict approval).
  
  Args:
    source_name: A unique identifier for this documentation source.
    url: The root URL to start indexing from.
    scope_mode: "page", "subpath", or "domain".
    max_pages: Maximum number of pages to index.
  """
  p = await plan_index(source_name=source_name, url=url, scope_mode=scope_mode, max_pages=max_pages)
  # In a strict approval setup, you would NOT auto-run here.
  r = await run_index(plan_id=p["plan_id"])
  return {"plan": p, "run": r}
  
@mcp.tool()
async def db_list_tables() -> dict[str, Any]:
  return {"tables": storage.list_tables()}


@mcp.tool()
async def db_describe_table(table: str, limit: int = 5) -> dict[str, Any]:
  return storage.describe_table(table, limit=limit)


@mcp.tool()
async def list_sources() -> dict[str, Any]:
  return {"sources": storage.list_sources()}


def main() -> None:
  # Initialize and run the server with stdio transport. :contentReference[oaicite:3]{index=3}
  mcp.run(transport="stdio")


if __name__ == "__main__":
  main()

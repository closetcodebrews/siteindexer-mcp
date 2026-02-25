from __future__ import annotations

import gzip
import re
from dataclasses import dataclass
from typing import Optional
from urllib.parse import urljoin, urldefrag, urlparse

import httpx
from xml.etree import ElementTree as ET


@dataclass(frozen=True)
class Scope:
  mode: str  # "page" | "subpath" | "domain"
  root_url: str

  @staticmethod
  def from_dict(d: dict) -> "Scope":
    return Scope(mode=str(d.get("mode", "page")), root_url=str(d["root_url"]))


def _same_origin(a: str, b: str) -> bool:
  pa, pb = urlparse(a), urlparse(b)
  return (pa.scheme, pa.netloc) == (pb.scheme, pb.netloc)


def _normalize(url: str) -> str:
  url, _frag = urldefrag(url)
  return url.rstrip("/")


def in_scope(url: str, scope: Scope) -> bool:
  url = _normalize(url)
  root = _normalize(scope.root_url)

  if scope.mode == "page":
    return url == root

  if scope.mode == "domain":
    return _same_origin(url, root)

  if scope.mode == "subpath":
    if not _same_origin(url, root):
      return False
    return url.startswith(root)

  return False


def matches_rules(url: str, include: list[str], exclude: list[str]) -> bool:
  for pat in exclude:
    if re.search(pat, url):
      return False
  if not include:
    return True
  return any(re.search(pat, url) for pat in include)


async def fetch_html(client: httpx.AsyncClient, url: str) -> tuple[int | None, str | None]:
  try:
    r = await client.get(url, timeout=30.0, follow_redirects=True)
    ct = (r.headers.get("content-type") or "").lower()
    if "text/html" not in ct and "application/xhtml+xml" not in ct:
      return r.status_code, None
    return r.status_code, r.text
  except Exception:
    return None, None


def extract_links(base_url: str, html: str) -> list[str]:
  hrefs = re.findall(r'href=["\'](.*?)["\']', html, flags=re.IGNORECASE)
  out: list[str] = []
  for h in hrefs:
    if not h or h.startswith("#") or h.startswith("mailto:") or h.startswith("javascript:"):
      continue
    u = urljoin(base_url, h)
    out.append(_normalize(u))
  seen = set()
  deduped = []
  for u in out:
    if u not in seen:
      seen.add(u)
      deduped.append(u)
  return deduped


# ----------------------------
# Sitemap discovery + parsing
# ----------------------------

def _sitemap_candidates(root_url: str) -> list[str]:
  """
  Common sitemap locations. We try these before robots.txt.
  """
  p = urlparse(root_url)
  base = f"{p.scheme}://{p.netloc}"
  return [
    f"{base}/sitemap.xml",
    f"{base}/sitemap_index.xml",
    f"{base}/sitemapindex.xml",
    f"{base}/sitemap.xml.gz",
    f"{base}/sitemap_index.xml.gz",
    f"{base}/sitemapindex.xml.gz",
  ]


async def _fetch_bytes(client: httpx.AsyncClient, url: str) -> tuple[int | None, bytes | None, str | None]:
  try:
    r = await client.get(url, timeout=30.0, follow_redirects=True)
    return r.status_code, r.content, (r.headers.get("content-type") or "")
  except Exception:
    return None, None, None


def _maybe_decompress(url: str, data: bytes) -> bytes:
  if url.lower().endswith(".gz"):
    return gzip.decompress(data)
  return data


def _parse_sitemap_xml(xml_bytes: bytes) -> tuple[list[str], list[str]]:
  """
  Returns (urls, sitemap_children).
  Supports both:
    - <urlset> (leaf sitemap)
    - <sitemapindex> (index of sitemaps)
  """
  try:
    root = ET.fromstring(xml_bytes)
  except ET.ParseError:
    return ([], [])

  # Strip namespace by checking tag endings
  tag = root.tag.lower()
  urls: list[str] = []
  children: list[str] = []

  if tag.endswith("urlset"):
    for url_el in root.findall(".//{*}url"):
      loc = url_el.find("{*}loc")
      if loc is not None and loc.text:
        urls.append(loc.text.strip())
    return (urls, [])

  if tag.endswith("sitemapindex"):
    for sm_el in root.findall(".//{*}sitemap"):
      loc = sm_el.find("{*}loc")
      if loc is not None and loc.text:
        children.append(loc.text.strip())
    return ([], children)

  # Unknown structure
  return ([], [])


async def _robots_sitemaps(client: httpx.AsyncClient, root_url: str) -> list[str]:
  """
  Parse robots.txt for Sitemap: directives.
  """
  p = urlparse(root_url)
  robots_url = f"{p.scheme}://{p.netloc}/robots.txt"
  status, data, _ct = await _fetch_bytes(client, robots_url)
  if not data or status is None or status >= 400:
    return []

  text = data.decode("utf-8", errors="ignore")
  sitemaps: list[str] = []
  for line in text.splitlines():
    line = line.strip()
    if not line:
      continue
    if line.lower().startswith("sitemap:"):
      sm = line.split(":", 1)[1].strip()
      if sm:
        sitemaps.append(sm)
  # de-dupe preserving order
  seen = set()
  out = []
  for u in sitemaps:
    if u not in seen:
      seen.add(u)
      out.append(u)
  return out


async def discover_sitemap_urls(
  client: httpx.AsyncClient,
  root_url: str,
  max_sitemaps: int = 25,
) -> list[str]:
  """
  Return a list of sitemap URLs to process. Tries common paths, then robots.txt.
  """
  found: list[str] = []

  # 1) Common candidates
  for cand in _sitemap_candidates(root_url):
    status, data, ct = await _fetch_bytes(client, cand)
    if status and 200 <= status < 300 and data and len(data) > 0:
      found.append(cand)

  # 2) robots.txt directives (add them if any)
  for sm in await _robots_sitemaps(client, root_url):
    found.append(sm)

  # de-dupe and cap
  seen = set()
  out: list[str] = []
  for u in found:
    if u not in seen:
      seen.add(u)
      out.append(u)
    if len(out) >= max_sitemaps:
      break
  return out


async def collect_urls_from_sitemaps(
  client: httpx.AsyncClient,
  sitemap_urls: list[str],
  scope: Scope,
  include: list[str],
  exclude: list[str],
  max_pages: int,
  max_depth: int = 3,
) -> list[str]:
  """
  Recursively process sitemap indexes and collect in-scope URLs.
  """
  planned: list[str] = []
  seen_urls: set[str] = set()
  seen_sitemaps: set[str] = set()

  # BFS over sitemap graph
  queue: list[tuple[str, int]] = [(u, 0) for u in sitemap_urls]

  while queue and len(planned) < max_pages:
    sm_url, depth = queue.pop(0)
    if sm_url in seen_sitemaps:
      continue
    seen_sitemaps.add(sm_url)

    if depth > max_depth:
      continue

    status, data, _ct = await _fetch_bytes(client, sm_url)
    if not data or status is None or status >= 400:
      continue

    try:
      xml_bytes = _maybe_decompress(sm_url, data)
    except Exception:
      continue

    urls, child_sitemaps = _parse_sitemap_xml(xml_bytes)

    # Add leaf URLs
    for u in urls:
      u = _normalize(u)
      if u in seen_urls:
        continue
      if not in_scope(u, scope):
        continue
      if not matches_rules(u, include, exclude):
        continue
      seen_urls.add(u)
      planned.append(u)
      if len(planned) >= max_pages:
        break

    # Enqueue child sitemaps
    if child_sitemaps and depth < max_depth:
      for child in child_sitemaps:
        if child not in seen_sitemaps:
          queue.append((child, depth + 1))

  return planned


# ----------------------------
# Crawl planning entrypoint
# ----------------------------

async def crawl_plan(
  root_url: str,
  scope: Scope,
  max_pages: int,
  include: list[str],
  exclude: list[str],
) -> list[str]:
  """
  Plan URLs using sitemap discovery when available; otherwise fall back to link crawl.
  """
  root_url = _normalize(root_url)
  queue = [root_url]
  seen = set([root_url])
  planned: list[str] = []

  async with httpx.AsyncClient(headers={"User-Agent": "siteindexer/0.1"}) as client:
    # 1) Try sitemap-based discovery first (fast + complete)
    sitemap_urls = await discover_sitemap_urls(client, root_url)
    if sitemap_urls:
      planned = await collect_urls_from_sitemaps(
        client=client,
        sitemap_urls=sitemap_urls,
        scope=scope,
        include=include,
        exclude=exclude,
        max_pages=max_pages,
      )
      if planned:
        return planned

    # 2) Fall back to link-based crawl
    while queue and len(planned) < max_pages:
      url = queue.pop(0)
      if not in_scope(url, scope):
        continue
      if not matches_rules(url, include, exclude):
        continue

      planned.append(url)

      if scope.mode == "page":
        continue

      status, html = await fetch_html(client, url)
      if not html:
        continue

      for link in extract_links(url, html):
        if link in seen:
          continue
        if not in_scope(link, scope):
          continue
        if not matches_rules(link, include, exclude):
          continue
        seen.add(link)
        queue.append(link)

  return planned

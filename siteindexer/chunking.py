from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable


@dataclass(frozen=True)
class Chunk:
  index: int
  heading_path: str | None
  text: str


def chunk_text(text: str, max_chars: int = 1400) -> list[Chunk]:
  """
  MVP chunker: splits into roughly max_chars windows on paragraph boundaries.
  Later: replace with heading-aware chunking from parsed HTML.
  """
  text = (text or "").strip()
  if not text:
    return []

  paras = [p.strip() for p in text.split("\n") if p.strip()]
  chunks: list[Chunk] = []

  buf: list[str] = []
  buf_len = 0
  idx = 0

  def flush() -> None:
    nonlocal idx, buf, buf_len
    if not buf:
      return
    chunks.append(Chunk(index=idx, heading_path=None, text="\n".join(buf).strip()))
    idx += 1
    buf = []
    buf_len = 0

  for p in paras:
    if buf_len + len(p) + 1 > max_chars and buf:
      flush()
    buf.append(p)
    buf_len += len(p) + 1

  flush()
  return chunks

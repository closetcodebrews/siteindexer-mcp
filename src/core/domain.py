from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class Scope:
  mode: str  # "page" | "subpath" | "domain"
  root_url: str

  @staticmethod
  def from_dict(d: dict) -> "Scope":
    return Scope(mode=str(d.get("mode", "page")), root_url=str(d["root_url"]))


@dataclass(frozen=True)
class PageRow:
  id: int
  url: str
  title: Optional[str]
  fetched_at: int
  status_code: Optional[int]
  content_text: Optional[str]


@dataclass(frozen=True)
class Chunk:
  index: int
  heading_path: Optional[str]
  text: str

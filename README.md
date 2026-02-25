# SiteIndexer (MCP)

## Run locally
```bash
uv venv
source .venv/bin/activate
uv run -m siteindexer.server
```

## Tools

- siteindexer.plan_index
- siteindexer.run_index
- siteindexer.search
- siteindexer.get_page
- siteindexer.refresh

## Storage

SQLite db at ./.siteindexer/siteindexer.db (override with SITEINDEXER_DB).


To delete storage and start fresh: 

```bash
rm -rf .siteindexer
```

## Running the project

```bash
uv run -m siteindexer.server
```

## Creating this project
This follows the MCP “FastMCP + stdio transport” approach.

```sh

mkdir siteindexer-mcp
cd siteindexer-mcp

uv init
uv venv
source .venv/bin/activate

uv add "mcp[cli]" httpx trafilatura
```

## 2. File layout
siteindexer-mcp/
  siteindexer/
    __init__.py
    server.py
    storage.py
    crawl.py
    chunking.py
  README.md

import os
import logging
from pathlib import Path

# Setup global logging
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("siteindexer")

DEFAULT_DB = os.environ.get("SITEINDEXER_DB", os.path.join(".siteindexer", "siteindexer.db"))

import os

OURA_ACCESS_TOKEN = os.environ["OURA_ACCESS_TOKEN"]
DATABASE_URL = os.environ["DATABASE_URL"]
OURA_API_BASE = "https://api.ouraring.com"

# For first-time full sync, how far back to go
FULL_SYNC_START_DATE = os.environ.get("OURA_FULL_SYNC_START_DATE", "2020-01-01")

# Oura may retroactively update records (e.g. recalculated scores).
# The API has no "updated_since" filter, so we re-fetch this many days
# of overlap on each run to catch backfilled / revised data.
OVERLAP_DAYS = int(os.environ.get("OURA_OVERLAP_DAYS", "30"))

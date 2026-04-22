from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone

from . import api, config, db

log = logging.getLogger(__name__)

# Endpoints that use start_date / end_date parameters
DOCUMENT_ENDPOINTS = [
    "tag",
    "enhanced_tag",
    "workout",
    "session",
    "daily_activity",
    "daily_sleep",
    "daily_spo2",
    "daily_readiness",
    "sleep",
    "sleep_time",
    "rest_mode_period",
    "daily_stress",
    "daily_resilience",
    "daily_cardiovascular_age",
    "vO2_max",
]

# ring_configuration has no date filter, always fetched in full
NO_DATE_FILTER_ENDPOINTS = ["ring_configuration"]

# Endpoints that use start_datetime / end_datetime (time-series)
TIMESERIES_ENDPOINTS = ["heartrate", "ring_battery_level", "interbeat_interval"]

# Map API endpoint name to DB table name
TABLE_MAP = {"vO2_max": "vo2_max"}


def _table(endpoint: str) -> str:
    return TABLE_MAP.get(endpoint, endpoint)


def _today() -> str:
    return date.today().isoformat()


def _sync_start(conn, endpoint: str) -> str:
    """Return the start date for this sync run.

    If we have synced before, go back OVERLAP_DAYS from that point to
    catch any records Oura may have retroactively updated.  The API
    filters by the record's own ``day``, not by ``updated_at``, so an
    overlap window is the only way to pick up backfilled changes.
    """
    last = db.get_last_sync(conn, endpoint)
    if last:
        d = date.fromisoformat(last) - timedelta(days=config.OVERLAP_DAYS)
        return max(d, date.fromisoformat(config.FULL_SYNC_START_DATE)).isoformat()
    return config.FULL_SYNC_START_DATE


def sync_personal_info(client, conn) -> None:
    log.info("Syncing personal_info")
    data = api.fetch_personal_info(client)
    doc_id = data.get("id", "me")
    db.upsert_document(conn, "personal_info", doc_id, data)
    log.info("personal_info saved")


def sync_document_endpoint(client, conn, endpoint: str) -> None:
    start = _sync_start(conn, endpoint)
    end = _today()
    table = _table(endpoint)
    log.info("Syncing %s from %s to %s", endpoint, start, end)

    count = 0
    for doc in api.fetch_documents(client, endpoint, start_date=start, end_date=end):
        doc_id = doc.get("id")
        if not doc_id:
            log.warning("Skipping document without id in %s", endpoint)
            continue
        db.upsert_document(conn, table, doc_id, doc)
        count += 1

    db.set_last_sync(conn, endpoint, end)
    log.info("Synced %d documents for %s", count, endpoint)


def sync_no_date_endpoint(client, conn, endpoint: str) -> None:
    table = _table(endpoint)
    log.info("Syncing %s (full)", endpoint)

    count = 0
    for doc in api.fetch_documents(client, endpoint):
        doc_id = doc.get("id")
        if not doc_id:
            continue
        db.upsert_document(conn, table, doc_id, doc)
        count += 1

    db.set_last_sync(conn, endpoint, _today())
    log.info("Synced %d documents for %s", count, endpoint)


def sync_timeseries_endpoint(client, conn, endpoint: str) -> None:
    start_date = _sync_start(conn, endpoint)
    start_dt = start_date + "T00:00:00+00:00"

    end_dt = datetime.now(timezone.utc).isoformat()
    table = _table(endpoint)
    log.info("Syncing %s from %s to %s", endpoint, start_dt, end_dt)

    count = 0
    for row in api.fetch_timeseries(client, endpoint, start_datetime=start_dt, end_datetime=end_dt):
        db.upsert_timeseries(conn, table, row)
        count += 1

    db.set_last_sync(conn, endpoint, _today())
    log.info("Synced %d rows for %s", count, endpoint)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    conn = db.connect(config.DATABASE_URL)
    try:
        db.init_schema(conn)
        client = api.new_client()
        try:
            sync_personal_info(client, conn)

            for endpoint in DOCUMENT_ENDPOINTS:
                try:
                    sync_document_endpoint(client, conn, endpoint)
                except Exception:
                    log.exception("Failed to sync %s", endpoint)

            for endpoint in NO_DATE_FILTER_ENDPOINTS:
                try:
                    sync_no_date_endpoint(client, conn, endpoint)
                except Exception:
                    log.exception("Failed to sync %s", endpoint)

            for endpoint in TIMESERIES_ENDPOINTS:
                try:
                    sync_timeseries_endpoint(client, conn, endpoint)
                except Exception:
                    log.exception("Failed to sync %s", endpoint)
        finally:
            client.close()
    finally:
        conn.close()

    log.info("Sync complete")


if __name__ == "__main__":
    main()

from __future__ import annotations

import logging
import time
from typing import Any, Iterator

import httpx

from . import config

log = logging.getLogger(__name__)

RATE_LIMIT_WAIT = 60  # seconds to wait on 429


def _client() -> httpx.Client:
    return httpx.Client(
        base_url=config.OURA_API_BASE,
        headers={"Authorization": f"Bearer {config.OURA_ACCESS_TOKEN}"},
        timeout=30.0,
    )


def _request_with_retry(client: httpx.Client, url: str, params: dict) -> httpx.Response:
    while True:
        resp = client.get(url, params={k: v for k, v in params.items() if v is not None})
        if resp.status_code == 429:
            log.warning("Rate limited, waiting %ds...", RATE_LIMIT_WAIT)
            time.sleep(RATE_LIMIT_WAIT)
            continue
        resp.raise_for_status()
        return resp


def fetch_personal_info(client: httpx.Client) -> dict:
    resp = _request_with_retry(client, "/v2/usercollection/personal_info", {})
    return resp.json()


def fetch_documents(
    client: httpx.Client,
    endpoint: str,
    start_date: str | None = None,
    end_date: str | None = None,
) -> Iterator[dict]:
    """Fetch all documents from a paginated endpoint."""
    params: dict[str, Any] = {
        "start_date": start_date,
        "end_date": end_date,
    }
    url = f"/v2/usercollection/{endpoint}"

    while True:
        resp = _request_with_retry(client, url, params)
        body = resp.json()
        for doc in body.get("data", []):
            yield doc
        next_token = body.get("next_token")
        if not next_token:
            break
        params["next_token"] = next_token


def fetch_timeseries(
    client: httpx.Client,
    endpoint: str,
    start_datetime: str | None = None,
    end_datetime: str | None = None,
) -> Iterator[dict]:
    """Fetch all rows from a time-series endpoint."""
    params: dict[str, Any] = {
        "start_datetime": start_datetime,
        "end_datetime": end_datetime,
    }
    url = f"/v2/usercollection/{endpoint}"

    while True:
        resp = _request_with_retry(client, url, params)
        body = resp.json()
        for row in body.get("data", []):
            yield row
        next_token = body.get("next_token")
        if not next_token:
            break
        params["next_token"] = next_token


def new_client() -> httpx.Client:
    return _client()

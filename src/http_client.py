"""
http_client.py — Shared httpx client with retry + exponential backoff.
"""
from __future__ import annotations

import time
import logging
from typing import Optional, Tuple

import httpx
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception,
    before_sleep_log,
)

from src.config import (
    HEADERS,
    REQUEST_TIMEOUT,
    MAX_RETRIES,
    BACKOFF_BASE,
    REQUEST_DELAY,
)

logger = logging.getLogger(__name__)

# Module-level client (reused across calls for connection pooling)
_client: Optional[httpx.Client] = None


def get_client() -> httpx.Client:
    global _client
    if _client is None or _client.is_closed:
        _client = httpx.Client(
            headers=HEADERS,
            timeout=REQUEST_TIMEOUT,
            follow_redirects=True,
            verify=False,        # some Italian sites have bad certs
        )
    return _client


def _should_retry(exc: BaseException) -> bool:
    if isinstance(exc, httpx.HTTPStatusError):
        # Don't retry 4xx errors except 429 Too Many Requests
        return exc.response.status_code >= 500 or exc.response.status_code == 429
    return isinstance(exc, (httpx.TimeoutException, httpx.NetworkError, Exception))


def close_client() -> None:
    global _client
    if _client and not _client.is_closed:
        _client.close()
    _client = None


@retry(
    stop=stop_after_attempt(MAX_RETRIES),
    wait=wait_exponential(multiplier=BACKOFF_BASE, min=BACKOFF_BASE, max=30),
    retry=retry_if_exception(_should_retry),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True,
)
def fetch(url: str, timeout: Optional[int] = REQUEST_TIMEOUT) -> Tuple[str, str, int]:
    """
    Fetch a URL with retry + backoff.

    Returns:
        (html_text, final_url, status_code)
    Raises:
        httpx.HTTPError after MAX_RETRIES failures
    """
    client = get_client()
    time.sleep(REQUEST_DELAY)
    if timeout is None:
        timeout = REQUEST_TIMEOUT
    resp = client.get(url, timeout=timeout)
    resp.raise_for_status()
    return resp.text, str(resp.url), resp.status_code


def safe_fetch(url: str, timeout: Optional[int] = REQUEST_TIMEOUT) -> Tuple[Optional[str], Optional[str], Optional[int]]:
    """
    Wrapper around fetch() that catches all exceptions and returns None tuple.
    Callers must check for None.
    """
    try:
        return fetch(url, timeout=timeout)
    except Exception as exc:
        logger.warning("safe_fetch failed for %s: %s", url, exc)
        return None, None, None

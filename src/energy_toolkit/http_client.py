"""Resilient HTTP client: retries with exponential backoff on 429/5xx."""
import logging

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

LOGGER = logging.getLogger("energy_toolkit.http")
DEFAULT_TIMEOUT_SECONDS = 60
USER_AGENT = "eu-energy-prices-fabric/1.0 (+Microsoft Fabric tutorial)"


def build_session(total_retries: int = 5, backoff_factor: float = 1.5) -> requests.Session:
    retry_policy = Retry(
        total=total_retries,
        backoff_factor=backoff_factor,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET"}),
        respect_retry_after_header=True,
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry_policy))
    session.headers.update({"User-Agent": USER_AGENT, "Accept": "application/json"})
    return session


def get_json(session: requests.Session, url: str, params: dict) -> tuple[dict, str]:
    """Return (parsed_json, raw_text). The raw text is landed as-is in bronze."""
    response = session.get(url, params=params, timeout=DEFAULT_TIMEOUT_SECONDS)
    LOGGER.info("GET %s -> HTTP %s", response.url, response.status_code)
    response.raise_for_status()
    return response.json(), response.text

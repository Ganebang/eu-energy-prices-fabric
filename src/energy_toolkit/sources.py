"""Clients for the public data sources used by the project.

* Energy-Charts (Fraunhofer ISE) - EPEX Spot day-ahead prices + public net power.
  Docs: https://api.energy-charts.info  (CC BY 4.0, no API key)
* Open-Meteo - hourly weather (history + forecast). Docs: https://open-meteo.com
  (CC BY 4.0, no API key, non-commercial use)
"""
from datetime import date, datetime, timezone

import requests

from .http_client import get_json

ENERGY_CHARTS_BASE_URL = "https://api.energy-charts.info"
OPEN_METEO_FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
WEATHER_VARIABLES = ("temperature_2m", "cloud_cover", "wind_speed_100m", "shortwave_radiation")


def _utc(epoch_seconds: int) -> datetime:
    return datetime.fromtimestamp(epoch_seconds, tz=timezone.utc).replace(tzinfo=None)


def fetch_day_ahead_prices(session: requests.Session, zone_code: str,
                           start_date: date, end_date: date) -> tuple[list[tuple], str]:
    """Day-ahead prices for one bidding zone. Resolution is 15 min since the
    EPEX switch to 15-minute MTU (1 Oct 2025), 60 min before.

    Returns rows (interval_start_utc, price_eur_mwh, unit) and the raw payload.
    """
    payload, raw_text = get_json(
        session,
        f"{ENERGY_CHARTS_BASE_URL}/price",
        {"bzn": zone_code, "start": start_date.isoformat(), "end": end_date.isoformat()},
    )
    timestamps = payload.get("unix_seconds") or []
    prices = payload.get("price") or []
    unit = payload.get("unit", "EUR / MWh")
    rows = [(_utc(ts), float(p), unit) for ts, p in zip(timestamps, prices) if p is not None]
    return rows, raw_text


def fetch_public_power(session: requests.Session, country_code: str,
                       start_date: date, end_date: date) -> tuple[list[tuple], str]:
    """Net public electricity generation per production type (MW).

    Returns long-format rows (interval_start_utc, production_type, power_mw)."""
    payload, raw_text = get_json(
        session,
        f"{ENERGY_CHARTS_BASE_URL}/public_power",
        {"country": country_code, "start": start_date.isoformat(), "end": end_date.isoformat()},
    )
    timestamps = payload.get("unix_seconds") or []
    rows: list[tuple] = []
    for production_type in payload.get("production_types", []):
        type_name = production_type.get("name")
        for ts, value in zip(timestamps, production_type.get("data") or []):
            if value is not None:
                rows.append((_utc(ts), type_name, float(value)))
    return rows, raw_text


def fetch_hourly_weather(session: requests.Session, latitude: float, longitude: float,
                         past_days: int, forecast_days: int) -> tuple[list[tuple], str]:
    """Hourly weather in UTC. Returns rows (interval_start_utc, t2m, cloud, wind100, ghi)."""
    payload, raw_text = get_json(
        session,
        OPEN_METEO_FORECAST_URL,
        {
            "latitude": latitude,
            "longitude": longitude,
            "hourly": ",".join(WEATHER_VARIABLES),
            "past_days": past_days,
            "forecast_days": forecast_days,
            "timezone": "UTC",
            "wind_speed_unit": "kmh",
        },
    )
    hourly = payload.get("hourly", {})
    times = hourly.get("time", [])
    columns = [hourly.get(name, [None] * len(times)) for name in WEATHER_VARIABLES]
    rows = []
    for index, iso_time in enumerate(times):
        values = [col[index] for col in columns]
        rows.append((datetime.fromisoformat(iso_time), *[None if v is None else float(v) for v in values]))
    return rows, raw_text

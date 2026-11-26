"""Parsing tests for the API clients, with a fake HTTP session (no network)."""
import json
from datetime import date, datetime

from energy_toolkit import sources


class FakeResponse:
    def __init__(self, payload):
        self._payload = payload
        self.text = json.dumps(payload)
        self.status_code = 200
        self.url = "https://example.test"

    def json(self):
        return self._payload

    def raise_for_status(self):
        return None


class FakeSession:
    def __init__(self, payload):
        self.payload = payload
        self.calls = []

    def get(self, url, params, timeout):
        self.calls.append((url, params))
        return FakeResponse(self.payload)


def test_day_ahead_prices_skip_missing_values():
    session = FakeSession({"unix_seconds": [1767225600, 1767226500, 1767227400],
                           "price": [85.2, None, -3.1], "unit": "EUR / MWh"})
    rows, raw = sources.fetch_day_ahead_prices(session, "FR", date(2026, 1, 1), date(2026, 1, 1))

    assert rows == [(datetime(2026, 1, 1, 0, 0), 85.2, "EUR / MWh"),
                    (datetime(2026, 1, 1, 0, 30), -3.1, "EUR / MWh")]
    assert json.loads(raw)["price"][1] is None
    url, params = session.calls[0]
    assert url.endswith("/price") and params["bzn"] == "FR"


def test_public_power_is_returned_in_long_format():
    session = FakeSession({"unix_seconds": [1767225600, 1767226500],
                           "production_types": [{"name": "Solar", "data": [0.0, 12.5]},
                                                {"name": "Wind onshore", "data": [None, 3100.0]}]})
    rows, _ = sources.fetch_public_power(session, "fr", date(2026, 1, 1), date(2026, 1, 1))

    assert ("Solar" in {r[1] for r in rows}) and len(rows) == 3
    assert (datetime(2026, 1, 1, 0, 15), "Wind onshore", 3100.0) in rows


def test_weather_keeps_nulls_and_converts_to_float():
    session = FakeSession({"hourly": {"time": ["2026-01-01T00:00", "2026-01-01T01:00"],
                                      "temperature_2m": [3, 2.5], "cloud_cover": [100, None],
                                      "wind_speed_100m": [20.1, 22.0], "shortwave_radiation": [0, 0]}})
    rows, _ = sources.fetch_hourly_weather(session, 46.6, 2.4, past_days=1, forecast_days=1)

    assert rows[0] == (datetime(2026, 1, 1, 0, 0), 3.0, 100.0, 20.1, 0.0)
    assert rows[1][2] is None
    assert session.calls[0][1]["timezone"] == "UTC"

import pytest

from energy_toolkit.reference import BIDDING_ZONES, get_zone, zones_as_rows

FOLLOWED_ZONES = ["FR", "BE", "DE-LU", "NL", "ES"]


@pytest.mark.parametrize("zone_code", FOLLOWED_ZONES)
def test_followed_zones_are_defined(zone_code):
    zone = get_zone(zone_code)
    assert zone.zone_code == zone_code
    assert zone.market_timezone.startswith("Europe/")


def test_unknown_zone_lists_the_known_codes():
    with pytest.raises(ValueError, match="Known zones"):
        get_zone("XX")


def test_coordinates_are_inside_europe():
    for zone in BIDDING_ZONES.values():
        assert 35 < zone.latitude < 72
        assert -10 < zone.longitude < 32


def test_rows_match_the_silver_dim_bidding_zone_columns():
    expected = {"zone_code", "country_name", "country_iso2", "energy_charts_country",
                "region_name", "market_timezone", "latitude", "longitude"}
    rows = zones_as_rows()
    assert len(rows) == len(BIDDING_ZONES)
    assert all(set(row) == expected for row in rows)

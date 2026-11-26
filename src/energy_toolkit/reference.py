"""Reference data for the bidding zones (EPEX day-ahead market areas)."""
from dataclasses import asdict, dataclass


@dataclass(frozen=True)
class BiddingZone:
    zone_code: str            # Energy-Charts `bzn` code (ENTSO-E bidding zone)
    country_name: str
    country_iso2: str
    energy_charts_country: str  # `country` code for /public_power
    region_name: str
    market_timezone: str
    latitude: float           # representative point used for weather data
    longitude: float


BIDDING_ZONES: dict[str, BiddingZone] = {z.zone_code: z for z in [
    BiddingZone("FR", "France", "FR", "fr", "West Europe", "Europe/Paris", 46.60, 2.40),
    BiddingZone("BE", "Belgium", "BE", "be", "West Europe", "Europe/Brussels", 50.64, 4.67),
    BiddingZone("NL", "Netherlands", "NL", "nl", "West Europe", "Europe/Amsterdam", 52.13, 5.29),
    BiddingZone("DE-LU", "Germany", "DE", "de", "Central Europe", "Europe/Berlin", 51.17, 10.45),
    BiddingZone("AT", "Austria", "AT", "at", "Central Europe", "Europe/Vienna", 47.52, 14.55),
    BiddingZone("CH", "Switzerland", "CH", "ch", "Central Europe", "Europe/Zurich", 46.82, 8.23),
    BiddingZone("ES", "Spain", "ES", "es", "South Europe", "Europe/Madrid", 40.46, -3.75),
    BiddingZone("PT", "Portugal", "PT", "pt", "South Europe", "Europe/Lisbon", 39.40, -8.22),
    BiddingZone("IT-North", "Italy", "IT", "it", "South Europe", "Europe/Rome", 45.46, 9.19),
    BiddingZone("PL", "Poland", "PL", "pl", "Central Europe", "Europe/Warsaw", 51.92, 19.15),
    BiddingZone("DK1", "Denmark", "DK", "dk", "Nordics", "Europe/Copenhagen", 56.26, 9.50),
    BiddingZone("SE3", "Sweden", "SE", "se", "Nordics", "Europe/Stockholm", 59.33, 18.07),
    BiddingZone("NO2", "Norway", "NO", "no", "Nordics", "Europe/Oslo", 58.97, 5.73),
    BiddingZone("FI", "Finland", "FI", "fi", "Nordics", "Europe/Helsinki", 61.92, 25.75),
]}


def get_zone(zone_code: str) -> BiddingZone:
    try:
        return BIDDING_ZONES[zone_code]
    except KeyError as exc:
        known = ", ".join(sorted(BIDDING_ZONES))
        raise ValueError(f"Unknown bidding zone '{zone_code}'. Known zones: {known}") from exc


def zones_as_rows() -> list[dict]:
    return [asdict(z) for z in BIDDING_ZONES.values()]

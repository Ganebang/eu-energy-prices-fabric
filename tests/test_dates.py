from datetime import date, datetime, timedelta, timezone

import pytest

from energy_toolkit.dates import resolve_window, split_into_chunks


def test_explicit_window_is_returned_as_dates():
    assert resolve_window("2026-01-01", "2026-01-31") == (date(2026, 1, 1), date(2026, 1, 31))


def test_empty_window_defaults_to_yesterday_and_tomorrow():
    today = datetime.now(timezone.utc).date()
    assert resolve_window("", "") == (today - timedelta(days=1), today + timedelta(days=1))


def test_custom_defaults_are_applied():
    today = datetime.now(timezone.utc).date()
    start, end = resolve_window("", "", default_days_back=2, default_days_ahead=0)
    assert (start, end) == (today - timedelta(days=2), today)


def test_end_before_start_is_rejected():
    with pytest.raises(ValueError):
        resolve_window("2026-02-01", "2026-01-01")


def test_chunks_cover_the_window_without_gaps_or_overlaps():
    chunks = split_into_chunks(date(2026, 1, 1), date(2026, 3, 15), max_days=31)
    assert chunks[0][0] == date(2026, 1, 1)
    assert chunks[-1][1] == date(2026, 3, 15)
    for (_, previous_end), (next_start, _) in zip(chunks, chunks[1:]):
        assert next_start == previous_end + timedelta(days=1)
    assert all((end - start).days < 31 for start, end in chunks)


def test_single_day_window_gives_one_chunk():
    assert split_into_chunks(date(2026, 5, 1), date(2026, 5, 1)) == [(date(2026, 5, 1), date(2026, 5, 1))]

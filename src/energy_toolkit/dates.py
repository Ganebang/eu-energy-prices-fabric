"""Date-window helpers shared by the ingestion notebooks."""
from datetime import date, datetime, timedelta, timezone


def resolve_window(start_text: str, end_text: str,
                   default_days_back: int = 1, default_days_ahead: int = 1) -> tuple[date, date]:
    """Turn optional 'yyyy-MM-dd' strings into a closed [start, end] date window.

    Empty strings fall back to a rolling window around today (UTC), which is
    what the scheduled pipeline uses. Explicit dates are used for back-fills.
    """
    today_utc = datetime.now(timezone.utc).date()
    window_start = date.fromisoformat(start_text) if start_text else today_utc - timedelta(days=default_days_back)
    window_end = date.fromisoformat(end_text) if end_text else today_utc + timedelta(days=default_days_ahead)
    if window_end < window_start:
        raise ValueError(f"end_date ({window_end}) is before start_date ({window_start})")
    return window_start, window_end


def split_into_chunks(window_start: date, window_end: date, max_days: int = 31) -> list[tuple[date, date]]:
    """Split a long back-fill into polite API calls of at most `max_days` days."""
    chunks = []
    chunk_start = window_start
    while chunk_start <= window_end:
        chunk_end = min(chunk_start + timedelta(days=max_days - 1), window_end)
        chunks.append((chunk_start, chunk_end))
        chunk_start = chunk_end + timedelta(days=1)
    return chunks

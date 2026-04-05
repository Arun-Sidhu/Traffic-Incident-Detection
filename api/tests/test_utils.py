# test_utils.py
from datetime import datetime, timezone

import main


def test_normalize_status_accepts_close_and_closed():
    assert main.normalize_status("close") == "closed"
    assert main.normalize_status("closed") == "closed"
    assert main.normalize_status("open") == "open"


def test_dedupe_history_incidents_prefers_closed_row():
    rows = [
        {
            "incident_id": "inc_1",
            "segment_id": "seg_1",
            "status": "open",
            "opened_at": "2024-03-09T16:00:00+00:00",
            "closed_at": None,
            "ingested_at": "2024-03-09T16:01:00+00:00",
        },
        {
            "incident_id": "inc_1",
            "segment_id": "seg_1",
            "status": "close",
            "opened_at": "2024-03-09T16:00:00+00:00",
            "closed_at": "2024-03-09T16:03:00+00:00",
            "ingested_at": "2024-03-09T16:03:00+00:00",
        },
    ]

    deduped = main.dedupe_history_incidents(rows)

    assert len(deduped) == 1
    assert deduped[0]["status"] == "closed"
    assert deduped[0]["closed_at"] == "2024-03-09T16:03:00+00:00"


def test_compute_dashboard_overview_counts_distinct_history_incidents():
    now = datetime(2024, 3, 9, 16, 5, tzinfo=timezone.utc)

    current_rows = [
        {"segment_id": "seg_a", "status": "open", "severity": 4.2}
    ]

    history_incidents = [
        {
            "incident_id": "inc_1",
            "segment_id": "seg_a",
            "status": "closed",
            "opened_at": "2024-03-09T16:04:00+00:00",
            "closed_at": "2024-03-09T16:05:00+00:00",
        },
        {
            "incident_id": "inc_2",
            "segment_id": "seg_b",
            "status": "open",
            "opened_at": "2024-03-09T15:50:00+00:00",
        },
    ]

    overview = main.compute_dashboard_overview(
        current_rows,
        history_incidents,
        history_event_row_count=3,
        now=now,
    )

    assert overview["active_incidents"] == 1
    assert overview["opened_last_5_minutes"] == 1
    assert overview["history_event_rows"] == 3
    assert overview["history_incidents"] == 2
    assert overview["most_affected_segment"]["segment_id"] == "seg_b"
    assert overview["most_affected_segment"]["incident_count"] == 1
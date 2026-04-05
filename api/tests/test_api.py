# test_api.py
from datetime import datetime, timezone

from fastapi.testclient import TestClient

import app.services.incidents as incidents_service
import main


class FakeCursor:
    def __init__(self, rows):
        self.rows = rows
        self.executed = []

    def execute(self, sql, params):
        self.executed.append((sql, params))

    def fetchall(self):
        return self.rows

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class FakeConn:
    def __init__(self, rows):
        self.rows = rows

    def cursor(self):
        return FakeCursor(self.rows)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


def test_history_endpoint_returns_distinct_incident_payload(monkeypatch):
    rows = [
        (
            "inc_1",
            "seg_1",
            "close",
            datetime(2024, 3, 9, 16, 0, tzinfo=timezone.utc),
            datetime(2024, 3, 9, 16, 2, tzinfo=timezone.utc),
            1.1,
            20.0,
            18.0,
            7,
            datetime(2024, 3, 9, 16, 2, tzinfo=timezone.utc),
        )
    ]
    monkeypatch.setattr(incidents_service, "get_conn", lambda: FakeConn(rows))

    client = TestClient(main.app)
    resp = client.get("/incidents/history")
    body = resp.json()

    assert resp.status_code == 200
    assert body["history_type"] == "distinct_incidents"
    assert body["incident_count"] == 1
    assert len(body["incidents"]) == 1
    assert body["incidents"][0]["status"] == "closed"
    assert body["incidents"][0]["closed_at"] == "2024-03-09T16:02:00+00:00"


def test_history_raw_endpoint_returns_event_rows(monkeypatch):
    rows = [
        (
            "inc_1",
            "seg_1",
            "close",
            datetime(2024, 3, 9, 16, 0, tzinfo=timezone.utc),
            datetime(2024, 3, 9, 16, 2, tzinfo=timezone.utc),
            1.1,
            20.0,
            18.0,
            7,
            datetime(2024, 3, 9, 16, 2, tzinfo=timezone.utc),
        ),
        (
            "inc_1",
            "seg_1",
            "open",
            datetime(2024, 3, 9, 16, 0, tzinfo=timezone.utc),
            None,
            1.1,
            20.0,
            18.0,
            7,
            datetime(2024, 3, 9, 16, 0, tzinfo=timezone.utc),
        ),
    ]
    monkeypatch.setattr(incidents_service, "get_conn", lambda: FakeConn(rows))

    client = TestClient(main.app)
    resp = client.get("/incidents/history/raw")
    body = resp.json()

    assert resp.status_code == 200
    assert body["history_type"] == "event_rows"
    assert body["row_count"] == 2
    assert len(body["history"]) == 2
    assert [row["status"] for row in body["history"]] == ["closed", "open"]
    assert body["history"][0]["closed_at"] == "2024-03-09T16:02:00+00:00"
    assert body["history"][1]["closed_at"] is None


def test_summary_endpoint_reports_raw_and_distinct_counts(monkeypatch):
    summary = {
        "generated_at": "2024-03-09T16:05:00+00:00",
        "active_incidents": 1,
        "average_severity": 4.4,
        "opened_last_5_minutes": 1,
        "most_affected_segment": {
            "segment_id": "seg_2",
            "incident_count": 1,
        },
        "current_incident_rows": 1,
        "history_event_rows": 3,
        "history_incidents": 2,
        "card_metrics": {
            "active_incidents": 1,
            "average_severity": 4.4,
            "top_segment": "seg_2",
            "top_segment_incident_count": 1,
            "opened_last_5_minutes": 1,
        },
    }

    monkeypatch.setattr(
        incidents_service,
        "fetch_dashboard_overview",
        lambda **kwargs: summary,
    )

    client = TestClient(main.app)
    resp = client.get("/summary")
    body = resp.json()

    assert resp.status_code == 200
    assert body["active_incidents"] == 1
    assert body["history_event_rows"] == 3
    assert body["history_incidents"] == 2
    assert body["most_affected_segment"]["segment_id"] == "seg_2"
    assert body["most_affected_segment"]["incident_count"] == 1
    assert body["card_metrics"]["active_incidents"] == 1
    assert body["card_metrics"]["top_segment"] == "seg_2"


def test_summary_endpoint_forwards_only_supported_filters(monkeypatch):
    captured = {}

    summary = {
        "generated_at": "2024-03-09T16:05:00+00:00",
        "active_incidents": 1,
        "average_severity": 4.4,
        "opened_last_5_minutes": 1,
        "most_affected_segment": {
            "segment_id": "seg_2",
            "incident_count": 1,
        },
        "current_incident_rows": 1,
        "history_event_rows": 3,
        "history_incidents": 2,
        "card_metrics": {
            "active_incidents": 1,
            "average_severity": 4.4,
            "top_segment": "seg_2",
            "top_segment_incident_count": 1,
            "opened_last_5_minutes": 1,
        },
    }

    def fake_fetch_dashboard_overview(**kwargs):
        captured.update(kwargs)
        return summary

    monkeypatch.setattr(
        incidents_service,
        "fetch_dashboard_overview",
        fake_fetch_dashboard_overview,
    )

    client = TestClient(main.app)
    resp = client.get(
        "/summary",
        params={
            "segment_id": "seg_2",
            "min_severity": 2.5,
            "recent_window_minutes": 10,
        },
    )

    assert resp.status_code == 200
    assert captured == {
        "segment_id": "seg_2",
        "min_severity": 2.5,
        "recent_window_minutes": 10,
    }


def test_current_row_to_api_uses_epoch_ms_for_start_ts():
    row = (
        "inc_live",
        "seg_live",
        datetime(2024, 3, 9, 16, 0, tzinfo=timezone.utc),
        4.4,
        20.0,
        18.0,
        7,
        datetime(2024, 3, 9, 16, 1, tzinfo=timezone.utc),
    )

    body = incidents_service.current_row_to_api(row)

    assert body["opened_at"] == "2024-03-09T16:00:00+00:00"
    assert body["start_ts"] == 1709990400000


def test_current_row_to_api_uses_epoch_ms_for_start_ts():
    row = (
        "inc_live",
        "seg_live",
        datetime(2024, 3, 9, 16, 0, tzinfo=timezone.utc),
        4.4,
        20.0,
        18.0,
        7,
        datetime(2024, 3, 9, 16, 1, tzinfo=timezone.utc),
    )

    body = incidents_service.current_row_to_api(row)

    assert body["opened_at"] == "2024-03-09T16:00:00+00:00"
    assert body["start_ts"] == 1710000000000


def test_history_row_to_api_uses_epoch_ms_for_start_ts_and_end_ts():
    row = (
        "inc_1",
        "seg_1",
        "closed",
        datetime(2024, 3, 9, 16, 0, tzinfo=timezone.utc),
        datetime(2024, 3, 9, 16, 2, tzinfo=timezone.utc),
        1.1,
        20.0,
        18.0,
        7,
        datetime(2024, 3, 9, 16, 2, tzinfo=timezone.utc),
    )

    body = incidents_service.history_row_to_api(row)

    assert body["opened_at"] == "2024-03-09T16:00:00+00:00"
    assert body["closed_at"] == "2024-03-09T16:02:00+00:00"
    assert body["start_ts"] == 1710000000000
    assert body["end_ts"] == 1710000120000
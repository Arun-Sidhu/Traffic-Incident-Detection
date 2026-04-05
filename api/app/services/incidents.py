# incidents.py
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from ..db import get_conn
from ..settings import MPS_TO_MPH


def to_float(value: Any) -> float | None:
    if value is None:
        return None
    return float(value)


def mps_to_mph(value: Any) -> float | None:
    f = to_float(value)
    if f is None:
        return None
    return round(f * MPS_TO_MPH, 1)


def to_api_time(value: Any) -> str | None:
    if value is None:
        return None

    if hasattr(value, "isoformat"):
        return value.isoformat()

    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(value / 1000.0, tz=timezone.utc).isoformat()
        except Exception:
            return str(value)

    return str(value)


def normalize_status(value: Any) -> str:
    status = str(value or "open").strip().lower()
    if status in {"close", "closed"}:
        return "closed"
    return "open"


def parse_api_datetime(value: Any) -> datetime | None:
    if value is None:
        return None

    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value

    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(float(value) / 1000.0, tz=timezone.utc)
        except (OverflowError, OSError, ValueError):
            return None

    text = str(value).strip()
    if not text:
        return None

    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        pass

    try:
        return datetime.fromtimestamp(float(text) / 1000.0, tz=timezone.utc)
    except (OverflowError, OSError, ValueError):
        return None


def to_epoch_ms(value: Any) -> int | None:
    dt = parse_api_datetime(value)
    if dt is None:
        return None
    return int(dt.timestamp() * 1000)


def current_row_to_api(row: tuple[Any, ...]) -> dict[str, Any]:
    incident_id = row[0]
    segment_id = row[1]

    opened_dt = row[2]
    opened_at = to_api_time(opened_dt)
    start_ts = to_epoch_ms(opened_dt)

    severity = to_float(row[3])
    observed_count = row[6]

    updated_dt = row[7]
    updated_at = to_api_time(updated_dt)

    baseline_speed_mph = mps_to_mph(row[4])
    current_speed_mph = mps_to_mph(row[5])

    return {
        "incident_id": incident_id,
        "segment_id": segment_id,
        "status": "open",
        "opened_at": opened_at,
        "severity": severity,
        "baseline_speed_mph": baseline_speed_mph,
        "current_speed_mph": current_speed_mph,
        "observed_count": observed_count,
        "updated_at": updated_at,
        "start_ts": start_ts,
    }


def history_row_to_api(row: tuple[Any, ...]) -> dict[str, Any]:
    incident_id = row[0]
    segment_id = row[1]
    status = normalize_status(row[2])

    opened_dt = row[3]
    opened_at = to_api_time(opened_dt)
    start_ts = to_epoch_ms(opened_dt)

    raw_closed_at = row[4]
    if status != "closed":
        closed_at = None
        end_ts = None
    elif isinstance(raw_closed_at, (int, float)) and raw_closed_at <= 0:
        closed_at = None
        end_ts = None
    else:
        closed_at = to_api_time(raw_closed_at)
        end_ts = to_epoch_ms(raw_closed_at)

    severity = to_float(row[5])
    observed_count = row[8]

    ingested_dt = row[9]
    ingested_at = to_api_time(ingested_dt)

    baseline_speed_mph = mps_to_mph(row[6])
    current_speed_mph = mps_to_mph(row[7])

    return {
        "incident_id": incident_id,
        "segment_id": segment_id,
        "status": status,
        "opened_at": opened_at,
        "closed_at": closed_at,
        "severity": severity,
        "baseline_speed_mph": baseline_speed_mph,
        "current_speed_mph": current_speed_mph,
        "observed_count": observed_count,
        "ingested_at": ingested_at,
        "start_ts": start_ts,
        "end_ts": end_ts,
    }


def fetch_current_rows(
    segment_id: str | None = None,
    min_severity: float | None = None,
    limit: int = 200,
) -> list[dict[str, Any]]:
    sql = """
      SELECT incident_id, segment_id, start_ts, severity,
             baseline_speed_mps, observed_speed_mps, observed_count, updated_at
      FROM current_incidents
      WHERE (%s IS NULL OR segment_id = %s)
        AND (%s IS NULL OR severity >= %s)
      ORDER BY updated_at DESC
      LIMIT %s;
    """

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (segment_id, segment_id, min_severity, min_severity, limit))
        rows = cur.fetchall()

    return [current_row_to_api(r) for r in rows]


def fetch_history_event_rows(
    segment_id: str | None = None,
    since_ms: int | None = None,
    limit: int = 500,
) -> list[dict[str, Any]]:
    sql = """
      SELECT incident_id, segment_id, status, start_ts, end_ts,
             severity, baseline_speed_mps, observed_speed_mps, observed_count, ingested_at
      FROM incident_events
      WHERE (%s IS NULL OR segment_id = %s)
        AND (
          %s IS NULL
          OR start_ts >= %s
          OR COALESCE(end_ts, start_ts) >= %s
        )
      ORDER BY ingested_at DESC
      LIMIT %s;
    """

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (segment_id, segment_id, since_ms, since_ms, since_ms, limit))
        rows = cur.fetchall()

    return [history_row_to_api(r) for r in rows]


def dedupe_history_incidents(history_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}

    for index, row in enumerate(history_rows):
        incident_id = str(row.get("incident_id") or f"_row_{index}")
        segment_id = row.get("segment_id")
        status = normalize_status(row.get("status"))

        opened_source = row.get("opened_at")
        if opened_source is None:
            opened_source = row.get("start_ts")

        closed_source = row.get("closed_at")
        if closed_source is None:
            closed_source = row.get("end_ts")

        ingested_source = row.get("ingested_at")

        opened_at = to_api_time(parse_api_datetime(opened_source)) if opened_source is not None else None
        start_ts = to_epoch_ms(opened_source)

        if status == "closed":
            closed_at = to_api_time(parse_api_datetime(closed_source)) if closed_source is not None else None
            end_ts = to_epoch_ms(closed_source)
        else:
            closed_at = None
            end_ts = None

        ingested_at = to_api_time(parse_api_datetime(ingested_source)) if ingested_source is not None else None

        candidate = {
            **row,
            "incident_id": incident_id,
            "segment_id": segment_id,
            "status": status,
            "opened_at": opened_at,
            "start_ts": start_ts,
            "closed_at": closed_at,
            "end_ts": end_ts,
            "ingested_at": ingested_at,
        }

        existing = merged.get(incident_id)
        if existing is None:
            merged[incident_id] = candidate
            continue

        existing_status = normalize_status(existing.get("status"))
        existing_ingested = parse_api_datetime(existing.get("ingested_at"))
        candidate_ingested = parse_api_datetime(candidate.get("ingested_at"))

        take_candidate = False
        if existing_status != "closed" and status == "closed":
            take_candidate = True
        elif candidate_ingested and (existing_ingested is None or candidate_ingested > existing_ingested):
            take_candidate = True

        if take_candidate:
            merged[incident_id] = {
                **existing,
                **candidate,
                "opened_at": existing.get("opened_at") or candidate.get("opened_at"),
                "start_ts": existing.get("start_ts") or candidate.get("start_ts"),
            }
        else:
            existing_closed_at = existing.get("closed_at")
            existing_end_ts = existing.get("end_ts")
            if status == "closed" and (not existing_closed_at or existing_end_ts is None):
                existing["closed_at"] = existing_closed_at or closed_at
                existing["end_ts"] = existing_end_ts or end_ts
                existing["status"] = "closed"

    return list(merged.values())


def fetch_history_views(
    segment_id: str | None = None,
    since_ms: int | None = None,
    limit: int = 500,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    event_rows = fetch_history_event_rows(
        segment_id=segment_id,
        since_ms=since_ms,
        limit=limit,
    )
    incidents = dedupe_history_incidents(event_rows)
    return event_rows, incidents


def fetch_current_summary(
    segment_id: str | None = None,
    min_severity: float | None = None,
) -> tuple[int, float]:
    sql = """
      SELECT COUNT(*)::int, AVG(severity)
      FROM current_incidents
      WHERE (%s IS NULL OR segment_id = %s)
        AND (%s IS NULL OR severity >= %s);
    """

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (segment_id, segment_id, min_severity, min_severity))
        row = cur.fetchone()

    active_incidents = int(row[0] or 0)
    average_severity = round(float(row[1]), 1) if row[1] is not None else 0.0
    return active_incidents, average_severity


def fetch_history_summary(
    segment_id: str | None = None,
    *,
    recent_window_minutes: int = 5,
    now: datetime | None = None,
) -> tuple[int, int, int, dict[str, Any] | None]:
    if now is None:
        now = datetime.now(timezone.utc)

    recent_cutoff_ms = int((now - timedelta(minutes=recent_window_minutes)).timestamp() * 1000)

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*)::int, COUNT(DISTINCT incident_id)::int
            FROM incident_events
            WHERE (%s IS NULL OR segment_id = %s);
            """,
            (segment_id, segment_id),
        )
        totals_row = cur.fetchone()
        history_event_rows = int(totals_row[0] or 0)
        history_incidents = int(totals_row[1] or 0)

        cur.execute(
            """
            SELECT COUNT(DISTINCT incident_id)::int
            FROM incident_events
            WHERE (%s IS NULL OR segment_id = %s)
              AND start_ts >= %s;
            """,
            (segment_id, segment_id, recent_cutoff_ms),
        )
        recent_row = cur.fetchone()
        opened_last_window = int(recent_row[0] or 0)

        cur.execute(
            """
            SELECT segment_id, COUNT(DISTINCT incident_id)::int AS incident_count
            FROM incident_events
            WHERE (%s IS NULL OR segment_id = %s)
              AND segment_id IS NOT NULL
              AND segment_id <> ''
            GROUP BY segment_id
            ORDER BY incident_count DESC, segment_id DESC
            LIMIT 1;
            """,
            (segment_id, segment_id),
        )
        top_row = cur.fetchone()

    most_affected_segment = None
    if top_row:
        most_affected_segment = {
            "segment_id": str(top_row[0]),
            "incident_count": int(top_row[1]),
        }

    return history_event_rows, history_incidents, opened_last_window, most_affected_segment


def compute_dashboard_overview(
    current_rows: list[dict[str, Any]],
    history_incidents: list[dict[str, Any]],
    *,
    history_event_row_count: int,
    recent_window_minutes: int = 5,
    now: datetime | None = None,
) -> dict[str, Any]:
    if now is None:
        now = datetime.now(timezone.utc)

    distinct_history_incidents = dedupe_history_incidents(history_incidents)

    active_rows = [
        row for row in current_rows
        if normalize_status(row.get("status")) != "closed"
    ]
    active_incidents = len(active_rows)

    severity_values: list[float] = []
    for row in active_rows:
        severity = to_float(row.get("severity"))
        if severity is not None:
            severity_values.append(severity)

    average_severity = round(sum(severity_values) / len(severity_values), 1) if severity_values else 0.0

    recent_cutoff = now - timedelta(minutes=recent_window_minutes)
    opened_last_window = 0
    segment_counts: dict[str, int] = {}

    for row in distinct_history_incidents:
        opened_at = parse_api_datetime(
            row.get("opened_at") if row.get("opened_at") is not None else row.get("start_ts")
        )
        if opened_at is not None and opened_at >= recent_cutoff:
            opened_last_window += 1

        segment_id = str(row.get("segment_id") or "").strip()
        if segment_id:
            segment_counts[segment_id] = segment_counts.get(segment_id, 0) + 1

    most_affected_segment = None
    if segment_counts:
        segment_id, incident_count = max(
            segment_counts.items(),
            key=lambda item: (item[1], item[0]),
        )
        most_affected_segment = {
            "segment_id": segment_id,
            "incident_count": incident_count,
        }

    history_incident_count = len(distinct_history_incidents)

    return {
        "generated_at": now.isoformat(),
        "active_incidents": active_incidents,
        "average_severity": average_severity,
        "opened_last_5_minutes": opened_last_window,
        "most_affected_segment": most_affected_segment,
        "current_incident_rows": active_incidents,
        "history_event_rows": int(history_event_row_count),
        "history_incidents": history_incident_count,
        "card_metrics": {
            "active_incidents": active_incidents,
            "average_severity": average_severity,
            "top_segment": most_affected_segment["segment_id"] if most_affected_segment else None,
            "top_segment_incident_count": most_affected_segment["incident_count"] if most_affected_segment else 0,
            "opened_last_5_minutes": opened_last_window,
        },
    }


def fetch_dashboard_overview(
    segment_id: str | None = None,
    min_severity: float | None = None,
    *,
    recent_window_minutes: int = 5,
    now: datetime | None = None,
) -> dict[str, Any]:
    if now is None:
        now = datetime.now(timezone.utc)

    active_incidents, average_severity = fetch_current_summary(
        segment_id=segment_id,
        min_severity=min_severity,
    )
    history_event_rows, history_incidents, opened_last_window, most_affected_segment = fetch_history_summary(
        segment_id=segment_id,
        recent_window_minutes=recent_window_minutes,
        now=now,
    )

    return {
        "generated_at": now.isoformat(),
        "active_incidents": active_incidents,
        "average_severity": average_severity,
        "opened_last_5_minutes": opened_last_window,
        "most_affected_segment": most_affected_segment,
        "current_incident_rows": active_incidents,
        "history_event_rows": history_event_rows,
        "history_incidents": history_incidents,
        "card_metrics": {
            "active_incidents": active_incidents,
            "average_severity": average_severity,
            "top_segment": most_affected_segment["segment_id"] if most_affected_segment else None,
            "top_segment_incident_count": most_affected_segment["incident_count"] if most_affected_segment else 0,
            "opened_last_5_minutes": opened_last_window,
        },
    }
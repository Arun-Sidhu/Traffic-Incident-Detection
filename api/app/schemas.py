# schemas.py
from __future__ import annotations

from pydantic import BaseModel


class CurrentIncident(BaseModel):
    incident_id: str
    segment_id: str
    status: str
    opened_at: str | None
    severity: float | None
    baseline_speed_mph: float | None
    current_speed_mph: float | None
    observed_count: int | None
    updated_at: str | None
    start_ts: int | None


class HistoryIncident(BaseModel):
    incident_id: str
    segment_id: str
    status: str
    opened_at: str | None
    closed_at: str | None
    severity: float | None
    baseline_speed_mph: float | None
    current_speed_mph: float | None
    observed_count: int | None
    ingested_at: str | None
    start_ts: int | None
    end_ts: int | None


class DistinctIncidentHistoryResponse(BaseModel):
    history_type: str
    incident_count: int
    incidents: list[HistoryIncident]


class RawHistoryResponse(BaseModel):
    history_type: str
    row_count: int
    history: list[HistoryIncident]


class MostAffectedSegment(BaseModel):
    segment_id: str
    incident_count: int


class CardMetrics(BaseModel):
    active_incidents: int
    average_severity: float
    top_segment: str | None
    top_segment_incident_count: int
    opened_last_5_minutes: int


class DashboardOverviewResponse(BaseModel):
    generated_at: str
    active_incidents: int
    average_severity: float
    opened_last_5_minutes: int
    most_affected_segment: MostAffectedSegment | None
    current_incident_rows: int
    history_event_rows: int
    history_incidents: int
    card_metrics: CardMetrics
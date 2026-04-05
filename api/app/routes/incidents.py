# incidents.py
from fastapi import APIRouter, Query

from ..schemas import (
    CurrentIncident,
    DashboardOverviewResponse,
    DistinctIncidentHistoryResponse,
    RawHistoryResponse,
)
from ..services import incidents as incident_service

router = APIRouter(tags=["incidents"])


@router.get("/incidents", response_model=list[CurrentIncident])
@router.get("/incidents/current", response_model=list[CurrentIncident])
def current_incidents(
    segment_id: str | None = None,
    min_severity: float | None = Query(default=None, ge=0.0),
    limit: int = Query(default=200, ge=1, le=1000),
):
    return incident_service.fetch_current_rows(
        segment_id=segment_id,
        min_severity=min_severity,
        limit=limit,
    )


@router.get("/incidents/history", response_model=DistinctIncidentHistoryResponse)
def incident_history(
    segment_id: str | None = None,
    since_ms: int | None = None,
    limit: int = Query(default=500, ge=1, le=5000),
):
    _, incidents = incident_service.fetch_history_views(
        segment_id=segment_id,
        since_ms=since_ms,
        limit=limit,
    )
    return {
        "history_type": "distinct_incidents",
        "incident_count": len(incidents),
        "incidents": incidents,
    }


@router.get("/incidents/history/raw", response_model=RawHistoryResponse)
def incident_history_raw(
    segment_id: str | None = None,
    since_ms: int | None = None,
    limit: int = Query(default=500, ge=1, le=5000),
):
    event_rows, _ = incident_service.fetch_history_views(
        segment_id=segment_id,
        since_ms=since_ms,
        limit=limit,
    )
    return {
        "history_type": "event_rows",
        "row_count": len(event_rows),
        "history": event_rows,
    }


@router.get("/summary", response_model=DashboardOverviewResponse)
@router.get("/dashboard/overview", response_model=DashboardOverviewResponse)
def dashboard_overview(
    segment_id: str | None = None,
    min_severity: float | None = Query(default=None, ge=0.0),
    recent_window_minutes: int = Query(default=5, ge=1, le=1440),
):
    return incident_service.fetch_dashboard_overview(
        segment_id=segment_id,
        min_severity=min_severity,
        recent_window_minutes=recent_window_minutes,
    )
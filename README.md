# traffic_incident_detection

This project is a local end to end traffic incident detection system.

It starts with simulated GPS pings. Those pings move through Kafka and into Flink. Flink builds segment metrics and looks for patterns that match an incident. Incident events move into Postgres through a sink service. A FastAPI app reads from Postgres and serves the current state, history, summary data, health checks, and Prometheus metrics. A browser dashboard shows the results.

## What this project covers

1. Streaming input data
2. Kafka topics for raw pings, segment metrics, and incident events
3. Flink jobs for segment metrics and incident detection
4. Postgres storage for current incidents and incident history
5. FastAPI endpoints for data access and health checks
6. A small dashboard for local inspection
7. Prometheus for local monitoring
8. A one command startup flow with `make`

## Repo layout

1. `simulator/`

   This folder has the GPS ping producer and the Postgres sink.

2. `flink-job/`

   This folder has the Java Flink jobs, the Maven build, the Flink image setup, and a small Java test.

3. `api/`

   This folder has the FastAPI app, routes, service layer, schemas, tests, static dashboard files, and API Dockerfile.

4. `db/`

   This folder has `init.sql` for the database tables and indexes.

5. `examples/`

   This folder has sample JSON files that show the path from normal traffic to slowdown, then incident open, then recovery, then incident close.

6. `observability/`

   This folder has the Prometheus config used by the local stack.

7. Root files

   The root folder has `Makefile` and `docker-compose.yml`. The root folder for this project is `traffic_incident_detection`.

## How the system works

1. The producer writes simulated GPS pings to Kafka.
2. Flink reads the raw pings and computes per segment traffic metrics.
3. The incident detector job reads those metrics and decides when to open or close an incident.
4. Incident events are written to Kafka.
5. The sink service reads incident events and writes them into Postgres.
6. The API reads from Postgres and returns current incidents, history, summary data, and metrics.
7. The dashboard calls the API and shows the current state in the browser.

## Quick start

You need Docker Desktop.

From the `traffic_incident_detection` root folder, run

```bash
make
```

That command does the full local startup flow.

1. It checks that Docker is available
2. It starts Docker Desktop on Mac if needed
3. It rebuilds the stack
4. It waits for both Flink jobs
5. It runs a smoke test
6. It opens the local dashboards

## What opens after startup

1. UI

```text
http://localhost:8000/ui
```

2. Summary

```text
http://localhost:8000/summary
```

3. API health

```text
http://localhost:8000/health
```

4. Flink UI

```text
http://localhost:8081
```

5. Kafka UI

```text
http://localhost:8080
```

6. Prometheus

```text
http://localhost:9090
```

## Main API endpoints

1. `/health`

   Basic service check.

2. `/incidents/current`

   Returns open incidents.

3. `/incidents/history`

   Returns incident history grouped by incident.

4. `/incidents/history/raw`

   Returns raw incident event rows.

5. `/summary`

   Returns dashboard level counts and summary values.

6. `/metrics`

   Returns Prometheus metrics from the API.

7. `/ui`

   Serves the dashboard page.

## Example incident flow

The `examples/` folder gives you a small story you can walk through during a demo.

1. `01_normal_metric.json`
2. `02_normal_metric.json`
3. `03_slowdown_metric.json`
4. `05_incident_open.json`
5. `04_recovery_metric.json`
6. `06_incident_close.json`

That sequence shows a segment moving from normal traffic to slowdown, then into an open incident, then back to recovery, and then into a close event.

## Useful commands

Start the stack

```bash
make
```

Show running containers

```bash
make ps
```

Show both Flink jobs

```bash
make jobs
```

Run the smoke test

```bash
make smoke
```

View API logs

```bash
make logs-api
```

View Flink logs

```bash
make logs-flink
```

Stop the stack

```bash
make down
```

Remove containers and volumes

```bash
make clean
```

## Running tests

The Python API tests live in `api/tests`.

From `api/`, run

```bash
python -m pytest -q
```

## Database shape

The database keeps two views of incident data.

1. `current_incidents`

   This table keeps one row for each open incident.

2. `incident_events`

   This table keeps the incident event history.

This split makes the API simple to query for the current state and for history.

## Monitoring note

Prometheus is part of the local stack and the API metrics endpoint is available. In the current working compose setup, the FastAPI and Kafka exporter targets are up. The Flink jobs run and the Flink UI works, but the Flink Prometheus scrape targets are not enabled in this version of the stack.

## Why this project matters

This repo is not just a small API or a script. It is a full backend pipeline with ingestion, stream processing, storage, an API layer, a local dashboard, tests, and one command startup. It is a good project for showing backend and data systems work in a form that another person can run on their machine.

## Current status

The project runs from the root folder with `make`. The API tests pass. The stack starts from scratch, waits for the Flink jobs, runs a smoke test, and opens the local dashboards.

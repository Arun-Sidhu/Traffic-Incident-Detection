# Makefile
SHELL := /bin/bash
.DEFAULT_GOAL := bootstrap-open

DOCKER_WAIT_RETRIES ?= 30
DOCKER_WAIT_SLEEP ?= 2
BOOTSTRAP_SMOKE_WAIT ?= 45

COMPOSE ?= docker compose
STACK_FILE ?= docker-compose.yml
PROJECT_NAME ?= traffic-incident-detection
SMOKE_WAIT ?= 20
JOBS_RETRIES ?= 10
JOBS_RETRY_SLEEP ?= 3
EXPECTED_JOB_1 ?= Traffic Segment Metrics (10s, event-time)
EXPECTED_JOB_2 ?= Traffic Incident Detector (open/close)

KAFKA_SERVICE := kafka
POSTGRES_SERVICE := postgres
FLINK_JM_SERVICE := flink-jobmanager
FLINK_TM_SERVICE := flink-taskmanager
FLINK_SUBMIT_SERVICE := flink-submit
PRODUCER_SERVICE := producer
SINK_SERVICE := incident-sink
API_SERVICE := api
PROM_SERVICE := prometheus
KAFKA_UI_SERVICE := kafka-ui

.PHONY: help bootstrap bootstrap-open check doctor build pull up rebuild up-core submit start-traffic start-observability restart ps jobs topics health smoke open open-api demo logs logs-core logs-api logs-flink db-shell kafka-shell down clean reset

help:
	@echo "Traffic Incident Detection - common commands"
	@echo
	@echo "  make                     Start from scratch, verify the stack, and open dashboards"
	@echo "  make bootstrap           Start from scratch and verify the stack"
	@echo "  make bootstrap-open      Same as bootstrap, then open dashboards"
	@echo "  make demo                Alias for bootstrap-open"
	@echo "  make check               Verify Docker/Compose are available"
	@echo "  make doctor              Show Docker reachability diagnostics"
	@echo "  make build               Build all local images"
	@echo "  make up                  Start the full stack without rebuilding"
	@echo "  make rebuild             Build and start the full stack"
	@echo "  make up-core             Start core infra without producer/API/observability"
	@echo "  make submit              Submit the Flink jobs only"
	@echo "  make start-traffic       Start producer + incident sink + API"
	@echo "  make ps                  Show running containers"
	@echo "  make jobs                Wait for and show both expected Flink jobs"
	@echo "  make jobs JOBS_RETRIES=15 JOBS_RETRY_SLEEP=2"
	@echo "  make jobs EXPECTED_JOB_1='Traffic Segment Metrics (10s, event-time)' EXPECTED_JOB_2='Traffic Incident Detector (open/close)'"
	@echo "  make topics              List Kafka topics"
	@echo "  make health              Quick health checks against local endpoints"
	@echo "  make smoke               End-to-end smoke test"
	@echo "  make smoke SMOKE_WAIT=45 Increase smoke-test wait time"
	@echo "  make start-observability Start Prometheus + Kafka UI"
	@echo "  make open                Open API, summary, Flink, Prometheus, and Kafka UI in browser"
	@echo "  make open-api            Open API JSON endpoints in browser"
	@echo "  make logs                Tail all logs"
	@echo "  make logs-flink          Tail Flink-related logs"
	@echo "  make logs-api            Tail API logs"
	@echo "  make db-shell            Open a psql shell in Postgres"
	@echo "  make kafka-shell         Open a shell in the Kafka container"
	@echo "  make down                Stop the stack"
	@echo "  make clean               Stop the stack and remove volumes"
	@echo "  make reset               Clean, rebuild, and start from scratch"

check:
	@command -v docker >/dev/null || (echo "docker is not installed or not on PATH" && exit 1)
	@if ! docker info >/dev/null 2>&1; then \
	  echo "Docker is not running."; \
	  if [ "$$(uname)" = "Darwin" ]; then \
	    echo "Launching Docker Desktop..."; \
	    open -ga Docker; \
	  fi; \
	  attempt=1; \
	  max_attempts=$(DOCKER_WAIT_RETRIES); \
	  while [ $$attempt -le $$max_attempts ]; do \
	    if docker info >/dev/null 2>&1; then \
	      break; \
	    fi; \
	    echo "Waiting for Docker to start ($$attempt/$$max_attempts) ..."; \
	    sleep $(DOCKER_WAIT_SLEEP); \
	    attempt=$$((attempt + 1)); \
	  done; \
	  docker info >/dev/null 2>&1 || (echo "Docker is still not ready. Start Docker and rerun 'make'." && exit 1); \
	fi
	@docker compose version >/dev/null 2>&1 || (echo "docker compose v2 is required" && exit 1)
	@echo "Docker and Docker Compose look good."

doctor:
	@echo "Docker context:"
	@docker context show 2>/dev/null || true
	@echo
	@echo "Docker info:"
	@if docker info >/dev/null 2>&1; then \
	  echo "Docker daemon is reachable."; \
	else \
	  echo "Docker daemon is NOT reachable."; \
	  if [ "$$(uname)" = "Darwin" ]; then \
	    echo "Launching Docker Desktop..."; \
	    open -ga Docker; \
	  fi; \
	  echo "Wait for Docker to finish starting, then rerun 'make check'."; \
	  exit 1; \
	fi

pull: check
	$(COMPOSE) -f $(STACK_FILE) pull || true

build: check
	$(COMPOSE) -f $(STACK_FILE) build

up: check
	$(COMPOSE) -f $(STACK_FILE) up -d
	@echo
	@echo "Stack starting. Use 'make health' and 'make smoke' to verify it."

rebuild: check
	$(COMPOSE) -f $(STACK_FILE) up -d --build
	@echo
	@echo "Stack rebuilding and starting. Use 'make health' and 'make smoke' to verify it."

up-core: check
	$(COMPOSE) -f $(STACK_FILE) up -d $(KAFKA_SERVICE) $(POSTGRES_SERVICE) $(FLINK_JM_SERVICE) $(FLINK_TM_SERVICE) kafka-init
	@echo "Core services starting."

submit: check
	$(COMPOSE) -f $(STACK_FILE) up -d $(FLINK_SUBMIT_SERVICE)
	@echo "Flink submit container launched."

start-traffic: check
	$(COMPOSE) -f $(STACK_FILE) up -d $(PRODUCER_SERVICE) $(SINK_SERVICE) $(API_SERVICE)
	@echo "Traffic pipeline services starting."

start-observability: check
	$(COMPOSE) -f $(STACK_FILE) up -d $(KAFKA_UI_SERVICE) kafka-exporter $(PROM_SERVICE)
	@echo "Observability services starting."

restart: check
	$(COMPOSE) -f $(STACK_FILE) restart

ps: check
	$(COMPOSE) -f $(STACK_FILE) ps

jobs: check
	@echo "Waiting for expected Flink jobs:"
	@echo "  1) $(EXPECTED_JOB_1)"
	@echo "  2) $(EXPECTED_JOB_2)"
	@attempt=1; \
	max_attempts=$(JOBS_RETRIES); \
	while [ $$attempt -le $$max_attempts ]; do \
	  output="$$(curl -fsS http://localhost:8081/jobs/overview 2>/dev/null || true)"; \
	  if [ -n "$$output" ]; then \
	    has_job_1=0; \
	    has_job_2=0; \
	    printf '%s' "$$output" | grep -Fq '"name":"$(EXPECTED_JOB_1)"' && has_job_1=1; \
	    printf '%s' "$$output" | grep -Fq '"name":"$(EXPECTED_JOB_2)"' && has_job_2=1; \
	    if [ $$has_job_1 -eq 1 ] && [ $$has_job_2 -eq 1 ]; then \
	      echo "Both expected Flink jobs are visible."; \
	      printf '%s\n' "$$output"; \
	      echo; \
	      exit 0; \
	    fi; \
	  fi; \
	  if [ $$attempt -lt $$max_attempts ]; then \
	    if [ -z "$$output" ]; then \
	      echo "Flink UI or jobs endpoint not ready yet (attempt $$attempt/$$max_attempts). Retrying in $(JOBS_RETRY_SLEEP)s ..."; \
	    else \
	      missing=""; \
	      printf '%s' "$$output" | grep -Fq '"name":"$(EXPECTED_JOB_1)"' || missing="$$missing [missing: $(EXPECTED_JOB_1)]"; \
	      printf '%s' "$$output" | grep -Fq '"name":"$(EXPECTED_JOB_2)"' || missing="$$missing [missing: $(EXPECTED_JOB_2)]"; \
	      echo "Flink UI is up but not all expected jobs are visible yet (attempt $$attempt/$$max_attempts).$$missing Retrying in $(JOBS_RETRY_SLEEP)s ..."; \
	    fi; \
	    sleep $(JOBS_RETRY_SLEEP); \
	    attempt=$$((attempt + 1)); \
	  else \
	    echo "Did not see both expected Flink jobs after $$max_attempts attempts."; \
	    if [ -n "$$output" ]; then printf '%s\n' "$$output"; fi; \
	    exit 1; \
	  fi; \
	done

topics: check
	$(COMPOSE) -f $(STACK_FILE) exec $(KAFKA_SERVICE) /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list

health: check
	@echo "Checking API ..."
	@curl -fsS http://localhost:8000/health && echo || (echo "API not ready" && exit 1)
	@echo "Checking Flink ..."
	@curl -fsS http://localhost:8081/overview >/dev/null && echo "Flink UI OK" || (echo "Flink UI not ready" && exit 1)
	@echo "Checking Prometheus ..."
	@curl -fsS http://localhost:9090/-/healthy && echo || (echo "Prometheus not ready" && exit 1)

smoke: check
	@echo "Waiting $(SMOKE_WAIT) seconds so the simulator can produce data ..."
	@sleep $(SMOKE_WAIT)
	@echo "Current incidents:"
	@curl -fsS http://localhost:8000/incidents/current && echo || true
	@echo
	@echo "Recent incident history:"
	@curl -fsS "http://localhost:8000/incidents/history?limit=10" && echo || true
	@echo
	@echo "Current rows in Postgres:"
	@$(COMPOSE) -f $(STACK_FILE) exec -T $(POSTGRES_SERVICE) psql -U traffic -d trafficdb -c "select incident_id, segment_id, severity, updated_at from current_incidents order by updated_at desc limit 10;" || true
	@echo
	@echo "History rows in Postgres:"
	@$(COMPOSE) -f $(STACK_FILE) exec -T $(POSTGRES_SERVICE) psql -U traffic -d trafficdb -c "select incident_id, segment_id, status, ingested_at from incident_events order by ingested_at desc limit 10;" || true

bootstrap:
	$(MAKE) reset
	$(MAKE) jobs
	$(MAKE) smoke SMOKE_WAIT=$(BOOTSTRAP_SMOKE_WAIT)
	@echo
	@echo "Stack is ready."
	@echo "Open these URLs:"
	@echo "  API/UI      http://localhost:8000/ui"
	@echo "  API health  http://localhost:8000/health"
	@echo "  Summary     http://localhost:8000/summary"
	@echo "  Flink UI    http://localhost:8081"
	@echo "  Kafka UI    http://localhost:8080"
	@echo "  Prometheus  http://localhost:9090"

bootstrap-open: bootstrap
	@if [ "$$(uname)" = "Darwin" ]; then \
	  $(MAKE) open; \
	else \
	  echo "Auto-open is only configured for macOS. Use the URLs printed above."; \
	fi

open: health
	@echo "Opening local dashboards ..."
	@if [ "$$(uname)" = "Darwin" ]; then \
	  open http://localhost:8000/ui; \
	  open http://localhost:8000/summary; \
	  open http://localhost:8081; \
	  open http://localhost:8080; \
	  open http://localhost:9090; \
	else \
	  echo "Browser auto-open is only configured for macOS."; \
	fi

open-api: health
	@echo "Opening API endpoints ..."
	@if [ "$$(uname)" = "Darwin" ]; then \
	  open http://localhost:8000/incidents/current; \
	  open http://localhost:8000/incidents/history; \
	  open http://localhost:8000/metrics; \
	  open http://localhost:8000/health; \
	else \
	  echo "Browser auto-open is only configured for macOS."; \
	fi

demo: bootstrap-open

logs: check
	$(COMPOSE) -f $(STACK_FILE) logs -f --tail=200

logs-core: check
	$(COMPOSE) -f $(STACK_FILE) logs -f --tail=200 $(KAFKA_SERVICE) $(POSTGRES_SERVICE) $(FLINK_JM_SERVICE) $(FLINK_TM_SERVICE)

logs-api: check
	$(COMPOSE) -f $(STACK_FILE) logs -f --tail=200 $(API_SERVICE)

logs-flink: check
	$(COMPOSE) -f $(STACK_FILE) logs -f --tail=200 $(FLINK_JM_SERVICE) $(FLINK_TM_SERVICE) $(FLINK_SUBMIT_SERVICE)

db-shell: check
	$(COMPOSE) -f $(STACK_FILE) exec $(POSTGRES_SERVICE) psql -U traffic -d trafficdb

kafka-shell: check
	$(COMPOSE) -f $(STACK_FILE) exec $(KAFKA_SERVICE) bash

down: check
	$(COMPOSE) -f $(STACK_FILE) down

clean: check
	$(COMPOSE) -f $(STACK_FILE) down -v --remove-orphans

reset: clean rebuild
	@echo "Fresh rebuild complete."
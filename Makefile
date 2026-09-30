# Live Analytics — developer commands. Run `make help`.
SHELL := /bin/bash
ENV_FILE := $(if $(wildcard .env),.env,.env.example)
LOAD_ENV := set -a; source $(ENV_FILE); set +a;

.PHONY: help infra-up infra-down generate build test test-integration lint run-ingest run-sink run-query \
	load verify dashboard-install dashboard-dev dashboard-test up down clean

help: ## Show this help
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

infra-up: ## Start Redpanda, ClickHouse and Jaeger
	docker compose up -d --wait redpanda clickhouse jaeger

infra-down: ## Stop infrastructure (keeps data volumes)
	docker compose down

generate: ## Regenerate Go and TypeScript types from api/openapi.yaml
	go generate ./...
	cd dashboard && npm run generate

build: ## Build all Go binaries into ./bin
	go build -o bin/ ./cmd/...

test: ## Unit tests (no infrastructure needed)
	go test -race ./...

test-integration: ## Unit + ClickHouse integration tests (needs `make infra-up`)
	$(LOAD_ENV) go test -race -count=1 ./...

lint: ## gofmt + go vet
	@test -z "$$(gofmt -l cmd internal)" || (gofmt -l cmd internal; exit 1)
	go vet ./...

run-ingest: build ## Run the ingest API on :8080
	$(LOAD_ENV) ./bin/ingest

run-sink: build ## Run the stream -> ClickHouse sink
	$(LOAD_ENV) ./bin/sink

run-query: build ## Run the query API on :8081
	$(LOAD_ENV) ./bin/query

load: build ## Send demo traffic (RATE events/s for DURATION)
	$(LOAD_ENV) ./bin/loadgen -rate $${RATE:-20000} -duration $${DURATION:-60s}

verify: build ## Send a fixed load and verify every accepted event was stored exactly once
	$(LOAD_ENV) ./bin/loadgen -rate $${RATE:-20000} -duration $${DURATION:-20s} -verify

dashboard-install: ## Install dashboard dependencies
	cd dashboard && npm ci

dashboard-dev: ## Run the dashboard dev server on :5174
	$(LOAD_ENV) cd dashboard && npm run dev

dashboard-test: ## Dashboard typecheck, lint and unit tests
	cd dashboard && npm run typecheck && npm run lint && npm test

up: ## Full stack in Docker (infra + services + dashboard on :8088)
	docker compose --profile app up -d --build --wait

down: ## Stop the full stack
	docker compose --profile app down

clean: ## Remove build output
	rm -rf bin dashboard/dist

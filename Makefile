# Short names for common commands. Run them as: make test, make up, ...
# Each target is one command, so you can also copy it and run it directly.
# All commands work in PowerShell, cmd.exe, and Linux/macOS shells.

# Recipe lines normally must start with a TAB character. Copy-pasting
# often turns tabs into spaces, which breaks the Makefile, so we use ">"
# as the recipe prefix instead.
.RECIPEPREFIX = >

.PHONY: help test test-integration test-all cover bench bench-integration vet lint fmt build \
        up down ps up-app down-app logs run-api run-worker

help:
> @echo "make test              unit tests (no database needed)"
> @echo "make test-integration  integration tests (needs: make up)"
> @echo "make test-all          unit + integration tests with the race detector (Linux/macOS; on Windows -race needs a C compiler)"
> @echo "make cover             coverage report -> coverage.html"
> @echo "make bench             micro-benchmarks"
> @echo "make bench-integration database and end-to-end throughput benchmarks"
> @echo "make vet / lint / fmt  static checks and formatting"
> @echo "make build             build both programs into bin/"
> @echo "make up / down         start / stop PostgreSQL and Redis"
> @echo "make up-app            run the whole stack in Docker (API + worker too)"
> @echo "make run-api           run the API on your machine"
> @echo "make run-worker        run a worker on your machine"

test:
> go test ./...

# -p 1: test packages one at a time, because they share one database.
test-integration:
> go test -tags=integration -p 1 -count=1 ./...

# The race detector needs cgo. On Windows without a C compiler, use
# "make test-integration" instead; CI runs the race detector on Linux.
test-all:
> go test -race -tags=integration -p 1 -count=1 ./...

cover:
> go test -tags=integration -p 1 -count=1 -coverprofile=coverage.out ./...
> go tool cover -html=coverage.out -o coverage.html

bench:
> go test -run=NONE -bench=. -benchmem ./...

bench-integration:
> go test -tags=integration -p 1 -run=NONE -bench=. -benchtime=2000x ./internal/store/...
> go test -tags=integration -p 1 -run=NONE -bench=Throughput -benchtime=1x ./internal/worker/...

vet:
> go vet -tags=integration ./...

lint:
> staticcheck -tags=integration ./...

fmt:
> gofmt -l -w .

build:
> go build -o bin/ ./cmd/...

up:
> docker compose up -d --wait

down:
> docker compose --profile app down

ps:
> docker compose --profile app ps

up-app:
> docker compose --profile app up -d --build --wait

logs:
> docker compose --profile app logs -f api worker

run-api:
> go run ./cmd/goflow-api

run-worker:
> go run ./cmd/goflow-worker

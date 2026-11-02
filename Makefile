# Short names for common commands. Run them as: make test, make up, ...
# Each target is one command, so you can also copy it and run it directly.

# Recipe lines normally must start with a TAB character. Copy-pasting
# often turns tabs into spaces, which breaks the Makefile, so we use ">"
# as the recipe prefix instead.
.RECIPEPREFIX = >

.PHONY: test vet lint build up down ps run-api run-worker

test:
> go test ./...

vet:
> go vet ./...

lint:
> staticcheck ./...

build:
> go build -o bin/ ./cmd/...

up:
> docker compose up -d --wait

down:
> docker compose down

ps:
> docker compose ps

run-api:
> go run ./cmd/goflow-api

run-worker:
> go run ./cmd/goflow-worker
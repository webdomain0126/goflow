# Multi-stage build: stage 1 has the full Go toolchain; the final image
# contains only the two compiled binaries (no shell, no compiler), which
# makes it small and gives attackers very little to work with.

# ---- build stage ----
FROM golang:1.24-alpine AS build
WORKDIR /src

# Download dependencies first, in their own layer. Docker caches it, so
# changing source code does not re-download every module.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
# CGO_ENABLED=0: a fully static binary that runs on a minimal base image.
# -trimpath and -s -w: reproducible builds, smaller binaries.
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ ./cmd/...

# ---- runtime stage ----
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/ /usr/local/bin/
# nonroot: the process does not run as root inside the container.
USER nonroot:nonroot
EXPOSE 8080 9091
# The same image runs either program; docker-compose picks one per service.
ENTRYPOINT ["/usr/local/bin/goflow-api"]

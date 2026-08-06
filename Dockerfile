FROM golang:1.22 AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY cmd/ cmd/
COPY pkg/ pkg/
RUN CGO_ENABLED=0 go build -o okd-release-watcher ./cmd/okd-release-watcher

FROM registry.access.redhat.com/ubi9/ubi-minimal:latest
COPY --from=builder /app/okd-release-watcher /okd-release-watcher
ENTRYPOINT ["/okd-release-watcher"]
EXPOSE 8080

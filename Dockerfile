# syntax=docker/dockerfile:1
FROM golang:1.27.4-bookworm@sha256:4fa7c968ba93f1635670522dbb01a2c78150443e1385746924566079f36570ea AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
COPY migrations ./migrations
ARG VERSION=0.2.0
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X github.com/raunaksahahere/postgres-embedding-sync/internal/version.Version=${VERSION}" \
    -o /out/embedsync ./cmd/embedsync

FROM gcr.io/distroless/static-debian12:nonroot@sha256:afa5c872c891853ca7fcf1f12c3edb23f7eeef36189728842dd51042ff57f7ab
COPY --from=build /out/embedsync /usr/local/bin/embedsync
USER nonroot:nonroot
EXPOSE 9187
ENTRYPOINT ["/usr/local/bin/embedsync"]
CMD ["run"]

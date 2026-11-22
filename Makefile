# One command per suite. Every target is what CI runs.
GO          ?= go
PG          ?= 17
STATICCHECK := honnef.co/go/tools/cmd/staticcheck@v0.8.2
VERSION     := $(shell cat VERSION)

.PHONY: all fmt fmt-check lint test unit race integration integration-race e2e images-check \
        bench bench-ci release-build release-check clean

all: fmt-check lint test

fmt:
	gofmt -w cmd internal migrations test
	cd embedder && uv run --frozen ruff format src tests
	cd bench && uv run --frozen ruff format src tests
	cd examples/langchain && uv run --frozen ruff format .
	cd examples/llamaindex && uv run --frozen ruff format .

fmt-check:
	@test -z "$$(gofmt -l cmd internal migrations test)" || (gofmt -l cmd internal migrations test; exit 1)
	cd embedder && uv run --frozen ruff format --check src tests
	cd bench && uv run --frozen ruff format --check src tests
	cd examples/langchain && uv run --frozen ruff format --check .
	cd examples/llamaindex && uv run --frozen ruff format --check .

lint:
	$(GO) vet ./...
	$(GO) vet -tags integration ./test/...
	$(GO) run $(STATICCHECK) -tags integration ./...
	cd embedder && uv run --frozen ruff check src tests
	cd bench && uv run --frozen ruff check src tests
	cd examples/langchain && uv run --frozen ruff check .
	cd examples/llamaindex && uv run --frozen ruff check .

# Unit tests (Go with the race detector, including the backfill interleaving
# property test), embedder, bench harness and integration example tests.
test: race
	cd embedder && uv run --frozen pytest -q
	cd bench && uv run --frozen pytest -q
	cd examples/langchain && uv run --frozen pytest -q
	cd examples/llamaindex && uv run --frozen pytest -q

unit:
	$(GO) test ./...

race:
	$(GO) test -race -count=1 ./...

# Real PostgreSQL integration tests (Docker): PG=16|17|18
integration:
	scripts/test-integration.sh $(PG)

integration-race:
	RACE=1 scripts/test-integration.sh $(PG)

# Docker Compose quickstart end to end (edit, delete, search, verify, gate report).
e2e:
	scripts/e2e.sh

images-check:
	scripts/check-images.sh

# Benchmarks (minutes to an hour on a laptop CPU); results in bench/results/.
bench:
	cd bench && uv run --frozen embedsync-bench all

release-build:
	scripts/release-build.sh $(VERSION)

release-check: release-build
	scripts/release-check.sh $(VERSION) dist --cli-only

clean:
	rm -rf dist

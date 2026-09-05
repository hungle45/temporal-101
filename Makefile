.PHONY: sync create-service proto lint-deps lint gen docker-up docker-down test run run-worker run-client

GOPATH := $(shell go env GOPATH)
BUF := $(GOPATH)/bin/buf
MODULES := $(shell go work edit -json | jq -r '.Use[].DiskPath')

sync:
	rm -rf vendor
	go work sync
	go work vendor

use:
	go work use -r .

# make create-service name=worker
create-service:
	@test -n "$(name)" || (echo "Usage: make create-service name=<service-name>" && exit 1)
	mkdir -p $(name)
	cd $(name) && go mod init go.101.temporal/$(name)
	go work use ./$(name)
	$(MAKE) sync

proto-deps:
	go install github.com/bufbuild/buf/cmd/buf@latest # orchestrator
	go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.11 # gen protobuf
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.6.2 # gen grpc
	$(BUF) dep update

# Generate Go code from proto files
proto-gen:
	rm -rf proto/gen/go
	$(BUF) generate

# Format proto files using buf
proto-format:
	$(BUF) format -w

# Lint proto files using buf
proto-lint:
	$(BUF) lint

# Run all proto-related tasks
proto: proto-deps proto-format proto-lint proto-gen

# Export proto files to vendor directory
proto-vendor:
	buf export . -o vendor-proto

lint-deps:
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest

lint:
	@for module in $(MODULES); do \
		echo "Linting $$module..."; \
		golangci-lint run $$module/... --fix || exit 1; \
	done

gen:
	go install go.uber.org/mock/mockgen@latest
	go install github.com/abice/go-enum@latest
	go generate ./...

docker-up:
	cd .dev && docker compose up -d

docker-down:
	cd .dev && docker compose down -v

run:
	$(MAKE) -j2 run-worker run-client

run-worker:
	go run worker/cmd/main.go

run-client:
	go run client/cmd/main.go

test:
	go work edit -json | jq -r '.Use[].DiskPath' | xargs -I {} go test {}/... -v

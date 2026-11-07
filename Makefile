SERVICE_NAME ?= notification_service

.PHONY: install test cover build run lint fmt vet tidy

install:
	go mod download

test:
	go test ./... -race -count=1 -parallel 4

cover:
	go test ./... -race -count=1 -parallel 4 -coverpkg=./internal/... -coverprofile=coverage.out
	go tool cover -func=coverage.out | tail -1

build:
	docker build -t $(SERVICE_NAME):local .

run:
	go run ./cmd/server

lint:
	golangci-lint run ./...

fmt:
	gofmt -w .

vet:
	go vet ./...

tidy:
	go mod tidy

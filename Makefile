SHELL := /bin/sh

.PHONY: build run-apiserver run-worker lint fmt

fmt:
	go fmt ./...

build:
	go build ./...

run-apiserver:
	go run ./cmd/apiserver

run-worker:
	go run ./cmd/worker

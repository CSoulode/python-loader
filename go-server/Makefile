SHELL := /bin/bash

.DEFAULT_GOAL := help

GO ?= go
DLV ?= $(or $(shell command -v dlv 2>/dev/null),/go/bin/dlv)

ENV_FILE ?= .env
SERVER_PKG := ./server
SERVER_DIR := ./server
OUT_DIR := ./out/server
BIN := $(OUT_DIR)/server
DEBUG_ADDR ?= 127.0.0.1:40511

.PHONY: help check-env build run run-bin debug dap test fmt clean

help: ## Show available targets
	@awk 'BEGIN {FS = ":.*## "}; /^[a-zA-Z0-9_.-]+:.*## / {printf "%-10s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

check-env: ## Ensure the runtime .env file exists
	@test -f "$(ENV_FILE)" || { echo "Missing $(ENV_FILE)"; exit 1; }

build: ## Build the Go server binary into ./out/server/server
	@mkdir -p "$(OUT_DIR)"
	@$(GO) build -trimpath -ldflags="-s -w" -o "$(BIN)" "$(SERVER_PKG)"

run: check-env ## Run the Go server from source with env from .env
	@set -a; source "$(ENV_FILE)"; set +a; $(GO) run "$(SERVER_PKG)"

run-bin: check-env build ## Run the built server binary with env from .env
	@set -a; source "$(ENV_FILE)"; set +a; "$(BIN)"

debug: check-env ## Start a headless Delve debug server for ./server
	@set -a; source "$(ENV_FILE)"; set +a; "$(DLV)" debug "$(SERVER_PKG)" --headless --listen="$(DEBUG_ADDR)" --api-version=2 --accept-multiclient --log

dap: ## Start a Delve DAP server from ./server for IDE attach flows
	@cd "$(SERVER_DIR)" && "$(DLV)" dap --listen="$(DEBUG_ADDR)" --log --log-dest=3

test: ## Run Go tests for the module
	@$(GO) test ./...

fmt: ## Run gofmt/go fmt for the module
	@$(GO) fmt ./...

clean: ## Remove the built server binary
	@rm -f "$(BIN)"

#!/usr/bin/env bash
set -euo pipefail

# Workspace path (mounted to host)
ENV_DST="./out/server/.env"

echo "Copying env file to $ENV_DST (host-mounted)"
mkdir -p ./out/server
cp .env "$ENV_DST"

echo "Building server binary"
go build -trimpath -ldflags="-s -w" -o ./out/server ./server

#!/bin/bash
set -e

# Get version from git tag or default to dev
RAW_VERSION=$(git describe --tags --always 2>/dev/null || echo "dev")
# Replace "-g" with "-git-" for readability (e.g., v0.7.1-1-ge978473 -> v0.7.1-1-git-e978473)
VERSION=$(echo "$RAW_VERSION" | sed 's/-g\([0-9a-f]\)/-git-\1/')
COMMIT=$(git rev-parse --short HEAD 2>/dev/null || echo "unknown")

# Build with version info
go build -ldflags "-X main.Version=${VERSION} -X main.GitCommit=${COMMIT}" -o ~/go/bin/tinykvs ./cmd/tinykvs

echo "Built tinykvs ${VERSION} (${COMMIT})"

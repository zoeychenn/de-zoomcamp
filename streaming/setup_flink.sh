#!/bin/bash
# Download Flink workshop files (uses --no-check-certificate for SSL issues)
set -e
PREFIX="https://raw.githubusercontent.com/DataTalksClub/data-engineering-zoomcamp/main/07-streaming/workshop"
# Skip Dockerfile.flink - use local version with pemja/JDK fixes
wget --no-check-certificate -O pyproject.flink.toml "${PREFIX}/pyproject.flink.toml"
wget --no-check-certificate -O flink-config.yaml "${PREFIX}/flink-config.yaml"
echo "Done. pyproject.flink.toml and flink-config.yaml downloaded. (Dockerfile.flink kept local for pemja fixes)"

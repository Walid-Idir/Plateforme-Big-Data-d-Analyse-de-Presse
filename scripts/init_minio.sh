#!/usr/bin/env bash
set -euo pipefail
echo "Initializing MinIO bucket 'news-data'..."
mc alias set local http://localhost:9000 minioadmin minioadmin || true
mc mb --ignore-existing local/news-data
mc policy set download local/news-data || true

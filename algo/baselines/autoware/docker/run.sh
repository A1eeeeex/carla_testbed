#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

case "${1:-up}" in
  up)
    docker compose -f compose.yaml up -d ;;
  down)
    docker compose -f compose.yaml down ;;
  logs)
    docker compose -f compose.yaml logs -f ;;
  *)
    echo "usage: $0 [up|down|logs]" ;;
esac

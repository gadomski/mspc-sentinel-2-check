#!/usr/bin/env bash
set -euo pipefail

if [ $# -ne 1 ]; then
    echo "usage: $0 <year>" >&2
    exit 1
fi

year="$1"

for month in 01 02 03 04 05 06 07 08 09 10 11 12; do
    echo "=== $year-$month ==="
    uv run mspc-sentinel-2-check prefixes "$year" "$month"
done

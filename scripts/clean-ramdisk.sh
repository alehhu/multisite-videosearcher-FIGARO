#!/bin/bash
# Remove the job directories left on a site's ramdisk after an experiment.
# The workers never delete them, so run this on EACH node between experiments.
#
#   scripts/clean-ramdisk.sh                  # dry run: shows what would be removed
#   scripts/clean-ramdisk.sh --delete         # actually removes
#   scripts/clean-ramdisk.sh --dir /mnt/ramdisk --pattern 'job-*' --delete
#
# Only directories directly under --dir whose name matches --pattern are touched:
# seed/, video.mp4 and anything else are left alone. The gateway names jobs job-<uuid>;
# jobs sent with an explicit "dir" (e.g. test-1) need their own --pattern.
set -euo pipefail

DIR="/mnt/ramdisk"
PATTERN="job-*"
DELETE="false"
while [[ $# -gt 0 ]]; do
    case "$1" in
        --dir) DIR="${2:-}"; shift 2 ;;
        --pattern) PATTERN="${2:-}"; shift 2 ;;
        --delete) DELETE="true"; shift ;;
        -h|--help) sed -n '2,11p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) echo "Error: unknown argument '$1'" >&2; exit 1 ;;
    esac
done

if [[ ! -d "$DIR" ]]; then
    echo "Error: $DIR is not a directory" >&2; exit 1
fi
if [[ -z "$PATTERN" || "$PATTERN" == "*" || "$PATTERN" == "seed" || "$PATTERN" == "seed*" ]]; then
    echo "Error: refusing pattern '$PATTERN' (it would match the seeds)" >&2; exit 1
fi

COUNT=$(find "$DIR" -mindepth 1 -maxdepth 1 -type d -name "$PATTERN" | wc -l | tr -d ' ')
SIZE=$(find "$DIR" -mindepth 1 -maxdepth 1 -type d -name "$PATTERN" -exec du -sk {} + 2>/dev/null | awk '{s+=$1} END {printf "%.1f", s/1024}')
echo "$COUNT job directories matching '$PATTERN' in $DIR (${SIZE:-0.0} MB)"

if [[ "$DELETE" != "true" ]]; then
    find "$DIR" -mindepth 1 -maxdepth 1 -type d -name "$PATTERN" | sort | head -5
    echo "Dry run: nothing removed. Re-run with --delete."
    exit 0
fi
find "$DIR" -mindepth 1 -maxdepth 1 -type d -name "$PATTERN" -exec rm -rf {} +
echo "Removed. Left in $DIR:"; ls "$DIR"

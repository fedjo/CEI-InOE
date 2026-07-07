#!/usr/bin/env bash
set -euo pipefail

SOURCE_DIR="${SOURCE_DIR:-/home/ocei-inoe-admin/DelavalShare}"
TARGET_DIR="${TARGET_DIR:-/data/incoming}"
LOCK_FILE="${LOCK_FILE:-/tmp/delavalshare-sync.lock}"

log() {
  logger -t delavalshare-sync "$1"
  printf '%s\n' "$1"
}

if [[ ! -d "$SOURCE_DIR" ]]; then
  log "Source directory does not exist: $SOURCE_DIR"
  exit 0
fi

mkdir -p "$TARGET_DIR"

# Prevent overlapping runs.
exec 9>"$LOCK_FILE"
if ! flock -n 9; then
  log "Another sync process is already running, skipping"
  exit 0
fi

moved_count=0
failed_count=0

while IFS= read -r -d '' file_path; do
  file_name="$(basename "$file_path")"

  # Rename canonical export file to include current date.
  if [[ "$file_name" == "output.xls" ]]; then
    file_name="output-$(date +%Y%m%d).xls"
  fi

  target_path="$TARGET_DIR/$file_name"

  # Skip if target already exists to avoid accidental overwrite.
  if [[ -e "$target_path" ]]; then
    log "Skipping existing target file: $target_path"
    continue
  fi

  if mv -- "$file_path" "$target_path"; then
    moved_count=$((moved_count + 1))
    log "Moved: $file_name"
  else
    failed_count=$((failed_count + 1))
    log "Failed to move: $file_name"
  fi
done < <(
  find "$SOURCE_DIR" -maxdepth 1 -type f \( \
    -iname '*.csv' -o -iname '*.xls' -o -iname '*.xlsx' \
  \) -print0
)

log "Sync complete. moved=$moved_count failed=$failed_count source=$SOURCE_DIR target=$TARGET_DIR"

#!/bin/sh
set -eu

cache_dir="${XDG_CACHE_HOME:-$HOME/.cache}/tmux"
disk_cache="$cache_dir/status_disk_root"
disk_ttl_seconds=300

format_bytes_gib() {
  awk -v bytes="$1" 'BEGIN { printf "%.0fG", bytes / 1024 / 1024 / 1024 }'
}

memory_status_darwin() {
  total_bytes=$(sysctl -n hw.memsize)

  # Prefer a pressure-oriented number for macOS: count memory that is actually
  # committed to apps/the OS (anonymous + wired + compressor footprint), but do
  # not count file-backed cached pages. Cached files are reclaimable and are
  # shown separately from "Memory Used" in Activity Monitor.
  vm_stat | awk -v total_bytes="$total_bytes" '
    /page size of/ { page_size=$8; gsub(/[^0-9]/, "", page_size) }
    /Pages wired down:/ { wired=$4; gsub(/\./, "", wired) }
    /Anonymous pages:/ { anonymous=$3; gsub(/\./, "", anonymous) }
    /Pages occupied by compressor:/ { compressor=$5; gsub(/\./, "", compressor) }
    END {
      if (!page_size) page_size = 4096
      used_bytes = (wired + anonymous + compressor) * page_size
      if (total_bytes > 0) printf "RAM %.0f%%", used_bytes * 100 / total_bytes
      else printf "RAM n/a"
    }'
}

memory_status_linux() {
  awk '/MemTotal:/ { total=$2 } /MemAvailable:/ { available=$2 } END { if (total > 0) printf "RAM %.0f%%", (total - available) * 100 / total; else printf "RAM n/a" }' /proc/meminfo
}

memory_status() {
  case "$(uname -s)" in
    Darwin) memory_status_darwin ;;
    Linux) memory_status_linux ;;
    *) printf 'RAM n/a' ;;
  esac
}

disk_status_refresh() {
  # User data volume usage. On macOS, / is the sealed system volume and can
  # misleadingly show low usage; $HOME maps to the APFS Data volume that fills up.
  df -H "$HOME" | awk 'NR==2 { printf "DSK %s", $5 }'
}

disk_status() {
  mkdir -p "$cache_dir"

  now=$(date +%s)
  cache_mtime=0
  if [ -f "$disk_cache" ]; then
    cache_mtime=$(stat -f %m "$disk_cache" 2>/dev/null || stat -c %Y "$disk_cache" 2>/dev/null || printf '0')
  fi

  if [ ! -s "$disk_cache" ] || [ $((now - cache_mtime)) -ge "$disk_ttl_seconds" ]; then
    disk_status_refresh > "$disk_cache.tmp" && mv "$disk_cache.tmp" "$disk_cache"
  fi

  cat "$disk_cache"
}

printf '%s %s' "$(memory_status)" "$(disk_status)"

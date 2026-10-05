#!/usr/bin/env bash
#
# Shared helper for install-/restart-/status-sinks.sh (adapted from
# linksys-velop-watcher, minus its legacy CrateDB set): prints one connector
# config path per line. Source this after setting HERE.

sink_files() {
  local f
  for f in "${HERE}"/tapo-sink-*-postgres.json; do
    echo "$f"
  done
}

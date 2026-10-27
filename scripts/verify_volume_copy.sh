#!/usr/bin/env bash
set -euo pipefail

src=${1:-/src}
dst=${2:-/dst}
mode=${3:-all}
case "$mode" in
  all) selector=( -type f ) ;;
  hdfs-nn) selector=( -type f \( -name VERSION -o -name 'fsimage_*' \) ) ;;
  hdfs-blocks) selector=( -type f -name 'blk_*' ) ;;
  *) echo "unknown verification mode: $mode" >&2; exit 2 ;;
esac
(cd "$src" && find . "${selector[@]}" -print0 | sort -z | xargs -0 -r sha256sum) >/tmp/src.sha
(cd "$dst" && find . "${selector[@]}" -print0 | sort -z | xargs -0 -r sha256sum) >/tmp/dst.sha
diff -u /tmp/src.sha /tmp/dst.sha
if [[ "$mode" == all ]]; then
  (cd "$src" && find . -type l -printf '%p -> %l\n' | sort) >/tmp/src.links
  (cd "$dst" && find . -type l -printf '%p -> %l\n' | sort) >/tmp/dst.links
  diff -u /tmp/src.links /tmp/dst.links
fi

src_count=$(find "$src" "${selector[@]}" | wc -l)
dst_count=$(find "$dst" "${selector[@]}" | wc -l)
src_bytes=$(find "$src" "${selector[@]}" -printf '%s\n' | awk '{s += $1} END {print s+0}')
dst_bytes=$(find "$dst" "${selector[@]}" -printf '%s\n' | awk '{s += $1} END {print s+0}')
test "$src_count" = "$dst_count"
test "$src_bytes" = "$dst_bytes"
echo "mode=$mode files=$src_count bytes=$src_bytes"

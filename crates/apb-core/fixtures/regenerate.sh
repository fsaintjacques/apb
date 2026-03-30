#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

for proto in *.proto; do
  bin="${proto%.proto}.bin"
  protoc --descriptor_set_out="$bin" --include_imports "$proto"
  echo "  $proto -> $bin"
done

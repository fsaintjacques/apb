#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

APB_PROTO_DIR="../proto"

# Regenerate the apb extension descriptor embedded by apb-core.
protoc -I "$APB_PROTO_DIR" --descriptor_set_out="$APB_PROTO_DIR/apb.bin" \
  --include_imports apb.proto
echo "  apb.proto -> $APB_PROTO_DIR/apb.bin"

for proto in *.proto; do
  bin="${proto%.proto}.bin"
  protoc -I . -I "$APB_PROTO_DIR" --descriptor_set_out="$bin" --include_imports "$proto"
  echo "  $proto -> $bin"
done

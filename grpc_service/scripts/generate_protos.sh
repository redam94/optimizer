#!/bin/bash
set -e

PROTO_DIR="protos"
OUT_DIR="generated"

# Create output directory
mkdir -p $OUT_DIR

# Generate Python files
python -m grpc_tools.protoc \
    --proto_path=$PROTO_DIR \
    --python_out=$OUT_DIR \
    --grpc_python_out=$OUT_DIR \
    $PROTO_DIR/budget_optimizer.proto

echo "Protocol buffer files generated successfully in $OUT_DIR"

# Fix import issues in generated files
sed -i 's/import budget_optimizer_pb2/from . import budget_optimizer_pb2/g' $OUT_DIR/budget_optimizer_pb2_grpc.py

echo "Import paths fixed"
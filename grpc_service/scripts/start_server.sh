#!/bin/bash
# scripts/start_server.sh

set -e

# Default values
HOST="0.0.0.0"
PORT="50051"
DATABASE_URL="sqlite:///budget_optimizer.db"
LOG_LEVEL="INFO"

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --host)
            HOST="$2"
            shift 2
            ;;
        --port)
            PORT="$2"
            shift 2
            ;;
        --database-url)
            DATABASE_URL="$2"
            shift 2
            ;;
        --log-level)
            LOG_LEVEL="$2"
            shift 2
            ;;
        *)
            echo "Unknown option $1"
            exit 1
            ;;
    esac
done

export DATABASE_URL=$DATABASE_URL
export LOG_LEVEL=$LOG_LEVEL

echo "Starting Budget Optimizer gRPC Server..."
echo "Host: $HOST"
echo "Port: $PORT"
echo "Database: $DATABASE_URL"
echo "Log Level: $LOG_LEVEL"

# Start the server
python budget_optimizer_server.py --host $HOST --port $PORT
#!/usr/bin/env bash
set -euo pipefail

# Determine script directory
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$DIR/../.." && pwd)"

# 1. Detect Docker Compose command
if docker compose version >/dev/null 2>&1; then
    DOCKER_COMPOSE="docker compose"
elif docker-compose version >/dev/null 2>&1; then
    DOCKER_COMPOSE="docker-compose"
else
    echo "Error: Neither 'docker compose' nor 'docker-compose' was found." >&2
    exit 1
fi

echo "Using Docker Compose command: $DOCKER_COMPOSE"

# Helper to check if local port is open/in-use
check_port() {
    local port=$1
    python3 -c "import socket; s = socket.socket(); s.settimeout(0.2); s.connect(('127.0.0.1', $port))" >/dev/null 2>&1
}

# Helper to wait for a port to be listening
wait_for_port() {
    local port=$1
    local name=$2
    local timeout=20
    echo "Waiting for $name to be ready on port $port..."
    for ((i=1; i<=timeout; i++)); do
        if check_port "$port"; then
            echo "$name is ready!"
            return 0
        fi
        if [ -n "${RECEIVER_PID:-}" ] && ! kill -0 "$RECEIVER_PID" 2>/dev/null; then
            echo "Error: Receiver process (PID: $RECEIVER_PID) died. Log output:" >&2
            cat "$DIR/receiver.log" >&2
            return 1
        fi
        sleep 1
    done
    echo "Error: Timeout waiting for $name on port $port" >&2
    return 1
}

echo "=== 1. Starting Iceberg & MinIO Services ==="
$DOCKER_COMPOSE -f "$DIR/docker-compose.yml" down -v
$DOCKER_COMPOSE -f "$DIR/docker-compose.yml" up -d

# Virtual environment setup
echo "=== 2. Setting up Python Virtual Environment ==="
if [ ! -d "$DIR/.venv" ]; then
    python3 -m venv "$DIR/.venv"
fi
source "$DIR/.venv/bin/activate"

# Only pip install if dependencies are not already satisfied
if ! python3 -c "import pyiceberg, pyarrow, requests" >/dev/null 2>&1; then
    echo "Installing Python dependencies..."
    pip install -r "$DIR/requirements.txt"
else
    echo "Python dependencies already satisfied."
fi

echo "=== 3. Bootstrapping Iceberg Tables ==="
python "$DIR/bootstrap.py"

echo "=== 4. Starting opentelemetry-datalake Receiver ==="
export AWS_ACCESS_KEY_ID=admin
export AWS_SECRET_ACCESS_KEY=password
export AWS_REGION=us-east-1
export AWS_ENDPOINT_URL=http://localhost:9000

# Build first to avoid starting receiver during long compilation
cargo build --bin opentelemetry-datalake

# Start receiver in background
"$ROOT_DIR/target/debug/opentelemetry-datalake" --config "$DIR/config.toml" > "$DIR/receiver.log" 2>&1 &
RECEIVER_PID=$!

# Ensure cleanup on exit
cleanup() {
    echo "=== Cleaning up ==="
    if [ -n "${RECEIVER_PID:-}" ]; then
        echo "Stopping receiver (PID: $RECEIVER_PID)..."
        kill -SIGINT "$RECEIVER_PID" || true
        wait "$RECEIVER_PID" || true
    fi
    echo "Stopping Docker services..."
    $DOCKER_COMPOSE -f "$DIR/docker-compose.yml" down -v
}
trap cleanup EXIT

# Wait for receiver ports to bind
wait_for_port 4317 "OTLP gRPC Receiver"
wait_for_port 4318 "OTLP HTTP Receiver"

# Telemetry volume configuration for performance evaluation
E2E_RATE="${E2E_RATE:-5000}"
E2E_DURATION="${E2E_DURATION:-1m}"

echo "=== 5. Generating Telemetry via telemetrygen ==="
# Detect OS: on Linux, --network host is the most robust way to hit host localhost.
# On other platforms (macOS/Windows), we fallback to host.docker.internal.
OS_NAME="$(uname -s)"
NETWORK_ARGS="--network host"
TARGET_ENDPOINT="localhost:4317"

if [ "$OS_NAME" != "Linux" ]; then
    NETWORK_ARGS="--add-host=host.docker.internal:host-gateway"
    TARGET_ENDPOINT="host.docker.internal:4317"
fi

echo "Generating logs using network: $NETWORK_ARGS, endpoint: $TARGET_ENDPOINT, rate: $E2E_RATE/s, duration: $E2E_DURATION"
docker run --rm $NETWORK_ARGS \
  ghcr.io/open-telemetry/opentelemetry-collector-contrib/telemetrygen:latest logs \
  --otlp-endpoint="$TARGET_ENDPOINT" \
  --otlp-insecure \
  --rate="$E2E_RATE" \
  --duration="$E2E_DURATION"

echo "Generating metrics using network: $NETWORK_ARGS, endpoint: $TARGET_ENDPOINT, rate: $E2E_RATE/s, duration: $E2E_DURATION"
docker run --rm $NETWORK_ARGS \
  ghcr.io/open-telemetry/opentelemetry-collector-contrib/telemetrygen:latest metrics \
  --otlp-endpoint="$TARGET_ENDPOINT" \
  --otlp-insecure \
  --rate="$E2E_RATE" \
  --duration="$E2E_DURATION"

echo "Generating traces using network: $NETWORK_ARGS, endpoint: $TARGET_ENDPOINT, rate: $E2E_RATE/s, duration: $E2E_DURATION"
docker run --rm $NETWORK_ARGS \
  ghcr.io/open-telemetry/opentelemetry-collector-contrib/telemetrygen:latest traces \
  --otlp-endpoint="$TARGET_ENDPOINT" \
  --otlp-insecure \
  --rate="$E2E_RATE" \
  --duration="$E2E_DURATION"

echo "=== 6. Verifying Persistence in Apache Iceberg ==="
if ! python "$DIR/verify.py"; then
    echo "Verification FAILED. Receiver logs:"
    cat "$DIR/receiver.log"
    exit 1
fi

echo "E2E Test completed successfully!"

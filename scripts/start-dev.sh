#!/usr/bin/env bash
#
# start-dev.sh
#
# One-command infrastructure setup for local development:
#   1. Validate Docker is installed and running
#   2. Start project containers (docker compose up -d)
#   3. Wait until they report healthy
#   4. Run initialization scripts (Kafka topics, etc.)
#
# This script does NOT install Docker, start Docker Desktop, or manage
# the OS in any way - it only validates and fails with a clear message
# if something is missing. Spring Boot is started separately, from the IDE.

set -euo pipefail

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
COMPOSE_FILE="${PROJECT_ROOT}/docker-compose.yml"
TOPICS_SCRIPT="${SCRIPT_DIR}/create-kafka-topics.sh"
WAIT_TIMEOUT_SECONDS="${WAIT_TIMEOUT_SECONDS:-180}"

log()  { echo "[start-dev] $*"; }
fail() { echo "[start-dev] ERROR: $*" >&2; exit 1; }

# ---------------------------------------------------------------------------
# 1. Validate Docker CLI
# ---------------------------------------------------------------------------
log "Validating Docker CLI..."
if ! command -v docker >/dev/null 2>&1; then
  fail "Docker CLI not found. Install Docker Desktop / Docker Engine, then re-run this script."
fi

if ! docker compose version >/dev/null 2>&1; then
  fail "'docker compose' plugin not found. Update Docker Desktop / install the Compose plugin, then re-run this script."
fi

# ---------------------------------------------------------------------------
# 2. Validate Docker daemon is reachable
# ---------------------------------------------------------------------------
log "Validating Docker daemon is running..."
if ! docker info >/dev/null 2>&1; then
  fail "Docker daemon is not reachable. Start Docker Desktop / the Docker service, then re-run this script."
fi

# ---------------------------------------------------------------------------
# 3 & 4. Start containers, wait until healthy.
#    docker compose --wait blocks until every service with a healthcheck
#    in docker-compose.yml reports healthy, and fails fast if one goes
#    unhealthy or the timeout is hit.
# ---------------------------------------------------------------------------
log "Starting containers and waiting for them to be healthy (timeout: ${WAIT_TIMEOUT_SECONDS}s)..."
if ! docker compose -f "${COMPOSE_FILE}" up -d --wait --wait-timeout "${WAIT_TIMEOUT_SECONDS}"; then
  fail "Containers failed to start or become healthy within ${WAIT_TIMEOUT_SECONDS}s.
  Check status with: docker compose -f \"${COMPOSE_FILE}\" ps
  Check logs with:    docker compose -f \"${COMPOSE_FILE}\" logs"
fi

# ---------------------------------------------------------------------------
# 5. Execute initialization scripts (each one must be idempotent)
# ---------------------------------------------------------------------------
log "Running initialization scripts..."
if [ ! -f "${TOPICS_SCRIPT}" ]; then
  fail "Expected initialization script not found: ${TOPICS_SCRIPT}"
fi
bash "${TOPICS_SCRIPT}" || fail "create-kafka-topics.sh failed."

# Future initialization scripts (DB seed, Redis init, etc.) go here,
# each as its own idempotent script called from this section.

# ---------------------------------------------------------------------------
# Done
# ---------------------------------------------------------------------------
log "Environment Ready."
log "Next: open the project in your IDE and run the Spring Boot application."

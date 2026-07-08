#!/usr/bin/env bash
#
# create-kafka-topics.sh
#
# Creates any topics from TOPICS that don't already exist on the broker.
# Safe to run multiple times: existing topics are left untouched.
#
# Can be run standalone (as long as the Kafka container is already up) or
# called from start-dev.sh as part of environment initialization.

set -euo pipefail

KAFKA_CONTAINER="${KAFKA_CONTAINER:-payflo-kafka}"
BOOTSTRAP_SERVER="${BOOTSTRAP_SERVER:-localhost:9092}"
KAFKA_TOPIC_PARTITIONS="${KAFKA_TOPIC_PARTITIONS:-3}"
KAFKA_TOPIC_REPLICATION_FACTOR="${KAFKA_TOPIC_REPLICATION_FACTOR:-1}"
KAFKA_BIN="/opt/kafka/bin/kafka-topics.sh"

TOPICS=(
  payflo.payment-initiated
  payflo.notification.payment-initiated
  payflo.payment-received
  payflo.notification.payment-completed
  payflo.payment-timedout
  payflo.notification.payment-timedout
  payflo.payment-failed
  payflo.notification.payment-failed
)

log()  { echo "[create-kafka-topics] $*"; }
fail() { echo "[create-kafka-topics] ERROR: $*" >&2; exit 1; }

log "Fetching existing topics from ${BOOTSTRAP_SERVER}..."
existingTopics=$(docker exec "${KAFKA_CONTAINER}" "${KAFKA_BIN}" --list --bootstrap-server "${BOOTSTRAP_SERVER}")

for topic in "${TOPICS[@]}"; do
  found=false
  for existing in $existingTopics; do
    if [ "$topic" = "$existing" ]; then
      found=true
      break
    fi
  done

  if [ "$found" = "false" ]; then
    log "Creating topic: ${topic}"
    docker exec "${KAFKA_CONTAINER}" "${KAFKA_BIN}" --create \
      --topic "${topic}" \
      --partitions "${KAFKA_TOPIC_PARTITIONS}" \
      --replication-factor "${KAFKA_TOPIC_REPLICATION_FACTOR}" \
      --bootstrap-server "${BOOTSTRAP_SERVER}"
  else
    log "Topic already exists: ${topic}"
  fi
done

log "Topic initialization completed."

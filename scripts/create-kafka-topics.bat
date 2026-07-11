@echo off
setlocal

rem ============================================================================
rem create-kafka-topics.bat
rem
rem Creates any topics from TOPICS that don't already exist on the broker.
rem Safe to run multiple times: existing topics are left untouched.
rem
rem Prerequisite:
rem   - Kafka container must already exist and be running.
rem
rem Can be run standalone (after Kafka has started) or called from
rem start-dev.bat as part of environment initialization.
rem ============================================================================

if "%KAFKA_CONTAINER%"=="" set "KAFKA_CONTAINER=payflo-kafka"
if "%BOOTSTRAP_SERVER%"=="" set "BOOTSTRAP_SERVER=localhost:9092"


if "%KAFKA_TOPIC_PARTITIONS%"=="" set "KAFKA_TOPIC_PARTITIONS=3"
if "%KAFKA_TOPIC_REPLICATION_FACTOR%"=="" set "KAFKA_TOPIC_REPLICATION_FACTOR=1"
if "%KAFKA_BIN%"=="" set "KAFKA_BIN=/opt/kafka/bin/kafka-topics.sh"

set "TOPICS=payflo.payment-initiated payflo.notification.payment-initiated payflo.payment-received payflo.notification.payment-completed payflo.payment-timed-out payflo.notification.payment-timed-out payflo.payment-failed payflo.notification.payment-failed payflo.DLT"

for %%T in (%TOPICS%) do (
    echo [create-kafka-topics] Ensuring topic exists: %%T
    docker exec "%KAFKA_CONTAINER%" %KAFKA_BIN% --create ^
        --if-not-exists ^
        --topic "%%T" ^
        --partitions %KAFKA_TOPIC_PARTITIONS% ^
        --replication-factor %KAFKA_TOPIC_REPLICATION_FACTOR% ^
        --bootstrap-server %BOOTSTRAP_SERVER%

    if errorlevel 1 (
        echo [create-kafka-topics] ERROR: Failed to ensure topic exists: %%T
        exit /b 1
    )
)

echo [create-kafka-topics] Topic initialization completed.

endlocal
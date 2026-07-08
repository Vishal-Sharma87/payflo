@echo off
setlocal

rem ============================================================================
rem start-dev.bat
rem
rem One-command infrastructure setup for local development:
rem   1. Validate Docker is installed and running
rem   2. Start project containers (docker compose up -d)
rem   3. Wait until they report healthy
rem   4. Run initialization scripts (Kafka topics, etc.)
rem
rem This script does NOT install Docker, start Docker Desktop, or manage
rem the OS in any way -- it only validates and fails with a clear message
rem if something is missing. Spring Boot is started separately, from the IDE.
rem ============================================================================

set "SCRIPT_DIR=%~dp0"
pushd "%SCRIPT_DIR%.."
set "PROJECT_ROOT=%CD%"
popd
set "COMPOSE_FILE=%PROJECT_ROOT%\docker-compose.yml"
set "TOPICS_SCRIPT=%SCRIPT_DIR%create-kafka-topics.bat"
if "%WAIT_TIMEOUT_SECONDS%"=="" set "WAIT_TIMEOUT_SECONDS=180"

echo [start-dev] Validating Docker CLI...
where docker >nul 2>nul
if errorlevel 1 (
    echo [start-dev] ERROR: Docker CLI not found. Install Docker Desktop, then re-run this script.
    exit /b 1
)

docker compose version >nul 2>nul
if errorlevel 1 (
    echo [start-dev] ERROR: 'docker compose' plugin not found. Update Docker Desktop, then re-run this script.
    exit /b 1
)

echo [start-dev] Validating Docker daemon is running...
docker info >nul 2>nul
if errorlevel 1 (
    echo [start-dev] ERROR: Docker daemon is not reachable. Start Docker Desktop, then re-run this script.
    exit /b 1
)

echo [start-dev] Starting containers and waiting for them to be healthy (timeout: %WAIT_TIMEOUT_SECONDS%s)...
docker compose -f "%COMPOSE_FILE%" up -d --wait --wait-timeout %WAIT_TIMEOUT_SECONDS%
if errorlevel 1 (
    echo [start-dev] ERROR: Containers failed to start or become healthy within %WAIT_TIMEOUT_SECONDS%s.
    echo [start-dev] Check status with: docker compose -f "%COMPOSE_FILE%" ps
    echo [start-dev] Check logs with:    docker compose -f "%COMPOSE_FILE%" logs
    exit /b 1
)

echo [start-dev] Running initialization scripts...
if not exist "%TOPICS_SCRIPT%" (
    echo [start-dev] ERROR: Expected initialization script not found: %TOPICS_SCRIPT%
    exit /b 1
)
call "%TOPICS_SCRIPT%"
if errorlevel 1 (
    echo [start-dev] ERROR: create-kafka-topics.bat failed.
    exit /b 1
)

rem Future initialization scripts (DB seed, Redis init, etc.) go here,
rem each as its own idempotent script called from this section.

echo [start-dev] Environment Ready.
echo [start-dev] Next: open the project in your IDE and run the Spring Boot application.

endlocal
